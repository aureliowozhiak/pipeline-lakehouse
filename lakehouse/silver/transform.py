"""Camada Silver: limpeza e padronização genéricas, sem regras de negócio.

Etapas (por dataset):
1. achata structs do JSON (endereco.cidade -> endereco_cidade); arrays/mapas viram texto JSON;
2. padroniza nomes de colunas (snake_case, sem acentos, únicos);
3. remove espaços nas bordas e converte vazios / tokens nulos ("NULL", "N/A"...) em nulo;
4. infere tipos de forma conservadora (ver type_inference.py);
5. marca duplicados (_is_duplicate = true a partir da 2ª ocorrência; nada é removido);
6. registra problemas por linha em _quality_issues.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse.config.settings import Settings
from lakehouse.constants import (
    BRONZE_METADATA,
    CORRUPT_COL,
    META_BATCH_ID,
    META_INGESTION_TS,
    META_RECORD_ID,
    META_SOURCE_FILE,
    META_SOURCE_FORMAT,
    RESERVED_COLUMNS,
    SILVER_DUPLICATE_COUNT,
    SILVER_HAS_ISSUES,
    SILVER_IS_DUPLICATE,
    SILVER_ISSUES,
    SILVER_PROCESSED_AT,
    SILVER_ROW_HASH,
)
from lakehouse.silver import type_inference as ti
from lakehouse.utils import delta_io
from lakehouse.utils.naming import normalize_column_names

_STRIP_RE = r"^[\s\u00A0\uFEFF]+|[\s\u00A0]+$"


@dataclass
class ColumnInfo:
    position: int
    column: str  # nome na Silver
    original_column: str  # nome na Bronze (caminho a.b para campos aninhados)
    silver_type: str
    inference_rule: str
    inference_label: str
    parse_rate: float | None
    non_null_raw: int  # não nulos na Bronze
    trimmed_count: int  # valores com espaços removidos
    nullified_count: int  # vazios/tokens convertidos em nulo
    failed_conversions: int  # não nulos que não converteram para o tipo escolhido


@dataclass
class SilverResult:
    dataset: str
    row_count: int
    columns: list[ColumnInfo] = field(default_factory=list)
    duplicate_rows: int = 0
    empty_rows: int = 0
    corrupt_rows: int = 0
    rows_with_issues: int = 0

    def columns_as_dicts(self) -> list[dict]:
        return [asdict(c) for c in self.columns]


def silver_path(settings: Settings, dataset: str) -> Path:
    return settings.silver_dir / dataset


def _flatten(schema: T.StructType, data_cols: list[str]) -> list[tuple[str, Column]]:
    """[(nome_origem, expressão)] com structs expandidos recursivamente."""
    result: list[tuple[str, Column]] = []

    def walk(prefix: str, col: Column, dtype: T.DataType) -> None:
        if isinstance(dtype, T.StructType):
            for fld in dtype.fields:
                walk(f"{prefix}.{fld.name}", col.getField(fld.name), fld.dataType)
        elif isinstance(dtype, (T.ArrayType, T.MapType)):
            result.append((prefix, F.to_json(col)))
        else:
            result.append((prefix, col.cast("string")))

    for name in data_cols:
        walk(name, F.col(f"`{name}`"), schema[name].dataType)
    return result


def build_silver(spark: SparkSession, settings: Settings, dataset: str, bronze_df: DataFrame) -> SilverResult:
    cfg = settings.silver
    threshold = float(cfg.get("type_inference_threshold", 0.95))
    null_tokens = sorted({str(t).strip().lower() for t in cfg.get("null_tokens", [])})
    candidates = ti.build_candidates(cfg.get("boolean_true", []), cfg.get("boolean_false", []))

    has_corrupt = CORRUPT_COL in bronze_df.columns
    data_cols = [c for c in bronze_df.columns if c not in BRONZE_METADATA and c != CORRUPT_COL]
    flat = _flatten(bronze_df.schema, data_cols)
    original_names = [name for name, _ in flat]
    silver_names = normalize_column_names([n.replace(".", "_") for n in original_names], reserved=RESERVED_COLUMNS)

    # --- 1) colunas brutas (texto) + limpas, num único DataFrame em cache --------
    raw_cols = [f"__raw_{i}" for i in range(len(flat))]
    meta = [META_RECORD_ID, META_SOURCE_FILE, META_SOURCE_FORMAT, META_BATCH_ID, META_INGESTION_TS]
    base = bronze_df.select(
        *[expr.alias(raw) for (_, expr), raw in zip(flat, raw_cols)],
        *[F.col(m) for m in meta],
        *([F.col(CORRUPT_COL)] if has_corrupt else []),
    )
    cleaned_exprs = []
    for raw, name in zip(raw_cols, silver_names):
        stripped = F.regexp_replace(F.col(raw), _STRIP_RE, "")
        cleaned_exprs.append(F.when(F.lower(stripped).isin(null_tokens), F.lit(None)).otherwise(stripped).alias(name))
    clean = base.select(*[F.col(r) for r in raw_cols], *cleaned_exprs, *[c for c in base.columns if c not in raw_cols])
    clean = clean.cache()

    # --- 2) estatísticas de limpeza + inferência de tipos em uma só agregação ---
    aggs = [F.count(F.lit(1)).alias("__rows")]
    for i, (raw, name) in enumerate(zip(raw_cols, silver_names)):
        r, c = F.col(raw), F.col(f"`{name}`")
        aggs.append(F.count(r).alias(f"raw_nn__{i}"))
        aggs.append(F.count(F.when(r.isNotNull() & c.isNotNull() & (r != c), 1)).alias(f"trim__{i}"))
        aggs.append(F.count(F.when(r.isNotNull() & c.isNull(), 1)).alias(f"nullified__{i}"))
    aggs += ti.inference_aggregations(silver_names, candidates)
    stats = clean.agg(*aggs).collect()[0].asDict()
    decisions = ti.decide_types(silver_names, candidates, stats, threshold)
    row_count = int(stats["__rows"])

    # --- 3) conversão de tipos + problemas por linha -----------------------------
    typed_cols: list[Column] = []
    issue_exprs: list[Column] = []
    for dec in decisions:
        c = F.col(f"`{dec.column}`")
        if dec.spark_type == "string":
            typed_cols.append(c.alias(dec.column))
            continue
        parsed = ti.parser_for(dec.rule, candidates)(c)
        typed_cols.append(parsed.alias(dec.column))
        failed = c.isNotNull() & parsed.isNull()
        if dec.parse_rate is not None and dec.parse_rate < 1.0:
            issue_exprs.append(
                F.when(failed, F.concat(F.lit(f"tipo_invalido:{dec.column}="), F.substring(c, 1, 60)))
            )

    # as falhas de conversão precisam ser avaliadas aqui, enquanto o valor original existe
    type_issues = F.filter(F.array(*issue_exprs), lambda x: x.isNotNull()) if issue_exprs else F.array().cast(
        "array<string>")
    df = clean.select(*typed_cols, type_issues.alias("__type_issues"), *[F.col(m) for m in meta],
                      *([F.col(CORRUPT_COL)] if has_corrupt else []))

    # --- 4) duplicados (linha idêntica em todas as colunas de dados) ------------
    data_struct = F.struct(*[F.col(f"`{n}`") for n in silver_names]) if silver_names else F.struct(F.lit(None))
    df = df.withColumn(SILVER_ROW_HASH, F.sha2(F.to_json(data_struct), 256))
    w_order = Window.partitionBy(SILVER_ROW_HASH).orderBy(META_SOURCE_FILE, META_RECORD_ID)
    w_all = Window.partitionBy(SILVER_ROW_HASH)
    df = df.withColumn(SILVER_IS_DUPLICATE, F.row_number().over(w_order) > 1)
    df = df.withColumn(SILVER_DUPLICATE_COUNT, F.count(F.lit(1)).over(w_all).cast("int"))

    # --- 5) lista de problemas por linha ----------------------------------------
    all_null = F.lit(True)
    for n in silver_names:
        all_null = all_null & F.col(f"`{n}`").isNull()
    row_issues = [
        F.when(F.col(SILVER_IS_DUPLICATE), F.lit("linha_duplicada")),
        F.when(all_null, F.lit("linha_vazia")),
        *([F.when(F.col(CORRUPT_COL).isNotNull(), F.lit("registro_malformado"))] if has_corrupt else []),
    ]
    df = df.withColumn(
        SILVER_ISSUES,
        F.concat(F.filter(F.array(*row_issues), lambda x: x.isNotNull()), F.col("__type_issues")),
    ).drop("__type_issues")
    df = df.withColumn(SILVER_HAS_ISSUES, F.size(F.col(SILVER_ISSUES)) > 0)
    df = df.withColumn(SILVER_PROCESSED_AT, F.lit(datetime.now()).cast("timestamp"))

    ordered = (
        silver_names
        + [SILVER_IS_DUPLICATE, SILVER_DUPLICATE_COUNT, SILVER_HAS_ISSUES, SILVER_ISSUES, SILVER_ROW_HASH]
        + ([CORRUPT_COL] if has_corrupt else [])
        + [META_SOURCE_FILE, META_SOURCE_FORMAT, META_RECORD_ID, META_BATCH_ID, META_INGESTION_TS, SILVER_PROCESSED_AT]
    )
    df = df.select(*[F.col(f"`{c}`") for c in ordered])

    path = silver_path(settings, dataset)
    delta_io.overwrite_table(df, path)
    clean.unpersist()

    # --- 6) métricas lidas da tabela gravada (barato e consistente) -------------
    written = delta_io.read_delta(spark, path)
    summary_aggs = [
        F.count(F.when(F.col(SILVER_IS_DUPLICATE), 1)).alias("dups"),
        F.count(F.when(F.array_contains(F.col(SILVER_ISSUES), "linha_vazia"), 1)).alias("empty"),
        F.count(F.when(F.col(SILVER_HAS_ISSUES), 1)).alias("issues"),
        F.count(F.when(F.col(CORRUPT_COL).isNotNull(), 1)).alias("corrupt") if has_corrupt else F.lit(0).alias("corrupt"),
    ]
    fail_counts = {}
    for i, dec in enumerate(decisions):
        if dec.spark_type != "string" and dec.parse_rate is not None and dec.parse_rate < 1.0:
            fail_counts[i] = int(round((1.0 - dec.parse_rate) * dec.non_null))
    srow = written.agg(*summary_aggs).collect()[0]

    result = SilverResult(
        dataset=dataset,
        row_count=row_count,
        duplicate_rows=int(srow["dups"]),
        empty_rows=int(srow["empty"]),
        corrupt_rows=int(srow["corrupt"]),
        rows_with_issues=int(srow["issues"]),
    )
    for i, dec in enumerate(decisions):
        result.columns.append(
            ColumnInfo(
                position=i + 1,
                column=dec.column,
                original_column=original_names[i],
                silver_type=dec.spark_type,
                inference_rule=dec.rule,
                inference_label=dec.label,
                parse_rate=dec.parse_rate,
                non_null_raw=int(stats[f"raw_nn__{i}"]),
                trimmed_count=int(stats[f"trim__{i}"]),
                nullified_count=int(stats[f"nullified__{i}"]),
                failed_conversions=fail_counts.get(i, 0),
            )
        )
    return result
