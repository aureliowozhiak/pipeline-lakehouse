"""Leitura dos arquivos de entrada com Spark (CSV, JSON e TXT delimitado/plano).

Princípio da Bronze: guardar o dado como veio. Por isso CSV/TXT são lidos com
todas as colunas como texto (a inferência de tipos acontece na Silver, de forma
controlada) e o JSON é lido com primitivesAsString=true (mantém a estrutura
aninhada, mas os valores ficam como texto).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse.constants import (
    BRONZE_METADATA,
    CORRUPT_COL,
    META_BATCH_ID,
    META_INGESTION_TS,
    META_RECORD_ID,
    META_SOURCE_FILE,
    META_SOURCE_FORMAT,
    META_SOURCE_HASH,
    META_SOURCE_PATH,
    META_SOURCE_SIZE,
    PLAIN_TEXT_COLUMN,
)
from lakehouse.ingestion import detection as det
from lakehouse.utils.delta_io import spark_path
from lakehouse.utils.files import InputFile
from lakehouse.utils.naming import bronze_column_names


class IngestionError(Exception):
    """Erro de leitura que deve ser registrado como FALHA (o pipeline continua)."""


@dataclass
class ReadResult:
    df: DataFrame
    row_count: int
    column_count: int
    corrupt_records: int
    effective_options: dict[str, Any]
    warnings: list[str] = field(default_factory=list)


def _describe_delim(d: str | None) -> str | None:
    return {"\t": "\\t", " ": "espaço"}.get(d, d) if d else d


def _transcode_to_utf8(f: InputFile, encoding: det.EncodingInfo, tmp_dir: Path, file_hash: str) -> Path:
    """Gera uma cópia UTF-8 (sem BOM) para leitores do Spark que não aceitam outra codificação."""
    tmp_dir.mkdir(parents=True, exist_ok=True)
    target = tmp_dir / f"{file_hash[:16]}{f.path.suffix.lower()}"
    # utf-8-sig e utf-16 já removem o BOM na decodificação
    with open(f.path, "r", encoding=encoding.python, errors="replace", newline="") as src, open(
        target, "w", encoding="utf-8", newline=""
    ) as dst:
        for chunk in iter(lambda: src.read(1024 * 1024), ""):
            dst.write(chunk)
    return target


def _read_delimited(spark: SparkSession, f: InputFile, opts: dict, text: str, encoding: det.EncodingInfo,
                    delimiter: str) -> tuple[DataFrame, dict]:
    quote = opts.get("quote") or '"'
    escape = opts.get("escape") or quote
    header = bool(opts.get("header", True))
    if header:
        names = det.first_record(text, delimiter, quote)
        if not names:
            raise IngestionError("Não foi possível ler o cabeçalho do arquivo.")
    else:
        names = [f"coluna_{i}" for i in range(1, det.max_columns(text, delimiter, quote) + 1)]
    columns = bronze_column_names(names, reserved=BRONZE_METADATA + [CORRUPT_COL])
    schema = T.StructType(
        [T.StructField(c, T.StringType(), True) for c in columns]
        + [T.StructField(CORRUPT_COL, T.StringType(), True)]
    )
    reader = (
        spark.read.format("csv")
        .schema(schema)
        .option("header", str(header).lower())
        .option("sep", delimiter)
        .option("encoding", encoding.spark)
        .option("quote", quote)
        .option("escape", escape)
        .option("multiLine", str(bool(opts.get("multiline", True))).lower())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .option("ignoreLeadingWhiteSpace", "false")
        .option("ignoreTrailingWhiteSpace", "false")
        .option("enforceSchema", "true")
    )
    df = reader.load(spark_path(f.path))
    effective = {
        "delimiter": _describe_delim(delimiter),
        "header": header,
        "encoding": encoding.spark + (" (BOM)" if encoding.has_bom else ""),
        "quote": quote,
        "multiline": bool(opts.get("multiline", True)),
        "original_columns": names,
    }
    return df, effective


def _read_plain(spark: SparkSession, f: InputFile, encoding: det.EncodingInfo, tmp_dir: Path,
                file_hash: str) -> tuple[DataFrame, dict]:
    source = f.path if encoding.is_plain_utf8 else _transcode_to_utf8(f, encoding, tmp_dir, file_hash)
    df = spark.read.text(spark_path(source)).withColumnRenamed("value", PLAIN_TEXT_COLUMN)
    return df, {"encoding": encoding.spark + (" (BOM)" if encoding.has_bom else "")}


def _sanitize_type(dt: T.DataType) -> T.DataType:
    """Renomeia campos aninhados de structs para nomes aceitos pelo Delta."""
    if isinstance(dt, T.StructType):
        names = bronze_column_names([fld.name for fld in dt.fields])
        return T.StructType(
            [T.StructField(n, _sanitize_type(fld.dataType), True) for n, fld in zip(names, dt.fields)]
        )
    if isinstance(dt, T.ArrayType):
        return T.ArrayType(_sanitize_type(dt.elementType), True)
    if isinstance(dt, T.MapType):
        return T.MapType(dt.keyType, _sanitize_type(dt.valueType), True)
    return dt


def _read_json(spark: SparkSession, f: InputFile, opts: dict, text: str, encoding: det.EncodingInfo,
               tmp_dir: Path, file_hash: str) -> tuple[DataFrame, dict]:
    multiline = opts.get("multiline", "auto")
    detected = multiline == "auto"
    if detected:
        multiline = det.detect_json_multiline(text)
    source = f.path if encoding.is_plain_utf8 else _transcode_to_utf8(f, encoding, tmp_dir, file_hash)
    df = (
        spark.read.format("json")
        .option("multiLine", str(bool(multiline)).lower())
        .option("primitivesAsString", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .load(spark_path(source))
    )
    df = df.cache()  # necessário para consultar _corrupt_record isoladamente
    data_cols = [c for c in df.columns if c != CORRUPT_COL]
    if not data_cols:
        raise IngestionError(
            "JSON inválido ou sem objetos: nenhum registro pôde ser interpretado"
            + (" (formato detectado: " + ("array/multilinha" if multiline else "JSON Lines") + ")")
        )
    safe = bronze_column_names(data_cols, reserved=BRONZE_METADATA + [CORRUPT_COL])
    renamed = df.toDF(*[safe[data_cols.index(c)] if c in data_cols else c for c in df.columns])
    renamed = renamed.select(
        *[
            F.col(f"`{c}`").cast(_sanitize_type(renamed.schema[c].dataType)).alias(c)
            if isinstance(renamed.schema[c].dataType, (T.StructType, T.ArrayType, T.MapType))
            else F.col(f"`{c}`")
            for c in renamed.columns
        ]
    )
    effective = {
        "multiline": bool(multiline),
        "json_format": "array/multilinha" if multiline else "JSON Lines",
        "detected": detected,
        "encoding": encoding.spark + (" (BOM)" if encoding.has_bom else ""),
        "original_columns": data_cols,
    }
    return renamed, effective


def read_input_file(spark: SparkSession, f: InputFile, opts: dict, *, file_hash: str, batch_id: str,
                    ingestion_ts: datetime, tmp_dir: Path) -> ReadResult:
    if f.size == 0:
        raise IngestionError("Arquivo vazio (0 bytes).")
    sample, truncated = det.read_sample(f.path)
    try:
        encoding = det.detect_encoding(sample, str(opts.get("encoding", "auto")))
    except LookupError as exc:
        raise IngestionError(f"Codificação desconhecida: {opts.get('encoding')!r}") from exc
    text = det.decode_sample(sample, encoding, truncated)
    if not text.strip():
        raise IngestionError("Arquivo sem conteúdo (apenas espaços/linhas em branco).")

    warnings: list[str] = []
    if f.fmt == "json":
        df, effective = _read_json(spark, f, opts, text, encoding, tmp_dir, file_hash)
        effective["format"] = "json"
    elif f.fmt == "csv":
        delimiter = opts.get("delimiter", "auto")
        if delimiter == "auto":
            guess = det.detect_delimiter(text, opts.get("quote") or '"')
            delimiter = guess.delimiter or ","
            if guess.delimiter is None:
                warnings.append("Delimitador não detectado; usando ','. Configure em settings.yaml se necessário.")
        df, effective = _read_delimited(spark, f, opts, text, encoding, delimiter)
        effective["format"] = "csv"
        effective["delimiter_detected"] = opts.get("delimiter", "auto") == "auto"
    else:  # txt
        mode = opts.get("mode", "auto")
        delimiter = opts.get("delimiter", "auto")
        detected_mode = mode == "auto"
        if mode == "auto":
            mode, guess = det.detect_txt_mode(text, opts.get("quote") or '"', delimiter)
            if mode == "delimited" and delimiter == "auto":
                delimiter = guess.delimiter
        elif mode == "delimited" and delimiter == "auto":
            guess = det.detect_delimiter(text, opts.get("quote") or '"')
            if guess.delimiter is None:
                raise IngestionError("TXT configurado como 'delimited', mas o delimitador não foi detectado. "
                                     "Informe 'delimiter' em settings.yaml.")
            delimiter = guess.delimiter
        if mode == "delimited":
            df, effective = _read_delimited(spark, f, opts, text, encoding, delimiter)
        else:
            df, effective = _read_plain(spark, f, encoding, tmp_dir, file_hash)
        effective["format"] = "txt"
        effective["txt_mode"] = mode
        effective["txt_mode_detected"] = detected_mode

    df = df.withColumn(META_RECORD_ID, F.monotonically_increasing_id())
    df = df.cache()
    row_count = df.count()
    corrupt = 0
    if CORRUPT_COL in df.columns:
        corrupt = df.filter(F.col(CORRUPT_COL).isNotNull()).count()
        if corrupt == 0:
            df = df.drop(CORRUPT_COL)
        elif corrupt == row_count:
            df.unpersist()
            raise IngestionError(
                f"Todas as {row_count} linhas estão malformadas (verifique delimitador/formato/aspas). "
                f"Opções usadas: {json.dumps({k: v for k, v in effective.items() if k != 'original_columns'}, ensure_ascii=False)}"
            )
        else:
            warnings.append(f"{corrupt} registro(s) malformado(s) preservado(s) em {CORRUPT_COL}.")
    if row_count == 0:
        warnings.append("Arquivo sem registros de dados (apenas cabeçalho?).")

    df = (
        df.withColumn(META_SOURCE_FILE, F.lit(f.rel_path))
        .withColumn(META_SOURCE_PATH, F.lit(str(f.path.resolve())))
        .withColumn(META_SOURCE_SIZE, F.lit(f.size).cast("long"))
        .withColumn(META_SOURCE_HASH, F.lit(file_hash))
        .withColumn(META_SOURCE_FORMAT, F.lit(effective["format"]))
        .withColumn(META_INGESTION_TS, F.lit(ingestion_ts).cast("timestamp"))
        .withColumn(META_BATCH_ID, F.lit(batch_id))
    )
    data_cols = [c for c in df.columns if c not in BRONZE_METADATA and c != CORRUPT_COL]
    return ReadResult(
        df=df,
        row_count=row_count,
        column_count=len(data_cols),
        corrupt_records=corrupt,
        effective_options=effective,
        warnings=warnings,
    )
