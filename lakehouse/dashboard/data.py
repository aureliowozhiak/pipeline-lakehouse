"""Leitura das tabelas Delta para o dashboard usando o pacote `deltalake` (delta-rs).

Não precisa de Spark nem de Java: é rápido para abrir o dashboard. As funções
recebem a versão da tabela Delta para que o cache do Streamlit seja invalidado
automaticamente quando o pipeline grava uma nova versão.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable

from lakehouse.config.settings import Settings
from lakehouse.constants import CONTROL_FILE_REGISTRY, CONTROL_PIPELINE_RUNS, GOLD_TABLES

LAYERS = ("bronze", "silver", "gold")

# inteiros com nulos continuam inteiros (em vez de virar float com NaN)
_NULLABLE_INTS = {
    pa.int8(): pd.Int8Dtype(), pa.int16(): pd.Int16Dtype(),
    pa.int32(): pd.Int32Dtype(), pa.int64(): pd.Int64Dtype(),
}


def is_delta(path: Path) -> bool:
    return (Path(path) / "_delta_log").is_dir()


def table_version(path: Path) -> int | None:
    """Versão atual da tabela (ou None se não existe). Usado como chave de cache."""
    if not is_delta(path):
        return None
    try:
        return DeltaTable(str(path)).version()
    except Exception:  # noqa: BLE001 - tabela sendo escrita / corrompida
        return None


def _local_naive(df: pd.DataFrame) -> pd.DataFrame:
    """Converte timestamps UTC (como o Delta guarda) para o fuso local, sem tz."""
    local_tz = datetime.now().astimezone().tzinfo
    for col in df.columns:
        if isinstance(df[col].dtype, pd.DatetimeTZDtype):
            df[col] = df[col].dt.tz_convert(local_tz).dt.tz_localize(None)
    return df


def _lists_to_text(df: pd.DataFrame, table: pa.Table) -> pd.DataFrame:
    for field in table.schema:
        if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
            df[field.name] = df[field.name].map(
                lambda v: "; ".join(str(x) for x in v) if v is not None and len(v) else ""
            )
    return df


def read_table(path: Path, columns: list[str] | None = None, limit: int | None = None) -> pd.DataFrame:
    if not is_delta(path):
        return pd.DataFrame()
    dataset = DeltaTable(str(path)).to_pyarrow_dataset()
    if columns:
        columns = [c for c in columns if c in dataset.schema.names]
    table = dataset.head(limit, columns=columns) if limit else dataset.to_table(columns=columns)
    df = table.to_pandas(types_mapper=_NULLABLE_INTS.get)
    return _lists_to_text(_local_naive(df), table)


def count_rows(path: Path) -> int:
    if not is_delta(path):
        return 0
    return DeltaTable(str(path)).to_pyarrow_dataset().count_rows()


def table_schema(path: Path) -> list[tuple[str, str]]:
    if not is_delta(path):
        return []
    schema = DeltaTable(str(path)).to_pyarrow_dataset().schema
    return [(f.name, str(f.type)) for f in schema]


def list_datasets(settings: Settings, layer: str) -> list[str]:
    base = {"bronze": settings.bronze_dir, "silver": settings.silver_dir, "gold": settings.gold_dir}[layer]
    if not base.exists():
        return []
    return sorted(p.name for p in base.iterdir() if p.is_dir() and is_delta(p))


def gold_path(settings: Settings, table: str) -> Path:
    return settings.gold_dir / table


def control_path(settings: Settings, table: str) -> Path:
    return settings.control_dir / table


def layer_path(settings: Settings, layer: str, name: str) -> Path:
    return {"bronze": settings.bronze_dir, "silver": settings.silver_dir, "gold": settings.gold_dir}[layer] / name


def lakehouse_signature(settings: Settings) -> tuple:
    """Versões de todas as tabelas de controle e Gold: muda a cada execução do pipeline."""
    paths = [control_path(settings, CONTROL_FILE_REGISTRY), control_path(settings, CONTROL_PIPELINE_RUNS)]
    paths += [gold_path(settings, t) for t in GOLD_TABLES]
    return tuple(table_version(p) for p in paths)


def load_gold(settings: Settings) -> dict[str, pd.DataFrame]:
    return {t: read_table(gold_path(settings, t)) for t in GOLD_TABLES}


def load_registry(settings: Settings) -> pd.DataFrame:
    df = read_table(control_path(settings, CONTROL_FILE_REGISTRY))
    return df.sort_values("file_key") if not df.empty else df


def load_runs(settings: Settings) -> pd.DataFrame:
    df = read_table(control_path(settings, CONTROL_PIPELINE_RUNS))
    return df.sort_values("started_at", ascending=False) if not df.empty else df
