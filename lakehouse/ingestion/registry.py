"""Registro de arquivos processados e histórico de execuções (tabelas Delta em _control/).

Chave do registro: caminho relativo do arquivo em data/input. O hash SHA-256 do
conteúdo decide se o arquivo mudou. Regras:

* novo (não está no registro)          -> processa
* hash diferente (arquivo alterado)    -> processa e SUBSTITUI os dados anteriores dele
* status FALHA na última tentativa     -> tenta de novo
* --reprocess / --reprocess-all        -> processa mesmo sem mudança
* caso contrário                       -> ignora (evita ingestão duplicada)
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from lakehouse.config.settings import Settings
from lakehouse.constants import CONTROL_FILE_REGISTRY, CONTROL_PIPELINE_RUNS, STATUS_FAILED, STATUS_SUCCESS
from lakehouse.utils import delta_io

REGISTRY_SCHEMA = T.StructType(
    [
        T.StructField("file_key", T.StringType(), False),
        T.StructField("file_name", T.StringType(), True),
        T.StructField("file_path", T.StringType(), True),
        T.StructField("file_format", T.StringType(), True),
        T.StructField("file_size_bytes", T.LongType(), True),
        T.StructField("file_hash", T.StringType(), True),
        T.StructField("dataset", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("action", T.StringType(), True),
        T.StructField("row_count", T.LongType(), True),
        T.StructField("column_count", T.IntegerType(), True),
        T.StructField("corrupt_records", T.LongType(), True),
        T.StructField("read_options", T.StringType(), True),
        T.StructField("warnings", T.StringType(), True),
        T.StructField("error_message", T.StringType(), True),
        T.StructField("batch_id", T.StringType(), True),
        T.StructField("processed_at", T.TimestampType(), True),
    ]
)

RUNS_SCHEMA = T.StructType(
    [
        T.StructField("batch_id", T.StringType(), False),
        T.StructField("started_at", T.TimestampType(), True),
        T.StructField("finished_at", T.TimestampType(), True),
        T.StructField("duration_seconds", T.DoubleType(), True),
        T.StructField("run_mode", T.StringType(), True),
        T.StructField("files_found", T.IntegerType(), True),
        T.StructField("files_processed", T.IntegerType(), True),
        T.StructField("files_skipped", T.IntegerType(), True),
        T.StructField("files_failed", T.IntegerType(), True),
        T.StructField("datasets_updated", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("message", T.StringType(), True),
    ]
)


def registry_path(settings: Settings):
    return settings.control_dir / CONTROL_FILE_REGISTRY


def runs_path(settings: Settings):
    return settings.control_dir / CONTROL_PIPELINE_RUNS


def load_registry(spark: SparkSession, settings: Settings) -> dict[str, dict[str, Any]]:
    path = registry_path(settings)
    if not delta_io.is_delta_table(path):
        return {}
    return {row["file_key"]: row.asDict() for row in delta_io.read_delta(spark, path).collect()}


def decide_action(entry: dict | None, file_hash: str, force: bool) -> str | None:
    """Retorna o motivo para processar o arquivo, ou None para ignorá-lo."""
    if entry is None:
        return "novo"
    if entry.get("file_hash") != file_hash:
        return "alterado"
    if force:
        return "reprocessamento"
    if entry.get("status") != STATUS_SUCCESS:
        return "nova tentativa"
    return None


def save_registry_entries(spark: SparkSession, settings: Settings, entries: list[dict[str, Any]]) -> None:
    if not entries:
        return
    df = spark.createDataFrame([tuple(e.get(f.name) for f in REGISTRY_SCHEMA.fields) for e in entries],
                               REGISTRY_SCHEMA).coalesce(1)
    delta_io.upsert(spark, df, registry_path(settings), "file_key")


def append_run(spark: SparkSession, settings: Settings, run: dict[str, Any]) -> None:
    df = spark.createDataFrame([tuple(run.get(f.name) for f in RUNS_SCHEMA.fields)], RUNS_SCHEMA).coalesce(1)
    delta_io.append(df, runs_path(settings))


__all__ = [
    "STATUS_FAILED",
    "STATUS_SUCCESS",
    "append_run",
    "decide_action",
    "load_registry",
    "save_registry_entries",
]
