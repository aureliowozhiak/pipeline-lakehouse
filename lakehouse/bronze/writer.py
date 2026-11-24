"""Camada Bronze: uma tabela Delta por dataset em data/lakehouse/bronze/<dataset>.

Cada linha carrega os metadados de origem (_source_file, _source_hash, ...).
Gravar um arquivo substitui, de forma atômica (replaceWhere), apenas as linhas
que vieram daquele mesmo arquivo — reprocessar não duplica dados.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config.settings import Settings
from lakehouse.constants import META_SOURCE_FILE
from lakehouse.utils import delta_io


def bronze_path(settings: Settings, dataset: str) -> Path:
    return settings.bronze_dir / dataset


def _sql_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def write_file_to_bronze(spark: SparkSession, settings: Settings, dataset: str, df: DataFrame, file_key: str) -> None:
    path = bronze_path(settings, dataset)
    if not delta_io.is_delta_table(path):
        df.write.format("delta").mode("overwrite").save(delta_io.spark_path(path))
        return
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{META_SOURCE_FILE} = {_sql_literal(file_key)}")
        .option("mergeSchema", "true")
        .save(delta_io.spark_path(path))
    )


def remove_file_from_bronze(spark: SparkSession, settings: Settings, dataset: str, file_key: str) -> None:
    delta_io.delete_rows(spark, bronze_path(settings, dataset), META_SOURCE_FILE, [file_key])


def bronze_row_count(spark: SparkSession, settings: Settings, dataset: str) -> int:
    path = bronze_path(settings, dataset)
    if not delta_io.is_delta_table(path):
        return 0
    return delta_io.read_delta(spark, path).select(F.lit(1)).count()
