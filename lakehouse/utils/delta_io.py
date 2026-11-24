"""Operações Delta Lake usadas pelas camadas (sobrescrever, substituir linhas, upsert)."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def spark_path(path: Path) -> str:
    return Path(path).resolve().as_posix()


def is_delta_table(path: Path) -> bool:
    return (Path(path) / "_delta_log").is_dir()


def read_delta(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.format("delta").load(spark_path(path))


def overwrite_table(df: DataFrame, path: Path) -> None:
    """Substitui o conteúdo e o schema da tabela (gera nova versão Delta)."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(spark_path(path))
    )


def delete_rows(spark: SparkSession, path: Path, column: str, values: Iterable[str]) -> None:
    from delta.tables import DeltaTable

    values = list(values)
    if values and is_delta_table(path):
        DeltaTable.forPath(spark, spark_path(path)).delete(F.col(column).isin(values))


def replace_rows(spark: SparkSession, df: DataFrame, path: Path, column: str, values: Iterable[str]) -> None:
    """Remove as linhas com column IN values e grava df (com evolução de schema).

    É assim que reprocessar um arquivo substitui os dados dele em vez de duplicá-los.
    """
    values = list(values)
    if is_delta_table(path):
        delete_rows(spark, path, column, values)
        df.write.format("delta").mode("append").option("mergeSchema", "true").save(spark_path(path))
    else:
        df.write.format("delta").mode("overwrite").save(spark_path(path))


def upsert(spark: SparkSession, df: DataFrame, path: Path, key: str) -> None:
    from delta.tables import DeltaTable

    if not is_delta_table(path):
        df.write.format("delta").mode("overwrite").save(spark_path(path))
        return
    (
        DeltaTable.forPath(spark, spark_path(path))
        .alias("t")
        .merge(df.alias("s"), f"t.`{key}` = s.`{key}`")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def append(df: DataFrame, path: Path) -> None:
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(spark_path(path))


def drop_table(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)
