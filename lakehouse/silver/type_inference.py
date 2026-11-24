"""Inferência de tipos conservadora para colunas de texto.

Para cada coluna, mede-se (em uma única agregação) quantos valores não nulos
podem ser convertidos por cada regra candidata. Uma coluna só muda de tipo se a
melhor regra converter pelo menos `threshold` dos valores (padrão 95%). Em caso
de empate vence a regra que aparece primeiro em CANDIDATES (ex.: inteiro antes
de decimal). Valores que não convertem ficam nulos na coluna tipada e são
registrados em _quality_issues (o valor original continua na Bronze).

Proteções:
* números com zero à esquerda ("00123") NÃO viram número (são códigos);
* inteiros com mais de 18 dígitos continuam texto;
* datas/horas precisam casar com um formato inteiro (sem texto sobrando).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import Column
from pyspark.sql import functions as F

INT_RE = r"^[+-]?(0|[1-9][0-9]{0,17})$"
DEC_DOT_RE = r"^[+-]?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$"
DEC_COMMA_RE = r"^[+-]?(0|[1-9][0-9]{0,2}(\.[0-9]{3})+|[1-9][0-9]*)(,[0-9]+)?$"
ISO_TS_RE = r"^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}[T ][0-9]{1,2}:[0-9]{2}"


@dataclass(frozen=True)
class Candidate:
    name: str  # identificador da regra
    label: str  # descrição em português (vai para a Gold / dashboard)
    spark_type: str
    parse: Callable[[Column], Column]


def _bool_parser(true_tokens: list[str], false_tokens: list[str]) -> Callable[[Column], Column]:
    t = [s.lower() for s in true_tokens]
    f = [s.lower() for s in false_tokens]

    def parse(v: Column) -> Column:
        low = F.lower(v)
        return F.when(low.isin(t), F.lit(True)).when(low.isin(f), F.lit(False))

    return parse


def _regex_cast(regex: str, spark_type: str, prep: Callable[[Column], Column] | None = None):
    def parse(v: Column) -> Column:
        value = prep(v) if prep else v
        return F.when(v.rlike(regex), value.cast(spark_type))

    return parse


def _comma_decimal(v: Column) -> Column:
    return F.regexp_replace(F.regexp_replace(v, r"\.", ""), ",", ".")


def _date_fmt(fmt: str):
    return lambda v: F.to_date(v, fmt)


def _ts_fmt(fmt: str):
    return lambda v: F.to_timestamp(v, fmt)


def _iso_ts(v: Column) -> Column:
    return F.when(v.rlike(ISO_TS_RE), v.cast("timestamp"))


def build_candidates(true_tokens: list[str], false_tokens: list[str]) -> list[Candidate]:
    return [
        Candidate("boolean", "booleano", "boolean", _bool_parser(true_tokens, false_tokens)),
        Candidate("integer", "inteiro", "bigint", _regex_cast(INT_RE, "bigint")),
        Candidate("decimal_dot", "decimal (ponto como separador)", "double", _regex_cast(DEC_DOT_RE, "double")),
        Candidate("decimal_comma", "decimal (vírgula, padrão BR)", "double",
                  _regex_cast(DEC_COMMA_RE, "double", _comma_decimal)),
        Candidate("date_iso", "data yyyy-MM-dd", "date", _date_fmt("yyyy-M-d")),
        Candidate("date_br", "data dd/MM/yyyy", "date", _date_fmt("d/M/yyyy")),
        Candidate("date_br_dash", "data dd-MM-yyyy", "date", _date_fmt("d-M-yyyy")),
        Candidate("date_slash_iso", "data yyyy/MM/dd", "date", _date_fmt("yyyy/M/d")),
        Candidate("date_dot", "data dd.MM.yyyy", "date", _date_fmt("d.M.yyyy")),
        Candidate("date_us", "data MM/dd/yyyy", "date", _date_fmt("M/d/yyyy")),
        Candidate("timestamp_iso", "data/hora ISO 8601", "timestamp", _iso_ts),
        Candidate("timestamp_br", "data/hora dd/MM/yyyy HH:mm:ss", "timestamp", _ts_fmt("d/M/yyyy H:m:s")),
        Candidate("timestamp_br_min", "data/hora dd/MM/yyyy HH:mm", "timestamp", _ts_fmt("d/M/yyyy H:m")),
    ]


@dataclass
class TypeDecision:
    column: str
    spark_type: str  # "string" quando nenhuma regra é segura
    rule: str
    label: str
    parse_rate: float | None  # fração convertida pela regra escolhida
    non_null: int


def inference_aggregations(columns: list[str], candidates: list[Candidate]) -> list[Column]:
    exprs = []
    for i, col in enumerate(columns):
        v = F.col(f"`{col}`")
        exprs.append(F.count(v).alias(f"nn__{i}"))
        for cand in candidates:
            exprs.append(F.count(cand.parse(v)).alias(f"ok__{i}__{cand.name}"))
    return exprs


def decide_types(columns: list[str], candidates: list[Candidate], agg_row: dict, threshold: float) -> list[TypeDecision]:
    decisions = []
    for i, col in enumerate(columns):
        nn = int(agg_row.get(f"nn__{i}") or 0)
        best: tuple[float, int, Candidate] | None = None
        if nn > 0:
            for order, cand in enumerate(candidates):
                rate = int(agg_row.get(f"ok__{i}__{cand.name}") or 0) / nn
                if rate >= threshold and (best is None or rate > best[0]):
                    best = (rate, order, cand)
        if best is None:
            decisions.append(TypeDecision(col, "string", "texto", "texto (sem conversão segura)", None, nn))
        else:
            rate, _, cand = best
            decisions.append(TypeDecision(col, cand.spark_type, cand.name, cand.label, rate, nn))
    return decisions


def parser_for(rule: str, candidates: list[Candidate]) -> Callable[[Column], Column] | None:
    for cand in candidates:
        if cand.name == rule:
            return cand.parse
    return None
