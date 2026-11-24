import pytest

from lakehouse.silver import type_inference as ti

pytestmark = pytest.mark.spark

TRUE = ["true", "sim", "s", "yes"]
FALSE = ["false", "não", "nao", "n", "no"]


def _infer(spark, data: dict[str, list], threshold: float = 0.95) -> dict[str, ti.TypeDecision]:
    cols = list(data)
    n = len(next(iter(data.values())))
    rows = [tuple(data[c][i] for c in cols) for i in range(n)]
    df = spark.createDataFrame(rows, ", ".join(f"`{c}` string" for c in cols))
    candidates = ti.build_candidates(TRUE, FALSE)
    agg = df.agg(*ti.inference_aggregations(cols, candidates)).collect()[0].asDict()
    return {d.column: d for d in ti.decide_types(cols, candidates, agg, threshold)}


def test_tipos_basicos(spark):
    d = _infer(spark, {
        "inteiro": ["1", "-20", "300", None],
        "decimal": ["1.5", "2", "-3.25", "1e3"],
        "decimal_br": ["1.234,56", "10,5", "3", "0,99"],
        "data_br": ["01/02/2024", "15/12/2023", "1/1/2020", None],
        "data_iso": ["2024-01-15", "2023-12-31", "2020-02-29", "2021-07-04"],
        "ts": ["2024-01-15 10:30:00", "2024-01-15T08:00:00", "2023-12-31 23:59:59", None],
        "booleano": ["Sim", "não", "SIM", "nao"],
    })
    assert d["inteiro"].spark_type == "bigint"
    assert d["decimal"].spark_type == "double" and d["decimal"].rule == "decimal_dot"
    assert d["decimal_br"].spark_type == "double" and d["decimal_br"].rule == "decimal_comma"
    assert d["data_br"].spark_type == "date" and d["data_br"].rule == "date_br"
    assert d["data_iso"].spark_type == "date"
    assert d["ts"].spark_type == "timestamp"
    assert d["booleano"].spark_type == "boolean"


def test_conservador_com_codigos_e_misturas(spark):
    d = _infer(spark, {
        "codigo": ["001", "002", "010", "100"],  # zeros à esquerda: continua texto
        "misto": ["1", "2", "abc", "def"],  # 50% numérico: continua texto
        "data_com_hora": ["01/02/2024 10:00", "02/02/2024 11:00", "03/02/2024 12:00", "04/02/2024 13:00"],
        "zero_um": ["0", "1", "1", "0"],  # inteiro, não booleano
        "vazio": [None, None, None, None],
    })
    assert d["codigo"].spark_type == "string"
    assert d["misto"].spark_type == "string"
    assert d["data_com_hora"].spark_type == "timestamp"
    assert d["zero_um"].spark_type == "bigint"
    assert d["vazio"].spark_type == "string"


def test_limiar_de_conversao(spark):
    values = [str(i) for i in range(1, 40)] + ["quarenta"]  # 97,5% inteiros
    assert _infer(spark, {"n": values}, threshold=0.95)["n"].spark_type == "bigint"
    assert _infer(spark, {"n": values}, threshold=1.0)["n"].spark_type == "string"
    rate = _infer(spark, {"n": values})["n"].parse_rate
    assert rate == pytest.approx(39 / 40)
