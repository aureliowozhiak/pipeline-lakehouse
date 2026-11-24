"""Teste ponta a ponta com Spark: ingestão -> Bronze -> Silver -> Gold + controle."""

import pytest

from lakehouse.constants import GOLD_TABLES
from lakehouse.dashboard import data as dd
from lakehouse.pipeline import run_pipeline

pytestmark = pytest.mark.spark


def test_primeira_execucao(built_lakehouse):
    settings, summary = built_lakehouse
    processed = {p["file"]: p for p in summary.processed}
    assert set(processed) == {"produtos.csv", "eventos.json", "notas.txt"}
    assert [f["file"] for f in summary.failed] == ["ruim.json"]
    assert processed["produtos.csv"]["rows"] == 4
    assert processed["eventos.json"]["rows"] == 3
    assert processed["notas.txt"]["rows"] == 4

    for layer, name in [("bronze", "produtos"), ("silver", "produtos"), ("silver", "eventos"),
                        ("silver", "notas")]:
        assert (dd.layer_path(settings, layer, name) / "_delta_log").is_dir()
    for table in GOLD_TABLES:
        assert dd.is_delta(dd.gold_path(settings, table))


def test_silver_padroniza_tipos_nulos_e_duplicados(built_lakehouse):
    settings, _ = built_lakehouse
    df = dd.read_table(dd.layer_path(settings, "silver", "produtos")).sort_values("_record_id")
    assert list(df.columns[:5]) == ["codigo", "nome_do_produto", "preco", "data_cadastro", "ativo"]
    assert df["codigo"].tolist() == ["001", "002", "003", "002"]  # zeros à esquerda preservados
    assert df["nome_do_produto"].iloc[0] == "Caneta"  # espaços removidos
    assert df["preco"].iloc[0] == pytest.approx(1.5)
    assert df["preco"].isna().iloc[2]  # "NULL" -> nulo
    assert df["ativo"].tolist() == [True, False, True, False]
    assert df["_is_duplicate"].tolist() == [False, False, False, True]

    eventos = dd.read_table(dd.layer_path(settings, "silver", "eventos"))
    assert "meta_origem" in eventos.columns  # struct achatado

    notas = dd.read_table(dd.layer_path(settings, "silver", "notas")).sort_values("_record_id")
    assert notas["linha"].isna().iloc[2]  # linha em branco -> nulo
    assert notas["_is_duplicate"].iloc[3]


def test_gold_e_registro(built_lakehouse):
    settings, _ = built_lakehouse
    gold = dd.load_gold(settings)
    summary = gold["datasets_summary"].set_index("dataset")
    assert summary.loc["produtos", "row_count"] == 4
    assert summary.loc["produtos", "duplicate_rows"] == 1
    profile = gold["column_profile"].query("dataset == 'produtos'").set_index("column")
    assert profile.loc["preco", "data_type"] == "double"
    assert profile.loc["preco", "null_count"] == 1
    assert profile.loc["preco", "mean"] == pytest.approx((1.5 + 0.99 + 0.99) / 3)
    hist = gold["numeric_histograms"].query("dataset == 'produtos' and column == 'preco'")
    assert hist["count"].sum() == 3
    freq = gold["value_frequencies"].query("dataset == 'produtos' and column == 'nome_do_produto'")
    assert freq.sort_values("rank").iloc[0]["value"] == "Lápis"

    registry = dd.load_registry(settings).set_index("file_key")
    assert registry.loc["ruim.json", "status"] == "FALHA"
    assert registry.loc["produtos.csv", "status"] == "SUCESSO"


def test_reexecucao_ignora_e_reprocessa_sem_duplicar(built_lakehouse, spark):
    settings, _ = built_lakehouse
    again = run_pipeline(settings, spark=spark)
    assert set(again.skipped) == {"produtos.csv", "eventos.json", "notas.txt"}
    assert [f["file"] for f in again.failed] == ["ruim.json"]  # falhas são tentadas de novo

    redo = run_pipeline(settings, spark=spark, reprocess=["produtos.csv"])
    assert [p["file"] for p in redo.processed] == ["produtos.csv"]
    assert dd.count_rows(dd.layer_path(settings, "bronze", "produtos")) == 4

    # arquivo alterado: os dados dele são substituídos
    path = settings.input_dir / "produtos.csv"
    path.write_text(path.read_text(encoding="utf-8") + "004;Régua;2,00;01/05/2024;sim\n", encoding="utf-8")
    changed = run_pipeline(settings, spark=spark)
    assert [(p["file"], p["action"]) for p in changed.processed] == [("produtos.csv", "alterado")]
    assert dd.count_rows(dd.layer_path(settings, "bronze", "produtos")) == 5
    assert dd.count_rows(dd.layer_path(settings, "silver", "produtos")) == 5
    gold = dd.load_gold(settings)["datasets_summary"].set_index("dataset")
    assert gold.loc["produtos", "row_count"] == 5
    assert gold.loc["eventos", "row_count"] == 3  # outros datasets intactos
