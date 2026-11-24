import pytest

from lakehouse.config.settings import load_settings


def test_padroes_e_ajustes_por_arquivo():
    s = load_settings(overrides={"files": {
        "vendas_*.txt": {"mode": "delimited", "delimiter": "pipe", "dataset": "Vendas"},
        "vendas_2024.txt": {"header": False},
    }})
    opts = s.read_options("vendas_2024.txt", "vendas_2024.txt", "txt")
    assert opts["mode"] == "delimited"
    assert opts["delimiter"] == "|"
    assert opts["header"] is False
    assert opts["dataset"] == "Vendas"
    outro = s.read_options("outro.txt", "outro.txt", "txt")
    assert outro["mode"] == "auto" and outro["delimiter"] == "auto" and outro["header"] is True


def test_aliases_de_delimitador_e_json():
    s = load_settings(overrides={"files": {"a.csv": {"delimiter": "\\t"}, "b.json": {"multiline": "false"}}})
    assert s.read_options("a.csv", "a.csv", "csv")["delimiter"] == "\t"
    assert s.read_options("b.json", "b.json", "json")["multiline"] is False
    assert s.read_options("c.json", "c.json", "json")["multiline"] == "auto"


def test_opcao_desconhecida_gera_erro():
    s = load_settings(overrides={"files": {"a.csv": {"separador": ";"}}})
    with pytest.raises(ValueError):
        s.read_options("a.csv", "a.csv", "csv")
