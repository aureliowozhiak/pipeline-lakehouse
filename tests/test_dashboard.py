"""Executa o dashboard com o AppTest do Streamlit e navega por todas as páginas."""

import pytest
from streamlit.testing.v1 import AppTest

from tests.conftest import ROOT

pytestmark = pytest.mark.spark

PAGES = ["Visão geral", "Explorar dataset", "Qualidade", "Arquivos processados", "Camadas (Bronze/Silver/Gold)"]


def test_dashboard_sem_excecoes(built_lakehouse, monkeypatch):
    settings, _ = built_lakehouse
    monkeypatch.setenv("LAKEHOUSE_CONFIG", str(settings.config_path))
    at = AppTest.from_file(str(ROOT / "dashboard.py"), default_timeout=120)
    at.run()
    assert not at.exception
    for page in PAGES:
        at.sidebar.radio[0].set_value(page).run()
        assert not at.exception, f"exceção na página {page}: {at.exception}"
    # camadas: percorre Bronze, Silver e Gold
    for layer in ("bronze", "silver", "gold"):
        at.main.radio[0].set_value(layer).run()
        assert not at.exception, f"exceção na camada {layer}"
    assert any("Prévia" in h.value for h in at.header)
