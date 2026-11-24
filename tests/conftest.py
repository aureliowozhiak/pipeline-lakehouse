"""Fixtures compartilhadas. Os testes com Spark usam um lakehouse temporário."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from lakehouse.config.settings import load_settings  # noqa: E402
from lakehouse.utils.logging_utils import setup_logging  # noqa: E402


def write_settings(base: Path, files: dict | None = None) -> Path:
    cfg = {
        "paths": {
            "input_dir": str(base / "input"),
            "lakehouse_dir": str(base / "lakehouse"),
            "log_dir": str(base / "logs"),
        },
        "spark": {"master": "local[2]", "shuffle_partitions": 2},
        "files": files or {},
    }
    path = base / "settings.yaml"
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    return path


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    base = tmp_path_factory.mktemp("spark_session")
    settings = load_settings(write_settings(base))
    setup_logging(None)
    try:
        from lakehouse.utils.spark import get_spark

        session = get_spark(settings, app_name="pipeline-lakehouse-tests")
    except RuntimeError as exc:  # sem Java compatível, por exemplo
        pytest.skip(f"Spark indisponível: {exc}")
    yield session
    session.stop()


def make_input_files(input_dir: Path) -> None:
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "produtos.csv").write_text(
        "Código;Nome do Produto;Preço;Data Cadastro;Ativo\n"
        "001;  Caneta ;1,50;01/02/2024;sim\n"
        "002;Lápis;0,99;15/03/2024;não\n"
        "003;Borracha;NULL;20/04/2024;sim\n"
        "002;Lápis;0,99;15/03/2024;não\n",
        encoding="utf-8",
    )
    (input_dir / "eventos.json").write_text(
        "\n".join(json.dumps(r) for r in [
            {"id": 1, "tipo": "click", "valor": 10.5, "meta": {"origem": "web"}},
            {"id": 2, "tipo": "view", "valor": 3, "meta": {"origem": "app"}},
            {"id": 3, "tipo": "click", "valor": None, "meta": {"origem": "web"}},
        ]) + "\n",
        encoding="utf-8",
    )
    (input_dir / "notas.txt").write_text("primeira linha\nsegunda linha, com vírgula\n\nprimeira linha\n",
                                         encoding="utf-8")
    (input_dir / "ruim.json").write_text('{"a": 1, "b": [1, 2', encoding="utf-8")


@pytest.fixture(scope="session")
def built_lakehouse(spark, tmp_path_factory):
    """Executa o pipeline uma vez sobre arquivos pequenos e devolve (settings, summary)."""
    from lakehouse.pipeline import run_pipeline

    base = tmp_path_factory.mktemp("lakehouse_e2e")
    make_input_files(base / "input")
    settings = load_settings(write_settings(base))
    summary = run_pipeline(settings, spark=spark)
    yield settings, summary
    shutil.rmtree(base, ignore_errors=True)
