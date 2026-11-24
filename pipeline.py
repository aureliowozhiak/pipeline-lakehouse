"""Executa o pipeline: Ingestão -> Bronze -> Silver -> Gold.

Uso:
    python pipeline.py                         # processa arquivos novos ou alterados
    python pipeline.py --reprocess vendas.txt  # força o reprocessamento de um arquivo
    python pipeline.py --reprocess-all         # força todos os arquivos de data/input
    python pipeline.py --full-refresh          # apaga o lakehouse e reconstrói do zero
"""

from __future__ import annotations

import argparse
import sys

from lakehouse.config.settings import load_settings
from lakehouse.pipeline import run_pipeline
from lakehouse.utils.logging_utils import setup_logging


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pipeline lakehouse local (Spark + Delta Lake).")
    parser.add_argument("--reprocess", action="append", nargs="+", metavar="ARQUIVO", default=[],
                        help="reprocessa o(s) arquivo(s) informado(s) (nome ou caminho relativo a data/input)")
    parser.add_argument("--reprocess-all", action="store_true", help="reprocessa todos os arquivos de entrada")
    parser.add_argument("--full-refresh", action="store_true",
                        help="apaga Bronze/Silver/Gold/controle e processa tudo novamente")
    parser.add_argument("--config", default=None, help="caminho alternativo para o settings.yaml")
    args = parser.parse_args(argv)

    try:
        settings = load_settings(args.config)
    except Exception as exc:  # noqa: BLE001
        print(f"Erro na configuração: {exc}", file=sys.stderr)
        return 2
    log_file = setup_logging(settings.log_dir)

    reprocess = [name for group in args.reprocess for name in group]
    spark = None
    try:
        from lakehouse.utils.spark import get_spark

        spark = get_spark(settings)
        summary = run_pipeline(settings, spark=spark, reprocess=reprocess,
                               reprocess_all=args.reprocess_all, full_refresh=args.full_refresh)
    except RuntimeError as exc:
        print(f"\nERRO: {exc}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()
    print(f"\nLog completo: {log_file}")
    print("Para ver os resultados: streamlit run dashboard.py")
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())
