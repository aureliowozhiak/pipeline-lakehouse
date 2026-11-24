"""Orquestrador do pipeline: Ingestão -> Bronze -> Silver -> Gold.

Leia `run_pipeline` de cima para baixo: cada etapa da arquitetura Medallion é
uma função deste módulo, chamada na ordem em que acontece.

  Etapa 1  Descoberta  - lista os arquivos suportados em data/input
  Etapa 2  Bronze      - lê cada arquivo novo/alterado e grava o dado bruto
  Etapa 3  Silver      - limpa, padroniza e tipa cada dataset alterado
  Etapa 4  Gold        - calcula métricas genéricas para o dashboard
  Etapa 5  Controle    - atualiza o registro de arquivos e o histórico de execuções
"""

from __future__ import annotations

import json
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from lakehouse.bronze.writer import bronze_path, remove_file_from_bronze, write_file_to_bronze
from lakehouse.config.settings import Settings
from lakehouse.constants import GOLD_TABLES, STATUS_FAILED, STATUS_SUCCESS
from lakehouse.gold.metrics import compute_dataset_gold, write_gold
from lakehouse.ingestion import registry as reg
from lakehouse.ingestion.reader import IngestionError, read_input_file
from lakehouse.silver.transform import SilverResult, build_silver, silver_path
from lakehouse.utils import delta_io
from lakehouse.utils.files import InputFile, discover_input_files, file_sha256
from lakehouse.utils.logging_utils import get_logger
from lakehouse.utils.naming import dataset_name_from_file, to_snake_case

log = get_logger("pipeline")


@dataclass
class RunSummary:
    """Resultado de uma execução (usado no resumo final, nos testes e no histórico)."""

    batch_id: str
    files_found: int = 0
    processed: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[dict[str, Any]] = field(default_factory=list)
    datasets_updated: list[str] = field(default_factory=list)
    dataset_errors: dict[str, str] = field(default_factory=dict)
    unmatched_reprocess: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.dataset_errors


def run_pipeline(settings: Settings, spark=None, reprocess: list[str] | None = None,
                 reprocess_all: bool = False, full_refresh: bool = False) -> RunSummary:
    started = datetime.now()
    t0 = time.time()
    summary = RunSummary(batch_id=f"{started:%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:6]}")
    reprocess = reprocess or []
    mode = _run_mode(reprocess, reprocess_all, full_refresh)
    log.info("Execução %s | modo: %s", summary.batch_id, mode)

    if full_refresh:
        _clear_lakehouse(settings)

    _stage(1, "Descoberta de arquivos em data/input")
    files = _discover_files(settings, reprocess, summary)

    if spark is None:
        from lakehouse.utils.spark import get_spark

        spark = get_spark(settings)
    registry = reg.load_registry(spark, settings)

    _stage(2, "Ingestão -> Bronze (dado bruto + metadados de origem)")
    entries, touched = _ingest_to_bronze(spark, settings, files, registry, summary, started,
                                         force=lambda f: reprocess_all or full_refresh
                                         or any(_matches(n, f) for n in reprocess))

    _stage(3, "Silver (limpeza, tipos, duplicatas, qualidade)")
    silver_results, emptied = _build_silver_layer(spark, settings, touched, summary)

    _stage(4, "Gold (métricas genéricas para o dashboard)")
    _build_gold_layer(spark, settings, silver_results, emptied, summary)

    _stage(5, "Controle (registro de arquivos e histórico)")
    _save_control(spark, settings, entries, summary, mode, started, t0)

    shutil.rmtree(_tmp_dir(settings), ignore_errors=True)
    _log_final_summary(summary, time.time() - t0)
    return summary


# --------------------------------------------------------------------------- etapa 1
def _discover_files(settings: Settings, reprocess: list[str], summary: RunSummary) -> list[InputFile]:
    files, ignored = discover_input_files(settings.input_dir)
    summary.files_found = len(files)
    log.info("%s arquivo(s) suportado(s) encontrado(s)", len(files))
    for path in ignored:
        log.warning("Ignorado (extensão não suportada; use .csv, .json, .jsonl ou .txt): %s", path.name)
    if not files:
        log.warning("Nenhum arquivo em %s. Gere exemplos com: python generate_sample_data.py",
                    settings.input_dir)
    for name in reprocess:
        if not any(_matches(name, f) for f in files):
            summary.unmatched_reprocess.append(name)
            log.warning("--reprocess %s: arquivo não encontrado em %s", name, settings.input_dir)
    return files


# --------------------------------------------------------------------------- etapa 2
def _ingest_to_bronze(spark, settings: Settings, files: list[InputFile], registry: dict, summary: RunSummary,
                      started: datetime, force) -> tuple[list[dict[str, Any]], set[str]]:
    """Lê cada arquivo novo/alterado e substitui as linhas dele na Bronze do dataset.

    Arquivos sem alteração (mesmo hash SHA-256) são pulados. Um erro de leitura
    vira FALHA no registro e o pipeline segue para o próximo arquivo.
    Retorna as entradas do registro e os datasets que mudaram.
    """
    entries: list[dict[str, Any]] = []
    touched: set[str] = set()
    for f in files:
        file_hash = file_sha256(f.path)
        entry = registry.get(f.rel_path)
        action = reg.decide_action(entry, file_hash, force(f))
        if action is None:
            summary.skipped.append(f.rel_path)
            log.info("  pulado     %s (já processado, sem alterações)", f.rel_path)
            continue

        record: dict[str, Any] = {
            "file_key": f.rel_path, "file_name": f.name, "file_path": str(f.path.resolve()),
            "file_format": f.fmt, "file_size_bytes": f.size, "file_hash": file_hash,
            "batch_id": summary.batch_id, "processed_at": datetime.now(), "action": action,
        }
        try:
            opts = settings.read_options(f.name, f.rel_path, f.fmt)
            dataset = to_snake_case(opts["dataset"]) if opts.get("dataset") else dataset_name_from_file(f.path.stem)
            record["dataset"] = dataset or "dataset"
            result = read_input_file(spark, f, opts, file_hash=file_hash, batch_id=summary.batch_id,
                                     ingestion_ts=started, tmp_dir=_tmp_dir(settings))
            write_file_to_bronze(spark, settings, record["dataset"], result.df, f.rel_path)

            old_dataset = entry.get("dataset") if entry else None
            if old_dataset and old_dataset != record["dataset"]:
                remove_file_from_bronze(spark, settings, old_dataset, f.rel_path)
                touched.add(old_dataset)
            touched.add(record["dataset"])

            opts_view = {k: v for k, v in result.effective_options.items() if k != "original_columns"}
            record.update(status=STATUS_SUCCESS, row_count=result.row_count, column_count=result.column_count,
                          corrupt_records=result.corrupt_records,
                          read_options=json.dumps(opts_view, ensure_ascii=False, default=str),
                          warnings=" | ".join(result.warnings) or None, error_message=None)
            summary.processed.append({"file": f.rel_path, "dataset": record["dataset"], "rows": result.row_count,
                                      "columns": result.column_count, "action": action})
            log.info("  %-10s %s -> bronze/%s: %s linhas, %s colunas [%s]", action, f.rel_path,
                     record["dataset"], result.row_count, result.column_count, _describe_read(opts_view))
            for warning in result.warnings:
                log.warning("  %s: %s", f.rel_path, warning)
        except Exception as exc:  # noqa: BLE001 - qualquer erro de leitura vira FALHA registrada
            msg = str(exc) if isinstance(exc, IngestionError) else _short_error(exc)
            if entry and entry.get("status") == STATUS_SUCCESS:
                msg += " (os dados da versão anterior deste arquivo foram mantidos)"
            record.update(status=STATUS_FAILED, row_count=0, column_count=0, corrupt_records=0,
                          error_message=msg, dataset=record.get("dataset"))
            summary.failed.append({"file": f.rel_path, "error": msg})
            log.error("  FALHA      %s: %s", f.rel_path, msg)
        finally:
            spark.catalog.clearCache()
        entries.append(record)

    present = {f.rel_path for f in files}
    for key in registry:
        if key not in present:
            log.info("  '%s' não está mais em data/input; os dados dele foram mantidos "
                     "(use --full-refresh para limpar).", key)
    return entries, touched


# --------------------------------------------------------------------------- etapa 3
def _build_silver_layer(spark, settings: Settings, touched: set[str],
                        summary: RunSummary) -> tuple[dict[str, SilverResult], list[str]]:
    """Reconstrói a Silver de cada dataset alterado a partir da Bronze inteira dele."""
    results: dict[str, SilverResult] = {}
    emptied: list[str] = []
    if not touched:
        log.info("  nenhum dataset alterado")
    for dataset in sorted(touched):
        try:
            bpath = bronze_path(settings, dataset)
            bronze_df = delta_io.read_delta(spark, bpath) if delta_io.is_delta_table(bpath) else None
            if bronze_df is None or bronze_df.limit(1).count() == 0:
                log.info("  %s ficou vazio: removendo Bronze, Silver e linhas Gold", dataset)
                delta_io.drop_table(silver_path(settings, dataset))
                delta_io.drop_table(bpath)
                emptied.append(dataset)
                continue
            silver = build_silver(spark, settings, dataset, bronze_df)
            results[dataset] = silver
            log.info("  silver/%s: %s linhas | %s duplicadas | %s com problemas de qualidade",
                     dataset, silver.row_count, silver.duplicate_rows, silver.rows_with_issues)
            log.info("             tipos: %s", ", ".join(f"{c.column}={c.silver_type}" for c in silver.columns))
        except Exception as exc:  # noqa: BLE001
            summary.dataset_errors[dataset] = _short_error(exc)
            log.exception("  Erro na Silver do dataset '%s'", dataset)
        finally:
            spark.catalog.clearCache()
    return results, emptied


# --------------------------------------------------------------------------- etapa 4
def _build_gold_layer(spark, settings: Settings, silver_results: dict[str, SilverResult],
                      emptied: list[str], summary: RunSummary) -> None:
    """Calcula as métricas de cada dataset e grava cada tabela Gold uma única vez."""
    gold_rows: dict[str, list[dict]] = {t: [] for t in GOLD_TABLES}
    gold_datasets: list[str] = list(emptied)  # datasets vazios: só removem as linhas antigas
    for dataset, silver in silver_results.items():
        try:
            silver_df = delta_io.read_delta(spark, silver_path(settings, dataset))
            rows = compute_dataset_gold(spark, settings, silver_df, silver)
            for table, items in rows.items():
                gold_rows[table].extend(items)
            gold_datasets.append(dataset)
            summary.datasets_updated.append(dataset)
            log.info("  %s: %s", dataset, ", ".join(f"{t}={len(rows.get(t, []))}" for t in GOLD_TABLES))
        except Exception as exc:  # noqa: BLE001
            summary.dataset_errors[dataset] = _short_error(exc)
            log.exception("  Erro na Gold do dataset '%s'", dataset)
        finally:
            spark.catalog.clearCache()
    if gold_datasets:
        write_gold(spark, settings, gold_rows, gold_datasets)
        log.info("  tabelas Gold gravadas em %s", settings.lakehouse_dir / "gold")
    else:
        log.info("  nada para atualizar")


# --------------------------------------------------------------------------- etapa 5
def _save_control(spark, settings: Settings, entries: list[dict[str, Any]], summary: RunSummary,
                  mode: str, started: datetime, t0: float) -> None:
    reg.save_registry_entries(spark, settings, entries)
    if not entries:
        status = "SEM_ALTERACOES"
    elif summary.failed or summary.dataset_errors:
        status = "PARCIAL" if summary.processed else "FALHA"
    else:
        status = "SUCESSO"
    reg.append_run(spark, settings, {
        "batch_id": summary.batch_id, "started_at": started, "finished_at": datetime.now(),
        "duration_seconds": round(time.time() - t0, 2), "run_mode": mode,
        "files_found": summary.files_found, "files_processed": len(summary.processed),
        "files_skipped": len(summary.skipped), "files_failed": len(summary.failed),
        "datasets_updated": ", ".join(summary.datasets_updated) or None, "status": status,
        "message": "; ".join(f"{k}: {v}" for k, v in summary.dataset_errors.items()) or None,
    })
    log.info("  %s arquivo(s) no registro desta execução | status: %s", len(entries), status)


# --------------------------------------------------------------------------- auxiliares
def _stage(number: int, title: str) -> None:
    log.info("")
    log.info("[Etapa %s/5] %s", number, title)


def _log_final_summary(summary: RunSummary, seconds: float) -> None:
    log.info("")
    log.info("=== Resumo: %s encontrados | %s processados | %s ignorados | %s com falha | %.1fs ===",
             summary.files_found, len(summary.processed), len(summary.skipped), len(summary.failed), seconds)
    for p in summary.processed:
        log.info("  OK     %-35s -> %-20s %8s linhas", p["file"], p["dataset"], p["rows"])
    for fl in summary.failed:
        log.info("  FALHA  %-35s %s", fl["file"], fl["error"])
    if not summary.processed and not summary.failed:
        log.info("Nada novo para processar. Use --reprocess <arquivo> ou --reprocess-all para forçar.")


def _run_mode(reprocess: list[str], reprocess_all: bool, full_refresh: bool) -> str:
    if full_refresh:
        return "full-refresh"
    if reprocess_all:
        return "reprocess-all"
    return "reprocess" if reprocess else "incremental"


def _clear_lakehouse(settings: Settings) -> None:
    log.info("--full-refresh: apagando Bronze, Silver, Gold e controle em %s", settings.lakehouse_dir)
    for sub in ("bronze", "silver", "gold", "_control"):
        shutil.rmtree(settings.lakehouse_dir / sub, ignore_errors=True)


def _tmp_dir(settings: Settings):
    return settings.lakehouse_dir.parent / "tmp" / "transcoded"


def _describe_read(opts: dict[str, Any]) -> str:
    parts = [str(opts.get("format", ""))]
    if opts.get("format") == "txt":
        parts.append("delimitado" if opts.get("txt_mode") == "delimited" else "texto simples")
    if opts.get("format") == "json":
        parts.append("array/multilinha" if opts.get("multiline") else "JSON Lines")
    elif "delimiter" in opts:
        parts.append(f"delimitador '{opts['delimiter']}'")
    if opts.get("encoding"):
        parts.append(str(opts["encoding"]))
    return ", ".join(p for p in parts if p)


def _short_error(exc: BaseException, limit: int = 600) -> str:
    text = str(exc).strip()
    # Erros do Py4J trazem o stack trace Java inteiro; a primeira linha útil basta.
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("at ") and "An error occurred while calling" not in line:
            text = line
            break
    return f"{type(exc).__name__}: {text}"[:limit]


def _matches(name_or_path: str, f: InputFile) -> bool:
    target = name_or_path.replace("\\", "/").lower()
    return target in (f.name.lower(), f.rel_path.lower()) or target.endswith("/" + f.rel_path.lower())
