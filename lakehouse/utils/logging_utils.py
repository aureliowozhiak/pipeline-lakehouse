"""Logging do pipeline: terminal (curto, fácil de ler) + arquivo completo em data/logs/."""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

_FILE_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
_LEVEL_TAGS = {logging.WARNING: "AVISO ", logging.ERROR: "ERRO  ", logging.CRITICAL: "ERRO  "}


class _ConsoleFormatter(logging.Formatter):
    """Mostra "HH:MM:SS  mensagem"; avisos e erros ganham um prefixo."""

    def format(self, record: logging.LogRecord) -> str:
        if not record.getMessage():
            return ""
        tag = _LEVEL_TAGS.get(record.levelno, "")
        text = f"{self.formatTime(record, '%H:%M:%S')}  {tag}{record.getMessage()}"
        if record.exc_info:
            text += "\n" + self.formatException(record.exc_info)
        return text


def _use_utf8_console() -> None:
    # O terminal do Windows pode usar cp1252/cp850; sem isso, acentos viram "?" ou geram erro.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, OSError, ValueError):
            pass


def setup_logging(log_dir: Path | None = None, level: int = logging.INFO) -> Path | None:
    _use_utf8_console()
    root = logging.getLogger("lakehouse")
    root.setLevel(level)
    root.propagate = False
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(_ConsoleFormatter())
    root.addHandler(stream)

    log_file = None
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter(_FILE_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
        root.addHandler(fh)

    logging.getLogger("py4j").setLevel(logging.ERROR)
    return log_file


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"lakehouse.{name}")
