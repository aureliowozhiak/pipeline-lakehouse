"""Utilitários de arquivos: hash, descoberta de arquivos de entrada."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from lakehouse.config.settings import SUPPORTED_FORMATS


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass
class InputFile:
    path: Path
    rel_path: str  # caminho relativo a data/input, com '/' (chave do registro)
    name: str
    fmt: str  # csv | json | txt
    size: int


def discover_input_files(input_dir: Path) -> tuple[list[InputFile], list[Path]]:
    """Retorna (arquivos suportados, arquivos ignorados). Busca também em subpastas."""
    supported: list[InputFile] = []
    ignored: list[Path] = []
    if not input_dir.exists():
        return supported, ignored
    for path in sorted(input_dir.rglob("*")):
        if not path.is_file():
            continue
        rel_parts = path.relative_to(input_dir).parts
        if any(part.startswith(".") or part.startswith("~$") for part in rel_parts):
            continue  # ocultos, .gitkeep, temporários do Excel
        fmt = SUPPORTED_FORMATS.get(path.suffix.lower())
        if fmt is None:
            ignored.append(path)
            continue
        supported.append(
            InputFile(
                path=path,
                rel_path="/".join(rel_parts),
                name=path.name,
                fmt=fmt,
                size=path.stat().st_size,
            )
        )
    return supported, ignored
