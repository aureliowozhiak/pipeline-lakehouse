"""Padronização de nomes de colunas e de datasets."""

from __future__ import annotations

import re
import unicodedata
from typing import Iterable

# Caracteres proibidos em nomes de coluna do Delta Lake (sem column mapping).
_DELTA_INVALID = re.compile(r"[ ,;{}()\n\t=.]")


def strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def to_snake_case(name: str) -> str:
    """'Data de Cadastro' -> 'data_de_cadastro'; 'valorTotal' -> 'valor_total'.

    Retorna string vazia quando não sobra nenhum caractere válido.
    """
    text = str(name).replace("\ufeff", "").strip()
    text = strip_accents(text)
    text = text.replace("ß", "ss").replace("æ", "ae").replace("ø", "o")
    # camelCase / PascalCase -> snake_case
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", text)
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    if text and text[0].isdigit():
        text = f"col_{text}"
    return text


def make_unique(names: Iterable[str], reserved: Iterable[str] = (), case_insensitive: bool = True) -> list[str]:
    """Garante nomes únicos acrescentando sufixos _2, _3, ..."""
    key = (lambda s: s.lower()) if case_insensitive else (lambda s: s)
    used = {key(r) for r in reserved}
    result = []
    for name in names:
        candidate = name
        n = 2
        while key(candidate) in used:
            candidate = f"{name}_{n}"
            n += 1
        used.add(key(candidate))
        result.append(candidate)
    return result


def normalize_column_names(names: Iterable[str], reserved: Iterable[str] = ()) -> list[str]:
    """snake_case, sem acentos, sem vazios e sem duplicados."""
    base = []
    for i, name in enumerate(names, start=1):
        snake = to_snake_case(name)
        base.append(snake or f"coluna_{i}")
    return make_unique(base, reserved=reserved)


def bronze_column_names(names: Iterable[str], reserved: Iterable[str] = ()) -> list[str]:
    """Ajuste mínimo para gravar no Delta preservando o nome original o máximo possível.

    Só troca os caracteres que o Delta não aceita por '_' e remove BOM/espaços nas bordas.
    """
    base = []
    for i, name in enumerate(names, start=1):
        text = "" if name is None else str(name).replace("\ufeff", "").strip()
        text = _DELTA_INVALID.sub("_", text)
        base.append(text or f"coluna_{i}")
    return make_unique(base, reserved=reserved)


def dataset_name_from_file(stem: str) -> str:
    name = to_snake_case(stem)
    return name or "dataset"
