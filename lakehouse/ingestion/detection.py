"""Detecção (sem Spark) de codificação, delimitador, modo do TXT e formato do JSON.

Tudo é feito sobre uma amostra do início do arquivo, então arquivos enormes não
são lidos inteiros nesta etapa.
"""

from __future__ import annotations

import codecs
import csv
import io
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

SAMPLE_BYTES = 256 * 1024
MAX_SAMPLE_RECORDS = 500
DELIMITER_CANDIDATES = (",", ";", "\t", "|")

_SPARK_CHARSETS = {
    "utf-8": "UTF-8",
    "utf8": "UTF-8",
    "utf-8-sig": "UTF-8",
    "latin-1": "ISO-8859-1",
    "latin1": "ISO-8859-1",
    "iso-8859-1": "ISO-8859-1",
    "iso8859-1": "ISO-8859-1",
    "cp1252": "windows-1252",
    "windows-1252": "windows-1252",
    "utf-16": "UTF-16",
    "utf-16-le": "UTF-16LE",
    "utf-16-be": "UTF-16BE",
}


@dataclass
class EncodingInfo:
    python: str  # nome para o Python (codecs)
    spark: str  # nome do charset Java
    has_bom: bool

    @property
    def is_plain_utf8(self) -> bool:
        return self.spark == "UTF-8" and not self.has_bom


def read_sample(path: Path, max_bytes: int = SAMPLE_BYTES) -> tuple[bytes, bool]:
    """Retorna (amostra, truncada?)."""
    with open(path, "rb") as fh:
        data = fh.read(max_bytes + 1)
    return data[:max_bytes], len(data) > max_bytes


def detect_encoding(sample: bytes, configured: str = "auto") -> EncodingInfo:
    if sample.startswith(codecs.BOM_UTF8):
        return EncodingInfo("utf-8-sig", "UTF-8", True)
    if sample.startswith(codecs.BOM_UTF16_LE) or sample.startswith(codecs.BOM_UTF16_BE):
        return EncodingInfo("utf-16", "UTF-16", True)
    configured = (configured or "auto").strip()
    if configured.lower() != "auto":
        key = configured.lower()
        codecs.lookup("cp1252" if key == "windows-1252" else key)  # valida o nome
        return EncodingInfo(configured, _SPARK_CHARSETS.get(key, configured), False)
    decoder = codecs.getincrementaldecoder("utf-8")()
    try:
        decoder.decode(sample, final=False)  # tolera caractere cortado no fim da amostra
        return EncodingInfo("utf-8", "UTF-8", False)
    except UnicodeDecodeError:
        return EncodingInfo("cp1252", "windows-1252", False)


def decode_sample(sample: bytes, encoding: EncodingInfo, truncated: bool) -> str:
    py_enc = "latin-1" if encoding.python == "cp1252" else encoding.python
    text = sample.decode(py_enc, errors="replace")
    if truncated and "\n" in text:
        text = text[: text.rfind("\n")]  # descarta a última linha (possivelmente incompleta)
    return text.lstrip("\ufeff")


def _parse_records(text: str, delimiter: str, quotechar: str = '"', limit: int = MAX_SAMPLE_RECORDS) -> list[list[str]]:
    reader = csv.reader(io.StringIO(text), delimiter=delimiter, quotechar=quotechar or '"')
    rows: list[list[str]] = []
    try:
        for row in reader:
            if row and any(cell.strip() for cell in row):
                rows.append(row)
            if len(rows) >= limit:
                break
    except csv.Error:
        pass
    return rows


@dataclass
class DelimiterGuess:
    delimiter: str | None
    consistency: float  # fração das linhas com o número de colunas mais comum
    columns: int
    records: int


def detect_delimiter(text: str, quotechar: str = '"', candidates=DELIMITER_CANDIDATES) -> DelimiterGuess:
    """Escolhe o delimitador que gera o número de colunas mais consistente (>= 2).

    Desempate: maior consistência, depois mais colunas.
    """
    best = DelimiterGuess(None, 0.0, 0, 0)
    for delim in candidates:
        rows = _parse_records(text, delim, quotechar)
        if not rows:
            continue
        counts = Counter(len(r) for r in rows)
        mode_cols, mode_freq = counts.most_common(1)[0]
        if mode_cols < 2:
            continue
        consistency = mode_freq / len(rows)
        if (consistency, mode_cols) > (best.consistency, best.columns):
            best = DelimiterGuess(delim, consistency, mode_cols, len(rows))
    return best


def detect_txt_mode(text: str, quotechar: str = '"', delimiter: str = "auto") -> tuple[str, DelimiterGuess]:
    """'delimited' se as linhas formam colunas de forma consistente, senão 'plain'."""
    if delimiter and delimiter != "auto":
        rows = _parse_records(text, delimiter, quotechar)
        counts = Counter(len(r) for r in rows)
        if rows and counts.most_common(1)[0][0] >= 2:
            cols, freq = counts.most_common(1)[0]
            return "delimited", DelimiterGuess(delimiter, freq / len(rows), cols, len(rows))
        return "plain", DelimiterGuess(None, 0.0, 1, len(rows))
    guess = detect_delimiter(text, quotechar)
    if guess.delimiter and guess.records >= 2 and guess.consistency >= 0.9:
        return "delimited", guess
    return "plain", guess


def first_record(text: str, delimiter: str, quotechar: str = '"') -> list[str]:
    rows = _parse_records(text, delimiter, quotechar, limit=1)
    return rows[0] if rows else []


def max_columns(text: str, delimiter: str, quotechar: str = '"') -> int:
    rows = _parse_records(text, delimiter, quotechar)
    return max((len(r) for r in rows), default=0)


def detect_json_multiline(text: str) -> bool:
    """True para array JSON / objeto em várias linhas; False para JSON Lines."""
    stripped = text.lstrip()
    if not stripped:
        return False
    if stripped[0] == "[":
        return True
    if stripped[0] == "{":
        first_line = next((ln for ln in stripped.splitlines() if ln.strip()), "")
        try:
            json.loads(first_line)
            return False
        except ValueError:
            return True
    return False
