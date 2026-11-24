"""Carregamento da configuração (config/settings.yaml) com valores padrão."""

from __future__ import annotations

import copy
import fnmatch
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from lakehouse import PROJECT_ROOT

DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "settings.yaml"

SUPPORTED_FORMATS = {".csv": "csv", ".json": "json", ".jsonl": "json", ".txt": "txt"}

DEFAULTS: dict[str, Any] = {
    "paths": {
        "input_dir": "data/input",
        "lakehouse_dir": "data/lakehouse",
        "log_dir": "data/logs",
    },
    "spark": {
        "master": "local[*]",
        "driver_memory": "2g",
        "shuffle_partitions": 4,
        "java_home": "",
    },
    "read_defaults": {
        "encoding": "auto",
        "csv": {"delimiter": "auto", "header": True, "quote": '"', "escape": '"', "multiline": True},
        "txt": {
            "mode": "auto",
            "delimiter": "auto",
            "header": True,
            "quote": '"',
            "escape": '"',
            "multiline": True,
        },
        "json": {"multiline": "auto"},
    },
    "files": {},
    "silver": {
        "null_tokens": [
            "", "null", "none", "nan", "na", "n/a", "n.a.", "nil", "-", "--", "?",
            "#n/d", "#n/a", "(null)", "undefined",
        ],
        "type_inference_threshold": 0.95,
        "boolean_true": ["true", "t", "yes", "y", "sim", "s", "verdadeiro"],
        "boolean_false": ["false", "f", "no", "n", "nao", "não", "falso"],
    },
    "gold": {"top_values": 15, "histogram_bins": 20, "categorical_max_distinct": 50},
}

FILE_OPTION_KEYS = {"encoding", "delimiter", "header", "quote", "escape", "multiline", "mode", "dataset"}

DELIMITER_ALIASES = {
    "tab": "\t",
    "\\t": "\t",
    "tabulacao": "\t",
    "tabulação": "\t",
    "pipe": "|",
    "semicolon": ";",
    "ponto_e_virgula": ";",
    "comma": ",",
    "virgula": ",",
    "vírgula": ",",
    "space": " ",
    "espaco": " ",
    "espaço": " ",
}


def _deep_merge(base: dict, override: dict) -> dict:
    result = copy.deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def normalize_delimiter(value: Any) -> str:
    """Converte aliases ("tab", "pipe", "\\t"...) no caractere real; "auto" é mantido."""
    if value is None:
        return "auto"
    text = str(value)
    if text.lower() == "auto":
        return "auto"
    return DELIMITER_ALIASES.get(text.lower(), text)


def _to_bool_or_auto(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("auto", ""):
        return "auto"
    if text in ("true", "sim", "yes", "1"):
        return True
    if text in ("false", "nao", "não", "no", "0"):
        return False
    raise ValueError(f"Valor booleano inválido na configuração: {value!r}")


@dataclass
class Settings:
    raw: dict[str, Any]
    config_path: Path | None = None
    root: Path = field(default=PROJECT_ROOT)

    def _path(self, key: str) -> Path:
        p = Path(self.raw["paths"][key])
        return p if p.is_absolute() else (self.root / p)

    @property
    def input_dir(self) -> Path:
        return self._path("input_dir")

    @property
    def lakehouse_dir(self) -> Path:
        return self._path("lakehouse_dir")

    @property
    def log_dir(self) -> Path:
        return self._path("log_dir")

    @property
    def bronze_dir(self) -> Path:
        return self.lakehouse_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.lakehouse_dir / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.lakehouse_dir / "gold"

    @property
    def control_dir(self) -> Path:
        return self.lakehouse_dir / "_control"

    @property
    def spark(self) -> dict[str, Any]:
        return self.raw["spark"]

    @property
    def silver(self) -> dict[str, Any]:
        return self.raw["silver"]

    @property
    def gold(self) -> dict[str, Any]:
        return self.raw["gold"]

    def read_options(self, file_name: str, rel_path: str, fmt: str) -> dict[str, Any]:
        """Opções efetivas de leitura: padrões do formato + ajustes por arquivo."""
        defaults = self.raw["read_defaults"]
        opts: dict[str, Any] = {"encoding": defaults.get("encoding", "auto")}
        opts.update(copy.deepcopy(defaults.get(fmt, {})))
        opts["dataset"] = None
        for pattern, override in (self.raw.get("files") or {}).items():
            if not isinstance(override, dict):
                continue
            pat = str(pattern).replace("\\", "/")
            if fnmatch.fnmatch(file_name.lower(), pat.lower()) or fnmatch.fnmatch(
                rel_path.lower(), pat.lower()
            ):
                unknown = set(override) - FILE_OPTION_KEYS
                if unknown:
                    raise ValueError(
                        f"Opções desconhecidas para '{pattern}' em files: {sorted(unknown)}. "
                        f"Aceitas: {sorted(FILE_OPTION_KEYS)}"
                    )
                opts.update(override)
        if "delimiter" in opts:
            opts["delimiter"] = normalize_delimiter(opts["delimiter"])
        if "header" in opts:
            opts["header"] = bool(_to_bool_or_auto(opts["header"]))
        if "multiline" in opts:
            opts["multiline"] = _to_bool_or_auto(opts["multiline"])
        if "mode" in opts:
            mode = str(opts["mode"]).lower()
            if mode not in ("auto", "delimited", "plain"):
                raise ValueError(f"mode inválido: {opts['mode']!r} (use auto, delimited ou plain)")
            opts["mode"] = mode
        return opts


def load_settings(config_path: str | Path | None = None, overrides: dict | None = None) -> Settings:
    """Ordem: argumento > variável de ambiente LAKEHOUSE_CONFIG > config/settings.yaml."""
    config_path = config_path or os.environ.get("LAKEHOUSE_CONFIG") or None
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    user_cfg: dict[str, Any] = {}
    if path.exists():
        with open(path, encoding="utf-8") as fh:
            user_cfg = yaml.safe_load(fh) or {}
    elif config_path:
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {path}")
    raw = _deep_merge(DEFAULTS, user_cfg)
    if overrides:
        raw = _deep_merge(raw, overrides)
    raw["files"] = raw.get("files") or {}
    return Settings(raw=raw, config_path=path if path.exists() else None)
