"""Pipeline lakehouse local: ingestão -> Bronze -> Silver -> Gold (Delta Lake)."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
