# -*- coding: utf-8 -*-
"""Project path resolution — cwd-independent."""
from __future__ import annotations

from pathlib import Path

# src/paths.py → parents[1] = repo root
PACKAGE_DIR = Path(__file__).resolve().parent  # src/
PROJECT_ROOT = PACKAGE_DIR.parent

LOG_DIR = PROJECT_ROOT / "log"

COMMAND_TXT = LOG_DIR / "command.txt"
BALANCE_TXT = LOG_DIR / "balance.txt"
STATE_TXT = LOG_DIR / "state.txt"
KEY_TXT = PROJECT_ROOT / "key.txt"
KEY_BITHUMB_TXT = PROJECT_ROOT / "key_bithumb.txt"


def ensure_log_dir() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    return LOG_DIR
