# -*- coding: utf-8 -*-
"""Deprecated shim — sync runs with the bot: python run.py

Standalone sync only:
  python run.py --no-command-sync   # not this
  Use: python -c "..." or run package command_sync
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"


def _bootstrap():
    if "laissez_faire" in sys.modules:
        return
    init_py = SRC / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        "laissez_faire", init_py, submodule_search_locations=[str(SRC)])
    mod = importlib.util.module_from_spec(spec)
    mod.__path__ = [str(SRC)]
    sys.modules["laissez_faire"] = mod
    spec.loader.exec_module(mod)


_bootstrap()
from laissez_faire.command_sync import sync_loop_forever  # noqa: E402

if __name__ == "__main__":
    sync_loop_forever()
