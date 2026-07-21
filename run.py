# -*- coding: utf-8 -*-
"""Repo-root launcher: python run.py -e upbit

Maps package name `laissez_faire` → `src/` (flat modules).
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"


def _bootstrap_package() -> None:
    """Register src/ as importable package laissez_faire without nested folder."""
    if "laissez_faire" in sys.modules:
        return
    init_py = SRC / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        "laissez_faire",
        init_py,
        submodule_search_locations=[str(SRC)],
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load package from {SRC}")
    mod = importlib.util.module_from_spec(spec)
    mod.__path__ = [str(SRC)]
    sys.modules["laissez_faire"] = mod
    spec.loader.exec_module(mod)


_bootstrap_package()

from laissez_faire.__main__ import main  # noqa: E402

if __name__ == "__main__":
    main()
