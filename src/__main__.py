# -*- coding: utf-8 -*-
"""python -m laissez_faire [-e upbit|bithumb] [--no-command-sync]"""
from __future__ import annotations

import sys


def _parse_sidecar_flags(argv: list[str]):
    """Strip package-only flags before engine.main sees argv."""
    command_sync = True
    kept = []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--no-command-sync":
            command_sync = False
            i += 1
            continue
        kept.append(a)
        i += 1
    return kept, command_sync


def main():
    from .paths import ensure_log_dir

    ensure_log_dir()
    kept, want_sync = _parse_sidecar_flags(sys.argv[1:])
    sys.argv = [sys.argv[0]] + kept

    if want_sync:
        from .command_sync import start_background as start_command_sync
        start_command_sync(daemon=True)

    from .engine import main as engine_main
    engine_main()


if __name__ == "__main__":
    main()
