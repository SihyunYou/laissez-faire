# -*- coding: utf-8 -*-
"""python -m laissez_faire [-e upbit|bithumb] [--command-sync]

AUTO_SELECT 기본 — command.txt 심볼 소스 폐지.
Pastebin command_sync는 기본 OFF (--command-sync 로만 켜짐).
"""
from __future__ import annotations

import sys


def _parse_sidecar_flags(argv: list[str]):
    """Strip package-only flags before engine.main sees argv."""
    # 기본 OFF — AUTO_SELECT가 command.txt를 덮어쓰지 않게
    command_sync = False
    kept = []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--command-sync":
            command_sync = True
            i += 1
            continue
        if a == "--no-command-sync":
            # 하위 호환 (이미 기본 OFF)
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
