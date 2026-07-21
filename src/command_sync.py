# -*- coding: utf-8 -*-
"""Pastebin → log/command.txt sync (background daemon).

Formerly log/command.py — started automatically with `python -m laissez_faire`.
"""
from __future__ import annotations

import threading
import time
from typing import Optional

import requests

from .paths import COMMAND_TXT, ensure_log_dir

REMOTE_URL = "https://pastebin.com/raw/iGrXJ3yP"
CHECK_INTERVAL = 5.0

_stop = threading.Event()
_thread: Optional[threading.Thread] = None
_lock = threading.Lock()


def _normalize_text(text: str) -> str:
    """개행/끝공백 정규화 — Windows read가 \\r\\n→\\n 변환해도 동일 비교되게."""
    if not text:
        return ""
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    # 줄 끝 공백 제거, 파일 끝 빈 줄은 하나로 통일
    lines = [ln.rstrip() for ln in t.split("\n")]
    while lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines) + ("\n" if lines else "")


def fetch_remote_text() -> Optional[str]:
    try:
        response = requests.get(REMOTE_URL, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"[command_sync] fetch error: {e}")
        return None


def load_local_text() -> str:
    ensure_log_dir()
    if not COMMAND_TXT.exists():
        return ""
    return COMMAND_TXT.read_text(encoding="utf-8")


def save_local_text(text: str) -> None:
    ensure_log_dir()
    # 정규화된 LF 텍스트 저장 (비교는 항상 _normalize_text로 수행)
    COMMAND_TXT.write_text(_normalize_text(text), encoding="utf-8")


def sync_once() -> bool:
    """Pull remote once. Returns True if local file content actually changed."""
    remote = fetch_remote_text()
    if remote is None:
        return False
    remote_n = _normalize_text(remote)
    local_n = _normalize_text(load_local_text())
    if remote_n == local_n:
        return False
    save_local_text(remote_n)
    print("[command_sync] command.txt updated from remote")
    return True


def _loop(initial_sync: bool = True) -> None:
    print(f"[command_sync] started → {COMMAND_TXT} every {CHECK_INTERVAL:.0f}s")
    first = True
    while not _stop.is_set():
        if first and not initial_sync:
            first = False
        else:
            try:
                sync_once()
            except Exception as e:
                print(f"[command_sync] loop error: {e}")
            first = False
        if _stop.wait(CHECK_INTERVAL):
            break
    print("[command_sync] stopped")


def start_background(daemon: bool = True) -> threading.Thread:
    """Idempotent — start sync thread once."""
    global _thread
    with _lock:
        if _thread is not None and _thread.is_alive():
            return _thread
        _stop.clear()
        ensure_log_dir()
        # first pull immediately so trading sees remote symbols ASAP
        try:
            sync_once()
        except Exception:
            pass
        _thread = threading.Thread(
            target=_loop, kwargs={"initial_sync": False},
            name="command-sync", daemon=daemon)
        _thread.start()
        return _thread


def stop_background(timeout: float = 2.0) -> None:
    global _thread
    _stop.set()
    t = _thread
    if t is not None and t.is_alive():
        t.join(timeout=timeout)
    _thread = None


def sync_loop_forever() -> None:
    """Blocking standalone loop (for `python -m laissez_faire.command_sync`)."""
    _stop.clear()
    ensure_log_dir()
    _loop()


if __name__ == "__main__":
    sync_loop_forever()
