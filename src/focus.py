# -*- coding: utf-8 -*-
"""Deep-ladder focus mode.

6라운드(레벨) 이상 돌입 코인이 있으면:
  - ★ 다른 코인 추가 매수만 금지 (기존 매도/보유는 그대로 — 억지 매도 없음)
  - 타코인이 순서대로 자연 매도되어 KRW가 풀리면 → 집중 코인 L8(drop_count+1)에 투입
  - 집중 코인이 매도(사이클 종료)된 뒤에야 병렬 매수 재개
"""
from __future__ import annotations

import threading
from typing import List, Optional

from .config import DEEP_LADDER_LEVEL, MIN_ORDER_AMOUNT

_lock = threading.RLock()
_focus_symbol: Optional[str] = None  # UPPER


def buyer_deep_level(buyer) -> int:
    """현재 사다리에서 *실제 돌입*한 최고 라운드.
    미체결 계획레벨은 제외 — L1만 떠 있는데 L6 planned로 허위 집중모드 켜지던 버그 차단."""
    if buyer is None:
        return 0
    best = 0
    try:
        best = max(best, int(getattr(buyer, 'last_executed_level', 0) or 0))
    except (TypeError, ValueError):
        pass
    try:
        exec_c = int(getattr(buyer, 'executed_count', 0) or 0)
        if exec_c > 0:
            best = max(best, exec_c)
    except (TypeError, ValueError):
        pass
    # pending = 이미 POST된 레벨만 (계획만 있는 레벨 제외)
    for p in getattr(buyer, 'pending_orders', None) or []:
        try:
            best = max(best, int(p.get('level') or 0))
        except (TypeError, ValueError):
            pass
    for lv in getattr(buyer, 'partial_levels', None) or []:
        try:
            best = max(best, int(lv))
        except (TypeError, ValueError):
            pass
    for lv in getattr(buyer, 'pending_levels', None) or []:
        try:
            best = max(best, int(lv))
        except (TypeError, ValueError):
            pass
    return int(best)


def buyer_in_deep_rounds(buyer, threshold: int = None) -> bool:
    """6라운드 이상 돌입 여부."""
    th = int(threshold if threshold is not None else DEEP_LADDER_LEVEL)
    return buyer_deep_level(buyer) >= th


def get_focus_symbol() -> Optional[str]:
    with _lock:
        return _focus_symbol


def set_focus_symbol(symbol: Optional[str], *, reason: str = '') -> Optional[str]:
    """포커스 심볼 설정/해제. 반환=현재 포커스."""
    global _focus_symbol
    with _lock:
        if not symbol:
            prev = _focus_symbol
            _focus_symbol = None
            if prev:
                try:
                    from . import engine as eng
                    eng.print_log(
                        eng.LogLevel.SUCCESS,
                        f"집중모드 해제 ({prev}) — 병렬매수 재개"
                        + (f" | {reason}" if reason else ""))
                except Exception:
                    pass
            return None
        su = str(symbol).upper()
        prev = _focus_symbol
        _focus_symbol = su
        if prev != su:
            try:
                from . import engine as eng
                eng.print_log(
                    eng.LogLevel.WARNING,
                    f"집중모드 ON → {su} (L{DEEP_LADDER_LEVEL}+) "
                    f"— 타코인 추가매수 금지 (매도·보유는 유지), "
                    f"자연매도 KRW는 {su}에 집중"
                    + (f" | {reason}" if reason else ""))
            except Exception:
                pass
        return su


def clear_focus_if(symbol: str, *, reason: str = '') -> bool:
    """해당 심볼이 포커스면 해제. True=해제됨."""
    su = str(symbol or '').upper()
    with _lock:
        if not (_focus_symbol and _focus_symbol == su):
            return False
    set_focus_symbol(None, reason=reason or f'{su} 매도/사이클종료')
    return True


def sync_focus_from_workers(workers: List) -> Optional[str]:
    """활성 워커 중 6라운드+ 코인을 포커스로 선정 (가장 깊은 레벨 우선).
    포커스 코인 워커가 사라지면(매도 완료) 해제.
    선정 시 해당 buyer에 L8(assist) 라운드를 편입."""
    best_sym = None
    best_lv = 0
    best_buyer = None
    for w in workers or []:
        if not w:
            continue
        buyer = getattr(w, 'dynamic_buyer', None)
        if not buyer_in_deep_rounds(buyer):
            continue
        if getattr(buyer, '_cycle_ended', False):
            continue
        lv = buyer_deep_level(buyer)
        sym = str(getattr(w, 'symbol', '') or getattr(buyer, 'symbol', '')).upper()
        if not sym:
            continue
        if lv > best_lv:
            best_lv = lv
            best_sym = sym
            best_buyer = buyer
    if best_sym:
        try:
            if best_buyer is not None and hasattr(best_buyer, 'ensure_assist_level'):
                best_buyer.ensure_assist_level()
        except Exception:
            pass
        return set_focus_symbol(best_sym, reason=f'L{best_lv} 돌입')
    cur = get_focus_symbol()
    if cur:
        alive = {
            str(getattr(w, 'symbol', '')).upper()
            for w in (workers or [])
            if w and getattr(w, 'alive', True)
            and not getattr(getattr(w, 'dynamic_buyer', None), '_cycle_ended', False)
        }
        if cur in alive:
            return cur
        set_focus_symbol(None, reason=f'{cur} 매도완료·워커종료')
    return get_focus_symbol()


def apply_focus_budget(pool, focus_symbol: str = None) -> None:
    """타코인 자연매도로 풀린 KRW → 집중 코인 L8(assist)에 dump.
    타코인 워커는 죽이지 않음(매도 유지). 추가 claim만 막음."""
    su = str(focus_symbol or get_focus_symbol() or '').upper()
    if not su or pool is None:
        return
    try:
        with pool._lock:
            target = None
            for wid, wb in pool._workers.items():
                if str(wb.symbol or '').upper() == su:
                    target = wb
                    break
            if not target:
                return
            base = pool.total_equity if pool.total_equity > 0 else pool.free_krw
            # 풀린 자금 포함해 집중 코인 soft ceiling 상향
            target.allocated = max(
                float(target.allocated),
                float(base),
                float(MIN_ORDER_AMOUNT))
            target.dump_remaining = True
            # 타코인: 추가 매수 claim만 억제 (매도·보유 유지)
            for wid, wb in pool._workers.items():
                if wb is target:
                    continue
                wb.dump_remaining = False
                wb.allocated = max(
                    float(MIN_ORDER_AMOUNT),
                    float(wb.spent + wb.reserved))
    except Exception:
        pass


def should_block_buy(symbol: str) -> bool:
    """집중모드에서 타코인 추가매수 금지. (매도는 막지 않음)
    ★ 포커스 코인이 MA60 위(게이트 실패)면 타코인 매수를 막지 않음 —
      포커스가 진입 못하는 동안 LA/ERA 등이 슬롯만 점유한 채 매수 불능이 되던 버그."""
    focus = get_focus_symbol()
    if not focus:
        return False
    su = str(symbol or '').upper()
    if su == focus:
        return False
    try:
        from . import engine as eng
        ok, _ = eng.RealMarketData.check_tick_ma_gate(focus)
        if not ok:
            return False
    except Exception:
        pass
    return True
