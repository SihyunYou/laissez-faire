# -*- coding: utf-8 -*-
"""Shared KRW budget pool for parallel coin workers.

★ 유저 규칙 (혼동 금지):
1) 심볼 = AUTO_SELECT ticker/all 변동성 TopN → HybridMA+vol 통과 (command.txt 폐지)
2) 게이트 = HybridMA + 변동성보호만 (cycle_runner에서 검사)
3) 한도 = ★현재 보유 가용 KRW(free) × WORKER_ALLOC_PCT[N]
   - 각 워커가 free×alloc% 전액 (÷N·형제 reserved 차감 금지, 합>100% 오버서브)
   - 총자산(equity)·보유코인 평가액 기준 아님
   - N = 지금 게이트통과(또는 운용 중) 코인 수 ≤ PARALLEL_WORKERS
#   4) 최초가용KRW 대비 현재가용 < KRW_RESERVE_RATIO(50%)이면
#      신규매수(새 심볼 spawn) 중단 → 잔여 KRW는 기존 코인에 dump
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from .config import (
    MIN_ORDER_AMOUNT,
    PARALLEL_WORKERS,
    WORKER_ALLOC_PCT,
    KRW_RESERVE_RATIO,
)


def alloc_pct_for(selected_count: int) -> float:
    """동시 매수 코인 수 N → 각 워커 한도 비율 (가용 KRW 기준, ÷N 없음)."""
    n = max(1, min(int(selected_count or 1), int(PARALLEL_WORKERS)))
    if n in WORKER_ALLOC_PCT:
        return float(WORKER_ALLOC_PCT[n])
    known = sorted(WORKER_ALLOC_PCT.keys())
    return float(WORKER_ALLOC_PCT[known[min(len(known) - 1, n - 1)]])


@dataclass
class WorkerBudget:
    worker_id: str
    symbol: str = ""
    allocated: float = 0.0  # soft ceiling = free_krw × alloc%
    reserved: float = 0.0
    spent: float = 0.0
    dump_remaining: bool = False
    created_mono: float = field(default_factory=time.monotonic)


class BudgetPool:
    """가용 KRW × alloc% 한도 풀. equity 기준 할당 금지."""

    def __init__(self, max_workers: int = None):
        self._lock = threading.RLock()
        self.max_workers = int(max_workers or PARALLEL_WORKERS)
        # N = 게이트통과 command 심볼 수 (할당% 키). command 전체 개수 아님.
        self.command_coin_count = 1
        self._workers: Dict[str, WorkerBudget] = {}
        self.free_krw = 0.0
        self.total_equity = 0.0  # 로그용만 — 할당 계산에 쓰지 않음
        # ★ 세션 최초 가용 KRW (신규매수 50% 하한 기준)
        self.baseline_krw = 0.0
        self._new_buy_halted = False
        self._halt_log_ts = 0.0

    def set_command_coin_count(self, n: int):
        """할당%용 N 갱신. spawn 용량은 PARALLEL_WORKERS 고정."""
        with self._lock:
            n = max(0, int(n or 0))
            if n <= 0:
                self.command_coin_count = 1
            else:
                self.command_coin_count = min(int(PARALLEL_WORKERS), n)
            self.max_workers = int(PARALLEL_WORKERS)
            self._recompute_ceilings_unlocked()

    set_trade_slots = set_command_coin_count

    def target_slots(self) -> int:
        with self._lock:
            return max(1, int(PARALLEL_WORKERS))

    def alloc_slots(self) -> int:
        with self._lock:
            return max(1, min(
                int(PARALLEL_WORKERS),
                int(self.command_coin_count or 1)))

    def _ceiling_from_free_unlocked(self, coin_n: int = None) -> float:
        """★ 한도 = 현재 가용 KRW × alloc%[N]. equity 사용 금지."""
        n = int(coin_n if coin_n is not None else self.alloc_slots())
        n = max(1, min(int(PARALLEL_WORKERS), n))
        return max(float(MIN_ORDER_AMOUNT),
                   float(self.free_krw) * alloc_pct_for(n))

    def _recompute_ceilings_unlocked(self):
        ceil = self._ceiling_from_free_unlocked()
        for wb in self._workers.values():
            # 이미 쓴 금액은 유지, 추가매수 한도만 free 기준으로 재산정
            wb.allocated = max(ceil, float(wb.spent) + float(wb.reserved))

    def refresh_balances(self, free_krw: float, total_equity: float = None):
        with self._lock:
            self.free_krw = max(0.0, float(free_krw or 0))
            if total_equity is not None and float(total_equity) > 0:
                self.total_equity = float(total_equity)
            elif self.total_equity <= 0:
                self.total_equity = self.free_krw
            # 세션 최초 가용 KRW 잠금 (한 번만, 최소주문 이상일 때)
            if (self.baseline_krw < float(MIN_ORDER_AMOUNT)
                    and self.free_krw >= float(MIN_ORDER_AMOUNT)):
                self.baseline_krw = float(self.free_krw)
            self._update_new_buy_halt_unlocked()
            self._recompute_ceilings_unlocked()

    def _reserve_floor_unlocked(self) -> float:
        """신규매수 허용 하한 = 최초가용 × KRW_RESERVE_RATIO."""
        if self.baseline_krw < float(MIN_ORDER_AMOUNT):
            return 0.0
        return float(self.baseline_krw) * float(KRW_RESERVE_RATIO)

    def _update_new_buy_halt_unlocked(self):
        """현재가용 < 최초×50% → 신규매수 중단 + 기존 코인 dump."""
        floor = self._reserve_floor_unlocked()
        if floor <= 0:
            self._new_buy_halted = False
            return
        was = self._new_buy_halted
        now_halt = self.free_krw + 1e-9 < floor
        self._new_buy_halted = now_halt
        if now_halt and self._workers:
            # 잔여 KRW → 기존 워커 집중
            for wb in self._workers.values():
                wb.dump_remaining = True
            ceil = max(float(self.free_krw), float(MIN_ORDER_AMOUNT))
            for wb in self._workers.values():
                wb.allocated = max(
                    ceil, float(wb.spent) + float(wb.reserved))
        if now_halt != was:
            try:
                from . import engine as eng
                if now_halt:
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        f"신규매수 중단: 가용={int(self.free_krw):,} "
                        f"< 최초×{int(float(KRW_RESERVE_RATIO)*100)}%"
                        f"({int(floor):,}) "
                        f"최초={int(self.baseline_krw):,} "
                        f"— 잔여KRW는 기존 코인 집중")
                else:
                    eng.print_log(
                        eng.LogLevel.SUCCESS,
                        f"신규매수 재개: 가용={int(self.free_krw):,} "
                        f"≥ 최초×{int(float(KRW_RESERVE_RATIO)*100)}%"
                        f"({int(floor):,})")
            except Exception:
                pass

    def is_new_buy_halted(self) -> bool:
        """True면 신규 심볼 spawn/매수 금지. 기존 코인 추가매수는 허용(dump)."""
        with self._lock:
            self._update_new_buy_halt_unlocked()
            return bool(self._new_buy_halted)

    def can_spawn(self) -> bool:
        with self._lock:
            if len(self._workers) >= self.target_slots():
                return False
            self._update_new_buy_halt_unlocked()
            # 신규매수 중단 중이면 spawn 금지 (기존 코인 assist만)
            if self._new_buy_halted:
                return False
            return self.free_krw + 1e-9 >= float(MIN_ORDER_AMOUNT)

    def register_worker(self, worker_id: str, symbol: str = "") -> Optional[WorkerBudget]:
        with self._lock:
            if worker_id in self._workers:
                return self._workers[worker_id]
            if len(self._workers) >= self.target_slots():
                return None
            self._update_new_buy_halt_unlocked()
            if self._new_buy_halted:
                return None
            if self.free_krw + 1e-9 < float(MIN_ORDER_AMOUNT):
                return None
            coin_n = self.alloc_slots()
            allocated = self._ceiling_from_free_unlocked(coin_n)
            wb = WorkerBudget(
                worker_id=worker_id, symbol=symbol, allocated=allocated)
            self._workers[worker_id] = wb
            return wb

    def active_count(self) -> int:
        with self._lock:
            return len(self._workers)

    def spawn_threshold(self, coin_count: int = None):
        """참고용: 현재 free × alloc%."""
        with self._lock:
            return self._ceiling_from_free_unlocked(coin_count)

    def unregister_worker(self, worker_id: str):
        with self._lock:
            self._workers.pop(worker_id, None)

    def plan_budget(self, worker_id: str) -> float:
        """DynamicBuyOrder.total_amount = 현재 free×alloc% 한도."""
        with self._lock:
            wb = self._workers.get(worker_id)
            if not wb:
                return 0.0
            ceil = self._ceiling_from_free_unlocked()
            wb.allocated = max(ceil, float(wb.spent) + float(wb.reserved))
            return float(wb.allocated)

    def claim(self, worker_id: str, want: float) -> float:
        """한도 = 가용×alloc% 전액 예약.

        ★ 형제 reserved 로 깎지 않음 — N명 각각 free×pct (합>100% 오버서브 의도).
        ★ free/N · (free−형제예약) 로 반토막 금지. 절대 ÷N 하지 말 것.
        """
        want = max(0.0, float(want or 0))
        if want < MIN_ORDER_AMOUNT:
            return 0.0
        with self._lock:
            wb = self._workers.get(worker_id)
            if not wb:
                return 0.0
            # 매번 free 기준 한도 재적용 (÷N 없음)
            ceil = self._ceiling_from_free_unlocked()
            wb.allocated = max(ceil, float(wb.spent) + float(wb.reserved))
            # 본인 예약만 제외. 형제 예약은 무시(오버서브).
            idle = max(0.0, self.free_krw - wb.reserved)
            room = max(0.0, wb.allocated - wb.spent - wb.reserved)
            if wb.dump_remaining or room < MIN_ORDER_AMOUNT:
                grant = idle
            else:
                grant = min(want, room, idle)
            if grant < MIN_ORDER_AMOUNT:
                return 0.0
            wb.reserved += grant
            return grant

    def mark_spent(self, worker_id: str, amount: float):
        amount = max(0.0, float(amount or 0))
        with self._lock:
            wb = self._workers.get(worker_id)
            if not wb:
                return
            use = min(amount, wb.reserved)
            wb.reserved = max(0.0, wb.reserved - use)
            wb.spent += amount
            self.free_krw = max(0.0, self.free_krw - amount)
            self._update_new_buy_halt_unlocked()
            self._recompute_ceilings_unlocked()

    def release_reserved(self, worker_id: str, amount: float = None):
        with self._lock:
            wb = self._workers.get(worker_id)
            if not wb:
                return
            if amount is None:
                wb.reserved = 0.0
            else:
                wb.reserved = max(0.0, wb.reserved - max(0.0, float(amount)))

    def enable_dump_remaining(self, worker_id: str):
        with self._lock:
            wb = self._workers.get(worker_id)
            if wb:
                wb.dump_remaining = True

    def available_for(self, worker_id: str) -> float:
        with self._lock:
            wb = self._workers.get(worker_id)
            if not wb:
                return 0.0
            ceil = self._ceiling_from_free_unlocked()
            wb.allocated = max(ceil, float(wb.spent) + float(wb.reserved))
            # 형제 reserved 무시 — 각 워커 기준 가용×alloc%
            idle = max(0.0, self.free_krw - wb.reserved)
            if wb.dump_remaining:
                return idle
            room = max(0.0, wb.allocated - wb.spent - wb.reserved)
            return min(idle, room)

    def assist_idle_to_workers(self) -> Dict[str, float]:
        """잔여 KRW를 기존 워커에 몰아줌.
        신규매수 중단(50% 하한) 시 dump_remaining 강제."""
        with self._lock:
            if not self._workers:
                return {}
            self._update_new_buy_halt_unlocked()
            out = {}
            if self._new_buy_halted:
                ceil = max(float(self.free_krw), float(MIN_ORDER_AMOUNT))
                for wid, wb in self._workers.items():
                    wb.dump_remaining = True
                    before = float(wb.allocated)
                    wb.allocated = max(
                        ceil, float(wb.spent) + float(wb.reserved))
                    if wb.allocated > before:
                        out[wid] = wb.allocated - before
                return out
            self._recompute_ceilings_unlocked()
            ceil = self._ceiling_from_free_unlocked()
            for wid, wb in self._workers.items():
                before = float(wb.allocated)
                wb.allocated = max(ceil, float(wb.spent) + float(wb.reserved))
                if wb.allocated > before:
                    out[wid] = wb.allocated - before
            return out

    def snapshot(self) -> dict:
        with self._lock:
            n = max(1, min(int(PARALLEL_WORKERS), self.command_coin_count))
            pct = alloc_pct_for(n)
            floor = self._reserve_floor_unlocked()
            return {
                'command_coins': n,
                'selected_slots': n,
                'candidate_note': 'command.txt 지정심볼만 / 한도=가용KRW×alloc%',
                'alloc_pct': pct,
                'ceiling': self._ceiling_from_free_unlocked(n),
                'free_krw': self.free_krw,
                'baseline_krw': self.baseline_krw,
                'reserve_floor': floor,
                'new_buy_halted': bool(self._new_buy_halted),
                'total_equity': self.total_equity,
                'slots': self.target_slots(),
                'active': len(self._workers),
                'workers': {
                    wid: {
                        'symbol': w.symbol,
                        'allocated': w.allocated,
                        'reserved': w.reserved,
                        'spent': w.spent,
                        'dump': w.dump_remaining,
                    }
                    for wid, w in self._workers.items()
                },
            }


budget_pool = BudgetPool()
