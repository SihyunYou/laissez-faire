# -*- coding: utf-8 -*-
"""Parallel coin workers — N independent buy/sell cycles sharing KRW + rate limit.

Phase status:
  ✓ BudgetPool (budget.py) — alloc% / spawn gate / dump-remaining / assist
  ✓ OrderRateLimiter fair quota — set_active_workers + acquire(worker_id=)
  ◐ WorkerSupervisor — spawn/reap orchestration (wired next)
  ○ Per-symbol avg-sell registry (private_ws still single-target)
  ○ main() pool loop replacing serial run_managed_cycle

One CoinWorker ≈ today's single cycle: own DynamicBuyOrder, SellController,
TradingManager slice, CycleContext modules. Shared: budget_pool, order_rate_limiter.
"""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .budget import BudgetPool, budget_pool, alloc_pct_for
from .config import MIN_ORDER_AMOUNT, PARALLEL_WORKERS, WORKER_ALLOC_PCT


def describe_alloc_table() -> str:
    """Human-readable allocation table for boot log."""
    from .config import AUTO_SELECT_TOP_N
    parts = [
        f"코인{n}개→가용KRW×{int(p * 100)}%"
        for n, p in sorted(WORKER_ALLOC_PCT.items())
    ]
    return (
        f"PARALLEL_WORKERS≤{PARALLEL_WORKERS} "
        f"AUTO CV Top{AUTO_SELECT_TOP_N} "
        f"한도[{', '.join(parts)}]"
    )


@dataclass
class CoinWorker:
    """One coin trading cycle under the shared pool."""
    worker_id: str
    symbol: str
    budget: Any = None  # WorkerBudget
    thread: Optional[threading.Thread] = None
    stop: threading.Event = field(default_factory=threading.Event)
    done: threading.Event = field(default_factory=threading.Event)
    error: Optional[BaseException] = None
    # filled by runner
    trading_manager: Any = None
    dynamic_buyer: Any = None
    sell_controller: Any = None

    @property
    def alive(self) -> bool:
        return bool(self.thread and self.thread.is_alive() and not self.done.is_set())


class WorkerSupervisor:
    """Owns N CoinWorkers. Picks symbols, allocates budget, starts cycles.

    `cycle_runner(worker) -> None` must run one full buy→sell cycle and return
    when the coin is flat (or stop set). Injected from engine to avoid cycles.
    """

    def __init__(
            self,
            pool: BudgetPool = None,
            max_workers: int = None,
            cycle_runner: Callable[[CoinWorker], None] = None,
            symbol_picker: Callable[[], Optional[str]] = None):
        self.pool = pool or budget_pool
        self.max_workers = int(max_workers or PARALLEL_WORKERS)
        self.pool.max_workers = self.max_workers
        self.cycle_runner = cycle_runner
        self.symbol_picker = symbol_picker
        self._lock = threading.RLock()
        self._workers: Dict[str, CoinWorker] = {}
        self._stop = threading.Event()

    def active_symbols(self) -> List[str]:
        with self._lock:
            return [w.symbol for w in self._workers.values() if w.alive]

    def active_count(self) -> int:
        with self._lock:
            return sum(1 for w in self._workers.values() if w.alive)

    def sync_rate_limiter(self):
        try:
            from . import engine as eng
            eng.order_rate_limiter.set_active_workers(
                max(1, self.active_count() or self.max_workers))
        except Exception:
            pass

    def force_end_worker(self, worker: CoinWorker, *, reason: str = '') -> None:
        """워커 사이클 강제 종료 (슬롯 회수)."""
        if not worker:
            return
        try:
            buyer = worker.dynamic_buyer
            if buyer is not None:
                buyer._cycle_ended = True
                buyer.is_active = False
                try:
                    buyer._cancel_pending_sync()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            worker.stop.set()
            tm = worker.trading_manager
            if tm is not None and hasattr(tm, 'trading_completed'):
                pass
        except Exception:
            pass
        try:
            from . import engine as eng
            eng.print_log(
                eng.LogLevel.WARNING,
                f"워커 강제종료 {worker.symbol} ({worker.worker_id})"
                + (f": {reason}" if reason else ""))
        except Exception:
            pass

    def reap_ma_wait_zombies(self, keep_symbols=None) -> int:
        """체결0·보유0·MA대기로 슬롯만 차지하는 워커 회수.
        keep_symbols=지금 게이트통과 심볼은 유지."""
        keep = {str(s).upper() for s in (keep_symbols or [])}
        killed = 0
        with self._lock:
            self._reap_unlocked()
            for w in list(self._workers.values()):
                if not w or not w.alive:
                    continue
                su = str(w.symbol or '').upper()
                if su in keep:
                    continue
                buyer = w.dynamic_buyer
                if buyer is None:
                    continue
                try:
                    exec_c = int(getattr(buyer, 'executed_count', 0) or 0)
                except (TypeError, ValueError):
                    exec_c = 0
                if exec_c > 0:
                    continue
                if getattr(buyer, '_cycle_ended', False):
                    continue
                # 보유/매도 진행이면 유지
                try:
                    vol = float(getattr(w, 'current_volume', 0) or 0)
                except (TypeError, ValueError):
                    vol = 0.0
                if vol > 0.00001:
                    continue
                paused = bool(getattr(buyer, '_ma_gate_paused', False))
                # 체결0인데 MA대기 pause인 경우만 회수 (포커스차단 등은 매도유지)
                if not paused:
                    continue
                self.force_end_worker(
                    w, reason='MA대기 좀비 슬롯회수 (타코인 MA통과 매수 우선)')
                killed += 1
        return killed

    def reap_not_in_command(self, command_symbols) -> int:
        """선정 목록(AUTO TopN)에서 빠진 심볼 워커 슬롯 회수.

        - TOPN_EXCLUDE_GRACE_S 유예: 순위 깜빡임으로 즉시 halt/kill 금지
        - 체결0·보유0 → 유예 후 강제종료 (새 코인 매수 자리 확보)
        - 체결/보유 있음 → 유예 후 추가매수만 중단 (매도 사이클 유지)
        - keep에 재편입되면 TopN halt 해제·매수 재개
        """
        from .config import TOPN_EXCLUDE_GRACE_S
        keep = {str(s).upper() for s in (command_symbols or []) if s}
        if not keep:
            return 0
        grace = float(TOPN_EXCLUDE_GRACE_S or 0)
        now = time.time()
        killed = 0
        with self._lock:
            self._reap_unlocked()
            for w in list(self._workers.values()):
                if not w or not w.alive:
                    continue
                su = str(w.symbol or '').upper()
                if not su:
                    continue
                buyer = w.dynamic_buyer
                if su in keep:
                    # TopN 복귀 — miss 타이머·TopN halt 해제
                    w._topn_miss_since = None
                    if (buyer is not None
                            and getattr(buyer, '_buys_halted_by_topn', False)
                            and not getattr(buyer, '_cycle_ended', False)):
                        buyer._buys_halted = False
                        buyer._buys_halted_by_topn = False
                        buyer.is_active = True
                        try:
                            from . import engine as eng
                            eng.print_log(
                                eng.LogLevel.SUCCESS,
                                f"AUTO TopN 복귀 {su}: 추가매수 재개")
                        except Exception:
                            pass
                    continue

                miss = getattr(w, '_topn_miss_since', None)
                if miss is None:
                    w._topn_miss_since = now
                    continue
                if grace > 0 and (now - float(miss)) < grace:
                    continue  # 유예 중 — halt/kill 보류

                try:
                    exec_c = int(getattr(buyer, 'executed_count', 0) or 0) if buyer else 0
                except (TypeError, ValueError):
                    exec_c = 0
                try:
                    vol = float(getattr(w, 'current_volume', 0) or 0)
                except (TypeError, ValueError):
                    vol = 0.0
                # ctx volume이 비어 있으면 buyer 체결로 추정
                if vol <= 0.00001 and buyer is not None:
                    try:
                        fills = getattr(buyer, 'executed_orders', None) or []
                        if fills:
                            vol = 1.0  # 보유 있음으로 간주
                    except Exception:
                        pass
                if exec_c > 0 or vol > 0.00001:
                    # 매도 끝날 때까지 유지 — 추가 매수만 차단
                    if buyer is not None:
                        already = bool(getattr(buyer, '_buys_halted', False))
                        if already:
                            # 이미 중단됨 — 잔여 pending만 조용히 정리, 로그 스팸 금지
                            if buyer.pending_orders:
                                try:
                                    buyer._cancel_pending_sync()
                                except Exception:
                                    pass
                            continue
                        try:
                            buyer._buys_halted = True
                            buyer._buys_halted_by_topn = True
                            buyer.is_active = False
                            if buyer.pending_orders:
                                buyer._cancel_pending_sync()
                        except Exception:
                            try:
                                from . import engine as eng
                                eng.OrderCanceler().cancel_buy_orders_for_symbol(
                                    su, verify=False)
                            except Exception:
                                pass
                        try:
                            from . import engine as eng
                            eng.print_log(
                                eng.LogLevel.WARNING,
                                f"AUTO TopN 제외 {su}: 추가매수 중단 "
                                f"(보유/체결 유지·매도 계속, "
                                f"유예{int(grace)}s 경과)")
                        except Exception:
                            pass
                    continue
                self.force_end_worker(
                    w, reason=f'AUTO TopN 제외 — 슬롯회수 ({su})')
                killed += 1
        return killed

    def wake_gate_pass_buyers(self, pass_symbols) -> int:
        """이미 활성인데 MA통과인데 is_active=False인 워커 강제 재활성.
        TopN halt였다가 복귀한 경우에도 매수 재개."""
        want = {str(s).upper() for s in (pass_symbols or [])}
        if not want:
            return 0
        woken = 0
        with self._lock:
            for w in list(self._workers.values()):
                if not w or not w.alive:
                    continue
                su = str(w.symbol or '').upper()
                if su not in want:
                    continue
                buyer = w.dynamic_buyer
                if buyer is None:
                    continue
                if getattr(buyer, '_cycle_ended', False):
                    continue
                # TopN 복귀 halt 해제 (다른 사유 halt는 유지)
                if getattr(buyer, '_buys_halted', False):
                    if getattr(buyer, '_buys_halted_by_topn', False):
                        buyer._buys_halted = False
                        buyer._buys_halted_by_topn = False
                    else:
                        continue
                if getattr(buyer, '_first_order_timed_out', False):
                    continue
                try:
                    from . import focus as focus_mod
                    if focus_mod.should_block_buy(su):
                        continue
                except Exception:
                    pass
                n_plan = len(getattr(buyer, 'active_planned_orders', None) or [])
                exec_c = int(getattr(buyer, 'executed_count', 0) or 0)
                if n_plan > 0 and exec_c >= n_plan:
                    continue
                buyer._ma_gate_paused = False
                buyer._skip_ma_gate = False
                if not buyer.is_active:
                    buyer.is_active = True
                    woken += 1
                    try:
                        from . import engine as eng
                        eng.print_log(
                            eng.LogLevel.SUCCESS,
                            f"게이트 wake 매수재개 {su} "
                            f"(exec={exec_c}/{n_plan} pending="
                            f"{len(buyer.pending_orders or [])})")
                    except Exception:
                        pass
                # pending 없으면 즉시 다음 호가 시도
                if buyer.is_active and not (buyer.pending_orders or []):
                    try:
                        buyer.check_and_continue(None)
                    except Exception:
                        pass
        return woken

    def try_spawn(self, symbol: str = None) -> Optional[CoinWorker]:
        """Spawn a worker if budget gate allows and runner is set.
        symbol 지정(감시모듈) 시: PARALLEL_WORKERS 여유만 보면 됨
        (할당슬롯 축소 때문에 신규 MA통과 코인이 막히던 버그 수정)."""
        if self._stop.is_set() or not self.cycle_runner:
            return None
        from . import focus as focus_mod
        from .config import PARALLEL_WORKERS
        focus = focus_mod.get_focus_symbol()
        hard_cap = int(PARALLEL_WORKERS)
        with self._lock:
            self._reap_unlocked()
            try:
                focus_mod.sync_focus_from_workers(list(self._workers.values()))
                focus = focus_mod.get_focus_symbol()
            except Exception:
                pass
            if focus:
                try:
                    focus_mod.apply_focus_budget(self.pool, focus)
                except Exception:
                    pass

            # spawn 용량은 항상 PARALLEL_WORKERS
            self.max_workers = hard_cap
            try:
                self.pool.max_workers = hard_cap
            except Exception:
                pass

            if self.active_count() >= hard_cap:
                return None

            sym = symbol
            if not sym and self.symbol_picker:
                if focus:
                    sym = focus
                else:
                    sym = self.symbol_picker()
            if not sym:
                return None
            su = str(sym).upper()

            if focus and su != focus:
                return None
            if su in set(s.upper() for s in self.active_symbols()):
                return None

            # 최초×50% 하한: 신규 spawn 금지 → 기존 코인 dump
            if self.pool.is_new_buy_halted():
                self.pool.assist_idle_to_workers()
                try:
                    from . import engine as eng
                    snap = self.pool.snapshot()
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        f"워커 spawn 거부({su}): 신규매수중단 "
                        f"free={int(snap.get('free_krw', 0)):,} "
                        f"< 최초×50%({int(snap.get('reserve_floor', 0)):,})")
                except Exception:
                    pass
                return None

            # picker 경로만 alloc 슬롯 제한. 감시 직접 spawn은 hard_cap만.
            if symbol is None:
                try:
                    alloc_n = self.pool.alloc_slots()
                except Exception:
                    alloc_n = hard_cap
                if self.active_count() >= alloc_n and not focus:
                    return None

            if self.active_count() > 0 and not self.pool.can_spawn():
                if not (focus and su == focus):
                    snap = self.pool.snapshot()
                    try:
                        from . import engine as eng
                        eng.print_log(
                            eng.LogLevel.WARNING,
                            f"워커 spawn 보류({su}): free={int(snap.get('free_krw', 0)):,} "
                            f"(min주문) active={self.active_count()}/{hard_cap}")
                    except Exception:
                        pass
                    self.pool.assist_idle_to_workers()
                    return None

            wid = f"w-{uuid.uuid4().hex[:8]}"
            wb = self.pool.register_worker(wid, symbol=su)
            if wb is None:
                self.pool.assist_idle_to_workers()
                try:
                    from . import engine as eng
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        f"워커 register 실패({su}): active={self.active_count()} "
                        f"cap={hard_cap} alloc_slots={self.pool.alloc_slots()} "
                        f"focus={focus}")
                except Exception:
                    pass
                return None
            worker = CoinWorker(worker_id=wid, symbol=su, budget=wb)
            self._workers[wid] = worker
            t = threading.Thread(
                target=self._run_worker,
                args=(worker,),
                name=f"coin-{su}-{wid}",
                daemon=True,
            )
            worker.thread = t
            t.start()
            self.sync_rate_limiter()
            try:
                from . import engine as eng
                eng.print_log(
                    eng.LogLevel.SUCCESS,
                    f"워커 spawn OK {su} id={wid} "
                    f"({self.active_count()}/{hard_cap}) "
                    f"alloc={int(wb.allocated):,}원")
            except Exception:
                pass
            return worker

    def _run_worker(self, worker: CoinWorker):
        try:
            self.cycle_runner(worker)
        except BaseException as e:
            worker.error = e
            try:
                from . import engine as eng
                eng.print_log(
                    eng.LogLevel.ERROR,
                    f"[worker {worker.worker_id} {worker.symbol}] {e}")
            except Exception:
                pass
        finally:
            worker.done.set()
            try:
                from . import focus as focus_mod
                focus_mod.clear_focus_if(
                    worker.symbol, reason='워커종료')
            except Exception:
                pass
            try:
                self.pool.release_reserved(worker.worker_id)
                self.pool.unregister_worker(worker.worker_id)
            except Exception:
                pass
            with self._lock:
                self._workers.pop(worker.worker_id, None)
            self.sync_rate_limiter()

    def _reap_unlocked(self):
        dead = [wid for wid, w in self._workers.items() if w.done.is_set()]
        for wid in dead:
            self._workers.pop(wid, None)

    def tick(self):
        """Supervisor heartbeat — reap, assist, try fill empty slots."""
        if self._stop.is_set():
            return
        with self._lock:
            self._reap_unlocked()
            try:
                from . import focus as focus_mod
                focus_mod.sync_focus_from_workers(list(self._workers.values()))
                focus = focus_mod.get_focus_symbol()
                if focus:
                    focus_mod.apply_focus_budget(self.pool, focus)
            except Exception:
                pass
        # refresh is caller's job (free_krw); assist if under capacity but broke
        if self.active_count() < self.max_workers:
            if self.pool.is_new_buy_halted():
                self.pool.assist_idle_to_workers()
            elif self.pool.can_spawn():
                self.try_spawn()
            elif self.active_count() > 0:
                self.pool.assist_idle_to_workers()
        elif self.pool.is_new_buy_halted():
            self.pool.assist_idle_to_workers()

    def stop_all(self, timeout: float = 30.0):
        self._stop.set()
        with self._lock:
            workers = list(self._workers.values())
        for w in workers:
            w.stop.set()
        deadline = time.time() + timeout
        for w in workers:
            remain = max(0.0, deadline - time.time())
            if w.thread:
                w.thread.join(timeout=remain)

    def status(self) -> dict:
        with self._lock:
            return {
                'max_workers': self.max_workers,
                'alloc_pct': alloc_pct_for(
                    max(1, self.active_count() or self.max_workers)),
                'active': [
                    {
                        'id': w.worker_id,
                        'symbol': w.symbol,
                        'alive': w.alive,
                        'allocated': getattr(w.budget, 'allocated', 0),
                    }
                    for w in self._workers.values()
                ],
                'pool': self.pool.snapshot(),
            }
