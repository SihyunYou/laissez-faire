# -*- coding: utf-8 -*-
"""Per-module worker threads — buy / sell / command / market run independently.

Each ModuleWorker owns a daemon thread and ticks on its own schedule.
Shared CycleContext carries symbol/buyer/seller; order mutations take ctx.lock.
"""
from __future__ import annotations

import threading
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class CycleContext:
    """Shared state for one trading cycle — modules read/write under lock."""
    symbol: str
    dynamic_buyer: Any
    sell_controller: Any
    trading_manager: Any
    profit_targets: list
    private_ws: Any
    cycle_timeout: float = 86400.0
    cycle_start_mono: float = field(default_factory=time.time)

    lock: threading.RLock = field(default_factory=threading.RLock)
    stop: threading.Event = field(default_factory=threading.Event)
    trading_completed: threading.Event = field(default_factory=threading.Event)
    command_changed: threading.Event = field(default_factory=threading.Event)

    # cross-module signals
    last_buy_stop: threading.Event = field(default_factory=threading.Event)
    stop_loss: threading.Event = field(default_factory=threading.Event)
    dust_exit: threading.Event = field(default_factory=threading.Event)

    cached_price: float = 0.0
    current_volume: float = 0.0


class ModuleWorker:
    """Independent threaded module — subclass and override tick()."""

    name: str = "module"
    interval: float = 0.01

    def __init__(self, ctx: CycleContext):
        self.ctx = ctx
        self._thread: Optional[threading.Thread] = None
        self._error: Optional[BaseException] = None

    def start(self):
        self._thread = threading.Thread(
            target=self._loop, name=f"mod-{self.name}", daemon=True)
        self._thread.start()

    def join(self, timeout=None):
        if self._thread:
            self._thread.join(timeout=timeout)

    def _loop(self):
        while not self.ctx.stop.is_set() and not self.ctx.trading_completed.is_set():
            try:
                self.tick()
            except Exception as e:
                self._error = e
                try:
                    from . import engine as eng
                    eng.print_log(eng.LogLevel.ERROR, f"[{self.name}] {e}")
                except Exception:
                    traceback.print_exc()
            # stop wait with interval (wake early on stop)
            self.ctx.stop.wait(self.interval)

    def tick(self):
        raise NotImplementedError


class MarketModule(ModuleWorker):
    """Refresh cached price / volume snapshot for other modules."""

    name = "market"
    interval = 0.01

    def tick(self):
        from . import engine as eng

        ctx = self.ctx
        px = eng.RealMarketData.get_current_price(ctx.symbol) or 0.0
        pw = ctx.private_ws
        if pw is not None and getattr(pw, "_is_initialized", False) and pw.is_connected:
            bal, locked, _ = pw.get_symbol_info(ctx.symbol)
        else:
            bal, locked, _ = eng.AccountChecker().get_symbol_info(ctx.symbol)
        if bal < 0:
            bal, locked = 0.0, 0.0
        with ctx.lock:
            ctx.cached_price = float(px or 0.0)
            ctx.current_volume = float(bal) + float(locked)


class CommandModule(ModuleWorker):
    """Watch command.txt without blocking buy/sell hot paths."""

    name = "command"
    interval = 0.2

    def tick(self):
        ctx = self.ctx
        tm = ctx.trading_manager
        if tm.check_command_file():
            ctx.command_changed.set()


class BuyModule(ModuleWorker):
    """Buy ladder — independent of sell thread."""

    name = "buy"
    interval = 0.01

    def tick(self):
        from . import engine as eng

        ctx = self.ctx
        buyer = ctx.dynamic_buyer
        sell = ctx.sell_controller
        tm = ctx.trading_manager

        with ctx.lock:
            if (time.time() - ctx.cycle_start_mono) > ctx.cycle_timeout:
                eng.print_log(eng.LogLevel.WARNING,
                              f"Trading cycle timeout after {ctx.cycle_timeout} seconds")
                if buyer.is_active or buyer.pending_orders:
                    buyer.stop_trading()
                else:
                    eng.cancel_buy_orders_async()
                ctx.trading_completed.set()
                ctx.stop.set()
                return

            n_plan = len(buyer.active_planned_orders or [])
            buys_done = n_plan > 0 and buyer.executed_count >= n_plan
            if not buyer.is_active and not buys_done:
                buyer.is_active = True
                eng.print_log(eng.LogLevel.WARNING,
                              f"Buy ladder resume: executed="
                              f"{buyer.executed_count}/{n_plan}")

            if buyer.is_active:
                buyer.check_and_continue(ctx.cached_price or None)
                if n_plan >= 2 and buyer.executed_count == n_plan - 1:
                    if sell.check_last_buy_stop(
                            ctx.symbol, tm, ctx.cached_price or None,
                            dynamic_buyer=buyer):
                        eng.print_log(eng.LogLevel.WARNING,
                                      "Stop loss at last-buy stage — "
                                      "skipping final buy, selling all holdings")
                        buyer.stop_trading()
                        ctx.last_buy_stop.set()
                        ctx.trading_completed.set()
                        ctx.stop.set()


class BuyWatchdogModule(ModuleWorker):
    """1분마다 매수주문 존재 여부 점검 — 매도만 남는 간헐 버그 사후 복구."""

    name = "buy-watchdog"
    interval = 60.0

    def tick(self):
        from . import engine as eng

        ctx = self.ctx
        # 사이클 시작 직후 1분은 정상 배치 대기 — 오탐 방지
        if (time.time() - ctx.cycle_start_mono) < 60.0:
            return
        if ctx.trading_completed.is_set() or ctx.stop.is_set():
            return
        with ctx.lock:
            buyer = ctx.dynamic_buyer
            if buyer is None:
                return
            try:
                buyer.watchdog_ensure_buy_orders()
            except Exception as e:
                eng.print_log(eng.LogLevel.WARNING,
                              f"buy-watchdog error: {str(e)[:120]}")


class BuyFillTimeoutWatchdogModule(ModuleWorker):
    """5초마다 미체결 매수 타임아웃 점검 — 3초 재호가 누락/무한대기 사후 복구."""

    name = "fill-timeout-watchdog"
    interval = 5.0

    def tick(self):
        from . import engine as eng

        ctx = self.ctx
        if ctx.trading_completed.is_set() or ctx.stop.is_set():
            return
        # 배치 직후 즉시 오탐 방지
        if (time.time() - ctx.cycle_start_mono) < 3.0:
            return
        with ctx.lock:
            buyer = ctx.dynamic_buyer
            if buyer is None:
                return
            try:
                buyer.watchdog_fill_timeout()
            except Exception as e:
                eng.print_log(eng.LogLevel.WARNING,
                              f"fill-timeout-watchdog error: {str(e)[:120]}")


class SellModule(ModuleWorker):
    """Sell manager — independent of buy thread."""

    name = "sell"
    interval = 0.01

    def tick(self):
        from . import engine as eng
        from .config import MIN_HOLDING_VOLUME

        ctx = self.ctx
        buyer = ctx.dynamic_buyer
        sell = ctx.sell_controller
        tm = ctx.trading_manager
        pw = ctx.private_ws

        with ctx.lock:
            vol = ctx.current_volume
            sell_tracking = (
                tm.sell_orders_placed or sell.has_open_sell_orders())
            need_fast = (
                vol > MIN_HOLDING_VOLUME
                and (not tm.sell_orders_placed
                     or (buyer.executed_count > sell._sell_placed_at_buy_count)))

            if vol > 0.00001 and not tm.buy_orders_executed:
                tm.mark_buy_orders_executed()
                eng.print_log(eng.LogLevel.SUCCESS,
                              f"Buy orders executed - Holdings: {vol:.6f}")

            if vol > 0.00001 or sell_tracking or need_fast:
                done = sell.manage_sell_orders(
                    ctx.symbol, ctx.profit_targets, tm, 0, buyer)
                if done:
                    # REST 전량확인은 _complete_cycle_on_sell_done 내부
                    eng.print_log(eng.LogLevel.SELL_SUCCESS,
                                  "Trading completed (sell orders executed)")
                    ctx.trading_completed.set()
                    ctx.stop.set()
                    return
                if getattr(sell, "dust_holdings", False):
                    eng.print_log(eng.LogLevel.WARNING,
                                  "먼지진 감지 — 전량확인 후 사이클 종료")
                    if sell._complete_cycle_on_sell_done(
                            tm, buyer, force=True,
                            profit_percentages=ctx.profit_targets):
                        ctx.dust_exit.set()
                        ctx.trading_completed.set()
                        ctx.stop.set()
                        return
                    # 전량 미완 — 종료하지 않고 재매도 유지
                    return
            elif tm.sell_orders_placed:
                # WS flat만으로 종료 금지 — REST 전량 확인
                if not eng.rest_holdings_cleared(ctx.symbol):
                    eng.print_log(eng.LogLevel.WARNING,
                                  "WS flat but REST holdings remain — keep selling")
                    sell.place_sell_orders(
                        ctx.symbol, ctx.profit_targets, buyer,
                        volume_hint=None, force_replace=False)
                    return
                eng.print_log(eng.LogLevel.SUCCESS,
                              "Holdings flat (REST) — completing cycle")
                if sell._complete_cycle_on_sell_done(
                        tm, buyer, force=True,
                        profit_percentages=ctx.profit_targets):
                    ctx.trading_completed.set()
                    ctx.stop.set()
                    return

            if sell.check_stop_loss(ctx.symbol, tm, buyer):
                eng.print_log(eng.LogLevel.WARNING, "Stop loss triggered")
                if buyer.is_active:
                    buyer.is_active = False
                    buyer.pending_orders.clear()
                    buyer._reset_runtime_indexes()
                try:
                    eng.cancel_buy_orders_sync(verify=True)
                except Exception:
                    eng.cancel_buy_orders_async()
                ctx.stop_loss.set()
                ctx.trading_completed.set()
                ctx.stop.set()
                return

        if (pw is not None and getattr(pw, "_is_initialized", False)
                and ((buyer.is_active and buyer.pending_orders)
                     or sell.has_open_sell_orders())):
            pw.wait_fill(0.01)


class ParallelRuntime:
    """Start/stop a set of ModuleWorkers for one cycle."""

    def __init__(self, ctx: CycleContext, modules: list[ModuleWorker]):
        self.ctx = ctx
        self.modules = modules

    def start(self):
        from . import engine as eng
        names = ", ".join(m.name for m in self.modules)
        eng.print_log(eng.LogLevel.SUCCESS,
                      f"ParallelRuntime start — modules=[{names}]")
        for m in self.modules:
            m.start()

    def wait(self):
        """Block until cycle completes or stop."""
        while not self.ctx.trading_completed.wait(timeout=0.05):
            if self.ctx.stop.is_set():
                break
            for m in self.modules:
                if m._error is not None:
                    self.ctx.stop.set()
                    raise m._error
        self.ctx.stop.set()
        for m in self.modules:
            m.join(timeout=2.0)

    def stop(self):
        self.ctx.stop.set()
        self.ctx.trading_completed.set()


def run_managed_cycle(
        symbol,
        dynamic_buyer,
        sell_controller,
        trading_manager,
        profit_targets,
        private_ws,
        cycle_timeout=86400.0) -> CycleContext:
    """Run buy/sell/market/command modules concurrently until cycle ends."""
    ctx = CycleContext(
        symbol=symbol,
        dynamic_buyer=dynamic_buyer,
        sell_controller=sell_controller,
        trading_manager=trading_manager,
        profit_targets=list(profit_targets),
        private_ws=private_ws,
        cycle_timeout=cycle_timeout,
        cycle_start_mono=time.time(),
    )
    modules = [
        MarketModule(ctx),
        CommandModule(ctx),
        BuyModule(ctx),
        BuyWatchdogModule(ctx),
        BuyFillTimeoutWatchdogModule(ctx),
        SellModule(ctx),
    ]
    rt = ParallelRuntime(ctx, modules)
    rt.start()
    try:
        rt.wait()
    finally:
        rt.stop()
    return ctx
