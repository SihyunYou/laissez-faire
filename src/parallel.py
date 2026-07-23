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
    """command.txt 감시 — AUTO_SELECT 기본 시 심볼 선정에 미사용 (no-op).
    EXIT 등 특수 명령만 남겨둘 여지. 워커 강제종료는 하지 않음."""

    name = "command"
    interval = 1.0

    def tick(self):
        try:
            from .config import AUTO_SELECT
            if AUTO_SELECT:
                return
        except Exception:
            return
        # 레거시: AUTO_SELECT=False 일 때만 command.txt 제외 처리
        from . import engine as eng

        ctx = self.ctx
        tm = getattr(eng, 'trading_manager', None) or ctx.trading_manager
        if tm is None:
            return
        prev = list(getattr(tm, 'current_command_symbols', None) or [])
        changed = bool(tm.check_command_file())
        now = list(getattr(tm, 'current_command_symbols', None) or [])
        if changed or now != prev:
            ctx.command_changed.set()
            su = str(ctx.symbol or '').upper()
            keep = {str(s).upper() for s in now if s}
            if su and keep and su not in keep:
                buyer = ctx.dynamic_buyer
                exec_c = int(getattr(buyer, 'executed_count', 0) or 0) if buyer else 0
                vol = float(getattr(ctx, 'current_volume', 0) or 0)
                if exec_c <= 0 and vol <= 0.00001:
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        f"command.txt 제외 — 워커 사이클 종료 {su}")
                    if buyer is not None:
                        buyer._cycle_ended = True
                        buyer.is_active = False
                    ctx.trading_completed.set()
                    ctx.stop.set()
                elif buyer is not None:
                    buyer._buys_halted = True
                    buyer.is_active = False
                    try:
                        buyer._cancel_pending_sync()
                    except Exception:
                        pass
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        f"command.txt 제외 {su}: 추가매수 중단 (매도 유지)")


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

        do_continue = False
        n_plan = 0
        with ctx.lock:
            if (time.time() - ctx.cycle_start_mono) > ctx.cycle_timeout:
                eng.print_log(eng.LogLevel.WARNING,
                              f"Trading cycle timeout after {ctx.cycle_timeout} seconds")
                if buyer.is_active or buyer.pending_orders:
                    buyer.stop_trading()
                else:
                    try:
                        eng.OrderCanceler().cancel_buy_orders_for_symbol(
                            ctx.symbol, verify=False)
                    except Exception:
                        pass
                ctx.trading_completed.set()
                ctx.stop.set()
                return

            n_plan = len(buyer.active_planned_orders or [])
            buys_done = n_plan > 0 and buyer.executed_count >= n_plan
            # 사이클 종료/강제정지 후에는 사다리 재개 금지
            # (종료 시 is_active=False → 여기서 다시 True → 6레벨 재POST 버그)
            if getattr(buyer, '_cycle_ended', False):
                return
            if ctx.trading_completed.is_set() or ctx.stop.is_set():
                return
            if getattr(buyer, '_buys_halted', False):
                # 매도후 매수중단인데 보유0이면 슬롯 좀비 — 사이클 강제종료
                vol0 = float(getattr(ctx, 'current_volume', 0) or 0) <= 0.00001
                if vol0 and not getattr(buyer, '_cycle_ended', False):
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        f"buys_halted+보유0 — 사이클 강제종료 {ctx.symbol}")
                    try:
                        sell._complete_cycle_on_sell_done(
                            tm, buyer, force=True,
                            profit_percentages=ctx.profit_targets)
                    except Exception:
                        buyer._cycle_ended = True
                    ctx.trading_completed.set()
                    ctx.stop.set()
                return
            # 고전 첫주문 타임아웃 → 심볼 사이클 종료 (watchdog/이전 tick)
            if getattr(buyer, '_first_order_timed_out', False):
                eng.print_log(
                    eng.LogLevel.WARNING,
                    "First order timeout - stopping trading cycle")
                buyer._cycle_ended = True
                ctx.trading_completed.set()
                ctx.stop.set()
                return

            # ★ 6라운드+ 집중모드: 타코인 추가매수 금지 + L8(assist) 편입
            from . import focus as focus_mod
            from .budget import budget_pool
            try:
                if focus_mod.buyer_in_deep_rounds(buyer):
                    try:
                        buyer.ensure_assist_level()
                    except Exception:
                        pass
                    focus_mod.set_focus_symbol(
                        ctx.symbol,
                        reason=f'L{focus_mod.buyer_deep_level(buyer)}')
                    focus_mod.apply_focus_budget(budget_pool, ctx.symbol)
            except Exception:
                pass

            if focus_mod.should_block_buy(ctx.symbol):
                # 타코인: 미체결 매수만 취소, 매도 모듈은 그대로
                if buyer.pending_orders:
                    try:
                        buyer._cancel_pending_sync()
                    except Exception:
                        try:
                            eng.OrderCanceler().cancel_buy_orders_for_symbol(
                                ctx.symbol, verify=False)
                        except Exception:
                            pass
                    buyer._clear_pending_tracking_only()
                if buyer.is_active:
                    buyer.is_active = False
                    now_f = time.time()
                    last_f = float(getattr(self, '_focus_block_log_ts', 0) or 0)
                    if now_f - last_f >= 10.0:
                        self._focus_block_log_ts = now_f
                        eng.print_log(
                            eng.LogLevel.WARNING,
                            f"집중모드 — {ctx.symbol} 추가매수 금지 "
                            f"(focus={focus_mod.get_focus_symbol()}, "
                            f"매도·보유 유지)")
                return

            # ★ MA60 = 진입 검증장치만.
            # 매도주문/체결 진행 중이면 동적슬라이딩 매수 계속 (무한대기 금지)
            gate_ok, gate_info = eng.RealMarketData.check_tick_ma_gate(
                ctx.symbol)
            in_trade = bool(
                tm.sell_orders_placed
                or tm.buy_orders_executed
                or sell.has_open_sell_orders()
                or buyer.executed_count > 0
                or (hasattr(buyer, '_has_any_level_fill')
                    and buyer._has_any_level_fill())
                or float(getattr(ctx, 'current_volume', 0) or 0) > 0.00001
            )
            if in_trade:
                # 포지션/매도 진행 중 — MA pause 해제, 사다리·plan_shift 유지
                buyer._ma_gate_paused = False
                buyer._skip_ma_gate = True
                if (not buyer.is_active
                        and not getattr(buyer, '_cycle_ended', False)
                        and not getattr(buyer, '_buys_halted', False)
                        and not buys_done):
                    buyer.is_active = True
                # ★ 매도만 남은 고착: partial 풀고 다음 레벨 즉시 재개
                if (not buys_done
                        and not (buyer.pending_orders or [])
                        and not getattr(buyer, '_cycle_ended', False)):
                    try:
                        buyer._unlock_stuck_partial_levels()
                    except Exception:
                        pass
                    now_u = time.time()
                    last_u = float(getattr(self, '_ladder_resume_log_ts', 0) or 0)
                    if now_u - last_u >= 5.0:
                        self._ladder_resume_log_ts = now_u
                        eng.print_log(
                            eng.LogLevel.WARNING,
                            f"ladder resume {ctx.symbol}: "
                            f"매도진행·매수0 — 다음레벨 재배치 "
                            f"(exec={buyer.executed_count}/{n_plan} "
                            f"partial={sorted(buyer.partial_levels or [])})")
            elif not gate_ok:
                now = time.time()
                last = float(getattr(self, '_ma_block_log_ts', 0) or 0)
                if now - last >= 5.0:
                    self._ma_block_log_ts = now
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        f"HybridMA gate wait entry {ctx.symbol}: "
                        f"now={gate_info.get('last_price')} "
                        f"hybrid={(gate_info.get('hybrid') or gate_info.get('ma60'))} "
                        f"tick={gate_info.get('ma_tick')} min={gate_info.get('ma_min')} "
                        f"candles={gate_info.get('candle_count', 0)}/"
                        f"{gate_info.get('min_candle_count', 0)} "
                        f"— 진입대기 (exec={buyer.executed_count}/{n_plan} "
                        f"pending={len(buyer.pending_orders or [])})")
                buyer.is_active = False
                buyer._ma_gate_paused = True
                buyer._skip_ma_gate = False
                try:
                    if buyer.pending_orders:
                        buyer._cancel_pending_sync()
                except Exception:
                    try:
                        eng.OrderCanceler().cancel_buy_orders_for_symbol(
                            ctx.symbol, verify=False)
                    except Exception as e:
                        eng.print_log(
                            eng.LogLevel.WARNING,
                            f"MA60 cancel buys failed: {str(e)[:80]}")
                return
            else:
                buyer._skip_ma_gate = False

            # MA 통과 재개 — 진입만. 포지션 중이면 위에서 이미 active
            if not in_trade and gate_ok:
                was_paused = bool(getattr(buyer, '_ma_gate_paused', False))
                if was_paused:
                    buyer._ma_gate_paused = False
                    eng.print_log(
                        eng.LogLevel.SUCCESS,
                        f"HybridMA gate PASS resume {ctx.symbol}: "
                        f"now={gate_info.get('last_price')} "
                        f"< hybrid={(gate_info.get('hybrid') or gate_info.get('ma60'))} "
                        f"tick={gate_info.get('ma_tick')} min={gate_info.get('ma_min')} "
                        f"exec={buyer.executed_count}/{n_plan}")
                try:
                    adopted = buyer._adopt_exchange_bids_if_any()
                    if adopted:
                        eng.print_log(
                            eng.LogLevel.WARNING,
                            f"MA60 resume: 거래소 bid {adopted}건 재흡수 "
                            f"— 사다리 계속")
                        # ★ 예전에 return 해서 매수 영구 스킵되던 버그 수정
                except Exception:
                    pass
                # paused 아니어도 진입게이트 통과면 매수 재활성
                if (not getattr(buyer, '_cycle_ended', False)
                        and not getattr(buyer, '_buys_halted', False)
                        and not getattr(buyer, '_first_order_timed_out', False)
                        and not buys_done
                        and buyer._buys_allowed()):
                    if not buyer.is_active:
                        buyer.is_active = True
                        if not was_paused:
                            eng.print_log(
                                eng.LogLevel.SUCCESS,
                                f"HybridMA gate PASS wake {ctx.symbol}: "
                                f"now={gate_info.get('last_price')} "
                                f"< hybrid={(gate_info.get('hybrid') or gate_info.get('ma60'))} "
                                f"tick={gate_info.get('ma_tick')} min={gate_info.get('ma_min')} "
                                f"— 매수 재활성")

            # ★ 제거됨: "Buy ladder resume" 맹목 is_active=True (무한매수 원흉)

            if buyer.is_active and buyer._buys_allowed():
                try:
                    if len(buyer.pending_orders or []) > eng.MAX_OPEN_BUYS_PER_WORKER:
                        buyer._enforce_max_open_buys()
                except Exception:
                    pass
                do_continue = True

        # ★ 주문 POST는 ctx.lock 밖에서 — 매도 스레드와 상호 블로킹 제거
        if do_continue:
            buyer.check_and_continue(ctx.cached_price or None)
            with ctx.lock:
                # 고전: 첫주문 타임아웃 → 이 심볼 사이클 종료
                if getattr(buyer, '_first_order_timed_out', False):
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        "First order timeout - stopping trading cycle")
                    buyer._cycle_ended = True
                    ctx.trading_completed.set()
                    ctx.stop.set()
                    return
                if n_plan >= 2 and buyer.executed_count == n_plan - 1:
                    # L8 assist 라운드는 타코인 매도자금 투입용 — last-buy stop으로 스킵 금지
                    final = (buyer.active_planned_orders or [None])[-1] or {}
                    if final.get('assist'):
                        pass
                    elif sell.check_last_buy_stop(
                            ctx.symbol, tm, ctx.cached_price or None,
                            dynamic_buyer=buyer):
                        eng.print_log(
                            eng.LogLevel.WARNING,
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
        # MA60 위면 워치독 재매수 금지 — 단 매도/보유 진행 중이면 허용
        with ctx.lock:
            buyer = ctx.dynamic_buyer
            if buyer is None:
                return
            tm = ctx.trading_manager
            sell = ctx.sell_controller
            in_trade = bool(
                getattr(buyer, '_skip_ma_gate', False)
                or (tm and (tm.sell_orders_placed or tm.buy_orders_executed))
                or (sell and sell.has_open_sell_orders())
                or buyer.executed_count > 0
                or float(getattr(ctx, 'current_volume', 0) or 0) > 0.00001
            )
        if not in_trade:
            gate_ok, _ = eng.RealMarketData.check_tick_ma_gate(ctx.symbol)
            if not gate_ok:
                return
        with ctx.lock:
            buyer = ctx.dynamic_buyer
            if buyer is None:
                return
            if getattr(buyer, '_buys_halted', False) or not buyer._buys_allowed():
                return
            try:
                buyer.watchdog_ensure_buy_orders()
            except Exception as e:
                eng.print_log(eng.LogLevel.WARNING,
                              f"buy-watchdog error: {str(e)[:120]}")


class BuyFillTimeoutWatchdogModule(ModuleWorker):
    """0.25초마다 고전 첫주문 타임아웃 점검 (취소+사이클중단)."""

    name = "fill-timeout-watchdog"
    interval = 0.25

    def tick(self):
        from . import engine as eng

        ctx = self.ctx
        if ctx.trading_completed.is_set() or ctx.stop.is_set():
            return
        if (time.time() - ctx.cycle_start_mono) < 0.5:
            return
        with ctx.lock:
            buyer = ctx.dynamic_buyer
            if buyer is None:
                return
            try:
                buyer.watchdog_fill_timeout()
                if getattr(buyer, '_first_order_timed_out', False):
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        "First order timeout - stopping trading cycle")
                    buyer._cycle_ended = True
                    ctx.trading_completed.set()
                    ctx.stop.set()
            except Exception as e:
                eng.print_log(eng.LogLevel.WARNING,
                              f"fill-timeout-watchdog error: {str(e)[:120]}")


class SellOrphanWatchdogModule(ModuleWorker):
    """최후수단: 매도가능 보유가 있는데 ask가 없으면 강제 전량매도.

    SellModule/avg-sell이 빠져도 여기서 복구.
    연속 확인 후에만 POST (WS 잔고 지연 오탐 완화).
    """

    name = "sell-orphan-watchdog"
    interval = 2.0
    # 사이클 시작 직후 유예 (정상 매도 fire 시간)
    _grace_s = 5.0
    # 연속 몇 틱 확인해야 강제 POST
    _need_streak = 2

    def __init__(self, ctx: CycleContext):
        super().__init__(ctx)
        self._orphan_streak = 0
        self._last_force_t = 0.0
        self._last_log_t = 0.0

    def tick(self):
        from . import engine as eng
        from .config import MIN_ORDER_AMOUNT, MIN_HOLDING_VOLUME

        ctx = self.ctx
        if ctx.trading_completed.is_set() or ctx.stop.is_set():
            return
        if (time.time() - ctx.cycle_start_mono) < self._grace_s:
            return

        with ctx.lock:
            buyer = ctx.dynamic_buyer
            sell = ctx.sell_controller
            tm = ctx.trading_manager
            if sell is None or tm is None:
                return
            # 이미 매도 진행 중이면 OK
            if sell.has_open_sell_orders():
                self._orphan_streak = 0
                return
            if getattr(sell, '_sell_placing', False):
                return

        symbol = ctx.symbol
        try:
            # 1) WS/캐시
            bal, locked, avg = sell._get_cached_symbol_info(symbol)
            if bal < 0:
                bal, locked, avg = 0.0, 0.0, 0.0
            total = max(float(bal), 0.0) + max(float(locked), 0.0)
            # 2) 캐시 빈약하면 REST
            if total < MIN_HOLDING_VOLUME or not sell._holding_sellable(symbol, total):
                bal2, loc2, avg2 = sell._get_fresh_symbol_info(symbol)
                if bal2 >= 0:
                    total = max(float(bal2), 0.0) + max(float(loc2), 0.0)
                    if avg2 and avg2 > 0:
                        avg = float(avg2)
            # 3) buyer 체결 힌트 (잔고 지연 대비)
            hint = 0.0
            if buyer is not None:
                try:
                    for e in (getattr(buyer, 'executed_orders', None) or []):
                        hint += float(e.get('volume') or 0)
                except Exception:
                    hint = 0.0
                if int(getattr(buyer, 'executed_count', 0) or 0) <= 0 and hint <= 0:
                    # 매수 체결 이력도 없으면 관여하지 않음
                    if total < MIN_HOLDING_VOLUME:
                        self._orphan_streak = 0
                        return

            vol_use = max(total, hint)
            if vol_use < MIN_HOLDING_VOLUME:
                self._orphan_streak = 0
                return
            if not sell._holding_sellable(symbol, vol_use):
                # 먼지진
                self._orphan_streak = 0
                return

            # 거래소에 ask가 이미 있으면 흡수만
            if sell._adopt_open_ask_tracking(symbol):
                tm.mark_sell_orders_placed()
                self._orphan_streak = 0
                return
            if sell._open_sell_count() > 0:
                self._orphan_streak = 0
                return

            self._orphan_streak += 1
            now = time.time()
            if self._orphan_streak < self._need_streak:
                if now - self._last_log_t >= 5.0:
                    self._last_log_t = now
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        f"sell-orphan 감시 {symbol}: 보유≈{vol_use:.8f} "
                        f"ask=0 streak={self._orphan_streak}/{self._need_streak}")
                return

            # 강제 POST 쿨다운 (연타 방지)
            if now - self._last_force_t < 3.0:
                return
            self._last_force_t = now

            base = float(avg or 0)
            if base <= 0:
                base = float(sell._resolve_sell_base_price(
                    symbol, None, buyer) or 0)
            if base <= 0:
                try:
                    px = float(eng.RealMarketData.get_current_price(symbol) or 0)
                    if px > 0:
                        base = px
                except Exception:
                    pass
            if base <= 0 or vol_use * base < float(MIN_ORDER_AMOUNT):
                eng.print_log(
                    eng.LogLevel.ERROR,
                    f"sell-orphan 강제매도 불가 {symbol}: "
                    f"base={base} vol={vol_use:.8f}")
                return

            eng.print_log(
                eng.LogLevel.ERROR,
                f"sell-orphan 강제 전량매도 {symbol}: "
                f"vol={vol_use:.8f} avg={base:,.8f} "
                f"(보유있는데 ask없음 — 최후수단)")
            ok = sell._force_full_sell(
                symbol, ctx.profit_targets, buyer,
                vol_use, base=base, min_interval=2.0)
            if ok:
                tm.mark_sell_orders_placed()
                if not tm.buy_orders_executed:
                    tm.mark_buy_orders_executed()
                if buyer is not None:
                    sell._sell_placed_at_buy_count = buyer.executed_count
                self._orphan_streak = 0
                eng.print_log(
                    eng.LogLevel.SUCCESS,
                    f"sell-orphan 전량매도 복구 OK {symbol}")
            else:
                skip = str(getattr(sell, '_last_sell_skip', '') or '')
                if skip in ('busy', 'cooldown', 'placing'):
                    return
                eng.print_log(
                    eng.LogLevel.ERROR,
                    f"sell-orphan 전량매도 실패 {symbol} — 다음 틱 재시도")
                sell._log_sell_fail_throttled(
                    symbol, vol_use, base, 'orphan')
        except Exception as e:
            eng.print_log(eng.LogLevel.WARNING,
                          f"sell-orphan-watchdog error: {str(e)[:120]}")


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

        do_manage = False
        check_flat = False
        vol = 0.0
        with ctx.lock:
            vol = ctx.current_volume
            sell_tracking = (
                tm.sell_orders_placed or sell.has_open_sell_orders())
            # 가용잔량 합산 대기 중이면 매 틱 manage 강제
            free_pending = int(
                getattr(sell, '_free_above_min_streak', 0) or 0) > 0
            buy_filled = (
                int(getattr(buyer, 'executed_count', 0) or 0) > 0
                or bool(tm.buy_orders_executed)
                or bool(getattr(buyer, 'executed_orders', None)))
            need_fast = (
                (vol > MIN_HOLDING_VOLUME or buy_filled)
                and (not tm.sell_orders_placed
                     or (buyer.executed_count > sell._sell_placed_at_buy_count)
                     or free_pending
                     or (buy_filled and not sell.has_open_sell_orders())))

            if vol > 0.00001 and not tm.buy_orders_executed:
                tm.mark_buy_orders_executed()
                eng.print_log(eng.LogLevel.SUCCESS,
                              f"Buy orders executed - Holdings: {vol:.6f}")

            # ★ 매수체결됐는데 ask 없으면 무조건 manage (WS vol=0 지연 대비)
            # place 진행 중이면 연타 금지
            if getattr(sell, '_sell_placing', False):
                return
            if (vol > 0.00001 or sell_tracking or need_fast
                    or (buy_filled and not sell.has_open_sell_orders())):
                do_manage = True
            elif tm.sell_orders_placed:
                check_flat = True

        # ★ 매도 POST는 ctx.lock 밖 — 매수 스레드와 상호 블로킹 제거
        if do_manage:
            done = sell.manage_sell_orders(
                ctx.symbol, ctx.profit_targets, tm, 0, buyer)
            with ctx.lock:
                if done:
                    # REST 전량확인은 _complete_cycle_on_sell_done 내부
                    eng.print_log(eng.LogLevel.SELL_SUCCESS,
                                  "Trading completed (sell orders executed)")
                    try:
                        from . import focus as focus_mod
                        focus_mod.clear_focus_if(
                            ctx.symbol, reason='매도완료')
                    except Exception:
                        pass
                    ctx.trading_completed.set()
                    ctx.stop.set()
                    return
                if getattr(sell, "dust_holdings", False):
                    # 먼지/보유0 — 사다리 붙잡지 말고 매수취소 후 종료
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        f"먼지진 감지 — 매수취소 후 사이클 종료 | "
                        f"{sell._dust_detail(ctx.symbol, vol)} | "
                        f"{sell._ladder_detail(buyer)}")
                    if sell._complete_cycle_on_sell_done(
                            tm, buyer, force=True,
                            profit_percentages=ctx.profit_targets):
                        try:
                            from . import focus as focus_mod
                            focus_mod.clear_focus_if(
                                ctx.symbol, reason='먼지진종료')
                        except Exception:
                            pass
                        ctx.dust_exit.set()
                        ctx.trading_completed.set()
                        ctx.stop.set()
                        return
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        f"먼지진 종료실패 — 재매도 유지 | "
                        f"{sell._ladder_detail(buyer)}")
                    return
        elif check_flat:
            # WS flat만으로 종료 금지 — REST 전량 확인
            if not eng.rest_holdings_cleared(ctx.symbol):
                eng.print_log(eng.LogLevel.WARNING,
                              f"WS flat but REST holdings remain — "
                              f"keep selling {ctx.symbol}")
                sell.place_sell_orders(
                    ctx.symbol, ctx.profit_targets, buyer,
                    volume_hint=None, force_replace=False)
                return
            # ★ 보유0(REST) = 매도완료 → 잔여매수 취소 후 다음 사이클
            with ctx.lock:
                eng.print_log(eng.LogLevel.SUCCESS,
                              f"Holdings flat (REST) — cancel buys, next cycle | "
                              f"{sell._ladder_detail(buyer)}")
                if sell._complete_cycle_on_sell_done(
                        tm, buyer, force=True,
                        profit_percentages=ctx.profit_targets):
                    try:
                        from . import focus as focus_mod
                        focus_mod.clear_focus_if(
                            ctx.symbol, reason='보유0 사이클종료')
                    except Exception:
                        pass
                    ctx.trading_completed.set()
                    ctx.stop.set()
                    return

        with ctx.lock:
            if sell.check_stop_loss(ctx.symbol, tm, buyer):
                eng.print_log(eng.LogLevel.WARNING, "Stop loss triggered")
                if buyer.is_active or not getattr(buyer, '_cycle_ended', False):
                    try:
                        buyer.stop_trading()
                    except Exception:
                        buyer.is_active = False
                        buyer._cycle_ended = True
                        buyer.pending_orders.clear()
                        buyer._reset_runtime_indexes()
                        try:
                            eng.OrderCanceler().cancel_buy_orders_for_symbol(
                                ctx.symbol, verify=True)
                        except Exception:
                            pass
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
        SellOrphanWatchdogModule(ctx),
    ]
    rt = ParallelRuntime(ctx, modules)
    rt.start()
    try:
        rt.wait()
    finally:
        rt.stop()
    return ctx
