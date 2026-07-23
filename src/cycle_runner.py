# -*- coding: utf-8 -*-
"""One-coin cycle runner for WorkerSupervisor.

Extracted from engine.main's single-cycle body so N workers can share
budget_pool + order_rate_limiter while each owns buyer/seller/TM.
"""
from __future__ import annotations

import math
import threading
import time
import traceback
from typing import TYPE_CHECKING, Any, Callable, List, Optional

if TYPE_CHECKING:
    from .worker_pool import CoinWorker

# run_managed_cycle 캐시 — 워커 스레드 순환import 경합 방지
_RUN_MANAGED_CYCLE = None


def _load_run_managed_cycle():
    """parallel.run_managed_cycle 안전 로드 (부분초기화 시 짧게 재시도)."""
    global _RUN_MANAGED_CYCLE
    if _RUN_MANAGED_CYCLE is not None:
        return _RUN_MANAGED_CYCLE
    import importlib
    last_err = None
    for _ in range(50):
        try:
            mod = importlib.import_module('.parallel', __package__)
            fn = getattr(mod, 'run_managed_cycle', None)
            if callable(fn):
                _RUN_MANAGED_CYCLE = fn
                return fn
        except Exception as e:
            last_err = e
        time.sleep(0.02)
    raise ImportError(
        f"run_managed_cycle load failed: {last_err}"
    ) from last_err


def run_coin_cycle(
        worker: "CoinWorker",
        *,
        drop_percentage: float,
        drop_count: int,
        distribution_type: Any,
        distribution_weight: float,
        profit_percentage: float,
        stop_event=None,
) -> None:
    """Run one symbol: plan → L1 place → managed buy/sell → clear.

    Uses worker.budget.allocated as DynamicBuyOrder total_amount (soft ceiling).
    On KRW starvation (claim returns 0), waits until free KRW returns then
    continues ladder at original planned prices (buyer keeps plan).
    """
    from . import engine as eng
    from .budget import budget_pool
    from .config import MIN_ORDER_AMOUNT, MIN_HOLDING_VOLUME

    symbol = worker.symbol
    wid = worker.worker_id
    eng.set_current_worker_id(wid)
    stop = stop_event or worker.stop

    try:
        eng.print_log(
            eng.LogLevel.SUCCESS,
            f"[worker {wid}] start {symbol} "
            f"alloc={int(getattr(worker.budget, 'allocated', 0) or 0):,}원")

        # Subscribe market data — 기존 다중 구독에 추가만 (덮어쓰기 금지)
        eng.RealMarketData.subscribe_trade_stream(symbol)
        eng.RealMarketData.subscribe_websocket(symbol)

        # ★ 검증장치 1: HybridMA — 현재가 ≥ 분MA 또는 ≥ hybrid 이면 매수 절대 금지
        gate_ok, gate_info = eng.RealMarketData.check_tick_ma_gate(symbol)
        if not gate_ok:
            # MA 미준비면 최대 20초 warmup, 이미 MA 위면 즉시 abort
            hybrid = (gate_info.get('hybrid') or gate_info.get('ma60')
                      or gate_info.get('ma25') or gate_info.get('ma20'))
            px = gate_info.get('last_price')
            if (hybrid is not None and px is not None
                    and gate_info.get('ma_min') is not None
                    and (px >= hybrid or px >= gate_info.get('ma_min'))):
                eng.print_log(
                    eng.LogLevel.ERROR,
                    f"[worker {wid}] HybridMA 미통과 {symbol}: "
                    f"now={px} hybrid={hybrid} "
                    f"tick={gate_info.get('ma_tick')} "
                    f"min={gate_info.get('ma_min')} — 매수 abort")
                return
            warmed = False
            deadline = time.time() + 20.0
            while time.time() < deadline and not stop.is_set():
                gate_ok, gate_info = eng.RealMarketData.check_tick_ma_gate(
                    symbol)
                if gate_ok:
                    warmed = True
                    break
                hybrid = (gate_info.get('hybrid') or gate_info.get('ma60')
                          or gate_info.get('ma25') or gate_info.get('ma20'))
                px = gate_info.get('last_price')
                if (hybrid is not None and px is not None
                        and gate_info.get('ma_min') is not None
                        and (px >= hybrid or px >= gate_info.get('ma_min'))):
                    eng.print_log(
                        eng.LogLevel.ERROR,
                        f"[worker {wid}] HybridMA 미통과 {symbol}: "
                        f"now={px} hybrid={hybrid} "
                        f"tick={gate_info.get('ma_tick')} "
                        f"min={gate_info.get('ma_min')} — 매수 abort")
                    return
                time.sleep(0.25)
            if not warmed:
                eng.print_log(
                    eng.LogLevel.INFO,
                    f"[worker {wid}] HybridMA 미준비 {symbol} "
                    f"tick_c={gate_info.get('candle_count', 0)} "
                    f"min_c={gate_info.get('min_candle_count', 0)} "
                    f"— 신규/데이터부족 skip")
                return
        eng.print_log(
            eng.LogLevel.SUCCESS,
            f"[worker {wid}] HybridMA PASS {symbol}: "
            f"now={gate_info.get('last_price')} "
            f"< hybrid={(gate_info.get('hybrid') or gate_info.get('ma60'))} "
            f"tick={gate_info.get('ma_tick')} min={gate_info.get('ma_min')} "
            f"w_tick={gate_info.get('w_tick')}")

        # ★ 검증장치 2: volatility check (고변동 보호)
        if eng.VolatilityProtector.check_volatility_protection(symbol):
            eng.print_log(
                eng.LogLevel.WARNING,
                f"[worker {wid}] volatility check 차단 {symbol} — abort")
            return
        eng.print_log(
            eng.LogLevel.SUCCESS,
            f"[worker {wid}] volatility check PASS {symbol}")

        try:
            analyzer = eng.MarketAnalyzer(symbol)
        except Exception as e:
            eng.print_log(
                eng.LogLevel.INFO,
                f"[worker {wid}] 캔들 로드 실패 {symbol}: {str(e)[:80]} — skip")
            return
        if not getattr(analyzer, 'ok', False):
            eng.print_log(
                eng.LogLevel.INFO,
                f"[worker {wid}] 신규/캔들부족 {symbol} — MarketAnalyzer skip")
            return
        live_px = eng.RealMarketData.get_current_price(symbol)
        buy_base = (
            live_px if live_px and live_px > 0
            else analyzer.candle.current_price)
        # ★ REST로 최종 검증 — TREE 시세가 CALDERA 계획에 섞이는 것 차단
        buy_base, contaminated = eng.RealMarketData.sanitize_symbol_price(
            symbol, buy_base, force_rest=True)
        if contaminated:
            eng.print_log(
                eng.LogLevel.ERROR,
                f"[worker {wid}] {symbol} buy_base 오염 교정 → {buy_base}")
        if not buy_base or buy_base <= 0:
            eng.print_log(
                eng.LogLevel.INFO,
                f"[worker {wid}] {symbol} buy_base 없음 — 신규/시세없음 skip")
            return
        lows = list(getattr(analyzer.candle, 'low_prices', None) or [])
        low_px = lows[-1] if lows else 0.0
        # low도 시세와 동떨어지면 (캔들 캐시 오염) live 근처로 클램프
        try:
            low_px = float(low_px or 0)
        except (TypeError, ValueError):
            low_px = 0.0
        if low_px <= 0 or abs(low_px - buy_base) / buy_base > 0.5:
            eng.print_log(
                eng.LogLevel.WARNING,
                f"[worker {wid}] {symbol} low={low_px} 이상 — "
                f"buy_base={buy_base} 사용")
            low_px = buy_base

        # Soft budget from pool
        plan_krw = int(math.floor(
            budget_pool.plan_budget(wid) + 1e-9))
        if plan_krw < MIN_ORDER_AMOUNT:
            # dump-remaining: take whatever free is left
            budget_pool.enable_dump_remaining(wid)
            plan_krw = int(math.floor(
                budget_pool.available_for(wid) + 1e-9))
        if plan_krw < MIN_ORDER_AMOUNT:
            eng.print_log(
                eng.LogLevel.WARNING,
                f"[worker {wid}] {symbol} KRW 부족 — 가용 대기")
            _wait_for_krw(wid, stop, eng, budget_pool)
            plan_krw = int(math.floor(
                max(budget_pool.plan_budget(wid),
                    budget_pool.available_for(wid)) + 1e-9))
        if plan_krw < MIN_ORDER_AMOUNT:
            eng.print_log(
                eng.LogLevel.ERROR,
                f"[worker {wid}] {symbol} 예산 불가 — abort")
            return

        # claim 직전 재확인
        gate_ok, gate_info = eng.RealMarketData.check_tick_ma_gate(symbol)
        if not gate_ok:
            eng.print_log(
                eng.LogLevel.ERROR,
                f"[worker {wid}] claim전 HybridMA 차단 {symbol}: "
                f"now={gate_info.get('last_price')} "
                f"hybrid={(gate_info.get('hybrid') or gate_info.get('ma60'))} "
                f"tick={gate_info.get('ma_tick')} min={gate_info.get('ma_min')} — abort")
            return

        # 각 워커 = 가용×alloc% 전액 claim (형제와 나눠 먹지 않음)
        claimed = budget_pool.claim(wid, plan_krw)
        if claimed < MIN_ORDER_AMOUNT:
            budget_pool.enable_dump_remaining(wid)
            claimed = budget_pool.claim(wid, plan_krw)
        if claimed < MIN_ORDER_AMOUNT:
            eng.print_log(
                eng.LogLevel.WARNING,
                f"[worker {wid}] claim 실패 — 대기 후 재시도")
            _wait_for_krw(wid, stop, eng, budget_pool)
            claimed = budget_pool.claim(
                wid, max(plan_krw, budget_pool.available_for(wid)))
        if claimed < MIN_ORDER_AMOUNT:
            return

        # POST 직전 최종 게이트
        gate_ok, gate_info = eng.RealMarketData.check_tick_ma_gate(symbol)
        if not gate_ok:
            budget_pool.release_reserved(wid)
            eng.print_log(
                eng.LogLevel.ERROR,
                f"[worker {wid}] POST전 HybridMA 차단 {symbol}: "
                f"now={gate_info.get('last_price')} "
                f"hybrid={(gate_info.get('hybrid') or gate_info.get('ma60'))} "
                f"tick={gate_info.get('ma_tick')} min={gate_info.get('ma_min')} — 예약해제 abort")
            return

        total_amount = int(math.floor(claimed + 1e-9))
        tm = eng.TradingManager(watch_command=False)
        tm.set_symbol(symbol)
        worker.trading_manager = tm

        buyer = eng.DynamicBuyOrder(
            symbol, buy_base, low_px, total_amount,
            distribution_weight, 0)
        buyer.calculate_order_plan(
            drop_percentage, drop_count, distribution_type)
        worker.dynamic_buyer = buyer

        placed_ok = False
        with eng._buy_lifecycle_lock:
            try:
                # symbol-scoped cancel only
                eng.OrderCanceler().cancel_buy_orders_for_symbol(symbol)
            except Exception as e:
                eng.print_log(
                    eng.LogLevel.WARNING,
                    f"[worker {wid}] pre-buy cancel: {str(e)[:80]}")
            eng.begin_buy_placement_window()
            placed_ok = bool(buyer.execute_dynamic_buy_orders())

        if not placed_ok:
            budget_pool.release_reserved(wid)
            eng.print_log(
                eng.LogLevel.ERROR,
                f"[worker {wid}] {symbol} L1 실패")
            return

        tm.mark_buy_orders_placed()
        budget_pool.mark_spent(wid, total_amount * 0.0)  # reserved until fills
        # keep reserved until cycle end; spent tracked loosely

        sell = eng.SellController()
        worker.sell_controller = sell
        profit_targets = [float(profit_percentage)]
        tm._last_profit_pct = float(profit_percentage)

        def _on_buy_fill_sell(avg, vol):
            if avg <= 0 or vol <= 0:
                return
            is_local = bool(getattr(
                eng.private_ws, '_avg_sell_fire_is_local', False))
            try:
                avg_use = float(avg)
            except (TypeError, ValueError):
                avg_use = float(avg)
            if avg_use <= 0 or vol * avg_use < MIN_ORDER_AMOUNT:
                return
            tag = "local-vwap" if is_local else "rest-avg"
            pct = profit_targets[0]
            tgt = eng.UpbitTickSystem.calculate_sell_price(avg_use, pct)
            min_px = eng.UpbitTickSystem.min_no_loss_sell_price(avg_use)
            if min_px > 0 and tgt + 1e-12 < min_px:
                tgt = min_px
            eng.print_log(
                eng.LogLevel.INFO,
                f"[worker {wid}] 매도기준평단 {avg_use:,.8f} ({tag}) "
                f"목표호가={tgt:,.8f}")
            ok = sell.place_sell_orders(
                symbol, profit_targets, buyer,
                sell_base_price=avg_use, force_replace=False,
                volume_hint=vol, avg_refresh=True)
            # ★ ask 실존 확인 — 가짜 성공이면 강제 재POST
            if not (ok and sell._open_sell_count() > 0):
                if sell._adopt_open_ask_tracking(symbol):
                    ok = True
                elif str(getattr(sell, '_last_sell_skip', '') or '') in (
                        'busy', 'cooldown', 'placing'):
                    # 다른 스레드가 POST 중 — 재arm 금지(수량 폭증)
                    return
                else:
                    ok = sell._force_full_sell(
                        symbol, profit_targets, buyer, vol,
                        base=avg_use, min_interval=1.0)
            if ok and (sell._open_sell_count() > 0
                       or sell._adopt_open_ask_tracking(symbol)):
                sell._sell_base_provisional = is_local
                if not is_local:
                    sell._last_server_avg_seen = float(avg_use)
                    sell._sell_base_provisional = False
                tm.mark_sell_orders_placed()
                if not tm.buy_orders_executed:
                    tm.mark_buy_orders_executed()
                sell._sell_placed_at_buy_count = buyer.executed_count
            else:
                skip = str(getattr(sell, '_last_sell_skip', '') or '')
                if skip in ('busy', 'cooldown', 'placing'):
                    return
                if sell.has_open_sell_orders() or sell._open_sell_count() > 0:
                    tm.mark_sell_orders_placed()
                    return
                # 재arm: fill_price/uuid 없음 → 장부 중복가산 금지. 실보유만.
                try:
                    bal, loc, _ = eng.private_ws.get_symbol_info(symbol)
                    held = max(float(bal or 0), 0) + max(float(loc or 0), 0)
                except Exception:
                    held = 0.0
                re_vol = held if held >= MIN_HOLDING_VOLUME else float(vol)
                now_l = time.time()
                last_l = float(getattr(sell, '_last_rearm_log_t', 0) or 0)
                if now_l - last_l >= 5.0:
                    sell._last_rearm_log_t = now_l
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        f"[worker {wid}] 전량매도 미확인 — 재arm "
                        f"{symbol} avg={avg_use:,.8f} vol={re_vol:.8f}")
                try:
                    eng.private_ws.arm_avg_sell(
                        vol_hint=re_vol, symbol=symbol)
                except Exception:
                    pass

        buyer.on_buy_fill_sell = _on_buy_fill_sell
        eng.private_ws.set_avg_sell_target(symbol, _on_buy_fill_sell)

        eng.print_log(
            eng.LogLevel.SUCCESS,
            f"[worker {wid}] modules start {symbol} budget={total_amount:,}")

        # ★ 워커 스레드에서 순환import 경합 방지 — 지연 로드 + 재시도
        run_managed_cycle = _load_run_managed_cycle()
        ctx = run_managed_cycle(
            symbol=symbol,
            dynamic_buyer=buyer,
            sell_controller=sell,
            trading_manager=tm,
            profit_targets=profit_targets,
            private_ws=eng.private_ws,
            cycle_timeout=86400,
        )
        # If stop requested mid-cycle, modules exit via ctx.stop
        if stop.is_set():
            ctx.stop.set()

        eng.print_log(
            eng.LogLevel.INFO,
            f"[worker {wid}] cycle end {symbol} "
            f"done={ctx.trading_completed.is_set()}")

    except Exception as e:
        msg = str(e)
        soft = (
            'CandleInfoFetcher' in msg
            or 'list index out of range' in msg
            or 'buy_base' in msg.lower()
        )
        eng.print_log(
            eng.LogLevel.WARNING if soft else eng.LogLevel.ERROR,
            f"[worker {wid}] cycle error {symbol}: {e}")
        if not soft:
            traceback.print_exc()
        # 신규상장/데이터부족 soft 오류는 워커만 종료 — supervisor 전파 raise 금지
        if not soft:
            raise
    finally:
        try:
            if worker.dynamic_buyer is not None:
                worker.dynamic_buyer._cycle_ended = True
                worker.dynamic_buyer.is_active = False
        except Exception:
            pass
        try:
            eng.OrderCanceler().cancel_buy_orders_for_symbol(
                symbol, verify=True)
        except Exception as e:
            eng.print_log(
                eng.LogLevel.WARNING,
                f"[worker {wid}] end cancel buys: {str(e)[:80]}")
        try:
            eng.private_ws.clear_avg_sell_target(symbol)
        except Exception:
            pass
        try:
            budget_pool.release_reserved(wid)
        except Exception:
            pass
        eng.set_current_worker_id(None)


def _wait_for_krw(worker_id, stop, eng, pool, timeout=3600.0):
    """가용 KRW가 생길 때까지 대기 (다른 워커 매도 회수)."""
    from .config import MIN_ORDER_AMOUNT
    deadline = time.time() + timeout
    while time.time() < deadline:
        if stop is not None and stop.is_set():
            return
        try:
            if eng.private_ws._is_initialized and eng.private_ws.is_connected:
                free = float(eng.private_ws.get_krw_balance(1) or 0)
            else:
                free = float(eng.AccountChecker().get_krw_balance() or 0)
        except Exception:
            free = 0.0
        pool.refresh_balances(free)
        if pool.available_for(worker_id) >= MIN_ORDER_AMOUNT:
            return
        time.sleep(0.5)


def invalidate_eligible_rank_cache():
    """레거시 호환 — no-op."""
    return


def _passes_buy_gates(eng, symbol: str, *, quiet: bool = False) -> tuple:
    """매수 게이트: 1) HybridMA (틱+1분) 2) 변동성보호.
    신규상장(캔들<60)은 미준비/vol차단으로 조용히 탈락.
    Returns: (ok: bool, reason: str)"""
    su = str(symbol).upper()
    try:
        eng.RealMarketData.subscribe_trade_stream(su)
    except Exception:
        pass
    try:
        ok, info = eng.RealMarketData.check_tick_ma_gate(su)
    except Exception as e:
        return False, f'HybridMA err:{str(e)[:40]}'
    if not ok:
        hybrid = (info.get('hybrid') or info.get('ma60')
                  or info.get('ma25') or info.get('ma20'))
        px = info.get('last_price')
        if hybrid is None or info.get('ma_tick') is None or info.get('ma_min') is None:
            return False, (
                f"HybridMA 미준비 tick_c={info.get('candle_count', 0)} "
                f"min_c={info.get('min_candle_count', 0)}")
        return False, (
            f"HybridMA fail now={px}≥hybrid={hybrid} "
            f"tick={info.get('ma_tick')} min={info.get('ma_min')}")
    try:
        blocked = eng.VolatilityProtector.check_volatility_protection(
            su, quiet=bool(quiet))
    except Exception as e:
        return False, f'vol-check err:{str(e)[:40]}'
    if blocked:
        return False, 'volatility protection'
    return True, 'HybridMA+vol PASS'


def _get_scanner(eng=None):
    if eng is None:
        from . import engine as eng
    return getattr(eng, 'volatility_scanner', None)


def auto_select_candidates(
        *,
        eng=None,
        pool: int = None,
        excluded=None,
) -> List[str]:
    """VolatilityScanner ticker/all 변동성 상위 후보 (게이트 전). command.txt 미사용."""
    from .config import AUTO_SELECT_CANDIDATE_POOL
    if eng is None:
        from . import engine as eng
    scanner = _get_scanner(eng)
    n = int(pool if pool is not None else AUTO_SELECT_CANDIDATE_POOL)
    if scanner is None or not getattr(scanner, 'is_running', False):
        return []
    try:
        ranked = scanner.get_top_volatility_symbols(
            n, excluded_symbols=excluded)
    except Exception:
        return []
    return [str(s).upper() for s, _cv, _px in (ranked or []) if s]


def eligible_auto_symbols(
        symbols: List[str] = None,
        *,
        eng=None,
        quiet: bool = False,
        k: int = None,
        prefer_symbols: List[str] = None,
) -> List[str]:
    """CV 상위 후보 → HybridMA+vol 통과 → Top k.
    prefer_symbols(이미 운용 중)은 게이트만 통과하면 우선 유지(sticky) —
    순위 깜빡임으로 슬롯이 2개로 줄지 않게."""
    from .config import AUTO_SELECT_TOP_N, PARALLEL_WORKERS
    if eng is None:
        from . import engine as eng
    top_n = int(k if k is not None else AUTO_SELECT_TOP_N)
    top_n = max(1, min(top_n, int(PARALLEL_WORKERS)))

    cleaned = []
    seen = set()
    src = symbols if symbols is not None else auto_select_candidates(eng=eng)
    for s in (src or []):
        if not s:
            continue
        su = str(s).upper()
        if su not in seen:
            seen.add(su)
            cleaned.append(su)

    prefer = []
    for s in (prefer_symbols or []):
        if not s:
            continue
        su = str(s).upper()
        if su not in prefer:
            prefer.append(su)

    # sticky 심볼도 구독·게이트 대상에 포함
    for su in prefer:
        if su not in seen:
            seen.add(su)
            cleaned.append(su)
    if not cleaned and not prefer:
        return []

    # 게이트 판정용 선구독
    try:
        eng.RealMarketData.subscribe_trade_stream_symbols(cleaned)
        for s in cleaned:
            try:
                eng.RealMarketData.subscribe_websocket(s)
            except Exception:
                pass
    except Exception:
        pass

    passed = []
    passed_set = set()

    def _try_add(su: str) -> bool:
        if su in passed_set:
            return False
        ok, reason = _passes_buy_gates(eng, su, quiet=True)
        if not ok:
            rs = str(reason or '')
            if (not quiet
                    and not rs.startswith('HybridMA fail')
                    and '미준비' not in rs
                    and 'candle' not in rs.lower()):
                eng.print_log(
                    eng.LogLevel.INFO,
                    f"후보제외 {su}: {reason}")
            return False
        passed.append(su)
        passed_set.add(su)
        return True

    # 1) sticky: 이미 운용 중 + 게이트 통과 → 우선 유지
    for su in prefer:
        _try_add(su)
        if len(passed) >= top_n:
            return passed

    # 2) CV 순위 순으로 빈 자리 채움
    for su in cleaned:
        if su in passed_set:
            continue
        _try_add(su)
        if len(passed) >= top_n:
            break
    return passed


# 하위 호환 이름 (command → auto)
def eligible_command_symbols(
        symbols: List[str] = None,
        *,
        eng=None,
        quiet: bool = False,
) -> List[str]:
    """AUTO_SELECT: CV순위+게이트 통과. symbols 있으면 그 목록만 필터."""
    from .config import AUTO_SELECT_TOP_N, PARALLEL_WORKERS
    k = max(1, min(int(AUTO_SELECT_TOP_N), int(PARALLEL_WORKERS)))
    if symbols is None:
        return eligible_auto_symbols(eng=eng, quiet=quiet, k=k)
    # 명시 목록: 순서 유지 필터 (레거시/테스트)
    if eng is None:
        from . import engine as eng
    out = []
    for s in symbols:
        if not s:
            continue
        su = str(s).upper()
        ok, reason = _passes_buy_gates(eng, su, quiet=True)
        if not ok:
            rs = str(reason or '')
            if (not quiet
                    and not rs.startswith('HybridMA fail')
                    and '미준비' not in rs
                    and 'candle' not in rs.lower()):
                eng.print_log(
                    eng.LogLevel.INFO,
                    f"후보제외 {su}: {reason}")
            continue
        out.append(su)
    return out


def top_eligible_command_symbols(
        symbols: List[str] = None,
        k: int = None,
        *,
        eng=None,
) -> List[str]:
    """AUTO_SELECT Top k (기본 AUTO_SELECT_TOP_N)."""
    from .config import AUTO_SELECT_TOP_N, PARALLEL_WORKERS
    if k is None:
        k = int(AUTO_SELECT_TOP_N)
    k = max(1, min(int(k), int(PARALLEL_WORKERS)))
    return eligible_auto_symbols(symbols=symbols, eng=eng, k=k)


def eligible_command_symbols_ranked(symbols=None, *, eng=None, ttl=None, quiet=False):
    """[(sym, cv)] — 스캐너 CV. symbols 없으면 auto 후보."""
    if eng is None:
        from . import engine as eng
    scanner = _get_scanner(eng)
    passed = eligible_auto_symbols(symbols=symbols, eng=eng, quiet=quiet)
    cv_map = {}
    if scanner is not None:
        try:
            for su, cv, _px in scanner.get_top_volatility_symbols(50):
                cv_map[su] = cv
        except Exception:
            pass
    return [(s, float(cv_map.get(s, 0.0))) for s in passed]


def rank_command_symbols_by_volatility(*a, **kw):
    return eligible_command_symbols_ranked(*a, **kw)


def top_volatile_command_symbols(*a, **kw):
    return top_eligible_command_symbols(*a, **kw)


def make_symbol_picker(
        candidate_symbols_fn: Callable[[], List[str]],
        active_symbols_fn: Callable[[], List[str]],
        *,
        max_parallel: int = None,
) -> Callable[[], Optional[str]]:
    """CV 상위 후보 → HybridMA+vol 통과 Top N → 미활성 1개."""

    def _pick():
        from . import engine as eng
        from .config import AUTO_SELECT_TOP_N, PARALLEL_WORKERS
        k = int(max_parallel or AUTO_SELECT_TOP_N or PARALLEL_WORKERS)
        k = max(1, min(k, int(PARALLEL_WORKERS)))
        active = set(s.upper() for s in (active_symbols_fn() or []))
        # candidate_fn이 풀 후보를 주거나 비어 있으면 스캐너에서
        raw = list(candidate_symbols_fn() or [])
        top = eligible_auto_symbols(
            symbols=raw if raw else None, eng=eng, quiet=True, k=k)
        if not top:
            return None
        last = getattr(_pick, '_last_top_log', None)
        top_key = tuple(top)
        if top_key != last:
            _pick._last_top_log = top_key
            eng.print_log(
                eng.LogLevel.SUCCESS,
                f"매수대상 AUTO CV+HybridMA 통과 "
                f"{len(top)}/{k}: " + ", ".join(top))

        for su in top:
            if su in active:
                continue
            ok, reason = _passes_buy_gates(eng, su, quiet=True)
            if ok:
                return su
            eng.print_log(
                eng.LogLevel.INFO,
                f"pick skip {su}: {reason}")
        return None

    return _pick


def start_ma_gate_monitor(
        supervisor,
        candidate_symbols_fn: Callable[[], List[str]],
        *,
        max_parallel: int = None,
        stop_event=None,
) -> threading.Thread:
    """AUTO_SELECT: CV 상위 후보 감시.
    HybridMA+변동성 통과 & 미활성이면 즉시 spawn.
    한도 = 가용 KRW × alloc% (budget_pool)."""
    from . import engine as eng
    from .budget import budget_pool
    from .config import (
        PARALLEL_WORKERS, MA_GATE_WATCH_INTERVAL_S, MIN_ORDER_AMOUNT,
        AUTO_SELECT_TOP_N,
    )

    n = int(max_parallel or AUTO_SELECT_TOP_N or PARALLEL_WORKERS)
    n = max(1, min(n, int(PARALLEL_WORKERS)))
    stop = stop_event or threading.Event()
    gate_blocked: set = set()
    last_status_log = 0.0
    last_pass_key = None

    def _refresh_krw():
        try:
            if (eng.private_ws._is_initialized
                    and eng.private_ws.is_connected):
                free = float(eng.private_ws.get_krw_balance(1) or 0)
                locked = float(eng.private_ws.get_krw_balance(2) or 0)
            else:
                free = float(eng.AccountChecker().get_krw_balance() or 0)
                locked = 0.0
            total = free + max(locked, 0.0)
            try:
                total = max(total, float(eng.ws_krw_total() or total))
            except Exception:
                pass
            # ★ 할당 기준 = free (가용 KRW). equity는 참고만.
            budget_pool.refresh_balances(
                free, total_equity=max(total, free))
            return free, total
        except Exception:
            return 0.0, 0.0

    def _loop():
        nonlocal last_status_log, last_pass_key
        eng.print_log(
            eng.LogLevel.SUCCESS,
            f"게이트 감시 시작 "
            f"(AUTO CV Top{n} / HybridMA+변동성 / 한도=가용KRW×alloc% / "
            f"interval={MA_GATE_WATCH_INTERVAL_S}s / 상한={n})")
        while not stop.is_set():
            try:
                cand = list(candidate_symbols_fn() or [])
                if not cand:
                    cand = auto_select_candidates(eng=eng)
                if cand:
                    try:
                        eng.RealMarketData.subscribe_trade_stream_symbols(cand)
                    except Exception:
                        pass

                # 게이트 통과 Top N — 이미 운용 중 심볼 sticky 우선
                active_now = list(supervisor.active_symbols() or [])
                passed = eligible_auto_symbols(
                    symbols=cand if cand else None,
                    eng=eng, quiet=True, k=n,
                    prefer_symbols=active_now)
                # ★ TopN에서 빠진 워커 슬롯 회수 (보유 있으면 매도만 유지)
                try:
                    dropped = supervisor.reap_not_in_command(passed)
                    if dropped:
                        eng.print_log(
                            eng.LogLevel.WARNING,
                            f"게이트 감시: AUTO TopN 제외 워커 {dropped}개 회수")
                except Exception as e:
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        f"AUTO 제외 회수 오류: {str(e)[:80]}")

                pass_set = set(passed)
                for su in (cand or []):
                    su = str(su).upper()
                    if su in pass_set:
                        if su in gate_blocked:
                            eng.print_log(
                                eng.LogLevel.SUCCESS,
                                f"게이트 PASS전환 {su}: HybridMA+vol OK "
                                f"— AUTO 매수 편입")
                        gate_blocked.discard(su)
                    else:
                        gate_blocked.add(su)

                pass_key = tuple(passed)
                if pass_key != last_pass_key:
                    last_pass_key = pass_key
                    if passed:
                        eng.print_log(
                            eng.LogLevel.SUCCESS,
                            f"게이트통과 AUTO Top{len(passed)}/{n}: "
                            + ", ".join(passed))

                active = set(
                    s.upper() for s in (supervisor.active_symbols() or []))
                idle = [s for s in passed if s not in active]
                pool_n = min(n, len(passed))
                _refresh_krw()

                hard_cap = int(n)
                supervisor.max_workers = hard_cap
                supervisor.pool.max_workers = hard_cap
                if pool_n > 0:
                    budget_pool.set_trade_slots(pool_n)
                try:
                    eng.order_rate_limiter.set_active_workers(
                        max(1, hard_cap))
                except Exception:
                    pass

                if idle:
                    try:
                        killed = supervisor.reap_ma_wait_zombies(
                            keep_symbols=pass_set)
                        if killed:
                            eng.print_log(
                                eng.LogLevel.WARNING,
                                f"게이트 감시: MA대기 좀비 {killed}개 회수")
                            active = set(
                                s.upper()
                                for s in (supervisor.active_symbols() or []))
                            idle = [s for s in passed if s not in active]
                    except Exception as e:
                        eng.print_log(
                            eng.LogLevel.WARNING,
                            f"게이트 감시 zombie reap: {str(e)[:80]}")

                # 신규매수 중단(최초×50% 하한) → spawn 금지, 기존 코인 dump
                if budget_pool.is_new_buy_halted():
                    if supervisor.active_count() > 0:
                        budget_pool.assist_idle_to_workers()
                    now = time.time()
                    if now - last_status_log >= 10.0:
                        last_status_log = now
                        snap = budget_pool.snapshot()
                        eng.print_log(
                            eng.LogLevel.WARNING,
                            f"신규매수중단 유지: free={int(snap.get('free_krw', 0)):,} "
                            f"< 최초×50%({int(snap.get('reserve_floor', 0)):,}) "
                            f"최초={int(snap.get('baseline_krw', 0)):,} "
                            f"활성={sorted(active) or '-'} → 기존코인 집중")
                    stop.wait(timeout=float(MA_GATE_WATCH_INTERVAL_S))
                    continue

                # 이미 활성 + 게이트통과인데 매수 멈춘 워커 깨우기
                try:
                    woken = supervisor.wake_gate_pass_buyers(passed)
                    if woken:
                        eng.print_log(
                            eng.LogLevel.SUCCESS,
                            f"게이트 감시: 매수재개 wake {woken}개")
                except Exception as e:
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        f"게이트 wake 오류: {str(e)[:80]}")

                # 포커스가 HybridMA 위면 집중 해제 → 타코인 매수 가능
                try:
                    from . import focus as focus_mod
                    foc = focus_mod.get_focus_symbol()
                    if foc and foc in gate_blocked:
                        focus_mod.clear_focus_if(
                            foc, reason=f'{foc} HybridMA위 — 타코인매수 재개')
                except Exception:
                    pass

                # CV 순위(통과 목록) 순으로 미활성 심볼 spawn
                for su in idle:
                    if supervisor.active_count() >= hard_cap:
                        break
                    try:
                        from . import focus as focus_mod
                        if focus_mod.should_block_buy(su):
                            eng.print_log(
                                eng.LogLevel.WARNING,
                                f"게이트 spawn skip {su}: "
                                f"집중모드 focus={focus_mod.get_focus_symbol()}")
                            continue
                    except Exception:
                        pass
                    _refresh_krw()
                    if float(budget_pool.free_krw or 0) < float(MIN_ORDER_AMOUNT):
                        eng.print_log(
                            eng.LogLevel.WARNING,
                            f"게이트 spawn 보류 {su}: "
                            f"가용KRW={int(budget_pool.free_krw):,} "
                            f"< min={int(MIN_ORDER_AMOUNT)}")
                        break
                    w = supervisor.try_spawn(su)
                    if w:
                        snap = budget_pool.snapshot()
                        eng.print_log(
                            eng.LogLevel.SUCCESS,
                            f"게이트 spawn {w.symbol} id={w.worker_id} "
                            f"한도={int(getattr(w.budget, 'allocated', 0)):,}원 "
                            f"(가용KRW×{int(float(snap.get('alloc_pct', 0))*100)}%) "
                            f"({supervisor.active_count()}/{hard_cap})")
                        active.add(su)
                    else:
                        eng.print_log(
                            eng.LogLevel.WARNING,
                            f"게이트 spawn 실패 {su} "
                            f"active={supervisor.active_count()}/{hard_cap} "
                            f"free={int(budget_pool.free_krw):,}")

                now = time.time()
                if now - last_status_log >= 10.0:
                    last_status_log = now
                    snap = budget_pool.snapshot()
                    active = set(
                        s.upper()
                        for s in (supervisor.active_symbols() or []))
                    idle_now = [s for s in passed if s not in active]
                    eng.print_log(
                        eng.LogLevel.INFO,
                        f"게이트 감시: "
                        f"AUTO통과={passed or '-'} "
                        f"미활성={idle_now or '-'} "
                        f"활성={sorted(active) or '-'} "
                        f"슬롯={supervisor.active_count()}/{n} "
                        f"N={budget_pool.alloc_slots()} "
                        f"free={int(snap.get('free_krw', 0)):,} "
                        f"한도={int(snap.get('ceiling', 0)):,} "
                        f"({int(float(snap.get('alloc_pct', 0))*100)}%) "
                        f"최초={int(snap.get('baseline_krw', 0)):,} "
                        f"하한50%={int(snap.get('reserve_floor', 0)):,} "
                        f"신규중단={'Y' if snap.get('new_buy_halted') else 'N'} "
                        f"차단={sorted(gate_blocked) or '-'}")
            except Exception as e:
                try:
                    eng.print_log(
                        eng.LogLevel.WARNING,
                        f"게이트 감시 오류: {str(e)[:160]}")
                except Exception:
                    pass
            stop.wait(timeout=float(MA_GATE_WATCH_INTERVAL_S))

    t = threading.Thread(
        target=_loop, name='gate-monitor', daemon=True)
    t.start()
    return t


# 호환 별칭
start_gate_monitor = start_ma_gate_monitor


def run_parallel_supervisor_loop(
        *,
        drop_percentage: float,
        drop_count: int,
        distribution_type: Any,
        distribution_weight: float,
        profit_percentage: float,
        max_workers: int = None,
) -> None:
    """AUTO_SELECT: CV 상위 → HybridMA+vol 통과 Top N 상시 매수.
    command.txt 심볼 소스 폐지. 한도 = 가용 KRW × alloc%[N]."""
    from . import engine as eng
    from .budget import budget_pool
    from .config import PARALLEL_WORKERS, AUTO_SELECT_TOP_N
    from .worker_pool import WorkerSupervisor, describe_alloc_table

    # ★ 메인 스레드에서 parallel 선행 로드 — 워커 spawn 시 부분초기화 방지
    try:
        _load_run_managed_cycle()
    except Exception as e:
        eng.print_log(
            eng.LogLevel.WARNING,
            f"parallel 선행로드 실패(재시도는 워커에서): {e}")

    n = int(max_workers or AUTO_SELECT_TOP_N or PARALLEL_WORKERS)
    n = max(1, min(n, int(PARALLEL_WORKERS)))
    eng.print_log(
        eng.LogLevel.SUCCESS,
        f"=== MULTI-WORKER MODE: {describe_alloc_table()} ===")
    eng.print_log(
        eng.LogLevel.INFO,
        f"심볼=AUTO CV Top{n} / 게이트=HybridMA(틱+1분)+변동성 / "
        f"한도=가용KRW×alloc% / command.txt 폐지 / 동시상한={n}")

    def _cycle_runner(worker):
        run_coin_cycle(
            worker,
            drop_percentage=drop_percentage,
            drop_count=drop_count,
            distribution_type=distribution_type,
            distribution_weight=distribution_weight,
            profit_percentage=profit_percentage,
            stop_event=worker.stop,
        )

    def _candidate_symbols():
        return auto_select_candidates(eng=eng)

    supervisor = WorkerSupervisor(
        pool=budget_pool,
        max_workers=n,
        cycle_runner=_cycle_runner,
    )
    supervisor.symbol_picker = make_symbol_picker(
        _candidate_symbols, supervisor.active_symbols, max_parallel=n)

    # ★ HybridMA 통과 후보 즉시 편입 — 백그라운드 감시모듈
    _monitor_stop = threading.Event()
    start_ma_gate_monitor(
        supervisor, _candidate_symbols,
        max_parallel=n, stop_event=_monitor_stop)

    while True:
        cand = _candidate_symbols()
        trade_pool = eligible_auto_symbols(
            symbols=cand if cand else None, eng=eng, quiet=True, k=n,
            prefer_symbols=list(supervisor.active_symbols() or []))
        pool_n = min(n, len(trade_pool)) if trade_pool else 0
        # 후보·통과 심볼 선구독 (게이트/분봉 MA)
        try:
            sub = list(dict.fromkeys((cand or []) + (trade_pool or [])))
            if sub:
                eng.RealMarketData.subscribe_trade_stream_symbols(sub)
                for s in sub:
                    eng.RealMarketData.subscribe_websocket(s)
        except Exception:
            pass
        budget_pool.set_trade_slots(pool_n if pool_n > 0 else 1)
        hard_cap = int(n)
        eng.order_rate_limiter.set_active_workers(max(1, hard_cap))
        supervisor.max_workers = hard_cap
        supervisor.pool.max_workers = hard_cap

        try:
            if eng.private_ws._is_initialized and eng.private_ws.is_connected:
                free = float(eng.private_ws.get_krw_balance(1) or 0)
                locked = float(eng.private_ws.get_krw_balance(2) or 0)
            else:
                free = float(eng.AccountChecker().get_krw_balance() or 0)
                locked = 0.0
        except Exception:
            free, locked = 0.0, 0.0
        total = free + max(locked, 0.0)
        try:
            total = max(total, float(eng.ws_krw_total() or total))
        except Exception:
            pass
        budget_pool.refresh_balances(free, total_equity=max(total, free))

        before = supervisor.active_count()
        # TopN에서 빠진 워커 슬롯 회수 후 spawn
        try:
            dropped = supervisor.reap_not_in_command(trade_pool)
            if dropped:
                eng.print_log(
                    eng.LogLevel.WARNING,
                    f"AUTO TopN 제외 워커 {dropped}개 회수 (메인루프)")
        except Exception:
            pass
        pct = int(budget_pool.snapshot().get('alloc_pct', 0) * 100)
        alloc_cap = budget_pool.alloc_slots() if pool_n > 0 else 0
        for _ in range(max(0, alloc_cap)):
            if supervisor.active_count() >= hard_cap:
                break
            if supervisor.active_count() >= alloc_cap:
                break
            if supervisor.active_count() > 0 and not budget_pool.can_spawn():
                break
            w = supervisor.try_spawn()
            if not w:
                if supervisor.active_count() > 0:
                    budget_pool.assist_idle_to_workers()
                break
            eng.print_log(
                eng.LogLevel.SUCCESS,
                f"워커 spawn {w.symbol} id={w.worker_id} "
                f"한도={int(getattr(w.budget, 'allocated', 0)):,}원 "
                f"(가용KRW×{pct}%) "
                f"({supervisor.active_count()}/{hard_cap}) "
                f"[AUTO통과{pool_n}/{n} 후보{len(cand)}]")
        supervisor.tick()

        if supervisor.active_count() == 0 and before == 0:
            if not cand:
                eng.print_log(
                    eng.LogLevel.WARNING,
                    "AUTO 스캐너 후보 없음 — 시드/유동성 대기")
            elif not trade_pool:
                eng.print_log(
                    eng.LogLevel.WARNING,
                    f"AUTO 후보 {len(cand)}개 중 HybridMA+vol 통과 0 — 대기")
            time.sleep(1.0)
        else:
            time.sleep(0.25)
