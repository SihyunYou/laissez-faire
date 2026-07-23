# -*- coding: utf-8 -*-
"""Static configuration and exchange profiles."""
from __future__ import annotations

HTTP_TIMEOUT = (0.4, 1.0)
HTTP_TIMEOUT_SLOW = (2.0, 8.0)
ORDER_TIMEOUT = (0.5, 1.0)

UNIT = 1
COMMISSION = 0.9995
MIN_ORDER_AMOUNT = 5000
MIN_HOLDING_VOLUME = 0.0001
STOP_LOSS_PERCENTAGE = -8.0
SPLIT_ORDER_MAX = 3
SPLIT_STEP_PERCENT = 0.2

# ── 병렬 코인 워커 ──
# ★ 규칙 (혼동 금지):
#   1) 심볼 = AUTO_SELECT: ticker/all 변동성 Top → HybridMA+vol 통과 N개 (command.txt 폐지)
#   2) 게이트 = HybridMA(틱 MA60 + 1분 MA60) + 변동성보호
#   3) 한도 = 가용 KRW(free) × WORKER_ALLOC_PCT[N] 안에서만 분할매수
#      N = 게이트통과 동시 코인 수 ≤ PARALLEL_WORKERS
#   4) 최초가용 대비 현재가용 < 50% → 신규매수 중단, 잔여→기존코인 집중
AUTO_SELECT = True
AUTO_SELECT_TOP_N = 3          # 상시 매수 코인 수
AUTO_SELECT_CANDIDATE_POOL = 20  # 게이트 검사 전 ticker 변동성 상위 후보 수
# TopN 제외 유예(초) — 순위 깜빡임으로 3슬롯이 2로 줄지 않게
TOPN_EXCLUDE_GRACE_S = 60.0
# ticker/all 1차 랭킹 갱신 주기(초) — 전종목 캔들 REST 금지
TICKER_RANK_REFRESH_S = 15.0
VOLUME_THRESHOLD_M = 5000      # 24h 거래대금 하한 (백만원) = 50억원
PARALLEL_WORKERS = 3           # spawn 상한 = AUTO_SELECT_TOP_N
# 스캐너 랭킹 로그 스로틀(초)
VOL_RANK_TTL_S = 30.0
# MA60+volatility 감시 주기(초)
MA_GATE_WATCH_INTERVAL_S = 1.0
GATE_WATCH_INTERVAL_S = MA_GATE_WATCH_INTERVAL_S
# 하이브리드 MA: 1분봉 MA 기간 + 틱·분 괴리 가중 계수
MINUTE_MA_PERIOD = 60
HYBRID_MA_DIV_K = 8.0  # 괴리율(div)당 틱 가중 감소 — 정렬 시 ≈0.5/0.5
DEEP_LADDER_LEVEL = 6
# 최초가용KRW 대비 현재가용 < 이 비율이면 신규매수 중단 → 기존 코인 집중
KRW_RESERVE_RATIO = 0.50
# N개 동시 매수 시 각 워커 한도 = 가용KRW × 아래 % (÷N 없음, 합>100% OK)
WORKER_ALLOC_PCT = {
    1: 0.9,   # 1개 → 각 가용 90%
    2: 0.8,   # 2개 → 각 가용 80%
    3: 0.7,   # 3개 → 각 가용 70%
    4: 0.6,   # 4개 → 각 가용 60%
}

EXCHANGE_CONFIGS = {
    "upbit": {
        "name": "upbit",
        "server_url": "https://api.upbit.com",
        "ws_public_url": "wss://api.upbit.com/websocket/v1",
        "ws_private_url": "wss://api.upbit.com/websocket/v1/private",
        "tick_candle_url": "https://crix-api-cdn.upbit.com/v1/crix/candles/ticks/60",
        "tick_candle_code": "CRIX.UPBIT.KRW-{symbol}",
        "order_endpoint": "/v1/orders",
        "orders_list_endpoint": "/v1/orders/open",
        "orders_uuids_endpoint": "/v1/orders/uuids",
        "orders_open_cancel_endpoint": "/v1/orders/open",
        "cancel_and_new_endpoint": "/v1/orders/cancel_and_new",
        "ticker_all_endpoint": "/v1/ticker/all",
        "cancel_endpoint": "/v1/order",
        "order_query_endpoint": "/v1/order",
        "order_type_field": "ord_type",
        "order_id_param": "uuid",
        "ws_order_id_field": "uuid",
        "ws_side_map": {"bid": "bid", "ask": "ask"},
        "mytrade_supported": True,
        "supports_batch_cancel_ids": True,
        "supports_batch_cancel_open": True,
        "supports_batch_query_ids": True,
        "supports_cancel_and_new": True,
        "supports_ticker_all": True,
    },
    "bithumb": {
        "name": "bithumb",
        "server_url": "https://api.bithumb.com",
        "ws_public_url": "wss://ws-api.bithumb.com/websocket/v1",
        "ws_private_url": "wss://ws-api.bithumb.com/websocket/v2/private",
        "tick_candle_url": None,
        "tick_candle_code": None,
        "order_endpoint": "/v2/orders",
        "orders_list_endpoint": "/v2/orders/pending",
        "orders_uuids_endpoint": None,
        "orders_open_cancel_endpoint": None,
        "cancel_and_new_endpoint": None,
        "ticker_all_endpoint": None,
        "cancel_endpoint": "/v2/order",
        "order_query_endpoint": "/v1/order",
        "order_type_field": "order_type",
        "order_id_param": "order_id",
        "order_query_id_param": "uuid",
        "ws_order_id_field": "order_id",
        "ws_side_map": {"buy": "bid", "sell": "ask"},
        "mytrade_supported": False,
        "jwt_requires_timestamp": True,
        "private_ws_jwt_alg": "HS256",
        "order_post_json": True,
        "supports_batch_cancel_ids": False,
        "supports_batch_cancel_open": False,
        "supports_batch_query_ids": False,
        "supports_cancel_and_new": False,
        "supports_ticker_all": False,
    },
}
