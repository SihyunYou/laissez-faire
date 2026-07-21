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
