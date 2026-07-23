# -*- coding: utf-8 -*-
"""Shared mutable runtime state — safe to import from every module."""
from __future__ import annotations

import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from . import config

try:
    import websocket  # noqa: F401
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

ACCESS_KEY = ""
SECRET_KEY = ""
EXCHANGE = config.EXCHANGE_CONFIGS["upbit"]
SERVER_URL = EXCHANGE["server_url"]
CANDLE_URL = ""
TRADES_URL = ""
TICKER_URL = ""
ORDERBOOK_URL = ""
TICK_CANDLE_URL = None
TICK_CANDLE_CODE = None
ORDER_URL = ""
CANCEL_URL = ""
ORDER_QUERY_URL = ""
ORDERS_UUIDS_URL = ""
ORDERS_OPEN_URL = ""
CANCEL_AND_NEW_URL = ""

VERBOSE = False
AUTO_SELECT = True
START_TIME = None
InitialBalance = 0
buy_uuids: set = set()
sell_uuids: set = set()
traded_symbols = {}
current_trading_symbol = None
symbol_cache_time = None

_buy_epoch = 0
_buy_epoch_lock = threading.Lock()
_buy_lifecycle_lock = threading.RLock()

http_session = None
http_session_slow = None
_ORDER_POOL = ThreadPoolExecutor(max_workers=12, thread_name_prefix="ord")
order_rate_limiter = None

private_ws = None
trading_manager = None

_dns_cache = {}
_dns_lock = threading.Lock()
_dns_ttl = 30.0
_orig_getaddrinfo = socket.getaddrinfo


def refresh_hot_urls():
    global ORDER_URL, CANCEL_URL, ORDER_QUERY_URL
    global ORDERS_UUIDS_URL, ORDERS_OPEN_URL, CANCEL_AND_NEW_URL
    ORDER_URL = SERVER_URL + EXCHANGE["order_endpoint"]
    CANCEL_URL = SERVER_URL + EXCHANGE["cancel_endpoint"]
    ORDER_QUERY_URL = SERVER_URL + EXCHANGE["order_query_endpoint"]
    ep = EXCHANGE.get("orders_uuids_endpoint")
    ORDERS_UUIDS_URL = (SERVER_URL + ep) if ep else ""
    op = EXCHANGE.get("orders_list_endpoint")
    ORDERS_OPEN_URL = (SERVER_URL + op) if op else ""
    cn = EXCHANGE.get("cancel_and_new_endpoint")
    CANCEL_AND_NEW_URL = (SERVER_URL + cn) if cn else ""


def apply_exchange(exchange_name: str):
    """Switch EXCHANGE profile and derived URLs."""
    global EXCHANGE, SERVER_URL, CANDLE_URL, TRADES_URL, TICKER_URL
    global ORDERBOOK_URL, TICK_CANDLE_URL, TICK_CANDLE_CODE
    if exchange_name not in config.EXCHANGE_CONFIGS:
        raise ValueError(exchange_name)
    EXCHANGE = config.EXCHANGE_CONFIGS[exchange_name]
    SERVER_URL = EXCHANGE["server_url"]
    CANDLE_URL = SERVER_URL + "/v1/candles/minutes/" + str(config.UNIT)
    TRADES_URL = SERVER_URL + "/v1/trades/ticks"
    TICKER_URL = SERVER_URL + "/v1/ticker"
    ORDERBOOK_URL = SERVER_URL + "/v1/orderbook"
    TICK_CANDLE_URL = EXCHANGE.get("tick_candle_url")
    TICK_CANDLE_CODE = EXCHANGE.get("tick_candle_code")
    refresh_hot_urls()


# default URLs
apply_exchange("upbit")
