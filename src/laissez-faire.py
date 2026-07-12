import requests
import json
import math
import time
import os
import jwt
import uuid
import hashlib
from urllib.parse import urlencode, unquote
import winsound
import argparse
import numpy as np
import threading
from tqdm import tqdm
import datetime
from datetime import datetime, timedelta
from colorama import init, Fore, Back, Style
import traceback
from enum import Enum, IntEnum
import functools
import talib
from talib import MA_Type
import hmac
import random

# 웹소켓 라이브러리 (선택) — 미설치 시 REST 폴백
try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

init(autoreset=True)

# Configuration
UNIT = 1
SLEEP_TIME = 0.00
EXCEPTION_SLEEP_TIME = 0.25
CANDLE_URL = "https://api.upbit.com/v1/candles/minutes/" + str(UNIT)
TICKER_URL = "https://api.upbit.com/v1/ticker"
ORDERBOOK_URL = "https://api.upbit.com/v1/orderbook"
ACCESS_KEY = ''
SECRET_KEY = ''
SERVER_URL = 'https://api.upbit.com'
COMMISSION = 0.9995  # 0.05% 수수료
MIN_ORDER_AMOUNT = 5000
MIN_HOLDING_VOLUME = 0.0001  # 최소 보유 수량
STOP_LOSS_PERCENTAGE = -20.0  # 스탑로스 -20%

# 다중 분할매수 설정 — 단일 주문을 N개 가격으로 쪼개어 POST.
# SPLIT_ORDER_COUNT=1 이면 분할 없이 기존 단일 주문 동작.
# 분할 시 각 주문 금액이 MIN_ORDER_AMOUNT(5000원) 미만이면 자동으로 단일 폴백.
SPLIT_ORDER_COUNT = 3        # 분할 주문 개수 (3=삼중, 4=사중...)
SPLIT_STEP_PERCENT = 0.2     # 분할 가격 간격 (%). 기준가 중심 대칭: -0.2%, 0%, +0.2%

# Global state
InitialBalance = 0
buy_uuids = []
sell_uuids = []

# 거래 완료된 코인 저장 (1시간 동안 유효)
traded_symbols = {}

# 현재 거래 중인 코인 캐시
current_trading_symbol = None
symbol_cache_time = None
CACHE_DURATION = 3600  # 1시간 캐시

# 무시할 심볼 목록 파일
SYMIGNORE_FILE = "../symignore.txt"

class LogLevel:
    INFO = Fore.GREEN + Style.BRIGHT
    SUCCESS = Fore.LIGHTWHITE_EX + Back.LIGHTCYAN_EX + Style.BRIGHT
    WARNING = Fore.LIGHTWHITE_EX + Back.LIGHTMAGENTA_EX + Style.BRIGHT
    EXCEPTION = Fore.LIGHTYELLOW_EX + Style.BRIGHT
    ERROR = Fore.LIGHTWHITE_EX + Back.LIGHTRED_EX + Style.BRIGHT

def print_log(level, message):
    datetime_prefix = Fore.MAGENTA + Style.NORMAL
    timestamp = '[' + datetime.now().strftime('%m/%d %X') + '] '
    print(datetime_prefix + timestamp + level + message)

def log_balance(balance):
    try:
        with open("../log/balance.txt", 'w', encoding='utf-8') as f:
            f.write(str(int(balance)) + ',' + str(int(InitialBalance)))
    except PermissionError:
        pass

class LogState(IntEnum):
    ERROR = 0
    WAITING = 1
    INITIALIZING = 2
    BUYING = 3
    INVESTING = 4
    TIMEOUT = 5
    COMPLETED = 6
    FORCED_EXIT = 7

def log_state(state, additional_info=''):
    try:
        with open("../log/state.txt", 'w', encoding='utf-8') as f:
            f.write('#' + str(int(state)))
            if additional_info != '':
                f.write(',' + additional_info)
    except PermissionError:
        pass

class UpbitTickSystem:
    # 업비트 KRW 마켓 호가 단위 테이블 (2025-07-31 변경 정책 반영)
    # 출처: 업비트 개발자센터 / 고객센터 거래 이용 안내 (공식)
    # https://docs.upbit.com/kr/changelog/krw_tick_unit_change_250731
    # https://support.upbit.com/hc/ko/articles/4403838454809
    # (하한가, 호가단위) 쌍의 리스트 — 하한가 이상인 첫 구간의 호가단위를 사용.
    TICK_TABLE = [
        (2000000,    1000),  # 2,000,000원 이상
        (1000000,    1000),  # 1,000,000원 이상 ~ 2,000,000원 미만
        (500000,      500),  #   500,000원 이상 ~ 1,000,000원 미만
        (100000,      100),  #   100,000원 이상 ~   500,000원 미만
        (50000,        50),  #    50,000원 이상 ~   100,000원 미만
        (10000,        10),  #    10,000원 이상 ~    50,000원 미만
        (5000,          5),  #     5,000원 이상 ~    10,000원 미만
        (1000,          1),  #     1,000원 이상 ~     5,000원 미만
        (100,           1),  #       100원 이상 ~     1,000원 미만
        (10,          0.1),  #        10원 이상 ~       100원 미만
        (1,          0.01),  #         1원 이상 ~        10원 미만
        (0.1,        0.001), #       0.1원 이상 ~         1원 미만
        (0.01,      0.0001), #      0.01원 이상 ~       0.1원 미만
        (0.001,    0.00001), #     0.001원 이상 ~      0.01원 미만
        (0.0001,  0.000001), #    0.0001원 이상 ~     0.001원 미만
        (0.00001, 0.0000001),#   0.00001원 이상 ~    0.0001원 미만
        (0,       0.00000001),# 0.00001원 미만
    ]

    @staticmethod
    def get_minimum_tick(price):
        for lower_bound, tick in UpbitTickSystem.TICK_TABLE:
            if price >= lower_bound:
                return tick
        return UpbitTickSystem.TICK_TABLE[-1][1]

    @staticmethod
    def round_down(price, proportion):
        t = price - (price / 100) * proportion
        tick = UpbitTickSystem.get_minimum_tick(t)
        return math.floor(t / tick) * tick

    @staticmethod
    def round_up(price):
        t = price
        tick = UpbitTickSystem.get_minimum_tick(t)
        return math.ceil(t / tick) * tick
    
    @staticmethod
    def calculate_sell_price(avg_buy_price, profit_percentage):
        # 수수료 보전 제거 — 단순히 평단가 × (1 + 목표수익률%).
        # 수수료(매수+매도 0.10%)는 체결 시 업비트가 부과하므로 여기서 반영하지 않음.
        required_price = avg_buy_price * (1 + profit_percentage / 100)
        return UpbitTickSystem.round_up(required_price)
    
    @staticmethod
    def is_excluded_tick_range(price):
        if 100 <= price < 270:
            return True
        if 10.0 <= price < 27.0:
            return True
        if price < 2.70:
            return True
        return False

    @staticmethod
    def generate_split_prices(base_price, count, step_pct):
        """base_price 를 중심으로 count 개의 가격을 step_pct 간격(대칭)으로 생성.
        각각 호가단위로 snap. snap 결과 중복 가격이 생기면 분할이 의미 없으므로
        분할 금지 → [base_price] 만 반환 (단일 주문 폴백).
        Returns: list[float]
        - count 홀수: 중심 포함 대칭 (예: 3 -> [-0.25%, 0%, +0.25%])
        - count 진수: 중심 양옆 반칸 (예: 4 -> [-0.375%, -0.125%, +0.125%, +0.375%])"""
        if count <= 1:
            return [base_price]

        # 대칭 offset(%) 생성
        if count % 2 == 1:
            half = (count - 1) // 2
            offsets = [(-half + i) * step_pct for i in range(count)]
        else:
            half = count // 2
            offsets = [(-half + i + 0.5) * step_pct for i in range(count)]

        # offset 적용 → 호가단위 snap (가장 가까운 호가로 반올림)
        prices = []
        for off in offsets:
            raw = base_price * (1 + off / 100.0)
            tk = UpbitTickSystem.get_minimum_tick(raw)
            snapped = round(raw / tk) * tk
            prices.append(snapped)

        # 호가 snap 후 중복 가격이 있으면 분할이 무의미 → 분할 금지, 단일 폴백
        # 예: 150원(호가 1원) 3중분할 → 149.625/150/150.375 → snap → 150/150/150 → 중복 → 금지
        if len(set(prices)) < count:
            return [base_price]

        return sorted(prices)

class UpbitWebSocket:
    """업비트 ticker 웹소켓 스트림 — 백그라운드 스레드로 최신가 수신/캐싱.
    REST polling(GET /v1/ticker)을 대체하여 API 호출 없이 실시간 시세 제공."""
    WS_URL = "wss://api.upbit.com/websocket/v1"
    CACHE_TTL = 5.0  # 캐시 만료 시간(초) — 이 지나면 REST 폴백

    def __init__(self):
        self.ws = None
        self.thread = None
        self.price_cache = {}        # {symbol: latest_price}
        self.cache_timestamp = {}    # {symbol: timestamp}
        self.current_symbol = None
        self.is_connected = False
        self._should_reconnect = True

    def subscribe(self, symbol):
        """심볼 구독 시작. 기존 연결이 있으면 종료 후 새 심볼로 재연결."""
        if self.current_symbol == symbol and self.is_connected:
            return  # 이미 같은 심볼 구독 중
        self.current_symbol = symbol
        # 기존 연결 종료
        self._should_reconnect = False
        if self.ws:
            try:
                self.ws.close()
            except:
                pass
        # 새 연결 시작
        self._should_reconnect = True
        self.thread = threading.Thread(target=self._connect_loop, daemon=True)
        self.thread.start()
        print_log(LogLevel.INFO, f"WebSocket ticker 구독 시작: KRW-{symbol}")

    def get_price(self, symbol):
        """캐시에서 최신가 조회. 캐시 만료 시 None 반환(호출자가 REST 폴백)."""
        if symbol not in self.price_cache:
            return None
        ts = self.cache_timestamp.get(symbol, 0)
        if (datetime.now().timestamp() - ts) > self.CACHE_TTL:
            return None  # 만료 — REST 폴백
        return self.price_cache[symbol]

    def _connect_loop(self):
        """백그라운드 스레드 — 재연결 루프."""
        backoff = 1
        while self._should_reconnect and self.current_symbol:
            try:
                self.ws = websocket.WebSocketApp(
                    self.WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                self.ws.run_forever(ping_interval=60, ping_timeout=10)
            except Exception as e:
                print_log(LogLevel.WARNING, f"WebSocket 연결 오류: {str(e)}")
            # 재연결 대기
            if self._should_reconnect:
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)  # 최대 30초

    def _on_open(self, ws):
        self.is_connected = True
        code = f"KRW-{self.current_symbol}"
        req = [{"ticket": f"laissez-faire-{int(time.time())}"},
               {"type": "ticker", "codes": [code]}]
        ws.send(json.dumps(req))
        print_log(LogLevel.SUCCESS, f"WebSocket 연결 성공 — {code} ticker 수신 대기")

    def _on_message(self, ws, message):
        """ticker 메시지 파싱 → 캐시 갱신."""
        try:
            data = json.loads(message.decode('utf-8')) if isinstance(message, (bytes, bytearray)) else json.loads(message)
            code = data.get('code', '')
            if code.startswith('KRW-'):
                symbol = code[4:]
                price = data.get('trade_price')
                if price:
                    self.price_cache[symbol] = float(price)
                    self.cache_timestamp[symbol] = datetime.now().timestamp()
        except Exception as e:
            pass  # 파싱 오류는 조용히 무시

    def _on_error(self, ws, error):
        self.is_connected = False
        print_log(LogLevel.WARNING, f"WebSocket 에러: {str(error)[:100]}")

    def _on_close(self, ws, close_status, close_msg):
        self.is_connected = False
        if self._should_reconnect:
            print_log(LogLevel.INFO, "WebSocket 연결 종료 — 재연결 대기")


class RealMarketData:
    # 웹소켓 싱글톤 (라이브러리 사용 가능 시만)
    _ws = UpbitWebSocket() if WEBSOCKET_AVAILABLE else None

    @staticmethod
    def get_current_price(symbol):
        # 웹소켓 캐시 우선 (핫 루프 최적화 — API 호출 없음)
        if RealMarketData._ws:
            cached = RealMarketData._ws.get_price(symbol)
            if cached is not None:
                return cached
        # 폴백: 기존 REST 코드 (웹소켓 미가동/캐시 만료 시)
        try:
            def api_call():
                url = f"{TICKER_URL}?markets=KRW-{symbol}"
                headers = {"Accept": "application/json"}
                response = requests.get(url, headers=headers, timeout=10)

                if response.status_code == 200:
                    data = response.json()
                    if data and len(data) > 0:
                        return float(data[0]['trade_price'])
                elif response.status_code == 429:
                    print_log(LogLevel.WARNING, "API rate limit exceeded")
                    time.sleep(1)
                    return None
                else:
                    print_log(LogLevel.WARNING, f"Failed to get current price: {response.status_code}")
                    return None

            return safe_api_call(api_call)
        except Exception as e:
            print_log(LogLevel.EXCEPTION, f"Error getting current price: {str(e)}")
            return None

    @staticmethod
    def subscribe_websocket(symbol):
        """웹소켓 구독 시작 (심볼 확정 시 호출)."""
        if RealMarketData._ws:
            RealMarketData._ws.subscribe(symbol)

class RateLimiter:
    def __init__(self, interval):
        self.interval = interval
        self.last_call = 0
        self.lock = threading.Lock()
    
    def acquire(self):
        with self.lock:
            current_time = time.time()
            elapsed = current_time - self.last_call
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)
            self.last_call = time.time()

rate_limiter = RateLimiter(SLEEP_TIME)

def safe_api_call(func, *args, **kwargs):
    """네트워크 오류 시 무한 재시도 — 프로그램이 종료되지 않고 복구 대기.
    백오프는 지수(exponential)로 증가하되 최대 30초로 상한."""
    max_backoff = 30.0
    attempt = 0

    while True:
        try:
            rate_limiter.acquire()
            result = func(*args, **kwargs)
            return result

        except requests.exceptions.RequestException as e:
            wait_time = min(SLEEP_TIME * (2 ** attempt), max_backoff)
            attempt += 1
            print_log(LogLevel.WARNING,
                      f"API call failed (attempt {attempt}), retrying in {wait_time:.2f}s: {str(e)}")
            time.sleep(wait_time)



class OrderCanceler:
    def cancel_buy_orders(self):
        global buy_uuids

        while True:
            try:
                for uuid_val in buy_uuids:
                    self.cancel_order(uuid_val)
                buy_uuids.clear()
                return
            except:
                print_log(LogLevel.EXCEPTION, "Failed to cancel buy orders")
                time.sleep(EXCEPTION_SLEEP_TIME)
            
    def cancel_sell_orders(self):
        global sell_uuids

        while True:
            try:
                for uuid_val in sell_uuids:
                    self.cancel_order(uuid_val)
                sell_uuids.clear()
                return
            except:
                print_log(LogLevel.EXCEPTION, "Failed to cancel sell orders")
                time.sleep(EXCEPTION_SLEEP_TIME)

    def cancel_all_orders(self, cancel_type):
        params = {'state': 'wait'}
        query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")

        m = hashlib.sha512()
        m.update(query_string)
        query_hash = m.hexdigest()

        payload = {
            'access_key': ACCESS_KEY,
            'nonce': str(uuid.uuid4()),
            'query_hash': query_hash,
            'query_hash_alg': 'SHA512',
        }

        jwt_token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
        authorization = f'Bearer {jwt_token}'
        headers = {'Authorization': authorization, 'Accept': 'application/json'}

        def api_call():
            response = requests.get(SERVER_URL + '/v1/orders', params=params, headers=headers)
            return response.json()

        try:
            response_dict = safe_api_call(api_call)
        except Exception as e:
            print_log(LogLevel.ERROR, f"Failed to fetch orders: {str(e)}")
            return

        if isinstance(response_dict, list):
            orders = response_dict
        elif isinstance(response_dict, dict) and 'error' in response_dict:
            print_log(LogLevel.ERROR, f"API Error: {response_dict['error']}")
            return
        else:
            print_log(LogLevel.WARNING, f"Unexpected API response format: {response_dict}")
            return

        cancelled_count = 0
        if cancel_type == 1:
            for order in orders:
                if isinstance(order, dict) and order.get('side') == 'bid':
                    if self.cancel_order(order.get('uuid')):
                        cancelled_count += 1
        elif cancel_type == 2:
            for order in orders:
                if isinstance(order, dict) and order.get('side') == 'ask':
                    if self.cancel_order(order.get('uuid')):
                        cancelled_count += 1
        elif cancel_type == 3:
            for order in orders:
                if isinstance(order, dict):
                    if self.cancel_order(order.get('uuid')):
                        cancelled_count += 1

        print_log(LogLevel.INFO, f"Cancelled {cancelled_count} orders (type: {cancel_type})")

    def cancel_order(self, order_uuid):
        if not order_uuid:
            return False

        params = {'uuid': order_uuid}
        query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")

        m = hashlib.sha512()
        m.update(query_string)
        query_hash = m.hexdigest()

        payload = {
            'access_key': ACCESS_KEY,
            'nonce': str(uuid.uuid4()),
            'query_hash': query_hash,
            'query_hash_alg': 'SHA512',
        }

        jwt_token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
        authorization = f'Bearer {jwt_token}'
        headers = {"Authorization": authorization, "Accept": "application/json"}

        # 1회 시도 후 실패하면 그냥 패스 (재시도 없음)
        try:
            def api_call():
                response = requests.delete(SERVER_URL + "/v1/order", params=params, headers=headers)
                return response

            response = safe_api_call(api_call)
            if response.status_code == 200:
                print_log(LogLevel.INFO, f"Successfully cancelled order: {order_uuid}")
                return True
            else:
                # 400/401/404 등 — 이미 체결/취소/없는 주문은 그냥 패스
                print_log(LogLevel.INFO, f"Order {order_uuid} cancel skipped ({response.status_code})")
                return True
        except Exception as e:
            print_log(LogLevel.WARNING, f"Cancel error {order_uuid}: {str(e)} — skipped")
            return True

class AccountChecker:
    def __init__(self):
        payload = {
            'access_key': ACCESS_KEY,
            'nonce': str(uuid.uuid4()),
        }

        jwt_token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
        authorization = f'Bearer {jwt_token}'
        headers = {"Authorization": authorization}

        def api_call():
            response = requests.get(SERVER_URL + "/v1/accounts", headers=headers)
            return response.json()
        
        self.response_dict = safe_api_call(api_call)

    def get_krw_balance(self, balance_type=1):
        for account in self.response_dict:
            if account.get('currency') == "KRW":
                if balance_type == 1:
                    return float(account.get('balance'))
                elif balance_type == 2:
                    return float(account.get('locked'))
                elif balance_type == 3:
                    return float(account.get('balance')) + float(account.get('locked'))
        return 0

    def get_owned_symbols(self):
        symbols = []
        for account in self.response_dict:
            symbols.append(account.get('currency'))
        return symbols

    def get_symbol_info(self, symbol):
        for account in self.response_dict:
            if account.get('currency') == symbol:
                balance = float(account.get('balance'))
                locked = float(account.get('locked'))
                avg_buy_price = float(account.get('avg_buy_price'))
                return balance, locked, avg_buy_price
        return -1, -1, -1

class DynamicBuyOrder:
    """동적 분할 매수 주문 관리 클래스 - 저가 기준 재설계"""
    
    def __init__(self, symbol, current_price, low_price, total_amount, weight, exclude_count=0):
        self.symbol = symbol
        self.original_price = current_price + UpbitTickSystem.get_minimum_tick(current_price)
        self.current_price = current_price
        self.low_price = low_price  # 저가를 별도로 받음
        self.total_amount = total_amount
        self.weight = weight
        self.exclude_count = exclude_count
        
        # 상태 관리
        self.is_active = False
        self.last_order_time = None
        self.last_check_time = None
        self.first_order_start_time = None
        self.first_order_timeout = 7
        
        # 계획 관리
        self.original_planned_orders = []
        self.active_planned_orders = []
        self.executed_orders = []
        self.pending_orders = []
        
        # 밀림 관리
        self.plan_shift_amount = 0.0
        self.last_shift_check_price = current_price
        
        # OrderCanceler 인스턴스
        self.order_canceler = OrderCanceler()

    class DistributionType(Enum):
        LINEAR = 1
        LOG_LINEAR_II = 2
        LOG_LINEAR_I = 3
        PARABOLIC_II = 4
        PARABOLIC_I = 5
        EXPONENTIAL = 6
        FIBONACCI = 7

    def calculate_order_plan(self, drop_percentage, drop_count, distribution_type):
        """주문 계획 계산 - 저가 기준"""
        print_log(LogLevel.INFO, f"Starting order plan calculation for {self.symbol} based on low price {self.low_price:.4f}")
        
        self.original_planned_orders = []
        self.active_planned_orders = []
        
        if distribution_type == self.DistributionType.LINEAR:
            self._calculate_linear_plan(drop_percentage, drop_count)
        elif distribution_type == self.DistributionType.LOG_LINEAR_II:
            self._calculate_log_linear_plan(drop_percentage, drop_count, 3)
        elif distribution_type == self.DistributionType.LOG_LINEAR_I:
            self._calculate_log_linear_plan(drop_percentage, drop_count, 2)
        elif distribution_type == self.DistributionType.PARABOLIC_II:
            self._calculate_parabolic2_plan(drop_percentage, drop_count)
        elif distribution_type == self.DistributionType.PARABOLIC_I:
            self._calculate_parabolic_plan(drop_percentage, drop_count)
        elif distribution_type == self.DistributionType.EXPONENTIAL:
            self._calculate_exponential_plan(drop_percentage, drop_count, 1.2)
        elif distribution_type == self.DistributionType.FIBONACCI:
            self._calculate_fibonacci_plan(drop_percentage, drop_count)
        else:
            self._calculate_linear_plan(drop_percentage, drop_count)

        # 분할매수 적용 시 각 레벨을 '이전 레벨 분할 최저가' 기준으로 재계산
        self._adjust_to_split_lowest_base(drop_percentage)

        # 인접 주문 가격 중복 시 호가 최소단위만큼 강제 하락 (예: 150/150 → 150/149)
        self._enforce_min_tick_gap()

        self.original_planned_orders = [order.copy() for order in self.active_planned_orders]
        print_log(LogLevel.SUCCESS, f"Calculated {len(self.active_planned_orders)} buy orders for {self.symbol} based on low price {self.low_price:.4f}")

    def _adjust_to_split_lowest_base(self, drop_percentage):
        """다중 분할매수(SPLIT_ORDER_COUNT>1)일 때, 각 레벨의 기준가를
        '이전 레벨 분할의 최저가'를 출발점으로 재계산.
        분할 최저가 = planned_price * (1 - 하단 최대 offset%).
        레벨 n+1 의 간격은 레벨 n 의 분할 최저가에서 drop%*height_weight 만큼 하락.
        SPLIT_ORDER_COUNT<=1 이면 아무 것도 하지 않음(기존 동작)."""
        if SPLIT_ORDER_COUNT <= 1 or not self.active_planned_orders:
            return

        # 분할 하단 최대 offset(%). 3중 분할 대칭 → -0.25%.
        if SPLIT_ORDER_COUNT % 2 == 1:
            half = (SPLIT_ORDER_COUNT - 1) // 2
        else:
            half = SPLIT_ORDER_COUNT // 2
        low_offset_pct = half * SPLIT_STEP_PERCENT

        weight = self.weight
        # 첫 레벨은 그대로, 이후 레벨은 직전 분할 최저가에서 간격만큼 하락
        curr_base = self.active_planned_orders[0]['planned_price']
        for i in range(1, len(self.active_planned_orders)):
            order = self.active_planned_orders[i]
            n = order['level']
            height_weight = 1 + weight * (n - 1)
            # 이전 레벨의 분할 최저가(예상) — 직전 planned_price 기준
            prev_planned = self.active_planned_orders[i - 1]['planned_price']
            prev_split_lowest = prev_planned * (1 - low_offset_pct / 100.0)
            # 출발점(prev_split_lowest)에서 drop%*hw 만큼 하락한 가격을 새 기준가로
            new_price = UpbitTickSystem.round_down(prev_split_lowest, drop_percentage * height_weight)
            order['planned_price'] = new_price
            order['original_planned_price'] = new_price
            order['volume'] = order['quantity'] / new_price if new_price > 0 else 0

    def _enforce_min_tick_gap(self):
        """인접 분할 매수 주문이 같은 가격(또는 더 높은 가격)이면, n+1번째를
        직전 가격에서 호가 최소단위 1개만큼 낮춰 강제 하락시킨다.
        예: 150원 코인(호가 1원)에서 [150, 150] → [150, 149]."""
        for i in range(1, len(self.active_planned_orders)):
            prev = self.active_planned_orders[i - 1]
            curr = self.active_planned_orders[i]
            if curr['planned_price'] >= prev['planned_price']:
                tick = UpbitTickSystem.get_minimum_tick(prev['planned_price'])
                new_price = prev['planned_price'] - tick
                old_price = curr['planned_price']
                curr['planned_price'] = new_price
                curr['original_planned_price'] = new_price
                curr['volume'] = curr['quantity'] / new_price if new_price > 0 else 0
                print_log(LogLevel.INFO,
                          f"Enforced min tick gap at level {curr['level']}: "
                          f"{old_price} -> {new_price} (tick={tick})")

    def _calculate_linear_plan(self, drop_percentage, drop_count):
        total_weight = sum(range(1, drop_count + 1))
        
        for n in range(1, drop_count + 1 - self.exclude_count):
            height_weight = 1 + self.weight * (n - 1)
            planned_price = UpbitTickSystem.round_down(UpbitTickSystem.round_up(self.original_price), (n - 1) * (drop_percentage * height_weight))
            quantity = self.total_amount * n / total_weight
            
            self.active_planned_orders.append({
                'level': n,
                'original_planned_price': planned_price,
                'planned_price': planned_price,
                'quantity': quantity,
                'volume': quantity / planned_price,
                'executed': False,
                'shift_applied': 0.0
            })

    def _calculate_log_linear_plan(self, drop_percentage, drop_count, weight):
        total_weight = sum(n * math.log(n + weight) for n in range(1, drop_count + 1))
        
        for n in range(1, drop_count + 1 - self.exclude_count):
            height_weight = 1 + self.weight * (n - 1)
            planned_price = UpbitTickSystem.round_down(UpbitTickSystem.round_up(self.original_price), (n - 1) * (drop_percentage * height_weight))
            quantity = self.total_amount * (n * math.log(n + weight)) / total_weight
            
            self.active_planned_orders.append({
                'level': n,
                'original_planned_price': planned_price,
                'planned_price': planned_price,
                'quantity': quantity,
                'volume': quantity / planned_price,
                'executed': False,
                'shift_applied': 0.0
            })

    def _calculate_parabolic_plan(self, drop_percentage, drop_count):
        total_weight = drop_count * (pow(drop_count, 2) + 5) / 6
        
        for n in range(1, drop_count + 1 - self.exclude_count):
            height_weight = 1 + self.weight * (n - 1)
            planned_price = UpbitTickSystem.round_down(UpbitTickSystem.round_up(self.original_price), (n - 1) * (drop_percentage * height_weight))
            weight_factor = (pow(n, 2) / 2) - (n / 2) + 1
            quantity = self.total_amount * weight_factor / total_weight
            
            self.active_planned_orders.append({
                'level': n,
                'original_planned_price': planned_price,
                'planned_price': planned_price,
                'quantity': quantity,
                'volume': quantity / planned_price,
                'executed': False,
                'shift_applied': 0.0
            })

    def _calculate_parabolic2_plan(self, drop_percentage, drop_count):
        total_weight = drop_count * (5 * pow(drop_count, 2) + 15 * drop_count + 40) / 6
        
        for n in range(1, drop_count + 1 - self.exclude_count):
            height_weight = 1 + self.weight * (n - 1)
            planned_price = UpbitTickSystem.round_down(UpbitTickSystem.round_up(self.original_price), (n - 1) * (drop_percentage * height_weight))
            weight_factor = 5 / 2 * pow(n, 2) + 5 / 2 * n + 5
            quantity = self.total_amount * weight_factor / total_weight
            
            self.active_planned_orders.append({
                'level': n,
                'original_planned_price': planned_price,
                'planned_price': planned_price,
                'quantity': quantity,
                'volume': quantity / planned_price,
                'executed': False,
                'shift_applied': 0.0
            })

    def _calculate_exponential_plan(self, drop_percentage, drop_count, exponent):
        h = drop_count
        r = exponent
        a = self.total_amount * (r - 1) / (pow(r, h) - 1)

        for n in range(1, drop_count + 1 - self.exclude_count):
            height_weight = 1 + self.weight * (n - 1)
            planned_price = UpbitTickSystem.round_down(UpbitTickSystem.round_up(self.original_price), (n - 1) * (drop_percentage * height_weight))
            quantity = a * pow(r, n - 1)
            
            self.active_planned_orders.append({
                'level': n,
                'original_planned_price': planned_price,
                'planned_price': planned_price,
                'quantity': quantity,
                'volume': quantity / planned_price,
                'executed': False,
                'shift_applied': 0.0
            })

    def _calculate_fibonacci_plan(self, drop_percentage, drop_count):
        fibonacci = [1, 1, 2, 2, 3, 3, 5, 5, 8, 8, 13, 13, 21, 21, 34, 34, 55, 55, 89, 89, 144, 144, 233, 233, 377, 377, 610, 610, 987, 987]
        my_fibonacci = fibonacci[:drop_count]
        total_fibonacci = sum(my_fibonacci)

        for n in range(1, drop_count + 1 - self.exclude_count):
            height_weight = 1 + self.weight * (n - 1)
            planned_price = UpbitTickSystem.round_down(UpbitTickSystem.round_up(self.original_price), (n - 1) * (drop_percentage * height_weight))
            quantity = self.total_amount * fibonacci[n - 1] / total_fibonacci
            
            self.active_planned_orders.append({
                'level': n,
                'original_planned_price': planned_price,
                'planned_price': planned_price,
                'quantity': quantity,
                'volume': quantity / planned_price,
                'executed': False,
                'shift_applied': 0.0
            })

    def _calculate_required_shift(self, current_low_price):
        """필요한 밀림량 계산 - 저가 기준"""
        required_shift = 0.0
        
        for order in self.active_planned_orders:
            if not order['executed'] and order['planned_price'] > current_low_price:
                gap = order['planned_price'] - current_low_price
                if gap > required_shift:
                    required_shift = gap
        
        # 최소 밀림량 체크 (현재 심볼의 호가단위 기준 — 7/31 호가정책 반영)
        min_shift = UpbitTickSystem.get_minimum_tick(self.low_price) if self.low_price else 0.0001
        if required_shift < min_shift:
            return 0.0

        return required_shift

    def _apply_plan_shift(self, shift_amount):
        """계획 밀림 적용 - 저가 기준"""
        print_log(LogLevel.INFO, f"Applying plan shift: {shift_amount:.4f} {self.symbol}")
        
        # 모든 미체결 주문에 밀림 적용
        for order in self.active_planned_orders:
            if not order['executed']:
                new_planned_price = UpbitTickSystem.round_down(order['original_planned_price'] - shift_amount, 0)
                
                order['planned_price'] = new_planned_price
                order['shift_applied'] = shift_amount
                order['volume'] = order['quantity'] / order['planned_price'] if order['planned_price'] > 0 else 0
        
        self.plan_shift_amount = shift_amount
        print_log(LogLevel.SUCCESS, f"✅ Plan shifted by {shift_amount:.4f} {self.symbol}")

    def execute_dynamic_buy_orders(self):
        """동적 매수 시작"""
        if not self.active_planned_orders:
            print_log(LogLevel.ERROR, "No planned orders to execute")
            return False
            
        self.is_active = True
        self.pending_orders.clear()
        self.plan_shift_amount = 0.0
        self.first_order_start_time = datetime.now()
        
        print_log(LogLevel.INFO, f"Starting dynamic buying with {len(self.active_planned_orders)} planned orders based on low price {self.low_price:.4f}")
        
        # 바로 첫 주문 실행
        return self._execute_next_available_order()

    def check_and_continue(self):
        """체결 확인 및 다음 주문 실행 - 저가 기준"""
        if not self.is_active:
            return False
            
        current_time = datetime.now()
        
        # 체크 간격 제한
        if self.last_check_time and (current_time - self.last_check_time).total_seconds() < SLEEP_TIME:
            return False
        self.last_check_time = current_time
        
        # 현재가와 저가 확인
        current_price = RealMarketData.get_current_price(self.symbol)
        if not current_price:
            return False
            
        self.current_price = current_price
        
        # 계획 밀림 확인 (현재가 기준)
        self._check_and_apply_plan_shift(current_price)
        
        # 첫 주문 타임아웃 체크
        if (self.first_order_start_time and 
            len(self.executed_orders) == 0 and 
            len(self.pending_orders) > 0):
            
            elapsed_seconds = (current_time - self.first_order_start_time).total_seconds()
            if elapsed_seconds > self.first_order_timeout:
                print_log(LogLevel.WARNING, f"First order timeout after {elapsed_seconds:.0f} seconds")
                self.cancel_all_pending_orders()
                self.is_active = False
                return False
        
        # 체결 확인
        has_new_execution = self._check_order_execution()
        
        # 체결되었거나 대기 주문이 없으면 다음 주문 실행
        if has_new_execution or len(self.pending_orders) == 0:
            return self._execute_next_available_order()
        
        return False

    def _check_and_apply_plan_shift(self, current_price):
        """계획 밀림 확인 및 적용 - 현재가 기준"""
        required_shift = self._calculate_required_shift(current_price)
        
        if required_shift > 0:
            # ✅ 체결되지 않은 대기 주문만 취소
            orders_to_cancel = []
            for pending_order in self.pending_orders:
                order_uuid = pending_order.get('uuid')
                if order_uuid:
                    # 주문 상태를 먼저 확인하여 체결 여부 확인
                    order_info = self._get_order_info(order_uuid)
                    if order_info and order_info.get('state') != 'done':
                        orders_to_cancel.append(pending_order)
            
            if orders_to_cancel:
                print_log(LogLevel.INFO, f"Cancelling {len(orders_to_cancel)} pending orders due to plan shift")
                for pending_order in orders_to_cancel:
                    order_uuid = pending_order.get('uuid')
                    if order_uuid:
                        self._cancel_single_order(order_uuid)
                        # 글로벌 UUID에서도 제거
                        global buy_uuids
                        if order_uuid in buy_uuids:
                            buy_uuids.remove(order_uuid)
                
                # 취소된 주문만 pending_orders에서 제거
                self.pending_orders = [p for p in self.pending_orders if p not in orders_to_cancel]
            
            # 밀림 적용
            self._apply_plan_shift(required_shift)
            
            # ✅ 핵심: 밀림 적용 후 바로 다음 주문 실행 시도
            self._execute_next_available_order()

    def _execute_next_available_order(self):
        """다음 실행 가능한 주문 실행"""
        if not self.is_active:
            return False
            
        # 이미 대기 중인 주문이 있으면 실행하지 않음
        if self.pending_orders:
            return False
            
        # 실행할 다음 주문 찾기
        for order in sorted(self.active_planned_orders, key=lambda x: x['level']):
            if not order['executed'] and not self._is_order_pending(order['level']):
                return self._execute_single_order(order)
        
        # 모든 주문 완료
        if all(order['executed'] for order in self.active_planned_orders):
            print_log(LogLevel.SUCCESS, "All planned orders executed!")
            self.is_active = False
            return False
            
        return False

    def _execute_single_order(self, order):
        """주문 실행 — 다중 분할매수(SPLIT_ORDER_COUNT) 지원.
        기준가(planned_price)를 N개 가격으로 쪼개어 각각 POST.
        각 주문 금액이 MIN_ORDER_AMOUNT 미만이면 분할 금지하고 단일 주문으로 폴백."""
        current_price = RealMarketData.get_current_price(self.symbol)
        if not current_price:
            return False

        order_price = order['planned_price']
        order_volume = order['volume']

        # 분할 가격 생성 (기준가 중심 대칭, 호가 snap, 중복 시 강제 분리)
        split_prices = UpbitTickSystem.generate_split_prices(
            order_price, SPLIT_ORDER_COUNT, SPLIT_STEP_PERCENT)

        # 최소주문금액 검사 — 분할 시 각 주문이 5000원 미만이면 분할 금지(단일 폴백)
        per_volume = order_volume / len(split_prices) if split_prices else order_volume
        per_amount = per_volume * order_price
        if per_amount < MIN_ORDER_AMOUNT and len(split_prices) > 1:
            print_log(LogLevel.INFO,
                      f"Order {order['level']} split skipped — per-order amount "
                      f"{per_amount:,.0f} KRW < {MIN_ORDER_AMOUNT} (단일 주문으로 폴백)")
            split_prices = [order_price]
            per_volume = order_volume

        print_log(LogLevel.INFO,
                 f"🎯 Executing order {order['level']} - "
                 f"Base Price: {order_price:.4f} KRW, "
                 f"Total Volume: {order_volume:.6f}, "
                 f"Split: {len(split_prices)} (per {per_volume:.6f} @ {[round(p,8) for p in split_prices]})")

        # 각 가격으로 주문 POST, 성공한 만큼 pending_orders 에 개별 추적
        success_count = 0
        for idx, sp in enumerate(split_prices):
            order_uuid = self.place_dynamic_buy_order(sp, per_volume)
            if order_uuid:
                pending_order = {
                    'level': order['level'],
                    'planned_price': order['planned_price'],
                    'actual_price': sp,
                    'volume': per_volume,
                    'order_time': datetime.now(),
                    'uuid': order_uuid,
                    'split_idx': idx,
                    'split_total': len(split_prices),
                }
                self.pending_orders.append(pending_order)
                success_count += 1
            else:
                print_log(LogLevel.ERROR, f"❌ Failed to place split order {order['level']}-{idx+1}/{len(split_prices)}")

        if success_count > 0:
            print_log(LogLevel.SUCCESS,
                      f"✅ Order {order['level']} placed ({success_count}/{len(split_prices)} splits)")
            return True
        else:
            print_log(LogLevel.ERROR, f"❌ Failed to place order {order['level']} (all splits failed)")
            return False

    def _is_order_executed(self, pending_order):
        """주문 체결 여부 확인"""
        order_uuid = pending_order.get('uuid')
        if not order_uuid:
            return False
        
        try:
            order_info = self._get_order_info(order_uuid)
            if order_info:
                state = order_info.get('state')
                executed_volume = float(order_info.get('executed_volume', 0))
                return state == 'done' and executed_volume > 0
            return False
        except Exception as e:
            print_log(LogLevel.ERROR, f"Error checking order execution: {str(e)}")
            return False

    def _check_order_execution(self):
        """주문 체결 확인"""
        if not self.pending_orders:
            return False
        
        executed_any = False
        
        for i in range(len(self.pending_orders) - 1, -1, -1):
            pending_order = self.pending_orders[i]
            order_uuid = pending_order.get('uuid')
            
            if not order_uuid:
                continue
                
            try:
                order_info = self._get_order_info(order_uuid)
                if order_info:
                    state = order_info.get('state')
                    executed_volume = float(order_info.get('executed_volume', 0))
                    
                    if state == 'done' and executed_volume > 0:
                        # 체결된 주문 처리
                        self._process_executed_order(pending_order, i, order_info)
                        executed_any = True
                    elif state == 'cancel':
                        # 취소된 주문은 pending에서 제거
                        self.pending_orders.pop(i)
                        print_log(LogLevel.INFO, f"Order {pending_order['level']} was cancelled")
                        
            except Exception as e:
                print_log(LogLevel.ERROR, f"Error checking order execution: {str(e)}")
                continue
        
        return executed_any

    def _process_executed_order(self, pending_order, pending_index, order_info):
        """체결된 주문 처리"""
        order_uuid = pending_order.get('uuid')
        
        try:
            executed_volume = float(order_info.get('executed_volume', 0))
            executed_funds = float(order_info.get('executed_funds', 0))
            avg_executed_price = executed_funds / executed_volume if executed_volume > 0 else pending_order['actual_price']
            
            executed_order = {
                'level': pending_order['level'],
                'planned_price': pending_order['planned_price'],
                'executed_price': avg_executed_price,
                'quantity': executed_funds,
                'volume': executed_volume,
                'uuid': order_uuid,
                'executed_time': datetime.now()
            }
            
            self.executed_orders.append(executed_order)
            
            for order in self.active_planned_orders:
                if order['level'] == pending_order['level']:
                    order['executed'] = True
                    break
            
            self.pending_orders.pop(pending_index)
            
            global buy_uuids
            if order_uuid in buy_uuids:
                buy_uuids.remove(order_uuid)
            
            print_log(LogLevel.SUCCESS, 
                     f"✅ Order {pending_order['level']} executed! "
                     f"Price: {avg_executed_price:.4f} {self.symbol}, "
                     f"Volume: {executed_volume:.6f}")
                        
        except Exception as e:
            print_log(LogLevel.ERROR, f"Error processing executed order: {str(e)}")

    def _is_order_pending(self, level):
        """주문 대기 중인지 확인"""
        return any(pending['level'] == level for pending in self.pending_orders)

    def place_dynamic_buy_order(self, price, volume):
        """매수 주문 실행"""
        global buy_uuids
        
        current_time = datetime.now()
        if self.last_order_time is not None:
            time_since_last = (current_time - self.last_order_time).total_seconds()
            if time_since_last < SLEEP_TIME:
                time.sleep(SLEEP_TIME - time_since_last)
        
        self.last_order_time = datetime.now()
        
        query = {
            'market': "KRW-" + self.symbol,
            'side': 'bid',
            'volume': str(volume), 
            'price': str(price),
            'ord_type': 'limit',
        }

        query_string = unquote(urlencode(query, doseq=True)).encode("utf-8")
        m = hashlib.sha512()
        m.update(query_string)
        query_hash = m.hexdigest()

        payload = {
            'access_key': ACCESS_KEY,
            'nonce': str(uuid.uuid4()),
            'query_hash': query_hash,
            'query_hash_alg': 'SHA512',
        }

        jwt_token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
        authorization = f'Bearer {jwt_token}'
        headers = {"Authorization": authorization, "Accept": "application/json"}

        try:
            def api_call():
                response = requests.post(SERVER_URL + "/v1/orders", params=query, headers=headers)
                return response.json()
            
            response_dict = safe_api_call(api_call)
            if 'uuid' in response_dict:
                order_uuid = response_dict['uuid']
                buy_uuids.append(order_uuid)
                print_log(LogLevel.INFO, f"💰 Buy order placed at {price:.4f} KRW, Volume: {volume:.6f}")
                return order_uuid
            else:
                print_log(LogLevel.ERROR, f"Failed to place buy order: {response_dict}")
                return None
        except Exception as e:
            print_log(LogLevel.ERROR, f"Error placing buy order: {str(e)}")
            return None

    def cancel_all_pending_orders(self):
        """✅ 대기 중인 주문만 취소 (체결된 주문은 제외)"""
        print_log(LogLevel.INFO, f"Cancelling pending orders for {self.symbol}")
        
        orders_to_cancel = []
        for pending_order in self.pending_orders:
            order_uuid = pending_order.get('uuid')
            if order_uuid and not self._is_order_executed(pending_order):
                orders_to_cancel.append(pending_order)
        
        cancelled_count = 0
        for pending_order in orders_to_cancel:
            order_uuid = pending_order.get('uuid')
            if order_uuid:
                if self._cancel_single_order(order_uuid):
                    cancelled_count += 1
                    # 글로벌 UUID에서도 제거
                    global buy_uuids
                    if order_uuid in buy_uuids:
                        buy_uuids.remove(order_uuid)
        
        # 취소된 주문만 pending_orders에서 제거
        self.pending_orders = [p for p in self.pending_orders if p not in orders_to_cancel]
        
        print_log(LogLevel.INFO, f"Cancelled {cancelled_count} pending orders, {len(self.pending_orders)} remaining")

    def _cancel_single_order(self, order_uuid):
        """단일 주문 취소 - OrderCanceler 사용"""
        try:
            if order_uuid:
                success = self.order_canceler.cancel_order(order_uuid)
                if success:
                    print_log(LogLevel.INFO, f"Order {order_uuid[:8]}... cancelled")
                    return True               
                print_log(LogLevel.WARNING, f"Failed to cancel order {order_uuid[:8]}...")

            return False
        except Exception as e:
            print_log(LogLevel.ERROR, f"Error cancelling order {order_uuid[:8]}...: {str(e)}")
            return False

    def _get_order_info(self, order_uuid):
        """주문 정보 조회"""
        try:
            params = {'uuid': order_uuid}
            query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")
            m = hashlib.sha512()
            m.update(query_string)
            query_hash = m.hexdigest()

            payload = {
                'access_key': ACCESS_KEY,
                'nonce': str(uuid.uuid4()),
                'query_hash': query_hash,
                'query_hash_alg': 'SHA512',
            }

            jwt_token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
            authorization = f'Bearer {jwt_token}'
            headers = {"Authorization": authorization, "Accept": "application/json"}

            def api_call():
                response = requests.get(SERVER_URL + "/v1/order", params=params, headers=headers)
                return response.json()
            
            response_dict = safe_api_call(api_call)
            return response_dict
                
        except Exception as e:
            print_log(LogLevel.ERROR, f"Error getting order info: {str(e)}")
            return None

    def stop_trading(self):
        """거래 중지"""
        self.is_active = False
        self.cancel_all_pending_orders()
        print_log(LogLevel.INFO, f"Trading stopped for {self.symbol}")

    def get_status(self):
        """상태 정보"""
        total_executed_quantity = sum(order['quantity'] for order in self.executed_orders)
        total_executed_volume = sum(order['volume'] for order in self.executed_orders)
        
        return {
            'symbol': self.symbol,
            'is_active': self.is_active,
            'current_price': self.current_price,
            'low_price': self.low_price,
            'plan_shift_amount': self.plan_shift_amount,
            'total_planned': len(self.active_planned_orders),
            'executed_orders': len(self.executed_orders),
            'pending_orders': len(self.pending_orders),
            'total_executed_quantity': total_executed_quantity,
            'total_executed_volume': total_executed_volume,
            'completion_rate': (len(self.executed_orders) / len(self.active_planned_orders)) * 100 if self.active_planned_orders else 0
        }

    def get_detailed_status(self):
        """상세 상태 정보"""
        status = self.get_status()
        status['planned_orders'] = [
            {
                'level': order['level'],
                'planned_price': order['planned_price'],
                'quantity': order['quantity'],
                'volume': order['volume'],
                'executed': order['executed'],
                'shift_applied': order['shift_applied']
            }
            for order in self.active_planned_orders
        ]
        status['executed_orders_detail'] = [
            {
                'level': order['level'],
                'executed_price': order['executed_price'],
                'quantity': order['quantity'],
                'volume': order['volume'],
                'executed_time': order['executed_time'].strftime("%Y-%m-%d %H:%M:%S")
            }
            for order in self.executed_orders
        ]
        status['pending_orders_detail'] = [
            {
                'level': order['level'],
                'actual_price': order['actual_price'],
                'volume': order['volume'],
                'order_time': order['order_time'].strftime("%Y-%m-%d %H:%M:%S")
            }
            for order in self.pending_orders
        ]
        return status

    def reset(self):
        """리셋 - 새로운 거래 준비"""
        self.stop_trading()
        self.active_planned_orders.clear()
        self.executed_orders.clear()
        self.pending_orders.clear()
        self.plan_shift_amount = 0.0
        self.last_order_time = None
        self.last_check_time = None
        self.first_order_start_time = None
        print_log(LogLevel.INFO, f"Reset completed for {self.symbol}")

class SellOrder:
    def __init__(self, symbol, volume, price):
        global sell_uuids
        self.uuid = None  # 체결 추적용 — 성공 시 UUID 저장

        query = {
            'market': 'KRW-' + symbol,
            'side': 'ask',
            'volume': str(volume),
            'price': str(price),
            'ord_type': 'limit',
        }

        query_string = urlencode(query).encode()
        m = hashlib.sha512()
        m.update(query_string)
        query_hash = m.hexdigest()

        payload = {
            'access_key': ACCESS_KEY,
            'nonce': str(uuid.uuid4()),
            'query_hash': query_hash,
            'query_hash_alg': 'SHA512',
        }

        jwt_token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
        authorization = f'Bearer {jwt_token}'
        headers = {"Authorization": authorization}

        def api_call():
            response = requests.post(SERVER_URL + "/v1/orders", params=query, headers=headers)
            return response.json()

        response_dict = safe_api_call(api_call)
        if 'uuid' in response_dict:
            self.uuid = response_dict['uuid']
            sell_uuids.append(self.uuid)
            print_log(LogLevel.INFO, f"Sell order placed at {price:,.0f} KRW, volume: {volume:.6f}")
        else:
            print_log(LogLevel.ERROR, f"Failed to place sell order: {response_dict}")

class TradingManager:
    """거래 상태 및 캐시 관리 클래스"""
    
    def __init__(self):
        self.current_symbol = None
        self.symbol_cache_time = None
        self.buy_orders_placed = False
        self.buy_orders_executed = False
        self.sell_orders_placed = False
        self.sell_orders_executed = False
        self.start_time = None
        self.stop_loss_triggered = False
        self.buy_order_start_time = None
        self.buy_timeout_seconds = 60  # 1분 타임아웃
        self.last_command_check = None
        self.command_check_interval = 2  # 2초로 단축 (기존 5초)
        self.forced_symbol_change = False
        self.pending_symbol_change = None  # 대기 중인 심볼 변경
        
    def set_symbol(self, symbol):
        """심볼 설정 및 캐시"""
        global current_trading_symbol, symbol_cache_time
        self.current_symbol = symbol
        current_trading_symbol = symbol
        self.symbol_cache_time = datetime.now()
        symbol_cache_time = self.symbol_cache_time
        self.start_time = datetime.now()
        print_log(LogLevel.INFO, f"Trading symbol set to: {symbol}")
        
    def get_cached_symbol(self):
        """캐시된 심볼 반환"""
        global current_trading_symbol, symbol_cache_time
        
        if (current_trading_symbol and symbol_cache_time and
            (datetime.now() - symbol_cache_time).total_seconds() < CACHE_DURATION):
            cache_remaining = CACHE_DURATION - (datetime.now() - symbol_cache_time).total_seconds()
            print_log(LogLevel.INFO, f"Using cached symbol: {current_trading_symbol} (valid for {int(cache_remaining//60)}m {int(cache_remaining%60)}s)")
            return current_trading_symbol
        return None
        
    def check_command_file(self):
        """command.txt 파일 변경 체크 - 거래 중단 없이 대기만 시킴"""
        current_time = datetime.now()
        
        if (self.last_command_check and 
            (current_time - self.last_command_check).total_seconds() < self.command_check_interval):
            return False
            
        self.last_command_check = current_time
        
        try:
            with open("../log/command.txt", 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for line in lines:
                    text = line.strip().upper()
                    parts = text.split(' ')
                    if parts[0] == 'SYMBOL' and len(parts) > 1:
                        new_symbol = parts[1]
                        
                        # 현재 심볼과 다를 경우 대기 목록에 추가
                        if new_symbol != self.current_symbol and new_symbol != self.pending_symbol_change:
                            # 변동성 보호 체크
                            if VolatilityProtector.check_volatility_protection(new_symbol):
                                print_log(LogLevel.WARNING, f"Command symbol {new_symbol} blocked by volatility protection")
                                return False
                                
                            print_log(LogLevel.INFO, f"Symbol change requested: {new_symbol} (will apply after current trading)")
                            self.pending_symbol_change = new_symbol
                            return True
                            
                    elif parts[0] == 'EXIT':
                        log_state(LogState.FORCED_EXIT)
                        print_log(LogLevel.WARNING, "Exit command detected")
                        exit(0)
                        
        except Exception as e:
            print_log(LogLevel.WARNING, f"Error reading command file: {str(e)}")
            
        return False

    def has_pending_symbol_change(self):
        """대기 중인 심볼 변경이 있는지 확인"""
        return self.pending_symbol_change is not None

    def apply_pending_symbol_change(self):
        """대기 중인 심볼 변경 적용"""
        if self.pending_symbol_change:
            symbol_to_use = self.pending_symbol_change
            self.pending_symbol_change = None
            self.set_symbol(symbol_to_use)
            print_log(LogLevel.SUCCESS, f"Applied pending symbol change: {symbol_to_use}")
            return symbol_to_use
        return None

    def get_command_symbol_override(self):
        """command.txt에서 지정된 심볼 오버라이드 값을 반환"""
        return self.pending_symbol_change

    def is_trading_in_progress(self):
        """거래가 진행 중인지 확인"""
        return (self.buy_orders_placed or self.buy_orders_executed or 
                self.sell_orders_placed or self.sell_orders_executed)

    def mark_buy_orders_placed(self):
        """매수 주문 걸림 표시"""
        self.buy_orders_placed = True
        self.buy_order_start_time = datetime.now()  # 매수 시작 시간 기록
        print_log(LogLevel.INFO, "Buy orders placed")
        
    def mark_buy_orders_executed(self):
        """매수 주문 체결 표시"""
        self.buy_orders_executed = True
        print_log(LogLevel.SUCCESS, "Buy orders executed")
        
    def mark_sell_orders_placed(self):
        """매도 주문 걸림 표시"""
        self.sell_orders_placed = True
        print_log(LogLevel.INFO, "Sell orders placed")
        
    def mark_sell_orders_executed(self):
        """매도 주문 체결 표시"""
        self.sell_orders_executed = True
        print_log(LogLevel.SUCCESS, "Sell orders executed")
        
    def mark_stop_loss_triggered(self):
        """스탑로스 발동 표시"""
        self.stop_loss_triggered = True
        print_log(LogLevel.WARNING, "Stop loss triggered")
        
    def should_place_buy_orders(self):
        """매수 주문을 걸어야 하는지"""
        return not self.buy_orders_placed
        
    def should_wait_for_buy_execution(self):
        """매수 체결을 기다려야 하는지"""
        return self.buy_orders_placed and not self.buy_orders_executed
        
    def should_place_sell_orders(self):
        """매도 주문을 걸어야 하는지"""
        return self.buy_orders_executed and not self.sell_orders_placed
        
    def should_wait_for_sell_execution(self):
        """매도 체결을 기다려야 하는지"""
        return self.sell_orders_placed and not self.sell_orders_executed
        
    def is_trading_complete(self):
        """거래 완료 여부"""
        return self.sell_orders_executed or self.stop_loss_triggered
        
    def is_buy_timeout(self):
        """매수 주문 타임아웃 체크"""
        if self.buy_order_start_time and self.buy_orders_placed and not self.buy_orders_executed:
            elapsed = (datetime.now() - self.buy_order_start_time).total_seconds()
            return elapsed > self.buy_timeout_seconds
        return False
        
    def reset(self):
        """상태 초기화 (캐시는 유지)"""
        self.buy_orders_placed = False
        self.buy_orders_executed = False
        self.sell_orders_placed = False
        self.sell_orders_executed = False
        self.stop_loss_triggered = False
        self.start_time = None
        self.buy_order_start_time = None
        self.forced_symbol_change = False
        # pending_symbol_change는 유지 (다음 사이클에서 적용)
        print_log(LogLevel.INFO, "Trading state reset (symbol cache maintained)")

class SellController:
    def __init__(self):
        self.last_sell_check_time = None
        self.sell_check_interval = 60
        self.last_sell_placement_time = None
        self.last_stop_loss_check = None
        self.stop_loss_check_interval = 30
        # 그리드 매매용 per-order 매도 추적
        self.sell_orders_tracking = []  # [{uuid, price, volume, tier, filled}]
        self.filled_sell_count = 0      # 체결된 매도 개수
        self.sell_round = 0             # 매도 주문 라운드 누적 (분할 개수 결정용)
        self.last_sell_base_price = None  # 직전 매도 기준가 (갱신 감지용)

    def has_holdings(self, symbol):
        """보유 코인이 있는지 확인"""
        balance, locked, avg_buy_price = AccountChecker().get_symbol_info(symbol)
        return (balance + locked) >= MIN_HOLDING_VOLUME

    def has_pending_sell_orders(self, symbol):
        """미체결 매도 주문이 있는지 확인"""
        return len(sell_uuids) > 0

    def get_avg_buy_price(self, symbol):
        """매수 평균가 조회"""
        balance, locked, avg_buy_price = AccountChecker().get_symbol_info(symbol)
        return avg_buy_price

    def get_total_volume(self, symbol):
        """총 보유 수량 조회"""
        balance, locked, avg_buy_price = AccountChecker().get_symbol_info(symbol)
        return balance + locked

    def get_available_volume(self, symbol):
        """실제 매도 가능한 수량 확인"""
        balance, locked, avg_buy_price = AccountChecker().get_symbol_info(symbol)
        return balance

    def cancel_all_sell_orders(self, symbol):
        """모든 매도 주문 취소"""
        print_log(LogLevel.INFO, f"Cancelling all sell orders for {symbol}")
        OrderCanceler().cancel_sell_orders()

    def place_sell_orders(self, symbol, profit_percentages, dynamic_buyer=None):
        """매도 주문 걸기 — 매수 평단가 기준.
        매도가는 무조건 평단가보다 위여야 이득 (매수 최저가 기준은 평단가 대비 손실).
        locked(이미 매도 주문에 잠긴 수량)은 포함하지 않아 insufficient_funds_ask 방지."""
        try:
            available_volume = self.get_available_volume(symbol)

            if available_volume < MIN_HOLDING_VOLUME:
                print_log(LogLevel.WARNING, f"매도 불가 - 부족한 수량: {available_volume:.6f}")
                return False

            # 매도 기준가 = 매수 평단가 (업비트 /v1/accounts avg_buy_price)
            sell_base_price = self.get_avg_buy_price(symbol)
            if sell_base_price <= 0:
                print_log(LogLevel.WARNING, f"매도 불가 - 평단가 조회 실패: {sell_base_price}")
                return False
            print_log(LogLevel.INFO, f"매도 기준가 = 평단가 {sell_base_price:,.4f}")

            print_log(LogLevel.INFO, f"매도주문 - 기준가: {sell_base_price:,.4f}, 매도가능수량: {available_volume:.6f}")

            # 단계적 분할 개수 결정 — 매도 라운드 누적 기준
            # 1~4라운드: 단일, 5~8: 이중, 9~: 삼중
            self.sell_round += 1
            if self.sell_round <= 4:
                max_splits = 1
            elif self.sell_round <= 8:
                max_splits = 2
            else:
                max_splits = 3
            split_percentages = profit_percentages[:max_splits]
            print_log(LogLevel.INFO,
                      f"매도 라운드 {self.sell_round} → {len(split_percentages)}중 분할매도")

            # 최소주문금액 검사 — 분할 시 각 매도가 5000원 미만이면 분할 개수 축소
            effective_percentages = split_percentages
            while len(effective_percentages) > 1:
                per_amount = (available_volume / len(effective_percentages)) * sell_base_price
                if per_amount >= MIN_ORDER_AMOUNT:
                    break
                print_log(LogLevel.INFO,
                          f"매도 분할 축소 — 건당 금액 {per_amount:,.0f}원 < {MIN_ORDER_AMOUNT}원, "
                          f"{len(effective_percentages)}중 → {len(effective_percentages)-1}중")
                effective_percentages = effective_percentages[:-1]

            sell_volume_per_order = available_volume / len(effective_percentages)

            # 기준가 저장 (갱신 감지용)
            self.last_sell_base_price = sell_base_price

            # 매도가 계산 — 최저가 기준 목표% 적용, 호가 중복 시 최소호가단위로 강제 분리
            sell_prices = []
            for profit_pct in effective_percentages:
                sell_prices.append(UpbitTickSystem.calculate_sell_price(sell_base_price, profit_pct))
            for i in range(1, len(sell_prices)):
                if sell_prices[i] <= sell_prices[i - 1]:
                    tick = UpbitTickSystem.get_minimum_tick(sell_prices[i - 1])
                    sell_prices[i] = sell_prices[i - 1] + tick

            for i, profit_pct in enumerate(effective_percentages):
                sell_price = sell_prices[i]

                print_log(LogLevel.INFO,
                         f"매도 #{i+1} - 목표: {profit_pct}%, "
                         f"가격: {sell_price:,.0f} KRW, 수량: {sell_volume_per_order:.6f}")

                sell_order = SellOrder(symbol, sell_volume_per_order, sell_price)
                # per-order 추적 — UUID/가격/수량/tier 저장 (체결 시 되사들이기용)
                if sell_order and sell_order.uuid:
                    self.sell_orders_tracking.append({
                        'uuid': sell_order.uuid,
                        'price': sell_price,
                        'volume': sell_volume_per_order,
                        'tier': i + 1,
                        'filled': False
                    })

            self.last_sell_placement_time = datetime.now()
            return True

        except Exception as e:
            print_log(LogLevel.ERROR, f"매도주문 실패: {str(e)}")
            traceback.print_exc()
            return False

    def check_stop_loss(self, symbol, trading_manager):
        """스탑로스 조건 체크 (-20% 이상 하락 시 매도)"""
        current_time = datetime.now()
        
        if (self.last_stop_loss_check and 
            (current_time - self.last_stop_loss_check).total_seconds() < self.stop_loss_check_interval):
            return False
            
        self.last_stop_loss_check = current_time
        
        try:
            current_price = RealMarketData.get_current_price(symbol)
            avg_buy_price = self.get_avg_buy_price(symbol)
            
            if current_price is None or avg_buy_price <= 0:
                return False
                
            loss_percentage = ((current_price - avg_buy_price) / avg_buy_price) * 100
            
            if loss_percentage <= STOP_LOSS_PERCENTAGE:
                print_log(LogLevel.WARNING, 
                         f"Stop loss triggered! Loss: {loss_percentage:.2f}% "
                         f"(Current: {current_price:,.0f}, Avg: {avg_buy_price:,.0f})")
                
                self.cancel_all_sell_orders(symbol)
                
                total_volume = self.get_available_volume(symbol)
                if total_volume >= MIN_HOLDING_VOLUME:
                    print_log(LogLevel.WARNING, f"Emergency sell at market price: {total_volume:.6f}")
                    self.place_emergency_sell_order(symbol, total_volume)
                    trading_manager.mark_stop_loss_triggered()
                    return True
                    
        except Exception as e:
            print_log(LogLevel.EXCEPTION, f"Stop loss check error: {str(e)}")
            
        return False

    def place_emergency_sell_order(self, symbol, volume):
        """비상 시장가 매도 주문"""
        try:
            query = {
                'market': 'KRW-' + symbol,
                'side': 'ask',
                'volume': str(volume),
                'ord_type': 'market',
            }

            query_string = urlencode(query).encode()
            m = hashlib.sha512()
            m.update(query_string)
            query_hash = m.hexdigest()

            payload = {
                'access_key': ACCESS_KEY,
                'nonce': str(uuid.uuid4()),
                'query_hash': query_hash,
                'query_hash_alg': 'SHA512',
            }

            jwt_token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
            authorization = f'Bearer {jwt_token}'
            headers = {"Authorization": authorization}

            def api_call():
                response = requests.post(SERVER_URL + "/v1/orders", params=query, headers=headers)
                return response.json()
            
            response_dict = safe_api_call(api_call)
            if 'uuid' in response_dict:
                print_log(LogLevel.WARNING, f"Emergency sell order placed: {volume:.6f} {symbol}")
                return True
            else:
                print_log(LogLevel.ERROR, f"Failed to place emergency sell order: {response_dict}")
                return False
                
        except Exception as e:
            print_log(LogLevel.ERROR, f"Emergency sell order error: {str(e)}")
            return False

    def check_sell_fills(self, symbol, dynamic_buyer):
        """매도 per-order 체결 확인 — 체결된 매도는 되받은 KRW만큼
        잠재 매수 예산에 가산. 추적 중인 매도 전체가 체결되면 True(사이클 완료) 반환.
        수동 취소된 매도는 tracking에서 제거만 하고 사이클 종료로 간주하지 않음."""
        if not self.sell_orders_tracking:
            return False

        all_filled = True
        for entry in self.sell_orders_tracking:
            if entry['filled']:
                continue
            try:
                # 매도 주문 상태 직접 조회 (GET /v1/order)
                params = {'uuid': entry['uuid']}
                query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")
                m = hashlib.sha512()
                m.update(query_string)
                query_hash = m.hexdigest()
                payload = {
                    'access_key': ACCESS_KEY,
                    'nonce': str(uuid.uuid4()),
                    'query_hash': query_hash,
                    'query_hash_alg': 'SHA512',
                }
                jwt_token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
                headers = {"Authorization": f'Bearer {jwt_token}', "Accept": "application/json"}

                def api_call():
                    response = requests.get(SERVER_URL + "/v1/order", params=params, headers=headers)
                    return response.json()
                order_info = safe_api_call(api_call)

                if order_info and order_info.get('state') == 'done':
                    entry['filled'] = True
                    self.filled_sell_count += 1
                    # 체결된 매도 UUID를 sell_uuids에서 제거 — has_pending_sell_orders 갱신
                    if entry['uuid'] in sell_uuids:
                        sell_uuids.remove(entry['uuid'])
                    executed_vol = float(order_info.get('executed_volume', entry['volume']))
                    sell_price = entry['price']
                    rebuy_krw = executed_vol * sell_price  # 매도로 되받은 KRW

                    print_log(LogLevel.SUCCESS,
                              f"✅ 매도#{entry['tier']} 체결 확인 (수량 {executed_vol:.6f} @ {sell_price:,.4f}원)")
                    # 잠재 매수 예산에 되받은 KRW 가산
                    self._reinvest_to_next_buy(dynamic_buyer, rebuy_krw, entry['tier'])
                elif order_info and order_info.get('state') == 'cancel':
                    # 외부(업비트 앱/웹)에서 수동 취소된 매도.
                    # 사이클을 종료하지 않고 — tracking/uuid에서만 제거하여 다음 루프에서
                    # has_pending_sell_orders=False → 새 매도 자동 재설정.
                    print_log(LogLevel.WARNING,
                              f"⚠ 매도#{entry['tier']} 가 수동 취소됨 — tracking 제거, "
                              f"다음 루프에서 새 매도 재설정 (사이클 유지)")
                    entry['filled'] = True  # 이 entry는 더 이상 조회하지 않음
                    if entry['uuid'] in sell_uuids:
                        sell_uuids.remove(entry['uuid'])
                    all_filled = False  # ★ 수동 취소는 사이클 종료 아님
                else:
                    all_filled = False
            except Exception as e:
                print_log(LogLevel.ERROR, f"매도 체결 확인 중 오류 (tier {entry['tier']}): {str(e)}")
                all_filled = False

        return all_filled

    def _reinvest_to_next_buy(self, dynamic_buyer, krw_amount, sell_tier):
        """매도로 되받은 KRW를 잠재 매수 주문의 예산에 비례 가산.
        즉시 주문을 실행하지 않고 예산(quantity)만 늘려둠 — 나중에 해당 레벨이
        차례가 되어 _execute_single_order 가 호출될 때 가산된 예산이 반영됨.
        잠재 매수가 0개면, 마지막 체결 지점에 즉시 재매수(예산 가산 불가)."""
        if not dynamic_buyer or krw_amount < MIN_ORDER_AMOUNT:
            if krw_amount < MIN_ORDER_AMOUNT:
                print_log(LogLevel.INFO,
                          f"매도#{sell_tier} 체결 → 재투자액 {krw_amount:,.0f}원 < {MIN_ORDER_AMOUNT}원, 스킵")
            return

        # 잠재 매수 주문: 아직 실행된 적도 없고 pending도 아닌 주문
        pending_levels = set()
        for p in dynamic_buyer.pending_orders:
            pending_levels.add(p['level'])

        potential = [o for o in sorted(dynamic_buyer.active_planned_orders, key=lambda x: x['level'])
                     if not o['executed'] and o['level'] not in pending_levels]

        if potential:
            # 잠재 매수 예산에 균등 가산 (즉시 주문 실행 없음)
            share = krw_amount / len(potential)
            levels_str = ', '.join(f"L{o['level']}({o['planned_price']:.2f})" for o in potential)
            print_log(LogLevel.INFO,
                      f"매도#{sell_tier} 체결 → {krw_amount:,.0f}원을 "
                      f"잠재 매수 {len(potential)}건 예산에 가산 (건당 +{share:,.0f}원): [{levels_str}]")
            for order in potential:
                order['quantity'] += share
                order['volume'] = order['quantity'] / order['planned_price']
                print_log(LogLevel.INFO,
                          f"📌 level {order['level']} ({order['planned_price']:.4f}) 예산 "
                          f"{order['quantity']-share:,.0f} → {order['quantity']:,.0f}원 (나중에 실행 시 반영)")
        else:
            # 잠재 매수가 0개 — 마지막 체결 지점에 즉시 재매수 (가산할 잠재 주문이 없으므로)
            if not dynamic_buyer.executed_orders:
                print_log(LogLevel.WARNING, f"매도#{sell_tier} 체결 → 잠재/체결 매수 모두 없음, 스킵")
                return
            last_executed = max(dynamic_buyer.executed_orders, key=lambda o: o['level'])
            last_price = last_executed['executed_price']
            volume = krw_amount / last_price if last_price > 0 else 0
            if volume > 0:
                dynamic_buyer.place_dynamic_buy_order(last_price, volume)
                print_log(LogLevel.SUCCESS,
                          f"💰 매도#{sell_tier} 체결 → 잠재 매수 없음, "
                          f"마지막 체결 지점 level {last_executed['level']} ({last_price:.4f})에 "
                          f"{krw_amount:,.0f}원 재매수")

    def manage_sell_orders(self, symbol, profit_percentages, trading_manager, wait_count, dynamic_buyer=None):
        """매도 주문 관리 - per-order 체결 추적 + 되사들이기 + 스탑로스"""

        if self.check_stop_loss(symbol, trading_manager):
            return True

        # 매도 per-order 체결 추적 — 체결 시 매수 되사들이기, 전체 체결 시 완료
        if self.sell_orders_tracking:
            if self.check_sell_fills(symbol, dynamic_buyer):
                # 3건 모두 체결 → 사이클 완전 종료
                trading_manager.mark_sell_orders_executed()
                print_log(LogLevel.SUCCESS,
                          f"🎉 매도 {self.filled_sell_count}건 모두 체결 — 사이클 완전 종료")
                self.sell_orders_tracking = []
                self.filled_sell_count = 0
                sell_uuids.clear()
                return True

        if not self.has_holdings(symbol):
            if trading_manager.sell_orders_placed:
                trading_manager.mark_sell_orders_executed()
                print_log(LogLevel.SUCCESS, "보유량 없음 - 거래 완료")
            return True

        if not self.has_pending_sell_orders(symbol):
            available_volume = self.get_available_volume(symbol)
            if available_volume >= MIN_HOLDING_VOLUME:
                print_log(LogLevel.INFO, f"매도주문 없음 - 새 매도주문 걸기 (매도가능수량: {available_volume:.6f})")
                # 추적 상태 정리 (이전 라운드 잔여 방지)
                self.sell_orders_tracking = [e for e in self.sell_orders_tracking if not e['filled']]
                if self.place_sell_orders(symbol, profit_percentages, dynamic_buyer):
                    trading_manager.mark_sell_orders_placed()
            else:
                # 매도 가능 수량이 없으면 locked(미체결 매도)가 있는지 확인
                total_vol = self.get_total_volume(symbol)
                if total_vol < MIN_HOLDING_VOLUME:
                    print_log(LogLevel.WARNING, f"매도 불가 - 총 수량 부족: {total_vol:.6f}")
            return False

        return False

class CandleInfoFetcher:
    def __reverse_array(self, key, count):
        arr = [0] * count
        for i in range(count):
            arr[count - i - 1] = self.response_dict[i].get(key)
        return arr

    def __init__(self, symbol):
        count = 200
        querystring = {
            "market": "KRW-" + symbol,
            "count": str(count)
        }

        try:
            def api_call():
                response = requests.get(CANDLE_URL, params=querystring, timeout=10)
                return response.json()
            
            self.response_dict = safe_api_call(api_call)
        except:
            print_log(LogLevel.EXCEPTION, "Failed to fetch candle data")
            raise Exception("CandleInfoFetcher")

        self.opening_prices = self.__reverse_array('opening_price', count)
        self.trade_prices = self.__reverse_array('trade_price', count)
        self.current_price = self.trade_prices[-1]
        self.high_prices = self.__reverse_array('high_price', count)
        self.low_prices = self.__reverse_array('low_price', count)
        self.acc_trade_prices = self.__reverse_array('candle_acc_trade_price', count)
        self.acc_trade_volumes = self.__reverse_array('candle_acc_trade_volume', count)

class VolatilityProtector:
    """동적 매수 보호 클래스 - 고변동성 코인 매수 방지"""
    
    @staticmethod
    def check_volatility_protection(symbol, lookback_period=60, threshold_percentage=40.0):
        """
        변동성 보호 체크
        최근 lookback_period 캔들 동안 최소값과 최대값 차이가 threshold_percentage 이상이면 매수 금지
        
        Args:
            symbol: 심볼명
            lookback_period: 확인할 캔들 수 (기본 60개)
            threshold_percentage: 변동성 임계값 (기본 40%)
            
        Returns:
            bool: True=보호 적용(매수금지), False=매수 가능
        """
        try:
            # 최근 캔들 데이터 가져오기
            count = max(lookback_period, 60)  # 최소 60개
            querystring = {
                "market": "KRW-" + symbol,
                "count": str(count)
            }

            def api_call():
                response = requests.get(CANDLE_URL, params=querystring, timeout=10)
                return response.json()
            
            candles = safe_api_call(api_call)
            
            if not candles or len(candles) < lookback_period:
                print_log(LogLevel.WARNING, f"Not enough candle data for {symbol}, skipping volatility check")
                return False
            
            # 최근 lookback_period개의 고가/저가 추출
            high_prices = []
            low_prices = []
            
            for i in range(min(lookback_period, len(candles))):
                candle = candles[i]
                high_prices.append(float(candle['high_price']))
                low_prices.append(float(candle['low_price']))
            
            # 최소값과 최대값 계산
            min_price = min(low_prices)
            max_price = max(high_prices)
            
            # 변동성 계산 (백분율)
            if min_price > 0:
                volatility_percentage = ((max_price - min_price) / min_price) * 100
            else:
                return False
            
            print_log(LogLevel.INFO, 
                     f"Volatility Check for {symbol}: "
                     f"Min={min_price:,.0f}, Max={max_price:,.0f}, "
                     f"Volatility={volatility_percentage:.2f}% (Threshold: {threshold_percentage}%)")
            
            # 임계값 초과 시 보호 적용
            if volatility_percentage >= threshold_percentage:
                print_log(LogLevel.WARNING, 
                         f"VOLATILITY PROTECTION TRIGGERED for {symbol}: "
                         f"{volatility_percentage:.2f}% >= {threshold_percentage}% - BUY BLOCKED")
                return True
            else:
                print_log(LogLevel.INFO, 
                         f"Volatility within safe range for {symbol}: "
                         f"{volatility_percentage:.2f}% < {threshold_percentage}%")
                return False
                
        except Exception as e:
            print_log(LogLevel.EXCEPTION, f"Error in volatility protection check for {symbol}: {str(e)}")
            # 에러 발생 시 보호 적용 (안전 측면)
            return True

class MarketAnalyzer:
    def __init__(self, symbol):
        self.candle = CandleInfoFetcher(symbol)
        self.max_price = max(self.candle.high_prices)
        self.min_price = min(self.candle.low_prices)
        self.ma20 = talib.MA(np.array(self.candle.trade_prices), timeperiod=20)[-1]
        self.ma60 = talib.MA(np.array(self.candle.trade_prices), timeperiod=60)[-1]
        self.std20 = talib.STDDEV(np.array(self.candle.trade_prices), timeperiod=20)[-1]
        self.normalized_std20 = self.std20 / self.candle.current_price
        self.relative_deviation_index = (self.candle.current_price - self.ma20) / self.std20
        self.volatility_ratio = self.std20 / self.ma20 if self.ma20 > 0 else 0

        # ATR 계산
        self.atr = talib.ATR(
            np.array(self.candle.high_prices),
            np.array(self.candle.low_prices),
            np.array(self.candle.trade_prices),
            timeperiod=14
        )[-1]
        self.atr_pct = self.atr / self.candle.current_price if self.candle.current_price > 0 else 0

        # 거래량 평균 계산
        avg_volume = np.mean(self.candle.acc_trade_volumes[-20:])  # 최근 20봉 평균
        current_volume = self.candle.acc_trade_volumes[-1]
        self.volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1.0

        # 이동평균 기울기 계산 (최근 5봉 기준)
        if len(self.candle.trade_prices) >= 5:
            recent_prices = self.candle.trade_prices[-5:]
            x = np.arange(len(recent_prices))
            slope = np.polyfit(x, recent_prices, 1)[0]
            self.ma_slope = (slope / recent_prices[0]) * 100  # 백분율 변화율
        else:
            self.ma_slope = 0.0

    def get_rsi(self):
        rsi_values = talib.RSI(np.array(self.candle.trade_prices), timeperiod=14)
        valid_rsi = rsi_values[~np.isnan(rsi_values)]
        if len(valid_rsi) > 0:
            return float(valid_rsi[-1])
        else:
            return 50.0

    def get_mfi(self):
        mfi = talib.MFI(
            np.array(self.candle.high_prices),
            np.array(self.candle.low_prices),
            np.array(self.candle.trade_prices),
            np.array(self.candle.acc_trade_prices),
            timeperiod=14)
        valid_mfi = mfi[~np.isnan(mfi)]
        return float(valid_mfi[-1]) if len(valid_mfi) > 0 else 50.0

    def get_macd(self):
        macd, macd_signal, macd_hist = talib.MACD(
            np.array(self.candle.trade_prices),
            fastperiod=12, slowperiod=26, signalperiod=9)
        
        if len(macd_hist) > 0 and not np.isnan(macd_hist[-1]):
            return {
                'macd': float(macd[-1]),
                'macd_signal': float(macd_signal[-1]),
                'macd_hist': float(macd_hist[-1])
            }
        else:
            return {'macd': 0.0, 'macd_signal': 0.0, 'macd_hist': 0.0}

    def get_williams_r(self):
        williams = talib.WILLR(
            np.array(self.candle.high_prices),
            np.array(self.candle.low_prices),
            np.array(self.candle.trade_prices),
            timeperiod=14)
        valid_wr = williams[~np.isnan(williams)]
        return float(valid_wr[-1]) if len(valid_wr) > 0 else -50.0

    def get_momentum(self):
        momentum = talib.MOM(np.array(self.candle.trade_prices), timeperiod=10)
        valid_momentum = momentum[~np.isnan(momentum)]
        if len(valid_momentum) > 0:
            base_price = self.candle.trade_prices[-11] if len(self.candle.trade_prices) >= 11 else self.candle.trade_prices[0]
            return (valid_momentum[-1] / base_price) * 100
        return 100.0

    def is_below_ma60(self):
        return self.candle.current_price < self.ma60

class SymbolSelector:
    @staticmethod
    def load_ignored_symbols():
        """symignore.txt에서 무시할 심볼 목록 로드"""
        ignored_symbols = set()
        try:
            if os.path.exists(SYMIGNORE_FILE):
                with open(SYMIGNORE_FILE, 'r', encoding='utf-8') as f:
                    for line in f:
                        symbol = line.strip().upper()
                        if symbol and not symbol.startswith('#'):
                            ignored_symbols.add(symbol)
                print_log(LogLevel.INFO, f"Loaded {len(ignored_symbols)} ignored symbols from {SYMIGNORE_FILE}")
            else:
                # 기본 무시 심볼 생성
                default_ignored = ["BTC", "ETH", "XRP", "ADA", "SOL"]
                with open(SYMIGNORE_FILE, 'w', encoding='utf-8') as f:
                    for symbol in default_ignored:
                        f.write(symbol + '\n')
                print_log(LogLevel.INFO, f"Created default {SYMIGNORE_FILE} with {len(default_ignored)} symbols")
                ignored_symbols = set(default_ignored)
        except Exception as e:
            print_log(LogLevel.WARNING, f"Failed to load {SYMIGNORE_FILE}: {str(e)}")
        return ignored_symbols

    @staticmethod
    def get_all_krw_markets():
        try:
            def api_call():
                url = "https://api.upbit.com/v1/market/all"
                headers = {"Accept": "application/json"}
                response = requests.get(url, headers=headers, timeout=10)
                return response.json()
            
            markets = safe_api_call(api_call)
            krw_markets = [market for market in markets if market['market'].startswith('KRW-') and not market['market_event']['warning']]
            return [market['market'].replace('KRW-', '') for market in krw_markets]
        except Exception as e:
            print_log(LogLevel.ERROR, f"Failed to get KRW markets: {str(e)}")
            return []

    @staticmethod
    def get_recent_trading_volume(symbol, hours=3):
        try:
            def api_call():
                url = "https://api.upbit.com/v1/candles/minutes/60"
                params = {
                    'market': f"KRW-{symbol}",
                    'count': hours
                }
                headers = {"Accept": "application/json"}
                response = requests.get(url, params=params, headers=headers, timeout=10)
                return response.json()
            
            candles = safe_api_call(api_call)
            if not candles:
                return 0
                
            total_volume = sum(candle['candle_acc_trade_price'] for candle in candles) / 1000000
            return total_volume
            
        except Exception as e:
            print_log(LogLevel.WARNING, f"Failed to get trading volume for {symbol}: {str(e)}")
            return 0

    @staticmethod
    def calculate_rank_score(symbol_data_list):
        """순위 기반 점수 계산: 각 지표별 순위를 합산하여 총점 계산 (낮을수록 좋음)"""
        
        # 변동성 순위 (높을수록 좋음 -> 낮은 순위가 좋음)
        volatility_rank = {}
        sorted_by_volatility = sorted(symbol_data_list, key=lambda x: x['volatility'], reverse=True)
        for rank, data in enumerate(sorted_by_volatility, 1):
            volatility_rank[data['symbol']] = rank
        
        # 거래량 순위 (높을수록 좋음 -> 낮은 순위가 좋음)
        volume_rank = {}
        sorted_by_volume = sorted(symbol_data_list, key=lambda x: x['trading_volume_3h'], reverse=True)
        for rank, data in enumerate(sorted_by_volume, 1):
            volume_rank[data['symbol']] = rank
        
        # 각 심볼별 총 순위 점수 계산 (낮을수록 좋음)
        rank_scores = {}
        for data in symbol_data_list:
            symbol = data['symbol']
            total_rank = volatility_rank[symbol] + volume_rank[symbol]
            rank_scores[symbol] = total_rank
        
        return rank_scores

    @staticmethod
    def analyze_market_volatility(symbol):
        try:
            if symbol in traded_symbols:
                completion_time = traded_symbols[symbol]
                if (datetime.now() - completion_time).total_seconds() < 3600:
                    return None
                else:
                    del traded_symbols[symbol]
                
            trading_volume = SymbolSelector.get_recent_trading_volume(symbol, 3)
            if trading_volume < 1000:  # 10억 원 이상 거래량 필터
                return None
                
            # 변동성 보호 체크 - 30% 이상 변동성 코인 제외
            if VolatilityProtector.check_volatility_protection(symbol):
                print_log(LogLevel.WARNING, f"Skipping {symbol} due to high volatility protection")
                return None
                
            analyzer = MarketAnalyzer(symbol)
            
            if UpbitTickSystem.is_excluded_tick_range(analyzer.candle.current_price):
                return None
                
            return {
                'symbol': symbol,
                'volatility': analyzer.volatility_ratio,
                'current_price': analyzer.candle.current_price,
                'ma60': analyzer.ma60,
                'rsi': analyzer.get_rsi(),
                'mfi': analyzer.get_mfi(),
                'below_ma60_ratio': (analyzer.ma60 - analyzer.candle.current_price) / analyzer.ma60,
                'trading_volume_3h': trading_volume
            }
        except Exception as e:
            print_log(LogLevel.WARNING, f"Failed to analyze {symbol}: {str(e)}")
            return None

    @staticmethod
    def select_best_symbol():
        print_log(LogLevel.INFO, "Analyzing all KRW markets for best symbol...")
        
        symbols = SymbolSelector.get_all_krw_markets()
        if not symbols:
            print_log(LogLevel.ERROR, "No KRW markets found")
            return None
        
        # 무시할 심볼 로드
        ignored_symbols = SymbolSelector.load_ignored_symbols()
        filtered_symbols = [s for s in symbols if s not in ignored_symbols]
        
        print_log(LogLevel.INFO, f"Total symbols: {len(symbols)}, After filtering: {len(filtered_symbols)}")
        
        valid_symbols = []
        
        for symbol in tqdm(filtered_symbols, desc="Analyzing markets"):
            try:
                result = SymbolSelector.analyze_market_volatility(symbol)
                if result:
                    valid_symbols.append(result)
                time.sleep(0.05)
            except Exception as e:
                continue
        
        if not valid_symbols:
            print_log(LogLevel.WARNING, "No valid symbols found with sufficient trading volume and safe volatility")
            return None

        # 순위 기반 점수 계산
        rank_scores = SymbolSelector.calculate_rank_score(valid_symbols)
        
        # 순위 점수를 각 심볼 데이터에 추가
        for symbol_data in valid_symbols:
            symbol_data['rank_score'] = rank_scores[symbol_data['symbol']]
        
        # 순위 점수로 정렬 (낮을수록 좋음)
        valid_symbols.sort(key=lambda x: x['rank_score'])
        
        print_log(LogLevel.INFO, "Top 10 symbols by rank score (lower is better):")
        for i, symbol_data in enumerate(valid_symbols[:10]):
            print_log(LogLevel.INFO, 
                     f"{i+1}. {symbol_data['symbol']}: "
                     f"Rank Score: {symbol_data['rank_score']} "
                     f"(Vol: {symbol_data['volatility']:.4f}, "
                     f"Vol: {symbol_data['trading_volume_3h']:,.0f}M)")
        
        best_symbol = valid_symbols[0]['symbol']
        best_data = valid_symbols[0]
        print_log(LogLevel.SUCCESS, 
                 f"Selected symbol: {best_symbol} "
                 f"(Rank Score: {best_data['rank_score']}, "
                 f"Volatility: {best_data['volatility']:.4f}, "
                 f"3H Volume: {best_data['trading_volume_3h']:,.0f}M KRW)")
        
        return best_symbol

    @staticmethod
    def mark_symbol_as_traded(symbol):
        global traded_symbols
        traded_symbols[symbol] = datetime.now()
        print_log(LogLevel.INFO, f"Marked {symbol} as traded (valid for 1 hour)")

# 전역 트레이딩 매니저
trading_manager = TradingManager()

if __name__=="__main__":
    try:
        parser = argparse.ArgumentParser(description="Upbit Trading Bot")
        parser.add_argument('-a', '--cancel-type', type=int, required=False, help='Cancel order type (1: buy, 2: sell, 3: all)')
        parser.add_argument('-d', '--drop-percentage', type=float, required=False, help='Drop percentage for buy orders')
        parser.add_argument('-f', '--distribution-type', type=int, required=False, help='Distribution type for buy orders')
        parser.add_argument('-p', '--weight', type=float, required=False, help='Weight for distribution')
        parser.add_argument('-s', '--starting-balance', type=int, required=False, help='Starting balance')
        parser.add_argument('-t', '--timeout', type=int, required=False, help='Timeout value')
        parser.add_argument('-v', '--profit-percentage', type=float, required=False, help='Profit percentage for sell orders')
        parser.add_argument('--auto-select', action='store_true', help='Auto select symbol based on composite scoring')
        
        args = parser.parse_args()
        
        with open("../key.txt", 'r', encoding='utf-8') as f:
            ACCESS_KEY = f.readline().strip()
            SECRET_KEY = f.readline().strip()
        
        START_TIME = datetime.now()
    
        if args.cancel_type is not None:
            OrderCanceler().cancel_all_orders(args.cancel_type)
        else:
            OrderCanceler().cancel_all_orders(1)

        drop_percentage = args.drop_percentage if args.drop_percentage else 1 / 3
        distribution_type = args.distribution_type if args.distribution_type else DynamicBuyOrder.DistributionType.LOG_LINEAR_II
        distribution_weight = args.weight if args.weight else 1 / 30
        profit_percentage = args.profit_percentage if args.profit_percentage else 0.16

        InitialBalance = S = AccountChecker().get_krw_balance()
        print_log(LogLevel.INFO, f"Available KRW: {int(S):,}")
        log_balance(S)

        if args.starting_balance is not None:
            if args.starting_balance < 1000000:
                print_log(LogLevel.ERROR, "Minimum starting balance is 1,000,000 won")
                exit()
            else:
                S = int(args.starting_balance)
        else:
            S = int(S)  # 수수료 사전 공제 제거 — 잔액 전액 투자 (수수료는 체결 시 업비트가 부과)

        cycle_count = 0
        while True:
            cycle_count += 1

            # 1. command.txt 변경 체크 (새로운 심볼만 저장, 현재 거래는 중단하지 않음)
            new_symbol_detected = trading_manager.check_command_file()
            if new_symbol_detected:
                print_log(LogLevel.INFO, f"New symbol detected in command file: {new_symbol_detected}, will switch after current trading completes")
                # 현재 거래는 계속 진행, 다음 사이클에서 새로운 심볼로 전환

            cached_symbol = trading_manager.get_cached_symbol()
            if cached_symbol:
                symbol = cached_symbol
                print_log(LogLevel.INFO, f"Using symbol: {symbol}")
                
                # 캐시된 심볼에 대해 변동성 보호 체크
                if VolatilityProtector.check_volatility_protection(symbol):
                    print_log(LogLevel.WARNING, f"Symbol {symbol} blocked by volatility protection - clearing cache")
                    trading_manager.reset()
                    current_trading_symbol = None
                    symbol_cache_time = None
                    continue
                    
                analyzer = MarketAnalyzer(symbol)
            else:
                # command.txt에서 심볼 읽기 (기존 로직)
                symbol_from_command = None
                try:
                    with open("../log/command.txt", 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                        for line in lines:
                            text = line.strip().upper()
                            parts = text.split(' ')
                            if parts[0] == 'SYMBOL' and len(parts) > 1:
                                symbol_from_command = parts[1]
                                break
                except:
                    symbol_from_command = None

                if symbol_from_command:
                    symbol = symbol_from_command
                    # 변동성 보호 체크
                    if VolatilityProtector.check_volatility_protection(symbol):
                        print_log(LogLevel.WARNING, f"Command symbol {symbol} blocked by volatility protection")
                        log_state(LogState.ERROR, "VOLATILITY_PROTECTION")
                        time.sleep(30)
                        continue
                else:
                    # 자동 선별 — 심볼 발견될 때까지 무한 대기 (대안 폴백 없음)
                    selected_symbol = SymbolSelector.select_best_symbol()
                    if selected_symbol is None:
                        print_log(LogLevel.WARNING,
                                 "No valid symbol found — waiting 30s, will retry until a symbol is available")
                        time.sleep(30)
                        continue
                    symbol = selected_symbol

                analyzer = MarketAnalyzer(symbol)
                trading_manager.set_symbol(symbol)

            print_log(LogLevel.INFO, f"=== Trading Cycle {cycle_count} ===")
            print_log(LogLevel.INFO, f"Target Symbol: {symbol}")

            # 웹소켓 ticker 구독 (심볼 확정 시)
            RealMarketData.subscribe_websocket(symbol)

            # 매수 프로세스 시작 전 command 변경 체크
            if trading_manager.should_place_buy_orders():
                # command 오버라이드가 있으면 즉시 적용 (새로운 거래 시작 시에만)
                if trading_manager.pending_symbol_change and not trading_manager.is_trading_in_progress():
                    symbol = trading_manager.apply_pending_symbol_change()
                    print_log(LogLevel.INFO, f"Applied command override symbol: {symbol}")
                    analyzer = MarketAnalyzer(symbol)
                    
                analyzer = MarketAnalyzer(symbol)
                drop_count = 10

                # 새 매수 전 선제 스윕: 잔존 매수(bid) 주문 확실히 정리
                # (사이클 타임아웃/재시작 등으로 거래소에 남은 주문이 새 매수와 겹치는 것 방지)
                OrderCanceler().cancel_buy_orders()

                print_log(LogLevel.INFO,
                         f"Market Analysis - RSI: {analyzer.get_rsi():.2f}, "
                         f"Volatility: {analyzer.volatility_ratio:.4f}, "
                         f"Drop Levels: {drop_count}")

                dynamic_buyer = DynamicBuyOrder(symbol, analyzer.candle.current_price, analyzer.candle.low_prices[-1], S, distribution_weight, 0)
                dynamic_buyer.calculate_order_plan(drop_percentage, drop_count, distribution_type)

                # 동적 매수 실행
                if dynamic_buyer.execute_dynamic_buy_orders():
                    print_log(LogLevel.SUCCESS, "Dynamic buying started successfully")
                    trading_manager.mark_buy_orders_placed()
                    
                    # 병렬 관리: 매수 진행 중에도 매도 관리 시작
                    print_log(LogLevel.SUCCESS, "=== STARTING PARALLEL BUY/SELL MANAGEMENT ===")
                    sell_controller = SellController()
                    # 삼중 분할매도 — 0.15% / 0.18% / 0.21%, 각각 보유량의 1/3씩
                    profit_targets = [0.15, 0.18, 0.21]
                    
                    cycle_start_time = datetime.now()
                    cycle_timeout = 86400  
                    
                    trading_completed = False
                    command_changed_during_trading = False
                    
                    while not trading_completed:
                        current_time = datetime.now()
                        
                        # 타임아웃 체크
                        if (current_time - cycle_start_time).total_seconds() > cycle_timeout:
                            print_log(LogLevel.WARNING, f"Trading cycle timeout after {cycle_timeout} seconds")
                            # 타임아웃 종료 시 잔존 매수 확실히 취소 (다음 사이클로 넘어가기 전)
                            OrderCanceler().cancel_buy_orders()
                            trading_completed = True
                            break
                        
                        # 1. command 변경 체크 (거래 중에는 플래그만 설정, 중단하지 않음)
                        if trading_manager.check_command_file():
                            command_changed_during_trading = True
                            new_symbol = trading_manager.get_command_symbol_override()
                            print_log(LogLevel.INFO, f"Command file changed during trading (new symbol: {new_symbol}). Will complete current trading first.")
                            # 현재 거래는 계속 진행, 다음 사이클에서 새로운 심볼로 전환
                        
                        # 2. 동적 매수 진행 체크
                        if dynamic_buyer.is_active:
                            dynamic_buyer.check_and_continue()
                        else:
                            # 첫 번째 주문이 타임아웃되면 거래 중단
                            if len(dynamic_buyer.executed_orders) == 0:
                                print_log(LogLevel.WARNING, "First order timeout - stopping trading cycle")
                                # 잔존 매수 확실히 취소 후 종료
                                OrderCanceler().cancel_buy_orders()
                                trading_completed = True
                                break
                        
                        # 3. 매도 관리
                        balance, locked, avg_buy_price = AccountChecker().get_symbol_info(symbol)
                        current_volume = balance + locked
                        
                        if current_volume > 0.00001:
                            # 보유량이 있으면 매도 관리 시작
                            if not trading_manager.buy_orders_executed:
                                trading_manager.mark_buy_orders_executed()
                                print_log(LogLevel.SUCCESS, f"Buy orders executed - Holdings: {current_volume:.6f}")
                            
                            # 매도 주문 관리
                            is_trading_complete = sell_controller.manage_sell_orders(
                                symbol, profit_targets, trading_manager, 0, dynamic_buyer
                            )
                            
                            if is_trading_complete:
                                print_log(LogLevel.SUCCESS, "Trading completed (sell orders executed)")
                                trading_completed = True
                                break
                        else:
                            # 보유량이 없으면 거래 완료
                            if trading_manager.buy_orders_executed:
                                sell_uuids.clear()

                                print_log(LogLevel.SUCCESS, "No holdings left - trading completed")
                                trading_manager.mark_sell_orders_executed()
                                trading_completed = True
                                break
                        
                        # 4. 스탑로스 체크
                        if sell_controller.check_stop_loss(symbol, trading_manager):
                            print_log(LogLevel.WARNING, "Stop loss triggered")
                            trading_completed = True
                            break
                        
                        time.sleep(SLEEP_TIME)
                    
                    # 거래 중 command 변경이 있었으면 알림
                    if command_changed_during_trading:
                        new_symbol = trading_manager.get_command_symbol_override()
                        print_log(LogLevel.INFO, f"Current trading completed. Will switch to new symbol '{new_symbol}' in next cycle.")

                log_state(LogState.BUYING, symbol)
                print_log(LogLevel.INFO, f"Buy orders placed for '{symbol}'")
                threading.Thread(target=winsound.Beep, args=(440, 500)).start()

            # 거래 완료 처리
            if trading_manager.is_trading_complete():
                if args.auto_select and not trading_manager.stop_loss_triggered:
                    SymbolSelector.mark_symbol_as_traded(symbol)
                
                OrderCanceler().cancel_buy_orders()

                if trading_manager.stop_loss_triggered:
                    print_log(LogLevel.WARNING, "Trading completed due to stop loss")
                    exit(0)
                else:
                    print_log(LogLevel.SUCCESS, "Trading completed successfully")

            # 잔고 업데이트 및 다음 사이클 준비
            S = int(AccountChecker().get_krw_balance())
            profit_loss = int(S - InitialBalance)
            print_log(LogLevel.INFO,
                     f"Cycle {cycle_count} Result - Profit/Loss: {profit_loss:+,} KRW ({datetime.now() - START_TIME})")

            log_balance(S)
            S = int(S)
            
            trading_manager.reset()
            print_log(LogLevel.INFO, f"Cycle {cycle_count} completed. Waiting for next cycle...")
            
    except Exception as e:
        log_state(LogState.ERROR)
        print_log(LogLevel.ERROR, f"Unexpected error: {str(e)}")
        traceback.print_exc()
        time.sleep(60)
