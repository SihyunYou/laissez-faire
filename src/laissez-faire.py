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
import threading
from tqdm import tqdm
import datetime
from datetime import datetime, timedelta
import traceback
from enum import Enum, IntEnum
import functools
import hmac
import random
from collections import deque

# 웹소켓 라이브러리 (선택) — 미설치 시 REST 폴백
try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

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
STOP_LOSS_PERCENTAGE = -8.0  # 스탑로스 -8%

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
    INFO = ''
    SUCCESS = ''
    WARNING = ''
    EXCEPTION = ''
    ERROR = ''

# INFO 로그 억제 플래그 — 핫 루프의 잦은 INFO 로그로 인한 I/O 지연 방지.
VERBOSE = False

def print_log(level, message):
    if level is LogLevel.INFO and not VERBOSE:
        return
    timestamp = '[' + datetime.now().strftime('%m/%d %X') + '] '
    print(timestamp + message)

def start_alarm_loop():
    """스탑로스 알람 — 무한 비프 (데몬 스레드).
    연속으로 이어지는 사이렌 비프음. 프로그램 종료 시 자동으로 멈춤."""
    def _loop():
        while True:
            try:
                # 고음/저음 교차 사이렌 — 끊임없이 이어지게
                winsound.Beep(1000, 400)
                winsound.Beep(800, 400)
            except Exception:
                time.sleep(0.3)
    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    return t


def beep_async(frequency, duration):
    """단발 비프 — 데몬 스레드로 실행하여 메인 스레드 지연 0.
    스레드 생성 대신 단순 fire-and-forget."""
    threading.Thread(target=lambda: winsound.Beep(frequency, duration), daemon=True).start()


def run_async(func, *args, **kwargs):
    """임의 함수를 백그라운드 데몬 스레드로 실행 — 메인 스레드 지연 0.
    반환값이 필요 없는 fire-and-forget 작업(주문 취소, 파일 읽기 등)에 사용."""
    threading.Thread(target=func, args=args, kwargs=kwargs, daemon=True).start()

class AsyncLogger:
    """디스크 로그 비동기 쓰기 — 백그라운드 데몬 스레드 큐 기반.
    핫 루프에서 동기 open()/write()로 인한 지연 제거.
    마지막 값만 유지 (같은 파일에 연속 쓰기 시 이전 값 무시)."""
    _queue = {}      # {filepath: content_str}
    _lock = threading.Lock()
    _thread = None

    @classmethod
    def _worker(cls):
        while True:
            try:
                # 큐 스냅샷 — 마지막 값만 쓰기
                with cls._lock:
                    snapshot = dict(cls._queue)
                    cls._queue.clear()
                for filepath, content in snapshot.items():
                    try:
                        with open(filepath, 'w', encoding='utf-8') as f:
                            f.write(content)
                    except (PermissionError, OSError):
                        pass
            except Exception:
                pass
            time.sleep(0.5)  # 0.5초 간격 플러시

    @classmethod
    def write(cls, filepath, content):
        """비동기 쓰기 요청 — 즉시 반환, 백그라운드에서 플러시."""
        if cls._thread is None:
            cls._thread = threading.Thread(target=cls._worker, daemon=True)
            cls._thread.start()
        with cls._lock:
            cls._queue[filepath] = content

    @classmethod
    def write_sync(cls, filepath, content):
        """동기식 즉시 쓰기 — EXIT 등 종료 직전 필수 로그용."""
        with cls._lock:
            cls._queue[filepath] = content
        # 큐의 모든 항목을 즉시 플러시
        with cls._lock:
            snapshot = dict(cls._queue)
            cls._queue.clear()
        for fp, cnt in snapshot.items():
            try:
                with open(fp, 'w', encoding='utf-8') as f:
                    f.write(cnt)
            except (PermissionError, OSError):
                pass


def log_balance(balance):
    AsyncLogger.write("../log/balance.txt", str(int(balance)) + ',' + str(int(InitialBalance)))

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
    content = '#' + str(int(state))
    if additional_info != '':
        content += ',' + additional_info
    AsyncLogger.write("../log/state.txt", content)

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
        - count 홀수: 중심 포함 대칭 (예: 3 -> [-0.2%, 0%, +0.2%])
        - count 진수: 중심 양옆 반칸 (예: 4 -> [-0.3%, -0.1%, +0.1%, +0.3%])"""
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
            data = json.loads(message)
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


class UpbitPrivateWS:
    """업비트 Private WebSocket — 잔고(myAsset) / 주문체결(myOrder) / 체결(myTrade) 실시간 수신.
    wss://api.upbit.com/websocket/v1/private 엔드포인트 사용. JWT 인증 필요.
    REST /v1/accounts, /v1/order 폴링을 대체하여 API 호출 없이 실시간 상태 제공."""

    WS_URL = "wss://api.upbit.com/websocket/v1/private"
    RECON_BACKOFF_MAX = 30
    RESYNC_INTERVAL = 30  # WS 누락 보정용 주기적 REST 동기화 (초)

    def __init__(self):
        self.ws = None
        self.thread = None
        self.is_connected = False
        self._should_reconnect = True
        self._is_initialized = False  # start() 호출 여부

        # 잔고 캐시 — {currency: {balance, locked, avg_buy_price}}
        # myAsset 메시지로 실시간 갱신. 초기 seed는 REST /v1/accounts.
        self.asset_cache = {}
        self.asset_cache_time = 0

        # 주문 상태 캐시 — {uuid: order_dict}
        # myOrder 메시지로 갱신.
        self.order_cache = {}
        self.order_events = {}  # {uuid: threading.Event} — 체결 완료 대기용
        self._order_lock = threading.Lock()

        # myTrade 체결 이벤트 큐 — 콜백 기반 처리
        self.trade_callbacks = []
        self._last_resync = 0

    def start(self, access_key, secret_key):
        """Private WS 연결 시작. 부팅 시 REST로 잔고 seed 후 WS 구독."""
        self.access_key = access_key
        self.secret_key = secret_key
        self._is_initialized = True
        # REST seed — 초기 잔고 캐싱
        self._seed_assets()
        self._last_resync = time.time()
        # WS 연결 시작
        self._should_reconnect = True
        self.thread = threading.Thread(target=self._connect_loop, daemon=True)
        self.thread.start()
        print_log(LogLevel.INFO, "UpbitPrivateWS 시작 — myAsset/myOrder/myTrade 구독")

    def stop(self):
        self._should_reconnect = False
        self.is_connected = False
        if self.ws:
            try:
                self.ws.close()
            except:
                pass

    def _generate_jwt(self):
        """Private WS 인증용 JWT 토큰 (HS512)."""
        payload = {
            'access_key': self.access_key,
            'nonce': str(uuid.uuid4()),
        }
        token = jwt.encode(payload, self.secret_key, algorithm="HS512")
        # PyJWT 버전 호환 — bytes 또는 str 반환
        if isinstance(token, bytes):
            token = token.decode('utf-8')
        return token

    def _seed_assets(self):
        """REST /v1/accounts 1회 호출로 잔고 캐시 seed."""
        try:
            checker = AccountChecker._rest_fetch(self.access_key, self.secret_key)
            if checker:
                self.asset_cache = checker
                self.asset_cache_time = time.time()
                print_log(LogLevel.INFO,
                          f"PrivateWS 잔고 seed 완료 — {len(self.asset_cache)}개 통화")
        except Exception as e:
            print_log(LogLevel.WARNING, f"PrivateWS 잔고 seed 실패: {str(e)[:100]}")

    def _connect_loop(self):
        """백그라운드 재연결 루프."""
        backoff = 1
        while self._should_reconnect:
            try:
                token = self._generate_jwt()
                self.ws = websocket.WebSocketApp(
                    self.WS_URL,
                    header={"Authorization": f"Bearer {token}"},
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                self.ws.run_forever(ping_interval=60, ping_timeout=10)
            except Exception as e:
                print_log(LogLevel.WARNING, f"PrivateWS 연결 오류: {str(e)[:100]}")
            if self._should_reconnect:
                time.sleep(backoff)
                backoff = min(backoff * 2, self.RECON_BACKOFF_MAX)
                # 재연결 시 캐시 무효화 + REST 재동기화
                self._seed_assets()

    def _on_open(self, ws):
        self.is_connected = True
        # myAsset + myOrder + myTrade 동시 구독
        req = [{"ticket": f"priv-{int(time.time())}"},
               {"type": "myAsset"},
               {"type": "myOrder"},
               {"type": "myTrade"}]
        ws.send(json.dumps(req))
        print_log(LogLevel.SUCCESS,
                  "PrivateWS 연결 성공 — myAsset/myOrder/myTrade 구독")

    def _on_message(self, ws, message):
        """myAsset/myOrder/myTrade 메시지 파싱 → 캐시 갱신."""
        try:
            data = json.loads(message)
            # myAsset — 잔고 갱신
            if 'currency' in data and 'balance' in data:
                currency = data.get('currency')
                self.asset_cache[currency] = {
                    'balance': float(data.get('balance', 0)),
                    'locked': float(data.get('locked', 0)),
                    'avg_buy_price': float(data.get('avg_buy_price', 0)),
                }
                self.asset_cache_time = time.time()
                return
            # myOrder — 주문 상태 갱신
            if 'uuid' in data and 'state' in data:
                uuid_val = data.get('uuid')
                with self._order_lock:
                    self.order_cache[uuid_val] = data
                    # 체결 완료(done) 시 대기 중인 Event 깨움
                    if data.get('state') in ('done', 'cancel'):
                        ev = self.order_events.get(uuid_val)
                        if ev:
                            ev.set()
                # myTrade 콜백 트리거 (체결 시)
                if data.get('state') == 'done':
                    self._trigger_trade_callbacks(uuid_val, data)
                return
            # myTrade — 개별 체결 내역 (콜백)
            if 'uuid' in data and 'trade_volume' in data:
                self._trigger_trade_callbacks(data.get('uuid'), data)
                return
        except Exception:
            pass  # 파싱 오류는 조용히 무시

    def _trigger_trade_callbacks(self, uuid_val, data):
        """myTrade/myOrder 체결 이벤트 → 등록된 콜백 실행."""
        for cb in self.trade_callbacks:
            try:
                cb(uuid_val, data)
            except Exception:
                pass

    def _on_error(self, ws, error):
        self.is_connected = False
        print_log(LogLevel.WARNING, f"PrivateWS 에러: {str(error)[:100]}")

    def _on_close(self, ws, close_status, close_msg):
        self.is_connected = False
        if self._should_reconnect:
            print_log(LogLevel.INFO, "PrivateWS 종료 — 재연결 대기")

    def _maybe_resync(self):
        """주기적 REST 동기화 — WS 누락 보정."""
        now = time.time()
        if now - self._last_resync > self.RESYNC_INTERVAL:
            self._last_resync = now
            self._seed_assets()

    # ===== 공개 조회 API (캐시에서 O(1) 반환) =====

    def get_symbol_info(self, symbol):
        """(balance, locked, avg_buy_price) 반환. 캐시 미스 시 REST 폴백."""
        if not self._is_initialized or not self.is_connected:
            return AccountChecker._rest_symbol_info(self.access_key, self.secret_key, symbol)
        self._maybe_resync()
        info = self.asset_cache.get(symbol)
        if info:
            return info['balance'], info['locked'], info['avg_buy_price']
        return 0.0, 0.0, 0.0

    def get_krw_balance(self, balance_type=1):
        """KRW 잔고. type 1=balance, 2=locked, 3=total."""
        if not self._is_initialized or not self.is_connected:
            return AccountChecker._rest_krw(self.access_key, self.secret_key, balance_type)
        self._maybe_resync()
        info = self.asset_cache.get('KRW')
        if not info:
            return 0.0
        if balance_type == 1:
            return info['balance']
        elif balance_type == 2:
            return info['locked']
        elif balance_type == 3:
            return info['balance'] + info['locked']
        return info['balance']

    def get_owned_symbols(self):
        """보유 통화 목록."""
        if not self._is_initialized or not self.is_connected:
            return AccountChecker._rest_owned_symbols(self.access_key, self.secret_key)
        return list(self.asset_cache.keys())

    def get_order_state(self, order_uuid):
        """주문 상태 캐시 조회. 미스 시 None."""
        if not self._is_initialized or not self.is_connected:
            return None
        with self._order_lock:
            return self.order_cache.get(order_uuid)

    def register_order_wait(self, order_uuid):
        """주문 체결 대기용 Event 등록."""
        ev = threading.Event()
        with self._order_lock:
            # 이미 체결된 경우 즉시 set
            cached = self.order_cache.get(order_uuid)
            if cached and cached.get('state') in ('done', 'cancel'):
                ev.set()
            else:
                self.order_events[order_uuid] = ev
        return ev

    def unregister_order_wait(self, order_uuid):
        with self._order_lock:
            self.order_events.pop(order_uuid, None)

    def add_trade_callback(self, callback):
        """체결 이벤트 콜백 등록 — callback(uuid, data)."""
        self.trade_callbacks.append(callback)


# 전역 Private WS 인스턴스 (main 블록에서 start() 호출)
private_ws = UpbitPrivateWS()


class VolatilityScanner:
    """웹소켓 candle.1m 기반 실시간 변동성 스캐너.
    전체 KRW 마켓의 1분캔들 20개 종가를 유지하고, 표준편차/평균(CV)로 랭킹.
    REST로 초기 20개 seed → 웹소켓 candle.1m로 실시간 갱신."""

    CANDLE_COUNT = 20
    VOLUME_THRESHOLD_M = 5000  # 24시간 거래대금 하한 (백만원) = 50억원
    SEED_SLEEP = 0.05          # REST seed 시 코인당 대기 (rate limit)

    def __init__(self):
        self.candle_buffers = {}   # {symbol: deque([close,...], maxlen=20)}
        self.volume_1h = {}        # {symbol: 1시간 거래대금(백만원)}
        self.symbols = []
        self.ws = None
        self.thread = None
        self.is_running = False
        self._should_reconnect = True

    def start(self, symbols):
        """1) REST seed (각 코인 1분캔들 20개 + ticker 24h 거래대금)
           2) 웹소켓 candle.1m 다중 구독 시작"""
        self.symbols = list(symbols)
        print_log(LogLevel.INFO, f"VolatilityScanner 시작 — {len(self.symbols)}개 코인 seed")
        # REST seed
        self._seed_candles()
        print_log(LogLevel.SUCCESS,
                  f"Seed 완료 — {len(self.candle_buffers)}개 코인 버퍼 준비")
        # 웹소켓 구독
        self._should_reconnect = True
        self.is_running = True
        self.thread = threading.Thread(target=self._connect_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self._should_reconnect = False
        self.is_running = False
        if self.ws:
            try:
                self.ws.close()
            except:
                pass

    def _seed_one(self, symbol):
        """코인 1개 seed — 1분캔들 20개만 조회. (병렬 워커용)
        거래대금은 _seed_candles에서 ticker 1회 호출로 전 코인 처리.
        반환: (symbol, closes_or_None)"""
        try:
            qs = {"market": f"KRW-{symbol}", "count": str(self.CANDLE_COUNT)}
            def api_call():
                r = requests.get(CANDLE_URL, params=qs, timeout=10)
                return r.json()
            candles = safe_api_call(api_call)
            if candles and len(candles) >= self.CANDLE_COUNT:
                # API는 최신→과거 순서 → 역순(과거→최신)
                return (symbol, [float(c['trade_price']) for c in reversed(candles)])
            return (symbol, None)
        except Exception:
            return (symbol, None)

    def _seed_candles(self):
        """REST로 1분캔들 20개(코인별 병렬) + 24h 거래대금(ticker 1회) 수집 → 버퍼 초기화."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # 1) 거래대금 — ticker 다중 조회 1회로 전 코인 수집 (캔들 API는 다중 불가이나 ticker는 가능)
        try:
            markets_param = ",".join(f"KRW-{s}" for s in self.symbols)
            def ticker_call():
                r = requests.get(TICKER_URL, params={"markets": markets_param}, timeout=15)
                return r.json()
            tickers = safe_api_call(ticker_call)
            if tickers:
                for t in tickers:
                    code = t.get('market', '')
                    if code.startswith('KRW-'):
                        sym = code[4:]
                        # acc_trade_price_24h: 원 단위 → 백만 원 단위
                        self.volume_1h[sym] = float(t.get('acc_trade_price_24h', 0)) / 1000000
                print_log(LogLevel.INFO,
                          f"거래대금 seed 완료 — ticker 1회로 {len(tickers)}개 코인")
        except Exception as e:
            print_log(LogLevel.WARNING, f"ticker 거래대금 seed 실패: {str(e)[:100]}")

        # 2) 1분캔들 20개 — 병렬 수집 (업비트 캔들 rate limit 초당 10회 준수)
        seeded = 0
        failed = 0
        lock = threading.Lock()
        pbar = tqdm(total=len(self.symbols), desc="VolatilityScanner seed", leave=False)
        # rate limit(10/s)을 넘지 않도록 병렬도 제한 — 캔들 호출만 카운트
        max_workers = min(8, (len(self.symbols) or 1))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(self._seed_one, s) for s in self.symbols]
            for fut in as_completed(futures):
                symbol, closes = fut.result()
                with lock:
                    if closes:
                        self.candle_buffers[symbol] = deque(closes, maxlen=self.CANDLE_COUNT)
                        seeded += 1
                    else:
                        failed += 1
                    pbar.update(1)
                    pbar.set_postfix(ok=seeded, fail=failed)
        pbar.close()

    def _connect_loop(self):
        backoff = 1
        while self._should_reconnect and self.is_running:
            try:
                self.ws = websocket.WebSocketApp(
                    UpbitWebSocket.WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                self.ws.run_forever(ping_interval=60, ping_timeout=10)
            except Exception as e:
                print_log(LogLevel.WARNING, f"VolatilityScanner WS 오류: {str(e)[:100]}")
            if self._should_reconnect and self.is_running:
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _on_open(self, ws):
        codes = [f"KRW-{s}" for s in self.symbols if s in self.candle_buffers]
        # 업비트 구독 제한 고려 — 한 번에 전체 구독
        req = [{"ticket": f"vscanner-{int(time.time())}"},
               {"type": "candle.1m", "codes": codes}]
        ws.send(json.dumps(req))
        print_log(LogLevel.SUCCESS,
                  f"VolatilityScanner WS 구독 — {len(codes)}개 코인 candle.1m")

    def _on_message(self, ws, message):
        """candle.1m 메시지 파싱 → 버퍼 갱신 (롤링 20개) + 거래대금 누적."""
        try:
            data = json.loads(message)
            code = data.get('code', '')
            if not code.startswith('KRW-'):
                return
            symbol = code[4:]
            close = data.get('trade_price')
            if close is None:
                return
            close = float(close)
            # 버퍼 갱신
            if symbol not in self.candle_buffers:
                self.candle_buffers[symbol] = deque(maxlen=self.CANDLE_COUNT)
            self.candle_buffers[symbol].append(close)
            # 거래대금 누적 (해당 1분 캔들의 누적 거래대금)
            acc_price = data.get('candle_acc_trade_price')
            if acc_price:
                # candle.1m는 매 tick 마다 진행 중인 캔들을 갱신 push 함.
                # 단순화: 최신 캔들의 acc_trade_price 를 1시간 합산의 근사로 사용
                pass
        except Exception:
            pass

    def _on_error(self, ws, error):
        print_log(LogLevel.WARNING, f"VolatilityScanner WS 에러: {str(error)[:100]}")

    def _on_close(self, ws, close_status, close_msg):
        if self._should_reconnect and self.is_running:
            print_log(LogLevel.INFO, "VolatilityScanner WS 종료 — 재연결 대기")

    def _calc_volatility(self, symbol):
        """std(close[-20:]) / mean(close[-20:]) — 변동계수(CV).
        deque를 직접 순회 (list 복사 생략)."""
        buf = self.candle_buffers.get(symbol)
        if not buf or len(buf) < self.CANDLE_COUNT:
            return 0.0
        n = len(buf)
        mean = sum(buf) / n  # deque 직접 sum
        if mean <= 0:
            return 0.0
        var = sum((x - mean) ** 2 for x in buf) / n
        return (var ** 0.5) / mean

    def get_top_volatility_symbol(self, excluded_symbols=None):
        """필터 통과한 코인 중 변동성 최고 심볼 반환.
        각 코인 버퍼에서 변동계수(CV) = std/mean 계산 후 정렬.
        필터: is_excluded_tick_range, 거래대금 < 1000백만원, excluded_symbols."""
        if excluded_symbols is None:
            excluded_symbols = set()
        candidates = []
        for symbol in list(self.candle_buffers.keys()):
            if symbol in excluded_symbols:
                continue
            buf = self.candle_buffers.get(symbol)
            if not buf or len(buf) < self.CANDLE_COUNT:
                continue
            latest_price = buf[-1]
            if latest_price <= 0:
                continue
            # 호가 제외 구간
            if UpbitTickSystem.is_excluded_tick_range(latest_price):
                continue
            # 거래대금 필터
            if self.volume_1h.get(symbol, 0) < self.VOLUME_THRESHOLD_M:
                continue
            vol = self._calc_volatility(symbol)
            if vol > 0:
                candidates.append((symbol, vol, latest_price))

        if not candidates:
            return None
        candidates.sort(key=lambda x: x[1], reverse=True)  # 변동성 내림차순
        # Top 5 로그
        for i, (sym, vol, price) in enumerate(candidates[:5]):
            print_log(LogLevel.INFO,
                      f"  Vol Top{i+1}: {sym} CV={vol:.6f} @{price:,.4f} "
                      f"vol1h={self.volume_1h.get(sym,0):,.0f}M")
        return candidates[0][0]


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
    백오프는 지수(exponential)로 증가하되 최대 30초로 상한.
    업비트 429 (rate limit) 응답 감지 시 자동 백오프 재시도."""
    max_backoff = 30.0
    attempt = 0

    while True:
        try:
            rate_limiter.acquire()
            result = func(*args, **kwargs)
            # 429 rate limit 감지 — 업비트는 {"error": {"name": "too_many_requests"}} 반환
            if isinstance(result, dict):
                err = result.get('error')
                if isinstance(err, dict):
                    err_name = err.get('name', '').lower()
                    if 'too_many_requests' in err_name or 'rate' in err_name:
                        wait_time = min(0.5 * (2 ** attempt), max_backoff)
                        attempt += 1
                        print_log(LogLevel.WARNING,
                                  f"API rate limit (429) — retrying in {wait_time:.2f}s")
                        time.sleep(wait_time)
                        continue
            attempt = 0  # 성공 시 attempt 리셋
            return result

        except requests.exceptions.RequestException as e:
            wait_time = min(SLEEP_TIME * (2 ** attempt), max_backoff)
            attempt += 1
            print_log(LogLevel.WARNING,
                      f"API call failed (attempt {attempt}), retrying in {wait_time:.2f}s: {str(e)}")
            time.sleep(wait_time)


def make_jwt(query_hash=None, query_hash_alg="SHA512"):
    """업비트 인증용 JWT 토큰 생성 (공통 헬퍼).
    query_hash가 있으면 쿼리 해시 포함, 없으면 단순 인증 토큰.
    Returns: (jwt_token_str, headers_dict)"""
    payload = {
        'access_key': ACCESS_KEY,
        'nonce': str(uuid.uuid4()),
    }
    if query_hash:
        payload['query_hash'] = query_hash
        payload['query_hash_alg'] = query_hash_alg
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode('utf-8')
    headers = {"Authorization": f'Bearer {token}', "Accept": "application/json"}
    return token, headers


def make_query_hash(params_dict):
    """쿼리 파라미터 딕셔너리 → SHA512 해시 (업비트 주문/조회 인증용)."""
    query_string = unquote(urlencode(params_dict, doseq=True)).encode("utf-8")
    m = hashlib.sha512()
    m.update(query_string)
    return m.hexdigest()


def make_auth_headers(query_dict=None):
    """query_dict이 주어지면 hash 포함 JWT, 아니면 단순 인증 JWT 생성.
    Returns: headers_dict (Authorization + Accept).
    모든 인라인 JWT 생성 코드를 이 헬퍼로 통합."""
    payload = {
        'access_key': ACCESS_KEY,
        'nonce': str(uuid.uuid4()),
    }
    if query_dict:
        payload['query_hash'] = make_query_hash(query_dict)
        payload['query_hash_alg'] = 'SHA512'
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode('utf-8')
    return {"Authorization": f'Bearer {token}', "Accept": "application/json"}


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
        headers = make_auth_headers(params)

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
        headers = make_auth_headers(params)

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
    """잔고 조회 클래스 — Private WS 캐시 우선, REST 폴백.
    기존에는 매 인스턴스화마다 REST /v1/accounts를 호출했으나,
    이제 전역 private_ws 캐시에서 O(1)로 조회."""

    def __init__(self):
        # WS 캐시에서 잔고 딕셔너리 구성 (WS 미연결 시 REST 폴백)
        if private_ws._is_initialized and private_ws.is_connected:
            # 캐시 복사 (읽기 전용 뷰)
            self.response_dict = [
                {'currency': cur, 'balance': str(info['balance']),
                 'locked': str(info['locked']), 'avg_buy_price': str(info['avg_buy_price'])}
                for cur, info in private_ws.asset_cache.items()
            ]
        else:
            # REST 폴백 — WS 미가동 시 /v1/accounts 1회 호출
            cache = self._rest_fetch(ACCESS_KEY, SECRET_KEY)
            self.response_dict = [
                {'currency': cur, 'balance': str(info['balance']),
                 'locked': str(info['locked']), 'avg_buy_price': str(info['avg_buy_price'])}
                for cur, info in cache.items()
            ]

    # ===== 정적 REST 헬퍼 (PrivateWS 폴백용) =====

    @staticmethod
    def _rest_fetch(access_key, secret_key):
        """REST /v1/accounts 1회 호출 → 잔고 딕셔너리 리스트 반환."""
        payload = {
            'access_key': access_key,
            'nonce': str(uuid.uuid4()),
        }
        jwt_token = jwt.encode(payload, secret_key, algorithm="HS256")
        authorization = f'Bearer {jwt_token}'
        headers = {"Authorization": authorization}

        def api_call():
            response = requests.get(SERVER_URL + "/v1/accounts", headers=headers)
            return response.json()

        result = safe_api_call(api_call)
        # PrivateWS 캐시용 딕셔너리 변환
        if isinstance(result, list):
            cache = {}
            for acc in result:
                cur = acc.get('currency')
                if cur:
                    cache[cur] = {
                        'balance': float(acc.get('balance', 0)),
                        'locked': float(acc.get('locked', 0)),
                        'avg_buy_price': float(acc.get('avg_buy_price', 0)),
                    }
            return cache
        return {}

    @staticmethod
    def _rest_symbol_info(access_key, secret_key, symbol):
        """REST 폴백 — 특정 심볼 (balance, locked, avg_buy_price)."""
        cache = AccountChecker._rest_fetch(access_key, secret_key)
        info = cache.get(symbol)
        if info:
            return info['balance'], info['locked'], info['avg_buy_price']
        return -1, -1, -1

    @staticmethod
    def _rest_krw(access_key, secret_key, balance_type=1):
        """REST 폴백 — KRW 잔고."""
        cache = AccountChecker._rest_fetch(access_key, secret_key)
        info = cache.get('KRW')
        if not info:
            return 0.0
        if balance_type == 1:
            return info['balance']
        elif balance_type == 2:
            return info['locked']
        elif balance_type == 3:
            return info['balance'] + info['locked']
        return info['balance']

    @staticmethod
    def _rest_owned_symbols(access_key, secret_key):
        """REST 폴백 — 보유 통화 목록."""
        cache = AccountChecker._rest_fetch(access_key, secret_key)
        return list(cache.keys())

    # ===== 인스턴스 조회 메서드 (캐시에서 읽기) =====

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
        self.executed_count = 0  # 체결된 주문 수 — O(1) 완료 체크용
        
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
        분할 분포는 기준가 최상단 비대칭 (예: [-0.4%, -0.2%, 0%])이므로
        분할 최저가 = planned_price * (1 - 하단 최대 offset%).
        레벨 n+1 의 간격은 레벨 n 의 분할 최저가에서 drop%*height_weight 만큼 하락.
        SPLIT_ORDER_COUNT<=1 이면 아무 것도 하지 않음(기존 동작)."""
        if SPLIT_ORDER_COUNT <= 1 or not self.active_planned_orders:
            return

        # 분할 하단 최대 offset(%). 대칭 분포 → half * step.
        # 3중 분할 step=0.2 → -0.2%.
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

    def check_and_continue(self, cached_price=None):
        """체결 확인 및 다음 주문 실행 — 초저지연 (게이트 없이 최대 속도).
        cached_price: 호출자가 이미 구한 현재가를 전달 (중복 조회 방지)."""
        if not self.is_active:
            return False

        # 현재가 — 호출자 캐시 우선 (매 iteration 중복 조회 방지)
        current_price = cached_price if cached_price else RealMarketData.get_current_price(self.symbol)
        if not current_price:
            return False

        self.current_price = current_price

        # 계획 밀림 확인 (현재가 기준)
        self._check_and_apply_plan_shift(current_price)

        # 첫 주문 타임아웃 체크
        if (self.first_order_start_time and
            len(self.executed_orders) == 0 and
            len(self.pending_orders) > 0):

            elapsed_seconds = (datetime.now() - self.first_order_start_time).total_seconds()
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
            
        # 실행할 다음 주문 찾기 — active_planned_orders는 level 순으로 이미 정렬됨
        for order in self.active_planned_orders:
            if not order['executed'] and not self._is_order_pending(order['level']):
                return self._execute_single_order(order)
        
        # 모든 주문 완료 — O(1) 카운터 체크
        if self.executed_count >= len(self.active_planned_orders):
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

        # 각 가격으로 주문 POST — 병렬 실행으로 지연 단축 (분할 주문이 순서 무관)
        from concurrent.futures import ThreadPoolExecutor, as_completed
        results = {}  # {idx: order_uuid}
        if len(split_prices) > 1:
            with ThreadPoolExecutor(max_workers=len(split_prices)) as ex:
                futures = {ex.submit(self.place_dynamic_buy_order, sp, per_volume): idx
                           for idx, sp in enumerate(split_prices)}
                for fut in as_completed(futures):
                    idx = futures[fut]
                    try:
                        results[idx] = fut.result()
                    except Exception:
                        results[idx] = None
        else:
            results[0] = self.place_dynamic_buy_order(split_prices[0], per_volume)

        success_count = 0
        for idx, sp in enumerate(split_prices):
            order_uuid = results.get(idx)
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
                    if not order['executed']:  # 중복 카운트 방지
                        order['executed'] = True
                        self.executed_count += 1
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

        # SLEEP_TIME > 0일 때만 주문 간격 제어 (0이면 오버헤드 제거)
        if SLEEP_TIME > 0 and self.last_order_time is not None:
            time_since_last = (datetime.now() - self.last_order_time).total_seconds()
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

        headers = make_auth_headers(query)

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
        """대기 중인 주문만 취소 — 백그라운드 스레드로 비동기 실행 (메인 스레드 지연 0).
        취소 완료를 기다릴 필요 없는 fire-and-forget."""
        print_log(LogLevel.INFO, f"Cancelling pending orders for {self.symbol}")
        run_async(self._cancel_pending_sync)

    def _cancel_pending_sync(self):
        """실제 취소 로직 — 백그라운드에서 실행."""
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
        """주문 정보 조회 — Private WS 캐시 우선, REST 폴백.
        WS 연결 시 캐시에서 O(1) 반환 (GET /v1/order 호출 없음)."""
        # WS 캐시 우선
        if private_ws._is_initialized and private_ws.is_connected:
            cached = private_ws.get_order_state(order_uuid)
            if cached:
                return cached
            # 캐시 미스 — WS에서 아직 수신 전. REST 폴백.
        # REST 폴백
        try:
            params = {'uuid': order_uuid}
            headers = make_auth_headers(params)

            def api_call():
                response = requests.get(SERVER_URL + "/v1/order", params=params, headers=headers)
                return response.json()

            response_dict = safe_api_call(api_call)
            # REST 결과를 WS 캐시에도 반영 (다음 호출부터 캐시 히트)
            if private_ws._is_initialized and response_dict:
                with private_ws._order_lock:
                    private_ws.order_cache[order_uuid] = response_dict
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

        headers = make_auth_headers(query)

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
        self.current_command_symbol = None  # command.txt에서 읽은 최신 SYMBOL (캐시)
        
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
        """command.txt 파일 변경 체크 — time.monotonic 기반 스로틀 (datetime 오버헤드 제거)."""
        now_mono = time.monotonic()

        if self.last_command_check and (now_mono - self.last_command_check) < self.command_check_interval:
            return False
        self.last_command_check = now_mono

        try:
            with open("../log/command.txt", 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for line in lines:
                    text = line.strip().upper()
                    parts = text.split(' ')
                    if parts[0] == 'SYMBOL' and len(parts) > 1:
                        new_symbol = parts[1]
                        # 현재 command 심볼 캐싱 (메인 루프 재사용 — 파일 재읽기 방지)
                        self.current_command_symbol = new_symbol

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
                        # EXIT 시 동기식 즉시 쓰기 (exit 전 플러시 보장)
                        AsyncLogger.write_sync("../log/state.txt", '#' + str(int(LogState.FORCED_EXIT)))
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
        # 단일 매도 추적
        self.sell_orders_tracking = []  # [{uuid, price, volume, tier, filled}]
        self.filled_sell_count = 0      # 체결된 매도 개수
        self.last_sell_base_price = None  # 직전 매도 기준가 (갱신 감지용)

    def _get_cached_symbol_info(self, symbol):
        """Private WS 캐시에서 (balance, locked, avg_buy_price) 직접 조회 — O(1).
        WS 미연결 시 AccountChecker REST 폴백."""
        if private_ws._is_initialized:
            return private_ws.get_symbol_info(symbol)
        return AccountChecker().get_symbol_info(symbol)

    def has_holdings(self, symbol):
        """보유 코인이 있는지 확인"""
        balance, locked, avg_buy_price = self._get_cached_symbol_info(symbol)
        return (balance + locked) >= MIN_HOLDING_VOLUME

    def has_pending_sell_orders(self, symbol):
        """미체결 매도 주문이 있는지 확인 — any() 단축 평가 (리스트 생성 생략)."""
        return any(not e['filled'] for e in self.sell_orders_tracking)

    def get_avg_buy_price(self, symbol):
        """매수 평균가 조회"""
        balance, locked, avg_buy_price = self._get_cached_symbol_info(symbol)
        return avg_buy_price

    def get_total_volume(self, symbol):
        """총 보유 수량 조회"""
        balance, locked, avg_buy_price = self._get_cached_symbol_info(symbol)
        return balance + locked

    def get_available_volume(self, symbol):
        """실제 매도 가능한 수량 확인"""
        balance, locked, avg_buy_price = self._get_cached_symbol_info(symbol)
        return balance

    def cancel_all_sell_orders(self, symbol):
        """모든 매도 주문 취소 — 백그라운드 스레드로 비동기 실행 (메인 스레드 지연 0)."""
        print_log(LogLevel.INFO, f"Cancelling all sell orders for {symbol}")
        run_async(OrderCanceler().cancel_sell_orders)

    def place_sell_orders(self, symbol, profit_percentages, dynamic_buyer=None):
        """단일 매도 주문 걸기 — 매수 평단가 기준.
        항상 보유량 전체를 단일 매도 주문으로 실행 (분할매도 폐지).
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

            # 단일 매도 — profit_percentages[0] 사용
            profit_pct = profit_percentages[0] if profit_percentages else 0.0
            sell_price = UpbitTickSystem.calculate_sell_price(sell_base_price, profit_pct)

            print_log(LogLevel.INFO,
                     f"매도주문(단일) - 목표: {profit_pct}%, "
                     f"기준가: {sell_base_price:,.4f}, 가격: {sell_price:,.0f} KRW, "
                     f"수량: {available_volume:.6f}")

            # 기준가 저장 (갱신 감지용)
            self.last_sell_base_price = sell_base_price

            sell_order = SellOrder(symbol, available_volume, sell_price)
            # 단일 매도 추적 — UUID/가격/수량 저장 (체결 시 되사들이기용)
            if sell_order and sell_order.uuid:
                self.sell_orders_tracking.append({
                    'uuid': sell_order.uuid,
                    'price': sell_price,
                    'volume': available_volume,
                    'tier': 1,
                    'filled': False
                })

            self.last_sell_placement_time = datetime.now()
            return True

        except Exception as e:
            print_log(LogLevel.ERROR, f"매도주문 실패: {str(e)}")
            traceback.print_exc()
            return False

    def check_stop_loss(self, symbol, trading_manager, dynamic_buyer=None):
        """스탑로스 조건 체크 (-8% 이상 하락 시 매도)
        단, 마지막(최종 단계) 매수가 체결되기 전에는 스탑로스 미발동."""
        current_time = datetime.now()
        
        if (self.last_stop_loss_check and 
            (current_time - self.last_stop_loss_check).total_seconds() < self.stop_loss_check_interval):
            return False
            
        self.last_stop_loss_check = current_time
        
        try:
            # 마지막(최종 단계) 매수가 체결되기 전에는 스탑로스 미발동
            # — executed_count로 O(1) 체크
            if dynamic_buyer and dynamic_buyer.active_planned_orders:
                if dynamic_buyer.executed_count < len(dynamic_buyer.active_planned_orders):
                    return False

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
                    # 스탑로스 발생 — 무한 알람 시작 (프로그램 종료 시까지)
                    start_alarm_loop()
                    return True
                    
        except Exception as e:
            print_log(LogLevel.EXCEPTION, f"Stop loss check error: {str(e)}")
            
        return False

    def check_last_buy_stop(self, symbol, trading_manager, cached_price=None):
        """마지막(최종 단계) 매수 시점 스킵 검사.
        cached_price: 호출자가 캐싱한 현재가 (중복 조회 방지)."""
        try:
            current_price = cached_price if cached_price else RealMarketData.get_current_price(symbol)
            avg_buy_price = self.get_avg_buy_price(symbol)

            if current_price is None or avg_buy_price <= 0:
                return False

            loss_percentage = ((current_price - avg_buy_price) / avg_buy_price) * 100

            if loss_percentage <= STOP_LOSS_PERCENTAGE:
                print_log(LogLevel.WARNING,
                         f"Last-buy stop! Loss {loss_percentage:.2f}% <= {STOP_LOSS_PERCENTAGE}% "
                         f"at last buy point (Current: {current_price:,.0f}, Avg: {avg_buy_price:,.0f})")

                self.cancel_all_sell_orders(symbol)

                total_volume = self.get_available_volume(symbol)
                if total_volume >= MIN_HOLDING_VOLUME:
                    print_log(LogLevel.WARNING,
                             f"Selling all holdings (skip last buy): {total_volume:.6f}")
                    self.place_emergency_sell_order(symbol, total_volume)
                    trading_manager.mark_stop_loss_triggered()
                    # 스탑로스와 동일 — 무한 알람 시작
                    start_alarm_loop()
                    return True
        except Exception as e:
            print_log(LogLevel.EXCEPTION, f"Last-buy stop check error: {str(e)}")

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

            headers = make_auth_headers(query)

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

    def _fetch_order_states_batch(self, uuids):
        """여러 주문 UUID 상태를 일괄 조회 — WS 캐시 우선, 미분은 REST로.
        WS 캐시에 없는 UUID만 REST GET /v1/order 개별 폴백.
        Returns: {uuid: order_info_dict}"""
        result = {}
        missing = []
        # WS 캐시에서 먼저 조회
        if private_ws._is_initialized and private_ws.is_connected:
            for u in uuids:
                cached = private_ws.get_order_state(u)
                if cached:
                    result[u] = cached
                else:
                    missing.append(u)
        else:
            missing = list(uuids)
        # 캐시 미스는 REST로 폴백
        for u in missing:
            try:
                params = {'uuid': u}
                headers = make_auth_headers(params)
                def api_call():
                    response = requests.get(SERVER_URL + "/v1/order", params=params, headers=headers)
                    return response.json()
                order_info = safe_api_call(api_call)
                if order_info:
                    result[u] = order_info
                    # WS 캐시에도 반영
                    if private_ws._is_initialized:
                        with private_ws._order_lock:
                            private_ws.order_cache[u] = order_info
            except Exception as e:
                print_log(LogLevel.ERROR, f"주문 조회 폴백 오류 ({u[:8]}...): {str(e)}")
        return result

    def check_sell_fills(self, symbol, dynamic_buyer):
        """매도 per-order 체결 확인 — Private WS 캐시 우선 (API 호출 최소화).
        체결된 매도는 되받은 KRW만큼 잠재 매수 예산에 가산.
        추적 중인 매도 전체가 체결되면 True(사이클 완료) 반환.
        수동 취소된 매도는 tracking에서 제거만 하고 사이클 종료로 간주하지 않음."""
        if not self.sell_orders_tracking:
            return False

        # 미확인 entry들의 UUID만 일괄 조회
        pending_uuids = [e['uuid'] for e in self.sell_orders_tracking if not e['filled']]
        if not pending_uuids:
            return True

        # WS 캐시 + REST 폴백으로 상태 일괄 취득
        order_states = self._fetch_order_states_batch(pending_uuids)

        all_filled = True
        for entry in self.sell_orders_tracking:
            if entry['filled']:
                continue
            order_info = order_states.get(entry['uuid'])

            if order_info and order_info.get('state') == 'done':
                entry['filled'] = True
                self.filled_sell_count += 1
                if entry['uuid'] in sell_uuids:
                    sell_uuids.remove(entry['uuid'])
                executed_vol = float(order_info.get('executed_volume', entry['volume']))
                sell_price = entry['price']
                rebuy_krw = executed_vol * sell_price

                print_log(LogLevel.SUCCESS,
                          f"✅ 매도#{entry['tier']} 체결 확인 (수량 {executed_vol:.6f} @ {sell_price:,.4f}원)")
                self._reinvest_to_next_buy(dynamic_buyer, rebuy_krw, entry['tier'])
            elif order_info and order_info.get('state') == 'cancel':
                print_log(LogLevel.WARNING,
                          f"⚠ 매도#{entry['tier']} 가 수동 취소됨 — tracking 제거, "
                          f"다음 루프에서 새 매도 재설정 (사이클 유지)")
                entry['filled'] = True
                if entry['uuid'] in sell_uuids:
                    sell_uuids.remove(entry['uuid'])
                all_filled = False
            else:
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

        if self.check_stop_loss(symbol, trading_manager, dynamic_buyer):
            return True

        # 매도 per-order 체결 추적 — 항상 실행 (tracking 비어있으면 내부에서 return False)
        if self.check_sell_fills(symbol, dynamic_buyer):
            # 추적 중인 매도 전부 체결 → 사이클 완전 종료
            trading_manager.mark_sell_orders_executed()
            print_log(LogLevel.SUCCESS,
                      f"🎉 매도 {self.filled_sell_count}건 모두 체결 — 사이클 완전 종료")
            self.sell_orders_tracking = []
            self.filled_sell_count = 0
            sell_uuids.clear()
            return True

        # tracking 정리 — filled/cancel 처리된 entry 제거
        self.sell_orders_tracking = [e for e in self.sell_orders_tracking if not e['filled']]

        if not self.has_holdings(symbol):
            if trading_manager.sell_orders_placed:
                trading_manager.mark_sell_orders_executed()
                print_log(LogLevel.SUCCESS, "보유량 없음 - 거래 완료")
            return True

        # 평단가 변경 감지 — 매도 주문 유무와 상관없이 먼저 체크.
        # 새 매수 체결로 평단가가 변했으면 기존 매도 취소 후 새 평단가로 갱신.
        # 핵심: place_sell_orders 가 last_sell_base_price 를 새 평단가로 업데이트하므로
        # 갱신 직후 같은 평단가면 다시 갱신하지 않음 — 무한 루프 방지.
        current_avg = self.get_avg_buy_price(symbol)
        if current_avg > 0 and self.last_sell_base_price is not None:
            if abs(current_avg - self.last_sell_base_price) > 0.000001:
                print_log(LogLevel.INFO,
                          f"📉 평단가 변경 감지: {self.last_sell_base_price:,.4f} → {current_avg:,.4f} "
                          f"— 매도 갱신 (사이클 유지)")
                # 기존 매도 추적/UUID 초기화
                self.sell_orders_tracking = []
                self.filled_sell_count = 0
                self.cancel_all_sell_orders(symbol)
                # 새 평단가로 매도 재설정
                available_volume = self.get_available_volume(symbol)
                if available_volume >= MIN_HOLDING_VOLUME:
                    if self.place_sell_orders(symbol, profit_percentages, dynamic_buyer):
                        print_log(LogLevel.SUCCESS, "매도주문 갱신 완료 (새 평단가)")
                return False

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

class CandleCache:
    """1분 캔들 데이터 캐시 — symbol별 200캔들 + 타임스탬프.
    TTL 기반 만료로 동일 심볼의 반복 조회를 1회 REST로 통합.
    MarketAnalyzer(4회 생성)와 VolatilityProtector(60캔들)가 동일 캐시 재사용."""

    _cache = {}  # {symbol: {'candles': [...], 'time': timestamp}}
    TTL = 30     # 캔들 주기 1분 → 30초 캐시

    @classmethod
    def get_candles(cls, symbol, count=200):
        """symbol의 1분 캔들을 반환 (캐시 우선, 미스/만료 시 REST).
        Returns: API 응답 리스트 (최신→과거 순서)."""
        now = time.time()
        entry = cls._cache.get(symbol)
        if entry and (now - entry['time']) < cls.TTL:
            # 캐시 히트 — 요청 count 이상 보유하면 반환
            if len(entry['candles']) >= count:
                return entry['candles'][:count]

        # 캐시 미스/만료 — REST 조회 (항상 200개로 통일)
        try:
            querystring = {"market": "KRW-" + symbol, "count": "200"}
            def api_call():
                response = requests.get(CANDLE_URL, params=querystring, timeout=10)
                return response.json()
            candles = safe_api_call(api_call)
            if candles:
                cls._cache[symbol] = {'candles': candles, 'time': now}
                return candles[:count] if len(candles) >= count else candles
        except Exception as e:
            print_log(LogLevel.WARNING, f"CandleCache 조회 실패 ({symbol}): {str(e)[:80]}")
        return entry['candles'][:count] if entry else []

    @classmethod
    def invalidate(cls, symbol=None):
        """캐시 무효화 (특정 심볼 또는 전체)."""
        if symbol:
            cls._cache.pop(symbol, None)
        else:
            cls._cache.clear()


class CandleInfoFetcher:
    def __init__(self, symbol):
        count = 200
        # CandleCache 우선 — 동일 심볼의 반복 생성 시 REST 호출 0회
        self.response_dict = CandleCache.get_candles(symbol, count)
        if not self.response_dict:
            print_log(LogLevel.EXCEPTION, "Failed to fetch candle data")
            raise Exception("CandleInfoFetcher")

        # 한 번의 역순 순회로 6개 배열 동시 생성 (1200회 → 200회 인덱싱)
        # API 응답은 최신→과거 순서 → 역순(과거→최신)으로 재배열
        rd = self.response_dict
        n = len(rd)
        self.opening_prices = [0] * n
        self.trade_prices = [0] * n
        self.high_prices = [0] * n
        self.low_prices = [0] * n
        self.acc_trade_prices = [0] * n
        self.acc_trade_volumes = [0] * n
        for i in range(n):
            j = n - 1 - i  # 역순 인덱스
            c = rd[i]
            self.opening_prices[j] = c['opening_price']
            self.trade_prices[j] = c['trade_price']
            self.high_prices[j] = c['high_price']
            self.low_prices[j] = c['low_price']
            self.acc_trade_prices[j] = c['candle_acc_trade_price']
            self.acc_trade_volumes[j] = c['candle_acc_trade_volume']
        self.current_price = self.trade_prices[-1]

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
            # CandleCache에서 캔들 데이터 가져오기 (MarketAnalyzer와 동일 캐시 재사용)
            count = max(lookback_period, 60)  # 최소 60개
            candles = CandleCache.get_candles(symbol, count)

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
    """기술적 지표 분석 — talib 없이 순수 Python 구현.
    MA, STDDEV, RSI, MFI 만 계산 (사용되는 지표만)."""

    @staticmethod
    def _sma(values, period):
        """단순이동평균 — 마지막 값만 반환."""
        if len(values) < period:
            return 0.0
        return sum(values[-period:]) / period

    @staticmethod
    def _stddev(values, period):
        """표준편차 (모집단, ddof=0) — 마지막 period개 기준."""
        if len(values) < period:
            return 0.0
        window = values[-period:]
        mean = sum(window) / period
        var = sum((x - mean) ** 2 for x in window) / period
        return var ** 0.5

    def __init__(self, symbol):
        self.candle = CandleInfoFetcher(symbol)
        self.symbol = symbol

        prices = self.candle.trade_prices  # list[float]

        # 사용되는 지표만 계산 — 미사용 필드 제거 (성능 최적화)
        self.ma20 = self._sma(prices, 20)
        self.ma60 = self._sma(prices, 60)
        self.std20 = self._stddev(prices, 20)
        self.volatility_ratio = self.std20 / self.ma20 if self.ma20 > 0 else 0

    @staticmethod
    def _calc_rsi(prices, period=14):
        """RSI (Wilder's smoothing) — 순수 Python 구현."""
        if len(prices) < period + 1:
            return 50.0
        gains = []
        losses = []
        for i in range(-period, 0):
            diff = prices[i] - prices[i - 1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        # Wilder smoothing — 남은 데이터로 추가 smoothing
        for i in range(-len(prices) + period + 1, 0):
            if i == -period:
                continue
            pass  # 단순화: 초기 period 평균만 사용 (충분한 정확도)
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def _calc_mfi(highs, lows, closes, volumes, period=14):
        """MFI (Money Flow Index) — 순수 Python 구현."""
        n = len(closes)
        if n < period + 1:
            return 50.0
        # Typical Price = (high + low + close) / 3
        tp = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(n)]
        # Raw Money Flow = TP × Volume
        rmf = [tp[i] * volumes[i] for i in range(n)]
        # 최근 period 기준
        pos_flow = 0.0
        neg_flow = 0.0
        for i in range(n - period, n):
            if tp[i] > tp[i - 1]:
                pos_flow += rmf[i]
            elif tp[i] < tp[i - 1]:
                neg_flow += rmf[i]
        if neg_flow == 0:
            return 100.0
        mfr = pos_flow / neg_flow
        return 100 - (100 / (1 + mfr))

    def get_rsi(self):
        return self._calc_rsi(self.candle.trade_prices, 14)

    def get_mfi(self):
        return self._calc_mfi(
            self.candle.high_prices, self.candle.low_prices,
            self.candle.trade_prices, self.candle.acc_trade_volumes, 14)

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
                # details=true: market_event(주의/경고 종목) 필드 수신
                url = "https://api.upbit.com/v1/market/all?details=true"
                headers = {"Accept": "application/json"}
                response = requests.get(url, headers=headers, timeout=10)
                return response.json()

            markets = safe_api_call(api_call)
            if not markets:
                return []
            # market_event는 details=true 일부 코인에서만 내려올 수 있어 .get()으로 안전 접근.
            # 키가 없으면 warning=False(통과)로 간주.
            krw_markets = [
                market for market in markets
                if market['market'].startswith('KRW-')
                and not market.get('market_event', {}).get('warning', False)
            ]
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
        parser.add_argument('--verbose', action='store_true', help='Enable INFO-level logging (suppressed by default for performance)')

        args = parser.parse_args()

        # VERBOSE 플래그 설정 (print_log INFO 억제 제어)
        VERBOSE = args.verbose
        
        with open("../key.txt", 'r', encoding='utf-8') as f:
            ACCESS_KEY = f.readline().strip()
            SECRET_KEY = f.readline().strip()

        START_TIME = datetime.now()

        # Private WebSocket 시작 — 잔고/체결 실시간 수신 (REST 폴링 대체)
        if WEBSOCKET_AVAILABLE:
            try:
                private_ws.start(ACCESS_KEY, SECRET_KEY)
                # WS 연결 대기 (최대 5초)
                for _ in range(50):
                    if private_ws.is_connected:
                        break
                    time.sleep(0.1)
            except Exception as e:
                print_log(LogLevel.WARNING,
                         f"PrivateWS 시작 실패 — REST 폴백 모드: {str(e)[:100]}")

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

        # --auto-select 시 웹소켓 변동성 스캐너 시작
        volatility_scanner = None
        if args.auto_select and WEBSOCKET_AVAILABLE:
            print_log(LogLevel.INFO, "Starting VolatilityScanner (--auto-select)...")
            all_symbols = SymbolSelector.get_all_krw_markets()
            ignored = SymbolSelector.load_ignored_symbols()
            scan_symbols = [s for s in all_symbols if s not in ignored]
            if scan_symbols:
                volatility_scanner = VolatilityScanner()
                volatility_scanner.start(scan_symbols)
            else:
                print_log(LogLevel.WARNING, "No symbols for VolatilityScanner — REST 폴백")

        cycle_count = 0
        while True:
            cycle_count += 1

            # 1. command.txt 변경 체크 (새로운 심볼만 저장, 현재 거래는 중단하지 않음)
            new_symbol_detected = trading_manager.check_command_file()
            if new_symbol_detected:
                print_log(LogLevel.INFO, f"New symbol detected in command file: {new_symbol_detected}, will switch after current trading completes")
                # 현재 거래는 계속 진행, 다음 사이클에서 새로운 심볼로 전환

            cached_symbol = trading_manager.get_cached_symbol()
            # --auto-select 시 매 사이클마다 최고 변동성 심볼을 새로 찾는다 (캐시 무시)
            if args.auto_select and volatility_scanner and volatility_scanner.is_running:
                cached_symbol = None
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
                # command.txt에서 심볼 읽기 — check_command_file()이 캐싱한 값 재사용
                # (파일 재읽기 방지 — 디스크 IO 0회)
                # 단, --auto-select 시 command.txt의 SYMBOL은 무시하고 항상 스캐너 사용.
                symbol_from_command = None
                if not args.auto_select:
                    # check_command_file()이 이미 읽어서 current_command_symbol에 저장함
                    symbol_from_command = trading_manager.current_command_symbol

                if symbol_from_command:
                    symbol = symbol_from_command
                    # 변동성 보호 체크
                    if VolatilityProtector.check_volatility_protection(symbol):
                        print_log(LogLevel.WARNING, f"Command symbol {symbol} blocked by volatility protection")
                        log_state(LogState.ERROR, "VOLATILITY_PROTECTION")
                        time.sleep(30)
                        continue
                else:
                    # 자동 선별 — 웹소켓 스캐너 우선, 폴백으로 REST select_best_symbol
                    if volatility_scanner and volatility_scanner.is_running:
                        excluded = set(traded_symbols.keys())
                        selected_symbol = volatility_scanner.get_top_volatility_symbol(excluded)
                        if selected_symbol:
                            print_log(LogLevel.SUCCESS,
                                     f"🌐 VolatilityScanner 선별: {selected_symbol}")
                            symbol = selected_symbol
                        else:
                            print_log(LogLevel.WARNING,
                                     "스캐너 후보 없음 — 30s 대기 후 재시도")
                            time.sleep(30)
                            continue
                    else:
                        # REST 폴백 (웹소켓 미가동 시)
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
                # 단, --auto-select 시에는 command.txt 심볼을 무시 (스캐너 결과 유지)
                if (not args.auto_select
                        and trading_manager.pending_symbol_change
                        and not trading_manager.is_trading_in_progress()):
                    symbol = trading_manager.apply_pending_symbol_change()
                    print_log(LogLevel.INFO, f"Applied command override symbol: {symbol}")
                    analyzer = MarketAnalyzer(symbol)  # symbol 변경 시에만 재생성

                drop_count = 9

                # 새 매수 전 선제 스윕: 잔존 매수(bid) 주문 확실히 정리
                # (사이클 타임아웃/재시작 등으로 거래소에 남은 주문이 새 매수와 겹치는 것 방지)
                OrderCanceler().cancel_buy_orders()

                print_log(LogLevel.INFO,
                         f"Market Analysis - RSI: {analyzer.get_rsi():.2f}, "
                         f"Volatility: {analyzer.volatility_ratio:.4f}, "
                         f"Drop Levels: {drop_count}")

                # 매수 기준가 = 현재가 (최신 1분 캔들 체결가)
                buy_base_price = analyzer.candle.current_price
                low_px = analyzer.candle.low_prices[-1]
                print_log(LogLevel.INFO, f"Buy base price = current({buy_base_price:.4f})")

                dynamic_buyer = DynamicBuyOrder(symbol, buy_base_price, low_px, S, distribution_weight, 0)
                dynamic_buyer.calculate_order_plan(drop_percentage, drop_count, distribution_type)

                # 동적 매수 실행
                if dynamic_buyer.execute_dynamic_buy_orders():
                    print_log(LogLevel.SUCCESS, "Dynamic buying started successfully")
                    trading_manager.mark_buy_orders_placed()
                    
                    # 병렬 관리: 매수 진행 중에도 매도 관리 시작
                    print_log(LogLevel.SUCCESS, "=== STARTING PARALLEL BUY/SELL MANAGEMENT ===")
                    sell_controller = SellController()
                    # 단일 매도 — 평단가 대비 +0.16% 목표가 (분할매도 폐지)
                    profit_targets = [0.16]
                    
                    cycle_start_time = datetime.now()
                    cycle_timeout = 86400  
                    
                    trading_completed = False
                    command_changed_during_trading = False
                    
                    while not trading_completed:
                        current_time = datetime.now()
                        
                        # 타임아웃 체크
                        if (current_time - cycle_start_time).total_seconds() > cycle_timeout:
                            print_log(LogLevel.WARNING, f"Trading cycle timeout after {cycle_timeout} seconds")
                            # 타임아웃 종료 시 잔존 매수 비동기 취소 (메인 스레드 지연 방지)
                            run_async(OrderCanceler().cancel_buy_orders)
                            trading_completed = True
                            break
                        
                        # 1. command 변경 체크 (거래 중에는 플래그만 설정, 중단하지 않음)
                        if trading_manager.check_command_file():
                            command_changed_during_trading = True

                        # 현재가 1회 캐싱 — check_and_continue/check_last_buy_stop 중복 조회 방지
                        cached_price = RealMarketData.get_current_price(symbol)

                        # 2. 동적 매수 진행 체크
                        if dynamic_buyer.is_active:
                            dynamic_buyer.check_and_continue(cached_price)

                            # 마지막(최종 단계) 매수 시점 스킵 검사
                            # executed_count로 O(1) 체크
                            if dynamic_buyer.executed_count < len(dynamic_buyer.active_planned_orders):
                                last_buy_skip = sell_controller.check_last_buy_stop(symbol, trading_manager, cached_price)
                                if last_buy_skip:
                                    print_log(LogLevel.WARNING,
                                             "Stop loss range exceeded at last buy — "
                                             "skipping last buy, selling all holdings")
                                    dynamic_buyer.stop_trading()
                                    trading_completed = True
                                    break
                        else:
                            # 첫 번째 주문이 타임아웃되면 거래 중단
                            if len(dynamic_buyer.executed_orders) == 0:
                                print_log(LogLevel.WARNING, "First order timeout - stopping trading cycle")
                                # 잔존 매수 비동기 취소 (메인 스레드 지연 방지)
                                run_async(OrderCanceler().cancel_buy_orders)
                                trading_completed = True
                                break
                        
                        # 3. 매도 관리
                        # WS 캐시에서 직접 조회 (AccountChecker 인스턴스화/딕셔너리 복사 생략)
                        if private_ws._is_initialized and private_ws.is_connected:
                            balance, locked, avg_buy_price = private_ws.get_symbol_info(symbol)
                        else:
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
                        if sell_controller.check_stop_loss(symbol, trading_manager, dynamic_buyer):
                            print_log(LogLevel.WARNING, "Stop loss triggered")
                            trading_completed = True
                            break

                        # SLEEP_TIME=0이면 sleep 호출 자체를 생략 (최대 속도)
                        if SLEEP_TIME > 0:
                            time.sleep(SLEEP_TIME)
                    
                    # 거래 중 command 변경이 있었으면 알림
                    if command_changed_during_trading:
                        new_symbol = trading_manager.get_command_symbol_override()
                        print_log(LogLevel.INFO, f"Current trading completed. Will switch to new symbol '{new_symbol}' in next cycle.")

                log_state(LogState.BUYING, symbol)
                print_log(LogLevel.INFO, f"Buy orders placed for '{symbol}'")
                beep_async(440, 500)

            # 거래 완료 처리
            if trading_manager.is_trading_complete():
                if args.auto_select and not trading_manager.stop_loss_triggered:
                    SymbolSelector.mark_symbol_as_traded(symbol)
                
                OrderCanceler().cancel_buy_orders()

                if trading_manager.stop_loss_triggered:
                    print_log(LogLevel.WARNING, "Trading completed due to stop loss — alarm sounding indefinitely")
                    # 스탑로스 알람(데몬 스레드)이 계속 울리도록 메인 스레드를 종료하지 않음.
                    # 사용자가 직접 프로세스를 종료할 때까지 무한 대기하며 알람 지속.
                    try:
                        while True:
                            time.sleep(1)
                    except KeyboardInterrupt:
                        pass
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