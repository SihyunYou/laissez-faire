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
    @staticmethod
    def round_down(price, proportion):
        t = price - (price / 100) * proportion
    
        if t >= 2000000:
            t = math.floor(t / 1000) * 1000
        elif t >= 1000000:
            t = math.floor(t / 1000) * 1000
        elif t >= 500000:
            t = math.floor(t / 500) * 500
        elif t >= 100000:
            t = math.floor(t / 100) * 100
        elif t >= 50000:
            t = math.floor(t / 50) * 50
        elif t >= 10000:
            t = math.floor(t / 10) * 10
        elif t >= 5000:
            t = math.floor(t / 5) * 5
        elif t >= 1000:
            t = math.floor(t / 1) * 1
        elif t >= 100:
            t = math.floor(t / 1) * 1
        elif t >= 10:
            t = math.floor(t / 0.1) * 0.1
        elif t >= 1:
            t = math.floor(t / 0.01) * 0.01
        elif t >= 0.1:
            t = math.floor(t / 0.001) * 0.001
        elif t >= 0.01:
            t = math.floor(t / 0.0001) * 0.0001
        elif t >= 0.0001:
            t = math.floor(t / 0.000001) * 0.000001
        elif t >= 0.00001:
            t = math.floor(t / 0.0000001) * 0.0000001
        else:
            t = math.floor(t / 0.00000001) * 0.00000001
    
        return t

    @staticmethod
    def round_up(price):
        t = price
    
        if t >= 2000000:
            t = math.ceil(t / 1000) * 1000
        elif t >= 1000000:
            t = math.ceil(t / 1000) * 1000
        elif t >= 500000:
            t = math.ceil(t / 500) * 500
        elif t >= 100000:
            t = math.ceil(t / 100) * 100
        elif t >= 50000:
            t = math.ceil(t / 50) * 50
        elif t >= 10000:
            t = math.ceil(t / 10) * 10
        elif t >= 5000:
            t = math.ceil(t / 5) * 5
        elif t >= 1000:
            t = math.ceil(t / 1) * 1
        elif t >= 100:
            t = math.ceil(t / 1) * 1
        elif t >= 10:
            t = math.ceil(t / 0.1) * 0.1
        elif t >= 1:
            t = math.ceil(t / 0.01) * 0.01
        elif t >= 0.1:
            t = math.ceil(t / 0.001) * 0.001
        elif t >= 0.01:
            t = math.ceil(t / 0.0001) * 0.0001
        elif t >= 0.0001:
            t = math.ceil(t / 0.000001) * 0.000001
        elif t >= 0.00001:
            t = math.ceil(t / 0.0000001) * 0.0000001
        else:
            t = math.ceil(t / 0.00000001) * 0.00000001
    
        return t
    
    @staticmethod
    def calculate_sell_price(avg_buy_price, profit_percentage):
        required_price = avg_buy_price * (1 + profit_percentage / 100) / COMMISSION
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
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            rate_limiter.acquire()
            result = func(*args, **kwargs)
            return result
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = SLEEP_TIME * (2 ** attempt)
                print_log(LogLevel.WARNING, f"API call failed (attempt {attempt + 1}), retrying in {wait_time:.2f}s: {str(e)}")
                time.sleep(wait_time)
            else:
                print_log(LogLevel.ERROR, f"API call failed after {max_retries} attempts: {str(e)}")
                raise e

class RealMarketData:
    @staticmethod
    def get_current_price(symbol):
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

        def api_call():
            response = requests.delete(SERVER_URL + "/v1/order", params=params, headers=headers)
            return response
        
        try:
            response = safe_api_call(api_call)
            if response.status_code == 200:
                print_log(LogLevel.INFO, f"Successfully cancelled order: {order_uuid}")
                return True
            else:
                print_log(LogLevel.WARNING, f"Failed to cancel order {order_uuid}: {response.status_code}")
                return False
        except Exception as e:
            print_log(LogLevel.ERROR, f"Error cancelling order {order_uuid}: {str(e)}")
            return False

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
    """동적 분할 매수 주문 관리 클래스 - 체결된 주문 취소 문제 해결"""
    
    def __init__(self, symbol, current_price, total_amount, weight, exclude_count=0):
        self.symbol = symbol
        self.original_price = current_price
        self.current_price = current_price
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

    class DistributionType(Enum):
        LINEAR = 1
        LOG_LINEAR_II = 2
        LOG_LINEAR_I = 3
        PARABOLIC_II = 4
        PARABOLIC_I = 5
        EXPONENTIAL = 6
        FIBONACCI = 7

    def calculate_order_plan(self, drop_percentage, drop_count, distribution_type, confidence=1.0):
        """주문 계획 계산"""
        print_log(LogLevel.INFO, f"Starting order plan calculation for {self.symbol}")
        
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
        
        self.original_planned_orders = [order.copy() for order in self.active_planned_orders]
        print_log(LogLevel.SUCCESS, f"Calculated {len(self.active_planned_orders)} buy orders for {self.symbol}")

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

    def _calculate_required_shift(self, current_price):
        """필요한 밀림량 계산"""
        required_shift = 0.0
        
        for order in self.active_planned_orders:
            if not order['executed'] and order['planned_price'] > current_price:
                gap = order['planned_price'] - current_price
                if gap > required_shift:
                    required_shift = gap
        
        # 최소 밀림량 체크 (1000원 이상)
        if required_shift < 1000:
            return 0.0
            
        return required_shift

    def _apply_plan_shift(self, shift_amount):
        """계획 밀림 적용"""
        print_log(LogLevel.INFO, f"🔄 Applying plan shift: {shift_amount:,.0f} KRW")
        
        # 모든 미체결 주문에 밀림 적용
        for order in self.active_planned_orders:
            if not order['executed']:
                new_original_price = order['original_planned_price'] - shift_amount
                new_planned_price = UpbitTickSystem.round_down(UpbitTickSystem.round_up(new_original_price), 0)
                
                order['planned_price'] = new_planned_price
                order['shift_applied'] = shift_amount
                order['volume'] = order['quantity'] / order['planned_price']
        
        self.plan_shift_amount = shift_amount
        print_log(LogLevel.SUCCESS, f"✅ Plan shifted by {shift_amount:,.0f} KRW")

    def execute_dynamic_buy_orders(self):
        """동적 매수 시작"""
        if not self.active_planned_orders:
            print_log(LogLevel.ERROR, "No planned orders to execute")
            return False
            
        self.is_active = True
        self.pending_orders.clear()
        self.plan_shift_amount = 0.0
        self.first_order_start_time = datetime.now()
        
        print_log(LogLevel.INFO, f"Starting dynamic buying with {len(self.active_planned_orders)} planned orders")
        
        # 바로 첫 주문 실행
        return self._execute_next_available_order()

    def check_and_continue(self):
        """체결 확인 및 다음 주문 실행"""
        if not self.is_active:
            return False
            
        current_time = datetime.now()
        
        # 체크 간격 제한
        if self.last_check_time and (current_time - self.last_check_time).total_seconds() < SLEEP_TIME:
            return False
        self.last_check_time = current_time
        
        # 현재가 확인
        current_price = RealMarketData.get_current_price(self.symbol)
        if not current_price:
            return False
            
        self.current_price = current_price
        
        # 계획 밀림 확인
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
        """계획 밀림 확인 및 적용 - 체결된 주문 취소 문제 해결"""
        required_shift = self._calculate_required_shift(current_price)
        
        if required_shift > 0:
            # ✅ 체결되지 않은 대기 주문만 취소
            orders_to_cancel = []
            for pending_order in self.pending_orders:
                order_uuid = pending_order.get('uuid')
                if order_uuid and not self._is_order_executed(pending_order):
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
        """단일 주문 실행"""
        current_price = RealMarketData.get_current_price(self.symbol)
        if not current_price:
            return False

        order_price = order['planned_price']
        order_volume = order['volume']
        
        print_log(LogLevel.INFO, 
                 f"🎯 Executing order {order['level']} - "
                 f"Price: {order_price:,.0f} KRW, "
                 f"Volume: {order_volume:.6f}")
        
        order_uuid = self.place_dynamic_buy_order(order_price, order_volume)
        if order_uuid:
            pending_order = {
                'level': order['level'],
                'planned_price': order['planned_price'],
                'actual_price': order_price,
                'volume': order_volume,
                'order_time': datetime.now(),
                'uuid': order_uuid
            }
            
            self.pending_orders.append(pending_order)
            print_log(LogLevel.SUCCESS, f"✅ Order {order['level']} placed")
            return True
        else:
            print_log(LogLevel.ERROR, f"❌ Failed to place order {order['level']}")
            return False

    def _check_order_execution(self):
        """주문 체결 확인"""
        if not self.pending_orders:
            return False
        
        executed_any = False
        
        for i in range(len(self.pending_orders) - 1, -1, -1):
            pending_order = self.pending_orders[i]
            
            if self._is_order_executed(pending_order):
                self._process_executed_order(pending_order, i)
                executed_any = True
        
        return executed_any

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

    def _process_executed_order(self, pending_order, pending_index):
        """체결된 주문 처리"""
        order_uuid = pending_order.get('uuid')
        
        try:
            order_info = self._get_order_info(order_uuid)
            if order_info:
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
                         f"Price: {avg_executed_price:,.0f} KRW")
                        
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
                print_log(LogLevel.INFO, f"💰 Buy order placed at {price:,.0f} KRW")
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
        """단일 주문 취소"""
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
                response = requests.delete(SERVER_URL + "/v1/order", params=params, headers=headers)
                return response
            
            response = safe_api_call(api_call)
            if response.status_code == 200:
                print_log(LogLevel.INFO, f"Order {order_uuid[:8]}... cancelled")
                return True
            elif response.status_code == 404:
                print_log(LogLevel.WARNING, f"Order {order_uuid[:8]}... already cancelled or executed")
                return True
            else:
                print_log(LogLevel.WARNING, f"Failed to cancel order {order_uuid[:8]}...: {response.status_code}")
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
        return {
            'symbol': self.symbol,
            'is_active': self.is_active,
            'current_price': self.current_price,
            'plan_shift_amount': self.plan_shift_amount,
            'total_planned': len(self.active_planned_orders),
            'executed_orders': len(self.executed_orders),
            'pending_orders': len(self.pending_orders)
        }

class SellOrder:
    def __init__(self, symbol, volume, price):
        global sell_uuids

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
            sell_uuids.append(response_dict['uuid'])
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

    def place_sell_orders(self, symbol, profit_percentages):
        """매도 주문 걸기"""
        try:
            avg_buy_price = self.get_avg_buy_price(symbol)
            total_volume = self.get_total_volume(symbol)
            
            if total_volume < MIN_HOLDING_VOLUME or avg_buy_price <= 0:
                print_log(LogLevel.WARNING, f"매도 불가 - 부족한 수량: {total_volume:.6f}")
                return False

            print_log(LogLevel.INFO, f"매도주문 - 평단: {avg_buy_price:,.0f}, 전체수량: {total_volume:.6f}")

            sell_volume_per_order = total_volume / len(profit_percentages)
            
            for i, profit_pct in enumerate(profit_percentages):
                sell_price = UpbitTickSystem.calculate_sell_price(avg_buy_price, profit_pct)
                
                print_log(LogLevel.INFO, 
                         f"매도 #{i+1} - 목표: {profit_pct}%, "
                         f"가격: {sell_price:,.0f} KRW, 수량: {sell_volume_per_order:.6f}")
                
                SellOrder(symbol, sell_volume_per_order, sell_price)

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

    def manage_sell_orders(self, symbol, profit_percentages, trading_manager, wait_count):
        """매도 주문 관리 - 스탑로스 기능 추가"""
        
        if self.check_stop_loss(symbol, trading_manager):
            return True

        if not self.has_holdings(symbol):
            if trading_manager.sell_orders_placed:
                global sell_uuids
                
                if len(sell_uuids) > 0:
                    sell_uuids.pop(0)

                trading_manager.mark_sell_orders_executed()
                print_log(LogLevel.SUCCESS, "보유량 없음 - 거래 완료")
            return True

        if not self.has_pending_sell_orders(symbol):
            total_volume = self.get_total_volume(symbol)
            if total_volume >= MIN_HOLDING_VOLUME:
                print_log(LogLevel.INFO, f"매도주문 없음 - 새 매도주문 걸기 (전체수량: {total_volume:.6f})")
                if self.place_sell_orders(symbol, profit_percentages):
                    trading_manager.mark_sell_orders_placed()
            else:
                print_log(LogLevel.WARNING, f"매도주문 걸기 실패 - 부족한 수량: {total_volume:.6f}")
            return False

        available_volume = self.get_available_volume(symbol)
        total_volume = self.get_total_volume(symbol)
            
        if available_volume >= MIN_HOLDING_VOLUME:
            print_log(LogLevel.INFO, f"미체결 매도물량 발견 - 취소 후 재계산 (전체: {total_volume:.6f}, 미체결: {available_volume:.6f})")
            self.cancel_all_sell_orders(symbol)
            if self.place_sell_orders(symbol, profit_percentages):
                print_log(LogLevel.SUCCESS, "매도주문 전체 재계산 완료")
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
    def check_volatility_protection(symbol, lookback_period=60, threshold_percentage=60.0):
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

# --------------------------
# Refactored Confidence Calculator
# --------------------------
def clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))

def sigmoid(x: float, k: float = 1.0) -> float:
    try:
        return clamp(1.0 / (1.0 + math.exp(-k * x)))
    except OverflowError:
        return 0.0 if x < 0 else 1.0

def score_rsi(rsi: float, persist_above_80: int = 0, persist_thresh: int = 3) -> float:
    if rsi < 30: base = 0.05
    elif rsi < 45: base = 0.05 + 0.25 * ((rsi - 30)/15)
    elif rsi < 55: base = 0.30 + 0.25 * ((rsi - 45)/10)
    elif rsi < 65: base = 0.55 + 0.15 * ((rsi - 55)/10)
    elif rsi < 75: base = 0.70 + 0.10 * ((rsi - 65)/10)
    elif rsi < 80: base = 0.80
    else: base = 0.82

    fatigue = 0.0
    if rsi >= 80:
        frac = clamp((rsi - 80)/20)
        if persist_above_80 < persist_thresh:
            fatigue = 0.25 * (1 - persist_above_80/max(1,persist_thresh)) * frac
        else:
            fatigue = -0.04 * min(persist_above_80 - persist_thresh, 5) * frac
    return clamp(base - fatigue)

def score_mfi(mfi: float, persist_above_80: int = 0, persist_thresh: int = 3) -> float:
    if mfi < 30: base = 0.05
    elif mfi < 45: base = 0.20 + 0.15 * ((mfi - 30)/15)
    elif mfi < 55: base = 0.35 + 0.20 * ((mfi - 45)/10)
    elif mfi < 70: base = 0.55 + 0.15 * ((mfi - 55)/15)
    elif mfi < 80: base = 0.70 + 0.06 * ((mfi - 70)/10)
    else: base = 0.78

    fatigue = 0.0
    if mfi >= 80:
        frac = clamp((mfi - 80)/20)
        if persist_above_80 < persist_thresh:
            fatigue = 0.18 * (1 - persist_above_80/max(1,persist_thresh)) * frac
        else:
            fatigue = -0.03 * min(persist_above_80 - persist_thresh, 5) * frac
    return clamp(base - fatigue)

def score_macd(macd_hist: float, macd_scale: float = 0.1, k: float = 2.5) -> float:
    return sigmoid(macd_hist / (macd_scale or 0.1), k=k)

def score_williams(wr: float) -> float:
    r = clamp(wr, -100, 0)
    if r > -20: return 0.30
    elif r > -40: return clamp(0.75 + 0.2 * ((r + 40)/20))
    elif r > -70: return clamp(0.55 + 0.2 * ((r + 70)/30))
    else: return clamp(0.40 + 0.15 * ((r + 100)/30))

def score_momentum(momentum: float) -> float:
    if momentum >= 110: return 0.95
    elif momentum >= 105: return 0.85
    elif momentum >= 100: return 0.65
    elif momentum >= 95: return 0.45
    else: return 0.20

def score_vr(volume_ratio: float) -> float:
    if volume_ratio >= 2.0: return 1.0
    elif volume_ratio >= 1.5: return 0.9
    elif volume_ratio >= 1.2: return 0.7
    elif volume_ratio >= 0.9: return 0.5
    else: return 0.25

def score_ma_slope(ma_slope: float) -> float:
    if ma_slope <= 0: return clamp(0.30 + 0.20 * (ma_slope/5))
    elif ma_slope < 0.5: return clamp(0.50 + 0.30 * (ma_slope/0.5))
    elif ma_slope < 1.5: return clamp(0.80 + 0.15 * ((ma_slope-0.5)/1.0))
    else: return 1.0

def compute_composite_confidence(
    indicators: dict,
    weights: dict = None,
    persist_settings: dict = None,
    macd_scale: float = 0.1,
    atr_pct_floor: float = 0.04,
    atr_max_penalty: float = 0.20,
    market_bias: float = 0.0
) -> dict:

    default_weights = {
        'macd':0.3, 'rsi':0.18, 'mfi':0.15, 'williams':0.1,
        'momentum':0.1, 'vr':0.1, 'ma_slope':0.07
    }
    if weights is None:
        weights = default_weights.copy()
    else:
        for k,v in default_weights.items():
            weights.setdefault(k,v)

    # 지표 점수 계산
    rsi = indicators.get('rsi')
    mfi = indicators.get('mfi')
    macd_hist = indicators.get('macd_hist')
    if macd_hist is None and indicators.get('macd') is not None and indicators.get('macd_signal') is not None:
        macd_hist = indicators['macd'] - indicators['macd_signal']
        
    comps = {
        'rsi': score_rsi(rsi, persist_above_80=persist_settings.get('rsi_persist',0) if persist_settings else 0) if rsi is not None else None,
        'mfi': score_mfi(mfi, persist_above_80=persist_settings.get('mfi_persist',0) if persist_settings else 0) if mfi is not None else None,
        'macd': score_macd(macd_hist, macd_scale=macd_scale) if macd_hist is not None else None,
        'williams': score_williams(indicators['williams_r']) if indicators.get('williams_r') is not None else None,
        'momentum': score_momentum(indicators['momentum']) if indicators.get('momentum') is not None else None,
        'vr': score_vr(indicators['volume_ratio']) if indicators.get('volume_ratio') is not None else None,
        'ma_slope': score_ma_slope(indicators['ma_slope']) if indicators.get('ma_slope') is not None else None
    }

    # 가중치 정규화
    available = [(k,v,weights[k]) for k,v in comps.items() if v is not None]
    total_w = sum(w for _,_,w in available)
    if total_w==0: 
        return {'confidence': 0.5, 'components': comps}

    normalized = [(k,v,w/total_w) for k,v,w in available]
    raw_conf = sum(v*w for _,v,w in normalized)

    # 변동성 패널티
    atr_pct = indicators.get('atr_pct')
    vol_penalty = 0.0
    if atr_pct is not None and atr_pct > atr_pct_floor:
        vol_penalty = clamp((atr_pct - atr_pct_floor)/0.2,0,1)*atr_max_penalty

    # 시장 편향 조정
    market_adj = clamp(0.05*market_bias, -0.05, 0.05)
    final_conf = clamp(raw_conf - vol_penalty + market_adj)

    result = {
        'confidence': round(final_conf,4),
        'raw_confidence': round(raw_conf,4),
        'vol_penalty': round(vol_penalty,4),
        'market_adj': round(market_adj,4),
        'components': {k: (None if v is None else round(v,4)) for k,v in comps.items()},
        'used_weights': {k: round(w,4) for k,_,w in normalized}
    }
    return result

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

    def calculate_composite_confidence(self):
        """복합 confidence 계산"""
        rsi = self.get_rsi()
        mfi = self.get_mfi()
        macd_data = self.get_macd()
        williams_r = self.get_williams_r()
        momentum = self.get_momentum()
        
        # RSI 지속성 계산
        rsi_persist = 0
        rsi_values = talib.RSI(np.array(self.candle.trade_prices), timeperiod=14)
        for i in range(min(5, len(rsi_values))):
            if rsi_values[-(i+1)] >= 80:
                rsi_persist += 1
            else:
                break

        indicators = {
            'rsi': rsi,
            'mfi': mfi,
            'macd': macd_data['macd'],
            'macd_signal': macd_data['macd_signal'],
            'macd_hist': macd_data['macd_hist'],
            'williams_r': williams_r,
            'momentum': momentum,
            'volume_ratio': self.volume_ratio,
            'ma_slope': self.ma_slope,
            'atr_pct': self.atr_pct
        }

        persist_settings = {
            'rsi_persist': rsi_persist,
            'mfi_persist': 0
        }

        result = compute_composite_confidence(indicators, persist_settings=persist_settings)
        return result['confidence']

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
        
        # confidence 순위 (높을수록 좋음 -> 낮은 순위가 좋음)
        confidence_rank = {}
        sorted_by_confidence = sorted(symbol_data_list, key=lambda x: x['confidence'], reverse=True)
        for rank, data in enumerate(sorted_by_confidence, 1):
            confidence_rank[data['symbol']] = rank
        
        # 거래량 순위 (높을수록 좋음 -> 낮은 순위가 좋음)
        volume_rank = {}
        sorted_by_volume = sorted(symbol_data_list, key=lambda x: x['trading_volume_3h'], reverse=True)
        for rank, data in enumerate(sorted_by_volume, 1):
            volume_rank[data['symbol']] = rank
        
        # 각 심볼별 총 순위 점수 계산 (낮을수록 좋음)
        rank_scores = {}
        for data in symbol_data_list:
            symbol = data['symbol']
            total_rank = volatility_rank[symbol] # + confidence_rank[symbol] + volume_rank[symbol]
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
                
            confidence = analyzer.calculate_composite_confidence()
                
            return {
                'symbol': symbol,
                'volatility': analyzer.volatility_ratio,
                'current_price': analyzer.candle.current_price,
                'ma60': analyzer.ma60,
                'rsi': analyzer.get_rsi(),
                'mfi': analyzer.get_mfi(),
                'confidence': confidence,
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
                     f"Conf: {symbol_data['confidence']:.2f}, "
                     f"Vol: {symbol_data['trading_volume_3h']:,.0f}M)")
        
        best_symbol = valid_symbols[0]['symbol']
        best_data = valid_symbols[0]
        print_log(LogLevel.SUCCESS, 
                 f"Selected symbol: {best_symbol} "
                 f"(Rank Score: {best_data['rank_score']}, "
                 f"Volatility: {best_data['volatility']:.4f}, "
                 f"Confidence: {best_data['confidence']:.2f}, "
                 f"3H Volume: {best_data['trading_volume_3h']:,.0f}M KRW)")
        
        return best_symbol, best_data['confidence']

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
        profit_percentage = args.profit_percentage if args.profit_percentage else 0.32

        InitialBalance = S = AccountChecker().get_krw_balance()
        print_log(LogLevel.INFO, f"Available KRW: {int(S):,}")
        log_balance(S)

        if args.starting_balance is not None:
            if args.starting_balance < 1000000:
                print_log(LogLevel.ERROR, "Minimum starting balance is 1,000,000 won")
                exit()
            else:
                S = int(args.starting_balance * COMMISSION)
        else:
            S = int(S * COMMISSION)

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
                confidence = analyzer.calculate_composite_confidence()
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
                    if args.auto_select:
                        selection_result = SymbolSelector.select_best_symbol()
                        if selection_result is None:
                            print_log(LogLevel.WARNING, "No valid symbol found, waiting 30 seconds before retry...")
                            time.sleep(30)
                            continue
                        symbol, confidence = selection_result
                    else:
                        symbol = "BTC"
                        if VolatilityProtector.check_volatility_protection(symbol):
                            print_log(LogLevel.WARNING, f"Default symbol {symbol} blocked by volatility protection - using alternative")
                            # 대안 심볼 찾기
                            alternative_symbols = ["ETH", "XRP", "ADA"]
                            symbol_found = False
                            for alt_symbol in alternative_symbols:
                                if not VolatilityProtector.check_volatility_protection(alt_symbol):
                                    symbol = alt_symbol
                                    symbol_found = True
                                    print_log(LogLevel.INFO, f"Using alternative symbol: {symbol}")
                                    break
                            
                            if not symbol_found:
                                print_log(LogLevel.WARNING, "All alternative symbols blocked by volatility protection - waiting...")
                                time.sleep(30)
                                continue

                analyzer = MarketAnalyzer(symbol)
                confidence = analyzer.calculate_composite_confidence()
                trading_manager.set_symbol(symbol)

            print_log(LogLevel.INFO, f"=== Trading Cycle {cycle_count} ===")
            print_log(LogLevel.INFO, f"Target Symbol: {symbol}, Confidence: {confidence:.2f}")

            # 매수 프로세스 시작 전 command 변경 체크
            if trading_manager.should_place_buy_orders():
                # command 오버라이드가 있으면 즉시 적용 (새로운 거래 시작 시에만)
                if trading_manager.pending_symbol_change and not trading_manager.is_trading_in_progress():
                    symbol = trading_manager.apply_pending_symbol_change()
                    print_log(LogLevel.INFO, f"Applied command override symbol: {symbol}")
                    analyzer = MarketAnalyzer(symbol)
                    confidence = analyzer.calculate_composite_confidence()
                    
                analyzer = MarketAnalyzer(symbol)
                base_drop_count = 18
                drop_count = max(base_drop_count, int(base_drop_count * (2.0 - confidence) * 0))

                print_log(LogLevel.INFO, 
                         f"Market Analysis - RSI: {analyzer.get_rsi():.2f}, "
                         f"Volatility: {analyzer.volatility_ratio:.4f}, Confidence: {confidence:.2f}, "
                         f"Drop Levels: {drop_count} (base: {base_drop_count})")

                distribution_type = DynamicBuyOrder.DistributionType.LOG_LINEAR_I if confidence > 0.8 else DynamicBuyOrder.DistributionType.LOG_LINEAR_II if confidence < 0.3 else DynamicBuyOrder.DistributionType.PARABOLIC_II
                dynamic_buyer = DynamicBuyOrder(symbol, analyzer.candle.current_price, S, distribution_weight, 0)
                dynamic_buyer.calculate_order_plan(drop_percentage, drop_count, distribution_type, confidence)

                # 동적 매수 실행
                if dynamic_buyer.execute_dynamic_buy_orders():
                    print_log(LogLevel.SUCCESS, "Dynamic buying started successfully")
                    trading_manager.mark_buy_orders_placed()
                    
                    # 병렬 관리: 매수 진행 중에도 매도 관리 시작
                    print_log(LogLevel.SUCCESS, "=== STARTING PARALLEL BUY/SELL MANAGEMENT ===")
                    sell_controller = SellController()
                    profit_targets = [profit_percentage]
                    
                    cycle_start_time = datetime.now()
                    cycle_timeout = 86400  
                    
                    trading_completed = False
                    command_changed_during_trading = False
                    
                    while not trading_completed:
                        current_time = datetime.now()
                        
                        # 타임아웃 체크
                        if (current_time - cycle_start_time).total_seconds() > cycle_timeout:
                            print_log(LogLevel.WARNING, f"Trading cycle timeout after {cycle_timeout} seconds")
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
                                symbol, profit_targets, trading_manager, 0
                            )
                            
                            if is_trading_complete:
                                print_log(LogLevel.SUCCESS, "Trading completed (sell orders executed)")
                                trading_completed = True
                                break
                        else:
                            # 보유량이 없으면 거래 완료
                            if trading_manager.buy_orders_executed:
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
                print_log(LogLevel.INFO, f"Buy orders placed for '{symbol}' with confidence {confidence:.2f}")
                threading.Thread(target=winsound.Beep, args=(440, 500)).start()

            # 거래 완료 처리
            if trading_manager.is_trading_complete():
                if args.auto_select and not trading_manager.stop_loss_triggered:
                    SymbolSelector.mark_symbol_as_traded(symbol)
                
                OrderCanceler().cancel_buy_orders()
                OrderCanceler().cancel_sell_orders()
                
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
            S = int(S * COMMISSION)
            
            trading_manager.reset()
            print_log(LogLevel.INFO, f"Cycle {cycle_count} completed. Waiting for next cycle...")
            
    except Exception as e:
        log_state(LogState.ERROR)
        print_log(LogLevel.ERROR, f"Unexpected error: {str(e)}")
        traceback.print_exc()
        time.sleep(60)