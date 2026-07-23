# -*- coding: utf-8 -*-
"""Trading engine — full logic (split facades re-export from here).

Heavy classes live here to avoid circular-import breakage while
package facades + ParallelRuntime provide modular parallel execution.
"""
import requests
import json
import math
import time
import os
import uuid
import hashlib
import bisect
import base64
import socket
from urllib.parse import urlencode, unquote, urlparse
import winsound
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import datetime
from datetime import datetime, timedelta
import traceback
from enum import Enum, IntEnum
import functools
import hmac
import random
import secrets
from collections import deque

from requests.adapters import HTTPAdapter
from colorama import init, Fore, Back, Style

init(autoreset=True)

try:
    from .paths import (
        BALANCE_TXT, STATE_TXT, COMMAND_TXT, KEY_TXT, KEY_BITHUMB_TXT, ensure_log_dir,
    )
except ImportError:
    from laissez_faire.paths import (
        BALANCE_TXT, STATE_TXT, COMMAND_TXT, KEY_TXT, KEY_BITHUMB_TXT, ensure_log_dir,
    )


# 빠른 JSON — orjson 있으면 C 파서 사용 (WS/REST 핫패스)
try:
    import orjson
    def json_loads(data):
        return orjson.loads(data)
    def json_dumps_bytes(obj):
        return orjson.dumps(obj)
except ImportError:
    def json_loads(data):
        if isinstance(data, (bytes, bytearray)):
            return json.loads(data)
        return json.loads(data)
    def json_dumps_bytes(obj):
        return json.dumps(obj, separators=(',', ':'), ensure_ascii=False).encode('utf-8')

# ===== 로우레벨 HTTP — 트리플 세션 / TCP_NODELAY / Keepalive / DNS캐시 / 짧은 타임아웃 =====
# (connect, read) — 일반 REST. seed/캔들은 HTTP_TIMEOUT_SLOW.
HTTP_TIMEOUT = (0.4, 1.0)       # 잔고/일반 조회
HTTP_TIMEOUT_SLOW = (2.0, 8.0)  # seed/캔들/마켓목록
ORDER_TIMEOUT = (0.5, 1.0)      # 주문/취소/체결조회 — 핫패스 (짧은 read 타임아웃 재시도 폭주 방지)
# 평단(/v1/accounts) 전용 — 매도 핫패스. slow/safe_api_call 절대 금지.
AVG_TIMEOUT = (0.18, 0.40)

def _build_socket_options(rcvbuf=262144, sndbuf=262144):
    """저지연 소켓 옵션 — Nagle off + TCP keepalive + 버퍼 확대.
    플랫폼에 없는 상수는 스킵 (Windows는 KEEPIDLE 등 미지원)."""
    opts = [
        (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        (socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf),
        (socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf),
    ]
    # Linux/macOS TCP keepalive 세부 (유휴 연결 조기 감지)
    for name, val in (('TCP_KEEPIDLE', 20), ('TCP_KEEPINTVL', 5), ('TCP_KEEPCNT', 3)):
        const = getattr(socket, name, None)
        if const is not None:
            opts.append((socket.IPPROTO_TCP, const, val))
    return opts

_HTTP_SOCKOPTS = _build_socket_options()
# 공개 스트림: 중형 버퍼 / Private(평단·체결): 큰 수신 버퍼로 패킷 대기 감소
_WS_SOCKOPTS = _build_socket_options(rcvbuf=512 * 1024, sndbuf=128 * 1024)
_WS_SOCKOPTS_PRIVATE = _build_socket_options(rcvbuf=1024 * 1024, sndbuf=256 * 1024)

class _FastHTTPAdapter(HTTPAdapter):
    """TCP_NODELAY + Keepalive + 대형 커넥션 풀 (TLS 핸드셰이크 재사용)."""
    def __init__(self, pool_connections=12, pool_maxsize=24, **kwargs):
        # 재시도는 safe_api_call에서 처리 — urllib3 Retry 오버헤드 제거
        kwargs.setdefault('max_retries', 0)
        super().__init__(pool_connections=pool_connections,
                         pool_maxsize=pool_maxsize, **kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs['socket_options'] = _HTTP_SOCKOPTS
        # 풀 고갈 시 대기(새 소켓 폭증 방지) — 주문 버스트에도 핸드셰이크 재사용
        return super().init_poolmanager(connections, maxsize, block=True, **pool_kwargs)

def _make_session(pool_connections, pool_maxsize, accept_encoding='gzip, deflate'):
    s = requests.Session()
    s.trust_env = False  # 매 요청 프록시 환경변수 조회 생략
    s.headers.update({
        "Accept": "application/json",
        "Connection": "keep-alive",
        "Accept-Encoding": accept_encoding,
        "User-Agent": "laissez-faire/2.0",
    })
    adapter = _FastHTTPAdapter(pool_connections=pool_connections,
                               pool_maxsize=pool_maxsize)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

# 핫 세션: 주문/취소/체결조회 — 풀 크게 (분할매수 버스트 대비)
http_session = _make_session(pool_connections=16, pool_maxsize=32)
# 슬로우 세션: 캔들/시드/스캐너 — 핫 풀 고갈 격리
http_session_slow = _make_session(pool_connections=8, pool_maxsize=16)
# 평단 전용 세션 — 주문/캔들과 커넥션·인코딩 격리.
# identity: 소형 accounts JSON은 gzip 해제 CPU가 RTT보다 비싼 경우가 많음.
http_session_avg = _make_session(
    pool_connections=4, pool_maxsize=8, accept_encoding='identity')

# 주문/취소/배치조회 병렬 풀
_ORDER_POOL = ThreadPoolExecutor(max_workers=12, thread_name_prefix="ord")
# 평단 REST 전용 풀 — 주문 버스트에 밀리지 않음
_AVG_POOL = ThreadPoolExecutor(max_workers=3, thread_name_prefix="avg")

# EXCHANGE 변경 시 __main__에서 갱신 — 핫패스 URL 조립 생략
ORDER_URL = ''
CANCEL_URL = ''
ORDER_QUERY_URL = ''
ORDERS_UUIDS_URL = ''
ORDERS_OPEN_URL = ''
CANCEL_AND_NEW_URL = ''
ACCOUNTS_URL = ''

def _refresh_hot_urls():
    """SERVER_URL + EXCHANGE 경로 — 주문 핫패스 문자열 연결 1회."""
    global ORDER_URL, CANCEL_URL, ORDER_QUERY_URL
    global ORDERS_UUIDS_URL, ORDERS_OPEN_URL, CANCEL_AND_NEW_URL, ACCOUNTS_URL
    ORDER_URL = SERVER_URL + EXCHANGE['order_endpoint']
    CANCEL_URL = SERVER_URL + EXCHANGE['cancel_endpoint']
    ORDER_QUERY_URL = SERVER_URL + EXCHANGE['order_query_endpoint']
    ORDERS_UUIDS_URL = SERVER_URL + (
        EXCHANGE.get('orders_uuids_endpoint') or '/v1/orders/uuids')
    ORDERS_OPEN_URL = SERVER_URL + (
        EXCHANGE.get('orders_open_cancel_endpoint') or '/v1/orders/open')
    ep = EXCHANGE.get('cancel_and_new_endpoint')
    CANCEL_AND_NEW_URL = (SERVER_URL + ep) if ep else ''
    ACCOUNTS_URL = SERVER_URL + '/v1/accounts'

# ===== DNS 캐시 — getaddrinfo 스파이크 제거 (TTL 갱신) =====
_DNS_CACHE = {}          # host → (addrs, expiry)
_DNS_CACHE_TTL = 300.0   # 5분
_DNS_LOCK = threading.Lock()

def cached_getaddrinfo(host, port=443, ttl=_DNS_CACHE_TTL):
    """호스트 DNS 결과 캐시. 만료 시 백그라운드 갱신 + 즉시 캐시 반환."""
    now = time.time()
    with _DNS_LOCK:
        entry = _DNS_CACHE.get(host)
        if entry and entry[1] > now:
            return entry[0]
    try:
        addrs = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError:
        with _DNS_LOCK:
            stale = _DNS_CACHE.get(host)
        return stale[0] if stale else []
    with _DNS_LOCK:
        _DNS_CACHE[host] = (addrs, now + ttl)
    return addrs

# WS run_forever 공통 — UTF-8 검증 스킵 + 저지연 sockopt
# 공개 스트림: ping 여유 / Private(체결 임계): 짧은 ping으로 데드소켓 조기 감지
_WS_PING_PUBLIC = (45, 10)
_WS_PING_PRIVATE = (15, 5)

def _ws_backoff_seconds(last_error, attempt):
    """WS 재연결 — 429만 최소 대기(최대 5s). 그 외 즉시 재시도. 주문 핫패스 무관."""
    err = (last_error or '').lower()
    if '429' in err or 'too many' in err:
        return min(0.5 * (2 ** min(attempt, 3)), 5.0)
    return 0.0


def _ws_run_forever(ws, ping_interval=None, ping_timeout=None, private=False):
    """WebSocketApp.run_forever 래퍼 — 핫패스 수신 오버헤드 최소화.

    - skip_utf8_validation: 수신 UTF-8 검증 스킵
    - TCP_NODELAY + 큰 RCVBUF (private는 1MB)
    - compression=None: permessage-deflate 비활성
      (작은 myAsset/myOrder 빈번 수신 시 압축해제 CPU가 평단 핫패스를 늦춤)
    """
    if ping_interval is None or ping_timeout is None:
        ping_interval, ping_timeout = (
            _WS_PING_PRIVATE if private else _WS_PING_PUBLIC
        )
    sockopt = _WS_SOCKOPTS_PRIVATE if private else _WS_SOCKOPTS
    # 인자 조합을 넓은→좁은 순으로 시도 (라이브러리 버전 편차)
    attempts = (
        dict(ping_interval=ping_interval, ping_timeout=ping_timeout,
             skip_utf8_validation=True, sockopt=sockopt, compression=None),
        dict(ping_interval=ping_interval, ping_timeout=ping_timeout,
             skip_utf8_validation=True, sockopt=sockopt),
        dict(ping_interval=ping_interval, ping_timeout=ping_timeout,
             skip_utf8_validation=True),
        dict(ping_interval=ping_interval, ping_timeout=ping_timeout),
    )
    last_err = None
    for kwargs in attempts:
        try:
            return ws.run_forever(**kwargs)
        except TypeError as e:
            last_err = e
            continue
    if last_err:
        raise last_err

def _ws_send_json(ws, obj):
    """WS 구독/송신 — orjson bytes 우선 (stdlib json.dumps 회피)."""
    payload = json_dumps_bytes(obj)
    try:
        ws.send(payload)
    except TypeError:
        # 일부 websocket-client는 str만 허용
        ws.send(payload.decode('utf-8') if isinstance(payload, (bytes, bytearray))
                else payload)

def http_get(url, timeout=HTTP_TIMEOUT, slow=False, **kwargs):
    sess = http_session_slow if slow else http_session
    return sess.get(url, timeout=timeout, **kwargs)

def http_post(url, timeout=HTTP_TIMEOUT, slow=False, **kwargs):
    sess = http_session_slow if slow else http_session
    return sess.post(url, timeout=timeout, **kwargs)

def http_delete(url, timeout=HTTP_TIMEOUT, slow=False, **kwargs):
    sess = http_session_slow if slow else http_session
    return sess.delete(url, timeout=timeout, **kwargs)

def http_get_hot(url, timeout=ORDER_TIMEOUT, **kwargs):
    """주문/취소/체결조회 — 핫 세션 강제 + ORDER_TIMEOUT."""
    return http_session.get(url, timeout=timeout, **kwargs)

def http_post_hot(url, timeout=ORDER_TIMEOUT, **kwargs):
    return http_session.post(url, timeout=timeout, **kwargs)

def http_delete_hot(url, timeout=ORDER_TIMEOUT, **kwargs):
    return http_session.delete(url, timeout=timeout, **kwargs)

def http_get_avg(url=None, timeout=AVG_TIMEOUT, **kwargs):
    """평단(/v1/accounts) 전용 GET — avg 세션 + 초단 타임아웃."""
    return http_session_avg.get(url or ACCOUNTS_URL, timeout=timeout, **kwargs)

def response_json(response):
    """response.json() 대신 content 직접 파싱 (orjson 경로)."""
    data = response.content
    if not data:
        return None
    return json_loads(data)

def response_uuid(response):
    """주문 POST 성공 경로 — uuid/order_id 추출."""
    data = response.content
    if not data:
        return None
    obj = json_loads(data)
    if not isinstance(obj, dict):
        return None
    return obj.get('uuid') or obj.get('order_id')

def response_order_or_error(response):
    """주문 POST — uuid/order_id 또는 error dict 반환 (업비트·빗썸 공통)."""
    data = response.content
    if not data:
        return None, None
    obj = json_loads(data)
    if not isinstance(obj, dict):
        return None, obj
    uid = obj.get('uuid') or obj.get('order_id')
    if uid:
        return uid, None
    return None, obj


class OrderRateLimiter:
    """업비트 order 그룹(주문생성·cancel_and_new) 초당 한도 내부 추적.
    공식 8/sec — 슬롯 없으면 짧게만 대기(1초 풀블로킹 금지).
    병렬 워커: worker_id별 공정 쿼터로 한 워커가 SAFE 전체를 독점하지 않게."""
    LIMIT = 8
    SAFE = 7  # 1 여유 — 매수 삼중 POST가 매도 슬롯에 전부 막히지 않게
    WINDOW = 1.0

    def __init__(self):
        self._lock = threading.Lock()
        self._hits = deque()  # monotonic timestamps
        self._header_remaining = None  # Remaining-Req sec (order group)
        self._header_block_until = 0.0  # sec=0 / 429 후 이 시각까지 대기
        self._usage = deque(maxlen=40)  # (mono_ts, reason) — sec=0 원인 추적
        # worker fair-share: worker_id → deque of hit timestamps
        self._worker_hits = {}  # type: dict[str, deque]
        self._active_workers = 1

    def set_active_workers(self, n: int):
        """병렬 코인 워커 수 — 워커당 SAFE/n 몫 (최소 1)."""
        with self._lock:
            self._active_workers = max(1, int(n or 1))

    def note_use(self, reason):
        """order 그룹 슬롯 사용 사유 기록."""
        with self._lock:
            self._usage.append((time.monotonic(), str(reason or '?')))

    def recent_usage(self, window=1.2):
        """최근 window초 order 슬롯 사용 내역."""
        with self._lock:
            now = time.monotonic()
            rows = [(now - t, r) for t, r in self._usage if now - t <= window]
        return rows

    def _log_exhaustion(self, where):
        rows = self.recent_usage(1.5)
        if not rows:
            return
        summary = ', '.join(f"{r}({d:.2f}s)" for d, r in rows)
        print_log(LogLevel.WARNING,
                  f"order-slot exhaust ({where}): {summary}")

    def _prune(self, now):
        w = self.WINDOW
        while self._hits and (now - self._hits[0]) >= w:
            self._hits.popleft()
        if self._header_block_until and now >= self._header_block_until:
            self._header_block_until = 0.0
            self._header_remaining = None
        dead = []
        for wid, dq in self._worker_hits.items():
            while dq and (now - dq[0]) >= w:
                dq.popleft()
            if not dq:
                dead.append(wid)
        for wid in dead:
            self._worker_hits.pop(wid, None)

    def _worker_quota(self):
        n = max(1, int(self._active_workers))
        # 분할매수(최대 3) 기준으로 워커당 최소 3슬롯 — 너무 쪼개면 대기만 늘어남
        return max(3, (self.SAFE + n - 1) // n)

    def acquire(self, timeout=0.35, cost=1, worker_id=None):
        """슬롯 확보. 성공 True, timeout 초과 False.
        worker_id 있으면 워커 쿼터 우선 적용 후 전역 SAFE 여유분 사용."""
        cost = max(1, int(cost))
        deadline = time.monotonic() + max(0.02, float(timeout))
        wid = str(worker_id) if worker_id else None
        while True:
            with self._lock:
                now = time.monotonic()
                self._prune(now)
                hdr_block = self._header_block_until > now
                global_ok = (not hdr_block) and (len(self._hits) + cost <= self.SAFE)
                worker_ok = True
                if wid and global_ok:
                    dq = self._worker_hits.setdefault(wid, deque())
                    quota = self._worker_quota()
                    # 쿼터 초과여도 전역에 여유(SAFE - used_by_others)≥cost 면 허용
                    mine = len(dq)
                    if mine + cost > quota:
                        others = len(self._hits) - mine
                        spare = self.SAFE - others
                        worker_ok = spare >= cost
                if global_ok and worker_ok:
                    for _ in range(cost):
                        self._hits.append(now)
                        if wid:
                            self._worker_hits.setdefault(wid, deque()).append(now)
                    if self._header_remaining is not None:
                        self._header_remaining = max(
                            0, self._header_remaining - cost)
                    return True
                if self._hits:
                    wait = self.WINDOW - (now - self._hits[0]) + 0.001
                else:
                    wait = 0.02
                if hdr_block:
                    wait = max(0.01, min(wait, self._header_block_until - now))
            remain = deadline - time.monotonic()
            if remain <= 0:
                with self._lock:
                    now = time.monotonic()
                    self._prune(now)
                    if len(self._hits) + cost <= self.LIMIT:
                        for _ in range(cost):
                            self._hits.append(now)
                            if wid:
                                self._worker_hits.setdefault(
                                    wid, deque()).append(now)
                        return True
                return False
            time.sleep(max(0.001, min(wait, 0.02, remain)))

    def note_response(self, response):
        """응답 Remaining-Req / 429로 카운터 동기화.
        sec=0이어도 hits를 LIMIT까지 가짜로 채우지 않음(1초마다 1건 병목 주범)."""
        if response is None:
            return
        try:
            if _response_rate_limited(response):
                self.note_limited()
                return
            hdr = None
            if hasattr(response, 'headers'):
                hdr = (response.headers.get('Remaining-Req')
                       or response.headers.get('remaining-req'))
            if not hdr:
                return
            group = None
            sec = None
            for part in str(hdr).split(';'):
                part = part.strip()
                if part.startswith('group='):
                    group = part.split('=', 1)[1].strip().lower()
                elif part.startswith('sec='):
                    try:
                        sec = int(float(part.split('=', 1)[1].strip()))
                    except (TypeError, ValueError):
                        sec = None
            if group != 'order' or sec is None:
                return
            with self._lock:
                now = time.monotonic()
                self._prune(now)
                self._header_remaining = sec
                if sec <= 0:
                    self._header_block_until = now + 0.12
                    need_log = True
                else:
                    self._header_block_until = 0.0
                    need_log = False
            if need_log:
                self._log_exhaustion('Remaining-Req sec=0')
        except Exception:
            pass

    def note_limited(self):
        """429 — 짧게만 차단 후 재시도 허용."""
        with self._lock:
            now = time.monotonic()
            self._prune(now)
            self._header_remaining = 0
            self._header_block_until = now + 0.15


order_rate_limiter = OrderRateLimiter()

# 병렬 워커 컨텍스트 — http_post_order가 acquire(worker_id=)에 전달
_worker_tls = threading.local()


def set_current_worker_id(worker_id):
    _worker_tls.worker_id = worker_id


def get_current_worker_id():
    return getattr(_worker_tls, 'worker_id', None)



class _FakeRateLimitResponse:
    """acquire 실패 시 호출측이 기존 429 경로로 처리하도록."""
    status_code = 429
    content = b'{"error":{"name":"too_many_requests","message":"rate_limit"}}'
    headers = {}


def http_post_order(url, query, headers, reason='order_post'):
    """주문 POST — 업비트는 query params, 빗썸 v2는 JSON body.
    order 그룹 rate limit 슬롯 확보 후 전송.
    병렬 워커는 슬롯 대기 최대 3초(짧으면 fake-429 폭주 → 체감 지연)."""
    order_rate_limiter.note_use(reason)
    wid = get_current_worker_id()
    wait = 3.0 if wid else 0.35
    if not order_rate_limiter.acquire(timeout=wait, worker_id=wid):
        order_rate_limiter._log_exhaustion(f'acquire-fail:{reason}')
        return _FakeRateLimitResponse()
    if EXCHANGE.get('order_post_json'):
        h = dict(headers) if headers else {}
        h['Content-Type'] = 'application/json; charset=utf-8'
        resp = http_post_hot(url, json=query, headers=h)
    else:
        resp = http_post_hot(url, params=query, headers=headers)
    order_rate_limiter.note_response(resp)
    return resp


def order_id_of(obj):
    """주문 ID — 업비트 uuid/uid / 빗썸 order_id (내부 키도 uuid로 통일)."""
    if not isinstance(obj, dict):
        return None
    return obj.get('uuid') or obj.get('uid') or obj.get('order_id')


def normalize_side(side):
    """buy/sell/BID/ASK -> bid/ask. 이미 bid/ask면 그대로."""
    if side is None:
        return side
    mapped = EXCHANGE.get('ws_side_map', {}).get(side)
    if mapped is not None:
        return mapped
    s = str(side).lower()
    if s in ('bid', 'buy'):
        return 'bid'
    if s in ('ask', 'sell'):
        return 'ask'
    return side


def order_executed_volume(obj):
    if not isinstance(obj, dict):
        return 0.0
    try:
        return float(obj.get('executed_volume')
                     or obj.get('executed_quantity')
                     or obj.get('ev') or 0)
    except (TypeError, ValueError):
        return 0.0


def order_remaining_volume(obj):
    if not isinstance(obj, dict):
        return 1.0
    try:
        v = obj.get('remaining_volume')
        if v is None:
            v = obj.get('remaining_quantity')
        if v is None:
            v = obj.get('rv')
        if v is None:
            return 1.0
        return float(v)
    except (TypeError, ValueError):
        return 1.0


def order_executed_funds(obj):
    if not isinstance(obj, dict):
        return 0.0
    try:
        return float(obj.get('executed_funds')
                     or obj.get('executed_amount')
                     or obj.get('ef') or 0)
    except (TypeError, ValueError):
        return 0.0


# 업비트 WS SIMPLE → DEFAULT 키 (myOrder). 이미 긴 키가 있으면 유지.
_SIMPLE_ORDER_KEYS = (
    ('uid', 'uuid'),
    ('cd', 'code'),
    ('ab', 'ask_bid'),
    ('s', 'state'),
    ('ot', 'ord_type'),
    ('p', 'price'),
    ('ap', 'avg_price'),
    ('v', 'volume'),
    ('rv', 'remaining_volume'),
    ('ev', 'executed_volume'),
    ('ef', 'executed_funds'),
    ('st', 'stream_type'),
)


def normalize_order(obj, prev=None):
    """거래소 응답 -> 내부 공통 스키마 (uuid/executed_volume/bid-ask).
    prev merge: 빗썸 done이 수량 없이 덮어쓰는 것 방지.
    업비트 SIMPLE(uid/cd/ab/s/ev…)도 DEFAULT 키로 승격."""
    if not isinstance(obj, dict):
        return obj
    out = dict(prev) if isinstance(prev, dict) else {}
    out.update(obj)
    for short, long in _SIMPLE_ORDER_KEYS:
        if out.get(long) is None and out.get(short) is not None:
            out[long] = out[short]
    uid = order_id_of(out)
    if uid:
        out['uuid'] = uid
    if out.get('executed_volume') is None and out.get('executed_quantity') is not None:
        out['executed_volume'] = out['executed_quantity']
    if out.get('remaining_volume') is None and out.get('remaining_quantity') is not None:
        out['remaining_volume'] = out['remaining_quantity']
    if out.get('executed_funds') is None and out.get('executed_amount') is not None:
        out['executed_funds'] = out['executed_amount']
    if isinstance(prev, dict):
        for k in ('executed_volume', 'remaining_volume', 'executed_funds',
                  'executed_quantity', 'remaining_quantity', 'executed_amount'):
            if out.get(k) is None and prev.get(k) is not None:
                out[k] = prev[k]
        if out.get('state') == 'done':
            if order_executed_volume(out) <= 0 and order_executed_volume(prev) > 0:
                out['executed_volume'] = prev.get(
                    'executed_volume', prev.get('executed_quantity'))
                out['executed_funds'] = prev.get(
                    'executed_funds', prev.get('executed_amount'))
            if out.get('remaining_volume') is None and out.get('remaining_quantity') is None:
                out['remaining_volume'] = 0
    side = out.get('side')
    if side is None:
        side = out.get('ask_bid') or out.get('ab')
    if side is not None:
        out['side'] = normalize_side(side)
    if not out.get('market') and out.get('code'):
        out['market'] = out['code']
    return out


def unwrap_orders_payload(data):
    """주문 목록 — list | {orders} | {data} (빗썸 v2 pending)."""
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    if 'error' in data:
        return data
    for key in ('orders', 'data', 'success'):
        v = data.get(key)
        if isinstance(v, list):
            return v
    return []


def order_is_filled(order_info):
    """체결 판정 — 업비트/빗썸 공통."""
    if not isinstance(order_info, dict):
        return False
    state = order_info.get('state')
    exe = order_executed_volume(order_info)
    rem = order_remaining_volume(order_info)
    if state == 'done' and exe > 0:
        return True
    if state == 'done' and rem <= 0:
        return True
    if exe > 0 and rem <= 0:
        return True
    return False


def _response_rate_limited(result):
    """429 / too_many_requests 감지 — Response·dict 양쪽."""
    if hasattr(result, 'status_code') and result.status_code == 429:
        return True
    if isinstance(result, dict):
        err = result.get('error')
        if isinstance(err, dict):
            err_name = str(err.get('name', '') or err.get('message', '')).lower()
            if 'too_many_requests' in err_name or 'rate' in err_name:
                return True
    return False

def warm_http_connections(hosts=None, auth=False):
    """DNS + TLS + (선택) 인증 REST 프리웜 — 첫 주문 RTT 스파이크 제거.
    EXCHANGE/SERVER_URL 설정 후 호출할 것."""
    if hosts is None:
        hosts = []
        try:
            for u in (SERVER_URL,
                      EXCHANGE.get('tick_candle_url') or '',
                      EXCHANGE.get('ws_public_url') or '',
                      EXCHANGE.get('ws_private_url') or ''):
                if not u:
                    continue
                host = urlparse(u).hostname
                if host and host not in hosts:
                    hosts.append(host)
        except Exception:
            hosts = ["api.upbit.com", "crix-api-cdn.upbit.com", "api.bithumb.com"]
        for h in ("api.upbit.com", "crix-api-cdn.upbit.com", "api.bithumb.com",
                  "ws-api.bithumb.com"):
            if h not in hosts:
                hosts.append(h)

    # DNS 병렬 프리리졸브
    def _resolve(h):
        cached_getaddrinfo(h, 443)
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(hosts))),
                            thread_name_prefix="dns") as pool:
        list(pool.map(_resolve, hosts))

    # TLS 핸드셰이크 프리웜 — 선택 거래소 + CRIX (실패 무시)
    warm_urls = []
    if SERVER_URL:
        warm_urls.append((SERVER_URL + "/v1/market/all",
                          {"isDetails": "false"}, False))
    crix = EXCHANGE.get('tick_candle_url') if isinstance(EXCHANGE, dict) else None
    if crix:
        # CRIX는 가벼운 HEAD 대용 GET — 실제 캔들 1개
        warm_urls.append((crix, {"code": "CRIX.UPBIT.KRW-BTC", "count": "1"}, True))

    def _warm_get(item):
        url, params, use_slow = item
        try:
            http_get(url, params=params, timeout=(1.0, 2.0), slow=use_slow)
        except Exception:
            pass
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="warm") as pool:
        list(pool.map(_warm_get, warm_urls))

    # 인증 경로 프리웜 — 평단 전용 세션 TLS+Keepalive를 accounts로 예열
    if auth and ACCESS_KEY and SECRET_KEY:
        try:
            headers = make_auth_headers()
            url = ACCOUNTS_URL or (SERVER_URL + "/v1/accounts")
            http_get_avg(url, headers=headers, timeout=(0.8, 1.5))
            # 주문 핫 세션도 동일 경로 1회 (풀 분리 유지)
            http_get_hot(url, headers=make_auth_headers(), timeout=(0.8, 1.5))
        except Exception:
            pass

# 웹소켓 라이브러리 (선택) — 미설치 시 REST 폴백
try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

# Configuration
UNIT = 1
ACCESS_KEY = ''
SECRET_KEY = ''

# ===== 거래소 설정 — 업비트/빗썸 양쪽을 동일 코드로 구동 =====
# 두 거래소 모두 JWT(HS256) + query_hash(SHA512) 인증, KRW-BTC 심볼 포맷,
# /v1 accounts·ticker·candle·market 경로가 동일. 차이점만 이 딕셔너리로 캡슐화.
EXCHANGE_CONFIGS = {
    'upbit': {
        'name': 'upbit',
        'server_url': 'https://api.upbit.com',
        'ws_public_url': 'wss://api.upbit.com/websocket/v1',
        'ws_private_url': 'wss://api.upbit.com/websocket/v1/private',
        # 업비트 차트와 동일한 60T 틱봉 (공개 REST trades로는 경계/틱 정의가 불일치)
        'tick_candle_url': 'https://crix-api-cdn.upbit.com/v1/crix/candles/ticks/60',
        'tick_candle_code': 'CRIX.UPBIT.KRW-{symbol}',
        'order_endpoint': '/v1/orders',              # 주문 생성 (POST)
        'orders_list_endpoint': '/v1/orders/open',   # 체결대기 목록 (신규 GET /orders/open)
        'orders_uuids_endpoint': '/v1/orders/uuids', # id 일괄 조회/취소
        'orders_open_cancel_endpoint': '/v1/orders/open',  # 조건 일괄취소 DELETE
        'cancel_and_new_endpoint': '/v1/orders/cancel_and_new',
        'ticker_all_endpoint': '/v1/ticker/all',     # 마켓 단위 현재가
        'cancel_endpoint': '/v1/order',              # 개별 취소 (DELETE)
        'order_query_endpoint': '/v1/order',         # 개별 주문 조회 (GET)
        'order_type_field': 'ord_type',
        'order_id_param': 'uuid',
        'ws_order_id_field': 'uuid',
        # DEFAULT ask_bid=BID/ASK + SIMPLE ab, 그리고 이미 정규화된 bid/ask
        'ws_side_map': {
            'bid': 'bid', 'ask': 'ask',
            'BID': 'bid', 'ASK': 'ask',
            'buy': 'bid', 'sell': 'ask',
        },
        'mytrade_supported': True,
        # 신규 Exchange API 지원 플래그 (빗썸은 False → 레거시 폴백)
        'supports_batch_cancel_ids': True,   # DELETE /v1/orders/uuids (최대 20)
        'supports_batch_cancel_open': True,  # DELETE /v1/orders/open (최대 300)
        'supports_batch_query_ids': True,    # GET /v1/orders/uuids (최대 100)
        'supports_cancel_and_new': True,     # POST /v1/orders/cancel_and_new
        'supports_ticker_all': True,         # GET /v1/ticker/all
    },
    'bithumb': {
        'name': 'bithumb',
        'server_url': 'https://api.bithumb.com',
        'ws_public_url': 'wss://ws-api.bithumb.com/websocket/v1',
        'ws_private_url': 'wss://ws-api.bithumb.com/websocket/v2/private',
        'tick_candle_url': None,
        'tick_candle_code': None,
        'order_endpoint': '/v2/orders',              # POST JSON body
        'orders_list_endpoint': '/v2/orders/pending',  # GET 대기 주문
        'orders_uuids_endpoint': None,
        'orders_open_cancel_endpoint': None,
        'cancel_and_new_endpoint': None,
        'ticker_all_endpoint': None,
        'cancel_endpoint': '/v2/order',              # DELETE ?order_id=
        'order_query_endpoint': '/v1/order',         # GET ?uuid= (v1 스키마)
        'order_type_field': 'order_type',
        'order_id_param': 'order_id',                # 취소(v2)
        'order_query_id_param': 'uuid',              # 조회(v1) — 값은 order_id와 동일
        'ws_order_id_field': 'order_id',
        'ws_side_map': {'buy': 'bid', 'sell': 'ask'},
        'mytrade_supported': False,
        'jwt_requires_timestamp': True,             # 빗썸 JWT timestamp 필수
        'private_ws_jwt_alg': 'HS256',              # 빗썸 Private WS는 HS256
        'order_post_json': True,                    # POST /v2/orders 는 JSON body
        'supports_batch_cancel_ids': False,         # POST /v2/orders/cancel 별도 연동 가능
        'supports_batch_cancel_open': False,
        'supports_batch_query_ids': False,          # POST /v2/orders/search 별도 연동 가능
        'supports_cancel_and_new': False,
        'supports_ticker_all': False,
    },
}
# 기본값 — __main__에서 -e/--exchange로 교체. 모듈 임포트 시에는 upbit.
EXCHANGE = EXCHANGE_CONFIGS['upbit']

def _ws_format_extra():
    """업비트만 SIMPLE 포맷(페이로드 축소). 빗썸은 기본 포맷 유지.
    SIMPLE: JSON 키 축약(type→ty, assets→ast, currency→cu, balance→b …)
    — Protobuf는 거래소 미지원. 가능한 최소 페이로드는 SIMPLE."""
    if EXCHANGE.get('name') == 'upbit':
        return [{"format": "SIMPLE"}]
    return []


def _fast_float(v, default=0.0):
    """WS 필드 → float (핫패스). None/''/이미 float 허용."""
    if v is None or v == '':
        return default
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _asset_avg_field(a):
    """myAsset 항목에 평단 필드가 있으면 값, 없으면 None.
    업비트 공식 myAsset는 balance/locked만 제공 — 평단은 REST /v1/accounts."""
    if not isinstance(a, dict):
        return None
    if 'avg_buy_price' in a:
        return a.get('avg_buy_price')
    if 'abp' in a:
        return a.get('abp')
    return None


def _asset_avg_from_item(a, prev_avg=0.0):
    """myAsset 항목에서 평단 추출 — 필드 없으면 prev 유지."""
    v = _asset_avg_field(a)
    if v is None:
        return float(prev_avg or 0)
    return _fast_float(v, float(prev_avg or 0))

# 서버/조회 URL은 EXCHANGE에서 파생. server_url 변경 시 자동 갱신.
SERVER_URL = EXCHANGE['server_url']
CANDLE_URL = SERVER_URL + "/v1/candles/minutes/" + str(UNIT)
TRADES_URL = SERVER_URL + "/v1/trades/ticks"
TICKER_URL = SERVER_URL + "/v1/ticker"
ORDERBOOK_URL = SERVER_URL + "/v1/orderbook"
TICK_CANDLE_URL = EXCHANGE.get('tick_candle_url')
TICK_CANDLE_CODE = EXCHANGE.get('tick_candle_code')
_refresh_hot_urls()
COMMISSION = 0.9995  # 0.05% 수수료
MIN_ORDER_AMOUNT = 5000
MIN_HOLDING_VOLUME = 0.0001  # 최소 보유 수량
STOP_LOSS_PERCENTAGE = -8.0  # 스탑로스 -8%


def holding_notional_krw(symbol, volume=None):
    """보유 평가액(KRW). volume 없으면 잔고 조회."""
    if not symbol:
        return 0.0
    vol = volume
    if vol is None:
        try:
            if private_ws._is_initialized:
                bal, locked, _ = private_ws.get_symbol_info(symbol)
            else:
                bal, locked, _ = AccountChecker().get_symbol_info(symbol)
            if bal < 0:
                return 0.0
            vol = float(bal) + float(locked)
        except Exception:
            return 0.0
    if vol is None or vol < MIN_HOLDING_VOLUME:
        return 0.0
    px = RealMarketData.get_current_price(symbol) or 0.0
    if px <= 0:
        return 0.0
    return float(vol) * float(px)


def is_dust_holding(symbol, volume=None):
    """최소주문금액 미만 잔량 — 매도 불가, 매수로 흡수해야 함."""
    if volume is not None and volume < MIN_HOLDING_VOLUME:
        return False
    notional = holding_notional_krw(symbol, volume)
    if volume is None and notional <= 0:
        return False
    if volume is not None and volume >= MIN_HOLDING_VOLUME and notional <= 0:
        # 가격 미수신 시 수량이 있으면 먼지로 보지 않음
        return False
    return 0 < notional < MIN_ORDER_AMOUNT


def rest_holding_snapshot(symbol):
    """REST force로 (vol, notional, sellable) — 사이클 종료/전환 가드용.
    sellable=True 이면 전량매도 필요(≥5000원). 먼지·0만 False."""
    if not symbol:
        return 0.0, 0.0, False
    try:
        bal, locked, avg = AccountChecker._rest_symbol_info(
            ACCESS_KEY, SECRET_KEY, symbol, force=True)
        if bal < 0:
            bal, locked, avg = 0.0, 0.0, 0.0
        vol = max(float(bal), 0.0) + max(float(locked), 0.0)
    except Exception:
        return 0.0, 0.0, True  # 조회 실패 시 안전하게 매도 필요로 간주
    if vol < MIN_HOLDING_VOLUME:
        return vol, 0.0, False
    try:
        px = float(RealMarketData.get_current_price(symbol) or 0)
    except Exception:
        px = 0.0
    if px <= 0:
        try:
            px = float(avg or 0)
        except (TypeError, ValueError):
            px = 0.0
    notional = vol * max(px, 0.0)
    if notional <= 0 and vol >= MIN_HOLDING_VOLUME:
        # 가격 없으면 수량이 있으면 매도 필요로 보수 처리
        return vol, notional, True
    sellable = notional >= MIN_ORDER_AMOUNT
    return vol, notional, sellable


def rest_holdings_cleared(symbol):
    """전량매도 완료(또는 먼지만)면 True — 다음 사이클 진입 조건."""
    _, _, sellable = rest_holding_snapshot(symbol)
    return not sellable

# 다중 분할매수 설정 — 전 라운드 삼중매수 (SPLIT_ORDER_MAX).
# 분할 시 각 주문 금액이 MIN_ORDER_AMOUNT(5000원) 미만이면 자동으로 단일 폴백.
SPLIT_ORDER_MAX = 3          # 전 라운드 분할 개수
SPLIT_STEP_PERCENT = 0.2     # 분할 가격 간격 (%). 0%, -0.2%, -0.4% ...
# ★ 워커(심볼)당 미체결 매수 절대 상한 — 절대 초과 금지
MAX_OPEN_BUYS_PER_WORKER = SPLIT_ORDER_MAX

def split_count_for_level(level):
    """전 라운드 삼중매수 — 항상 SPLIT_ORDER_MAX."""
    return min(int(SPLIT_ORDER_MAX), int(MAX_OPEN_BUYS_PER_WORKER))

# Global state — set 로 O(1) 멤버십/제거 (핫패스)
InitialBalance = 0
buy_uuids = set()
sell_uuids = set()

# 매수 배치 epoch + lifecycle lock
# 주범: 매수 POST 직전 cancel_buy_orders_async() → DELETE /orders/open 이
# 방금 건 bid 를 지움. sync 취소 + epoch + lock 으로 차단.
_buy_epoch = 0
_buy_epoch_lock = threading.Lock()
_buy_lifecycle_lock = threading.RLock()

def begin_buy_placement_window():
    """새 매수 POST 직전 호출 — 대기 중이던 cancel 스레드를 무효화."""
    global _buy_epoch
    with _buy_epoch_lock:
        _buy_epoch += 1
        return _buy_epoch

def current_buy_epoch():
    with _buy_epoch_lock:
        return _buy_epoch


# 거래 완료된 코인 저장 (1시간 동안 유효)
traded_symbols = {}

# 현재 거래 중인 코인 캐시
current_trading_symbol = None
symbol_cache_time = None
CACHE_DURATION = 3600  # 1시간 캐시

class LogLevel:
    # colorama 스타일 문자열 — 레벨마다 값이 달라야 quiet 필터가 동작함
    # SUCCESS: 초록 글자 / SELL_SUCCESS: 매도 체결·완료만 하늘색 배경
    INFO = Back.RESET + Fore.GREEN
    SUCCESS = Back.RESET + Fore.GREEN + Style.BRIGHT
    SELL_SUCCESS = Fore.LIGHTWHITE_EX + Back.LIGHTCYAN_EX + Style.BRIGHT
    WARNING = Fore.LIGHTWHITE_EX + Back.LIGHTMAGENTA_EX + Style.BRIGHT
    EXCEPTION = Back.RESET + Fore.LIGHTYELLOW_EX + Style.BRIGHT
    ERROR = Fore.LIGHTWHITE_EX + Back.LIGHTRED_EX + Style.BRIGHT

# Quiet(기본): 거래 핵심만 콘솔. --verbose 시 INFO/WS·취소 내부까지 전부 출력.
VERBOSE = False

# Quiet 모드에서 허용할 SUCCESS 키워드 (거래 진행에 실제로 필요한 것만)
_QUIET_SUCCESS = (
    '거래소:',
    'Selected symbol:',
    'VolatilityScanner 선별',
    'Calculated ',
    'Starting dynamic buying',
    'Dynamic buying started',
    'STARTING PARALLEL',
    'Buy order placed',
    'Order ',
    'LAST order',
    'Level ',
    'All planned orders',
    'Buy orders executed',
    'Sell orders executed',
    'Trading completed',
    'Resumed cycle',
    'Holdings flat',
    'Applied pending',
    'Plan shifted',
    '매도',
    '재매수',
    'sell-orphan',
    'Command symbols',
    'Command 신규',
    '게이트',
    'Cycle ',
    'PnL',
)

# Quiet 모드에서 허용할 WARNING 키워드 (조치/중단이 필요한 것만)
_QUIET_WARNING = (
    'Stop loss',
    'stop loss',
    '스탑로스',
    'Last-buy',
    'Emergency',
    'insufficient',
    'Exit command',
    'Trading cycle timeout',
    'Resume cycle timeout',
    'Trading completed due to stop',
    'No valid symbol',
    'No valid symbols',
    'blocked by volatility',
    '매도 불가',
    '매도주문',
    '매도#',
    'Level ',
    'Resume incomplete',
    'holding',
    'KRW budget',
    '스캐너 후보 없음',
    'MA60 gate',
    'HybridMA',
    'Resume:',
    '잔고 조회',
    '먼지진',
    '먼지',
    '사다리',
    'cycle-end',
    '흡수대기',
    'insufficient_funds',
    '매도 대기',
    'Buy ladder resume',
    'BUY CAP',
    'BUY SAME-PX',
    'fill-timeout',
    'sell-orphan',
    '동일호가',
    '보유0',
    '사이클 종료',
    '사이클종료',
    'command.txt 제외',
    'command 제외',
    'Command 신규',
    '신규매수중단',
    '집중모드',
    '게이트 spawn',
    '게이트 감시',
    'AUTO',
    'Vol Top',
)

# Quiet 모드에서도 보여줄 INFO (사이클 경계 등)
_QUIET_INFO = (
    '=== Trading Cycle',
    'Target Symbol:',
)

def _ts_prefix():
    """핫패스용 가벼운 타임스탬프 (datetime 객체 생성 생략)."""
    return time.strftime('[%m/%d %X] ', time.localtime())

def _quiet_match(message, keywords):
    for kw in keywords:
        if kw in message:
            return True
    return False

def _emit_log(level, message):
    """colorama 컬러 로그 출력 (옛 스타일).
    타임스탬프 뒤에 RESET 후 레벨 적용 — 이전 로그 배경이 INFO에 남지 않게."""
    print(
        f"{Style.RESET_ALL}{Fore.MAGENTA}{Style.NORMAL}{_ts_prefix()}"
        f"{Style.RESET_ALL}{level}{message}{Style.RESET_ALL}",
        flush=True,
    )

def print_log(level, message):
    """기본(quiet): ERROR/EXCEPTION + 거래 핵심 SUCCESS/WARNING/INFO 만 콘솔.
    --verbose: 기존처럼 전부 출력. colorama 레벨 색상 적용."""
    if VERBOSE:
        _emit_log(level, message)
        return
    if level in (LogLevel.ERROR, LogLevel.EXCEPTION, LogLevel.SELL_SUCCESS):
        _emit_log(level, message)
        return
    if level == LogLevel.SUCCESS and _quiet_match(message, _QUIET_SUCCESS):
        _emit_log(level, message)
        return
    if level == LogLevel.WARNING and _quiet_match(message, _QUIET_WARNING):
        _emit_log(level, message)
        return
    if level == LogLevel.INFO and _quiet_match(message, _QUIET_INFO):
        _emit_log(level, message)
        return

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
                pass
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
    마지막 값만 유지 (같은 파일에 연속 쓰기 시 이전 값 무시).
    sleep 없이 Event로만 깨움."""
    _queue = {}      # {filepath: content_str}
    _lock = threading.Lock()
    _wake = threading.Event()
    _thread = None

    @classmethod
    def _worker(cls):
        while True:
            cls._wake.wait()
            cls._wake.clear()
            try:
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

    @classmethod
    def write(cls, filepath, content):
        """비동기 쓰기 요청 — 즉시 반환, 백그라운드에서 플러시."""
        if cls._thread is None:
            cls._thread = threading.Thread(target=cls._worker, daemon=True)
            cls._thread.start()
        with cls._lock:
            cls._queue[filepath] = content
        cls._wake.set()

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
    AsyncLogger.write(str(BALANCE_TXT), str(int(balance)) + ',' + str(int(InitialBalance)))


def ws_krw_total():
    """Private WS myAsset KRW(balance+locked). 미스 시 REST 폴백."""
    try:
        if private_ws._is_initialized and private_ws.is_connected:
            bal = float(private_ws.get_krw_balance(1) or 0)
            locked = float(private_ws.get_krw_balance(2) or 0)
            total = bal + locked
            if total > 0:
                return total
    except Exception:
        pass
    try:
        return float(AccountChecker().get_krw_balance() or 0)
    except Exception:
        return 0.0


def report_cycle_pnl(cycle_n, symbol, start_krw, end_krw=None):
    """사이클 손익 — WS KRW 스냅샷 기준. SUCCESS라 quiet에서도 출력."""
    global InitialBalance
    if end_krw is None:
        end_krw = ws_krw_total()
    start = float(start_krw or 0)
    end = float(end_krw or 0)
    cycle_pnl = int(round(end - start))
    cum_pnl = int(round(end - float(InitialBalance or start)))
    sign = '+' if cycle_pnl >= 0 else ''
    cum_sign = '+' if cum_pnl >= 0 else ''
    print_log(LogLevel.SUCCESS,
              f"Cycle {cycle_n} PnL [{symbol}] {sign}{cycle_pnl:,} KRW  "
              f"({int(start):,} → {int(end):,})  "
              f"누적 {cum_sign}{cum_pnl:,} KRW")
    log_balance(end)
    return end, cycle_pnl

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
    AsyncLogger.write(str(STATE_TXT), content)

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
    # bisect용 오름차순 하한/틱 (O(log n) 조회)
    _TICK_BOUNDS_ASC = [row[0] for row in reversed(TICK_TABLE)]
    _TICK_VALUES_ASC = [row[1] for row in reversed(TICK_TABLE)]
    # 밴드 캐시 — 동일 호가 구간 연속 조회 시 O(1)
    _band_lo = None
    _band_hi = None
    _band_tick = None
    _band_decimals = None

    # 코인 수량 최소 단위 (매 호출 10**-n 제거)
    VOLUME_PRECISION = 8
    VOLUME_QUANTUM = 1e-8

    @staticmethod
    def get_minimum_tick(price):
        """호가단위 O(log n) + 동일 밴드 O(1) 캐시."""
        if price is None:
            return UpbitTickSystem._TICK_VALUES_ASC[0]
        lo = UpbitTickSystem._band_lo
        hi = UpbitTickSystem._band_hi
        if lo is not None and lo <= price < hi:
            return UpbitTickSystem._band_tick

        bounds = UpbitTickSystem._TICK_BOUNDS_ASC
        ticks = UpbitTickSystem._TICK_VALUES_ASC
        i = bisect.bisect_right(bounds, price) - 1
        if i < 0:
            i = 0
        tick = ticks[i]
        # 밴드: [bounds[i], bounds[i+1]) — 최상단은 +inf
        UpbitTickSystem._band_lo = bounds[i]
        UpbitTickSystem._band_hi = bounds[i + 1] if i + 1 < len(bounds) else float('inf')
        UpbitTickSystem._band_tick = tick
        if tick >= 1:
            UpbitTickSystem._band_decimals = 0
        else:
            UpbitTickSystem._band_decimals = max(0, -int(math.floor(math.log10(tick))))
        return tick

    @staticmethod
    def _tick_decimals(tick):
        if tick >= 1:
            return 0
        return max(0, -int(math.floor(math.log10(tick))))

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
    def ceil_volume(volume):
        """코인 수량을 거래소 최소 단위(1e-8)로 올림."""
        if volume <= 0:
            return 0.0
        q = UpbitTickSystem.VOLUME_QUANTUM
        return math.ceil(volume / q) * q

    @staticmethod
    def calculate_sell_price(avg_buy_price, profit_percentage):
        required_price = avg_buy_price * (1 + profit_percentage / 100)
        return UpbitTickSystem.round_up(required_price)

    @staticmethod
    def min_no_loss_sell_price(cost_price):
        """매수 원가 대비 손해 없는 최소 매도호가.
        업비트 왕복 수수료(COMMISSION^2) + 호가 올림 + 동일틱이면 1틱 상향."""
        try:
            cost = float(cost_price or 0)
        except (TypeError, ValueError):
            return 0.0
        if cost <= 0:
            return 0.0
        try:
            be = cost / (COMMISSION * COMMISSION)
        except Exception:
            be = cost * 1.00101
        px = UpbitTickSystem.round_up(be)
        if px + 1e-15 < cost:
            px = UpbitTickSystem.round_up(cost)
        # 매도가 == 매수가(또는 그 이하)면 최소 1틱 위
        if px <= cost + 1e-15:
            tick = UpbitTickSystem.get_minimum_tick(cost)
            px = UpbitTickSystem.round_up(cost + max(tick, 1e-12))
        return px

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
    def format_price(price):
        """가격대에 맞춰 소수점 자리를 자동 선택한 문자열 반환."""
        if price is None:
            return 'N/A'
        tick = UpbitTickSystem.get_minimum_tick(price)
        if tick >= 1:
            return f"{price:,.0f}"
        decimals = UpbitTickSystem._band_decimals
        if decimals is None:
            decimals = UpbitTickSystem._tick_decimals(tick)
        return f"{price:,.{decimals}f}"

    @staticmethod
    def snap_to_tick(price, mode='round'):
        """호가단위로 정렬 — 수치 round만 (문자열 왕복 제거)."""
        if price is None or price <= 0:
            return 0.0
        tick = UpbitTickSystem.get_minimum_tick(price)
        if tick <= 0:
            return float(price)
        ratio = price / tick
        if mode == 'floor':
            n = math.floor(ratio + 1e-12)
        elif mode == 'ceil':
            n = math.ceil(ratio - 1e-12)
        else:
            n = round(ratio)
        snapped = n * tick
        decimals = UpbitTickSystem._band_decimals
        if decimals is None:
            decimals = UpbitTickSystem._tick_decimals(tick)
        return round(snapped, decimals) if decimals > 0 else float(int(round(snapped)))

    @staticmethod
    def format_order_price(price):
        """주문 API용 가격 문자열 — 호가단위 snap 후 콤마 없이."""
        if price is None or price <= 0:
            return '0'
        snapped = UpbitTickSystem.snap_to_tick(price)
        tick = UpbitTickSystem.get_minimum_tick(snapped if snapped > 0 else price)
        if tick >= 1:
            return str(int(round(snapped)))
        decimals = UpbitTickSystem._band_decimals
        if decimals is None:
            decimals = UpbitTickSystem._tick_decimals(tick)
        return f"{snapped:.{decimals}f}"

    @staticmethod
    def floor_volume(volume):
        """코인 수량을 거래소 최소 단위(1e-8)로 내림."""
        if volume <= 0:
            return 0.0
        q = UpbitTickSystem.VOLUME_QUANTUM
        return math.floor(volume / q + 1e-15) * q

    @staticmethod
    def ask_safe_volume(available, shrink=1.0):
        """매도(ask)용 수량 — 전량 floor.
        shrink=1.0 이면 quantum 차감 없음(잔량 방치 방지).
        shrink<1 재시도에만 여유를 둠."""
        if available is None or available <= 0:
            return 0.0
        capped = float(available) * float(shrink)
        if shrink < 1.0 - 1e-15:
            q = UpbitTickSystem.VOLUME_QUANTUM
            capped = max(0.0, capped - q)
        return UpbitTickSystem.floor_volume(capped)

    @staticmethod
    def format_order_volume(volume, decimals=8):
        """주문 API용 수량 문자열 — floor + 부동소수 잔여 제거."""
        if volume is None or volume <= 0:
            return '0'
        d = max(0, min(8, int(decimals)))
        scale = 10 ** d
        v = math.floor(float(volume) * scale + 1e-15) / scale
        if v <= 0:
            return '0'
        if d == 0:
            return str(int(v))
        return f"{v:.{d}f}".rstrip('0').rstrip('.')

    @staticmethod
    def volume_for_krw(price, krw):
        """KRW 예산 이하로 살 수 있는 최대 수량 (호가 snap + volume floor).
        업비트는 price×volume + 수수료(0.05%)를 잠그므로 COMMISSION을 미리 차감.
        (미차감 시 잔고≈주문액인 마지막 라운드가 insufficient_funds_bid로 거절됨)"""
        if krw is None or price is None or krw <= 0 or price <= 0:
            return 0.0
        px = UpbitTickSystem.snap_to_tick(price)
        if px <= 0:
            return 0.0
        # locked = notional × 1.0005 ≤ krw  →  notional ≤ krw × 0.9995
        spendable = float(krw) * COMMISSION
        return UpbitTickSystem.floor_volume(spendable / px)

    @staticmethod
    def generate_split_prices(base_price, count, step_pct):
        """base_price 기준 하락 사다리로 count 개의 가격을 생성.
        Returns: list[float] (기준가→저가). 분리 불가면 [base] 단일."""
        if count <= 1:
            return [UpbitTickSystem.snap_to_tick(base_price)]

        base = UpbitTickSystem.snap_to_tick(base_price)
        prices = []
        for i in range(count):
            raw = base_price * (1.0 - i * step_pct / 100.0)
            snapped = UpbitTickSystem.snap_to_tick(raw, 'round')
            if prices:
                prev = prices[-1]
                prev_tick = UpbitTickSystem.get_minimum_tick(prev)
                max_allowed = UpbitTickSystem.snap_to_tick(prev - prev_tick, 'floor')
                if snapped >= prev or max_allowed <= 0:
                    snapped = max_allowed
                elif snapped > max_allowed:
                    snapped = max_allowed
            if snapped <= 0:
                break
            prices.append(snapped)

        # 수치 키로 중복 제거 (format 문자열 왕복 제거)
        uniq = []
        seen = set()
        for p in prices:
            if p <= 0:
                continue
            key = p  # 이미 snap+round 됨
            if key not in seen:
                seen.add(key)
                uniq.append(p)

        if len(uniq) < count:
            uniq = UpbitTickSystem._tick_ladder_prices(base, count)

        if len(uniq) < count:
            return [base]
        return uniq

    @staticmethod
    def _tick_ladder_prices(base_price, count):
        """호가 1틱 간격 하락 사다리: [base, base-1tick, base-2tick, ...]."""
        cursor = UpbitTickSystem.snap_to_tick(base_price)
        prices = []
        seen = set()
        for _ in range(count):
            if cursor <= 0:
                break
            if cursor not in seen:
                seen.add(cursor)
                prices.append(cursor)
            tick = UpbitTickSystem.get_minimum_tick(cursor)
            cursor = UpbitTickSystem.snap_to_tick(cursor - tick, 'floor')
        return prices

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
        self._connect_gen = 0
        self._last_ws_error = ''
        self._ws_err_log_at = 0.0

    def subscribe(self, symbol):
        """심볼 구독 시작. 기존 연결이 있으면 종료 후 새 심볼로 재연결."""
        if self.current_symbol == symbol and self.is_connected:
            return  # 이미 같은 심볼 구독 중
        self.current_symbol = symbol
        # 기존 루프 무효화 후 단일 스레드만 재시작
        self._connect_gen += 1
        self._should_reconnect = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        self._should_reconnect = True
        gen = self._connect_gen
        self.thread = threading.Thread(
            target=self._connect_loop, args=(gen,), daemon=True)
        self.thread.start()
        print_log(LogLevel.INFO, f"WebSocket ticker 구독 시작: KRW-{symbol}")

    def get_price(self, symbol):
        """캐시에서 최신가 조회. 캐시 만료 시 None 반환(호출자가 REST 폴백)."""
        if symbol not in self.price_cache:
            return None
        ts = self.cache_timestamp.get(symbol, 0)
        if (time.time() - ts) > self.CACHE_TTL:
            return None  # 만료 — REST 폴백
        return self.price_cache[symbol]

    def _connect_loop(self, gen):
        """백그라운드 재연결 — gen 불일치 시 종료, 429 시 지수 백오프."""
        attempt = 0
        while self._should_reconnect and self.current_symbol and self._connect_gen == gen:
            try:
                self.ws = websocket.WebSocketApp(
                    self.WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                _ws_run_forever(self.ws)
                attempt = 0
            except Exception as e:
                self._last_ws_error = str(e)
                print_log(LogLevel.WARNING, f"WebSocket 연결 오류: {str(e)[:100]}")
            if not self._should_reconnect or self._connect_gen != gen:
                break
            delay = _ws_backoff_seconds(self._last_ws_error, attempt)
            attempt += 1
            if delay > 0:
                time.sleep(delay)

    def _on_open(self, ws):
        self.is_connected = True
        code = f"KRW-{self.current_symbol}"
        req = ([{"ticket": f"laissez-faire-{int(time.time())}"},
                {"type": "ticker", "codes": [code]}]
               + _ws_format_extra())
        _ws_send_json(ws, req)
        print_log(LogLevel.SUCCESS, f"WebSocket 연결 성공 — {code} ticker 수신 대기")

    def _on_message(self, ws, message):
        """ticker 메시지 파싱 → 캐시 갱신."""
        try:
            data = json_loads(message)
            # SIMPLE 포맷: cd/tp — 기본 포맷: code/trade_price
            code = data.get('code') or data.get('cd') or ''
            if code.startswith('KRW-'):
                symbol = code[4:]
                price = data.get('trade_price', data.get('tp'))
                if price:
                    self.price_cache[symbol] = float(price)
                    self.cache_timestamp[symbol] = time.time()
        except Exception:
            pass  # 파싱 오류는 조용히 무시

    def _on_error(self, ws, error):
        self.is_connected = False
        self._last_ws_error = str(error)
        now = time.time()
        if now - self._ws_err_log_at >= 2.0:
            self._ws_err_log_at = now
            print_log(LogLevel.WARNING, f"WebSocket 에러: {str(error)[:100]}")

    def _on_close(self, ws, close_status, close_msg):
        self.is_connected = False


class TradeTickStream:
    """업비트/빗썸 공개 WS `trade` 스트림 — 실시간 체결가 수신.
    60틱(60체결) = 1캔들 단위로 체결을 묶어 60T 틱차트 종가를 적재.
    == 업비트 차트 60T 틱차트와 동일 기준 ==
      - 업비트: CRIX 틱봉 API로 시드/갱신 (차트와 동일 소스).
        REST /v1/trades/ticks 60개 묶음은 차트 틱 정의와 불일치하므로 사용하지 않음.
      - 빗썸 등 CRIX 미지원: REST trades + WS trade 건수 집계 폴백
      - MA60 = 최근 60개 캔들 종가의 단순평균 (진행 중 봉 포함, 차트와 동일)
    ticker 전용 UpbitWebSocket과 동일한 패턴(백그라운드 스레드, 지수 백오프)."""

    TICKS_PER_CANDLE = 60   # 60틱(60체결) = 1 캔들 (업비트 60T)
    MA_PERIOD = 60          # 60 캔들 이동평균 (= 60틱 × 60 = 3,600틱)

    # WS_URL은 UpbitWebSocket.WS_URL을 그대로 사용 — main에서 거래소별 자동 갱신됨.
    # (두 거래소 모두 동일한 public v1 엔드포인트에서 trade 스트림 지원)

    def __init__(self):
        self.ws = None
        self.thread = None
        # 확정된 캔들 종가 버퍼 — {symbol: deque([close,...], maxlen=MA_PERIOD)}
        self.candle_closes = {}
        # 현재 진행 중인 캔들의 체결 카운터(0~TICKS_PER_CANDLE)와 종가 후보(마지막 체결가)
        self._tick_counter = {}      # {symbol: 0~59}
        self._pending_close = {}     # {symbol: price} — 진행 중 캔들의 최신 체결가
        # CRIX 틱봉 모드에서도 WS trade 체결가는 즉시 반영 (호가/스탑로스용)
        self._last_trade_price = {}  # {symbol: price}
        self._last_trade_ts = {}     # {symbol: monotonic ts} — 신선도 판정
        # 구독 중인 심볼 리스트 — 다중 심볼 동시 구독 지원 (codes 배열).
        # command.txt의 복수 심볼을 한 번에 구독하여 각각의 MA60을 독립 산출.
        self.subscribed_symbols = []    # [symbol, ...]
        self.current_symbol = None      # 단일 호환 (리스트의 첫 심볼)
        self.is_connected = False
        self._should_reconnect = True
        self._connect_gen = 0
        self._last_ws_error = ''
        self._tick_candle_fetched_at = {}
        # CRIX 백그라운드 폴링 — get_ma60 핫패스에서 REST 차단 제거
        self._crix_bg_stop = threading.Event()
        self._crix_bg_thread = None
        self._crix_bg_interval = 1.5  # bg만 — 주문 핫패스와 무관, CPU/REST 고갈 방지
        self._sub_lock = threading.Lock()

    def subscribe(self, symbol):
        """단일 심볼 추가 구독 — 기존 다중 구독을 절대 덮어쓰지 않음.
        (예전엔 subscribe_symbols([symbol])로 형제 코인 구독이 날아가
         CALDERA에 TREE 시세가 섞이는 오염이 났음.)"""
        if not symbol:
            return
        self.ensure_symbols([str(symbol).upper()])

    def ensure_symbols(self, symbols):
        """심볼을 기존 구독 집합에 합집합으로 추가. 축소(제거)는 하지 않음."""
        if not symbols:
            return
        with self._sub_lock:
            cur = list(self.subscribed_symbols)
            merged = list(cur)
            seen = set(cur)
            for s in symbols:
                if not s:
                    continue
                su = str(s).upper()
                if su not in seen:
                    seen.add(su)
                    merged.append(su)
            if set(merged) == set(cur) and self.is_connected and merged:
                return
            # 신규만 시드 — 이미 있는 심볼은 재시드/재연결 최소화
            new_only = [s for s in merged if s not in set(cur)]
        self.subscribe_symbols(merged, seed_symbols=new_only or None)

    def subscribe_symbols(self, symbols, seed_symbols=None):
        """복수 심볼 동시 구독. 집합이 같으면 no-op.
        ★ 기존 구독 심볼을 제거하지 않음 (부분 리스트로 덮어쓰기 오염 방지).
        seed_symbols: 시드할 심볼만 (None이면 신규만, 없으면 전체)."""
        add = []
        seen_add = set()
        for s in (symbols or []):
            if not s:
                continue
            su = str(s).upper()
            if su not in seen_add:
                seen_add.add(su)
                add.append(su)
        with self._sub_lock:
            cur = list(self.subscribed_symbols)
            cur_set = set(cur)
            merged = list(cur)
            for s in add:
                if s not in cur_set:
                    cur_set.add(s)
                    merged.append(s)
            new_only = [s for s in add if s not in set(cur)]
            if set(merged) == set(cur) and self.is_connected and merged:
                return  # 동일 집합 — no-op
            self.subscribed_symbols = merged
            self.current_symbol = merged[0] if merged else None
            if seed_symbols is not None:
                to_seed = [str(s).upper() for s in seed_symbols if s]
            elif new_only:
                to_seed = new_only
            else:
                to_seed = list(merged)
            self._connect_gen += 1
            self._should_reconnect = False
            if self.ws:
                try:
                    self.ws.close()
                except Exception:
                    pass
            for sym in to_seed:
                self.seed_tick_ma(sym)
            self._should_reconnect = True
            if merged:
                gen = self._connect_gen
                self.thread = threading.Thread(
                    target=self._connect_loop, args=(gen,), daemon=True)
                self.thread.start()
        self._ensure_crix_bg()

    def _ensure_crix_bg(self):
        """CRIX 틱봉 백그라운드 갱신 스레드 — 게이트 핫패스 REST 제거."""
        if not TICK_CANDLE_URL:
            return
        if self._crix_bg_thread and self._crix_bg_thread.is_alive():
            return
        self._crix_bg_stop.clear()

        def _loop():
            # Event.wait — 주문 스레드를 굶기는 busy-spin/CRIX 폭주 방지
            while not self._crix_bg_stop.wait(self._crix_bg_interval):
                for sym in list(self.subscribed_symbols):
                    try:
                        self._refresh_tick_candles_if_stale(
                            sym, ttl=self._crix_bg_interval)
                    except Exception:
                        pass

        self._crix_bg_thread = threading.Thread(
            target=_loop, daemon=True, name="crix-bg")
        self._crix_bg_thread.start()

    def seed_tick_ma(self, symbol):
        """60T MA60 버퍼 시드. 업비트는 차트와 동일한 CRIX 틱봉 API를 우선 사용.
        실패/미지원(빗썸) 시 REST /v1/trades/ticks 폴백."""
        if not symbol:
            return False
        symbol = str(symbol).upper()
        if TICK_CANDLE_URL and self.seed_from_tick_candles(symbol):
            return True
        return self.seed_from_trades(symbol)

    def seed_from_tick_candles(self, symbol):
        """업비트 CRIX 틱봉(/v1/crix/candles/ticks/60)으로 60T 종가·진행도를 시드.
        REST trades를 60개씩 묶는 방식은 차트 틱 정의와 달라 MA가 어긋남."""
        try:
            candles = self._fetch_tick_candles(symbol, self.MA_PERIOD + 1)
            if not candles:
                return False
            self._apply_tick_candles(symbol, candles)
            closes = self.candle_closes.get(symbol) or []
            ma_ready = self.get_ma60(symbol) is not None
            return ma_ready
        except Exception as e:
            print_log(LogLevel.WARNING,
                      f"TICKMA CRIX seed 예외 KRW-{symbol}: {str(e)[:100]}")
            return False

    def _fetch_tick_candles(self, symbol, count):
        """CRIX 틱봉 조회 (최신→과거). 차트 60T와 동일 소스.
        실패 시 None — 호출측에서 REST trades 폴백 가능하도록 무한 재시도하지 않음."""
        if not TICK_CANDLE_URL or not TICK_CANDLE_CODE:
            return None
        code = TICK_CANDLE_CODE.format(symbol=symbol)
        params = {"code": code, "count": str(count)}
        try:
            r = http_get(
                TICK_CANDLE_URL, params=params, timeout=HTTP_TIMEOUT_SLOW,
                slow=True)
            if r.status_code != 200:
                return None
            batch = response_json(r)
            if not batch or not isinstance(batch, list):
                return None
            return batch
        except Exception:
            return None

    def _apply_tick_candles(self, symbol, candles):
        """CRIX 응답(최신→과거)을 candle_closes / tick_counter / pending에 반영.
        미완성 봉(tickCount < 60)은 진행 중 캔들로 두고, 확정 봉만 종가 버퍼에 적재.
        신규상장/필드누락 시 KeyError 없이 스킵."""
        if not candles:
            return
        newest = candles[0] if isinstance(candles[0], dict) else None
        if not newest:
            return

        def _px(c):
            if not isinstance(c, dict):
                return None
            v = c.get('tradePrice', c.get('trade_price'))
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        tick_count = int(newest.get("tickCount") or newest.get("tick_count") or 60)
        forming_px = _px(newest)
        if tick_count < self.TICKS_PER_CANDLE:
            forming = newest
            completed = candles[1:]
            self._tick_counter[symbol] = tick_count
            if forming_px and forming_px > 0:
                self._pending_close[symbol] = forming_px
        else:
            completed = candles
            self._tick_counter[symbol] = 0
            if forming_px and forming_px > 0:
                self._pending_close[symbol] = forming_px

        closes = []
        for c in reversed(completed[:self.MA_PERIOD]):
            px = _px(c)
            if px is not None and px > 0:
                closes.append(px)
        if not closes:
            return
        self.candle_closes[symbol] = deque(closes, maxlen=self.MA_PERIOD)
        self._tick_candle_fetched_at[symbol] = time.time()
        # 시드 종가도 심볼별 최신가로 등록 — WS 재연결 전 오염/공란 방지
        pending = self._pending_close.get(symbol)
        if pending and pending > 0:
            self._last_trade_price[symbol] = float(pending)
            self._last_trade_ts[symbol] = time.time()

    def _refresh_tick_candles_if_stale(self, symbol, ttl=1.0):
        """업비트 CRIX 틱봉을 TTL 내로 재조회해 차트 MA60과 동기화.
        백그라운드 스레드에서 호출 — 게이트 핫패스 차단 금지."""
        if not TICK_CANDLE_URL:
            return
        fetched_at = self._tick_candle_fetched_at.get(symbol, 0)
        if time.time() - fetched_at < ttl:
            return
        try:
            candles = self._fetch_tick_candles(symbol, self.MA_PERIOD + 1)
            if candles:
                self._apply_tick_candles(symbol, candles)
        except Exception:
            pass

    def seed_from_trades(self, symbol):
        """REST 최근 체결(/v1/trades/ticks)로 60T 캔들 종가 버퍼를 시드 (폴백용).
        이미 MA_PERIOD개 이상이면 스킵. 실패해도 예외를 올리지 않고 live WS 축적에 맡김.
        주의: REST 체결 60개 묶음은 업비트 차트 60T와 틱 정의가 다를 수 있음."""
        if self.get_buffer_size(symbol) >= self.MA_PERIOD:
            return True
        need = self.MA_PERIOD * self.TICKS_PER_CANDLE  # 60 × 60 = 3,600
        try:
            trades = self._fetch_recent_trades(symbol, need)
            if not trades:
                print_log(LogLevel.WARNING,
                          f"TICKMA seed 실패 KRW-{symbol}: 체결 이력 없음")
                return False
            # API는 최신→과거. 시간순(과거→최신)으로 뒤집어 60개씩 묶음.
            prices = [float(t['trade_price']) for t in reversed(trades)
                      if t.get('trade_price') is not None]
            if len(prices) < self.TICKS_PER_CANDLE:
                print_log(LogLevel.WARNING,
                          f"TICKMA seed 부족 KRW-{symbol}: 체결 {len(prices)}개 "
                          f"(최소 {self.TICKS_PER_CANDLE} 필요)")
                if prices:
                    self._pending_close[symbol] = prices[-1]
                    self._tick_counter[symbol] = len(prices) % self.TICKS_PER_CANDLE
                return False

            # 최신 쪽 미완성 캔들(나머지 틱)을 진행 중으로 두고, 나머지로 확정 캔들 생성.
            remainder = len(prices) % self.TICKS_PER_CANDLE
            if remainder:
                pending = prices[-remainder:]
                complete = prices[:-remainder]
                self._tick_counter[symbol] = remainder
                self._pending_close[symbol] = pending[-1]
            else:
                complete = prices
                self._tick_counter[symbol] = 0
                self._pending_close[symbol] = complete[-1]

            closes = []
            for i in range(0, len(complete), self.TICKS_PER_CANDLE):
                chunk = complete[i:i + self.TICKS_PER_CANDLE]
                if len(chunk) == self.TICKS_PER_CANDLE:
                    closes.append(chunk[-1])  # 60번째 체결가 = 캔들 종가
            closes = closes[-self.MA_PERIOD:]
            self.candle_closes[symbol] = deque(closes, maxlen=self.MA_PERIOD)
            pending = self._pending_close.get(symbol)
            if pending and pending > 0:
                self._last_trade_price[symbol] = float(pending)
                self._last_trade_ts[symbol] = time.time()

            ma_ready = self.get_ma60(symbol) is not None
            return ma_ready
        except Exception as e:
            print_log(LogLevel.WARNING,
                      f"TICKMA seed 예외 KRW-{symbol}: {str(e)[:100]}")
            return False

    def _fetch_recent_trades(self, symbol, need_count):
        """/v1/trades/ticks 커서 페이지네이션으로 최근 체결 need_count개 수집 (최신→과거).
        당일 체결이 부족하면 days_ago=1..7 로 이어 조회. 페이지당 최대 500."""
        collected = []
        # None=당일, 이후 1~7일 전. 저유동성 심볼도 MA60 시드가 가능하도록.
        for days_ago in [None] + list(range(1, 8)):
            if len(collected) >= need_count:
                break
            cursor = None
            while len(collected) < need_count:
                qs = {"market": f"KRW-{symbol}", "count": "500"}
                if days_ago is not None:
                    qs["days_ago"] = str(days_ago)
                if cursor is not None:
                    qs["cursor"] = str(cursor)

                def api_call(params=dict(qs)):
                    r = http_get(TRADES_URL, params=params, timeout=HTTP_TIMEOUT_SLOW,
                                 slow=True)
                    return response_json(r)

                batch = safe_api_call(api_call)
                if not batch or not isinstance(batch, list):
                    break
                collected.extend(batch)
                if len(batch) < 500:
                    break  # 해당 일자 더 이상 없음 → 다음 days_ago
                cursor = batch[-1].get('sequential_id')
                if cursor is None:
                    break
        return collected[:need_count]

    def _connect_loop(self, gen):
        """백그라운드 재연결 — gen 불일치 시 종료, 429 시 지수 백오프."""
        attempt = 0
        while self._should_reconnect and self.subscribed_symbols and self._connect_gen == gen:
            try:
                self.ws = websocket.WebSocketApp(
                    UpbitWebSocket.WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                _ws_run_forever(self.ws)
                attempt = 0
            except Exception as e:
                self._last_ws_error = str(e)
            if not self._should_reconnect or self._connect_gen != gen:
                break
            delay = _ws_backoff_seconds(self._last_ws_error, attempt)
            attempt += 1
            if delay > 0:
                time.sleep(delay)

    def _on_open(self, ws):
        self.is_connected = True
        # 다중 심볼 동시 구독 — codes 배열로 한 번에 전체 심볼 구독.
        codes = [f"KRW-{s}" for s in self.subscribed_symbols]
        # trade 스트림 구독 — SIMPLE 포맷으로 페이로드 축소
        req = ([{"ticket": f"tradetick-{int(time.time())}"},
                {"type": "trade", "codes": codes}]
               + _ws_format_extra())
        _ws_send_json(ws, req)

    def _on_message(self, ws, message):
        """trade 메시지 파싱 → 체결가 갱신 + (비-CRIX) 60T 캔들 집계.
        업비트(CRIX 틱봉)는 MA60 집계는 CRIX에 맡기되, 실시간 체결가는 항상 갱신."""
        try:
            data = json_loads(message)
            code = data.get('code') or data.get('cd') or ''
            if not code.startswith('KRW-'):
                return
            symbol = code[4:].upper()
            price = data.get('trade_price', data.get('tp'))
            if price is None:
                return
            price = float(price)
            # 핫패스 가격 — CRIX 여부와 무관하게 항상 최신 체결가 유지
            # ★ code에서 파싱한 symbol만 키로 씀 (current_symbol 폴백 금지)
            self._last_trade_price[symbol] = price
            self._last_trade_ts[symbol] = time.time()

            # CRIX 틱봉 사용 시 확정봉은 bg 폴링, 진행봉 종가만 실시간 반영
            if TICK_CANDLE_URL:
                self._pending_close[symbol] = price
                return

            # 체결 건수 카운터 증가 — 60개 도달 시 캔들 확정
            count = self._tick_counter.get(symbol, 0) + 1
            self._pending_close[symbol] = price
            if count >= self.TICKS_PER_CANDLE:
                closes = self.candle_closes.setdefault(
                    symbol, deque(maxlen=self.MA_PERIOD))
                closes.append(price)
                self._tick_counter[symbol] = 0
            else:
                self._tick_counter[symbol] = count
        except Exception:
            pass

    def _on_error(self, ws, error):
        self.is_connected = False
        self._last_ws_error = str(error)

    def _on_close(self, ws, close_status, close_msg):
        self.is_connected = False

    def get_ma60(self, symbol):
        """MA60(최근 60개 60틱 캔들 종가의 단순이동평균) 반환.
        업비트 차트와 동일하게 진행 중(미완성) 봉의 현재 종가도 포함한다.
        종가 샘플이 60개 미만이면 None.
        CRIX REST는 백그라운드 스레드만 — 여기선 메모리 읽기만 (게이트 비가동)."""
        if not symbol:
            return None
        symbol = str(symbol).upper()
        closes = self.candle_closes.get(symbol)
        if not closes:
            return None
        pending = self._pending_close.get(symbol)
        progress = self._tick_counter.get(symbol, 0)
        if progress > 0 and pending is not None:
            values = list(closes) + [pending]
        else:
            values = list(closes)
        values = values[-self.MA_PERIOD:]
        if len(values) < self.MA_PERIOD:
            return None
        return sum(values) / len(values)

    # 하위 호환
    get_ma20 = get_ma60
    get_ma25 = get_ma60

    def get_last_price(self, symbol, max_age=None):
        """WS trade 실시간 체결가.
        - 구독 중이 아닌 심볼의 캐시는 무시 (덮어쓰기 오염 잔재 차단)
        - 동일 심볼 CRIX pending과 25% 이상 어긋나면 pending 우선
        - max_age 지정 시에만 신선도 만료"""
        if not symbol:
            return None
        su = str(symbol).upper()
        # 구독 집합에 없으면 캐시 불신 (형제 워커가 단독 구독으로 덮었던 시절 잔재)
        sub = set(self.subscribed_symbols or [])
        if sub and su not in sub:
            return None
        px = self._last_trade_price.get(su)
        if px is None:
            return None
        if max_age is not None and max_age > 0:
            ts = float(self._last_trade_ts.get(su) or 0)
            if (time.time() - ts) > max_age:
                return None
        # CRIX/시드 pending과 괴리 → 다른 코인 시세 오염으로 간주
        pending = self._pending_close.get(su)
        try:
            pending = float(pending) if pending is not None else 0.0
        except (TypeError, ValueError):
            pending = 0.0
        if pending > 0 and px > 0:
            if abs(px - pending) / pending > 0.25:
                return pending
        return px

    def get_buffer_size(self, symbol):
        """확정된 60틱 캔들 종가 개수 (MA60 산출엔 60개 이상 필요)."""
        if not symbol:
            return 0
        closes = self.candle_closes.get(str(symbol).upper())
        return len(closes) if closes else 0

    def get_tick_progress(self, symbol):
        """현재 진행 중인 캔들의 체결 진행도 (0~60). 디버깅/표시용."""
        if not symbol:
            return 0
        return self._tick_counter.get(str(symbol).upper(), 0)


class UpbitPrivateWS:
    """업비트 Private WebSocket — 잔고(myAsset) / 주문체결(myOrder) / 체결(myTrade) 실시간 수신.
    wss://api.upbit.com/websocket/v1/private 엔드포인트 사용. JWT 인증 필요.
    REST /v1/accounts, /v1/order 폴링을 대체하여 API 호출 없이 실시간 상태 제공."""

    WS_URL = "wss://api.upbit.com/websocket/v1/private"
    RESYNC_INTERVAL = 60  # WS healthy 시 REST 동기화 간격 (초)

    def __init__(self):
        self.ws = None
        self.thread = None
        self.is_connected = False
        self._should_reconnect = True
        self._is_initialized = False  # start() 호출 여부
        self._connect_gen = 0
        self._last_ws_error = ''
        self._ws_err_log_at = 0.0

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
        self._resync_lock = threading.Lock()
        self._resync_pending = False
        # fill / myAsset wake
        self.fills_event = threading.Event()
        self.fill_event = self.fills_event  # 구버전 별칭
        self.asset_event = threading.Event()
        # 평단→매도 핫패스 — 심볼별 슬롯 (병렬 워커)
        from .avg_sell_slot import AvgSellSlot
        self._avg_sell_lock = threading.RLock()
        self._avg_slots = {}  # symbol -> AvgSellSlot
        # legacy flat mirror (마지막 bind 심볼) — 단일워커 호환
        self.avg_sell_symbol = None
        self.avg_sell_cb = None  # callback(avg, vol)
        self._avg_sell_armed = False
        self._avg_sell_vol_hint = 0.0
        self._avg_sell_prev_avg = 0.0
        self._avg_sell_prev_t = 0.0
        self._avg_sell_baseline_avg = 0.0
        self._avg_sell_baseline_t = 0.0
        self._avg_sell_inflight = False
        self._avg_sell_fire_gen = 0
        self._avg_sell_price_floor = 0.0
        self._local_avg_qty0 = 0.0
        self._local_avg_avg0 = 0.0
        self._local_avg_fill_vol = 0.0
        self._local_avg_fill_cost = 0.0
        self._local_avg_uid_vol = {}
        self._local_avg_uid_cost = {}
        self._avg_sell_last_fired_avg = 0.0
        self._avg_sell_last_fired_vol = 0.0
        self._avg_sell_last_fire_t = 0.0
        self._avg_sell_fire_is_local = False
        self._avg_sell_awaiting_rest = False
        self._local_avg_rest_synced = False
        self._avg_rest_correct_gen = 0
        self._local_avg_max_fill = 0.0
        self._pending_avg_correct = None
        self._AvgSellSlot = AvgSellSlot

    def start(self, access_key, secret_key):
        """Private WS 연결 시작. 부팅 시 REST로 잔고 seed 후 WS 구독."""
        self.access_key = access_key
        self.secret_key = secret_key
        self._is_initialized = True
        if self.thread and self.thread.is_alive():
            return
        self._seed_assets()
        self._last_resync = time.time()
        self._connect_gen += 1
        self._should_reconnect = True
        gen = self._connect_gen
        self.thread = threading.Thread(
            target=self._connect_loop, args=(gen,), daemon=True)
        self.thread.start()
        # 평단 REST 세션 Keepalive 예열 (첫 체결 시 콜드 TLS 제거)
        self.prefetch_avg_rest()
        print_log(LogLevel.INFO, "UpbitPrivateWS 시작 — myAsset/myOrder/myTrade 구독")

    def stop(self):
        self._connect_gen += 1
        self._should_reconnect = False
        self.is_connected = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

    def _generate_jwt(self):
        """Private WS 인증용 JWT.
        업비트: HS512 + access_key/nonce
        빗썸: HS256 + timestamp 필수 (REST와 동일)"""
        payload = {
            'access_key': self.access_key,
            'nonce': _fast_nonce(),
        }
        if EXCHANGE.get('jwt_requires_timestamp'):
            payload['timestamp'] = int(time.time() * 1000)
        alg = EXCHANGE.get('private_ws_jwt_alg') or 'HS512'
        return _jwt_encode_hs(payload, alg=alg, secret=self.secret_key)

    def _seed_assets(self):
        """REST /v1/accounts 1회 — 평단 핫 세션으로 seed (slow 경로 금지)."""
        try:
            cache, _ = AccountChecker._rest_fetch_avg_hot(
                allow_stale=False, race=False)
            if cache:
                self.asset_cache = cache
                self.asset_cache_time = time.time()
                print_log(LogLevel.INFO,
                          f"PrivateWS 잔고 seed 완료 — {len(self.asset_cache)}개 통화")
        except Exception as e:
            print_log(LogLevel.WARNING, f"PrivateWS 잔고 seed 실패: {str(e)[:100]}")

    def _connect_loop(self, gen):
        """단일 스레드 재연결. 429만 짧게 대기, REST seed는 start/주기 resync만."""
        attempt = 0
        while self._should_reconnect and self._connect_gen == gen:
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
                _ws_run_forever(self.ws, private=True)
                attempt = 0
            except Exception as e:
                self._last_ws_error = str(e)
            if not self._should_reconnect or self._connect_gen != gen:
                break
            delay = _ws_backoff_seconds(self._last_ws_error, attempt)
            attempt += 1
            if delay > 0:
                time.sleep(delay)

    def _on_open(self, ws):
        self.is_connected = True
        # myAsset + myOrder + (myTrade — 거래소 지원 시) 구독
        # 빗썸 v2 Private은 myOrder만 지원 (myOrder가 체결정보 포함).
        # 업비트: SIMPLE로 페이로드 축소 (평단 수신 RTT 단축)
        types = [{"type": "myAsset"}, {"type": "myOrder"}]
        if EXCHANGE['mytrade_supported']:
            types.append({"type": "myTrade"})
        req = (
            [{"ticket": f"priv-{int(time.time())}"}]
            + types
            + _ws_format_extra()
        )
        _ws_send_json(ws, req)
        fmt = "SIMPLE" if EXCHANGE.get('name') == 'upbit' else "DEFAULT"
        print_log(LogLevel.SUCCESS,
                  f"PrivateWS 연결 성공 — {[t['type'] for t in types]} "
                  f"format={fmt}")

    def _ingest_asset_item(self, a):
        """단일 자산 항목 캐시 반영.
        Returns: (currency, avg_in_message) 또는 (None, False)."""
        if not isinstance(a, dict):
            return None, False
        currency = a.get('currency') or a.get('cu')
        if not currency:
            return None, False
        prev = self.asset_cache.get(currency) or {}
        avg_raw = _asset_avg_field(a)
        avg_in_msg = avg_raw is not None
        self.asset_cache[currency] = {
            'balance': _fast_float(a.get('balance', a.get('b', 0))),
            'locked': _fast_float(a.get('locked', a.get('l', 0))),
            'avg_buy_price': (
                _fast_float(avg_raw, float(prev.get('avg_buy_price', 0) or 0))
                if avg_in_msg
                else float(prev.get('avg_buy_price', 0) or 0)
            ),
        }
        return currency, avg_in_msg

    def _ensure_avg_slot(self, symbol):
        """심볼별 AvgSellSlot — 없으면 생성."""
        if not symbol:
            return None
        slot = self._avg_slots.get(symbol)
        if slot is None:
            slot = self._AvgSellSlot(symbol=symbol)
            self._avg_slots[symbol] = slot
        return slot

    def _mirror_slot_to_flat(self, slot):
        """단일워커 호환 — flat 필드를 슬롯과 동기화."""
        if not slot:
            return
        self.avg_sell_symbol = slot.symbol
        self.avg_sell_cb = slot.cb
        self._avg_sell_armed = slot.armed
        self._avg_sell_vol_hint = slot.vol_hint
        self._avg_sell_prev_avg = slot.prev_avg
        self._avg_sell_prev_t = slot.prev_t
        self._avg_sell_baseline_avg = slot.baseline_avg
        self._avg_sell_baseline_t = slot.baseline_t
        self._avg_sell_inflight = slot.inflight
        self._avg_sell_fire_gen = slot.fire_gen
        self._avg_sell_price_floor = slot.price_floor
        self._avg_sell_last_fired_avg = slot.last_fired_avg
        self._avg_sell_fire_is_local = slot.fire_is_local
        self._avg_sell_awaiting_rest = slot.awaiting_rest
        self._avg_rest_correct_gen = slot.rest_correct_gen
        self._pending_avg_correct = slot.pending_avg_correct
        self._local_avg_qty0 = slot.local_qty0
        self._local_avg_avg0 = slot.local_avg0
        self._local_avg_fill_vol = slot.local_fill_vol
        self._local_avg_fill_cost = slot.local_fill_cost
        self._local_avg_uid_vol = slot.local_uid_vol
        self._local_avg_uid_cost = slot.local_uid_cost
        self._local_avg_rest_synced = slot.local_rest_synced
        self._local_avg_max_fill = slot.local_max_fill

    def _mirror_flat_to_slot(self, slot):
        if not slot:
            return
        slot.cb = self.avg_sell_cb
        slot.armed = self._avg_sell_armed
        slot.vol_hint = self._avg_sell_vol_hint
        slot.prev_avg = self._avg_sell_prev_avg
        slot.prev_t = self._avg_sell_prev_t
        slot.baseline_avg = self._avg_sell_baseline_avg
        slot.baseline_t = self._avg_sell_baseline_t
        slot.inflight = self._avg_sell_inflight
        slot.fire_gen = self._avg_sell_fire_gen
        slot.price_floor = self._avg_sell_price_floor
        slot.last_fired_avg = self._avg_sell_last_fired_avg
        slot.fire_is_local = self._avg_sell_fire_is_local
        slot.awaiting_rest = self._avg_sell_awaiting_rest
        slot.rest_correct_gen = self._avg_rest_correct_gen
        slot.pending_avg_correct = self._pending_avg_correct
        slot.local_qty0 = self._local_avg_qty0
        slot.local_avg0 = self._local_avg_avg0
        slot.local_fill_vol = self._local_avg_fill_vol
        slot.local_fill_cost = self._local_avg_fill_cost
        slot.local_uid_vol = self._local_avg_uid_vol
        slot.local_uid_cost = self._local_avg_uid_cost
        slot.local_rest_synced = self._local_avg_rest_synced
        slot.local_max_fill = self._local_avg_max_fill

    def _active_avg_symbols(self):
        return [s for s, sl in self._avg_slots.items() if sl and sl.cb]

    def _clamp_sell_avg(self, avg):
        """경제 평단 — 입력 그대로(>0). max_fill로 부풀리지 않음."""
        try:
            a = float(avg or 0)
        except (TypeError, ValueError):
            a = 0.0
        return a if a > 0 else 0.0

    def safe_local_sell_base(self, avg):
        """로컬 매도 기준가 = 순수 VWAP (수수료 패딩·max_fill 가산 금지).
        손해방지는 place_sell의 min_no_loss_sell_price가 호가에만 적용."""
        a = self._clamp_sell_avg(avg)
        if a > 0:
            return a
        return float(self.compute_local_avg() or 0)

    def cost_floor_price(self):
        """손해 판정용 경제 원가 — VWAP/스냅샷평단만 (최고체결가≠평단)."""
        return max(
            float(self._local_avg_avg0 or 0),
            float(self.compute_local_avg() or 0),
        )

    def _reset_local_avg_ledger(self, qty0=0.0, avg0=0.0):
        """사이클 시작 스냅샷으로 로컬 VWAP 장부 초기."""
        q0 = max(float(qty0 or 0), 0.0)
        a0 = max(float(avg0 or 0), 0.0)
        # 수량만 있고 평단 0이면 잔량 dust로 보고 신규 매수 VWAP만 사용
        if a0 <= 0:
            q0 = 0.0
        self._local_avg_qty0 = q0
        self._local_avg_avg0 = a0
        self._local_avg_fill_vol = 0.0
        self._local_avg_fill_cost = 0.0
        self._local_avg_uid_vol = {}
        self._local_avg_uid_cost = {}
        self._local_avg_max_fill = a0 if a0 > 0 else 0.0

    def compute_local_avg(self):
        """업비트와 동일 VWAP: (avg0*qty0 + Σfill_cost) / (qty0 + Σfill_vol)."""
        q0 = float(self._local_avg_qty0 or 0)
        a0 = float(self._local_avg_avg0 or 0)
        fv = float(self._local_avg_fill_vol or 0)
        fc = float(self._local_avg_fill_cost or 0)
        tot = q0 + fv
        if tot <= 1e-15:
            return 0.0
        return (a0 * q0 + fc) / tot

    def note_local_buy_fill(self, vol, price, uuid=None):
        """체결 반영 → 로컬 평단. 동일 uuid는 수량 증가분만/전체 교체(주문 평균가).
        Returns: 갱신된 local avg."""
        try:
            vol = float(vol or 0)
            price = float(price or 0)
        except (TypeError, ValueError):
            return self.compute_local_avg()
        if vol <= 0 or price <= 0:
            return self.compute_local_avg()
        with self._avg_sell_lock:
            if uuid:
                uid = str(uuid)
                prev_v = float(self._local_avg_uid_vol.get(uid, 0) or 0)
                prev_c = float(self._local_avg_uid_cost.get(uid, 0) or 0)
                if vol + 1e-15 < prev_v:
                    # 감소/재전송 무시
                    return self.compute_local_avg()
                # 주문 전체 기여를 avg_price * executed_volume 으로 교체
                self._local_avg_fill_vol = max(
                    0.0, self._local_avg_fill_vol - prev_v)
                self._local_avg_fill_cost = max(
                    0.0, self._local_avg_fill_cost - prev_c)
                new_c = price * vol
                self._local_avg_uid_vol[uid] = vol
                self._local_avg_uid_cost[uid] = new_c
                self._local_avg_fill_vol += vol
                self._local_avg_fill_cost += new_c
            else:
                self._local_avg_fill_vol += vol
                self._local_avg_fill_cost += price * vol
            if price > 0:
                self._avg_sell_price_floor = max(
                    float(self._avg_sell_price_floor or 0), price)
                self._local_avg_max_fill = max(
                    float(self._local_avg_max_fill or 0), price)
            return self.compute_local_avg()

    def rebuild_local_avg_from_fills(self, fills):
        """executed_orders 등으로 장부 재구성. fills: [{executed_price, volume}| (px,vol)]."""
        with self._avg_sell_lock:
            self._local_avg_fill_vol = 0.0
            self._local_avg_fill_cost = 0.0
            self._local_avg_uid_vol = {}
            self._local_avg_uid_cost = {}
            # max_fill은 스냅샷 평단 이상 유지
            self._local_avg_max_fill = float(self._local_avg_avg0 or 0)
            for i, f in enumerate(fills or ()):
                if isinstance(f, dict):
                    try:
                        px = float(f.get('executed_price') or f.get('price') or 0)
                        vol = float(f.get('volume') or 0)
                    except (TypeError, ValueError):
                        continue
                    uid = f.get('uuid') or f"idx-{i}"
                else:
                    try:
                        px, vol = float(f[0]), float(f[1])
                    except (TypeError, ValueError, IndexError):
                        continue
                    uid = f"idx-{i}"
                if px <= 0 or vol <= 0:
                    continue
                cost = px * vol
                self._local_avg_uid_vol[str(uid)] = vol
                self._local_avg_uid_cost[str(uid)] = cost
                self._local_avg_fill_vol += vol
                self._local_avg_fill_cost += cost
                self._avg_sell_price_floor = max(
                    float(self._avg_sell_price_floor or 0), px)
                self._local_avg_max_fill = max(
                    float(self._local_avg_max_fill or 0), px)
            return self.compute_local_avg()

    def _local_total_vol_hint(self, extra=0.0):
        return max(
            float(extra or 0),
            float(self._local_avg_qty0 or 0) + float(self._local_avg_fill_vol or 0),
            float(self._avg_sell_vol_hint or 0),
        )

    def _schedule_avg_rest_correct(self, symbol):
        """로컬 매도 후 REST 평단으로 캐시·호가 교정 (변동 있을 때만 콜백)."""
        with self._avg_sell_lock:
            self._avg_rest_correct_gen += 1
            gen = self._avg_rest_correct_gen
            baseline = float(self._avg_sell_last_fired_avg
                             or self._avg_sell_baseline_avg or 0)

        def _bg():
            try:
                # sleep 금지 — inflight면 그냥 진행(사후교정). 로컬 POST와 겹치면 place lock이 직렬화.
                # floor=0 — REST 실평단을 max_fill로 부풀리지 않음
                bal, locked, avg = AccountChecker._rest_symbol_info_hot(
                    symbol, baseline=baseline, floor=0.0, max_attempts=6)
                with self._avg_sell_lock:
                    if gen != self._avg_rest_correct_gen:
                        return
                    if self.avg_sell_symbol != symbol or not self.avg_sell_cb:
                        return
                    vol_hint = self._local_total_vol_hint()
                if avg is None or float(avg) <= 0:
                    return
                bal_f = float(max(bal or 0, 0))
                loc_f = float(max(locked or 0, 0))
                avg_f = float(avg)
                self.asset_cache[symbol] = {
                    'balance': bal_f,
                    'locked': loc_f,
                    'avg_buy_price': avg_f,
                }
                self.asset_cache_time = time.monotonic()
                self.asset_event.set()
                # 스냅샷·장부를 서버 평단으로 동기화
                with self._avg_sell_lock:
                    tot = bal_f + loc_f
                    if tot > 0 and avg_f > 0:
                        # 서버 스냅샷으로 장부 리셋 + 이후 rebuild 이중집계 금지
                        self._reset_local_avg_ledger(tot, avg_f)
                        self._local_avg_rest_synced = True
                self._avg_sell_fire_is_local = False
                self._avg_sell_awaiting_rest = True  # REST 교정 콜백 강제
                print_log(LogLevel.INFO,
                          f"REST평단 수신 {avg_f:,.8f} "
                          f"(local_last={baseline:,.8f})")
                self.correct_avg_sell(
                    avg_f, max(bal_f + loc_f, vol_hint))
            except Exception as e:
                print_log(LogLevel.WARNING,
                          f"avg-sell REST correct: {str(e)[:100]}")

        try:
            _AVG_POOL.submit(_bg)
        except Exception:
            try:
                _ORDER_POOL.submit(_bg)
            except Exception:
                run_async(_bg)

    def _schedule_avg_rest_fire(self, symbol):
        """로컬 평단 불가 시에만 — REST로 최초 fire."""
        def _bg():
            try:
                with self._avg_sell_lock:
                    if (not self._avg_sell_armed
                            or self.avg_sell_symbol != symbol
                            or self._avg_sell_inflight):
                        return
                    vol_hint = float(self._avg_sell_vol_hint or 0)
                    baseline = float(self._avg_sell_baseline_avg or 0)
                    gen = int(self._avg_sell_fire_gen)
                bal, locked, avg = AccountChecker._rest_symbol_info_hot(
                    symbol, baseline=baseline, floor=0.0, max_attempts=6)
                with self._avg_sell_lock:
                    if (gen != self._avg_sell_fire_gen
                            or not self._avg_sell_armed
                            or self.avg_sell_symbol != symbol
                            or self._avg_sell_inflight):
                        return
                    vol_hint = max(vol_hint, float(self._avg_sell_vol_hint or 0))
                if avg is None or float(avg) <= 0:
                    return
                bal_f = float(max(bal or 0, 0))
                loc_f = float(max(locked or 0, 0))
                avg_f = self._clamp_sell_avg(float(avg))
                if avg_f <= 0:
                    return
                if bal_f + loc_f <= 0 and vol_hint <= 0:
                    return
                self.asset_cache[symbol] = {
                    'balance': bal_f,
                    'locked': loc_f,
                    'avg_buy_price': avg_f,
                }
                self.asset_cache_time = time.monotonic()
                self.asset_event.set()
                self._avg_sell_fire_is_local = False
                self.force_fire_avg_sell(
                    avg_f, max(bal_f + loc_f, vol_hint))
            except Exception as e:
                print_log(LogLevel.WARNING,
                          f"avg-sell REST hot: {str(e)[:100]}")

        try:
            _AVG_POOL.submit(_bg)
        except Exception:
            try:
                _ORDER_POOL.submit(_bg)
            except Exception:
                run_async(_bg)

    def prefetch_avg_rest(self, symbol=None):
        """사이클/arm 직전 TLS·JWT·accounts Keepalive 예열 (결과 버림/캐시만)."""
        sym = symbol or self.avg_sell_symbol

        def _warm():
            try:
                AccountChecker._rest_fetch_avg_hot(
                    symbol=sym, allow_stale=True, race=False)
            except Exception:
                pass

        try:
            _AVG_POOL.submit(_warm)
        except Exception:
            run_async(_warm)

    def _maybe_fire_avg_from_asset(self, currency, avg_in_msg):
        """myAsset — 평단 필드 드물음. 있으면 교정, 없으면 REST 교정만."""
        if not currency:
            return
        with self._avg_sell_lock:
            slot = self._avg_slots.get(currency)
            want = bool(slot and slot.cb)
            if want:
                self._mirror_slot_to_flat(slot)
        if not want:
            return
        if avg_in_msg:
            info = self.asset_cache.get(currency) or {}
            avg = float(info.get('avg_buy_price', 0) or 0)
            bal = float(info.get('balance', 0) or 0)
            locked = float(info.get('locked', 0) or 0)
            if avg > 0:
                with self._avg_sell_lock:
                    armed = self._avg_sell_armed and not self._avg_sell_inflight
                if armed:
                    self._avg_sell_fire_is_local = False
                    self._try_fire_avg_sell(currency)
                else:
                    self.correct_avg_sell(avg, bal + locked)
                return
        self._schedule_avg_rest_correct(currency)

    def _on_message(self, ws, message):
        """myAsset/myOrder/myTrade 파싱 (업비트/빗썸 공통).
        잔고(myAsset) 갱신 → 대상 심볼 우선 → 평단 fire/REST."""
        try:
            data = json_loads(message)
            # myAsset: upbit flat / SIMPLE{ast:[…]} / bithumb {assets:[...]}
            assets = data.get('assets') or data.get('ast')
            if isinstance(assets, list):
                with self._avg_sell_lock:
                    wants = list(self._active_avg_symbols())
                # 1) 매도 대상 심볼들 먼저 반영 (병렬 워커)
                seen = set()
                for want in wants:
                    for a in assets:
                        if not isinstance(a, dict):
                            continue
                        cu = a.get('currency') or a.get('cu')
                        if cu == want:
                            _, avg_in = self._ingest_asset_item(a)
                            self.asset_cache_time = time.monotonic()
                            self.asset_event.set()
                            self._maybe_fire_avg_from_asset(want, avg_in)
                            seen.add(want)
                            break
                # 2) 나머지 통화 캐시
                for a in assets:
                    self._ingest_asset_item(a)
                self.asset_cache_time = time.monotonic()
                self.asset_event.set()
                return
            # 단일 자산 객체 (일부 포맷)
            if ('currency' in data or 'cu' in data) and (
                    'balance' in data or 'b' in data):
                currency, avg_in_msg = self._ingest_asset_item(data)
                if currency:
                    self.asset_cache_time = time.monotonic()
                    self.asset_event.set()
                    self._maybe_fire_avg_from_asset(currency, avg_in_msg)
                return
            # myOrder — 평단보다 덜 급함. normalize 후 arm.
            order_id_field = EXCHANGE['ws_order_id_field']
            has_id = (
                (order_id_field in data) or ('uuid' in data)
                or ('uid' in data) or ('order_id' in data)
            )
            if has_id and ('state' in data or 's' in data):
                uuid_val = data.get(order_id_field) or order_id_of(data)
                with self._order_lock:
                    prev = self.order_cache.get(uuid_val)
                    cached = normalize_order(data, prev=prev)
                    if uuid_val:
                        cached['uuid'] = uuid_val
                    self.order_cache[uuid_val] = cached
                    state = cached.get('state')
                    if state in ('done', 'cancel', 'trade'):
                        ev = self.order_events.get(uuid_val)
                        if ev:
                            ev.set()
                        self.fills_event.set()
                if cached.get('state') in ('done', 'trade'):
                    self._trigger_trade_callbacks(uuid_val, cached)
                    self._arm_avg_sell_from_order(cached)
                return
            if has_id and (
                'trade_volume' in data or 'trade_quantity' in data
                or 'tq' in data or 'tv' in data
            ):
                uid = data.get(order_id_field) or order_id_of(data)
                cached = normalize_order(data)
                self._trigger_trade_callbacks(uid, cached)
                self._arm_avg_sell_from_order(cached)
                return
        except Exception:
            pass

    def _trigger_trade_callbacks(self, uuid_val, data):
        """myTrade/myOrder 체결 이벤트 → 등록된 콜백 실행."""
        for cb in self.trade_callbacks:
            try:
                cb(uuid_val, data)
            except Exception:
                pass

    def _on_error(self, ws, error):
        self.is_connected = False
        self._last_ws_error = str(error)
        now = time.time()
        if now - self._ws_err_log_at >= 2.0:
            self._ws_err_log_at = now
            print_log(LogLevel.WARNING, f"PrivateWS 에러: {str(error)[:100]}")

    def _on_close(self, ws, close_status, close_msg):
        self.is_connected = False

    def _maybe_resync(self):
        """주기적 REST 동기화 — 핫패스 블로킹 없이 백그라운드 스케줄."""
        now = time.time()
        if now - self._last_resync <= self.RESYNC_INTERVAL:
            return
        with self._resync_lock:
            if self._resync_pending or now - self._last_resync <= self.RESYNC_INTERVAL:
                return
            self._resync_pending = True
            self._last_resync = now

        def _bg():
            try:
                self._seed_assets()
            finally:
                with self._resync_lock:
                    self._resync_pending = False

        run_async(_bg)

    def wait_fill(self, timeout=0.01):
        """myOrder done/cancel 대기 — timeout 초 후 False. 깨어나면 Event 클리어."""
        ev = getattr(self, "fills_event", None) or getattr(self, "fill_event", None)
        if ev is None:
            ev = threading.Event()
            self.fills_event = ev
            self.fill_event = ev
        fired = ev.wait(timeout)
        if fired:
            ev.clear()
        return fired

    def wait_avg_ready(self, symbol, prev_avg=None, prev_time=0.0, timeout=0.2):
        """Buy fill -> wait myAsset avg. Returns (bal, locked, avg)."""
        deadline = time.monotonic() + max(0.02, float(timeout))
        prev_avg = float(prev_avg or 0.0)
        prev_time = float(prev_time or 0.0)
        while time.monotonic() < deadline:
            info = self.asset_cache.get(symbol) or {}
            bal = float(info.get('balance', 0) or 0)
            locked = float(info.get('locked', 0) or 0)
            avg = float(info.get('avg_buy_price', 0) or 0)
            updated = self.asset_cache_time > prev_time + 1e-9
            avg_changed = avg > 0 and abs(avg - prev_avg) > 1e-15
            if avg > 0 and (bal + locked) > 0:
                if updated or avg_changed or prev_avg <= 0:
                    return bal, locked, avg
            remain = deadline - time.monotonic()
            if remain <= 0:
                break
            self.asset_event.wait(min(0.015, remain))
            self.asset_event.clear()
        info = self.asset_cache.get(symbol) or {}
        return (float(info.get('balance', 0) or 0),
                float(info.get('locked', 0) or 0),
                float(info.get('avg_buy_price', 0) or 0))

    def set_avg_sell_target(self, symbol, callback):
        """사이클 시작 — 로컬 VWAP 스냅샷 + 매도 콜백 등록 (심볼별 슬롯)."""
        with self._avg_sell_lock:
            slot = self._ensure_avg_slot(symbol)
            slot.cb = callback
            slot.armed = False
            slot.vol_hint = 0.0
            slot.inflight = False
            slot.price_floor = 0.0
            slot.last_fired_avg = 0.0
            slot.fire_is_local = False
            slot.awaiting_rest = False
            slot.local_rest_synced = False
            slot.fire_gen += 1
            slot.rest_correct_gen += 1
            prev = self.asset_cache.get(symbol) or {}
            bal0 = float(prev.get('balance', 0) or 0)
            loc0 = float(prev.get('locked', 0) or 0)
            avg0 = float(prev.get('avg_buy_price', 0) or 0)
            slot.baseline_avg = avg0
            slot.baseline_t = float(self.asset_cache_time or 0)
            slot.prev_avg = avg0
            slot.prev_t = slot.baseline_t
            slot.reset_ledger(qty0=bal0 + loc0, avg0=avg0)
            self._mirror_slot_to_flat(slot)
        self.prefetch_avg_rest(symbol)

        def _snap():
            try:
                _, info = AccountChecker._rest_fetch_avg_hot(
                    symbol=symbol, allow_stale=True, race=False)
                if not info:
                    return
                with self._avg_sell_lock:
                    slot = self._avg_slots.get(symbol)
                    if not slot or slot.cb is None:
                        return
                    if slot.local_fill_vol > 1e-15:
                        return
                    bal = float(info.get('balance', 0) or 0)
                    loc = float(info.get('locked', 0) or 0)
                    avg = float(info.get('avg_buy_price', 0) or 0)
                    slot.reset_ledger(qty0=bal + loc, avg0=avg)
                    slot.baseline_avg = avg
                    self.asset_cache[symbol] = {
                        'balance': bal, 'locked': loc, 'avg_buy_price': avg,
                    }
                    self.asset_cache_time = time.monotonic()
                    if self.avg_sell_symbol == symbol:
                        self._mirror_slot_to_flat(slot)
            except Exception:
                pass

        try:
            _AVG_POOL.submit(_snap)
        except Exception:
            run_async(_snap)

    def clear_avg_sell_target(self, symbol=None):
        """symbol 지정 시 해당 슬롯만, 없으면 전체 해제."""
        with self._avg_sell_lock:
            if symbol:
                slot = self._avg_slots.pop(symbol, None)
                if slot:
                    slot.clear_trading()
                if self.avg_sell_symbol == symbol:
                    self.avg_sell_symbol = None
                    self.avg_sell_cb = None
                    active = self._active_avg_symbols()
                    if active:
                        self._mirror_slot_to_flat(self._avg_slots[active[0]])
                    else:
                        self._avg_sell_armed = False
                        self._avg_sell_inflight = False
                        self._reset_local_avg_ledger(0.0, 0.0)
                return
            for sl in list(self._avg_slots.values()):
                sl.clear_trading()
            self._avg_slots.clear()
            self.avg_sell_symbol = None
            self.avg_sell_cb = None
            self._avg_sell_armed = False
            self._avg_sell_vol_hint = 0.0
            self._avg_sell_inflight = False
            self._avg_sell_baseline_avg = 0.0
            self._avg_sell_baseline_t = 0.0
            self._avg_sell_price_floor = 0.0
            self._avg_sell_last_fired_avg = 0.0
            self._avg_sell_last_fired_vol = 0.0
            self._avg_sell_fire_is_local = False
            self._avg_sell_awaiting_rest = False
            self._local_avg_rest_synced = False
            self._avg_sell_fire_gen += 1
            self._avg_rest_correct_gen += 1
            self._reset_local_avg_ledger(0.0, 0.0)

    def arm_avg_sell(self, vol_hint=0.0, fill_price=0.0, fill_uuid=None,
                     symbol=None):
        """매수 체결 → 로컬 VWAP 즉시 매도 + REST 교정 예약.
        REST 평단 대기 없이 체결 데이터로 먼저 호가.
        ★ fill_uuid 없는 재arm은 장부 가산 금지(수량 2배 폭증 원흉)."""
        with self._avg_sell_lock:
            sym = symbol or self.avg_sell_symbol
            slot = self._avg_slots.get(sym) if sym else None
            if slot and slot.cb:
                self._mirror_slot_to_flat(slot)
            if not self.avg_sell_cb or not sym:
                return False
            self.avg_sell_symbol = sym
            self._avg_sell_prev_avg = float(self._avg_sell_baseline_avg or 0)
            self._avg_sell_prev_t = float(self._avg_sell_baseline_t or 0)
            self._avg_sell_armed = True
        try:
            fp = float(fill_price or 0)
        except (TypeError, ValueError):
            fp = 0.0
        try:
            vh = float(vol_hint or 0)
        except (TypeError, ValueError):
            vh = 0.0
        info = self.asset_cache.get(sym) or {}
        bal = float(info.get('balance', 0) or 0)
        locked = float(info.get('locked', 0) or 0)
        held = max(0.0, bal) + max(0.0, locked)

        local = 0.0
        # ★ 실제 체결(uuid)만 장부 반영. 재arm/재시도는 가산 금지.
        if fp > 0 and vh > 0 and fill_uuid:
            local = self.note_local_buy_fill(vh, fp, uuid=fill_uuid)
        else:
            local = self.compute_local_avg()
            if local <= 0 and fp > 0:
                local = fp
        local = float(local or 0) or float(self.compute_local_avg() or 0)

        ledger = (float(self._local_avg_qty0 or 0)
                  + float(self._local_avg_fill_vol or 0))
        # ★ 매도 수량 = 실보유 우선. 없으면 장부/힌트 (힌트 눈덩이 금지)
        if held >= float(MIN_HOLDING_VOLUME):
            vol = held
        else:
            vol = max(vh, ledger)
        with self._avg_sell_lock:
            if held >= float(MIN_HOLDING_VOLUME):
                self._avg_sell_vol_hint = held
            else:
                self._avg_sell_vol_hint = max(
                    float(self._avg_sell_vol_hint or 0), float(vol or 0))

        fired = False
        if local > 0 and vol > 0:
            prev = self.asset_cache.get(sym) or {}
            self.asset_cache[sym] = {
                'balance': float(prev.get('balance', bal) or bal),
                'locked': float(prev.get('locked', locked) or locked),
                'avg_buy_price': local,
            }
            self.asset_cache_time = time.monotonic()
            self._avg_sell_fire_is_local = True
            self._avg_sell_awaiting_rest = True
            with self._avg_sell_lock:
                already = float(self._avg_sell_last_fired_avg or 0) > 0
                last_avg = float(self._avg_sell_last_fired_avg or 0)
                last_vol = float(getattr(self, '_avg_sell_last_fired_vol', 0) or 0)
                inflight = bool(self._avg_sell_inflight)
            same_avg = (
                last_avg > 0
                and abs(local - last_avg) / max(last_avg, 1e-12) < 3e-5)
            vol_grew = False
            try:
                vol_grew = (
                    float(vol) > float(last_vol) + 1e-12
                    and (float(vol) - float(last_vol)) * float(local)
                    >= float(MIN_ORDER_AMOUNT))
            except (TypeError, ValueError):
                vol_grew = False
            last_fire_t = float(getattr(self, '_avg_sell_last_fire_t', 0) or 0)
            too_soon = (time.time() - last_fire_t) < 2.0
            if same_avg and already and (not vol_grew or too_soon):
                fired = True  # 이미 매도 경로 동작 중
            elif inflight and same_avg and not vol_grew:
                fired = True
            else:
                fired = self.force_fire_avg_sell(local, vol)
                if fired:
                    with self._avg_sell_lock:
                        self._avg_sell_last_fired_vol = float(vol or 0)
                        self._avg_sell_last_fire_t = time.time()
            if fired and (not (same_avg and already) or (vol_grew and not too_soon)):
                print_log(LogLevel.INFO,
                          f"로컬평단 매도 {'수량갱신' if vol_grew else ('갱신' if already else 'fire')} "
                          f"vwap={local:,.8f} vol={float(vol):.8f} "
                          f"(qty0={self._local_avg_qty0:.6f}+"
                          f"fills={self._local_avg_fill_vol:.6f})")
        # REST로 서버 평단 확정·호가 교정
        self._schedule_avg_rest_correct(sym)
        if not fired:
            self._schedule_avg_rest_fire(sym)
        with self._avg_sell_lock:
            slot = self._avg_slots.get(sym)
            if slot:
                self._mirror_flat_to_slot(slot)
        return fired

    def _arm_avg_sell_from_order(self, order):
        """myOrder 매수 체결 → 메인루프보다 먼저 arm (WS 스레드, non-blocking)."""
        try:
            if not isinstance(order, dict):
                return
            order = normalize_order(order)
            uid = order.get('uuid')
            if not uid or uid not in buy_uuids:
                return
            side = normalize_side(
                order.get('side') or order.get('ask_bid') or order.get('ab'))
            if side != 'bid':
                return
            market = order.get('market') or order.get('code') or ''
            with self._avg_sell_lock:
                targets = self._active_avg_symbols()
            if not targets:
                return
            sym = None
            for t in targets:
                if (not market) or market == f'KRW-{t}':
                    sym = t
                    break
            if not sym:
                return
            state = str(order.get('state', '') or '').lower()
            if not (order_is_filled(order) or state == 'trade'):
                return
            vol = order_executed_volume(order)
            funds = order_executed_funds(order)
            fill_px = 0.0
            if vol > 0 and funds > 0:
                fill_px = funds / vol
            if fill_px <= 0:
                try:
                    fill_px = float(
                        order.get('avg_price') or order.get('ap')
                        or order.get('price') or order.get('p') or 0)
                except (TypeError, ValueError):
                    fill_px = 0.0
            self.arm_avg_sell(
                vol_hint=vol, fill_price=fill_px, fill_uuid=uid, symbol=sym)
        except Exception:
            pass

    def _try_fire_avg_sell(self, currency=None):
        """myAsset 갱신 또는 arm 직후 — 평단 준비되면 주문 풀에서 매도 콜백."""
        with self._avg_sell_lock:
            if (not self._avg_sell_armed or not self.avg_sell_cb
                    or not self.avg_sell_symbol or self._avg_sell_inflight):
                return False
            sym = self.avg_sell_symbol
            if currency and currency != sym:
                return False
            info = self.asset_cache.get(sym) or {}
            bal = float(info.get('balance', 0) or 0)
            locked = float(info.get('locked', 0) or 0)
            avg = float(info.get('avg_buy_price', 0) or 0)
            avg = self._clamp_sell_avg(avg)
            if avg <= 0 or (bal + locked) <= 0:
                return False
            floor = float(self._avg_sell_price_floor or 0)
            if floor > 0 and avg + 1e-12 < floor:
                return False
            self._avg_sell_armed = False
            self._avg_sell_inflight = True
            gen = self._avg_sell_fire_gen
            cb = self.avg_sell_cb
            vol = max(bal + locked, float(self._avg_sell_vol_hint or 0))
            avg_f = avg
            # 다음 체결을 위해 기준선 갱신
            self._avg_sell_baseline_avg = avg_f
            self._avg_sell_baseline_t = float(self.asset_cache_time or 0)
            self._avg_sell_prev_avg = avg_f
            self._avg_sell_prev_t = self._avg_sell_baseline_t
            self._avg_sell_vol_hint = 0.0

        def _run():
            try:
                cb(avg_f, vol)
            except Exception as e:
                print_log(LogLevel.WARNING, f"avg-sell hot fire: {str(e)[:100]}")
            finally:
                pending = None
                with self._avg_sell_lock:
                    if gen == self._avg_sell_fire_gen:
                        self._avg_sell_inflight = False
                        pending = getattr(self, '_pending_avg_correct', None)
                        self._pending_avg_correct = None
                if pending:
                    try:
                        self.correct_avg_sell(pending[0], pending[1])
                    except Exception:
                        pass

        # 비동기 매도 — 매수 사다리를 막지 않음
        try:
            _ORDER_POOL.submit(_run)
        except Exception:
            run_async(_run)
        return True

    def force_fire_avg_sell(self, avg, vol):
        """최초 매도 fire — arm 필요. 체결가 하한 clamp 적용."""
        with self._avg_sell_lock:
            if not self.avg_sell_cb or self._avg_sell_inflight:
                return False
            avg_f = self._clamp_sell_avg(avg)
            if avg_f <= 0:
                return False
            if not self._avg_sell_armed and self._avg_sell_last_fired_avg <= 0:
                # 미arm·미소화 — REST safety 등에서 avg만 있으면 허용
                pass
            elif not self._avg_sell_armed:
                return False
            self._avg_sell_armed = False
            self._avg_sell_inflight = True
            gen = self._avg_sell_fire_gen
            cb = self.avg_sell_cb
            # ★ vol 눈덩이 금지 — 실보유 있으면 그걸로 고정
            info = self.asset_cache.get(self.avg_sell_symbol) or {}
            held = (max(float(info.get('balance', 0) or 0), 0.0)
                    + max(float(info.get('locked', 0) or 0), 0.0))
            ledger = (float(self._local_avg_qty0 or 0)
                      + float(self._local_avg_fill_vol or 0))
            if held >= float(MIN_HOLDING_VOLUME):
                vol_f = held
            else:
                vol_f = max(float(vol or 0), ledger,
                            float(self._avg_sell_vol_hint or 0))
            self._avg_sell_baseline_avg = avg_f
            self._avg_sell_baseline_t = float(self.asset_cache_time or 0)
            self._avg_sell_last_fired_avg = avg_f
            self._avg_sell_vol_hint = 0.0
            is_local = bool(self._avg_sell_fire_is_local)

        def _run():
            try:
                if avg_f > 0 and vol_f > 0:
                    self._avg_sell_fire_is_local = is_local
                    cb(avg_f, vol_f)
            except Exception as e:
                print_log(LogLevel.WARNING, f"avg-sell force fire: {str(e)[:100]}")
            finally:
                pending = None
                with self._avg_sell_lock:
                    if gen == self._avg_sell_fire_gen:
                        self._avg_sell_inflight = False
                        pending = getattr(self, '_pending_avg_correct', None)
                        self._pending_avg_correct = None
                if pending:
                    try:
                        self.correct_avg_sell(pending[0], pending[1])
                    except Exception:
                        pass

        # 비동기 매도 — 매수 사다리 POST를 REST/매도에 막지 않음
        try:
            _ORDER_POOL.submit(_run)
        except Exception:
            run_async(_run)
        return True

    def correct_avg_sell(self, avg, vol):
        """REST/후속 평단 교정.
        - awaiting_rest: 서버 평단이 로컬과 다르면 상·하향 모두 콜백.
        - 그 외: 상향만."""
        with self._avg_sell_lock:
            if not self.avg_sell_cb:
                return False
            self._avg_sell_fire_is_local = False
            try:
                avg_f = float(avg or 0)
            except (TypeError, ValueError):
                avg_f = 0.0
            if avg_f <= 0:
                return False
            last = float(self._avg_sell_last_fired_avg or 0)
            awaiting = bool(self._avg_sell_awaiting_rest)
            # 아주 작은 차이도 호가틱이 바뀔 수 있음(저가코인) — 1e-12 절대/상대
            differs = (last <= 0) or (
                abs(avg_f - last) > max(last * 1e-8, 1e-12))
            last_vol = float(getattr(self, '_avg_sell_last_fired_vol', 0) or 0)
            vol_f_pre = max(float(vol or 0), self._local_total_vol_hint())
            # 실보유 있으면 그걸로 cap (재arm 폭증 방지)
            try:
                info = self.asset_cache.get(self.avg_sell_symbol) or {}
                held = (max(float(info.get('balance', 0) or 0), 0.0)
                        + max(float(info.get('locked', 0) or 0), 0.0))
                if held >= float(MIN_HOLDING_VOLUME):
                    vol_f_pre = held
            except Exception:
                pass
            vol_grew = (
                vol_f_pre > last_vol + 1e-12
                and (vol_f_pre - last_vol) * avg_f >= float(MIN_ORDER_AMOUNT))
            if not differs and not vol_grew:
                self._avg_sell_awaiting_rest = False
                return False
            if (not differs and vol_grew):
                # 평단 동일·수량만 증가 → 합산 매도
                pass
            elif not awaiting and last > 0 and avg_f <= last * 1.00005:
                return False
            if self._avg_sell_inflight:
                # sleep 재시도 금지 — 현재 fire 종료 직후 즉시 교정
                self._pending_avg_correct = (avg_f, float(vol or 0))
                return True
            gen = self._avg_sell_fire_gen
            cb = self.avg_sell_cb
            vol_f = vol_f_pre
            self._avg_sell_inflight = True
            self._avg_sell_last_fired_avg = avg_f
            self._avg_sell_last_fired_vol = vol_f
            self._avg_sell_baseline_avg = avg_f
            self._avg_sell_awaiting_rest = False

        def _run():
            try:
                self._avg_sell_fire_is_local = False
                print_log(LogLevel.INFO,
                          f"평단교정 콜백 REST={avg_f:,.8f} (prev={last:,.8f})")
                cb(avg_f, vol_f)
            except Exception as e:
                print_log(LogLevel.WARNING, f"avg-sell correct: {str(e)[:100]}")
            finally:
                pending = None
                with self._avg_sell_lock:
                    if gen == self._avg_sell_fire_gen:
                        self._avg_sell_inflight = False
                        pending = getattr(self, '_pending_avg_correct', None)
                        self._pending_avg_correct = None
                if pending:
                    try:
                        self.correct_avg_sell(pending[0], pending[1])
                    except Exception:
                        pass

        try:
            _ORDER_POOL.submit(_run)
        except Exception:
            run_async(_run)
        return True
    # ===== 공개 조회 API (캐시에서 O(1) 반환) =====

    def get_symbol_info(self, symbol):
        """(balance, locked, avg_buy_price) 반환.
        핫패스: WS 캐시만 사용 (미스=0). REST는 _rest_symbol_info / fresh 경로만.
        매 루프 REST를 치면 429 → '잔고 조회 실패' 스팸이 난다."""
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
    """ticker/all 1회 기반 변동성 스캐너 (전종목 캔들 REST 금지 → rate limit 회피).

    1차 점수 = (당일고가−당일저가)/현재가  (없으면 |signed_change_rate|)
    + 24h 거래대금 ≥ VOLUME_THRESHOLD_M
    주기적으로 ticker/all 만 갱신. HybridMA 시드는 상위 후보만 cycle_runner가 수행."""

    CANDLE_COUNT = 20  # 레거시 호환 (더 이상 전종목 시드 안 함)
    VOLUME_THRESHOLD_M = 5000  # 백만원 = 50억 (config로 덮어씀)
    TICKER_REFRESH_S = 15.0

    def __init__(self):
        try:
            from .config import VOLUME_THRESHOLD_M, TICKER_RANK_REFRESH_S
            self.VOLUME_THRESHOLD_M = float(VOLUME_THRESHOLD_M)
            self.TICKER_REFRESH_S = float(TICKER_RANK_REFRESH_S)
        except Exception:
            pass
        self.candle_buffers = {}   # 레거시 호환 (비어 있을 수 있음)
        self.volume_1h = {}        # {symbol: 24h 거래대금(백만원)}
        self.ticker_snap = {}      # {symbol: {price, high, low, change_rate, range_pct, vol_m}}
        self.symbols = []
        self.ws = None
        self.thread = None
        self._bg_thread = None
        self.is_running = False
        self._should_reconnect = False  # ticker 폴링 모드 — candle WS 기본 OFF
        self._connect_gen = 0
        self._last_ws_error = ''
        self._ws_err_log_at = 0.0
        self._bg_stop = threading.Event()
        self._lock = threading.RLock()
        self._last_top_log_ts = 0.0
        self._last_refresh_ts = 0.0

    def start(self, symbols):
        """ticker/all 1회 시드 + 백그라운드 주기 갱신. 전종목 캔들 REST 없음."""
        self.symbols = [str(s).upper() for s in (symbols or []) if s]
        print_log(
            LogLevel.INFO,
            f"VolatilityScanner 시작 — ticker/all 랭킹 "
            f"(후보풀={len(self.symbols)}, 갱신={self.TICKER_REFRESH_S:.0f}s)")
        n = self._refresh_tickers()
        print_log(
            LogLevel.SUCCESS,
            f"ticker/all seed 완료 — {n}개 스냅샷 "
            f"(유동성≥{int(self.VOLUME_THRESHOLD_M)}M)")
        self.is_running = True
        self._bg_stop.clear()
        if self._bg_thread and self._bg_thread.is_alive():
            pass
        else:
            self._bg_thread = threading.Thread(
                target=self._ticker_bg_loop, name='vol-ticker-bg', daemon=True)
            self._bg_thread.start()

    def stop(self):
        self.is_running = False
        self._bg_stop.set()
        self._should_reconnect = False
        self._connect_gen += 1
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

    def _ticker_bg_loop(self):
        while not self._bg_stop.wait(self.TICKER_REFRESH_S):
            if not self.is_running:
                break
            try:
                self._refresh_tickers()
            except Exception:
                pass

    def _fetch_ticker_all(self):
        """KRW 전종목 ticker 1회. 실패 시 None."""
        tickers = None
        try:
            if EXCHANGE.get('supports_ticker_all') and EXCHANGE.get('ticker_all_endpoint'):
                def ticker_all_call():
                    r = http_get(
                        SERVER_URL + EXCHANGE['ticker_all_endpoint'],
                        params={"quote_currencies": "KRW"},
                        timeout=HTTP_TIMEOUT_SLOW, slow=True)
                    return response_json(r)
                tickers = safe_api_call(ticker_all_call)
        except Exception as e:
            print_log(LogLevel.WARNING, f"ticker/all 실패: {str(e)[:100]}")
            tickers = None
        if tickers:
            return tickers
        # 폴백: 심볼 배치 (가능하면 피함)
        if not self.symbols:
            return None
        try:
            markets_param = ",".join(f"KRW-{s}" for s in self.symbols[:100])
            def ticker_call():
                r = http_get(TICKER_URL, params={"markets": markets_param},
                             timeout=HTTP_TIMEOUT_SLOW, slow=True)
                return response_json(r)
            return safe_api_call(ticker_call)
        except Exception:
            return None

    @staticmethod
    def _range_score(high, low, price, change_rate):
        """1차 변동성 점수. 당일 고저폭 우선, 없으면 |등락률|."""
        try:
            high = float(high or 0)
            low = float(low or 0)
            price = float(price or 0)
            cr = abs(float(change_rate or 0))
        except (TypeError, ValueError):
            return 0.0
        if price > 0 and high >= low and high > 0:
            rng = (high - low) / price
            if rng > 0:
                return rng
        return cr if cr > 0 else 0.0

    def _refresh_tickers(self):
        """ticker/all → volume/range 스냅샷. 반환=갱신 심볼 수."""
        tickers = self._fetch_ticker_all()
        if not tickers:
            return 0
        wanted = set(self.symbols) if self.symbols else None
        snap = {}
        vol_map = {}
        for t in tickers:
            code = t.get('market', '')
            if not code.startswith('KRW-'):
                continue
            sym = code[4:].upper()
            if wanted is not None and sym not in wanted:
                continue
            try:
                price = float(t.get('trade_price') or 0)
                high = float(t.get('high_price') or 0)
                low = float(t.get('low_price') or 0)
                cr = float(t.get('signed_change_rate')
                           if t.get('signed_change_rate') is not None
                           else (t.get('change_rate') or 0))
                vol_m = float(t.get('acc_trade_price_24h') or 0) / 1_000_000.0
            except (TypeError, ValueError):
                continue
            if price <= 0:
                continue
            score = self._range_score(high, low, price, cr)
            snap[sym] = {
                'price': price,
                'high': high,
                'low': low,
                'change_rate': cr,
                'range_pct': score,
                'vol_m': vol_m,
            }
            vol_map[sym] = vol_m
        with self._lock:
            self.ticker_snap = snap
            self.volume_1h = vol_map
            self._last_refresh_ts = time.time()
        return len(snap)

    # ── 레거시 스텁 (호출부 호환) ──
    def _seed_one(self, symbol):
        return (symbol, None)

    def _seed_candles(self):
        return self._refresh_tickers()

    def _connect_loop(self, gen):
        return

    def _on_open(self, ws):
        pass

    def _on_message(self, ws, message):
        pass

    def _on_error(self, ws, error):
        pass

    def _on_close(self, ws, close_status, close_msg):
        pass

    def _calc_volatility(self, symbol):
        """ticker range_pct (레거시 이름 유지)."""
        with self._lock:
            ent = self.ticker_snap.get(str(symbol).upper())
        if not ent:
            return 0.0
        return float(ent.get('range_pct') or 0.0)

    def get_top_volatility_symbols(self, n=20, excluded_symbols=None):
        """ticker/all 변동성(고저폭) 내림차순 Top n.
        Returns: list[(symbol, score, price)]
        필터: 호가제외구간, 거래대금, excluded."""
        if excluded_symbols is None:
            excluded = set()
        else:
            excluded = {str(s).upper() for s in excluded_symbols}
        n = max(1, int(n or 1))
        # 너무 오래됐으면 동기 1회 갱신 (핫패스 드묾)
        if time.time() - float(self._last_refresh_ts or 0) > self.TICKER_REFRESH_S * 2:
            try:
                self._refresh_tickers()
            except Exception:
                pass
        with self._lock:
            items = list(self.ticker_snap.items())
        candidates = []
        for sym, ent in items:
            su = str(sym).upper()
            if su in excluded:
                continue
            price = float(ent.get('price') or 0)
            if price <= 0:
                continue
            if UpbitTickSystem.is_excluded_tick_range(price):
                continue
            vol_m = float(ent.get('vol_m') or self.volume_1h.get(su, 0) or 0)
            if vol_m < self.VOLUME_THRESHOLD_M:
                continue
            score = float(ent.get('range_pct') or 0)
            if score <= 0:
                continue
            candidates.append((su, score, price))
        if not candidates:
            return []
        candidates.sort(key=lambda x: x[1], reverse=True)
        top = candidates[:n]
        now = time.time()
        if now - float(self._last_top_log_ts or 0) >= 30.0:
            self._last_top_log_ts = now
            for i, (sym, score, price) in enumerate(top[:5]):
                print_log(
                    LogLevel.INFO,
                    f"  Vol Top{i+1}: {sym} range={score*100:.2f}% @{price:,.4f} "
                    f"vol24h={self.volume_1h.get(sym, 0):,.0f}M")
        return top

    def get_top_volatility_symbol(self, excluded_symbols=None):
        """필터 통과한 코인 중 변동성 최고 심볼 반환 (하위 호환)."""
        top = self.get_top_volatility_symbols(1, excluded_symbols=excluded_symbols)
        return top[0][0] if top else None


# 전역 trade(체결가) 스트림 싱글톤 — 60틱 MA60 매수 게이트용.
# 모듈 임포트 시점에 생성하되 WS 연결은 subscribe(symbol) 호출 전까지 대기.
trade_ws = TradeTickStream() if WEBSOCKET_AVAILABLE else None
# AUTO_SELECT 스캐너 — main에서 start 후 할당
volatility_scanner = None


class RealMarketData:
    # 웹소켓 싱글톤 (라이브러리 사용 가능 시만)
    _ws = UpbitWebSocket() if WEBSOCKET_AVAILABLE else None
    _rest_ticker_cache = {}  # {symbol: (price, ts)}
    _REST_TICKER_TTL = 1.0
    _PRICE_SANE_RATIO = 0.25  # 심볼 시세와 25% 이상 괴리 = 오염

    @staticmethod
    def _rest_ticker(symbol, ttl=None):
        """심볼별 REST ticker (짧은 캐시). 오염 검증/폴백용."""
        if not symbol:
            return None
        su = str(symbol).upper()
        ttl = RealMarketData._REST_TICKER_TTL if ttl is None else ttl
        ent = RealMarketData._rest_ticker_cache.get(su)
        now = time.time()
        if ent and (now - ent[1]) < ttl:
            return ent[0]
        try:
            def api_call():
                url = f"{TICKER_URL}?markets=KRW-{su}"
                headers = {"Accept": "application/json"}
                response = http_get(url, headers=headers, timeout=HTTP_TIMEOUT_SLOW,
                                    slow=True)
                if response.status_code == 200:
                    data = response_json(response)
                    if data and len(data) > 0:
                        return float(data[0]['trade_price'])
                elif response.status_code == 429:
                    print_log(LogLevel.WARNING, "API rate limit exceeded")
                    return None
                else:
                    print_log(LogLevel.WARNING,
                              f"Failed to get current price: {response.status_code}")
                    return None

            px = safe_api_call(api_call)
            if px and px > 0:
                RealMarketData._rest_ticker_cache[su] = (px, now)
            return px
        except Exception as e:
            print_log(LogLevel.EXCEPTION, f"Error getting current price: {str(e)}")
            return None

    @staticmethod
    def sanitize_symbol_price(symbol, price, *, force_rest=False):
        """다른 코인 시세 오염 차단 — 기준가와 25%+ 괴리면 ref로 교체.
        핫패스: CRIX pending만 비교 (REST 없음).
        force_rest=True: REST ticker로 최종 검증 (핫패스 매수/매도는 False — WS만).
        Returns: (clean_price, contaminated: bool)"""
        if not symbol or not price or price <= 0:
            return None, False
        su = str(symbol).upper()
        price = float(price)
        ref = None
        if trade_ws:
            pending = getattr(trade_ws, '_pending_close', {}).get(su)
            try:
                if pending and float(pending) > 0:
                    ref = float(pending)
            except (TypeError, ValueError):
                pass
        if force_rest or ref is None:
            # force_rest이거나 pending 없을 때만 REST (매수 직전)
            if force_rest:
                rest = RealMarketData._rest_ticker(su)
                if rest and rest > 0:
                    ref = rest
        if ref and ref > 0:
            if abs(price - ref) / ref > RealMarketData._PRICE_SANE_RATIO:
                print_log(
                    LogLevel.ERROR,
                    f"PRICE CONTAMINATION {su}: got={price} ref={ref} "
                    f"— 다른코인 시세 의심, ref 사용")
                return ref, True
        return price, False

    @staticmethod
    def get_current_price(symbol):
        # 1) trade 스트림 체결가 우선 — 다중 심볼 동시 구독 시 심볼별 최신가가
        #    ticker(단일 구독)에 없어도 스탑로스/게이트가 올바른 심볼 가격을 씀.
        if not symbol:
            return None
        su = str(symbol).upper()
        if trade_ws:
            last = trade_ws.get_last_price(su)
            if last is not None:
                clean, _bad = RealMarketData.sanitize_symbol_price(su, last)
                return clean if clean else last
        # 2) ticker 웹소켓 캐시 (핫 루프 — API 호출 없음)
        if RealMarketData._ws:
            cached = RealMarketData._ws.get_price(su)
            if cached is not None:
                clean, _bad = RealMarketData.sanitize_symbol_price(su, cached)
                return clean if clean else cached
        # 3) 폴백: REST (웹소켓 미가동/캐시 만료 시)
        return RealMarketData._rest_ticker(su)

    @staticmethod
    def subscribe_websocket(symbol):
        """ticker WS — trade 스트림이 체결가 제공 시 생략 (공개 WS 1개 절약)."""
        if trade_ws is not None:
            return
        if RealMarketData._ws:
            RealMarketData._ws.subscribe(symbol)

    @staticmethod
    def subscribe_trade_stream(symbol):
        """체결가(trade) 스트림 — 단일 심볼을 기존 다중 구독에 추가 (덮어쓰기 금지).
        1분 MA 시드도 함께 (하이브리드 게이트)."""
        if trade_ws and symbol:
            trade_ws.ensure_symbols([str(symbol).upper()])
        try:
            if minute_ma_cache is not None and symbol:
                minute_ma_cache.ensure(symbol)
        except Exception:
            pass

    @staticmethod
    def subscribe_trade_stream_symbols(symbols):
        """체결가(trade) 스트림 다중 심볼 동시 구독 — 기존 구독에 합집합.
        1분 MA 시드도 함께."""
        cleaned = [str(s).upper() for s in (symbols or []) if s]
        if trade_ws and cleaned:
            trade_ws.ensure_symbols(cleaned)
        try:
            if minute_ma_cache is not None and cleaned:
                minute_ma_cache.ensure(cleaned)
        except Exception:
            pass

    @staticmethod
    def compute_hybrid_ma(symbol):
        """틱 MA60 + 1분 MA60 → 괴리 적응형 합성.
        Returns: (hybrid|None, info: dict) — 핫패스 REST 없음."""
        try:
            from .config import HYBRID_MA_DIV_K
            k = float(HYBRID_MA_DIV_K)
        except Exception:
            k = 8.0
        info = {
            'ma_tick': None, 'ma_min': None, 'hybrid': None,
            'ma60': None, 'ma25': None, 'ma20': None,
            'w_tick': None, 'w_min': None, 'div': None,
            'last_price': None, 'candle_count': 0, 'tick_progress': 0,
            'min_candle_count': 0,
        }
        if trade_ws is None:
            return None, info
        ma_tick = trade_ws.get_ma60(symbol)
        ma_min = None
        try:
            if minute_ma_cache is not None:
                ma_min = minute_ma_cache.get_ma60(symbol)
                info['min_candle_count'] = minute_ma_cache.get_buffer_size(symbol)
        except Exception:
            ma_min = None
        info['ma_tick'] = ma_tick
        info['ma_min'] = ma_min
        info['candle_count'] = trade_ws.get_buffer_size(symbol)
        info['tick_progress'] = trade_ws.get_tick_progress(symbol)
        if ma_tick is None or ma_min is None or ma_min <= 0:
            return None, info
        div = abs(float(ma_tick) - float(ma_min)) / float(ma_min)
        # 정렬→0.5/0.5, 괴리↑→분 가중↑ (틱 최소 0.2)
        w_tick = 0.5 - k * div
        if w_tick < 0.2:
            w_tick = 0.2
        elif w_tick > 0.8:
            w_tick = 0.8
        w_min = 1.0 - w_tick
        hybrid = w_tick * float(ma_tick) + w_min * float(ma_min)
        info['div'] = div
        info['w_tick'] = w_tick
        info['w_min'] = w_min
        info['hybrid'] = hybrid
        info['ma60'] = hybrid
        info['ma25'] = hybrid
        info['ma20'] = hybrid
        return hybrid, info

    @staticmethod
    def check_tick_ma_gate(symbol):
        """하이브리드 MA 매수 진입 게이트 (틱 MA60 + 1분 MA60).
          - 분봉 MA = hard filter (합의/세력스파이크 차단)
          - hybrid = 괴리 적응형 합성 (정렬 시 ≈1:1)
          - 허용: price < ma_min AND price < hybrid
          - 어느 한쪽 MA 미준비 → 대기 (False)
        Returns: (allowed: bool, info: dict)."""
        hybrid, info = RealMarketData.compute_hybrid_ma(symbol)
        last_price = RealMarketData.get_current_price(symbol)
        info['last_price'] = last_price
        if trade_ws is None:
            return False, info
        if hybrid is None:
            # 미시드면 ensure만 걸고 대기 (핫패스 REST 없음 — bg/seed가 채움)
            try:
                if minute_ma_cache is not None and symbol:
                    minute_ma_cache.ensure(symbol)
            except Exception:
                pass
            return False, info
        if last_price is None:
            return False, info
        ma_min = info.get('ma_min')
        if ma_min is None:
            return False, info
        return (last_price < ma_min and last_price < hybrid), info

    @staticmethod
    def select_first_tradable_symbol(symbols):
        """다중 심볼 폴백 — 주어진 심볼 리스트를 기재 순서대로 순회하며
        첫 번째로 하이브리드 MA 게이트를 통과하는(매수 가능한) 심볼을 반환.
        게이트 판정은 check_tick_ma_gate()와 동일.
        단, 게이트 산출이 불가능한 심볼(trade_ws 미가용 등)은 통과로 간주하여
        기존 단일 심볼 동작을 보존.
        Returns: (selected_symbol: str|None, tried: list[(symbol, allowed, reason)])"""
        tried = []
        # trade_ws 자체가 없으면 게이트를 쓸 수 없음 → 첫 심볼을 그대로 반환 (기존 동작 보존)
        if trade_ws is None:
            if symbols:
                tried.append((symbols[0], True, 'trade_ws 미가용 — 게이트 우회'))
                return symbols[0], tried
            return None, tried

        need = int(getattr(trade_ws, 'MA_PERIOD', 60) or 60)
        for sym in symbols:
            allowed, info = RealMarketData.check_tick_ma_gate(sym)
            ma_tick = info.get('ma_tick')
            ma_min = info.get('ma_min')
            hybrid = info.get('hybrid') or info.get('ma60')
            last_price = info.get('last_price')
            if ma_tick is None:
                tried.append(
                    (sym, False,
                     f'틱캔들 부족 ({info.get("candle_count", 0)}/{need})'))
                continue
            if ma_min is None:
                tried.append(
                    (sym, False,
                     f'1분캔들 부족 ({info.get("min_candle_count", 0)}/'
                     f'{getattr(minute_ma_cache, "PERIOD", 60) if minute_ma_cache else 60})'))
                continue
            if last_price is None:
                tried.append((sym, False, '현재가 조회 실패'))
                continue
            if allowed:
                tried.append(
                    (sym, True,
                     f'now < HybridMA ({last_price:.4f} < {hybrid:.4f} '
                     f'tick={ma_tick:.4f} min={ma_min:.4f})'))
                return sym, tried
            tried.append(
                (sym, False,
                 f'now ≥ HybridMA/min ({last_price:.4f} hybrid={hybrid:.4f} '
                 f'min={ma_min:.4f})'))

        # 전원 폴백 불가 — 첫 심볼을 반환하고 매수 게이트는 메인 루프에서 대기 처리.
        if symbols:
            return symbols[0], tried
        return None, tried

def safe_api_call(func, *args, **kwargs):
    """일반 REST — 429/네트워크 시 소수 회만 즉시 재시도 (주문 핫패스 금지)."""
    max_spin = 5
    attempt = 0
    while True:
        try:
            result = func(*args, **kwargs)
            if _response_rate_limited(result):
                attempt += 1
                if attempt >= max_spin:
                    return result
                continue
            return result
        except requests.exceptions.RequestException:
            attempt += 1
            if attempt >= max_spin:
                raise


def hot_api_call(func, *args, **kwargs):
    """주문/취소 핫패스 — 1회(+네트워크 1회). 429 스핀으로 POST를 막지 않음."""
    try:
        return func(*args, **kwargs)
    except requests.exceptions.RequestException as e1:
        try:
            return func(*args, **kwargs)
        except requests.exceptions.RequestException as e2:
            raise


# JWT HS256/HS512 헤더 고정 (매 주문마다 json.dumps 반복 제거)
_JWT_HDR_HS256 = base64.urlsafe_b64encode(b'{"alg":"HS256","typ":"JWT"}').rstrip(b'=')
_JWT_HDR_HS512 = base64.urlsafe_b64encode(b'{"alg":"HS512","typ":"JWT"}').rstrip(b'=')

# 키 로드 후 캐시 — 매 주문 encode/lookup 제거
_SECRET_KEY_BYTES = None          # bytes
_ACCESS_KEY_CACHED = None         # str
_AUTH_HDR_PREFIX = 'Bearer '      # 상수

def _fast_nonce():
    """JWT nonce — uuid4 대비 경량 (32 hex, Upbit unique nonce 요건 충족)."""
    return secrets.token_hex(16)

def set_api_keys(access_key, secret_key):
    """API 키 설정 + 핫패스용 바이트/문자열 캐시 갱신."""
    global ACCESS_KEY, SECRET_KEY, _SECRET_KEY_BYTES, _ACCESS_KEY_CACHED
    ACCESS_KEY = access_key
    SECRET_KEY = secret_key
    _ACCESS_KEY_CACHED = access_key
    if isinstance(secret_key, str):
        _SECRET_KEY_BYTES = secret_key.encode('utf-8')
    else:
        _SECRET_KEY_BYTES = secret_key

def _b64url(data: bytes) -> bytes:
    return base64.urlsafe_b64encode(data).rstrip(b'=')

def _secret_key_bytes():
    cached = _SECRET_KEY_BYTES
    if cached is not None:
        return cached
    sk = SECRET_KEY
    return sk.encode('utf-8') if isinstance(sk, str) else sk

def _jwt_encode_hs(payload_dict, alg='HS256', secret=None):
    """PyJWT 우회 — hmac + base64url 직접 서명 (핫패스용)."""
    hdr = _JWT_HDR_HS256 if alg == 'HS256' else _JWT_HDR_HS512
    body = _b64url(json_dumps_bytes(payload_dict))
    msg = hdr + b'.' + body
    dig = hashlib.sha256 if alg == 'HS256' else hashlib.sha512
    sk = secret if secret is not None else _secret_key_bytes()
    if isinstance(sk, str):
        sk = sk.encode('utf-8')
    sig = _b64url(hmac.new(sk, msg, dig).digest())
    return (msg + b'.' + sig).decode('ascii')

def make_jwt(query_hash=None, query_hash_alg="SHA512"):
    """업비트/빗썸 인증용 JWT 토큰 생성 (공통 헬퍼).
    Returns: (jwt_token_str, headers_dict)"""
    payload = {
        'access_key': _ACCESS_KEY_CACHED or ACCESS_KEY,
        'nonce': _fast_nonce(),
    }
    if EXCHANGE.get('jwt_requires_timestamp'):
        payload['timestamp'] = int(time.time() * 1000)
    if query_hash:
        payload['query_hash'] = query_hash
        payload['query_hash_alg'] = query_hash_alg
    token = _jwt_encode_hs(payload, 'HS256')
    return token, {"Authorization": _AUTH_HDR_PREFIX + token}


def make_query_hash(params_dict):
    """쿼리 파라미터 → SHA512. unquote는 % 있을 때만."""
    qs = urlencode(params_dict, doseq=True)
    if '%' in qs:
        qs = unquote(qs)
    return hashlib.sha512(qs.encode('utf-8')).hexdigest()


def make_auth_headers(query_dict=None):
    """query_dict이 있으면 hash 포함 JWT. 빗썸은 timestamp 필수."""
    payload = {
        'access_key': _ACCESS_KEY_CACHED or ACCESS_KEY,
        'nonce': _fast_nonce(),
    }
    if EXCHANGE.get('jwt_requires_timestamp'):
        payload['timestamp'] = int(time.time() * 1000)
    if query_dict:
        payload['query_hash'] = make_query_hash(query_dict)
        payload['query_hash_alg'] = 'SHA512'
    token = _jwt_encode_hs(payload, 'HS256')
    return {"Authorization": _AUTH_HDR_PREFIX + token}


class OrderCanceler:
    """주문 취소 — 업비트 신규 일괄 API 우선, 미지원 거래소는 레거시 폴백.
    - DELETE /v1/orders/uuids : UUID 최대 20개 1회 취소
    - DELETE /v1/orders/open  : 조건(side/pairs) 최대 300개 일괄 취소
    - GET /v1/orders/open     : 잔여 검증 후 재스윕 (매도종료 후 매수 잔존 방지)
    """

    BATCH_CANCEL_IDS_MAX = 20
    BATCH_QUERY_IDS_MAX = 100

    def cancel_buy_orders_for_symbol(self, symbol, extra_uuids=None, verify=True):
        """해당 심볼 미체결 bid만 취소 — 병렬 워커가 타 코인 매수를 지우지 않게.
        buy_uuids 전역 clear 금지(다른 워커 UUID 보존)."""
        global buy_uuids
        if not symbol:
            return
        market = f"KRW-{symbol}"
        targets = set(u for u in (extra_uuids or []) if u)
        # 로컬 추적 중 해당 심볼로 추정 불가 → open list로 보강
        try:
            open_bids = self.list_open_bid_orders(market=market) or []
            for o in open_bids:
                uid = order_id_of(o)
                if uid:
                    targets.add(uid)
        except Exception:
            pass
        # buy_uuids 중 open에 있는 것만 교집합으로 추가 시도는 open list로 충분
        if targets:
            self.cancel_orders_parallel(list(targets))
        n = self.cancel_open_orders(cancel_side='bid', pairs=market)
        if n is None or n < 0:
            # batch open 미지원 — UUID만
            pass
        if verify:
            try:
                rem = self.list_open_bid_orders(market=market) or []
                rem_uids = [order_id_of(o) for o in rem if order_id_of(o)]
                if rem_uids:
                    self.cancel_orders_parallel(rem_uids)
                    for u in rem_uids:
                        self.cancel_order(u)
            except Exception as e:
                print_log(LogLevel.WARNING,
                          f"cancel_buy_orders_for_symbol verify: {str(e)[:80]}")
        # 전역 set에서 이 심볼 관련 UUID만 제거
        if targets:
            for u in targets:
                buy_uuids.discard(u)
        try:
            rem2 = self.list_open_bid_orders(market=market) or []
            for o in rem2:
                uid = order_id_of(o)
                if uid:
                    buy_uuids.discard(uid)
        except Exception:
            pass
        print_log(LogLevel.INFO,
                  f"매수 취소(심볼={symbol}) targets={len(targets)}")

    def cancel_buy_orders(self, extra_uuids=None, verify=True):
        """잔여 매수 전량 취소 — UUID 일괄 → open 일괄 → (verify) 잔여 검증 재스윕.
        lifecycle lock + epoch: lock 대기 중 매수가 시작되면 open-all 을 포기해
        새 bid 를 지우지 않음."""
        global buy_uuids
        intended_epoch = current_buy_epoch()
        max_attempts = 6 if verify else 1
        tid = threading.current_thread().name

        with _buy_lifecycle_lock:
            if current_buy_epoch() != intended_epoch:
                print_log(LogLevel.WARNING,
                          "매수 취소 스킵 — lock 대기 중 새 매수 배치 시작됨")
                return

            start_epoch = intended_epoch

            def _epoch_stale():
                return current_buy_epoch() != start_epoch

            for attempt in range(max_attempts):
                try:
                    if _epoch_stale():
                        print_log(LogLevel.WARNING,
                                  "매수 취소 중단 — 새 매수 배치 시작됨 (epoch)")
                        return

                    uuids = set(u for u in buy_uuids if u)
                    if extra_uuids:
                        uuids.update(u for u in extra_uuids if u)

                    if uuids:
                        self.cancel_orders_parallel(list(uuids))

                    if _epoch_stale():
                        print_log(LogLevel.WARNING,
                                  "매수 취소 중단 — open-all 직전 새 매수 배치 감지")
                        return

                    t0 = time.time()
                    self.cancel_all_orders(1)

                    if not verify:
                        if not _epoch_stale():
                            buy_uuids.clear()
                        return

                    if _epoch_stale():
                        return

                    remaining = self.list_open_bid_orders()
                    if not remaining:
                        buy_uuids.clear()
                        print_log(LogLevel.SUCCESS,
                                  f"매수 주문 전량 취소 확인 (attempt={attempt+1})")
                        return

                    rem_uuids = []
                    for o in remaining:
                        uid = order_id_of(o) or o.get(
                            EXCHANGE.get('order_id_param', 'uuid'))
                        if uid:
                            rem_uuids.append(uid)
                            buy_uuids.add(uid)
                    print_log(LogLevel.WARNING,
                              f"매수 잔여 {len(rem_uuids)}건 — UUID 강제 취소 "
                              f"(attempt={attempt+1})")
                    self.cancel_orders_parallel(rem_uuids)
                    for u in rem_uuids:
                        if _epoch_stale():
                            return
                        self.cancel_order(u)
                except Exception as e:
                    print_log(LogLevel.EXCEPTION,
                              f"Failed to cancel buy orders: {str(e)[:120]}")
                    if not verify:
                        break
            if not verify:
                if not _epoch_stale():
                    buy_uuids.clear()
                return
            if not _epoch_stale():
                buy_uuids.clear()
            print_log(LogLevel.ERROR, "매수 취소 검증 실패 — 로컬 UUID만 클리어")

    def cancel_sell_orders(self, symbol=None):
        """매도(ask) 취소.
        symbol 지정 시 해당 페어만 (KRW-SYMBOL).
        symbol 없으면 추적 UUID만 취소 — quote_currencies=KRW 전량 스윕 금지
        (다른 코인 ask까지 지워 매도 깜빡임/오염을 유발했음)."""
        global sell_uuids
        for _ in range(6):
            try:
                uuids = list(sell_uuids)
                if uuids:
                    self.cancel_orders_parallel(uuids)
                if symbol:
                    self.cancel_symbol_sell_orders(symbol)
                # symbol 없이 KRW 전체 ask open-cancel 하지 않음
                sell_uuids.clear()
                return
            except Exception:
                print_log(LogLevel.EXCEPTION, "Failed to cancel sell orders")
        sell_uuids.clear()

    def cancel_orders_parallel(self, uuids):
        """복수 주문 취소 — 업비트는 DELETE /orders/uuids (최대 20/요청),
        그 외는 개별 DELETE 병렬."""
        uuids = [u for u in uuids if u]
        if not uuids:
            return 0
        if EXCHANGE.get('supports_batch_cancel_ids'):
            ok = self._cancel_by_uuids_batched(uuids)
            # 배치 실패분은 개별 폴백
            if ok < len(uuids):
                for u in uuids:
                    self.cancel_order(u)
            return ok
        if len(uuids) == 1:
            return 1 if self.cancel_order(uuids[0]) else 0
        futures = [_ORDER_POOL.submit(self.cancel_order, u) for u in uuids]
        ok = 0
        for fut in as_completed(futures):
            try:
                if fut.result():
                    ok += 1
            except Exception:
                pass
        return ok

    def _cancel_by_uuids_batched(self, uuids):
        """DELETE /v1/orders/uuids — 최대 20개씩 청크."""
        total_ok = 0
        for i in range(0, len(uuids), self.BATCH_CANCEL_IDS_MAX):
            chunk = uuids[i:i + self.BATCH_CANCEL_IDS_MAX]
            params = {'uuids[]': chunk}
            headers = make_auth_headers(params)
            try:
                def api_call(p=params, h=headers):
                    return http_delete_hot(ORDERS_UUIDS_URL, params=p, headers=h)

                resp = hot_api_call(api_call)
                status = getattr(resp, 'status_code', 0)
                data = response_json(resp) if hasattr(resp, 'content') else resp
                if isinstance(data, dict) and 'error' in data:
                    print_log(LogLevel.WARNING,
                              f"Batch cancel uuids error: {data.get('error')} "
                              f"— fallback individual")
                    for u in chunk:
                        if self.cancel_order(u):
                            total_ok += 1
                    continue
                if isinstance(data, dict):
                    success = data.get('success') or {}
                    cnt = int(success.get('count', 0) or 0)
                    total_ok += cnt
                    failed_cnt = int((data.get('failed') or {}).get('count', 0) or 0)
                    print_log(LogLevel.INFO,
                              f"Batch cancel uuids: ok={cnt}/{len(chunk)} "
                              f"(failed={failed_cnt})")
                    # failed UUID는 개별 재시도
                    for fo in (data.get('failed') or {}).get('orders') or []:
                        uid = order_id_of(fo) if isinstance(fo, dict) else None
                        if uid:
                            self.cancel_order(uid)
                elif status == 200:
                    total_ok += len(chunk)
                else:
                    for u in chunk:
                        if self.cancel_order(u):
                            total_ok += 1
            except Exception as e:
                print_log(LogLevel.WARNING,
                          f"Batch cancel uuids failed — fallback parallel: "
                          f"{str(e)[:80]}")
                for u in chunk:
                    if self.cancel_order(u):
                        total_ok += 1
        return total_ok

    def cancel_open_orders(self, cancel_side='all', pairs=None,
                           quote_currencies=None, count=300):
        """DELETE /v1/orders/open — 조건 일괄 취소 (업비트 전용, 최대 300).
        pairs 와 quote_currencies 는 동시 사용 불가.
        실패 시 -1 반환 (호출측 레거시 폴백용)."""
        if not EXCHANGE.get('supports_batch_cancel_open'):
            return -1
        endpoint = EXCHANGE.get('orders_open_cancel_endpoint') or '/v1/orders/open'
        # 문서 예시와 동일 최소 파라미터 — 불필요 키로 invalid_parameter 방지
        params = {
            'cancel_side': cancel_side,  # bid | ask | all
            'count': str(min(int(count), 300)),
        }
        if pairs:
            params['pairs'] = pairs if isinstance(pairs, str) else ','.join(pairs)
        elif quote_currencies:
            params['quote_currencies'] = quote_currencies
        headers = make_auth_headers(params)
        try:
            def api_call():
                return http_delete_hot(ORDERS_OPEN_URL, params=params,
                                       headers=headers)
            resp = safe_api_call(api_call)
            status = getattr(resp, 'status_code', 0)
            data = response_json(resp) if hasattr(resp, 'content') else resp
            if isinstance(data, dict) and 'error' in data:
                print_log(LogLevel.WARNING,
                          f"Batch cancel open error: {data.get('error')} "
                          f"(status={status})")
                return -1
            if isinstance(data, dict):
                success = data.get('success') or {}
                cnt = int(success.get('count', 0) or 0)
                print_log(LogLevel.INFO,
                          f"Batch cancel open: side={cancel_side} "
                          f"scope={params.get('pairs') or params.get('quote_currencies')} "
                          f"ok={cnt}")
                return cnt
            if status == 200:
                return 0
            print_log(LogLevel.WARNING,
                      f"Batch cancel open unexpected status={status} body={data}")
            return -1
        except Exception as e:
            print_log(LogLevel.WARNING, f"Batch cancel open failed: {str(e)[:100]}")
            return -1

    def list_open_bid_orders(self, market=None):
        """미체결/예약 매수(bid) 목록 — GET /v1/orders/open 또는 레거시 /v1/orders."""
        try:
            list_ep = EXCHANGE.get('orders_list_endpoint') or '/v1/orders'
            if list_ep.endswith('/open') or EXCHANGE.get('supports_batch_cancel_open'):
                ep = EXCHANGE.get('orders_list_endpoint') or '/v1/orders/open'
                params = {'states[]': ['wait', 'watch'], 'limit': '100'}
                if market:
                    params['market'] = market
            else:
                ep = list_ep
                params = {'state': 'wait'}
            headers = make_auth_headers(params)

            def api_call():
                return response_json(
                    http_get(SERVER_URL + ep, params=params, headers=headers))

            data = safe_api_call(api_call)
            if isinstance(data, dict) and 'error' in data:
                print_log(LogLevel.WARNING, f"list open orders error: {data.get('error')}")
                return []
            orders = unwrap_orders_payload(data)
            if isinstance(orders, dict) and 'error' in orders:
                print_log(LogLevel.WARNING, f"list open orders error: {orders.get('error')}")
                return []
            bids = []
            for o in orders:
                if not isinstance(o, dict):
                    continue
                o = normalize_order(o)
                if o.get('side') != 'bid':
                    continue
                mkt = o.get('market') or o.get('code')
                if market and mkt != market:
                    continue
                bids.append(o)
            return bids
        except Exception as e:
            print_log(LogLevel.WARNING, f"list_open_bid_orders: {str(e)[:100]}")
            return []

    def cancel_all_orders(self, cancel_type):
        """cancel_type: 1=bid, 2=ask, 3=all.
        업비트: DELETE /orders/open 우선, 실패 시 목록+UUID 취소 폴백."""
        side_map = {1: 'bid', 2: 'ask', 3: 'all'}
        cancel_side = side_map.get(cancel_type, 'all')

        if EXCHANGE.get('supports_batch_cancel_open'):
            cnt = self.cancel_open_orders(cancel_side=cancel_side,
                                          quote_currencies='KRW')
            if cnt >= 0:
                print_log(LogLevel.INFO,
                          f"Cancelled open orders via batch API "
                          f"(type={cancel_type}, ok={cnt})")
                return
            print_log(LogLevel.WARNING,
                      "Batch cancel open failed — legacy list+cancel fallback")

        # 레거시/폴백: GET 목록 → UUID 일괄 취소
        orders = []
        if cancel_type == 1:
            orders = self.list_open_bid_orders()
        else:
            list_ep = EXCHANGE.get('orders_list_endpoint') or '/v1/orders'
            params = {'state': 'wait'}
            if str(list_ep).endswith('/open'):
                params = {'states[]': ['wait', 'watch']}
            headers = make_auth_headers(params)

            def api_call():
                return response_json(
                    http_get(SERVER_URL + list_ep, params=params, headers=headers))
            try:
                response_dict = safe_api_call(api_call)
            except Exception as e:
                print_log(LogLevel.ERROR, f"Failed to fetch orders: {str(e)}")
                return
            if isinstance(response_dict, dict) and 'error' in response_dict:
                print_log(LogLevel.ERROR, f"API Error: {response_dict['error']}")
                return
            orders = unwrap_orders_payload(response_dict)
            if isinstance(orders, dict) and 'error' in orders:
                print_log(LogLevel.ERROR, f"API Error: {orders.get('error')}")
                return

        id_field = EXCHANGE.get('order_id_param', 'uuid')
        targets = []
        for order in orders:
            if not isinstance(order, dict):
                continue
            order = normalize_order(order)
            side = order.get('side')
            if cancel_type == 1 and side != 'bid':
                continue
            if cancel_type == 2 and side != 'ask':
                continue
            targets.append(order_id_of(order) or order.get(id_field))

        cancelled_count = self.cancel_orders_parallel(targets)
        for u in targets:
            self.cancel_order(u)
        print_log(LogLevel.INFO, f"Cancelled {cancelled_count} orders (type: {cancel_type})")

    def cancel_symbol_sell_orders(self, symbol):
        """특정 심볼 미체결 매도(ask)만 취소 — 스탑로스 동기 경로.
        업비트: DELETE /orders/open?pairs=KRW-X&cancel_side=ask 1회."""
        market = f"KRW-{symbol}"
        if EXCHANGE.get('supports_batch_cancel_open'):
            cnt = self.cancel_open_orders(cancel_side='ask', pairs=market)
            if cnt:
                print_log(LogLevel.INFO, f"Cancelled ask orders for {market} (batch≈{cnt})")
            return

        list_ep = EXCHANGE.get('orders_list_endpoint') or '/v1/orders'
        params = {'state': 'wait'}
        if list_ep.endswith('/open'):
            params = {'market': market, 'states[]': ['wait']}
        headers = make_auth_headers(params)

        def api_call():
            return response_json(
                http_get(SERVER_URL + list_ep, params=params, headers=headers))

        try:
            response_dict = safe_api_call(api_call)
        except Exception as e:
            print_log(LogLevel.ERROR, f"Failed to fetch orders for {symbol}: {str(e)}")
            return

        orders = unwrap_orders_payload(response_dict)
        if isinstance(orders, dict):
            orders = []
        id_field = EXCHANGE.get('order_id_param', 'uuid')
        targets = []
        for order in orders:
            if not isinstance(order, dict):
                continue
            order = normalize_order(order)
            mkt = order.get('market') or order.get('code')
            if order.get('side') == 'ask' and mkt == market:
                targets.append(order_id_of(order) or order.get(id_field))
        cancelled = self.cancel_orders_parallel(targets)
        if cancelled:
            print_log(LogLevel.INFO, f"Cancelled {cancelled} ask orders for {market}")

    def cancel_order(self, order_uuid):
        if not order_uuid:
            return False
        params = {EXCHANGE['order_id_param']: order_uuid}
        headers = make_auth_headers(params)
        try:
            def api_call():
                return http_delete_hot(CANCEL_URL, params=params, headers=headers)
            response = hot_api_call(api_call)
            if getattr(response, 'status_code', 0) == 200:
                print_log(LogLevel.INFO, f"Successfully cancelled order: {order_uuid}")
                return True
            print_log(LogLevel.INFO,
                      f"Order {order_uuid} cancel skipped "
                      f"({getattr(response, 'status_code', '?')})")
            return True
        except Exception as e:
            print_log(LogLevel.WARNING, f"Cancel error {order_uuid}: {str(e)} — skipped")
            return True

    @staticmethod
    def fetch_orders_by_uuids(uuids):
        """GET /v1/orders/uuids — UUID 목록 일괄 조회 (최대 100).
        반환: {uuid: order_dict}. 미지원 시 빈 dict."""
        uuids = [u for u in uuids if u]
        if not uuids or not EXCHANGE.get('supports_batch_query_ids'):
            return {}
        result = {}
        max_n = OrderCanceler.BATCH_QUERY_IDS_MAX
        for i in range(0, len(uuids), max_n):
            chunk = uuids[i:i + max_n]
            params = {'uuids[]': chunk}
            headers = make_auth_headers(params)
            try:
                def api_call(p=params, h=headers):
                    return response_json(
                        http_get_hot(ORDERS_UUIDS_URL, params=p, headers=h))
                data = safe_api_call(api_call)
                if isinstance(data, list):
                    for order in data:
                        if isinstance(order, dict):
                            oid = order_id_of(order)
                            if oid:
                                result[oid] = normalize_order(order)
                elif isinstance(data, dict) and 'error' not in data:
                    # 일부 응답이 {orders:[...]} 형태일 수 있음
                    for order in data.get('orders') or []:
                        if isinstance(order, dict):
                            oid = order_id_of(order)
                            if oid:
                                result[oid] = normalize_order(order)
            except Exception as e:
                print_log(LogLevel.WARNING,
                          f"Batch query uuids failed: {str(e)[:80]}")
        return result

    @staticmethod
    def cancel_and_new_order(prev_uuid, new_price, new_volume,
                             new_ord_type='limit'):
        """POST /v1/orders/cancel_and_new — 취소+재주문을 1 RTT.
        JSON body + query_hash(JWT). 성공 시 신규 주문 uuid 또는 응답 dict.
        미지원/실패 시 None."""
        if not prev_uuid or not EXCHANGE.get('supports_cancel_and_new'):
            return None
        if not CANCEL_AND_NEW_URL:
            return None
        # new_volume='remain_only' 지원 — 잔량 그대로 재주문
        # 수량은 floor 포맷 (f"{x:.8f}" 반올림은 insufficient_funds 유발)
        if isinstance(new_volume, str):
            vol_str = new_volume
        else:
            vol_str = UpbitTickSystem.format_order_volume(float(new_volume), 8)
        if vol_str == '0' and not (isinstance(new_volume, str)
                                   and new_volume == 'remain_only'):
            return None
        params = {
            'prev_order_uuid': prev_uuid,
            'new_ord_type': new_ord_type,
            'new_price': UpbitTickSystem.format_order_price(new_price),
            'new_volume': vol_str,
        }
        headers = make_auth_headers(params)
        headers['Content-Type'] = 'application/json'
        try:
            order_rate_limiter.note_use('cancel_and_new')
            wid = get_current_worker_id()
            wait = 3.0 if wid else 0.35
            if not order_rate_limiter.acquire(timeout=wait, worker_id=wid):
                order_rate_limiter._log_exhaustion('acquire-fail:cancel_and_new')
                return None

            def api_call():
                return http_post_hot(
                    CANCEL_AND_NEW_URL,
                    headers=headers,
                    data=json_dumps_bytes(params),
                )
            resp = hot_api_call(api_call)
            order_rate_limiter.note_response(resp)
            if _response_rate_limited(resp):
                return None
            data = response_json(resp) if hasattr(resp, 'content') else resp
            if isinstance(data, dict) and data.get('uuid'):
                # 응답은 취소된 주문 정보 + new_order_uuid 필드인 경우 있음
                new_uid = data.get('new_order_uuid') or data.get('uuid')
                print_log(LogLevel.SUCCESS,
                          f"cancel_and_new ok: prev={prev_uuid[:8]}… "
                          f"→ {str(new_uid)[:8]}… @ {params['new_price']}")
                return data
            if isinstance(data, dict) and data.get('error'):
                err = data.get('error')
                err_l = str(err).lower()
                # insufficient는 매도 교체 경로에서 흔함 — 스팸 로그 금지
                if 'insufficient' not in err_l and 'fund' not in err_l:
                    print_log(LogLevel.WARNING,
                              f"cancel_and_new rejected: {err}")
            return None
        except Exception as e:
            print_log(LogLevel.WARNING, f"cancel_and_new error: {str(e)[:100]}")
            return None


def cancel_buy_orders_async(extra_uuids=None):
    """잔여 매수 취소 — 핫패스용 백그라운드 1회 스윕 (검증 없음)."""
    oc = OrderCanceler()
    run_async(oc.cancel_buy_orders, extra_uuids=extra_uuids, verify=False)


def cancel_buy_orders_sync(extra_uuids=None, verify=True):
    """사이클 종료용 동기 매수 취소 — 잔여 확인까지 끝난 뒤 다음 사이클 진입."""
    OrderCanceler().cancel_buy_orders(extra_uuids=extra_uuids, verify=verify)


class AccountChecker:
    """잔고 조회 클래스 — Private WS 캐시 우선, REST 폴백.
    내부는 currency→info dict 로 O(1) 조회 (list 선형 스캔 제거).
    평단 핫패스: http_session_avg + AVG_TIMEOUT + hot_api_call (slow/safe 금지)."""

    _accounts_cache = {}       # 마지막 성공 accounts
    _accounts_cache_time = 0.0
    _ACCOUNTS_TTL = 2.0        # 초 — 핫루프 REST 폭주/429 방지
    _avg_fetch_lock = threading.Lock()
    _avg_fetch_gen = 0         # 진행 중 평단 fetch 무효화용

    def __init__(self):
        if private_ws._is_initialized and private_ws.is_connected:
            # 참조만 유지 — 복사/str 변환 생략
            self._cache = private_ws.asset_cache
        else:
            self._cache = self._rest_fetch(ACCESS_KEY, SECRET_KEY) or {}

    # ===== 정적 REST 헬퍼 (PrivateWS 폴백용) =====

    @staticmethod
    def _parse_accounts_list(result, prefer_symbol=None):
        """accounts JSON list → cache dict. prefer_symbol 정보는 조기 추출."""
        cache = {}
        hit = None
        if not isinstance(result, list):
            return cache, hit
        for acc in result:
            if not isinstance(acc, dict):
                continue
            cur = acc.get('currency')
            if not cur:
                continue
            info = {
                'balance': _fast_float(acc.get('balance', 0)),
                'locked': _fast_float(acc.get('locked', 0)),
                'avg_buy_price': _fast_float(acc.get('avg_buy_price', 0)),
            }
            cache[cur] = info
            if prefer_symbol and cur == prefer_symbol:
                hit = info
        return cache, hit

    @staticmethod
    def _accounts_get_once():
        """평단 전용 1회 GET — JWT 생성→avg세션→orjson. 예외는 상향."""
        headers = make_auth_headers()
        url = ACCOUNTS_URL or (SERVER_URL + '/v1/accounts')
        return http_get_avg(url, headers=headers, timeout=AVG_TIMEOUT)

    @staticmethod
    def _accounts_get_race():
        """테일 레이턴시 제거 — 데몬 스레드 2개 레이스 (풀 중첩 데드락 금지).
        먼저 200+body 온 쪽을 채택."""
        box = {'resp': None}
        done = threading.Event()

        def _one():
            try:
                r = AccountChecker._accounts_get_once()
            except Exception:
                return
            if r is None or _response_rate_limited(r):
                return
            if getattr(r, 'status_code', 0) != 200 or not r.content:
                return
            if box['resp'] is None:
                box['resp'] = r
                done.set()

        t1 = threading.Thread(target=_one, name='avg-race1', daemon=True)
        t2 = threading.Thread(target=_one, name='avg-race2', daemon=True)
        t1.start()
        t2.start()
        done.wait(timeout=AVG_TIMEOUT[0] + AVG_TIMEOUT[1] + 0.12)
        if box['resp'] is not None:
            return box['resp']
        return hot_api_call(AccountChecker._accounts_get_once)

    @staticmethod
    def _rest_fetch_avg_hot(symbol=None, allow_stale=False, race=False):
        """평단 핫패스 /v1/accounts.
        - slow 세션·HTTP_TIMEOUT_SLOW·safe_api_call 사용 금지
        - 실패 시 기본은 스테일 캐시 미반환(잘못된 낮은 평단 매도 방지)
        Returns: (cache_dict|None, symbol_info|None)"""
        try:
            resp = (AccountChecker._accounts_get_race() if race
                    else hot_api_call(AccountChecker._accounts_get_once))
        except Exception:
            if allow_stale and AccountChecker._accounts_cache:
                c = AccountChecker._accounts_cache
                return c, (c.get(symbol) if symbol else None)
            return None, None
        if resp is None or _response_rate_limited(resp):
            if allow_stale and AccountChecker._accounts_cache:
                c = AccountChecker._accounts_cache
                return c, (c.get(symbol) if symbol else None)
            return None, None
        try:
            result = response_json(resp)
        except Exception:
            if allow_stale and AccountChecker._accounts_cache:
                c = AccountChecker._accounts_cache
                return c, (c.get(symbol) if symbol else None)
            return None, None
        cache, hit = AccountChecker._parse_accounts_list(result, prefer_symbol=symbol)
        if not cache:
            if allow_stale and AccountChecker._accounts_cache:
                c = AccountChecker._accounts_cache
                return c, (c.get(symbol) if symbol else None)
            return None, None
        AccountChecker._accounts_cache = cache
        AccountChecker._accounts_cache_time = time.time()
        if hit is None and symbol:
            hit = cache.get(symbol)
        return cache, hit

    @staticmethod
    def _rest_symbol_info_hot(symbol, baseline=0.0, floor=0.0, max_attempts=5):
        """평단 확보 루프 — 거래소 실avg 그대로 반환 (floor로 부풀리기 금지).
        baseline과 다르면 즉시, 같으면 재시도 후 best 반환."""
        baseline = float(baseline or 0)
        best = None  # (bal, locked, avg)
        for attempt in range(max(1, int(max_attempts))):
            cache, info = AccountChecker._rest_fetch_avg_hot(
                symbol=symbol, allow_stale=False, race=(attempt == 0))
            if info:
                bal = float(info.get('balance', 0) or 0)
                locked = float(info.get('locked', 0) or 0)
                avg = float(info.get('avg_buy_price', 0) or 0)
                if avg > 0 or (bal + locked) > 0:
                    best = (bal, locked, avg)
                if avg > 0 and (baseline <= 0 or abs(avg - baseline) > 1e-12):
                    return bal, locked, avg
                if avg > 0 and attempt >= 2:
                    # 동일해도 실값 반환 (가짜 floor 합성 금지)
                    return bal, locked, avg
            if attempt + 1 < max_attempts:
                time.sleep(0.008 if attempt == 0 else 0.015)
        if best and float(best[2] or 0) > 0:
            return best[0], best[1], float(best[2])
        return -1, -1, -1

    @staticmethod
    def _rest_fetch(access_key, secret_key, force=False):
        """REST /v1/accounts → 잔고 딕셔너리.
        force=True(매도/평단): avg 핫패스. 그 외: TTL + 일반 핫 세션.
        실패 시 스테일 캐시(일반 조회용)."""
        now = time.time()
        if (not force
                and AccountChecker._accounts_cache
                and (now - AccountChecker._accounts_cache_time)
                < AccountChecker._ACCOUNTS_TTL):
            return AccountChecker._accounts_cache

        if force:
            cache, _ = AccountChecker._rest_fetch_avg_hot(
                symbol=None, allow_stale=True, race=False)
            return cache

        headers = make_auth_headers()
        url = ACCOUNTS_URL or (SERVER_URL + '/v1/accounts')

        def api_call():
            response = http_get_hot(url, headers=headers, timeout=HTTP_TIMEOUT)
            return response_json(response)

        try:
            result = safe_api_call(api_call)
        except Exception:
            return AccountChecker._accounts_cache or None

        cache, _ = AccountChecker._parse_accounts_list(result)
        if cache:
            AccountChecker._accounts_cache = cache
            AccountChecker._accounts_cache_time = now
            return cache
        if AccountChecker._accounts_cache:
            return AccountChecker._accounts_cache
        return None

    @staticmethod
    def _rest_symbol_info(access_key, secret_key, symbol, force=False):
        """REST 폴백 — (balance, locked, avg_buy_price).
        목록에 없음 = 보유 0. 진짜 실패(캐시조차 없음)만 -1.
        force=True: 평단/가용 확정 — avg 핫패스."""
        if force:
            cache, info = AccountChecker._rest_fetch_avg_hot(
                symbol=symbol, allow_stale=False, race=False)
            if cache is None:
                return -1, -1, -1
            if info:
                return (info['balance'], info['locked'], info['avg_buy_price'])
            return 0.0, 0.0, 0.0
        cache = AccountChecker._rest_fetch(access_key, secret_key, force=False)
        if cache is None:
            return -1, -1, -1
        info = cache.get(symbol)
        if info:
            return info['balance'], info['locked'], info['avg_buy_price']
        return 0.0, 0.0, 0.0

    @staticmethod
    def _rest_krw(access_key, secret_key, balance_type=1):
        """REST 폴백 — KRW 잔고."""
        cache = AccountChecker._rest_fetch(access_key, secret_key)
        if cache is None:
            return 0.0
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
        if cache is None:
            return []
        return list(cache.keys())

    # ===== 인스턴스 조회 메서드 (캐시에서 읽기) =====

    def get_krw_balance(self, balance_type=1):
        info = self._cache.get('KRW')
        if not info:
            return 0
        if balance_type == 1:
            return float(info['balance'])
        elif balance_type == 2:
            return float(info['locked'])
        elif balance_type == 3:
            return float(info['balance']) + float(info['locked'])
        return float(info['balance'])

    def get_owned_symbols(self):
        return list(self._cache.keys())

    def get_symbol_info(self, symbol):
        """캐시에 없음 = 보유 0 (업비트 accounts 미포함)."""
        info = self._cache.get(symbol)
        if not info:
            return 0.0, 0.0, 0.0
        return float(info['balance']), float(info['locked']), float(info['avg_buy_price'])

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
        self._cycle_ended = False  # 사이클 종료 후 BuyModule 재개/재POST 금지
        self.last_order_time = None
        self.last_check_time = None
        self.first_order_start_time = None
        self.first_order_timeout = 3.0  # 첫주문 3초 미체결 → 취소+사이클중단 (고전)
        self._first_order_timed_out = False
        self.executed_count = 0  # 체결된 레벨 수 — O(1) 완료 체크용
        self._ma_gate_paused = False
        self._skip_ma_gate = False  # True=매도/체결 진행중 → MA60 진입게이트 무시
        self._skip_adopt_until = 0.0
        self._skip_place_until = 0.0  # CAP/취소 lag 직후 재POST 금지
        self._timeout_place_floor = None
        self._last_buy_px_key = ''
        self._same_px_requote_streak = 0
        self._blocked_buy_px_keys = set()
        self._buys_halted = False
        self._buys_halted_by_topn = False
        
        # 계획 관리
        self.original_planned_orders = []
        self.active_planned_orders = []
        self.executed_orders = []
        self.pending_orders = []

        # O(1) 인덱스 — 핫루프 list 스캔 제거
        self.planned_by_level = {}       # level → planned order dict
        self.pending_by_uuid = {}        # uuid → pending dict
        self.pending_levels = set()      # levels with ≥1 pending
        self.pending_level_counts = {}   # level → pending count
        self.pending_split_keys = set()  # (level, split_idx)
        self.level_fill_count = {}       # level → filled split count
        self.partial_levels = set()      # fills>0 but level not complete
        self.next_plan_idx = 0           # cursor into active_planned_orders
        self.last_executed_level = 0
        self.last_executed_price = 0.0
        self._min_shift_tick = UpbitTickSystem.get_minimum_tick(low_price) if low_price else 0.0001

        # 매수 체결 → 로컬 VWAP 즉시 매도 후 REST 교정
        self.on_buy_fill_sell = None  # callback(avg, total_vol)

        # 밀림 관리 — 워커별 동적 슬라이딩 (단일스레드 규칙 계승)
        self.plan_shift_amount = 0.0
        self.last_shift_check_price = current_price

        # 체결/재주문 추적 — 분할 POST 실패·취소분 재주문 큐
        self._failed_replaces = []
        self._last_replace_retry = 0.0
        self._last_fill_rest_check = 0.0
        
        # OrderCanceler 인스턴스
        self.order_canceler = OrderCanceler()
        # 분할 POST 동시 진입 방지 — 워커당 정확히 최대 3개만
        self._buy_place_lock = threading.RLock()

    def _notify_sell_after_buy_fill(self, fill_volume=0.0, fill_price=0.0,
                                     fill_uuid=None):
        """매수 체결 → 로컬 VWAP arm/fire. executed_orders로 장부 재동기화."""
        cb = getattr(self, 'on_buy_fill_sell', None)
        if not cb:
            return
        try:
            fp = float(fill_price or 0)
        except (TypeError, ValueError):
            fp = 0.0
        if fp <= 0:
            try:
                fp = float(self.last_executed_price or 0)
            except (TypeError, ValueError):
                fp = 0.0
        try:
            if (private_ws.avg_sell_cb is not cb
                    or private_ws.avg_sell_symbol != self.symbol):
                private_ws.set_avg_sell_target(self.symbol, cb)
            # REST 스냅샷 이후 rebuild는 qty0=전량과 이중집계 → 금지
            if not getattr(private_ws, '_local_avg_rest_synced', False):
                try:
                    private_ws.rebuild_local_avg_from_fills(self.executed_orders)
                except Exception:
                    pass
            private_ws.arm_avg_sell(
                vol_hint=float(fill_volume or 0),
                fill_price=fp,
                fill_uuid=fill_uuid,
                symbol=self.symbol)
            return
        except Exception:
            pass

        symbol = self.symbol
        vol_hint = float(fill_volume or 0)

        def _rest_safety():
            try:
                # sleep 금지 — 즉시 REST 사후통제
                with private_ws._avg_sell_lock:
                    has_cb = (private_ws.avg_sell_cb
                              and private_ws.avg_sell_symbol == symbol)
                    floor = float(private_ws._avg_sell_price_floor or 0)
                    baseline = float(private_ws._avg_sell_last_fired_avg
                                     or private_ws._avg_sell_baseline_avg or 0)
                    fired = float(private_ws._avg_sell_last_fired_avg or 0) > 0
                if not has_cb:
                    return
                bal2, loc2, avg2 = AccountChecker._rest_symbol_info_hot(
                    symbol, baseline=baseline, floor=0.0,
                    max_attempts=4)
                if avg2 and avg2 > 0:
                    vol = max(float(bal2) + float(max(loc2, 0)), vol_hint)
                    if fired:
                        private_ws._avg_sell_fire_is_local = False
                        private_ws.correct_avg_sell(float(avg2), vol)
                    else:
                        private_ws._avg_sell_armed = True
                        private_ws._avg_sell_fire_is_local = False
                        private_ws.force_fire_avg_sell(float(avg2), vol)
                elif fp > 0 and not fired:
                    private_ws._avg_sell_armed = True
                    private_ws._avg_sell_fire_is_local = True
                    private_ws.force_fire_avg_sell(fp, vol_hint)
            except Exception as e:
                print_log(LogLevel.WARNING, f"avg-sell REST safety: {str(e)[:100]}")

        try:
            _AVG_POOL.submit(_rest_safety)
        except Exception:
            try:
                _ORDER_POOL.submit(_rest_safety)
            except Exception:
                run_async(_rest_safety)

    def _rebuild_planned_index(self):
        """계획 확정 후 level→order 인덱스 재구축 O(L)."""
        self.planned_by_level = {o['level']: o for o in self.active_planned_orders}
        self.next_plan_idx = 0

    @staticmethod
    def _collect_pending_uuids_from_index(pending_by_uuid):
        """pending_by_uuid가 dict/list/None이어도 UUID 목록 수거."""
        if isinstance(pending_by_uuid, dict):
            return [uid for uid in pending_by_uuid.keys() if uid]
        if isinstance(pending_by_uuid, list):
            uuids = []
            for item in pending_by_uuid:
                if isinstance(item, dict):
                    uid = item.get('uuid')
                    if uid:
                        uuids.append(uid)
                elif isinstance(item, str) and item:
                    uuids.append(item)
            return uuids
        return []

    def _reset_runtime_indexes(self):
        """사이클 시작 시 런타임 인덱스 초기."""
        if not isinstance(self.pending_by_uuid, dict):
            self.pending_by_uuid = {}
        else:
            self.pending_by_uuid.clear()
        self.pending_levels.clear()
        self.pending_level_counts.clear()
        self.pending_split_keys.clear()
        self.level_fill_count.clear()
        self.partial_levels.clear()
        self.next_plan_idx = 0
        self.last_executed_level = 0
        self.last_executed_price = 0.0

    def _add_pending(self, pending):
        """pending 등록 — list + O(1) 인덱스 동기화.
        3개까지 허용. 4개째는 조용히 취소(로그 없음)."""
        if len(self.pending_orders) >= MAX_OPEN_BUYS_PER_WORKER:
            uid = pending.get('uuid') if isinstance(pending, dict) else None
            if uid:
                try:
                    self.order_canceler.cancel_order(uid)
                    buy_uuids.discard(uid)
                except Exception:
                    pass
            self._last_buy_error = 'max_open_buys'
            return False
        self.pending_orders.append(pending)
        uid = pending.get('uuid')
        if uid:
            if not isinstance(self.pending_by_uuid, dict):
                self.pending_by_uuid = {}
            self.pending_by_uuid[uid] = pending
        lv = pending['level']
        self.pending_levels.add(lv)
        self.pending_level_counts[lv] = self.pending_level_counts.get(lv, 0) + 1
        self.pending_split_keys.add((lv, pending.get('split_idx', 0)))
        return True

    def _local_open_buy_count(self):
        return len(self.pending_orders or [])

    def _exchange_open_buy_count(self):
        """거래소 해당 심볼 open bid 수 — 150ms 캐시 (분할 POST 버스트당 1회)."""
        now = time.time()
        cache = getattr(self, '_ex_bid_cache', None)
        if cache and (now - float(cache[0])) < 0.15:
            return int(cache[1])
        try:
            bids = OrderCanceler().list_open_bid_orders(
                market=f"KRW-{self.symbol}")
            n = len(bids or [])
        except Exception:
            n = -1  # 조회 실패 = 보수적으로 차단하지 않되 슬롯에서 구분
        self._ex_bid_cache = (now, n)
        return n

    def _invalidate_ex_bid_cache(self):
        self._ex_bid_cache = None

    def _hard_open_buy_count(self, *, check_exchange=True):
        """★ 실제 미체결 매수 = max(로컬, 거래소).
        check_exchange=False면 로컬만 (분할 POST 사이 REST 제거)."""
        local = self._local_open_buy_count()
        if not check_exchange:
            return local
        try:
            ex = self._exchange_open_buy_count()
        except Exception:
            ex = -1
        if ex < 0:
            return local
        return max(local, int(ex))

    def _open_buy_slots_left(self, check_exchange=True):
        """남은 POST 가능 수. ★ 거래소 포함 hard count 기준.
        이미 3개면 0 → 4번째 FAIL."""
        used = self._hard_open_buy_count(check_exchange=check_exchange)
        if check_exchange and used > MAX_OPEN_BUYS_PER_WORKER:
            try:
                self._enforce_max_open_buys()
            except Exception:
                pass
            used = self._hard_open_buy_count(check_exchange=True)
        return max(0, int(MAX_OPEN_BUYS_PER_WORKER) - int(used))

    def _buy_cap_full(self, where=''):
        """미체결 3개 꽉 참 — 정상 대기. 로그 없음(CAP FAIL 제거)."""
        self._last_buy_error = 'max_open_buys'
        return True

    # 하위 호환 — 예전 호출부
    def _buy_cap_fail(self, where=''):
        return self._buy_cap_full(where)

    def _enforce_max_open_buys(self):
        """미체결 매수가 3 초과(4+)일 때만 초과분 취소. 3개까지는 정상.
        초과분 취소만 하고 절대 여기서 재POST하지 않음."""
        # 로컬 먼저
        while len(self.pending_orders) > MAX_OPEN_BUYS_PER_WORKER:
            p = self.pending_orders[-1]
            uid = p.get('uuid')
            print_log(LogLevel.ERROR,
                      f"BUY CAP ENFORCE: local "
                      f"{len(self.pending_orders)}>{MAX_OPEN_BUYS_PER_WORKER} "
                      f"{self.symbol} — cancel 초과 {(uid or '')[:8]}")
            if uid:
                try:
                    self.order_canceler.cancel_order(uid)
                except Exception:
                    pass
                buy_uuids.discard(uid)
                self._remove_pendings_by_uuids({uid})
            else:
                try:
                    self._pop_pending_at(len(self.pending_orders) - 1)
                except Exception:
                    break
        # 거래소 4개 이상만 정리
        try:
            bids = OrderCanceler().list_open_bid_orders(
                market=f"KRW-{self.symbol}") or []
        except Exception:
            bids = []
        if len(bids) <= MAX_OPEN_BUYS_PER_WORKER:
            return self._hard_open_buy_count() <= MAX_OPEN_BUYS_PER_WORKER

        our = set()
        if isinstance(self.pending_by_uuid, dict):
            our = set(self.pending_by_uuid.keys())
        orphans = []
        ours_orders = []
        for o in bids:
            uid = order_id_of(o)
            if not uid:
                continue
            if uid in our:
                ours_orders.append(o)
            else:
                orphans.append(o)
        cancel_uids = []
        need = len(bids) - MAX_OPEN_BUYS_PER_WORKER
        for o in orphans:
            if need <= 0:
                break
            uid = order_id_of(o)
            if uid:
                cancel_uids.append(uid)
                need -= 1
        if need > 0 and ours_orders:
            def _px(o):
                try:
                    return float(o.get('price') or o.get('p') or 0)
                except (TypeError, ValueError):
                    return 0.0
            for o in sorted(ours_orders, key=_px):
                if need <= 0:
                    break
                uid = order_id_of(o)
                if uid:
                    cancel_uids.append(uid)
                    need -= 1
        if cancel_uids:
            print_log(LogLevel.ERROR,
                      f"BUY CAP ENFORCE: exchange "
                      f"{len(bids)}>{MAX_OPEN_BUYS_PER_WORKER} "
                      f"{self.symbol} — cancel 초과 {len(cancel_uids)}건 "
                      f"(재POST 없음)")
            try:
                self.order_canceler.cancel_orders_parallel(cancel_uids)
            except Exception:
                pass
            buy_uuids.difference_update(cancel_uids)
            self._remove_pendings_by_uuids(set(cancel_uids))
            self._skip_place_until = max(
                float(getattr(self, '_skip_place_until', 0) or 0),
                time.time() + 1.5)
        return self._hard_open_buy_count() <= MAX_OPEN_BUYS_PER_WORKER

    def _pop_pending_at(self, index):
        """pending_orders[index] 제거 + 인덱스 갱신. 반환: pending dict.
        범위 밖이면 None (동시 수정 대비)."""
        if index < 0 or index >= len(self.pending_orders):
            return None
        pending = self.pending_orders.pop(index)
        uid = pending.get('uuid')
        if uid:
            self.pending_by_uuid.pop(uid, None)
        lv = pending['level']
        key = (lv, pending.get('split_idx', 0))
        self.pending_split_keys.discard(key)
        cnt = self.pending_level_counts.get(lv, 1) - 1
        if cnt <= 0:
            self.pending_level_counts.pop(lv, None)
            self.pending_levels.discard(lv)
        else:
            self.pending_level_counts[lv] = cnt
        return pending

    def _pop_pending_by_uuid(self, order_uuid):
        """uuid로 pending 제거. 없으면 None. 동시 clear/pop에도 안전."""
        if not order_uuid:
            return None
        # pending_by_uuid에 없어도 리스트에 잔류할 수 있어 선형 탐색
        for i, p in enumerate(self.pending_orders):
            if p.get('uuid') == order_uuid:
                return self._pop_pending_at(i)
        self.pending_by_uuid.pop(order_uuid, None)
        return None

    def _remove_pendings_by_uuids(self, uuid_set):
        """uuid 집합에 해당하는 pending 일괄 제거 O(P)."""
        if not uuid_set:
            return
        keep = []
        for p in self.pending_orders:
            uid = p.get('uuid')
            if uid in uuid_set:
                self.pending_by_uuid.pop(uid, None)
                lv = p['level']
                self.pending_split_keys.discard((lv, p.get('split_idx', 0)))
                cnt = self.pending_level_counts.get(lv, 1) - 1
                if cnt <= 0:
                    self.pending_level_counts.pop(lv, None)
                    self.pending_levels.discard(lv)
                else:
                    self.pending_level_counts[lv] = cnt
            else:
                keep.append(p)
        self.pending_orders = keep

    class DistributionType(Enum):
        LINEAR = 1
        LOG_LINEAR_II = 2
        LOG_LINEAR_I = 3
        PARABOLIC_II = 4
        PARABOLIC_I = 5
        EXPONENTIAL = 6
        FIBONACCI = 7
        EXPLOSIVE = 8

    def calculate_order_plan(self, drop_percentage, drop_count, distribution_type):
        """주문 계획 계산 - 저가 기준"""
        print_log(LogLevel.INFO, f"Starting order plan calculation for {self.symbol} based on low price {self.low_price:.4f}")
        
        self._drop_percentage = drop_percentage
        self._drop_count = drop_count
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
            # exponent=2.5 — 하락 후반부 매집 비중을 기하급수적으로 늘리는 강매집 분배.
            self._calculate_exponential_plan(drop_percentage, drop_count, 2.5)
        elif distribution_type == self.DistributionType.FIBONACCI:
            self._calculate_fibonacci_plan(drop_percentage, drop_count)
        elif distribution_type == self.DistributionType.EXPLOSIVE:
            # 지수 폭발 분배 — 하락 후반부로 갈수록 매집 비중이 급격히 폭발.
            # 단계 배수: 1.4, 1.8, 2.2, 2.6, 3.0, … (초항 1.4, 공차 0.4).
            self._calculate_explosive_plan(drop_percentage, drop_count)
        else:
            self._calculate_linear_plan(drop_percentage, drop_count)

        # 분할매수 적용 시 각 레벨을 '이전 레벨 분할 최저가' 기준으로 재계산
        self._adjust_to_split_lowest_base(drop_percentage)

        # 인접 주문 가격 중복 시 호가 최소단위만큼 강제 하락 (예: 150/150 → 150/149)
        self._enforce_min_tick_gap()

        # 레벨별 KRW를 정수 원으로 정규화해 합 = total_amount (최종 0원 소진용)
        self._finalize_krw_budgets()

        self.original_planned_orders = [order.copy() for order in self.active_planned_orders]
        self._rebuild_planned_index()
        print_log(LogLevel.SUCCESS, f"Calculated {len(self.active_planned_orders)} buy orders for {self.symbol} based on low price {self.low_price:.4f}")

    def ensure_assist_level(self):
        """집중모드(L6+) — 타코인 매도금을 받을 제8라운드(drop_count+1) 편입.
        L8 drop_percentage = drop_count (예: drop_count=7 → 7% 하락 from L7 분할최저가).
        초기 예산은 L1~L7에 이미 배분됨 — L8은 최소금액 placeholder,
        실제 투입액은 마지막 레벨 전액흡수(타코인 매도 KRW)로 채움."""
        dc = int(getattr(self, '_drop_count', 0) or 0)
        if dc <= 0:
            return False
        assist_lv = dc + 1  # 7+1 = 8
        plans = self.active_planned_orders
        if not plans:
            return False
        if any(int(o.get('level') or 0) == assist_lv for o in plans):
            return False
        if getattr(self, '_cycle_ended', False) or getattr(self, '_buys_halted', False):
            return False

        prev = plans[-1]
        prev_px = float(prev.get('planned_price') or 0)
        if prev_px <= 0:
            return False
        prev_lv = int(prev.get('level') or dc)
        # drop_percentage ≡ drop_count (round_down의 proportion = %)
        drop_prop = float(dc)
        prev_splits = UpbitTickSystem.generate_split_prices(
            prev_px, split_count_for_level(prev_lv), SPLIT_STEP_PERCENT)
        base = min(prev_splits) if prev_splits else prev_px
        new_price = UpbitTickSystem.round_down(base, drop_prop)
        tick = UpbitTickSystem.get_minimum_tick(prev_px)
        if new_price >= prev_px:
            new_price = UpbitTickSystem.snap_to_tick(prev_px - tick, 'floor')
        if new_price <= 0:
            return False

        qty = float(MIN_ORDER_AMOUNT)
        vol = UpbitTickSystem.volume_for_krw(new_price, qty)
        if vol <= 0:
            return False

        plans.append({
            'level': assist_lv,
            'original_planned_price': new_price,
            'planned_price': new_price,
            'quantity': qty,
            'volume': vol,
            'executed': False,
            'shift_applied': 0.0,
            'assist': True,
        })
        # original 백업에도 반영 (상태/재개용)
        try:
            self.original_planned_orders.append(plans[-1].copy())
        except Exception:
            pass
        self._rebuild_planned_index()
        # L7까지 끝났어도 L8 대기 — 매수 재개
        if not getattr(self, '_cycle_ended', False):
            self.is_active = True
        print_log(
            LogLevel.WARNING,
            f"L{assist_lv} assist 편입 {self.symbol}: "
            f"px={new_price:.8f} (L{prev_lv}@{prev_px:.8f} → drop%{drop_prop:g}=drop_count) "
            f"— 타코인 매도 KRW → L{assist_lv}")
        return True

    def _finalize_krw_budgets(self):
        """가중 분배 결과를 정수 KRW 예산으로 고정. 합계 == floor(total_amount).
        각 레벨 ≥ MIN_ORDER_AMOUNT 보장(총액이 충분할 때) — 앞 레벨 거절 방지.
        quantity = KRW(원), volume = 그 예산으로 살 수 있는 최대 수량."""
        orders = self.active_planned_orders
        if not orders:
            return
        total = int(math.floor(float(self.total_amount) + 1e-9))
        if total <= 0:
            print_log(LogLevel.ERROR, f"KRW budget total <= 0: {self.total_amount}")
            return

        raw = [max(0.0, float(o.get('quantity') or 0)) for o in orders]
        raw_sum = sum(raw) or 1.0
        n = len(orders)
        min_amt = int(MIN_ORDER_AMOUNT)

        if total < min_amt:
            print_log(LogLevel.ERROR,
                      f"KRW total {total:,} < MIN_ORDER {min_amt:,} — cannot place")
            budgets = [0] * (n - 1) + [total]
        elif total < min_amt * n:
            # 전 레벨 최소 보장 불가 — 뒤 레벨부터 최소금액 채우고 잔량은 마지막
            budgets = [0] * n
            remain = total
            for i in range(n - 1, -1, -1):
                if remain >= min_amt:
                    budgets[i] = min_amt
                    remain -= min_amt
                else:
                    break
            budgets[-1] += remain
        else:
            # 각 레벨에 최소금액 선배정 후, 잔액을 가중치로 분배 (마지막이 잔량 흡수)
            rest = total - min_amt * n
            extras = [int(math.floor(rest * q / raw_sum)) for q in raw]
            extras[-1] = rest - sum(extras[:-1])
            budgets = [min_amt + max(0, e) for e in extras]
            # 반올림 오차 보정
            drift = total - sum(budgets)
            budgets[-1] += drift

        for o, b in zip(orders, budgets):
            o['quantity'] = float(max(0, b))
            px = float(o.get('planned_price') or 0)
            o['volume'] = UpbitTickSystem.volume_for_krw(px, b) if px > 0 else 0.0

        got = sum(int(o['quantity']) for o in orders)
        print_log(LogLevel.INFO,
                  f"KRW plan budgets {[int(o['quantity']) for o in orders]} "
                  f"sum={got:,} / total={total:,} ({self.symbol})")

    @staticmethod
    def _allocate_split_krw(total_krw, n_splits):
        """레벨 KRW를 분할 슬롯에 정수 원으로 균등 분배. 마지막 슬롯이 잔량 흡수."""
        total_krw = int(max(0, total_krw))
        n_splits = int(max(1, n_splits))
        if n_splits <= 1:
            return [total_krw]
        base = total_krw // n_splits
        budgets = [base] * (n_splits - 1)
        budgets.append(total_krw - base * (n_splits - 1))
        return budgets

    def _adjust_to_split_lowest_base(self, drop_percentage):
        """각 레벨의 기준가를 '이전 레벨 분할의 최저가'를 출발점으로 재계산.
        레벨별 분할 개수(1/2/3)에 맞춰 최저가를 산출.
        레벨 n+1 간격은 레벨 n 분할 최저가에서 drop%*height_weight 만큼 하락."""
        if SPLIT_ORDER_MAX <= 1 or not self.active_planned_orders:
            return

        weight = self.weight
        for i in range(1, len(self.active_planned_orders)):
            order = self.active_planned_orders[i]
            n = order['level']
            height_weight = 1 + weight * (n - 1)
            prev = self.active_planned_orders[i - 1]
            prev_planned = prev['planned_price']
            prev_splits = UpbitTickSystem.generate_split_prices(
                prev_planned, split_count_for_level(prev['level']), SPLIT_STEP_PERCENT)
            prev_split_lowest = min(prev_splits)
            new_price = UpbitTickSystem.round_down(prev_split_lowest, drop_percentage * height_weight)
            order['planned_price'] = new_price
            order['original_planned_price'] = new_price
            order['volume'] = UpbitTickSystem.volume_for_krw(new_price, order['quantity'])

    def _reanchor_subsequent_from(self, base_level, base_price):
        """base_level의 실제 기준가(시세보정/체결가)로 이후 라운드 계획가를 재연쇄.
        L1만 끌어올리고 L2가 옛 계획가에 남는 '너무 뒤처짐' 방지."""
        dp = getattr(self, '_drop_percentage', None)
        if not dp or not base_price or base_price <= 0 or not self.active_planned_orders:
            return

        start = None
        for i, o in enumerate(self.active_planned_orders):
            if o['level'] == base_level:
                start = i
                break
        if start is None:
            return

        prev_planned = float(base_price)
        prev_level = base_level
        for i in range(start + 1, len(self.active_planned_orders)):
            order = self.active_planned_orders[i]
            if order['executed'] or order['level'] in self.pending_levels:
                # 이미 진행/완료된 레벨은 건드리지 않고 체인 기준만 갱신
                prev_planned = order['planned_price']
                prev_level = order['level']
                continue

            n = order['level']
            height_weight = 1 + self.weight * (n - 1)
            prev_splits = UpbitTickSystem.generate_split_prices(
                prev_planned, split_count_for_level(prev_level), SPLIT_STEP_PERCENT)
            prev_split_lowest = min(prev_splits) if prev_splits else prev_planned
            new_price = UpbitTickSystem.round_down(
                prev_split_lowest, dp * height_weight)
            tick = UpbitTickSystem.get_minimum_tick(prev_planned)
            if new_price >= prev_planned:
                new_price = UpbitTickSystem.snap_to_tick(prev_planned - tick, 'floor')
            if new_price <= 0:
                break

            old = order['planned_price']
            order['planned_price'] = new_price
            order['original_planned_price'] = new_price
            order['shift_applied'] = 0.0
            order['volume'] = UpbitTickSystem.volume_for_krw(new_price, order['quantity'])
            if abs(old - new_price) > tick * 0.5:
                print_log(LogLevel.INFO,
                          f"Reanchor L{n}: {old:.8f} → {new_price:.8f} "
                          f"(from L{base_level}@{base_price:.8f})")
            prev_planned = new_price
            prev_level = n

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
                curr['volume'] = UpbitTickSystem.volume_for_krw(new_price, curr['quantity'])
                print_log(LogLevel.INFO,
                          f"Enforced min tick gap at level {curr['level']}: "
                          f"{old_price} -> {new_price} (tick={tick})")

    def _plan_levels(self, drop_count):
        """실제 생성할 레벨 번호 (exclude_count 반영)."""
        return list(range(1, drop_count + 1 - self.exclude_count))

    def _calculate_linear_plan(self, drop_percentage, drop_count):
        levels = self._plan_levels(drop_count)
        total_weight = sum(levels) or 1
        for n in levels:
            height_weight = 1 + self.weight * (n - 1)
            planned_price = UpbitTickSystem.round_down(UpbitTickSystem.round_up(self.original_price), (n - 1) * (drop_percentage * height_weight))
            quantity = self.total_amount * n / total_weight
            self.active_planned_orders.append({
                'level': n,
                'original_planned_price': planned_price,
                'planned_price': planned_price,
                'quantity': quantity,
                'volume': quantity / planned_price if planned_price > 0 else 0,
                'executed': False,
                'shift_applied': 0.0
            })

    def _calculate_log_linear_plan(self, drop_percentage, drop_count, weight):
        levels = self._plan_levels(drop_count)
        total_weight = sum(n * math.log(n + weight) for n in levels) or 1
        for n in levels:
            height_weight = 1 + self.weight * (n - 1)
            planned_price = UpbitTickSystem.round_down(UpbitTickSystem.round_up(self.original_price), (n - 1) * (drop_percentage * height_weight))
            quantity = self.total_amount * (n * math.log(n + weight)) / total_weight
            self.active_planned_orders.append({
                'level': n,
                'original_planned_price': planned_price,
                'planned_price': planned_price,
                'quantity': quantity,
                'volume': quantity / planned_price if planned_price > 0 else 0,
                'executed': False,
                'shift_applied': 0.0
            })

    def _calculate_parabolic_plan(self, drop_percentage, drop_count):
        levels = self._plan_levels(drop_count)
        weight_factors = [(pow(n, 2) / 2) - (n / 2) + 1 for n in levels]
        total_weight = sum(weight_factors) or 1
        for n, wf in zip(levels, weight_factors):
            height_weight = 1 + self.weight * (n - 1)
            planned_price = UpbitTickSystem.round_down(UpbitTickSystem.round_up(self.original_price), (n - 1) * (drop_percentage * height_weight))
            quantity = self.total_amount * wf / total_weight
            self.active_planned_orders.append({
                'level': n,
                'original_planned_price': planned_price,
                'planned_price': planned_price,
                'quantity': quantity,
                'volume': quantity / planned_price if planned_price > 0 else 0,
                'executed': False,
                'shift_applied': 0.0
            })

    def _calculate_parabolic2_plan(self, drop_percentage, drop_count):
        levels = self._plan_levels(drop_count)
        weight_factors = [5 / 2 * pow(n, 2) + 5 / 2 * n + 5 for n in levels]
        total_weight = sum(weight_factors) or 1
        for n, wf in zip(levels, weight_factors):
            height_weight = 1 + self.weight * (n - 1)
            planned_price = UpbitTickSystem.round_down(UpbitTickSystem.round_up(self.original_price), (n - 1) * (drop_percentage * height_weight))
            quantity = self.total_amount * wf / total_weight
            self.active_planned_orders.append({
                'level': n,
                'original_planned_price': planned_price,
                'planned_price': planned_price,
                'quantity': quantity,
                'volume': quantity / planned_price if planned_price > 0 else 0,
                'executed': False,
                'shift_applied': 0.0
            })

    def _calculate_exponential_plan(self, drop_percentage, drop_count, exponent):
        levels = self._plan_levels(drop_count)
        h = len(levels)
        r = exponent
        if h <= 0:
            return
        a = self.total_amount * (r - 1) / (pow(r, h) - 1) if r != 1 else self.total_amount / h
        for i, n in enumerate(levels):
            height_weight = 1 + self.weight * (n - 1)
            planned_price = UpbitTickSystem.round_down(UpbitTickSystem.round_up(self.original_price), (n - 1) * (drop_percentage * height_weight))
            quantity = a * pow(r, i)
            self.active_planned_orders.append({
                'level': n,
                'original_planned_price': planned_price,
                'planned_price': planned_price,
                'quantity': quantity,
                'volume': quantity / planned_price if planned_price > 0 else 0,
                'executed': False,
                'shift_applied': 0.0
            })

    def _calculate_fibonacci_plan(self, drop_percentage, drop_count):
        fibonacci = [1, 1, 2, 2, 3, 3, 5, 5, 8, 8, 13, 13, 21, 21, 34, 34, 55, 55, 89, 89, 144, 144, 233, 233, 377, 377, 610, 610, 987, 987]
        levels = self._plan_levels(drop_count)
        my_fibonacci = fibonacci[:len(levels)]
        total_fibonacci = sum(my_fibonacci) or 1
        for n, fib in zip(levels, my_fibonacci):
            height_weight = 1 + self.weight * (n - 1)
            planned_price = UpbitTickSystem.round_down(UpbitTickSystem.round_up(self.original_price), (n - 1) * (drop_percentage * height_weight))
            quantity = self.total_amount * fib / total_fibonacci
            self.active_planned_orders.append({
                'level': n,
                'original_planned_price': planned_price,
                'planned_price': planned_price,
                'quantity': quantity,
                'volume': quantity / planned_price if planned_price > 0 else 0,
                'executed': False,
                'shift_applied': 0.0
            })

    def _calculate_explosive_plan(self, drop_percentage, drop_count):
        """지수 폭발 분배 — 하락 후반부로 갈수록 매집 비중이 급격히 폭발.
        매 단계의 배수를 누적 곱셈(복리)으로 누적하여 가중치 산출.
        단계별 배수: 1.4부터 공차 0.4 등차수열 (1.4, 1.8, 2.2, 2.6, 3.0, …).
        누적 가중치 = 이전 누적 × 현재 단계 배수."""
        levels = self._plan_levels(drop_count)
        multipliers = [1.4 + 0.4 * (n - 1) for n in levels]
        weights = []
        cumulative = 1.0
        for m in multipliers:
            cumulative *= m
            weights.append(cumulative)
        total_weight = sum(weights) or 1

        for n, w in zip(levels, weights):
            height_weight = 1 + self.weight * (n - 1)
            planned_price = UpbitTickSystem.round_down(UpbitTickSystem.round_up(self.original_price), (n - 1) * (drop_percentage * height_weight))
            quantity = self.total_amount * w / total_weight
            self.active_planned_orders.append({
                'level': n,
                'original_planned_price': planned_price,
                'planned_price': planned_price,
                'quantity': quantity,
                'volume': quantity / planned_price if planned_price > 0 else 0,
                'executed': False,
                'shift_applied': 0.0
            })

    def _calculate_required_shift(self, current_low_price):
        """필요한 밀림량 계산 - 저가 기준. O(L) — set 재구축 없음."""
        required_shift = 0.0
        pending_levels = self.pending_levels
        partial_levels = self.partial_levels

        for order in self.active_planned_orders:
            if order['executed']:
                continue
            lv = order['level']
            if lv in pending_levels or lv in partial_levels:
                continue
            if order['planned_price'] > current_low_price:
                gap = order['planned_price'] - current_low_price
                if gap > required_shift:
                    required_shift = gap

        min_shift = self._min_shift_tick
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
                order['volume'] = UpbitTickSystem.volume_for_krw(
                    order['planned_price'], order['quantity'])
        
        self.plan_shift_amount = shift_amount
        print_log(LogLevel.SUCCESS, f"Plan shifted by {shift_amount:.4f} {self.symbol}")

    def skip_level1_for_resume(self, anchor_price=None):
        """보유 재개 시 L1 재매수 금지 — 이미 산 물량으로 L1 완료 처리."""
        if not self.active_planned_orders:
            return
        order = self.active_planned_orders[0]
        if order.get('level') != 1 or order.get('executed'):
            return
        px = float(anchor_price or 0)
        if px <= 0:
            px = float(getattr(self, 'current_price', 0) or 0)
        if px <= 0:
            px = float(order.get('planned_price') or 0)
        order['executed'] = True
        self.executed_count = max(int(self.executed_count or 0), 1)
        self.last_executed_level = 1
        self.last_executed_price = px
        self.partial_levels.discard(1)
        self.level_fill_count[1] = max(
            int(self.level_fill_count.get(1, 0) or 0),
            split_count_for_level(1))
        self.first_order_start_time = None
        if px > 0:
            self._reanchor_subsequent_from(1, px)
        print_log(LogLevel.WARNING,
                  f"Resume holdings — skip L1 re-buy (anchor={px:.4f})")

    def execute_dynamic_buy_orders(self, skip_level1=False):
        """동적 매수 시작. skip_level1=True면 보유 재개(L1 재주문 금지)."""
        if not self.active_planned_orders:
            print_log(LogLevel.ERROR, "No planned orders to execute")
            return False

        self.is_active = True
        self.pending_orders.clear()
        self._reset_runtime_indexes()
        self.plan_shift_amount = 0.0
        self._failed_replaces = []
        self._last_replace_retry = 0.0
        self._last_fill_rest_check = 0.0
        self.last_shift_check_price = self.current_price
        self._first_order_timed_out = False
        self._buys_halted = False
        self._cycle_ended = False

        if skip_level1:
            self.skip_level1_for_resume(self.current_price)
            self.first_order_start_time = None
            print_log(LogLevel.SUCCESS,
                      f"Resuming ladder after L1 skip — "
                      f"{len(self.active_planned_orders)} planned "
                      f"(exec={self.executed_count})")
            # L2 이하는 가격 도달 시 check_and_continue가 진행
            return True

        self.first_order_start_time = time.time()
        print_log(LogLevel.SUCCESS,
                  f"Starting dynamic buying with {len(self.active_planned_orders)} "
                  f"planned orders based on low price {self.low_price:.4f}")
        
        ok = self._execute_next_available_order()
        return ok

    def check_and_continue(self, cached_price=None):
        """체결 확인 + 동적 밀림 + 다음 주문.
        단일스레드 규칙을 워커마다 계승:
          1) 가격하락 → plan_shift
          2) 첫주문 3초 미체결(체결 0) → 취소 + is_active=False (재호가 추격 없음)
          3) 체결/빈 pending → 다음 레벨 POST
        """
        if getattr(self, '_cycle_ended', False):
            return False
        if not self.is_active:
            return False

        current_price = cached_price if cached_price else RealMarketData.get_current_price(self.symbol)
        if not current_price:
            return False
        clean, bad = RealMarketData.sanitize_symbol_price(
            self.symbol, current_price)
        if bad and clean:
            current_price = clean
        elif bad:
            current_price = RealMarketData.get_current_price(self.symbol)
            if not current_price:
                return False
        self.current_price = current_price

        # ★ 워커별 동적 슬라이딩 — 고전: 타임아웃보다 먼저
        self._check_and_apply_plan_shift(current_price)

        # ★ 고전 첫주문 타임아웃: 체결 0 + pending → 취소·중단 (재호가 금지)
        if self._should_first_order_timeout():
            return self._classic_first_order_timeout()

        has_new_execution = self._check_order_execution()

        if self.partial_levels and self.pending_orders:
            lv = min(self.partial_levels)
            logged = getattr(self, '_partial_wait_logged_levels', None)
            if logged is None:
                logged = set()
                self._partial_wait_logged_levels = logged
            if lv not in logged:
                logged.add(lv)
                filled = self.level_fill_count.get(lv, 0)
                pend_n = sum(1 for p in self.pending_orders if p.get('level') == lv)
                print_log(LogLevel.SUCCESS,
                          f"Level {lv} waiting splits: filled={filled}, "
                          f"pending={pend_n} — next level held")

        # pending 없어도 partial/실패큐 처리 후 다음 레벨 진행
        if has_new_execution or len(self.pending_orders) == 0:
            return self._execute_next_available_order()

        return False

    def _check_and_apply_plan_shift(self, current_price):
        """계획 밀림 확인 및 적용 - 저가 기준.
        가격이 하락하지 않으면 스캔 스킵 O(1).
        진행 중/부분체결 라운드 주문은 취소·밀림하지 않음."""
        # 하락 없으면 밀림 불필요 — 매 iteration O(L) 스캔 제거
        last = self.last_shift_check_price
        if last is not None and current_price >= last:
            return
        self.last_shift_check_price = current_price

        required_shift = self._calculate_required_shift(current_price)
        if required_shift <= 0:
            return

        protected_levels = self.pending_levels | self.partial_levels

        orders_to_cancel = []
        # uuid 스냅샷 — 체결 pop/동시 수정에도 안전
        for pending_order in list(self.pending_orders):
            if pending_order['level'] in protected_levels:
                continue
            order_uuid = pending_order.get('uuid')
            if not order_uuid or order_uuid not in self.pending_by_uuid:
                continue
            order_info = self._get_order_info(order_uuid, force_rest=False)
            if not order_info:
                continue
            if self._order_is_filled(order_info):
                self._process_executed_order(pending_order, None, order_info)
                continue
            state = str(order_info.get('state', '')).lower()
            if state != 'cancel':
                orders_to_cancel.append(pending_order)

        if orders_to_cancel:
            print_log(LogLevel.INFO,
                      f"Cancelling {len(orders_to_cancel)} pending orders due to plan shift")
            cancel_uuids = [p.get('uuid') for p in orders_to_cancel if p.get('uuid')]
            self.order_canceler.cancel_orders_parallel(cancel_uuids)
            global buy_uuids
            buy_uuids.difference_update(cancel_uuids)
            self._remove_pendings_by_uuids(set(cancel_uuids))

        print_log(LogLevel.INFO, f"Applying plan shift: {required_shift:.4f} {self.symbol}")
        for order in self.active_planned_orders:
            if order['executed'] or order['level'] in protected_levels:
                continue
            new_planned_price = UpbitTickSystem.round_down(
                order['original_planned_price'] - required_shift, 0)
            order['planned_price'] = new_planned_price
            order['shift_applied'] = required_shift
            order['volume'] = (
                order['quantity'] / new_planned_price if new_planned_price > 0 else 0
            )
        self.plan_shift_amount = required_shift
        print_log(LogLevel.SUCCESS, f"Plan shifted by {required_shift:.4f} {self.symbol}")

        self._execute_next_available_order()

    def _requote_first_buy(self, live_price):
        """첫 매수 미체결 타임아웃 시 — 1라운드 가격을 즉시체결가로 강제 상향."""
        if not live_price or live_price <= 0:
            return
        for order in self.active_planned_orders:
            if order['executed']:
                continue
            fillable = self._fillable_buy_price(0, live_price)  # planned 무시, 시세만
            print_log(LogLevel.INFO,
                      f"Requote level {order['level']}: "
                      f"{order['planned_price']:.8f} → {fillable:.8f}")
            order['planned_price'] = fillable
            order['original_planned_price'] = fillable
            order['volume'] = UpbitTickSystem.volume_for_krw(fillable, order['quantity'])
            self._reanchor_subsequent_from(order['level'], fillable)
            break

    def _requote_via_cancel_and_new(self, live_price):
        """업비트 POST /v1/orders/cancel_and_new — L1 재호가를 1 RTT로.
        성공 시 pending UUID를 신규 주문으로 교체하고 True."""
        if not EXCHANGE.get('supports_cancel_and_new'):
            return False
        if not self.pending_orders or not live_price or live_price <= 0:
            return False
        # L1 삼중매수 — 첫 pending만 cancel_and_new (나머지는 레거시 재호가 경로)
        pending = self.pending_orders[0]
        prev_uuid = pending.get('uuid')
        if not prev_uuid:
            return False
        fillable = self._fillable_buy_price(0, live_price)
        # 계획가 선반영
        self._requote_first_buy(live_price)
        data = OrderCanceler.cancel_and_new_order(
            prev_uuid, fillable, 'remain_only', 'limit')
        if not data:
            return False
        new_uuid = data.get('new_order_uuid')
        if not new_uuid:
            # 신규 UUID 없으면 레거시 경로로 넘김 (취소만 됐을 수 있음)
            return False
        global buy_uuids
        buy_uuids.discard(prev_uuid)
        buy_uuids.add(new_uuid)
        if private_ws._is_initialized:
            private_ws.unregister_order_wait(prev_uuid)
            with private_ws._order_lock:
                private_ws.order_cache.setdefault(new_uuid, {
                    'uuid': new_uuid, 'state': 'wait',
                    'executed_volume': '0',
                    'remaining_volume': pending.get('volume', 0),
                    'executed_funds': '0',
                })
            private_ws.register_order_wait(new_uuid)
        self._remove_pendings_by_uuids({prev_uuid})
        self._add_pending({
            'level': pending.get('level', 1),
            'planned_price': fillable,
            'actual_price': fillable,
            'volume': pending.get('volume', 0),
            'order_time': time.time(),
            'uuid': new_uuid,
            'split_idx': pending.get('split_idx', 0),
            'split_total': pending.get('split_total', 1),
        })
        print_log(LogLevel.SUCCESS,
                  f"L1 requote via cancel_and_new @ {fillable:.8f}")
        return True

    def _fillable_buy_price(self, planned_price, live_price=None):
        """즉시 체결용 매수가 — 최근가+여유틱 (스프레드/지연 흡수).
        단순 +1틱은 호가 스프레드에 막혀 3초 타임아웃이 반복될 수 있음."""
        live = live_price or getattr(self, 'current_price', None) \
            or RealMarketData.get_current_price(self.symbol)
        if not live or live <= 0:
            return planned_price
        tick = UpbitTickSystem.get_minimum_tick(live)
        # 최소 3틱 또는 0.15% 중 큰 쪽으로 상향 → 매수 호가 관통
        pad = max(tick * 3, live * 0.0015)
        fillable = UpbitTickSystem.round_up(live + pad)
        return max(planned_price, fillable)

    def _px_key(self, price):
        """호가 비교용 키 — 동일 틱이면 동일 문자열."""
        try:
            p = float(price or 0)
        except (TypeError, ValueError):
            return ''
        if p <= 0:
            return ''
        return UpbitTickSystem.format_order_price(
            UpbitTickSystem.snap_to_tick(p))

    def _holdings_avg_px(self):
        """현재 보유 평단 (WS). 없으면 0."""
        try:
            if private_ws._is_initialized and private_ws.is_connected:
                bal, locked, avg = private_ws.get_symbol_info(self.symbol)
                if (bal + locked) >= MIN_HOLDING_VOLUME and avg and avg > 0:
                    return float(avg)
        except Exception:
            pass
        return 0.0

    def _is_forbidden_buy_price(self, price):
        """동일 워커에서 절대 걸면 안 되는 매수가.
        - 이미 pending인 호가
        - 현재 보유 평단과 동일 틱
        - 직전 체결가와 동일 틱
        - 타임아웃으로 막힌 호가"""
        key = self._px_key(price)
        if not key:
            return True, 'invalid'
        for p in (self.pending_orders or []):
            if self._px_key(p.get('actual_price') or p.get('planned_price')) == key:
                return True, 'pending-same'
        avg = self._holdings_avg_px()
        if avg > 0 and self._px_key(avg) == key:
            return True, 'same-avg'
        if key in (self._blocked_buy_px_keys or set()):
            return True, 'blocked'
        le = float(getattr(self, 'last_executed_price', 0) or 0)
        if le > 0 and self._px_key(le) == key:
            return True, 'last-fill-same'
        return False, ''

    def _prune_blocked_buy_px(self, live_price):
        """시세에서 멀리 떨어진 금지호가 정리 — 무한 하향 chase 방지."""
        if not self._blocked_buy_px_keys or not live_price or live_price <= 0:
            return
        keep = set()
        for k in list(self._blocked_buy_px_keys):
            try:
                pk = float(k)
            except (TypeError, ValueError):
                continue
            # live ±3% 안만 유지 (너무 먼 금지는 폐기)
            if abs(pk - live_price) / live_price <= 0.03:
                keep.add(k)
        self._blocked_buy_px_keys = keep

    def _next_distinct_buy_price(self, preferred, prefer_down=True, floor=None, ceiling=None):
        """preferred와 금지호가를 피해 최소 1틱 이상 다른 매수가.
        floor/ceiling으로 시세 이탈 하한·상한 제한."""
        try:
            px = float(preferred or 0)
        except (TypeError, ValueError):
            px = 0.0
        if px <= 0:
            px = float(self.current_price or 0) or 0.0
        if px <= 0:
            return 0.0
        cursor = UpbitTickSystem.snap_to_tick(px)
        for i in range(40):
            bad, _why = self._is_forbidden_buy_price(cursor)
            if not bad:
                if floor is not None and cursor + 1e-15 < float(floor):
                    pass
                elif ceiling is not None and cursor > float(ceiling) + 1e-15:
                    pass
                else:
                    return cursor
            tick = UpbitTickSystem.get_minimum_tick(cursor)
            step = tick * max(1, (i // 2) + 1)
            if prefer_down:
                nxt = UpbitTickSystem.snap_to_tick(cursor - step, 'floor')
                if floor is not None and nxt + 1e-15 < float(floor):
                    # 하한 도달 — 위로 전환
                    prefer_down = False
                    cursor = UpbitTickSystem.snap_to_tick(
                        max(cursor, float(floor)) + tick, 'ceil')
                    continue
                cursor = nxt
            else:
                nxt = UpbitTickSystem.snap_to_tick(cursor + step, 'ceil')
                if ceiling is not None and nxt > float(ceiling) + 1e-15:
                    break
                cursor = nxt
            if cursor <= 0:
                break
        return 0.0

    def _wait_exchange_bids_cleared(self, timeout=1.0):
        """타임아웃 취소 후 거래소 bid가  lag로 남는 동안 재POST 금지."""
        deadline = time.time() + float(timeout)
        while time.time() < deadline:
            try:
                n = int(self._exchange_open_buy_count() or 0)
            except Exception:
                n = 0
            if n <= 0:
                return True
            time.sleep(0.05)
        try:
            return int(self._exchange_open_buy_count() or 0) <= 0
        except Exception:
            return False

    def _clear_pending_tracking_only(self):
        """pending 추적만 비움 — executed/plan/next_plan_idx 유지."""
        self.pending_orders.clear()
        if isinstance(self.pending_by_uuid, dict):
            self.pending_by_uuid.clear()
        else:
            self.pending_by_uuid = {}
        self.pending_levels.clear()
        self.pending_level_counts.clear()
        self.pending_split_keys.clear()

    def _pending_unfilled_age(self):
        """미체결 pending 중 가장 오래된 주문 경과초. 없으면 0."""
        if not self.pending_orders:
            return 0.0
        now = time.time()
        oldest = None
        for p in self.pending_orders:
            t = p.get('order_time')
            if t is None:
                continue
            try:
                t = float(t)
            except (TypeError, ValueError):
                continue
            if oldest is None or t < oldest:
                oldest = t
        if oldest is None:
            if self.first_order_start_time:
                return max(0.0, now - float(self.first_order_start_time))
            return 0.0
        return max(0.0, now - oldest)

    def _has_any_level_fill(self):
        """실제 체결(수량>0)이 있으면 True. level_fill_count={1:0} 오탐 방지."""
        if self.partial_levels:
            return True
        for v in (self.level_fill_count or {}).values():
            try:
                if int(v) > 0:
                    return True
            except (TypeError, ValueError):
                pass
        return False

    def _should_first_order_timeout(self):
        """고전: 체결 0 + pending + first_order_timeout 초과."""
        if getattr(self, '_cycle_ended', False):
            return False
        if getattr(self, '_ma_gate_paused', False):
            return False
        if not self.first_order_start_time:
            return False
        if not self.pending_orders:
            return False
        # 체결(레벨완료·부분)이 있으면 타임아웃 중단 안 함 — plan_shift/사다리 유지
        if self.executed_count > 0 or self._has_any_level_fill():
            return False
        age = self._pending_unfilled_age()
        if age <= 0:
            age = time.time() - float(self.first_order_start_time)
        return age > float(self.first_order_timeout or 3.0)

    def _classic_first_order_timeout(self):
        """고전 첫주문 타임아웃: 미체결 취소 + is_active=False. 재호가 추격 없음."""
        age = self._pending_unfilled_age()
        if age <= 0 and self.first_order_start_time:
            age = time.time() - float(self.first_order_start_time)
        print_log(LogLevel.WARNING,
                  f"First order timeout after {age:.0f} seconds")
        try:
            self._cancel_pending_sync()
        except Exception:
            pass
        try:
            OrderCanceler().cancel_buy_orders_for_symbol(
                self.symbol, verify=False)
        except Exception:
            pass
        self._clear_pending_tracking_only()
        self._failed_replaces = []
        self.is_active = False
        self._first_order_timed_out = True
        return False

    def _should_timeout_requote_pending(self):
        """호환 alias → 고전 첫주문 타임아웃."""
        return self._should_first_order_timeout()

    def _force_timeout_requote(self, live_price=None):
        """호환 alias → 고전 취소+중단 (상향재호가 제거)."""
        if not self._should_first_order_timeout():
            return False
        return self._classic_first_order_timeout()

    def _execute_next_available_order(self):
        """다음 실행 가능한 주문 실행 — cursor + O(1) 세트 조회."""
        if not self.is_active:
            return False
        if not self._buys_allowed():
            return False

        if self.pending_orders:
            return False

        if time.time() < float(getattr(self, '_skip_place_until', 0) or 0):
            return False

        # 타임아웃 취소 직후엔 옛 bid 재흡수 금지 (재호가 무력화 버그)
        if time.time() < float(getattr(self, '_skip_adopt_until', 0) or 0):
            pass
        else:
            try:
                if self._adopt_exchange_bids_if_any():
                    return False
            except Exception:
                pass

        # 로컬 pending 없는데 거래소 bid 잔존 → 재POST 금지 (CAP 루프)
        try:
            ex_n = int(self._exchange_open_buy_count() or 0)
        except Exception:
            ex_n = 0
        if ex_n > 0 and not self.pending_orders:
            if time.time() < float(getattr(self, '_skip_adopt_until', 0) or 0):
                return False
            try:
                if self._adopt_exchange_bids_if_any():
                    return False
            except Exception:
                pass
            try:
                OrderCanceler().cancel_buy_orders_for_symbol(
                    self.symbol, verify=False)
            except Exception:
                pass
            self._skip_place_until = time.time() + 1.0
            print_log(LogLevel.WARNING,
                      f"{self.symbol}: 거래소 bid {ex_n}건 잔존 "
                      f"— 정리 후 재POST 보류")
            return False

        # ★ 워커당 미체결 매수 3개 꽉 참 → 다음 레벨 대기(정상). 로그 없음.
        if self._open_buy_slots_left(check_exchange=True) <= 0:
            return False

        # 분할 POST 실패 재주문 큐가 있으면 전체 레벨 재주문 금지 (중복 주문 방지)
        if self._failed_replaces:
            return False

        # 완료된 레벨이 partial에 남은 경우 정리 (다음 라운드 영구 차단 버그 방지)
        for lv in list(self.partial_levels):
            if self._is_level_executed(lv):
                self.partial_levels.discard(lv)

        # 부분 체결만 남은 라운드 — 전체 재주문 금지 (누락 분할 재주문 경로만 사용)
        if self.partial_levels:
            return False

        plans = self.active_planned_orders
        n = len(plans)
        idx = self.next_plan_idx
        while idx < n:
            order = plans[idx]
            if order['executed']:
                idx += 1
                continue
            if order['level'] in self.pending_levels:
                self.next_plan_idx = idx
                return False
            self.next_plan_idx = idx
            return self._execute_single_order(order)

        self.next_plan_idx = n
        if self.executed_count >= n:
            print_log(LogLevel.SUCCESS, "All planned orders executed!")
            self.is_active = False
            return False

        return False

    def _execute_single_order(self, order):
        """주문 실행 — 전 라운드 삼중매수.
        각 주문 금액이 MIN_ORDER_AMOUNT 미만이면 분할 금지하고 단일 폴백.
        마지막 레벨은 잔여 KRW 전액 흡수 후 동일하게 삼중 분할.
        ★ 워커당 미체결 매수 3개까지 허용, 4개부터 금지."""
        level = order.get('level')
        if order.get('executed') or self._is_level_executed(level):
            print_log(LogLevel.WARNING,
                      f"Order {level} already executed — skip re-place")
            return False
        # 부분체결/재주문 큐가 있는데 전체 라운드를 다시 깔면 9건 중복 발생
        if level in self.partial_levels or self.level_fill_count.get(level, 0) > 0:
            print_log(LogLevel.WARNING,
                      f"Order {level} has fills/partial — "
                      f"full re-place blocked (use split retry only)")
            return False
        if level in self.pending_levels:
            print_log(LogLevel.WARNING,
                      f"Order {level} still pending — skip duplicate place")
            return False

        # 거래소+로컬 hard 기준 — 슬롯 0이면 대기
        slots = self._open_buy_slots_left(check_exchange=True)
        if slots <= 0:
            return False

        order_price = order['planned_price']
        order_volume = order['volume']
        if order_price <= 0 or order_volume <= 0:
            print_log(LogLevel.ERROR,
                      f"Order {order['level']} invalid price/volume "
                      f"({order_price}/{order_volume})")
            return False

        # 레벨 KRW 예산 (정수 원) — quantity가 예산 소스
        level_krw = int(math.floor(float(order.get('quantity') or 0) + 1e-9))

        # 1라운드(첫 매수): 체결 가능가로 보정 — KRW 예산은 유지, 수량만 재계산
        if order['level'] == 1 and self.executed_count == 0:
            bumped = self._fillable_buy_price(order_price, getattr(self, 'current_price', None))
            if bumped > order_price:
                print_log(LogLevel.INFO,
                          f"Level 1 price bump for fill: {order_price:.8f} → {bumped:.8f}")
                order_price = bumped
                order['planned_price'] = bumped
                order['original_planned_price'] = bumped
                order_volume = UpbitTickSystem.volume_for_krw(bumped, level_krw)
                order['volume'] = order_volume
                self._reanchor_subsequent_from(1, bumped)

        # 마지막 레벨 — 남은 가용 KRW 전액 흡수 후 삼중 분할로 진행
        is_last_level = (order is self.active_planned_orders[-1])

        if is_last_level:
            full_price = UpbitTickSystem.round_down(order_price, 1.0)
            try:
                if private_ws._is_initialized and private_ws.is_connected:
                    krw_left = private_ws.get_krw_balance(1)
                else:
                    krw_left = AccountChecker().get_krw_balance()
            except Exception:
                krw_left = level_krw
            krw_left = int(math.floor(float(krw_left) + 1e-9))
            if krw_left < MIN_ORDER_AMOUNT:
                print_log(LogLevel.WARNING,
                          f"LAST level skip — KRW left {krw_left:,} < {MIN_ORDER_AMOUNT}")
                return False
            # 수수료 락 여유를 둔 전액 흡수 (volume_for_krw가 COMMISSION 적용)
            spendable = int(math.floor(krw_left * COMMISSION + 1e-9))
            order_price = full_price
            order['planned_price'] = full_price
            level_krw = spendable
            order['quantity'] = float(spendable)
            order['volume'] = UpbitTickSystem.volume_for_krw(full_price, spendable)
            print_log(LogLevel.INFO,
                      f"Executing LAST order {order['level']} - "
                      f"triple full-absorb @ {full_price:.4f}, "
                      f"KRW left={krw_left:,} (fee-reserve spendable={spendable:,})")

        # ===== 전 라운드: KRW 균등 삼중 분할 → 각 호가에서 수량 환산 =====
        if level_krw < MIN_ORDER_AMOUNT:
            print_log(LogLevel.ERROR,
                      f"Order {order['level']} KRW budget {level_krw:,} < {MIN_ORDER_AMOUNT}")
            return False

        n_want = split_count_for_level(order['level'])
        # 틱 사다리로 생성 — format 키가 겹치지 않게 보장
        raw_prices = UpbitTickSystem._tick_ladder_prices(order_price, n_want)
        if len(raw_prices) < n_want:
            raw_prices = UpbitTickSystem.generate_split_prices(
                order_price, n_want, SPLIT_STEP_PERCENT)
        split_prices = []
        seen = set()
        for sp in (raw_prices or [order_price]):
            sp = UpbitTickSystem.snap_to_tick(sp)
            key = UpbitTickSystem.format_order_price(sp)
            if not key or key in seen:
                # 겹치면 1틱 아래 재시도
                tick = UpbitTickSystem.get_minimum_tick(sp if sp > 0 else order_price)
                for _ in range(8):
                    sp = UpbitTickSystem.snap_to_tick(sp - tick, 'floor')
                    if sp <= 0:
                        break
                    key = UpbitTickSystem.format_order_price(sp)
                    if key and key not in seen:
                        break
                else:
                    continue
            if key in seen:
                continue
            seen.add(key)
            split_prices.append(sp)
        n_splits = len(split_prices) or 1
        if n_splits < n_want and n_want > 1:
            # 부족분 틱 사다리 보충
            cursor = split_prices[-1] if split_prices else UpbitTickSystem.snap_to_tick(order_price)
            while len(split_prices) < n_want:
                tick = UpbitTickSystem.get_minimum_tick(cursor)
                cursor = UpbitTickSystem.snap_to_tick(cursor - tick, 'floor')
                if cursor <= 0:
                    break
                key = UpbitTickSystem.format_order_price(cursor)
                if key and key not in seen:
                    seen.add(key)
                    split_prices.append(cursor)
            n_splits = len(split_prices) or 1

        # ★ 슬롯 상한 — 남은 자리만큼만 분할 (절대 3 초과 계획 금지)
        # ★ 슬롯이 줄어도 level_krw 전액을 재배분 (slice로 /n만 쓰는 버그 금지)
        slots = self._open_buy_slots_left(check_exchange=True)
        if slots <= 0:
            return False
        if n_splits > slots:
            split_prices = split_prices[:slots]
            n_splits = slots

        # 분할당 KRW가 최소주문 미만이면 단일 폴백
        if n_splits > 1 and (level_krw // n_splits) < MIN_ORDER_AMOUNT:
            print_log(LogLevel.INFO,
                      f"Order {order['level']} split skipped — per-split KRW "
                      f"{level_krw // n_splits:,} < {MIN_ORDER_AMOUNT}")
            split_prices = [UpbitTickSystem.snap_to_tick(order_price)]
            n_splits = 1

        split_krw = self._allocate_split_krw(level_krw, n_splits)
        split_volumes = [
            UpbitTickSystem.volume_for_krw(sp, k)
            for sp, k in zip(split_prices, split_krw)
        ]

        # 수량 0 슬롯의 KRW를 마지막 유효 슬롯으로 흡수
        if any(v <= 0 for v in split_volumes):
            kept_p, kept_k = [], []
            orphan_krw = 0
            for sp, k, v in zip(split_prices, split_krw, split_volumes):
                if v > 0:
                    kept_p.append(sp)
                    kept_k.append(k)
                else:
                    orphan_krw += k
            if not kept_p:
                # 전량 단일 재시도
                split_prices = [UpbitTickSystem.snap_to_tick(order_price)]
                split_krw = [level_krw]
                split_volumes = [UpbitTickSystem.volume_for_krw(split_prices[0], level_krw)]
                n_splits = 1
            else:
                kept_k[-1] += orphan_krw
                split_prices = kept_p
                split_krw = kept_k
                split_volumes = [
                    UpbitTickSystem.volume_for_krw(sp, k)
                    for sp, k in zip(split_prices, split_krw)
                ]
                n_splits = len(split_prices)

        if not split_volumes or all(v <= 0 for v in split_volumes):
            print_log(LogLevel.ERROR,
                      f"Order {order['level']} volumes <= 0 for KRW={level_krw:,}")
            return False

        est_spend = sum(
            UpbitTickSystem.snap_to_tick(sp) * v
            for sp, v in zip(split_prices, split_volumes))
        print_log(LogLevel.INFO,
                 f"Executing order {order['level']} - "
                 f"KRW budget={level_krw:,} (est spend≈{est_spend:,.0f}), "
                 f"splits={n_splits} krw={split_krw} "
                 f"vol={[round(v, 8) for v in split_volumes]} @ "
                 f"{[UpbitTickSystem.format_order_price(p) for p in split_prices]}")

        # 삼중매수: 슬롯 재확인하며 순차 POST — 4번째 시도 자체를 안 함
        # ★ 분할 사이는 로컬 카운트만 (거래소 open-bid GET 제거 → POST 연타)
        success_count = 0
        rate_hits = 0
        placed_krw = 0
        first_unattempted = len(split_prices)  # 전부 시도했다고 가정
        with self._buy_place_lock:
            slots = self._open_buy_slots_left(check_exchange=True)
            if slots <= 0:
                return False
            if n_splits > slots:
                # ★ 전액 재배분 — split_krw[:slots] slice 금지 (전액/n만 사는 버그)
                split_prices = split_prices[:slots]
                n_splits = slots
                if n_splits > 1 and (level_krw // n_splits) < MIN_ORDER_AMOUNT:
                    split_prices = [UpbitTickSystem.snap_to_tick(order_price)]
                    n_splits = 1
                split_krw = self._allocate_split_krw(level_krw, n_splits)
                split_volumes = [
                    UpbitTickSystem.volume_for_krw(sp, k)
                    for sp, k in zip(split_prices, split_krw)
                ]
            first_unattempted = len(split_prices)
            for idx, sp in enumerate(split_prices):
                if self._open_buy_slots_left(check_exchange=False) <= 0:
                    first_unattempted = idx
                    break
                order_uuid = self.place_dynamic_buy_order(
                    sp, split_volumes[idx], 'floor')
                if order_uuid:
                    pending_order = {
                        'level': order['level'],
                        'planned_price': order['planned_price'],
                        'actual_price': sp,
                        'volume': split_volumes[idx],
                        'krw_budget': split_krw[idx] if idx < len(split_krw) else 0,
                        'order_time': time.time(),
                        'uuid': order_uuid,
                        'split_idx': idx,
                        'split_total': n_splits,
                    }
                    if not self._add_pending(pending_order):
                        first_unattempted = idx + 1
                        break
                    success_count += 1
                    placed_krw += int(split_krw[idx] if idx < len(split_krw) else 0)
                else:
                    err = str(getattr(self, '_last_buy_error', '') or '').lower()
                    if 'max_open_buys' in err:
                        first_unattempted = idx
                        break
                    if 'rate_limit' in err or '429' in err:
                        rate_hits += 1
                        if self._open_buy_slots_left(check_exchange=False) <= 0:
                            first_unattempted = idx
                            break
                        order_uuid = self.place_dynamic_buy_order(
                            sp, split_volumes[idx], 'floor')
                    if order_uuid:
                        if not self._add_pending({
                            'level': order['level'],
                            'planned_price': order['planned_price'],
                            'actual_price': sp,
                            'volume': split_volumes[idx],
                            'krw_budget': split_krw[idx] if idx < len(split_krw) else 0,
                            'order_time': time.time(),
                            'uuid': order_uuid,
                            'split_idx': idx,
                            'split_total': n_splits,
                        }):
                            first_unattempted = idx + 1
                            break
                        success_count += 1
                        placed_krw += int(split_krw[idx] if idx < len(split_krw) else 0)
                    else:
                        if 'max_open_buys' in str(
                                getattr(self, '_last_buy_error', '') or '').lower():
                            first_unattempted = idx
                            break
                        self._enqueue_failed_replace({
                            'level': order['level'],
                            'planned_price': order['planned_price'],
                            'actual_price': sp,
                            'volume': split_volumes[idx],
                            'krw_budget': split_krw[idx] if idx < len(split_krw) else 0,
                            'split_idx': idx,
                            'split_total': n_splits,
                        })

            # ★ 미시도 슬롯 KRW는 1호가로 몰아 재시도 (slice/중도 break → /n만 사는 버그)
            if first_unattempted < len(split_krw):
                remain_krw = sum(int(k) for k in split_krw[first_unattempted:])
                if remain_krw >= MIN_ORDER_AMOUNT:
                    px = (split_prices[first_unattempted]
                          if first_unattempted < len(split_prices)
                          else UpbitTickSystem.snap_to_tick(order_price))
                    vol = UpbitTickSystem.volume_for_krw(px, remain_krw)
                    if vol > 0:
                        self._enqueue_failed_replace({
                            'level': order['level'],
                            'planned_price': order['planned_price'],
                            'actual_price': px,
                            'volume': vol,
                            'krw_budget': remain_krw,
                            'split_idx': first_unattempted,
                            'split_total': max(n_splits, 1),
                        })
                        print_log(LogLevel.WARNING,
                                  f"Order {order['level']} remain KRW={remain_krw:,} "
                                  f"queued (placed {success_count}/{n_splits}, "
                                  f"spent={placed_krw:,}/{level_krw:,})")

        if success_count > 0:
            print_log(LogLevel.SUCCESS,
                      f"Order {order['level']} placed ({success_count}/{n_splits} splits)")
            self._enforce_max_open_buys()
            return True
        if 'max_open_buys' in str(getattr(self, '_last_buy_error', '') or '').lower():
            return False
        if rate_hits >= n_splits:
            print_log(LogLevel.WARNING,
                      f"Order {order['level']} deferred — rate limit, queued for retry")
        else:
            print_log(LogLevel.ERROR,
                      f"Failed to place order {order['level']} (all splits failed)")
        return False

    def _is_order_executed(self, order_info):
        """레거시 호환 — order_is_filled와 동일."""
        return order_is_filled(normalize_order(order_info) if order_info else None)

    def _is_level_executed(self, level):
        order = self.planned_by_level.get(level)
        return bool(order and order.get('executed'))

    def _level_fill_count(self, level):
        return self.level_fill_count.get(level, 0)

    def _enqueue_failed_replace(self, pending_like):
        """분할 재주문 큐에 추가 — 동일 (level, split_idx) 중복 방지."""
        if not pending_like:
            return
        level = pending_like.get('level')
        split_idx = pending_like.get('split_idx', 0)
        key = (level, split_idx)
        for existing in self._failed_replaces:
            if (existing.get('level'), existing.get('split_idx', 0)) == key:
                return
        self._failed_replaces.append(pending_like)

    def _dequeue_failed_replace(self, level, split_idx):
        """재주문 성공/불필요 시 큐에서 제거."""
        key = (level, split_idx)
        self._failed_replaces = [
            p for p in self._failed_replaces
            if (p.get('level'), p.get('split_idx', 0)) != key
        ]

    def _has_failed_replace_for_level(self, level):
        return any(p.get('level') == level for p in self._failed_replaces)

    def _complete_level_from_fills(self, level, fallback_price):
        """부분 분할만으로 레벨 완료 처리 — 다음 라운드 영구 차단 해제."""
        order = self.planned_by_level.get(level)
        newly_done = False
        if order and not order.get('executed'):
            order['executed'] = True
            self.executed_count += 1
            newly_done = True
            print_log(LogLevel.SUCCESS,
                      f"Level {level} complete ({self.executed_count}/"
                      f"{len(self.active_planned_orders)})")
        self.partial_levels.discard(level)
        if newly_done:
            fills = [e for e in self.executed_orders if e['level'] == level]
            vol_sum = sum(e['volume'] for e in fills)
            if vol_sum > 0:
                vwap = sum(e['executed_price'] * e['volume'] for e in fills) / vol_sum
            else:
                vwap = fallback_price
            self._reanchor_subsequent_from(level, vwap)
        return newly_done

    def _replace_cancelled_split(self, cancelled_pending):
        """취소/POST실패 분할 재주문.
        ★ 타임아웃 재호가 중이면 동일가 재POST 절대 금지."""
        if getattr(self, '_timeout_requoting', False):
            return
        with self._buy_place_lock:
            if self._open_buy_slots_left(check_exchange=True) <= 0:
                return
            self._replace_cancelled_split_locked(cancelled_pending)

    def _replace_cancelled_split_locked(self, cancelled_pending):
        """_replace_cancelled_split 본체 — place lock 보유 상태에서만 호출."""
        level = cancelled_pending['level']
        split_idx = cancelled_pending.get('split_idx', 0)
        if self._is_level_executed(level):
            self._dequeue_failed_replace(level, split_idx)
            return
        if (level, split_idx) in self.pending_split_keys:
            self._dequeue_failed_replace(level, split_idx)
            return
        price = cancelled_pending.get('actual_price') or cancelled_pending.get('planned_price')
        volume = cancelled_pending.get('volume', 0)
        if not price or volume <= 0:
            self._enqueue_failed_replace(cancelled_pending)
            return
        # 동일 평단/금지호가면 1틱 이상 아래로
        bad, why = self._is_forbidden_buy_price(price)
        if bad:
            alt = self._next_distinct_buy_price(price, prefer_down=True)
            if alt <= 0:
                print_log(LogLevel.WARNING,
                          f"Level {level} split re-place skipped "
                          f"({why} @ {price})")
                self._enqueue_failed_replace(cancelled_pending)
                return
            print_log(LogLevel.WARNING,
                      f"Level {level} split re-place {price}→{alt} ({why})")
            price = alt
        print_log(LogLevel.INFO,
                  f"Level {level} split {split_idx+1}/"
                  f"{cancelled_pending.get('split_total', '?')} — re-placing")
        order_uuid = self.place_dynamic_buy_order(price, volume, 'floor')
        if order_uuid:
            if self._add_pending({
                'level': level,
                'planned_price': cancelled_pending.get('planned_price', price),
                'actual_price': price,
                'volume': volume,
                'order_time': time.time(),
                'uuid': order_uuid,
                'split_idx': split_idx,
                'split_total': cancelled_pending.get('split_total', 1),
            }):
                self._dequeue_failed_replace(level, split_idx)
            return
        err = str(getattr(self, '_last_buy_error', '') or '').lower()
        if 'max_open_buys' in err:
            return
        self._enqueue_failed_replace(cancelled_pending)
        print_log(LogLevel.ERROR,
                  f"Failed to re-place split for level {level}")

    def _order_is_filled(self, order_info):
        """체결 여부 — 업비트/빗썸 필드 공통."""
        return order_is_filled(normalize_order(order_info) if order_info else None)

    def _unlock_stuck_partial_levels(self):
        """pending/재주문 없이 partial만 남은 레벨 → 완료 처리해 다음 라운드 해제.
        (MA취소 후 매도만 남는 고착 방지)"""
        unlocked = False
        for lv in list(self.partial_levels or []):
            if self._is_level_executed(lv):
                self.partial_levels.discard(lv)
                unlocked = True
                continue
            if lv in self.pending_levels or self._has_failed_replace_for_level(lv):
                continue
            filled = int(self.level_fill_count.get(lv, 0) or 0)
            if filled > 0:
                print_log(LogLevel.WARNING,
                          f"Level {lv} stuck incomplete ({filled} fills, "
                          f"no pending/retry) — completing to unlock next round")
                fallback = (self.last_executed_price
                            or getattr(self, 'current_price', 0) or 0)
                self._complete_level_from_fills(lv, fallback)
                unlocked = True
            else:
                self.partial_levels.discard(lv)
                unlocked = True
        return unlocked

    def _check_order_execution(self):
        """주문 체결 확인 — Private WS 우선, REST는 스로틀된 안전망.
        pending이 비어도 partial 고착 해제·실패재주문은 반드시 수행."""
        executed_any = False
        now = time.time()

        if self.pending_orders:
            holding_hint = False
            try:
                if private_ws._is_initialized and private_ws.is_connected:
                    bal, locked, _ = private_ws.get_symbol_info(self.symbol)
                    holding_hint = (bal + locked) >= MIN_HOLDING_VOLUME
            except Exception:
                holding_hint = False

            ws_healthy = private_ws._is_initialized and private_ws.is_connected
            if holding_hint:
                rest_interval = 0.08 if ws_healthy else 0.03
            else:
                rest_interval = 0.4 if ws_healthy else 0.12
            force_rest = (now - getattr(self, '_last_fill_rest_check', 0)) >= rest_interval
            if force_rest:
                self._last_fill_rest_check = now
                if EXCHANGE.get('supports_batch_query_ids') and len(self.pending_orders) > 1:
                    pending_uids = [p.get('uuid') for p in self.pending_orders if p.get('uuid')]
                    batched = OrderCanceler.fetch_orders_by_uuids(pending_uids)
                    if batched and private_ws._is_initialized:
                        with private_ws._order_lock:
                            private_ws.order_cache.update(batched)
                        self._batch_rest_at = now

            # uuid 스냅샷 — clear/pop 동시 발생해도 IndexError 없음
            pending_snapshot = list(self.pending_orders)
            for pending_order in pending_snapshot:
                order_uuid = pending_order.get('uuid')
                if not order_uuid:
                    continue
                # 이미 다른 경로에서 제거됨
                if order_uuid not in self.pending_by_uuid:
                    continue

                try:
                    order_info = self._get_order_info(order_uuid, force_rest=force_rest)
                    if not order_info:
                        continue

                    state = str(order_info.get('state', '')).lower()

                    if self._order_is_filled(order_info):
                        self._process_executed_order(pending_order, None, order_info)
                        executed_any = True
                    elif state == 'cancel':
                        cancelled = self._pop_pending_by_uuid(order_uuid)
                        if not cancelled:
                            continue
                        print_log(LogLevel.INFO,
                                  f"Order {cancelled['level']} split "
                                  f"{cancelled.get('split_idx', 0)+1} was cancelled")
                        self._replace_cancelled_split(cancelled)

                except Exception as e:
                    print_log(LogLevel.ERROR, f"Error checking order execution: {str(e)}")
                    continue

        # 재주문 실패분 재시도 (pending 유무와 무관)
        if self._failed_replaces:
            now3 = time.time()
            if now3 - getattr(self, '_last_replace_retry', 0) >= 0.05:
                self._last_replace_retry = now3
                for failed in list(self._failed_replaces):
                    if not self._is_level_executed(failed['level']):
                        self._replace_cancelled_split(failed)

        # ★ pending 비어 partial만 남은 고착 해제 (매도만 남는 버그)
        if self._unlock_stuck_partial_levels():
            executed_any = True

        return executed_any

    def _process_executed_order(self, pending_order, pending_index, order_info):
        """체결된 주문 처리 — 레벨 완료 판정 O(1)."""
        order_uuid = pending_order.get('uuid')
        level = pending_order['level']

        try:
            order_info = normalize_order(order_info)
            executed_volume = order_executed_volume(order_info)
            executed_funds = order_executed_funds(order_info)
            # 빗썸 done이 수량 없이 올 때 pending 수량으로 폴백
            if executed_volume <= 0:
                executed_volume = float(pending_order.get('volume') or 0)
            if executed_funds <= 0 and executed_volume > 0:
                executed_funds = executed_volume * float(
                    pending_order.get('actual_price')
                    or pending_order.get('planned_price') or 0)
            avg_executed_price = (
                executed_funds / executed_volume
                if executed_volume > 0 else pending_order['actual_price']
            )

            executed_order = {
                'level': level,
                'planned_price': pending_order['planned_price'],
                'executed_price': avg_executed_price,
                'quantity': executed_funds,
                'volume': executed_volume,
                'uuid': order_uuid,
                'executed_time': time.time()
            }

            self.executed_orders.append(executed_order)
            # uuid 기준 pop — 인덱스 기반은 동시 수정 시 오삭제/IndexError
            if order_uuid:
                self._pop_pending_by_uuid(order_uuid)
            elif pending_index is not None:
                self._pop_pending_at(pending_index)

            global buy_uuids
            buy_uuids.discard(order_uuid)
            if private_ws._is_initialized:
                private_ws.unregister_order_wait(order_uuid)

            split_idx = pending_order.get('split_idx', 0)
            split_total = pending_order.get('split_total', 1)
            filled_count = self.level_fill_count.get(level, 0) + 1
            self.level_fill_count[level] = filled_count
            self.partial_levels.add(level)
            self.last_executed_level = level
            self.last_executed_price = avg_executed_price
            # 동일 체결가 재매수 금지
            fk = self._px_key(avg_executed_price)
            if fk:
                if not isinstance(self._blocked_buy_px_keys, set):
                    self._blocked_buy_px_keys = set()
                self._blocked_buy_px_keys.add(fk)
                self._last_buy_px_key = fk
            # 해당 분할 재주문 큐 잔여분 제거 (이미 체결됨)
            self._dequeue_failed_replace(level, split_idx)

            print_log(LogLevel.SUCCESS,
                     f"Order {level} split {split_idx+1}/{split_total} executed! "
                     f"Price: {avg_executed_price:.4f} {self.symbol}, "
                     f"Volume: {executed_volume:.6f}")

            # 분할 일부 체결 — 첫주문 타임아웃(전량취소) 경로 무력화
            self.first_order_start_time = None

            still_pending = level in self.pending_levels
            if not still_pending and filled_count >= split_total:
                self._complete_level_from_fills(level, avg_executed_price)
            elif not still_pending and filled_count < split_total:
                if self._has_failed_replace_for_level(level):
                    print_log(LogLevel.WARNING,
                              f"Level {level} incomplete: {filled_count}/{split_total} fills, "
                              f"no pending — retrying failed split re-place")
                else:
                    # 재주문 대상이 없으면 보유 체결분만으로 완료 (다음 라운드 차단 방지)
                    print_log(LogLevel.WARNING,
                              f"Level {level} incomplete: {filled_count}/{split_total} fills, "
                              f"no pending/retry — completing with filled splits")
                    self._complete_level_from_fills(level, avg_executed_price)

            # 로컬 VWAP 즉시 매도 + REST 교정 (체결가·uuid 전달)
            self._notify_sell_after_buy_fill(
                executed_volume, fill_price=avg_executed_price,
                fill_uuid=order_uuid)

        except Exception as e:
            print_log(LogLevel.ERROR, f"Error processing executed order: {str(e)}")

    def _is_order_pending(self, level):
        """주문 대기 중인지 확인 O(1)"""
        return level in self.pending_levels

    def place_dynamic_buy_order(self, price, volume, round_mode='ceil'):
        """매수 주문 실행.
        ★ 워커당 미체결 최대 3 + MA60. (POST횟수 HALT/재호가추격 제거)"""
        global buy_uuids

        if getattr(self, '_cycle_ended', False) or getattr(self, '_buys_halted', False):
            self._last_buy_error = 'cycle_ended'
            return None
        if getattr(self, '_ma_gate_paused', False):
            self._last_buy_error = 'ma60_block'
            return None

        # ★ HARD CAP: 이미 3개면 POST 자체를 안 함 (로그 없음)
        # 핫패스: 로컬 우선 — 거래소 카운트는 150ms 캐시 (버스트당 ≤1회)
        if self._hard_open_buy_count(check_exchange=True) >= MAX_OPEN_BUYS_PER_WORKER:
            self._last_buy_error = 'max_open_buys'
            return None

        # HybridMA = 진입 검증만. 매도/체결 진행 중(_skip_ma_gate)이면 POST 허용
        if not getattr(self, '_skip_ma_gate', False):
            try:
                gate_ok, gi = RealMarketData.check_tick_ma_gate(self.symbol)
                if not gate_ok:
                    self._last_buy_error = 'ma60_block'
                    print_log(LogLevel.ERROR,
                              f"BUY HybridMA FAIL {self.symbol}: "
                              f"now={gi.get('last_price')} "
                              f"hybrid={(gi.get('hybrid') or gi.get('ma60'))} "
                              f"tick={gi.get('ma_tick')} min={gi.get('ma_min')} "
                              f"— POST 거부")
                    self.is_active = False
                    self._ma_gate_paused = True
                    return None
            except Exception:
                pass

        self.last_order_time = time.time()
        t_start = time.time()
        epoch_at = current_buy_epoch()
        vol_in = volume
        price_in = price

        # volume(코인 수량)을 거래소 최소 단위로 보정.
        if round_mode == 'floor':
            volume = math.floor(volume / UpbitTickSystem.VOLUME_QUANTUM + 1e-15) * UpbitTickSystem.VOLUME_QUANTUM
        else:
            volume = UpbitTickSystem.ceil_volume(volume)

        if volume <= 0:
            print_log(LogLevel.ERROR, f"Buy volume <= 0 after quantize: {volume}")
            return None

        price = UpbitTickSystem.snap_to_tick(price)

        # 타임아웃 재호가 시 시세 아래 호가 금지
        floor = getattr(self, '_timeout_place_floor', None)
        if floor is not None and price + 1e-15 < float(floor):
            price = UpbitTickSystem.snap_to_tick(float(floor), 'ceil')

        # ★ 핫패스: WS/trade pending만 오염검사 (REST ticker 제거 → POST 직전 RTT 0)
        clean, contaminated = RealMarketData.sanitize_symbol_price(
            self.symbol, price, force_rest=False)
        if contaminated and clean and clean > 0:
            print_log(LogLevel.ERROR,
                      f"BUY PRICE REJECT {self.symbol}: "
                      f"{price} → {clean} (오염 교정)")
            price = UpbitTickSystem.snap_to_tick(clean)
        elif clean is None:
            self._last_buy_error = 'price_contamination'
            print_log(LogLevel.ERROR,
                      f"BUY PRICE BLOCK {self.symbol} px={price} — 시세검증 실패")
            return None

        # ★ 동일 평단/동일 pending 호가 절대 금지
        # 재호가/시세 근처면 prefer_up, 아니면 prefer_down (floor 이상)
        bad, why = self._is_forbidden_buy_price(price)
        if bad:
            live = (getattr(self, 'current_price', None)
                    or RealMarketData.get_current_price(self.symbol) or price)
            live_f = float(live or 0)
            prefer_down = True
            floor_px = None
            if floor is not None:
                floor_px = float(floor)
                prefer_down = False
            elif live_f > 0:
                floor_px = UpbitTickSystem.snap_to_tick(live_f * 0.995, 'floor')
            alt = self._next_distinct_buy_price(
                price, prefer_down=prefer_down, floor=floor_px)
            if alt <= 0:
                alt = self._next_distinct_buy_price(
                    price, prefer_down=not prefer_down, floor=floor_px)
            if alt <= 0:
                self._last_buy_error = f'forbidden_px:{why}'
                print_log(LogLevel.ERROR,
                          f"BUY SAME-PX BLOCK {self.symbol} "
                          f"px={price} ({why}) — POST 거부")
                return None
            alt_key = self._px_key(alt)
            for p in (self.pending_orders or []):
                if self._px_key(p.get('actual_price') or p.get('planned_price')) == alt_key:
                    self._last_buy_error = f'forbidden_px:{why}'
                    print_log(LogLevel.ERROR,
                              f"BUY SAME-PX BLOCK {self.symbol} "
                              f"— 분할호가 충돌 {alt_key}")
                    return None
            print_log(LogLevel.WARNING,
                      f"BUY SAME-PX adjust {self.symbol} "
                      f"{price}→{alt} ({why})")
            price = alt

        est_krw = price * volume
        # 가용 KRW 스냅샷 (거절 원인 대조)
        try:
            if private_ws._is_initialized and private_ws.is_connected:
                krw_bal = private_ws.get_krw_balance(1)
                krw_locked = private_ws.get_krw_balance(2)
            else:
                krw_bal = AccountChecker().get_krw_balance()
                krw_locked = -1
        except Exception as e:
            krw_bal, krw_locked = -1, -1

        query = {
            'market': "KRW-" + self.symbol,
            'side': 'bid',
            'volume': f"{volume:.8f}",
            'price': UpbitTickSystem.format_order_price(price),
            EXCHANGE['order_type_field']: 'limit',
        }

        last_error = None
        for attempt in range(3):
            try:
                headers = make_auth_headers(query)

                def api_call(q=query, h=headers):
                    return http_post_order(ORDER_URL, q, h, reason='buy')

                t0 = time.time()
                response = hot_api_call(api_call)
                ms = (time.time() - t0) * 1000
                status = getattr(response, 'status_code', None)
                raw = None
                try:
                    raw = response.content[:500] if getattr(response, 'content', None) else None
                    if isinstance(raw, (bytes, bytearray)):
                        raw = raw.decode('utf-8', errors='replace')
                except Exception:
                    raw = '<body-read-fail>'


                if _response_rate_limited(response):
                    last_error = f'rate_limit status={status}'
                    continue
                order_uuid, err_body = response_order_or_error(response)
                if order_uuid:
                    buy_uuids.add(order_uuid)
                    # 캐시 유지: 로컬 pending이 증가하므로 max(local,ex)로 hard cap 충분
                    # (invalidate 시 분할마다 open-bid REST → POST 지연)
                    if private_ws._is_initialized:
                        with private_ws._order_lock:
                            private_ws.order_cache.setdefault(order_uuid, {
                                'uuid': order_uuid,
                                'state': 'wait',
                                'executed_volume': '0',
                                'remaining_volume': f"{volume:.8f}",
                                'executed_funds': '0',
                            })
                        private_ws.register_order_wait(order_uuid)
                    print_log(LogLevel.SUCCESS,
                              f"Buy order placed at {price:.4f} KRW, "
                              f"Volume: {volume:.6f} uuid={order_uuid[:8]}…")
                    self._last_buy_error = None
                    return order_uuid

                response_dict = err_body if isinstance(err_body, dict) else {}
                last_error = response_dict or err_body
                err = response_dict.get('error') if isinstance(response_dict, dict) else None
                if isinstance(err, dict):
                    err_name = str(err.get('name', '') or err.get('message', ''))
                    err_msg = str(err.get('message', ''))
                else:
                    err_name = str(response_dict)
                    err_msg = ''
                err_l = (err_name + ' ' + err_msg).lower()
                # 수수료/잔고 레이스 — 수량만 한 번 줄여 재시도 (동일 수량 무한 재시도 방지)
                if 'insufficient' in err_l or 'fund' in err_l:
                    shrunk = UpbitTickSystem.floor_volume(volume * COMMISSION)
                    if shrunk > 0 and shrunk < volume:
                        volume = shrunk
                        query['volume'] = f"{volume:.8f}"
                        est_krw = price * volume
                        continue
                    break
                if any(k in err_l for k in ('under_min', 'invalid')):
                    break
            except Exception as e:
                last_error = str(e)

        self._last_buy_error = last_error
        err_l = str(last_error or '').lower()
        # 429는 상위 분할 루프가 재시도 — 동일 로그 3연타 금지
        if 'rate_limit' not in err_l and '429' not in err_l:
            print_log(LogLevel.ERROR, f"Failed to place buy order: {last_error}")
        return None

    def cancel_all_pending_orders(self):
        """대기 중인 주문만 취소 — 백그라운드 스레드로 비동기 실행 (메인 스레드 지연 0).
        취소 완료를 기다릴 필요 없는 fire-and-forget."""
        print_log(LogLevel.INFO, f"Cancelling pending orders for {self.symbol}")
        run_async(self._cancel_pending_sync)

    def _cancel_pending_sync(self):
        """대기 주문 취소. 이미 체결된 건 먼저 처리 후 나머지만 취소.
        (체결인데 pending 잔류 → 타임아웃 루프에 갇히는 것 방지)"""
        # 1) 상태 일괄 조회 후 체결분 반영 (업비트 GET /orders/uuids)
        pending_uids = [p.get('uuid') for p in self.pending_orders if p.get('uuid')]
        if EXCHANGE.get('supports_batch_query_ids') and len(pending_uids) > 1:
            batched = OrderCanceler.fetch_orders_by_uuids(pending_uids)
            if batched and private_ws._is_initialized:
                with private_ws._order_lock:
                    private_ws.order_cache.update(batched)
        for pending in list(self.pending_orders):
            uid = pending.get('uuid')
            if not uid or uid not in self.pending_by_uuid:
                continue
            info = self._get_order_info(uid, force_rest=True)
            if self._order_is_filled(info):
                self._process_executed_order(pending, None, info)

        # 2) 남은 미체결만 일괄 취소 (업비트 DELETE /orders/uuids)
        cancel_uuids = [p.get('uuid') for p in self.pending_orders if p.get('uuid')]
        cancelled_count = self.order_canceler.cancel_orders_parallel(cancel_uuids)
        global buy_uuids
        buy_uuids.difference_update(cancel_uuids)
        self._remove_pendings_by_uuids(set(cancel_uuids))

        print_log(LogLevel.INFO,
                  f"Cancelled {cancelled_count} pending orders, "
                  f"{len(self.pending_orders)} remaining")

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

    def _get_order_info(self, order_uuid, force_rest=False):
        """주문 정보 조회 — Private WS 캐시 우선, REST 폴백.
        done/cancel(및 remaining=0)은 항상 캐시 신뢰.
        wait는 force_rest일 때 REST — 단 직전 배치조회(/orders/uuids) 윈도우면 캐시 재사용."""
        ws_ok = private_ws._is_initialized and private_ws.is_connected
        cached = private_ws.get_order_state(order_uuid) if ws_ok else None

        if cached:
            state = str(cached.get('state', '')).lower()
            if state in ('done', 'cancel'):
                return cached
            cached = normalize_order(cached)
            if order_is_filled(cached):
                return cached
            if not force_rest:
                return cached
            # 배치 REST 직후 0.5초 이내면 개별 재조회 생략
            if time.time() - getattr(self, '_batch_rest_at', 0) < 0.5:
                return cached

        try:
            qkey = EXCHANGE.get('order_query_id_param') or EXCHANGE.get('order_id_param', 'uuid')
            params = {qkey: order_uuid}
            headers = make_auth_headers(params)

            def api_call():
                response = http_get_hot(
                    ORDER_QUERY_URL, params=params, headers=headers)
                return response_json(response)

            response_dict = safe_api_call(api_call)
            if isinstance(response_dict, dict):
                response_dict = normalize_order(response_dict)
            if private_ws._is_initialized and response_dict:
                with private_ws._order_lock:
                    prev = private_ws.order_cache.get(order_uuid)
                    private_ws.order_cache[order_uuid] = normalize_order(
                        response_dict, prev=prev)
            return response_dict if response_dict else cached

        except Exception as e:
            print_log(LogLevel.ERROR, f"Error getting order info: {str(e)}")
            return cached

    def stop_trading(self):
        """거래 중지 — 해당 심볼 매수만 취소 (타 워커 보호)."""
        self.is_active = False
        self._buys_halted = True
        self._cycle_ended = True
        pending_uuids = [p.get('uuid') for p in self.pending_orders if p.get('uuid')]
        pending_uuids.extend(self._collect_pending_uuids_from_index(self.pending_by_uuid))
        self.pending_orders.clear()
        self._reset_runtime_indexes()
        try:
            OrderCanceler().cancel_buy_orders_for_symbol(
                self.symbol, extra_uuids=pending_uuids, verify=True)
        except Exception as e:
            print_log(LogLevel.WARNING, f"stop_trading cancel buys: {str(e)[:100]}")
        print_log(LogLevel.INFO, f"Trading stopped for {self.symbol}")

    def halt_further_buys(self, reason=''):
        """의도적 매수 중단 (stop 경로용). POST횟수/재호가 HALT 아님."""
        self._buys_halted = True
        self.is_active = False
        try:
            self._cancel_pending_sync()
        except Exception:
            try:
                OrderCanceler().cancel_buy_orders_for_symbol(
                    self.symbol, verify=False)
            except Exception:
                pass
        self._clear_pending_tracking_only()
        self._failed_replaces = []
        print_log(LogLevel.WARNING,
                  f"buys stopped {self.symbol}"
                  + (f" — {reason}" if reason else ""))

    def _buys_allowed(self):
        """매수 POST 허용 — cycle/stop/쿨다운만.
        MA pause는 진입대기용. 매도진행(_skip_ma_gate)이면 pause 무시."""
        if getattr(self, '_buys_halted', False):
            return False
        if getattr(self, '_cycle_ended', False):
            return False
        if (getattr(self, '_ma_gate_paused', False)
                and not getattr(self, '_skip_ma_gate', False)):
            return False
        if time.time() < float(getattr(self, '_skip_place_until', 0) or 0):
            return False
        return True

    def _adopt_exchange_bids_if_any(self):
        """거래소 open bid를 로컬 pending으로 흡수. 흡수 건수 반환.
        MA pause 재개·워치독에서 신규 POST 전에 호출 — 중복 매수 방지.
        ★ MAX_OPEN_BUYS_PER_WORKER 초과 흡수 금지."""
        plans = self.active_planned_orders or []
        if not plans:
            return 0
        market = f"KRW-{self.symbol}"
        try:
            open_bids = OrderCanceler().list_open_bid_orders(market=market)
        except Exception:
            open_bids = []
        if not open_bids:
            return 0
        # 거래소에 이미 3 초과면 먼저 강제 정리
        if len(open_bids) > MAX_OPEN_BUYS_PER_WORKER:
            self._enforce_max_open_buys()
            try:
                open_bids = OrderCanceler().list_open_bid_orders(market=market)
            except Exception:
                open_bids = []
        adopted = 0
        for o in open_bids or []:
            if self._hard_open_buy_count() >= MAX_OPEN_BUYS_PER_WORKER:
                break
            uid = order_id_of(o)
            if not uid or uid in getattr(self, 'pending_by_uuid', {}):
                continue
            try:
                px = float(o.get('price') or o.get('p') or 0)
                vol = float(o.get('remaining_volume')
                            or o.get('volume') or o.get('rv') or 0)
            except (TypeError, ValueError):
                px, vol = 0.0, 0.0
            if px <= 0 or vol <= 0:
                continue
            next_lv = None
            for p in plans:
                if not p.get('executed'):
                    next_lv = p.get('level')
                    break
            if next_lv is None:
                break
            ok = self._add_pending({
                'level': next_lv,
                'planned_price': px,
                'actual_price': px,
                'volume': vol,
                'order_time': time.time(),
                'uuid': uid,
                'split_idx': 0,
                'split_total': 1,
            })
            if ok is False:
                break
            buy_uuids.add(uid)
            adopted += 1
        if adopted:
            self.is_active = True
        return adopted

    def watchdog_ensure_buy_orders(self):
        """1분 워치독 — 사다리 미완료인데 매수 주문이 전무하면 재개.
        ★ buys_halted / POST한도면 절대 재POST 안 함."""
        if not self._buys_allowed():
            return False
        plans = self.active_planned_orders or []
        n_plan = len(plans)
        if n_plan <= 0:
            return False
        if getattr(self, '_cycle_ended', False):
            return False
        if self.executed_count >= n_plan:
            return False
        if time.time() < float(getattr(self, '_skip_place_until', 0) or 0):
            return False
        if getattr(self, '_timeout_requoting', False):
            return False

        if self.pending_orders or self._failed_replaces:
            return False

        try:
            ex_n = int(self._exchange_open_buy_count() or 0)
        except Exception:
            ex_n = 0
        if ex_n > 0:
            if time.time() < float(getattr(self, '_skip_adopt_until', 0) or 0):
                return False
            adopted = self._adopt_exchange_bids_if_any()
            if adopted:
                print_log(LogLevel.WARNING,
                          f"buy-watchdog: 거래소 bid {adopted}건 로컬 재흡수 "
                          f"(exec={self.executed_count}/{n_plan})")
                return True
            print_log(LogLevel.WARNING,
                      f"buy-watchdog: 거래소 bid {ex_n}건 잔존 — 재POST 스킵")
            return False

        # exec=0 + 보유 → L1 재시작 금지 (무한매수)
        if self.executed_count <= 0:
            try:
                if private_ws._is_initialized and private_ws.is_connected:
                    bal, locked, _ = private_ws.get_symbol_info(self.symbol)
                    if (float(bal) + float(locked)) > 0:
                        print_log(LogLevel.WARNING,
                                  f"buy-watchdog: 보유있음·exec=0 — L1 재POST 금지")
                        self.is_active = False
                        return False
            except Exception:
                pass

        print_log(LogLevel.WARNING,
                  f"buy-watchdog: 매수주문 없음 "
                  f"(exec={self.executed_count}/{n_plan}) — 다음 레벨 재배치")

        for lv in list(self.partial_levels):
            if self._is_level_executed(lv):
                self.partial_levels.discard(lv)
                continue
            filled = self.level_fill_count.get(lv, 0)
            if filled > 0:
                fallback = (self.last_executed_price
                            or getattr(self, 'current_price', 0) or 0)
                self._complete_level_from_fills(lv, fallback)
            else:
                self.partial_levels.discard(lv)
        self._failed_replaces = []
        self.is_active = True
        for i, p in enumerate(plans):
            if not p.get('executed'):
                self.next_plan_idx = i
                break
        return bool(self._execute_next_available_order())

    def watchdog_fill_timeout(self):
        """고전 첫주문 타임아웃만 — check_and_continue와 동일 경로."""
        if getattr(self, '_cycle_ended', False) or getattr(self, '_buys_halted', False):
            return False
        if not self.is_active:
            return False
        if not self._should_first_order_timeout():
            return False
        return self._classic_first_order_timeout()

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
        self.on_buy_fill_sell = None
        try:
            private_ws.clear_avg_sell_target(self.symbol)
        except Exception:
            pass
        print_log(LogLevel.INFO, f"Reset completed for {self.symbol}")

class SellOrder:
    def __init__(self, symbol, volume, price, max_available=None):
        """limit ask. max_available이 있으면 그 이하로 강제 캡 (insufficient 사전 차단)."""
        global sell_uuids
        self.uuid = None  # 체결 추적용 — 성공 시 UUID 저장
        self.placed_volume = None  # 거래소 수락 수량 (shrink/포맷 후)
        self.last_error = None

        # 호가 snap + 수량 단위 재시도 (invalid_volume_ask: 종목별 소수 자릿수 상이)
        price_str = UpbitTickSystem.format_order_price(price)
        raw_vol = float(volume)
        if max_available is not None and max_available >= 0:
            raw_vol = min(raw_vol, float(max_available))
        # 전량 floor만 — ask_safe_volume(shrink=1) 이중 차감 금지
        raw_vol = UpbitTickSystem.floor_volume(raw_vol)
        if raw_vol <= 0:
            self.last_error = 'zero_volume'
            return

        # shrink 재시도 — insufficient 시에만 (슬롯 낭비 최소화: 2회)
        shrink_steps = (1.0, 0.995)
        for shrink in shrink_steps:
            attempt_vol = (UpbitTickSystem.floor_volume(raw_vol)
                           if shrink >= 1.0 - 1e-15
                           else UpbitTickSystem.ask_safe_volume(raw_vol, shrink=shrink))
            if attempt_vol <= 0:
                continue
            insufficient = False
            for decimals in (8, 6, 4, 2, 0):
                vol_str = UpbitTickSystem.format_order_volume(attempt_vol, decimals)
                if vol_str == '0':
                    continue
                # 포맷 후에도 가용 초과 금지
                if max_available is not None and float(vol_str) > float(max_available) + 1e-15:
                    continue
                query = {
                    'market': 'KRW-' + symbol,
                    'side': 'ask',
                    'volume': vol_str,
                    'price': price_str,
                    EXCHANGE['order_type_field']: 'limit',
                }
                headers = make_auth_headers(query)

                def api_call(q=query, h=headers):
                    return http_post_order(ORDER_URL, q, h, reason='sell')

                try:
                    response = hot_api_call(api_call)
                except Exception as e:
                    self.last_error = e
                    print_log(LogLevel.ERROR, f"Failed to place sell order: {e}")
                    return
                # rate limit — http_post_order 내부 카운터가 슬롯 확보 후 재전송 (ERROR 없음)
                if _response_rate_limited(response):
                    ok_retry = False
                    for _ in range(4):
                        try:
                            response = hot_api_call(api_call)
                        except Exception as e:
                            self.last_error = e
                            print_log(LogLevel.ERROR, f"Failed to place sell order: {e}")
                            return
                        if not _response_rate_limited(response):
                            ok_retry = True
                            break
                    if not ok_retry:
                        self.last_error = 'rate_limit'
                        return
                order_uuid, err_body = response_order_or_error(response)
                if order_uuid:
                    self.uuid = order_uuid
                    try:
                        self.placed_volume = float(vol_str)
                    except (TypeError, ValueError):
                        self.placed_volume = attempt_vol
                    sell_uuids.add(self.uuid)
                    print_log(LogLevel.INFO,
                              f"Sell order placed at {price_str} KRW, volume: {vol_str}")
                    return
                self.last_error = err_body
                err_l = str(err_body).lower()
                if 'insufficient' in err_l or 'fund' in err_l:
                    insufficient = True
                    break
                if 'invalid_volume' not in err_l and 'volume' not in err_l:
                    # 기타 오류 — 로그 후 종료 (insufficient는 아래 silent)
                    print_log(LogLevel.ERROR,
                              f"Failed to place sell order: {self.last_error}")
                    return
            if not insufficient:
                break
            # insufficient → 다음 shrink (로그 없음)
            continue

        # insufficient 등으로 UUID 없음 — 스팸 로그 금지 (상위가 재시도/스킵)
        err_l = str(self.last_error or '').lower()
        if ('insufficient' in err_l or 'fund' in err_l
                or self.last_error == 'zero_volume'
                or self.last_error == 'rate_limit'):
            return
        if self.last_error:
            print_log(LogLevel.ERROR, f"Failed to place sell order: {self.last_error}")

class TradingManager:
    """거래 상태 및 캐시 관리 클래스"""
    
    def __init__(self, watch_command=True):
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
        self.forced_symbol_change = False
        self.pending_symbol_change = None  # 대기 중인 심볼 변경
        self.current_command_symbol = None  # command.txt에서 읽은 최신 SYMBOL (캐시, 단일 = 리스트의 첫 번째)
        self.current_command_symbols = []   # command.txt의 모든 SYMBOL (다중 심볼 폴백용)
        # command.txt 백그라운드 감시 — 파일 읽기를 별도 스레드로 분리
        self._command_lines = []  # 백그라운드 스레드가 갱신
        self._command_thread = None
        if watch_command:
            self._start_command_watcher()
        else:
            self._load_command_lines()

    def _load_command_lines(self):
        """command.txt 1회 동기 로드."""
        try:
            with open(str(COMMAND_TXT), 'r', encoding='utf-8') as f:
                self._command_lines = f.readlines()
        except Exception:
            pass

    def _start_command_watcher(self):
        """command.txt 백그라운드 폴링 → 메모리 캐시.
        시작 시 즉시 1회 로드(빈 캐시→전체마켓 ETH 선별 레이스 방지)."""
        self._load_command_lines()
        stop = threading.Event()

        def _watch():
            while not stop.wait(0.5):
                self._load_command_lines()
        self._command_thread = threading.Thread(target=_watch, daemon=True)
        self._command_thread.start()

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
        """command.txt 변경 체크 — 백그라운드 캐시에서 읽기 (파일 I/O 0, 지연 0).
        _command_watcher 스레드가 파일을 읽어 _command_lines에 캐싱.
        다중 심볼 지원: 모든 'SYMBOL X' 줄을 파싱하여 current_command_symbols 리스트에 저장.
        줄 단위 기재를 권장 (SYMBOL A\\n SYMBOL B\\n ...). 첫 심볼은 current_command_symbol(단일)에도 저장."""
        lines = self._command_lines
        if not lines:
            # 워처 첫 주기 전 빈 캐시 — 동기 1회 보강 (자동선별 레이스 차단)
            self._load_command_lines()
            lines = self._command_lines
        if not lines:
            return False

        # 모든 SYMBOL 줄 파싱 → 순서 보존 리스트 (중복 제거)
        parsed_symbols = []
        seen = set()
        detected_change = False
        for line in lines:
            text = line.strip().upper()
            if not text:
                continue
            parts = text.split(' ')
            if parts[0] == 'SYMBOL' and len(parts) > 1:
                new_symbol = parts[1]
                if new_symbol not in seen:
                    seen.add(new_symbol)
                    parsed_symbols.append(new_symbol)

            elif parts[0] == 'EXIT':
                # EXIT 시 동기식 즉시 쓰기 (exit 전 플러시 보장)
                AsyncLogger.write_sync(str(STATE_TXT), '#' + str(int(LogState.FORCED_EXIT)))
                print_log(LogLevel.WARNING, "Exit command detected")
                exit(0)

        # 파싱된 심볼 리스트 갱신 — 변경 시 로그
        # 빈 리스트(SYMBOL 줄이 모두 지워진 경우)도 반영하여 자동 선별 모드로 폴백.
        if parsed_symbols != self.current_command_symbols:
            prev = list(self.current_command_symbols or [])
            self.current_command_symbols = parsed_symbols
            detected_change = True  # 다중/단일 모두 변경 신호
            if parsed_symbols:
                # 단일 호환 필드 = 첫 심볼
                self.current_command_symbol = parsed_symbols[0]
                print_log(LogLevel.SUCCESS,
                          f"Command symbols updated: {parsed_symbols} "
                          f"(prev={prev or '-'}, {len(parsed_symbols)}개)")
                # ★ 신규 심볼 즉시 구독+MA 시드 (갱신 후 매수 안 되던 버그)
                try:
                    added = [s for s in parsed_symbols if s not in set(prev)]
                    if added:
                        RealMarketData.subscribe_trade_stream_symbols(added)
                        for s in added:
                            try:
                                RealMarketData.subscribe_websocket(s)
                            except Exception:
                                pass
                            try:
                                if trade_ws is not None:
                                    trade_ws.seed_tick_ma(s)
                            except Exception:
                                pass
                            try:
                                if minute_ma_cache is not None:
                                    minute_ma_cache.ensure(s)
                            except Exception:
                                pass
                        print_log(LogLevel.SUCCESS,
                                  f"Command 신규심볼 시드: {added}")
                except Exception as e:
                    print_log(LogLevel.WARNING,
                              f"Command 신규심볼 시드 실패: {str(e)[:80]}")
                # 단일 심볼만 pending 오버라이드. 다중(≥2)은 select_first_tradable이
                # 폴백 선택하므로 첫 심볼을 pending에 넣으면 거래 심볼/시세가 꼬임.
                if len(parsed_symbols) == 1:
                    first = parsed_symbols[0]
                    if first != self.current_symbol and first != self.pending_symbol_change:
                        if VolatilityProtector.check_volatility_protection(first):
                            print_log(LogLevel.WARNING, f"Command symbol {first} blocked by volatility protection")
                        else:
                            self.pending_symbol_change = first
                else:
                    # 다중 모드: pending이 폴백 선택을 덮어쓰지 않도록 항상 비움
                    self.pending_symbol_change = None
            else:
                # SYMBOL 줄 모두 제거 — 자동 선별 모드로 폴백
                self.current_command_symbol = None
                self.pending_symbol_change = None
                print_log(LogLevel.SUCCESS,
                          "Command symbols cleared — falling back to auto-select")

        return detected_change

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
        """거래가 진행 중인지 확인 (매수/매도 플래그)"""
        return (self.buy_orders_placed or self.buy_orders_executed or
                self.sell_orders_placed or self.sell_orders_executed)

    def get_holding_volume(self, symbol=None):
        """심볼 실보유(balance+locked). 없으면 0."""
        sym = symbol or self.current_symbol
        if not sym:
            return 0.0
        try:
            if private_ws._is_initialized:
                bal, locked, _ = private_ws.get_symbol_info(sym)
            else:
                bal, locked, _ = AccountChecker().get_symbol_info(sym)
            if bal < 0:
                return 0.0
            return float(bal) + float(locked)
        except Exception:
            return 0.0

    def is_cycle_locked(self):
        """사이클 잠금 — 진행 플래그 또는 현재 심볼 실보유가 있으면 다른 심볼로 전환 금지.
        다중 심볼 폴백이 미완료 매매 도중 심볼을 바꾸는 것을 막는다.
        단, 평가액 < 5000원 먼지진은 잠금하지 않음(매수로 흡수).
        ★ WS flat이어도 REST상 매도가능(≥5000원)이면 잠금 — 늦은 매수체결 레이스 차단."""
        if self.stop_loss_triggered:
            return True
        if self.is_trading_in_progress():
            return True
        sym = self.current_symbol
        if not sym:
            return False
        vol = self.get_holding_volume(sym)
        if vol >= MIN_HOLDING_VOLUME and not is_dust_holding(sym, vol):
            return True
        # WS 먼지/0이어도 REST 전량미완이면 잠금 (다음 코인 매수 금지)
        try:
            if not rest_holdings_cleared(sym):
                return True
        except Exception:
            return True  # 조회 실패 시 안전하게 잠금
        return False

    def touch_symbol_cache(self):
        """심볼 캐시 TTL만 갱신 (사이클 잠금 유지용, set_symbol 로그 없음)."""
        global current_trading_symbol, symbol_cache_time
        if self.current_symbol:
            current_trading_symbol = self.current_symbol
            self.symbol_cache_time = datetime.now()
            symbol_cache_time = self.symbol_cache_time

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
        self.stop_loss_check_interval = 0   # 스탑로스 — 매 루프 즉시 체크
        # 단일 매도 추적
        self.sell_orders_tracking = []  # [{uuid, price, volume, tier, filled}]
        self.filled_sell_count = 0      # 체결된 매도 개수
        self.unfilled_sell_count = 0    # 미체결 매도 수 — O(1) has_pending
        self.last_sell_base_price = None  # 직전 매도 기준가 (갱신 감지용)
        self._sell_placed_at_buy_count = 0  # 매도 걸 때점의 매수 완료 라운드 수
        self.dust_holdings = False  # 평가액 < 5000 — 매도 포기·매수 흡수
        self._last_sell_place_attempt = 0.0
        self._sell_placing = False
        self._sell_place_lock = threading.Lock()  # 동시 place_sell 폭주 차단
        self._sell_stable_until = 0.0  # 이 시각 전엔 취소/교체 금지 (깜빡임 차단)
        self._free_above_min_streak = 0  # 가용≥5000원 연속 확인 횟수 (WS 오탐 필터)
        # myAsset 서버 avg 수신 후 매도호가 교정
        self._sell_base_provisional = False
        self._last_server_avg_seen = 0.0
        self._open_asks_cache = None  # (ts, symbol, asks)
        self._open_asks_cache_ttl = 1.5
        self._last_committed_qty = 0.0
        self._last_replace_px = 0.0
        self._last_replace_t = 0.0
        self._last_cancel_and_new_t = 0.0
        self._last_sell_skip = ''
        self._last_full_sell_attempt_t = 0.0
        self._last_sell_fail_log_t = 0.0

    def _log_sell_fail_throttled(self, symbol, total_vol, base, why=''):
        """전량매도 실패 로그 — busy/cooldown은 침묵, 실실패만 5초에 1회."""
        skip = str(getattr(self, '_last_sell_skip', '') or '')
        if skip in ('busy', 'cooldown', 'placing'):
            return
        now = time.time()
        if now - float(getattr(self, '_last_sell_fail_log_t', 0) or 0) < 5.0:
            return
        self._last_sell_fail_log_t = now
        print_log(LogLevel.ERROR,
                  f"전량매도 실패 {symbol} vol={float(total_vol):.8f} "
                  f"avg={base or 0}"
                  + (f" ({why})" if why else ""))

    def _force_full_sell(self, symbol, profit_percentages, dynamic_buyer,
                         total_vol, base=None, min_interval=0.8):
        """ask 없을 때 전량 POST — 동시호출/연타 차단 후 place."""
        if getattr(self, '_sell_placing', False):
            self._last_sell_skip = 'placing'
            return False
        if self.has_open_sell_orders() or self._open_sell_count() > 0:
            return True
        if self._adopt_open_ask_tracking(symbol):
            return True
        now = time.time()
        last = float(getattr(self, '_last_full_sell_attempt_t', 0) or 0)
        if now - last < float(min_interval):
            self._last_sell_skip = 'cooldown'
            return False
        self._last_full_sell_attempt_t = now
        ok = self.place_sell_orders(
            symbol, profit_percentages, dynamic_buyer,
            sell_base_price=base, force_replace=True,
            volume_hint=total_vol)
        if ok and self._open_sell_count() > 0:
            return True
        if ok and self._adopt_open_ask_tracking(symbol):
            return True
        return False

    def _get_fresh_symbol_info(self, symbol):
        """REST로 balance/locked/avg_buy_price 확정 후 WS 캐시 동기화."""
        try:
            bal, locked, avg = AccountChecker._rest_symbol_info(
                ACCESS_KEY, SECRET_KEY, symbol)
            if bal >= 0:
                if private_ws._is_initialized:
                    private_ws.asset_cache[symbol] = {
                        'balance': float(bal),
                        'locked': float(locked),
                        'avg_buy_price': float(avg),
                    }
                    private_ws.asset_cache_time = time.time()
                return float(bal), float(locked), float(avg)
        except Exception:
            pass
        return self._get_cached_symbol_info(symbol)

    def _wait_locked_clear(self, symbol, max_wait=0, poll=0):
        """취소 후 locked 확인 — sleep 폴링 없이 캐시→즉시 REST 1회."""
        balance, locked, avg = self._get_cached_symbol_info(symbol)
        if locked > 0:
            balance, locked, avg = self._get_fresh_symbol_info(symbol)
        return balance, locked, avg

    def _instant_symbol_info(self, symbol):
        """대기 없이 즉시 잔고 스냅샷 — WS 캐시 → 미스 시 REST 1회."""
        bal, locked, avg = self._get_cached_symbol_info(symbol)
        if bal < 0 or (bal <= 0 and locked <= 0):
            bal, locked, avg = self._get_fresh_symbol_info(symbol)
        if bal < 0:
            return 0.0, 0.0, 0.0
        return float(bal), float(max(locked, 0.0)), float(avg or 0.0)

    def _confirmed_ask_available(self, symbol):
        """매도 POST 직전 — REST force로 가용(balance) 확정.
        WS/TTL 스테일은 insufficient_funds_ask의 주범이라 쓰지 않음.
        Returns: (available_balance, locked, avg)."""
        try:
            bal, locked, avg = AccountChecker._rest_symbol_info(
                ACCESS_KEY, SECRET_KEY, symbol, force=True)
            if bal >= 0:
                if private_ws._is_initialized:
                    private_ws.asset_cache[symbol] = {
                        'balance': float(bal),
                        'locked': float(max(locked, 0.0)),
                        'avg_buy_price': float(avg or 0.0),
                    }
                    private_ws.asset_cache_time = time.time()
                return float(bal), float(max(locked, 0.0)), float(avg or 0.0)
        except Exception:
            pass
        return self._instant_symbol_info(symbol)

    def _fetch_open_asks(self, symbol, force=False):
        """거래소 미체결 ask 목록 — WS locked 오탐 대신 REST가 진실.
        SellModule 0.01s 틱에서 폭주하지 않게 짧게 캐시."""
        now = time.time()
        cached = getattr(self, '_open_asks_cache', None)
        ttl = float(getattr(self, '_open_asks_cache_ttl', 1.5) or 1.5)
        if (not force and cached
                and cached[1] == symbol
                and (now - cached[0]) < ttl):
            return list(cached[2])
        market = f"KRW-{symbol}"
        list_ep = EXCHANGE.get('orders_list_endpoint') or '/v1/orders'
        if str(list_ep).endswith('/open'):
            params = {'market': market, 'states[]': ['wait']}
        else:
            params = {'market': market, 'state': 'wait'}
        headers = make_auth_headers(params)
        try:
            data = response_json(
                http_get_hot(SERVER_URL + list_ep, params=params, headers=headers,
                             timeout=HTTP_TIMEOUT))
            orders = unwrap_orders_payload(data)
            if not isinstance(orders, list):
                out = []
            else:
                out = []
                for o in orders:
                    if not isinstance(o, dict):
                        continue
                    o = normalize_order(o)
                    side = normalize_side(o.get('side') or o.get('ask_bid'))
                    mkt = o.get('market') or o.get('code') or ''
                    if side == 'ask' and mkt == market:
                        out.append(o)
            self._open_asks_cache = (now, symbol, list(out))
            return out
        except Exception:
            if cached and cached[1] == symbol:
                return list(cached[2])
            return []

    def _adopt_open_ask_tracking(self, symbol):
        """거래소에 이미 열린 ask가 있으면 tracking에 흡수하고 True.
        WS free/locked 유령오탐으로 실매도를 취소하는 경로를 끊음."""
        asks = self._fetch_open_asks(symbol)
        if not asks:
            return False
        # 최고가 ask 1건 유지 (봇은 단일 매도)
        def _px(o):
            try:
                return float(o.get('price') or o.get('p') or 0)
            except (TypeError, ValueError):
                return 0.0

        best = max(asks, key=_px)
        uid = order_id_of(best)
        if not uid:
            return False
        try:
            vol = float(best.get('remaining_volume')
                        or best.get('volume')
                        or best.get('rv') or 0)
        except (TypeError, ValueError):
            vol = 0.0
        px = _px(best)
        self.sell_orders_tracking = [{
            'uuid': uid,
            'price': px,
            'volume': vol,
            'tier': 1,
            'filled': False,
        }]
        self.unfilled_sell_count = 1
        self.filled_sell_count = 0
        sell_uuids.clear()
        sell_uuids.add(uid)
        if px > 0:
            self.last_sell_base_price = px
        self.last_sell_placement_time = time.time()
        self._mark_sell_stable(5.0)
        # 여분 ask 정리 (이중 매도 방지) — keep 제외
        extras = [order_id_of(a) for a in asks
                  if order_id_of(a) and order_id_of(a) != uid]
        extras = [u for u in extras if u]
        if extras:
            try:
                OrderCanceler().cancel_orders_parallel(extras)
            except Exception:
                pass
        print_log(LogLevel.INFO,
                  f"매도 tracking 흡수 uuid={str(uid)[:8]}… "
                  f"px={px:,.4f} vol={vol:.8f} (취소 없이 유지)")
        return True

    def _cancel_open_asks_for_replace(self, symbol):
        """해당 심볼 미체결 ask 전량 취소 — 추적 UUID + open ask 배치 둘 다.
        (UUID만 취소하면 미추적 orphan ask가 남아 매도 2건이 됨)"""
        uuids = [e['uuid'] for e in self.sell_orders_tracking
                 if e.get('uuid') and not e.get('filled')]
        if sell_uuids:
            uuids.extend(list(sell_uuids))
        uuids = list(dict.fromkeys(u for u in uuids if u))
        if uuids:
            OrderCanceler().cancel_orders_parallel(uuids)
        # orphan/미추적 ask까지 제거 (항상)
        OrderCanceler().cancel_symbol_sell_orders(symbol)
        sell_uuids.clear()
        self.sell_orders_tracking = []
        self.filled_sell_count = 0
        self.unfilled_sell_count = 0

    def _purge_extra_asks(self, symbol, keep_uuid):
        """keep_uuid 외 해당 심볼 ask가 있으면 즉시 취소 — 이중 매도 사후 차단."""
        if not keep_uuid:
            return
        try:
            # 로컬에 남은 다른 UUID
            extras = [u for u in list(sell_uuids) if u and u != keep_uuid]
            for e in self.sell_orders_tracking:
                u = e.get('uuid')
                if u and u != keep_uuid and not e.get('filled'):
                    extras.append(u)
            extras = list(dict.fromkeys(extras))
            if extras:
                OrderCanceler().cancel_orders_parallel(extras)
                for u in extras:
                    sell_uuids.discard(u)
            # 거래소 open ask 중 keep 제외는 open-cancel이 keep까지 지울 수 있어
            # UUID 단위만 정리. tracking은 keep 1건으로 고정.
            self.sell_orders_tracking = [
                e for e in self.sell_orders_tracking
                if e.get('uuid') == keep_uuid and not e.get('filled')
            ]
            if not self.sell_orders_tracking:
                # keep만 남기기 위해 tracking 재구성은 호출측이 함
                pass
            self.unfilled_sell_count = 1 if keep_uuid in sell_uuids or self.sell_orders_tracking else 0
        except Exception:
            pass

    def _try_cancel_and_new_sell(self, symbol, sell_price, sell_volume):
        """미체결 ask 1건이면 cancel_and_new 1RTT로 전량 교체.
        동일 호가·동일 수량 재교체는 API 호출 전에 거부 (깜빡임 최종차단)."""
        pending = [e for e in self.sell_orders_tracking if not e.get('filled')]
        if len(pending) != 1:
            return None
        prev = pending[0].get('uuid')
        if not prev:
            return None
        try:
            prev_px = float(pending[0].get('price') or 0)
            prev_vol = float(pending[0].get('volume') or 0)
        except (TypeError, ValueError):
            prev_px, prev_vol = 0.0, 0.0
        try:
            same_px = (
                UpbitTickSystem.format_order_price(float(sell_price))
                == UpbitTickSystem.format_order_price(prev_px))
        except Exception:
            same_px = abs(float(sell_price) - prev_px) <= 1e-15
        px_ref = float(sell_price or prev_px or 0)
        if (same_px and prev_vol > 0
                and abs(float(sell_volume) - prev_vol) * px_ref
                < MIN_ORDER_AMOUNT):
            return None
        data = OrderCanceler.cancel_and_new_order(
            prev, sell_price, sell_volume, 'limit')
        if not data:
            return None
        new_uid = data.get('new_order_uuid') or data.get('uuid')
        if not new_uid:
            return None
        sell_uuids.discard(prev)
        sell_uuids.add(new_uid)
        return new_uid

    def _tracked_sell_volume(self):
        """미체결 추적 매도 수량 합."""
        return sum(float(e.get('volume') or 0)
                   for e in self.sell_orders_tracking if not e.get('filled'))

    def _open_sell_count(self):
        return sum(1 for e in self.sell_orders_tracking if not e.get('filled'))

    def _free_notional(self, symbol, available_volume, ref_price=0.0):
        """가용(미잠금) 잔량의 평가액."""
        px = ref_price or RealMarketData.get_current_price(symbol) or 0.0
        return max(float(available_volume), 0.0) * max(float(px), 0.0)

    def _mark_sell_stable(self, seconds=3.0):
        """안정 윈도우 폐기 — 무조건 매도/사후합산. no-op."""
        self._sell_stable_until = 0.0
        self._free_above_min_streak = 0

    def _in_sell_stable_window(self):
        """안정 윈도우 폐기 — 항상 False (교체/합산 지연 금지)."""
        return False

    def _should_replace_for_free(self, symbol, available_volume, ref_price=0.0,
                                   buy_count_increased=False):
        """열린 매도 위 추가잔량(≥5000원)이면 합산 필요.
        buy_count_increased면 1틱, 아니면 2틱 연속 확인(유령 free 완화)."""
        free_krw = self._free_notional(symbol, available_volume, ref_price)
        if free_krw < MIN_ORDER_AMOUNT:
            self._free_above_min_streak = 0
            return False
        if self._in_sell_stable_window():
            return False
        self._free_above_min_streak = getattr(self, '_free_above_min_streak', 0) + 1
        need = 1 if buy_count_increased else 2
        return self._free_above_min_streak >= need

    def _get_cached_symbol_info(self, symbol):
        """Private WS 캐시에서 (balance, locked, avg_buy_price) 직접 조회 — O(1).
        WS 미연결 시 AccountChecker REST 폴백."""
        if private_ws._is_initialized:
            return private_ws.get_symbol_info(symbol)
        return AccountChecker().get_symbol_info(symbol)

    def has_holdings(self, symbol):
        balance, locked, avg_buy_price = self._get_cached_symbol_info(symbol)
        return (balance + locked) >= MIN_HOLDING_VOLUME

    def has_pending_sell_orders(self, symbol):
        return self.unfilled_sell_count > 0

    def has_open_sell_orders(self):
        if self.unfilled_sell_count > 0:
            return True
        if sell_uuids:
            return True
        return any(not e.get('filled') for e in self.sell_orders_tracking)

    def _sell_order_is_filled(self, order_info):
        return order_is_filled(normalize_order(order_info) if order_info else None)

    def _ladder_detail(self, dynamic_buyer):
        """사다리 상태 한 줄 (로그용)."""
        if dynamic_buyer is None:
            return "buyer=None"
        plan = getattr(dynamic_buyer, 'active_planned_orders', None) or []
        pending = getattr(dynamic_buyer, 'pending_orders', None) or []
        partial = getattr(dynamic_buyer, 'partial_levels', None) or {}
        failed = getattr(dynamic_buyer, '_failed_replaces', None) or []
        return (
            f"sym={getattr(dynamic_buyer, 'symbol', '?')} "
            f"active={bool(getattr(dynamic_buyer, 'is_active', False))} "
            f"ended={bool(getattr(dynamic_buyer, '_cycle_ended', False))} "
            f"exec={int(getattr(dynamic_buyer, 'executed_count', 0) or 0)}/{len(plan)} "
            f"pending={len(pending)} partial={len(partial)} "
            f"failed_repl={len(failed)}")

    def _dust_detail(self, symbol, total_vol):
        """먼지진 판정 상세 (수량·평가액·최소주문금액)."""
        notional = holding_notional_krw(symbol, total_vol)
        px = RealMarketData.get_current_price(symbol) or 0.0
        return (
            f"{symbol} vol={float(total_vol or 0):.8f} "
            f"px={float(px):.4f} 평가≈{notional:,.0f}원 "
            f"한도={MIN_ORDER_AMOUNT}원 "
            f"먼지={'Y' if 0 < notional < MIN_ORDER_AMOUNT else 'N'}")

    def _log_dust_once(self, key, message, level=None):
        """동일 key 먼시/사다리 대기 로그는 5초에 1회."""
        if level is None:
            level = LogLevel.WARNING
        now = time.time()
        last = getattr(self, '_dust_log_ts', None) or {}
        if now - float(last.get(key, 0) or 0) < 5.0:
            return
        last[key] = now
        self._dust_log_ts = last
        print_log(level, message)

    def _buy_ladder_active(self, dynamic_buyer):
        """미체결 매수/부분체결/재주문 큐가 살아 있거나,
        is_active인 채 미배치 레벨이 남으면 True.
        _cycle_ended/비활성 바이어는 False — 의도적 종료 후 다음 사이클 허용."""
        if dynamic_buyer is None:
            return False
        if getattr(dynamic_buyer, '_cycle_ended', False):
            return False
        if not getattr(dynamic_buyer, 'is_active', False):
            return False
        if getattr(dynamic_buyer, 'pending_orders', None):
            return True
        if getattr(dynamic_buyer, 'partial_levels', None):
            return True
        if getattr(dynamic_buyer, '_failed_replaces', None):
            return True
        plan = getattr(dynamic_buyer, 'active_planned_orders', None) or []
        done = int(getattr(dynamic_buyer, 'executed_count', 0) or 0)
        if plan and done < len(plan):
            return True
        return False

    def _complete_cycle_on_sell_done(self, trading_manager, dynamic_buyer=None,
                                      force=False, profit_percentages=None):
        """매도 완료 → 잔여 매수 취소 → REST로 전량매도 확정 후에만 사이클 종료.
        force=True: 사다리 진행 중이어도 매수 취소 후 종료 (보유0/전량매도).
        잔량(≥5000원)이 남으면 재매도하고 False (다음 사이클 금지)."""
        if not force and self._buy_ladder_active(dynamic_buyer):
            self._log_dust_once(
                'cycle_block',
                "cycle-end blocked — buy ladder still active "
                f"({self._ladder_detail(dynamic_buyer)})")
            return False

        symbol = getattr(trading_manager, 'current_symbol', None) or (
            getattr(dynamic_buyer, 'symbol', None) if dynamic_buyer else None)
        print_log(LogLevel.INFO,
                  f"사이클 종료 진행 force={force} "
                  f"sym={symbol} | {self._ladder_detail(dynamic_buyer)}")

        # 1) 매수 사다리 즉시 정지 (추가 체결 콜백 차단)
        pending_uuids = []
        if dynamic_buyer is not None:
            for p in list(getattr(dynamic_buyer, 'pending_orders', []) or []):
                uid = p.get('uuid') if isinstance(p, dict) else None
                if uid:
                    pending_uuids.append(uid)
            pending_uuids.extend(
                DynamicBuyOrder._collect_pending_uuids_from_index(
                    getattr(dynamic_buyer, 'pending_by_uuid', None)))
            dynamic_buyer.is_active = False
            dynamic_buyer._cycle_ended = True
            dynamic_buyer.pending_orders.clear()
            dynamic_buyer._reset_runtime_indexes()
            dynamic_buyer.on_buy_fill_sell = None
            try:
                private_ws.clear_avg_sell_target(symbol)
            except Exception:
                pass

        # 2) 잔여 매수 — 해당 심볼만 취소 (전역 cancel = 타워커 매수 삭제+재배치 버그)
        try:
            if symbol:
                OrderCanceler().cancel_buy_orders_for_symbol(
                    symbol, extra_uuids=pending_uuids, verify=True)
            elif pending_uuids:
                OrderCanceler().cancel_orders_parallel(pending_uuids)
        except Exception as e:
            print_log(LogLevel.WARNING,
                      f"cycle-end buy cancel failed: {str(e)[:120]}")
            try:
                if symbol:
                    OrderCanceler().cancel_buy_orders_for_symbol(
                        symbol, extra_uuids=pending_uuids, verify=False)
            except Exception:
                pass

        # 3) sleep 없이 즉시 REST 전량 확인 — 미완이면 재매도
        if symbol:
            vol, notional, sellable = rest_holding_snapshot(symbol)
            if sellable:
                print_log(LogLevel.WARNING,
                          f"cycle-end blocked — 전량매도 미완 "
                          f"vol={vol:.8f} ≈{notional:,.0f}원 — 재매도")
                self.dust_holdings = False
                self.sell_orders_tracking = []
                self.unfilled_sell_count = 0
                self.filled_sell_count = 0
                sell_uuids.clear()
                trading_manager.sell_orders_executed = False
                pcts = profit_percentages or (
                    [float(getattr(trading_manager, '_last_profit_pct', 0.149) or 0.149)])
                try:
                    if self.place_sell_orders(
                            symbol, pcts, dynamic_buyer,
                            volume_hint=vol, force_replace=True):
                        trading_manager.mark_sell_orders_placed()
                except Exception as e:
                    print_log(LogLevel.ERROR,
                              f"cycle-end re-sell failed: {str(e)[:120]}")
                return False

        # 4) 매수 재스윕 + REST 재확인 (심볼 스코프만)
        try:
            if symbol:
                OrderCanceler().cancel_buy_orders_for_symbol(
                    symbol, verify=True)
        except Exception:
            pass
        if symbol:
            vol2, notional2, sellable2 = rest_holding_snapshot(symbol)
            if sellable2:
                print_log(LogLevel.WARNING,
                          f"cycle-end race fill — 전량미완 "
                          f"vol={vol2:.8f} ≈{notional2:,.0f}원 — 종료 철회")
                trading_manager.sell_orders_executed = False
                trading_manager.sell_orders_placed = False
                self.dust_holdings = False
                try:
                    pcts = profit_percentages or (
                        [float(getattr(trading_manager, '_last_profit_pct', 0.149)
                               or 0.149)])
                    if self.place_sell_orders(
                            symbol, pcts, dynamic_buyer,
                            volume_hint=vol2, force_replace=True):
                        trading_manager.mark_sell_orders_placed()
                except Exception:
                    pass
                return False

        # 5) REST 전량(또는 먼지) 확정 — 이때만 사이클 종료 플래그
        trading_manager.mark_sell_orders_executed()
        print_log(LogLevel.SELL_SUCCESS,
                  "매도 체결 — 전량확인 후 사이클 종료, 잔여 매수 취소 완료")
        # 이 워커 매도 UUID만 제거 (전역 clear = 타워커 ask 추적 유실)
        for e in list(self.sell_orders_tracking or []):
            uid = e.get('uuid') if isinstance(e, dict) else None
            if uid:
                sell_uuids.discard(uid)
        self.sell_orders_tracking = []
        self.filled_sell_count = 0
        self.unfilled_sell_count = 0
        self._sell_stable_until = 0.0
        self._free_above_min_streak = 0
        self._sell_base_provisional = False
        self._last_server_avg_seen = 0.0
        # 종료 직후 최종 REST (sleep 없음)
        if symbol:
            try:
                OrderCanceler().cancel_buy_orders_for_symbol(symbol, verify=True)
            except Exception:
                pass
            vol3, notional3, sellable3 = rest_holding_snapshot(symbol)
            if sellable3:
                print_log(LogLevel.WARNING,
                          f"cycle-end post-flag race — 전량미완 "
                          f"vol={vol3:.8f} ≈{notional3:,.0f}원 — 종료 철회")
                trading_manager.sell_orders_executed = False
                trading_manager.sell_orders_placed = False
                self.dust_holdings = False
                try:
                    pcts = profit_percentages or (
                        [float(getattr(trading_manager, '_last_profit_pct', 0.149)
                               or 0.149)])
                    if self.place_sell_orders(
                            symbol, pcts, dynamic_buyer,
                            volume_hint=vol3, force_replace=True):
                        trading_manager.mark_sell_orders_placed()
                except Exception:
                    pass
                return False
        return True
    def get_avg_buy_price(self, symbol):
        balance, locked, avg_buy_price = self._get_cached_symbol_info(symbol)
        return avg_buy_price

    def get_total_volume(self, symbol):
        balance, locked, avg_buy_price = self._get_cached_symbol_info(symbol)
        return balance + locked

    def get_available_volume(self, symbol):
        balance, locked, avg_buy_price = self._get_cached_symbol_info(symbol)
        return balance

    def cancel_all_sell_orders(self, symbol, wait=False):
        """해당 심볼 ask만 취소 — 절대 KRW 전 종목 스윕하지 않음."""
        print_log(LogLevel.INFO, f"Cancelling sell orders for {symbol}"
                  + (" (sync)" if wait else " (async)"))
        if wait:
            OrderCanceler().cancel_sell_orders(symbol=symbol)
            sell_uuids.clear()
        else:
            run_async(OrderCanceler().cancel_sell_orders, symbol)

    def _holding_sellable(self, symbol, total_vol=None):
        if total_vol is None:
            bal, locked, _ = self._instant_symbol_info(symbol)
            total_vol = max(bal, 0.0) + max(locked, 0.0)
        if total_vol < MIN_HOLDING_VOLUME:
            return False
        notional = holding_notional_krw(symbol, total_vol)
        if notional <= 0:
            return total_vol >= MIN_HOLDING_VOLUME
        return notional >= MIN_ORDER_AMOUNT

    def _resolve_sell_base_price(self, symbol, sell_base_price=None, dynamic_buyer=None):
        """매도 기준가 — 명시값 > myAsset > buyer VWAP/체결가. REST 대기 금지."""
        if sell_base_price is not None and float(sell_base_price) > 0:
            return float(sell_base_price)
        _, _, server_avg = self._get_cached_symbol_info(symbol)
        if server_avg and server_avg > 0:
            return float(server_avg)
        if self.last_sell_base_price and self.last_sell_base_price > 0:
            return float(self.last_sell_base_price)
        # ★ 매수체결 직후 WS 평단 지연 → buyer 체결로 즉시 기준가
        if dynamic_buyer is not None:
            try:
                fills = list(getattr(dynamic_buyer, 'executed_orders', None) or [])
                vol_sum = 0.0
                cost = 0.0
                for e in fills:
                    v = float(e.get('volume') or 0)
                    px = float(e.get('executed_price') or 0)
                    if v > 0 and px > 0:
                        vol_sum += v
                        cost += v * px
                if vol_sum > 0 and cost > 0:
                    return cost / vol_sum
            except Exception:
                pass
            try:
                lex = float(getattr(dynamic_buyer, 'last_executed_price', 0) or 0)
                if lex > 0:
                    return lex
            except Exception:
                pass
        try:
            floor = float(private_ws.cost_floor_price() or 0)
            if floor > 0:
                return floor
        except Exception:
            pass
        return 0.0

    @staticmethod
    def _sell_target_differs(base_a, base_b, profit_pct):
        """두 기준가로 만든 매도호가가 다르면 True. 같으면 오차 없음."""
        if base_b is None or base_b <= 0:
            return False
        if base_a is None or base_a <= 0:
            return True
        pa = UpbitTickSystem.calculate_sell_price(base_a, profit_pct)
        pb = UpbitTickSystem.calculate_sell_price(base_b, profit_pct)
        return pa != pb

    def maybe_correct_to_server_avg(self, symbol, profit_percentages,
                                     trading_manager, dynamic_buyer=None):
        """서버 avg로 매도 교정.
        provisional(로컬)이면 stable 무시·상하향 허용.
        확정 후에는 호가 상승만."""
        if not self.has_open_sell_orders():
            return False
        provisional = bool(self._sell_base_provisional)
        if (not provisional) and self._in_sell_stable_window():
            return False
        _, _, server_avg = self._get_cached_symbol_info(symbol)
        if server_avg is None or server_avg <= 0:
            return False

        profit_pct = profit_percentages[0] if profit_percentages else 0.0
        if not self._sell_target_differs(
                self.last_sell_base_price, server_avg, profit_pct):
            self._sell_base_provisional = False
            self._last_server_avg_seen = float(server_avg)
            return False

        if (self._last_server_avg_seen > 0
                and not self._sell_target_differs(
                    self._last_server_avg_seen, server_avg, profit_pct)
                and not provisional):
            return False

        new_px = UpbitTickSystem.calculate_sell_price(float(server_avg), profit_pct)
        # 손해방지 하한
        min_px = UpbitTickSystem.min_no_loss_sell_price(float(server_avg))
        if min_px > 0 and new_px + 1e-12 < min_px:
            new_px = min_px
        tracked_px = self._tracked_sell_price()
        # 동일 틱이면 기준가만 동기화
        if tracked_px > 0 and abs(new_px - tracked_px) <= 1e-15:
            self.last_sell_base_price = float(server_avg)
            self._sell_base_provisional = False
            self._last_server_avg_seen = float(server_avg)
            return False
        # 틱이 다르면 provisional 아니어도 교정 (상·하향)

        bal, locked, _ = self._get_cached_symbol_info(symbol)
        total = max(float(bal or 0), 0.0) + max(float(locked or 0), 0.0)
        prev = self.last_sell_base_price
        ok = self.place_sell_orders(
            symbol, profit_percentages, dynamic_buyer,
            sell_base_price=float(server_avg), force_replace=False,
            volume_hint=total, avg_refresh=True)
        if ok:
            self._sell_base_provisional = False
            self._last_server_avg_seen = float(server_avg)
            trading_manager.mark_sell_orders_placed()
            if dynamic_buyer is not None:
                self._sell_placed_at_buy_count = dynamic_buyer.executed_count
            if prev:
                print_log(LogLevel.INFO,
                          f"매도교정(서버평단) {prev:,.8f} → {server_avg:,.8f}")
            else:
                print_log(LogLevel.INFO,
                          f"매도교정(서버평단) → {server_avg:,.8f}")
        return ok

    def _resolve_ask_volume(self, symbol, volume_hint=None, force_rest=False):
        """매도 수량 — (free, locked, total).
        hint는 total 하한만 — avail에 복제하지 않음(이중집계→insufficient 방지)."""
        if force_rest:
            bal, locked, _ = self._confirmed_ask_available(symbol)
        else:
            bal, locked, _ = self._get_cached_symbol_info(symbol)
            if bal < 0:
                bal, locked = 0.0, 0.0
        avail = max(float(bal), 0.0)
        locked = max(float(locked), 0.0)
        hint = max(float(volume_hint or 0.0), 0.0)
        held = avail + locked
        total = max(held, hint)
        return avail, locked, total

    def _tracked_sell_price(self):
        for e in self.sell_orders_tracking:
            if not e.get('filled') and e.get('price'):
                return float(e['price'])
        return 0.0

    def _exchange_ask_vol_px(self, symbol):
        """거래소 미체결 ask 잔량합·최고가. 없으면 (0, 0)."""
        asks = self._fetch_open_asks(symbol)
        if not asks:
            return 0.0, 0.0
        tot = 0.0
        best = 0.0
        for a in asks:
            try:
                tot += float(a.get('remaining_volume')
                             or a.get('volume') or a.get('rv') or 0)
            except (TypeError, ValueError):
                pass
            try:
                best = max(best, float(a.get('price') or a.get('p') or 0))
            except (TypeError, ValueError):
                pass
        return max(tot, 0.0), best

    def place_sell_orders(self, symbol, profit_percentages, dynamic_buyer=None,
                          sell_base_price=None, force_replace=False,
                          volume_hint=None, avg_refresh=False):
        """전량 단일 매도 1건 — 근본 규칙:
        1) 열린 ask 없으면 즉시 POST (hint/WS)
        2) 열린 ask 있으면 실 free(비유령)≥5000원일 때만 합산
        3) 동일 호가·동일 수량(갭<5000원)이면 절대 cancel_and_new 금지
        4) 호가 변경(avg_refresh)만 가격 교체
        """
        lock = getattr(self, '_sell_place_lock', None)
        if lock is None:
            self._sell_place_lock = threading.Lock()
            lock = self._sell_place_lock
        if not lock.acquire(blocking=False):
            # 다른 스레드가 POST 중 — 실패가 아님 (ERROR 스팸 금지)
            self._last_sell_skip = 'busy'
            return False
        self._last_sell_skip = ''
        self._sell_placing = True
        try:
            profit_pct = profit_percentages[0] if profit_percentages else 0.0
            base_hint = self._resolve_sell_base_price(
                symbol, sell_base_price, dynamic_buyer)
            if base_hint <= 0:
                try:
                    base_hint = float(private_ws.cost_floor_price() or 0)
                except Exception:
                    base_hint = 0.0
            if base_hint <= 0:
                return False

            buy_floor = float(base_hint)
            sell_price = UpbitTickSystem.calculate_sell_price(base_hint, profit_pct)
            min_px = UpbitTickSystem.min_no_loss_sell_price(buy_floor)
            if min_px > 0 and sell_price + 1e-12 < min_px:
                print_log(LogLevel.WARNING,
                          f"매도호가 손해방지 상향 {sell_price:,.8f} → {min_px:,.8f} "
                          f"(avg={buy_floor:,.8f})")
                sell_price = min_px

            def _px_key(p):
                try:
                    return UpbitTickSystem.format_order_price(float(p))
                except Exception:
                    return ''

            def _keep_existing(tag_note=None):
                if base_hint > 0:
                    self.last_sell_base_price = base_hint
                if avg_refresh:
                    self._sell_base_provisional = False
                return True

            def _commit(uid, px, qty, tag):
                self.sell_orders_tracking = [{
                    'uuid': uid, 'price': px, 'volume': qty,
                    'tier': 1, 'filled': False,
                }]
                self.unfilled_sell_count = 1
                self.filled_sell_count = 0
                self.last_sell_base_price = base_hint
                self.last_sell_placement_time = time.time()
                self._last_replace_px = float(px)
                self._last_replace_t = time.time()
                self._last_committed_qty = float(qty)
                sell_uuids.clear()
                sell_uuids.add(uid)
                self._mark_sell_stable(0)
                print_log(LogLevel.SUCCESS,
                          f"매도주문({tag}) 평단={base_hint:,.4f} +{profit_pct}% "
                          f"→ {px:,.4f} qty={qty:.8f} uuid={str(uid)[:8]}…")
                return True

            def _post_full(qty, px, tag):
                qty = UpbitTickSystem.floor_volume(qty)
                live_px = RealMarketData.get_current_price(symbol) or px
                if qty <= 0 or qty * max(float(live_px), 0.0) < MIN_ORDER_AMOUNT:
                    return False
                order = SellOrder(symbol, qty, px, max_available=qty)
                if order and order.uuid:
                    placed = float(getattr(order, 'placed_volume', qty) or qty)
                    return _commit(order.uuid, px, placed, tag)
                return False

            def _order_ask_vol(o):
                for k in ('remaining_volume', 'remaining_quantity', 'rv',
                          'volume', 'v'):
                    if o.get(k) is None:
                        continue
                    try:
                        return float(o[k])
                    except (TypeError, ValueError):
                        continue
                return 0.0

            def _ask_snapshot(force=True):
                if force:
                    self._open_asks_cache = None
                asks = self._fetch_open_asks(symbol, force=force)
                if not asks:
                    return 0.0, 0.0, None
                def _ap(o):
                    try:
                        return float(o.get('price') or o.get('p') or 0)
                    except (TypeError, ValueError):
                        return 0.0
                best = max(asks, key=_ap)
                return (sum(_order_ask_vol(a) for a in asks),
                        _ap(best), order_id_of(best))

            open_n = self._open_sell_count()
            hint_vol = max(float(volume_hint or 0), 0.0)

            # ★ 동일호가·동일수량 깜빡임 차단 (전량-fast 루프 원흉)
            last_px = float(getattr(self, '_last_replace_px', 0) or 0)
            last_qty = float(getattr(self, '_last_committed_qty', 0) or 0)
            last_t = float(getattr(self, '_last_replace_t', 0) or 0)
            age = time.time() - last_t if last_t > 0 else 1e9
            same_px_qty = (
                last_px > 0 and last_qty > 0
                and _px_key(sell_price) == _px_key(last_px)
                and abs(hint_vol - last_qty) * max(float(sell_price), 1e-15)
                < float(MIN_ORDER_AMOUNT))
            if not force_replace and same_px_qty and age < 60.0:
                if open_n > 0:
                    return _keep_existing()
                if self._adopt_open_ask_tracking(symbol):
                    return True
                # ★ ask 실존 없으면 쿨다운으로 성공 위장 금지 — 아래 POST

            # ── 1) 최초 매도: tracking 없고 hint 있으면 즉시 POST ──
            # ★ 초고속: REST ask GET 생략 — 캐시만 확인 후 volume_hint로 POST
            #   (이중POST는 _sell_place_lock + tracking으로 차단)
            if open_n == 0 and hint_vol >= MIN_HOLDING_VOLUME and not force_replace:
                ask_vol_c, ask_px_c, ask_uid_c = _ask_snapshot(force=False)
                if ask_vol_c > 0 or ask_uid_c:
                    if self._adopt_open_ask_tracking(symbol):
                        return True
                    return _keep_existing()
                bal_w, loc_w, _ = self._get_cached_symbol_info(symbol)
                if bal_w < 0:
                    bal_w, loc_w = 0.0, 0.0
                held_fast = max(
                    hint_vol,
                    max(float(bal_w), 0.0) + max(float(loc_w), 0.0))
                # ★ ask 없는 상태에서 동일수량 쿨다운 return True 금지
                #   (취소된 매도 후 매수만 남는 버그 원흉)
                if _post_full(held_fast, sell_price, "전량-fast"):
                    if avg_refresh:
                        self._sell_base_provisional = False
                    return True

            # ── 2) 사후통제 ──
            bal, locked, _ = self._confirmed_ask_available(symbol)
            bal = max(float(bal), 0.0)
            locked = max(float(locked), 0.0)
            ask_vol, ask_px, ask_uid = _ask_snapshot(force=True)

            if ask_uid and self._open_sell_count() == 0:
                self._adopt_open_ask_tracking(symbol)
            open_n = self._open_sell_count()
            tracked_px = self._tracked_sell_price() or ask_px
            tracked_vol = self._tracked_sell_volume()
            cur_px = tracked_px if tracked_px > 0 else ask_px
            cover = max(float(ask_vol), float(tracked_vol))
            has_ask = open_n >= 1 or ask_vol > 0 or cover > 0
            px_ref = float(sell_price or base_hint or cur_px or 0)

            # 유령 free: ask 있는데 locked≪ask 이고 balance≈전량
            ghost_free = (
                cover > 0
                and locked < cover * 0.30
                and bal >= cover * 0.50)
            free_vol = 0.0 if ghost_free else bal
            # buy_up hint — REST free 지연 시 힌트 초과분만 합산
            if cover > 0 and hint_vol > cover:
                hint_extra = hint_vol - cover
                if hint_extra * max(px_ref, 1e-15) >= MIN_ORDER_AMOUNT:
                    free_vol = max(free_vol, hint_extra)
            # 합산 목표 = 실ask + 실free (bal+locked 더블카운트 금지)
            if has_ask:
                sell_volume = UpbitTickSystem.floor_volume(cover + free_vol)
            else:
                sell_volume = UpbitTickSystem.floor_volume(
                    max(bal + locked, hint_vol, cover))
            same_tick = (
                cur_px > 0 and _px_key(sell_price) != ''
                and _px_key(sell_price) == _px_key(cur_px))
            qty_gap_krw = (
                abs(sell_volume - cover) * px_ref if cover > 0
                else sell_volume * px_ref)
            # ★ 방금 넣은 (px,qty)와 같으면 cover 오탐이어도 절대 재교체 금지
            last_cqty = float(getattr(self, '_last_committed_qty', 0) or 0)
            last_cpx = float(getattr(self, '_last_replace_px', 0) or 0)
            last_ct = float(getattr(self, '_last_replace_t', 0) or 0)
            same_as_last = (
                has_ask and last_cqty > 0 and last_cpx > 0
                and _px_key(sell_price) == _px_key(last_cpx)
                and abs(sell_volume - last_cqty) * px_ref < MIN_ORDER_AMOUNT
                and (time.time() - last_ct) < 120.0)
            # ★ 동일호가·동일수량 → 무조건 유지
            identical = (
                has_ask and cover > 0
                and same_tick
                and qty_gap_krw < MIN_ORDER_AMOUNT)
            if identical or same_as_last:
                return _keep_existing()

            need_merge = (
                has_ask and free_vol * px_ref >= MIN_ORDER_AMOUNT
                and qty_gap_krw >= MIN_ORDER_AMOUNT)
            need_reprice = (
                has_ask and not same_tick
                and (avg_refresh or force_replace
                     or sell_price > cur_px + 1e-15)
                and sell_price + 1e-12 >= UpbitTickSystem.min_no_loss_sell_price(
                    base_hint))
            # 다중 ask 정리만 force_replace로 허용 (동일수량 단건 재교체 금지)
            need_force = bool(force_replace and open_n > 1)

            place_px = sell_price if need_reprice or not has_ask else (
                cur_px if cur_px > 0 else sell_price)
            if need_reprice:
                place_px = sell_price

            # 교체 직전 최종 가드
            if (has_ask and cover > 0
                    and _px_key(place_px) == _px_key(cur_px or ask_px or last_cpx)
                    and abs(sell_volume - max(cover, last_cqty)) * px_ref
                    < MIN_ORDER_AMOUNT
                    and not need_force):
                return _keep_existing()

            if has_ask and (need_merge or need_reprice or need_force):
                if self._open_sell_count() == 0 and ask_uid:
                    self.sell_orders_tracking = [{
                        'uuid': ask_uid, 'price': ask_px or place_px,
                        'volume': ask_vol or sell_volume,
                        'tier': 1, 'filled': False,
                    }]
                    self.unfilled_sell_count = 1
                    sell_uuids.clear()
                    sell_uuids.add(ask_uid)

                live_px = RealMarketData.get_current_price(symbol) or place_px
                if (sell_volume > 0
                        and sell_volume * max(float(live_px), 0.0)
                        >= MIN_ORDER_AMOUNT):
                    # 또 한 번: 동일 (px,qty)면 API 호출 자체 금지
                    if (_px_key(place_px) == _px_key(cur_px or ask_px)
                            and abs(sell_volume - cover) * px_ref
                            < MIN_ORDER_AMOUNT):
                        return _keep_existing()
                    self._last_cancel_and_new_t = time.time()
                    new_uid = self._try_cancel_and_new_sell(
                        symbol, place_px, sell_volume)
                    if new_uid:
                        tag = ("전량합산" if need_merge
                               else ("교정" if need_reprice else "교체"))
                        return _commit(new_uid, place_px, sell_volume, tag)
                    # cancel_and_new 실패 — 합산 필요하면 취소+재POST
                    # ★ 단 목표 수량·호가가 직전과 같으면 재POST 금지
                    if need_merge:
                        if (last_cqty > 0
                                and _px_key(place_px) == _px_key(last_cpx or cur_px)
                                and abs(sell_volume - last_cqty) * px_ref
                                < MIN_ORDER_AMOUNT):
                            return _keep_existing()
                        print_log(
                            LogLevel.WARNING,
                            f"매도합산 cancel_and_new 실패 → 취소재POST "
                            f"{symbol} qty={sell_volume:.8f}")
                        self._cancel_open_asks_for_replace(symbol)
                        self._open_asks_cache = None
                        bal2, loc2, _ = self._confirmed_ask_available(symbol)
                        held2 = max(
                            float(bal2) + float(max(loc2, 0)),
                            hint_vol, sell_volume)
                        if _post_full(held2, place_px, "전량합산"):
                            if avg_refresh:
                                self._sell_base_provisional = False
                            return True
                        return False
                    if same_tick or same_as_last or not need_force:
                        return _keep_existing()
                    self._cancel_open_asks_for_replace(symbol)
                    self._open_asks_cache = None
                    bal2, loc2, _ = self._confirmed_ask_available(symbol)
                    held2 = max(float(bal2) + float(max(loc2, 0)), hint_vol)
                    if _post_full(held2, place_px,
                                  "전량합산" if need_merge else "전량"):
                        if avg_refresh:
                            self._sell_base_provisional = False
                        return True
                return _keep_existing()

            if has_ask:
                return _keep_existing()

            # ── 3) ask 없음 → 전량 POST (REST held) ──
            held = max(bal + locked, hint_vol)
            for attempt in range(3):
                if attempt > 0:
                    bal, locked, _ = self._confirmed_ask_available(symbol)
                    bal = max(float(bal), 0.0)
                    locked = max(float(locked), 0.0)
                    ask_vol, ask_px, ask_uid = _ask_snapshot(force=True)
                    held = max(bal + locked, hint_vol, float(ask_vol))
                if ask_vol > 0:
                    if self._adopt_open_ask_tracking(symbol):
                        return True
                    return True
                shrink = 1.0 if attempt == 0 else (0.999 if attempt == 1 else 0.995)
                qty = (UpbitTickSystem.floor_volume(held) if shrink >= 1.0 - 1e-15
                       else UpbitTickSystem.ask_safe_volume(held, shrink=shrink))
                if _post_full(qty, sell_price, "전량"):
                    if avg_refresh:
                        self._sell_base_provisional = False
                    return True
            return self._adopt_open_ask_tracking(symbol)
        except Exception as e:
            print_log(LogLevel.ERROR, f"매도주문 실패: {str(e)}")
            traceback.print_exc()
            return False
        finally:
            self._sell_placing = False
            try:
                lock.release()
            except Exception:
                pass

    def _emergency_stop_sell(self, symbol, trading_manager, reason_log):
        """스탑로스/라스트바이 공통 — 동기 매도취소 후 해당 심볼 전량 시장가 매도.
        비동기 취소+available(balance only) 조합은 locked 잔량 때문에 비상매도가
        스킵되며 stop_loss_triggered도 안 올라가는 문제가 있었음."""
        self.cancel_all_sell_orders(symbol, wait=True)
        # 취소 직후 Private WS 캐시가 늦을 수 있어 REST로 해당 심볼만 재조회
        try:
            bal, locked, _ = AccountChecker._rest_symbol_info(
                ACCESS_KEY, SECRET_KEY, symbol)
            # 미보유 시 REST 헬퍼가 (-1,-1,-1) 반환 — 캐시 폴백
            if bal < 0:
                total_volume = self.get_total_volume(symbol)
            else:
                total_volume = bal + locked
                # WS 캐시도 동기화 (이후 조회 일관성)
                if private_ws._is_initialized:
                    private_ws.asset_cache[symbol] = {
                        'balance': float(bal), 'locked': float(locked),
                        'avg_buy_price': private_ws.asset_cache.get(symbol, {}).get('avg_buy_price', 0),
                    }
        except Exception:
            total_volume = self.get_total_volume(symbol)

        if total_volume < MIN_HOLDING_VOLUME:
            # 보유 없이 플래그만 올리면 "스탑로스 아닌데 스탑로스 처리"로 보임
            print_log(LogLevel.WARNING,
                      f"{reason_log}: 매도 수량 없음 ({symbol} vol={total_volume:.8f}) "
                      f"— 스탑로스 미발동")
            return False
        print_log(LogLevel.WARNING, f"{reason_log}: {total_volume:.6f} {symbol}")
        self.place_emergency_sell_order(symbol, total_volume)
        # 스탑로스 시 잔여 매수(분할/재투자)도 즉시 전량 취소
        cancel_buy_orders_async()
        trading_manager.mark_stop_loss_triggered()
        start_alarm_loop()
        return True

    def _stop_loss_loss_pct(self, symbol, current_price=None, dynamic_buyer=None):
        """(loss_pct, current, avg) — 유효하지 않으면 (None, …).
        보유 없거나 평단/현재가 이상치면 스탑로스 금지.
        평단은 myAsset/서버 캐시."""
        if self.get_total_volume(symbol) < MIN_HOLDING_VOLUME:
            return None, current_price, 0.0
        if current_price is None:
            current_price = RealMarketData.get_current_price(symbol)
        avg_buy_price = 0.0
        # 정답지=myAsset/서버 캐시 avg
        try:
            avg_buy_price = float(self.get_avg_buy_price(symbol) or 0.0)
        except Exception:
            avg_buy_price = 0.0
        if avg_buy_price <= 0:
            avg_buy_price = self.get_avg_buy_price(symbol)
        if current_price is None or current_price <= 0 or avg_buy_price <= 0:
            return None, current_price, avg_buy_price
        # 평단 대비 현재가가 비정상 비율이면(티커/심볼 혼선) 오발동 방지
        ratio = current_price / avg_buy_price
        if ratio > 5.0 or ratio < 0.05:
            print_log(LogLevel.WARNING,
                      f"Stop-loss skip — price/avg sanity fail "
                      f"(px={current_price}, avg={avg_buy_price}, ratio={ratio:.4f}) "
                      f"symbol={symbol}")
            return None, current_price, avg_buy_price
        loss_pct = ((current_price - avg_buy_price) / avg_buy_price) * 100.0
        return loss_pct, current_price, avg_buy_price

    def check_stop_loss(self, symbol, trading_manager, dynamic_buyer=None):
        """스탑로스 조건 체크 (-8% 이상 하락 시 매도).
        분할매수가 남아 있으면(최종 레벨 미체결) 절대 발동하지 않음."""
        # 매수 사다리 미완료 → 스탑로스 금지
        # (active_planned_orders 가 빈 리스트여도 가드: 빈 리스트는 falsy라 예전엔 가드가 스킵됨)
        if dynamic_buyer is not None:
            n_plan = len(getattr(dynamic_buyer, 'active_planned_orders', None) or [])
            done = int(getattr(dynamic_buyer, 'executed_count', 0) or 0)
            if n_plan > 0 and done < n_plan:
                return False
            # 계획 없이 매수 진행 중(pending)이면 금지
            if getattr(dynamic_buyer, 'is_active', False) and getattr(
                    dynamic_buyer, 'pending_orders', None):
                return False

        current_time = time.time()
        if (self.last_stop_loss_check and
            (current_time - self.last_stop_loss_check) < self.stop_loss_check_interval):
            return False
        self.last_stop_loss_check = current_time

        try:
            loss_percentage, current_price, avg_buy_price = self._stop_loss_loss_pct(
                symbol, dynamic_buyer=dynamic_buyer)
            if loss_percentage is None:
                return False

            if loss_percentage <= STOP_LOSS_PERCENTAGE:
                print_log(LogLevel.WARNING,
                         f"Stop loss triggered! Loss: {loss_percentage:.2f}% "
                         f"(Current: {current_price}, Avg: {avg_buy_price}) "
                         f"symbol={symbol}")
                return self._emergency_stop_sell(
                    symbol, trading_manager, "Emergency sell at market price")

        except Exception as e:
            print_log(LogLevel.EXCEPTION, f"Stop loss check error: {str(e)}")

        return False

    def check_last_buy_stop(self, symbol, trading_manager, cached_price=None,
                            dynamic_buyer=None):
        """마지막(최종) 매수 직전 전용 — 이미 -8%면 최종매수 스킵 후 전량 매도.
        중간 라운드에서는 호출되면 안 됨(호출측 가드 필수)."""
        try:
            loss_percentage, current_price, avg_buy_price = self._stop_loss_loss_pct(
                symbol, cached_price, dynamic_buyer=dynamic_buyer)
            if loss_percentage is None:
                return False

            if loss_percentage <= STOP_LOSS_PERCENTAGE:
                print_log(LogLevel.WARNING,
                         f"Last-buy stop! Loss {loss_percentage:.2f}% <= {STOP_LOSS_PERCENTAGE}% "
                         f"at last buy point (Current: {current_price}, Avg: {avg_buy_price}) "
                         f"symbol={symbol}")
                return self._emergency_stop_sell(
                    symbol, trading_manager, "Selling all holdings (skip last buy)")
        except Exception as e:
            print_log(LogLevel.EXCEPTION, f"Last-buy stop check error: {str(e)}")

        return False

    def place_emergency_sell_order(self, symbol, volume):
        """비상 시장가 매도 주문 — 수량 단위 재시도 포함."""
        try:
            raw_vol = UpbitTickSystem.floor_volume(float(volume) * 0.999999)
            last_err = None
            for decimals in (8, 6, 4, 2, 0):
                vol_str = UpbitTickSystem.format_order_volume(raw_vol, decimals)
                if vol_str == '0':
                    continue
                query = {
                    'market': 'KRW-' + symbol,
                    'side': 'ask',
                    'volume': vol_str,
                    EXCHANGE['order_type_field']: 'market',
                }
                headers = make_auth_headers(query)

                def api_call(q=query, h=headers):
                    return http_post_order(ORDER_URL, q, h, reason='sell_mkt')

                response = hot_api_call(api_call)
                order_uuid, err_body = response_order_or_error(response)
                if order_uuid:
                    print_log(LogLevel.SELL_SUCCESS,
                              f"Emergency sell order placed: {vol_str} {symbol}")
                    return True
                last_err = err_body
                err_l = str(err_body).lower()
                if 'invalid_volume' not in err_l and 'volume' not in err_l:
                    break
            print_log(LogLevel.ERROR, f"Failed to place emergency sell order: {last_err}")
            return False

        except Exception as e:
            print_log(LogLevel.ERROR, f"Emergency sell order error: {str(e)}")
            return False

    def _fetch_order_states_batch(self, uuids, force_rest=False):
        """여러 주문 UUID 상태 일괄 조회 — WS 캐시 우선.
        REST: 업비트 GET /v1/orders/uuids (최대 100/요청) → 미지원 시 개별 병렬."""
        result = {}
        missing = []
        if force_rest:
            missing = list(uuids)
        elif private_ws._is_initialized and private_ws.is_connected:
            for u in uuids:
                cached = private_ws.get_order_state(u)
                if cached:
                    state = str(cached.get('state', '')).lower()
                    if state in ('done', 'cancel'):
                        result[u] = cached
                        continue
                    try:
                        cached = normalize_order(cached)
                        if order_is_filled(cached):
                            result[u] = cached
                            continue
                    except (TypeError, ValueError):
                        pass
                    result[u] = cached
                else:
                    missing.append(u)
        else:
            missing = list(uuids)

        if not missing:
            return result

        # 업비트 신규 일괄 조회 API
        if EXCHANGE.get('supports_batch_query_ids'):
            batched = OrderCanceler.fetch_orders_by_uuids(missing)
            for u, order_info in batched.items():
                order_info = normalize_order(order_info)
                result[u] = order_info
                if private_ws._is_initialized:
                    with private_ws._order_lock:
                        prev = private_ws.order_cache.get(u)
                        private_ws.order_cache[u] = normalize_order(
                            order_info, prev=prev)
            still = [u for u in missing if u not in batched]
            if not still:
                return result
            missing = still

        def _rest_one(u):
            try:
                qkey = EXCHANGE.get('order_query_id_param') or EXCHANGE.get(
                    'order_id_param', 'uuid')
                params = {qkey: u}
                headers = make_auth_headers(params)
                response = http_get_hot(
                    ORDER_QUERY_URL, params=params, headers=headers)
                return u, response_json(response)
            except Exception as e:
                print_log(LogLevel.ERROR, f"주문 조회 폴백 오류 ({u[:8]}...): {str(e)}")
                return u, None

        if len(missing) == 1:
            pairs = [_rest_one(missing[0])]
        else:
            futs = [_ORDER_POOL.submit(_rest_one, u) for u in missing]
            pairs = []
            for f in as_completed(futs):
                try:
                    pairs.append(f.result())
                except Exception:
                    pass

        for u, order_info in pairs:
            if order_info:
                order_info = normalize_order(order_info)
                result[u] = order_info
                if private_ws._is_initialized:
                    with private_ws._order_lock:
                        prev = private_ws.order_cache.get(u)
                        private_ws.order_cache[u] = normalize_order(
                            order_info, prev=prev)
        return result

    def check_sell_fills(self, symbol, dynamic_buyer):
        """매도 per-order 체결 확인 — Private WS 캐시 우선 (API 호출 최소화).
        ★ 실제 매도 체결 1건이라도 있으면 True (잔여매수 취소·사이클 종료 트리거).
        수동 취소만으로는 True 안 됨."""
        self._sell_filled_this_check = False
        if not self.sell_orders_tracking:
            return False

        # 미확인 entry들의 UUID만 일괄 조회
        pending_uuids = [e['uuid'] for e in self.sell_orders_tracking if not e['filled']]
        if not pending_uuids:
            # 전부 filled 표시만 된 경우 — 실제 체결 건이 있을 때만
            return self.filled_sell_count > 0

        # WS 우선 + REST 스로틀 (매 루프 force_rest는 매도 POST까지 지연)
        now = time.time()
        ws_healthy = private_ws._is_initialized and private_ws.is_connected
        rest_interval = 0.2 if ws_healthy else 0.05
        force_rest = (now - getattr(self, '_last_sell_rest_check', 0)) >= rest_interval
        if force_rest:
            self._last_sell_rest_check = now
        order_states = self._fetch_order_states_batch(
            pending_uuids, force_rest=force_rest)

        any_real_fill = False
        for entry in self.sell_orders_tracking:
            if entry['filled']:
                continue
            order_info = order_states.get(entry['uuid'])

            if self._sell_order_is_filled(order_info):
                order_info = normalize_order(order_info)
                side = normalize_side(order_info.get('side'))
                # 매수(bid) UUID가 tracking에 섞이면 오탐 → 잔여 매수 전량 취소 버그
                if side and side != 'ask':
                    print_log(LogLevel.WARNING,
                              f"매도 tracking 오염(side={side}, uuid="
                              f"{str(entry.get('uuid', ''))[:8]}…) — 제거, 체결 무시")
                    entry['filled'] = True  # tracking 정리용 (실제 매도체결 아님)
                    self.unfilled_sell_count = max(0, self.unfilled_sell_count - 1)
                    sell_uuids.discard(entry['uuid'])
                    continue
                if entry.get('uuid') in buy_uuids:
                    print_log(LogLevel.WARNING,
                              f"매도 tracking이 매수 UUID — 제거 "
                              f"({str(entry.get('uuid', ''))[:8]}…)")
                    entry['filled'] = True
                    self.unfilled_sell_count = max(0, self.unfilled_sell_count - 1)
                    sell_uuids.discard(entry['uuid'])
                    continue
                entry['filled'] = True
                self.filled_sell_count += 1
                self.unfilled_sell_count = max(0, self.unfilled_sell_count - 1)
                sell_uuids.discard(entry['uuid'])
                executed_vol = order_executed_volume(order_info) or float(entry['volume'])
                sell_price = entry['price']

                print_log(LogLevel.SELL_SUCCESS,
                          f"매도#{entry['tier']} 체결 확인 (수량 {executed_vol:.6f} @ {sell_price:,.8f}원)")
                any_real_fill = True
                self._sell_filled_this_check = True
                # ★ 재투자/추가매수 금지 — 매도 체결 = 잔여매수 취소 후 사이클 종료
            elif order_info and order_info.get('state') == 'cancel':
                # 봇 교체/이중POST로 이전 uuid가 cancel된 것 — 즉시 재POST 금지
                # 거래소에 남은 ask 흡수. 없으면 tracking만 정리.
                entry['filled'] = True
                self.unfilled_sell_count = max(0, self.unfilled_sell_count - 1)
                sell_uuids.discard(entry['uuid'])
                now_c = time.time()
                last_c = float(getattr(self, '_sell_cancel_log_ts', 0) or 0)
                adopted = False
                try:
                    adopted = bool(self._adopt_open_ask_tracking(symbol))
                except Exception:
                    adopted = False
                if now_c - last_c >= 5.0:
                    self._sell_cancel_log_ts = now_c
                    print_log(
                        LogLevel.WARNING,
                        f"매도#{entry['tier']} cancel 감지 — "
                        + ("열린 ask 흡수·유지" if adopted
                           else "ask없음, 재POST는 쿨다운 후"))
                if adopted:
                    # 흡수 성공 시 직전 commit 시각 갱신 → 전량-fast 쿨다운
                    self._last_replace_t = time.time()
            else:
                pass

        return bool(any_real_fill or (
            self.filled_sell_count > 0
            and not any(not e.get('filled') for e in self.sell_orders_tracking)))

    def _reinvest_to_next_buy(self, dynamic_buyer, krw_amount, sell_tier):
        """폐기 — 매도 체결 후 재투자/추가매수 하지 않음.
        매도 = 잔여매수 취소 + 사이클 종료."""
        return

    def manage_sell_orders(self, symbol, profit_percentages, trading_manager, wait_count, dynamic_buyer=None):
        """매도 관리 — 한 번 걸면 유지. 취소/재주문은 아래만 허용:
        1) 매도 체결 후 잔량이 최소주문 이상
        2) 열린 매도 위 가용잔량 ≥5000원이 연속 3회 확인 (추가매수 확정)
        3) 추적상 매도 0건인데 보유만 있음 → 신규 1건
        4) 선매도 후 서버 avg 수신 시 오차 있으면 교정 (없으면 pass)"""

        sell_just_filled = self.check_sell_fills(symbol, dynamic_buyer)

        if any(e.get('filled') for e in self.sell_orders_tracking):
            self.sell_orders_tracking = [e for e in self.sell_orders_tracking if not e.get('filled')]
            self.unfilled_sell_count = sum(1 for e in self.sell_orders_tracking if not e.get('filled'))

        # 서버 평단 수신 → 오차 있을 때만 매도 교정
        if not sell_just_filled:
            self.maybe_correct_to_server_avg(
                symbol, profit_percentages, trading_manager, dynamic_buyer)

        if sell_just_filled:
            balance, locked, server_avg = self._confirmed_ask_available(symbol)
            current_avg = server_avg
        else:
            balance, locked, server_avg = self._get_cached_symbol_info(symbol)
            if balance < 0:
                balance, locked, server_avg = 0.0, 0.0, 0.0
            current_avg = server_avg
            if (balance + locked) < MIN_HOLDING_VOLUME:
                bal2, loc2, avg2 = self._get_fresh_symbol_info(symbol)
                if bal2 >= 0:
                    balance, locked = bal2, loc2
                    if avg2 > 0:
                        current_avg = avg2

        total_vol = max(balance, 0.0) + max(locked, 0.0)
        available_volume = max(balance, 0.0)
        fresh_avg = current_avg if current_avg and current_avg > 0 else 0.0

        # 1) 체결 후: 잔여매수 즉시 취소 → 잔량있으면 재매도 / 없으면 사이클 종료
        if sell_just_filled:
            # ★ 매도 체결 = 사다리 중지 + 잔여 매수 취소 (재투자 금지)
            if dynamic_buyer is not None:
                try:
                    pending_uuids = [
                        p.get('uuid') for p in (dynamic_buyer.pending_orders or [])
                        if p.get('uuid')]
                    pending_uuids.extend(
                        DynamicBuyOrder._collect_pending_uuids_from_index(
                            getattr(dynamic_buyer, 'pending_by_uuid', None)))
                    dynamic_buyer.is_active = False
                    dynamic_buyer._buys_halted = True
                    dynamic_buyer._clear_pending_tracking_only()
                    dynamic_buyer._failed_replaces = []
                    OrderCanceler().cancel_buy_orders_for_symbol(
                        symbol, extra_uuids=pending_uuids, verify=False)
                    print_log(LogLevel.WARNING,
                              f"매도 체결 → 잔여매수 취소 {symbol} "
                              f"| {self._ladder_detail(dynamic_buyer)}")
                except Exception as e:
                    print_log(LogLevel.WARNING,
                              f"매도후 매수취소 실패: {str(e)[:100]}")

            if self._holding_sellable(symbol, total_vol):
                print_log(LogLevel.WARNING,
                          f"매도 체결 후 잔량 남음 vol={total_vol:.8f} "
                          f"(≈{holding_notional_krw(symbol, total_vol):,.0f}원) — 전량 재매도")
                self.sell_orders_tracking = []
                self.unfilled_sell_count = 0
                self.filled_sell_count = 0
                base = fresh_avg if fresh_avg and fresh_avg > 0 else None
                if self.place_sell_orders(symbol, profit_percentages, dynamic_buyer,
                                          sell_base_price=base, force_replace=True,
                                          volume_hint=total_vol):
                    trading_manager.mark_sell_orders_placed()
                    if dynamic_buyer:
                        self._sell_placed_at_buy_count = dynamic_buyer.executed_count
                return False
            # 보유0/먼지 — 사이클 종료 (다음 사이클)
            if total_vol >= MIN_HOLDING_VOLUME and is_dust_holding(symbol, total_vol):
                print_log(LogLevel.WARNING,
                          f"매도 후 먼지진 사이클종료 — "
                          f"{self._dust_detail(symbol, total_vol)} | "
                          f"{self._ladder_detail(dynamic_buyer)}")
                self.dust_holdings = True
            else:
                self.dust_holdings = False
                print_log(LogLevel.INFO,
                          f"매도 체결·보유0 — 매수취소·사이클종료 "
                          f"{symbol} vol={total_vol:.8f} | "
                          f"{self._ladder_detail(dynamic_buyer)}")
            return self._complete_cycle_on_sell_done(
                trading_manager, dynamic_buyer, force=True,
                profit_percentages=profit_percentages)

        # 먼지 (주문 불가) — 매도 이력 있으면 종료(매수취소). 사다리 때문에 붙잡지 않음.
        if total_vol >= MIN_HOLDING_VOLUME and is_dust_holding(symbol, total_vol):
            if self.has_open_sell_orders() or sell_uuids:
                self._cancel_open_asks_for_replace(symbol)
            self.dust_holdings = True
            if (trading_manager.sell_orders_placed
                    or trading_manager.sell_orders_executed
                    or trading_manager.buy_orders_executed):
                print_log(LogLevel.WARNING,
                          f"먼지진 사이클종료 — "
                          f"{self._dust_detail(symbol, total_vol)} | "
                          f"{self._ladder_detail(dynamic_buyer)}")
                return self._complete_cycle_on_sell_done(
                    trading_manager, dynamic_buyer, force=True,
                    profit_percentages=profit_percentages)
            self._log_dust_once(
                f'dust_no_history:{symbol}',
                f"먼지진(매도이력없음) 유지 — "
                f"{self._dust_detail(symbol, total_vol)}")
            return False
        self.dust_holdings = False

        if total_vol < MIN_HOLDING_VOLUME:
            if self.has_open_sell_orders():
                return False
            # ★ 매수체결 있는데 매도 미걸림 + 보유캐시0 → 사이클종료 금지, REST 재조회
            buy_done = bool(
                (dynamic_buyer and int(getattr(dynamic_buyer, 'executed_count', 0) or 0) > 0)
                or trading_manager.buy_orders_executed)
            no_sell = (not trading_manager.sell_orders_placed
                       and not self.has_open_sell_orders())
            if buy_done and no_sell:
                bal2, loc2, avg2 = self._get_fresh_symbol_info(symbol)
                if bal2 >= 0:
                    total_vol = max(float(bal2), 0.0) + max(float(loc2), 0.0)
                    available_volume = max(float(bal2), 0.0)
                    if avg2 and avg2 > 0:
                        fresh_avg = float(avg2)
                if self._holding_sellable(symbol, total_vol):
                    base = fresh_avg if fresh_avg and fresh_avg > 0 else None
                    ok = self._force_full_sell(
                        symbol, profit_percentages, dynamic_buyer,
                        total_vol, base=base)
                    if ok:
                        trading_manager.mark_sell_orders_placed()
                        if dynamic_buyer:
                            self._sell_placed_at_buy_count = (
                                dynamic_buyer.executed_count)
                        print_log(LogLevel.SUCCESS,
                                  f"보유캐시0→REST복구 전량매도 {symbol} "
                                  f"vol={total_vol:.8f}")
                    else:
                        self._log_sell_fail_throttled(
                            symbol, total_vol, base, 'rest-recover')
                    return False
                # REST도 0이면 힌트(체결수량)로라도 POST 시도
                hint = 0.0
                try:
                    for e in (getattr(dynamic_buyer, 'executed_orders', None) or []):
                        hint += float(e.get('volume') or 0)
                except Exception:
                    hint = 0.0
                if hint >= MIN_HOLDING_VOLUME:
                    base = fresh_avg if fresh_avg and fresh_avg > 0 else None
                    ok = self._force_full_sell(
                        symbol, profit_percentages, dynamic_buyer,
                        hint, base=base)
                    if ok:
                        trading_manager.mark_sell_orders_placed()
                        if dynamic_buyer:
                            self._sell_placed_at_buy_count = (
                                dynamic_buyer.executed_count)
                    else:
                        self._log_sell_fail_throttled(
                            symbol, hint, base, 'hint-recover')
                    return False
                return False  # 아직 매도 안 걸림 — 종료하지 않음
            # ★ 보유0 = 매도완료 → 잔여 매수 취소 후 다음 사이클 (사다리 유지 금지)
            if (trading_manager.sell_orders_placed
                    or trading_manager.sell_orders_executed
                    or trading_manager.buy_orders_executed
                    or self._buy_ladder_active(dynamic_buyer)):
                print_log(LogLevel.INFO,
                          f"보유0 사이클종료(매수취소) — {symbol} | "
                          f"{self._ladder_detail(dynamic_buyer)}")
                return self._complete_cycle_on_sell_done(
                    trading_manager, dynamic_buyer, force=True,
                    profit_percentages=profit_percentages)
            return False

        # 2) 열린 매도 유지 — 추가잔량만 드물게 교체
        if self.has_pending_sell_orders(symbol):
            open_n = self._open_sell_count()
            if open_n == 0:
                # tracking 유실 — 거래소 ask 흡수만 (place 재호출 = 깜빡임)
                if self._adopt_open_ask_tracking(symbol):
                    trading_manager.mark_sell_orders_placed()
                    return False
                # ask도 없으면 즉시 전량 재POST
                base = fresh_avg if fresh_avg and fresh_avg > 0 else None
                if self.place_sell_orders(symbol, profit_percentages, dynamic_buyer,
                                          sell_base_price=base, force_replace=True,
                                          volume_hint=total_vol):
                    trading_manager.mark_sell_orders_placed()
                    if dynamic_buyer:
                        self._sell_placed_at_buy_count = dynamic_buyer.executed_count
                    return False
                self.unfilled_sell_count = 0
                sell_uuids.clear()
                trading_manager.sell_orders_placed = False
            elif open_n > 1:
                base = fresh_avg if fresh_avg and fresh_avg > 0 else None
                if self.place_sell_orders(symbol, profit_percentages, dynamic_buyer,
                                          sell_base_price=base, force_replace=True,
                                          volume_hint=total_vol):
                    trading_manager.mark_sell_orders_placed()
                    if dynamic_buyer:
                        self._sell_placed_at_buy_count = dynamic_buyer.executed_count
                return False
            else:
                # 열린 매도 1건 — 매수체결↑ OR 가용잔량≥5000원이면 무조건 합산갱신
                buy_up = bool(
                    dynamic_buyer
                    and dynamic_buyer.executed_count > self._sell_placed_at_buy_count)
                need_free = self._should_replace_for_free(
                    symbol, available_volume,
                    ref_price=fresh_avg or None,
                    buy_count_increased=buy_up)
                if buy_up or need_free:
                    t0 = float(getattr(self, '_last_replace_t', 0) or 0)
                    q0 = float(getattr(self, '_last_committed_qty', 0) or 0)
                    base = fresh_avg if fresh_avg and fresh_avg > 0 else None
                    ok = self.place_sell_orders(
                        symbol, profit_percentages, dynamic_buyer,
                        sell_base_price=base, force_replace=False,
                        volume_hint=total_vol,
                        avg_refresh=bool(buy_up))
                    if ok:
                        trading_manager.mark_sell_orders_placed()
                    # keep/락미스는 True여도 워터마크 올리지 않음 → 재시도 가능
                    actually = (
                        float(getattr(self, '_last_replace_t', 0) or 0) > t0 + 1e-9
                        or float(getattr(self, '_last_committed_qty', 0) or 0)
                        > q0 + 1e-12)
                    if actually:
                        self._free_above_min_streak = 0
                        if dynamic_buyer:
                            self._sell_placed_at_buy_count = (
                                dynamic_buyer.executed_count)
                        print_log(
                            LogLevel.SUCCESS,
                            f"매도물량 합산갱신 {symbol} "
                            f"free={available_volume:.8f} "
                            f"total={total_vol:.8f} "
                            f"({'buy_up' if buy_up else 'free_vol'})")
                    elif need_free:
                        now_l = time.time()
                        last_l = float(
                            getattr(self, '_free_merge_pending_log_ts', 0) or 0)
                        if now_l - last_l >= 5.0:
                            self._free_merge_pending_log_ts = now_l
                            print_log(
                                LogLevel.WARNING,
                                f"매도물량 합산대기 {symbol}: "
                                f"free≈{self._free_notional(symbol, available_volume, fresh_avg):,.0f}원 "
                                f"streak={getattr(self, '_free_above_min_streak', 0)} "
                                f"— place 미반영, 재시도")
                return False

        # 3) 매도 없음 + 보유 → 거래소 ask 확인 후 없을 때만 신규
        if self._holding_sellable(symbol, total_vol):
            if self._adopt_open_ask_tracking(symbol):
                trading_manager.mark_sell_orders_placed()
                return False
            base = fresh_avg if fresh_avg and fresh_avg > 0 else None
            ok = self._force_full_sell(
                symbol, profit_percentages, dynamic_buyer,
                total_vol, base=base)
            if ok:
                trading_manager.mark_sell_orders_placed()
                if dynamic_buyer:
                    self._sell_placed_at_buy_count = dynamic_buyer.executed_count
            else:
                self._log_sell_fail_throttled(
                    symbol, total_vol, base, 'section3')
        return False


class MinuteMaCache:
    """1분봉 MA60 — 핫패스 REST 금지.
    CandleCache로 시드/백그라운드 갱신만. get_ma60은 메모리 읽기.
    Upbit 1분 REST는 최신봉=진행중 봉 포함 (틱 MA와 동일 철학)."""

    BG_INTERVAL_S = 25.0

    def __init__(self):
        try:
            from .config import MINUTE_MA_PERIOD
            period = int(MINUTE_MA_PERIOD)
        except Exception:
            period = 60
        self.PERIOD = max(1, period)
        self._closes = {}  # {symbol: deque(maxlen=PERIOD)} 과거→최신
        self._symbols = set()
        self._lock = threading.RLock()
        self._bg_stop = threading.Event()
        self._bg_thread = None

    def ensure(self, symbols):
        """심볼 등록 + 미시드분만 seed + bg 시작."""
        add = []
        for s in (symbols if isinstance(symbols, (list, tuple, set)) else [symbols]):
            if not s:
                continue
            add.append(str(s).upper())
        if not add:
            return
        with self._lock:
            for su in add:
                self._symbols.add(su)
            need = [su for su in add if su not in self._closes
                    or len(self._closes.get(su) or []) < self.PERIOD]
        for su in need:
            self.seed(su)
        self._ensure_bg()

    def seed(self, symbol):
        """CandleCache에서 1분 종가 PERIOD개 적재 (최신=진행중 봉)."""
        if not symbol:
            return False
        su = str(symbol).upper()
        try:
            candles = CandleCache.get_candles(su, self.PERIOD)
        except Exception:
            candles = []
        if not candles:
            return False
        # API: 최신→과거. 과거→최신 deque로 저장
        closes = []
        for c in reversed(candles[:self.PERIOD]):
            try:
                px = float(c.get('trade_price') or 0)
            except (TypeError, ValueError):
                px = 0.0
            if px > 0:
                closes.append(px)
        if len(closes) < self.PERIOD:
            return False
        with self._lock:
            self._closes[su] = deque(closes[-self.PERIOD:], maxlen=self.PERIOD)
            self._symbols.add(su)
        return True

    def get_ma60(self, symbol):
        """메모리 전용 SMA. PERIOD 미만이면 None."""
        if not symbol:
            return None
        su = str(symbol).upper()
        with self._lock:
            closes = self._closes.get(su)
            if not closes or len(closes) < self.PERIOD:
                return None
            vals = list(closes)[-self.PERIOD:]
        return sum(vals) / len(vals)

    def get_buffer_size(self, symbol):
        if not symbol:
            return 0
        su = str(symbol).upper()
        with self._lock:
            return len(self._closes.get(su) or [])

    def _ensure_bg(self):
        with self._lock:
            if self._bg_thread and self._bg_thread.is_alive():
                return
            self._bg_stop.clear()
            self._bg_thread = threading.Thread(
                target=self._bg_loop, name='minute-ma-bg', daemon=True)
            self._bg_thread.start()

    def _bg_loop(self):
        while not self._bg_stop.wait(self.BG_INTERVAL_S):
            with self._lock:
                syms = list(self._symbols)
            for su in syms:
                try:
                    # TTL 만료 유도 후 재조회
                    CandleCache.invalidate(su)
                    self.seed(su)
                except Exception:
                    pass


# 모듈 싱글톤 — CandleCache 정의 후 생성 (아래 블록에서 재바인딩)
minute_ma_cache = None


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
                response = http_get(CANDLE_URL, params=querystring,
                                    timeout=HTTP_TIMEOUT_SLOW, slow=True)
                return response_json(response)
            candles = safe_api_call(api_call)
            if candles:
                cls._cache[symbol] = {'candles': candles, 'time': now}
                return candles[:count] if len(candles) >= count else candles
            # 신규상장 직후 빈 응답 — 예외/경고 없이 [] (캐시 오염 방지)
            return entry['candles'][:count] if entry else []
        except Exception as e:
            # 404/미상장 등은 1회만 짧게 — 반복 스팸 금지
            err = str(e)[:80]
            if '404' not in err and 'not found' not in err.lower():
                print_log(LogLevel.WARNING,
                          f"CandleCache 조회 실패 ({symbol}): {err}")
        return entry['candles'][:count] if entry else []

    @classmethod
    def invalidate(cls, symbol=None):
        """캐시 무효화 (특정 심볼 또는 전체)."""
        if symbol:
            cls._cache.pop(symbol, None)
        else:
            cls._cache.clear()


# MinuteMaCache는 CandleCache에 의존 — 클래스 정의 후 인스턴스화
minute_ma_cache = MinuteMaCache()


class CandleInfoFetcher:
    """1분 캔들 OHLC 로더. 신규상장/데이터부족 시 raise 없이 ok=False."""

    @staticmethod
    def _f(c, *keys):
        if not isinstance(c, dict):
            return 0.0
        for k in keys:
            v = c.get(k)
            if v is None:
                continue
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
        return 0.0

    def __init__(self, symbol):
        count = 200
        self.symbol = str(symbol or '').upper()
        self.ok = False
        self.opening_prices = []
        self.trade_prices = []
        self.high_prices = []
        self.low_prices = []
        self.acc_trade_prices = []
        self.acc_trade_volumes = []
        self.current_price = 0.0
        self.response_dict = CandleCache.get_candles(self.symbol, count) or []
        if not self.response_dict:
            return

        # API 응답은 최신→과거 → 역순(과거→최신). 필드 누락 행은 스킵.
        rows = []
        for c in self.response_dict:
            o = self._f(c, 'opening_price', 'openingPrice')
            t = self._f(c, 'trade_price', 'tradePrice')
            h = self._f(c, 'high_price', 'highPrice')
            lo = self._f(c, 'low_price', 'lowPrice')
            if t <= 0 and h <= 0 and lo <= 0:
                continue
            if t <= 0:
                t = h if h > 0 else lo
            if h <= 0:
                h = max(t, lo)
            if lo <= 0:
                lo = min(t, h) if h > 0 else t
            if o <= 0:
                o = t
            rows.append((
                o, t, h, lo,
                self._f(c, 'candle_acc_trade_price', 'candleAccTradePrice'),
                self._f(c, 'candle_acc_trade_volume', 'candleAccTradeVolume'),
            ))
        if not rows:
            return
        n = len(rows)
        self.opening_prices = [0.0] * n
        self.trade_prices = [0.0] * n
        self.high_prices = [0.0] * n
        self.low_prices = [0.0] * n
        self.acc_trade_prices = [0.0] * n
        self.acc_trade_volumes = [0.0] * n
        for i, (o, t, h, lo, ap, av) in enumerate(rows):
            j = n - 1 - i
            self.opening_prices[j] = o
            self.trade_prices[j] = t
            self.high_prices[j] = h
            self.low_prices[j] = lo
            self.acc_trade_prices[j] = ap
            self.acc_trade_volumes[j] = av
        self.current_price = self.trade_prices[-1] if self.trade_prices else 0.0
        self.ok = self.current_price > 0 and len(self.trade_prices) > 0


class VolatilityProtector:
    """동적 매수 보호 클래스 - 고변동성 코인 매수 방지"""
    
    @staticmethod
    def check_volatility_protection(symbol, lookback_period=60, threshold_percentage=50.0,
                                      quiet=False):
        """
        변동성 보호 체크
        최근 lookback_period 캔들 동안 최소값과 최대값 차이가 threshold_percentage 이상이면 매수 금지
        
        Args:
            symbol: 심볼명
            lookback_period: 확인할 캔들 수 (기본 60개)
            threshold_percentage: 변동성 임계값 (기본 40%)
            quiet: True면 통과/상세 로그 생략 (감시 루프용). 차단 시에는 항상 WARNING.
            
        Returns:
            bool: True=보호 적용(매수금지), False=매수 가능
        """
        try:
            # CandleCache에서 캔들 데이터 가져오기 (MarketAnalyzer와 동일 캐시 재사용)
            count = max(lookback_period, 60)  # 최소 60개
            candles = CandleCache.get_candles(symbol, count)

            # 신규상장/캔들부족 — 변동성 판정 불가 → 매수 차단 (조용히)
            if not candles or len(candles) < lookback_period:
                if not quiet:
                    print_log(LogLevel.WARNING,
                              f"Not enough candle data for {symbol} "
                              f"({len(candles or [])}/{lookback_period}) — BUY BLOCKED")
                return True
            
            # 최근 lookback_period개의 고가/저가 추출
            high_prices = []
            low_prices = []
            
            for i in range(min(lookback_period, len(candles))):
                candle = candles[i] if isinstance(candles[i], dict) else None
                if not candle:
                    continue
                try:
                    h = float(candle.get('high_price')
                              or candle.get('highPrice') or 0)
                    lo = float(candle.get('low_price')
                               or candle.get('lowPrice') or 0)
                except (TypeError, ValueError):
                    continue
                if h > 0 and lo > 0:
                    high_prices.append(h)
                    low_prices.append(lo)
            
            if not high_prices or not low_prices:
                return True  # 필드 누락 — 차단
            
            # 최소값과 최대값 계산
            min_price = min(low_prices)
            max_price = max(high_prices)
            
            # 변동성 계산 (백분율)
            if min_price > 0:
                volatility_percentage = ((max_price - min_price) / min_price) * 100
            else:
                return True
            
            if not quiet:
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
                if not quiet:
                    print_log(LogLevel.INFO, 
                             f"Volatility within safe range for {symbol}: "
                             f"{volatility_percentage:.2f}% < {threshold_percentage}%")
                return False
                
        except Exception as e:
            if not quiet:
                print_log(LogLevel.WARNING,
                          f"volatility check 오류 {symbol}: {str(e)[:80]} — BUY BLOCKED")
            # 에러 발생 시 보호 적용 (안전 측면)
            return True

class MarketAnalyzer:
    """캔들 기반 단순 지표 — SMA/STDDEV만 (talib·RSI·MFI 폐기)."""

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
        self.symbol = str(symbol or '').upper()
        self.candle = CandleInfoFetcher(self.symbol)
        prices = list(self.candle.trade_prices or [])
        self.ok = bool(getattr(self.candle, 'ok', False) and prices)
        self.ma60 = self._sma(prices, 60) if self.ok else 0.0
        self.std60 = self._stddev(prices, 60) if self.ok else 0.0
        self.volatility_ratio = self.std60 / self.ma60 if self.ma60 > 0 else 0

    def is_below_ma60(self):
        if not self.ok or self.ma60 <= 0:
            return False
        return self.candle.current_price < self.ma60

class SymbolSelector:
    @staticmethod
    def get_all_krw_markets():
        try:
            def api_call():
                # details=true: market_event(주의/경고 종목) 필드 수신
                url = SERVER_URL + "/v1/market/all?details=true"
                headers = {"Accept": "application/json"}
                response = http_get(url, headers=headers, timeout=HTTP_TIMEOUT_SLOW,
                                    slow=True)
                return response_json(response)

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
                url = SERVER_URL + "/v1/candles/minutes/60"
                params = {
                    'market': f"KRW-{symbol}",
                    'count': hours
                }
                headers = {"Accept": "application/json"}
                response = http_get(url, params=params, headers=headers,
                                    timeout=HTTP_TIMEOUT_SLOW, slow=True)
                return response_json(response)
            
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
                
            # 변동성 보호 체크 - 50% 이상 변동성 코인 제외
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

        print_log(LogLevel.INFO, f"Total symbols: {len(symbols)}")

        valid_symbols = []

        for symbol in tqdm(symbols, desc="Analyzing markets"):
            try:
                result = SymbolSelector.analyze_market_volatility(symbol)
                if result:
                    valid_symbols.append(result)
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

def main():
    ensure_log_dir()
    # pastebin → command.txt (idempotent; also started from __main__)
    try:
        from .command_sync import start_background as _start_cmd_sync
        _start_cmd_sync(daemon=True)
    except Exception as _e:
        print_log(LogLevel.WARNING, f"command_sync start skipped: {_e}")
    try:
        # 주의: 이 블록은 모듈 스코프이므로 global 선언 불필요/불가.
        # 전역 변수(EXCHANGE, SERVER_URL 등)에 직접 재할당하면 모듈 전체에 반영됨.
        # 거래소만 CLI: -e upbit|bithumb (기본 upbit). 나머지 옵션 고정.
        exchange_name = 'upbit'
        argv = sys.argv[1:]
        i = 0
        while i < len(argv):
            a = argv[i]
            if a in ('-e', '--exchange') and i + 1 < len(argv):
                exchange_name = argv[i + 1].strip().lower()
                i += 2
                continue
            if a.startswith('--exchange='):
                exchange_name = a.split('=', 1)[1].strip().lower()
                i += 1
                continue
            i += 1
        if exchange_name not in EXCHANGE_CONFIGS:
            print_log(LogLevel.ERROR,
                      f"Unknown exchange '{exchange_name}' — use upbit or bithumb")
            raise SystemExit(2)

        VERBOSE = False
        AUTO_SELECT = True  # CV TopN + HybridMA+vol (command.txt 폐지)

        drop_percentage = 13 / 30
        distribution_type = DynamicBuyOrder.DistributionType.EXPLOSIVE
        distribution_weight = 1 / 30
        profit_percentage = 0.149

        # 거래소 선택 — 전역 EXCHANGE/URL 일괄 재할당
        EXCHANGE = EXCHANGE_CONFIGS[exchange_name]
        SERVER_URL = EXCHANGE['server_url']
        CANDLE_URL = SERVER_URL + "/v1/candles/minutes/" + str(UNIT)
        TRADES_URL = SERVER_URL + "/v1/trades/ticks"
        TICKER_URL = SERVER_URL + "/v1/ticker"
        ORDERBOOK_URL = SERVER_URL + "/v1/orderbook"
        TICK_CANDLE_URL = EXCHANGE.get('tick_candle_url')
        TICK_CANDLE_CODE = EXCHANGE.get('tick_candle_code')
        _refresh_hot_urls()
        # WS 클래스의 클래스 변수도 갱신 — 인스턴스 생성 전이므로 안전
        UpbitWebSocket.WS_URL = EXCHANGE['ws_public_url']
        UpbitPrivateWS.WS_URL = EXCHANGE['ws_private_url']
        print_log(LogLevel.SUCCESS, f"거래소: {EXCHANGE['name']} ({SERVER_URL})")
        try:
            from .config import PARALLEL_WORKERS as _PW
            from .worker_pool import describe_alloc_table
            print_log(LogLevel.INFO,
                      f"병렬워커: {describe_alloc_table()}")
            order_rate_limiter.set_active_workers(_PW)
        except Exception as _e:
            print_log(LogLevel.WARNING, f"worker_pool boot: {_e}")

        # 거래소별 API 키 파일 로드
        #   upbit   → key.txt (project root)        (기존 호환)
        #   bithumb → key_bithumb.txt (project root)
        if EXCHANGE['name'] == 'bithumb':
            key_file = str(KEY_BITHUMB_TXT)
        else:
            key_file = str(KEY_TXT)
        try:
            with open(key_file, 'r', encoding='utf-8') as f:
                ak = f.readline().strip()
                sk = f.readline().strip()
            set_api_keys(ak, sk)
        except FileNotFoundError:
            print_log(LogLevel.ERROR,
                      f"API 키 파일 없음: {key_file} (1행=Access, 2행=Secret)")
            raise

        # DNS + TLS + 인증 REST 프리웜 — 첫 주문 핸드셰이크/JWT 스파이크 제거
        _warm_done = threading.Event()

        def _warm_boot():
            try:
                warm_http_connections(auth=True)
            except Exception:
                pass
            finally:
                _warm_done.set()

        threading.Thread(
            target=_warm_boot, name='http-warm', daemon=True).start()
        _warm_done.wait(8.0)

        START_TIME = datetime.now()

        # Private WebSocket 시작 — 잔고/체결 실시간 수신 (REST 폴링 대체)
        if WEBSOCKET_AVAILABLE:
            try:
                private_ws.start(ACCESS_KEY, SECRET_KEY)
                # WS 연결 대기 — sleep 없이 busy-poll (최대 ~2초 wall)
                deadline = time.time() + 2.0
                while not private_ws.is_connected and time.time() < deadline:
                    pass
            except Exception as e:
                print_log(LogLevel.WARNING,
                         f"PrivateWS 시작 실패 — REST 폴백 모드: {str(e)[:100]}")

        OrderCanceler().cancel_all_orders(1)

        InitialBalance = S = AccountChecker().get_krw_balance()
        print_log(LogLevel.INFO, f"Available KRW: {int(S):,}")
        log_balance(S)
        S = int(S)

        volatility_scanner = None
        if WEBSOCKET_AVAILABLE:
            print_log(LogLevel.INFO, "Starting VolatilityScanner (AUTO_SELECT)...")
            scan_symbols = SymbolSelector.get_all_krw_markets()
            if scan_symbols:
                volatility_scanner = VolatilityScanner()
                volatility_scanner.start(scan_symbols)
                globals()['volatility_scanner'] = volatility_scanner
            else:
                print_log(LogLevel.WARNING, "No symbols for VolatilityScanner — REST 폴백")
        else:
            print_log(LogLevel.WARNING, "WS 미가용 — VolatilityScanner 생략")

        cycle_count = 0
        cycle_start_krw = 0.0

        # ── 병렬 워커 모드 (PARALLEL_WORKERS >= 2) ──
        try:
            from .config import PARALLEL_WORKERS as _PW, AUTO_SELECT_TOP_N as _TOP_N
        except Exception:
            _PW, _TOP_N = 3, 3
        if int(_PW) >= 2:
            from .cycle_runner import run_parallel_supervisor_loop
            run_parallel_supervisor_loop(
                drop_percentage=drop_percentage,
                drop_count=7,
                distribution_type=distribution_type,
                distribution_weight=distribution_weight,
                profit_percentage=profit_percentage,
                max_workers=min(int(_PW), int(_TOP_N)),
            )
            return

        while True:
            cycle_count += 1

            # 1. command.txt 변경 체크 (새로운 심볼만 저장, 현재 거래는 중단하지 않음)
            new_symbol_detected = trading_manager.check_command_file()
            if new_symbol_detected:
                print_log(LogLevel.INFO, f"New symbol detected in command file: {new_symbol_detected}, will switch after current trading completes")
                # 현재 거래는 계속 진행, 다음 사이클에서 새로운 심볼로 전환

            cached_symbol = trading_manager.get_cached_symbol()
            cycle_locked = trading_manager.is_cycle_locked()
            # --auto-select 시 매 사이클마다 최고 변동성 심볼을 새로 찾는다 (캐시 무시)
            # 단, 사이클 잠금(미완료 매매/보유) 중에는 절대 바꾸지 않음.
            if (AUTO_SELECT and volatility_scanner and volatility_scanner.is_running
                    and not cycle_locked):
                cached_symbol = None
            # 다중 심볼 폴백: 매수 전 MA60 재평가만 허용.
            # 사이클 잠금 중에는 캐시를 비우지 않음 — 심볼 갈아타기 방지.
            if (not AUTO_SELECT
                    and len(trading_manager.current_command_symbols) >= 2
                    and not cycle_locked):
                cached_symbol = None

            # 사이클 잠금이면 캐시/폴백 무시하고 현재 거래 심볼 고정
            if cycle_locked and trading_manager.current_symbol:
                symbol = trading_manager.current_symbol
                trading_manager.touch_symbol_cache()
                print_log(LogLevel.INFO,
                          f"Cycle lock — staying on {symbol} "
                          f"(flags/holdings active, no symbol switch)")
                analyzer = MarketAnalyzer(symbol)
            elif cached_symbol:
                # AUTO_SELECT 꺼짐이면 캐시도 command.txt 목록 안에 있을 때만 허용
                # (잘못 선별된 ETH 등이 캐시로 계속 매수되는 것 차단)
                cmds = trading_manager.current_command_symbols
                if (not AUTO_SELECT and cmds
                        and cached_symbol.upper() not in {c.upper() for c in cmds}):
                    print_log(LogLevel.WARNING,
                              f"캐시 심볼 {cached_symbol} ∉ command {cmds} — 캐시 폐기")
                    trading_manager.reset()
                    current_trading_symbol = None
                    symbol_cache_time = None
                    continue
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
                if not AUTO_SELECT:
                    # 다중 심볼 폴백 — command.txt의 심볼 리스트를 기재 순서대로 순회하여
                    # 첫 번째로 MA60 게이트를 통과하는 심볼 선택.
                    command_symbols = trading_manager.current_command_symbols
                    if command_symbols:
                        symbol_from_command, tried = RealMarketData.select_first_tradable_symbol(
                            command_symbols)
                        if len(command_symbols) > 1:
                            tried_summary = '; '.join(
                                f"{s}=({'OK' if a else 'X'}) {r}" for s, a, r in tried)
                            print_log(LogLevel.INFO,
                                      f"다중 심볼 폴백 결과: 선택={symbol_from_command} "
                                      f"[{tried_summary}]")

                if symbol_from_command:
                    symbol = symbol_from_command
                    # 변동성 보호 체크
                    if VolatilityProtector.check_volatility_protection(symbol):
                        print_log(LogLevel.WARNING, f"Command symbol {symbol} blocked by volatility protection")
                        log_state(LogState.ERROR, "VOLATILITY_PROTECTION")
                        continue
                elif not AUTO_SELECT:
                    # command.txt 전용 모드 — 목록 밖(ETH 등) 자동매수 절대 금지
                    cmds = trading_manager.current_command_symbols
                    if cmds:
                        print_log(LogLevel.WARNING,
                                  f"command 심볼 MA60 미통과 {cmds} — "
                                  f"전체마켓 선별 금지, 재시도")
                    else:
                        print_log(LogLevel.WARNING,
                                  "command.txt에 SYMBOL 없음 — 매수 대기 "
                                  "(AUTO_SELECT 꺼짐, 임의코인 금지)")
                    cycle_count -= 1
                    time.sleep(1.0)
                    continue
                else:
                    # AUTO_SELECT=True 일 때만 스캐너/전체마켓 선별
                    if volatility_scanner and volatility_scanner.is_running:
                        excluded = set(traded_symbols.keys())
                        selected_symbol = volatility_scanner.get_top_volatility_symbol(excluded)
                        if selected_symbol:
                            print_log(LogLevel.SUCCESS,
                                     f"VolatilityScanner 선별: {selected_symbol}")
                            symbol = selected_symbol
                        else:
                            print_log(LogLevel.WARNING,
                                     "스캐너 후보 없음 — 즉시 재시도")
                            continue
                    else:
                        selected_symbol = SymbolSelector.select_best_symbol()
                        if selected_symbol is None:
                            print_log(LogLevel.WARNING,
                                     "No valid symbol found — immediate retry")
                            continue
                        symbol = selected_symbol

                analyzer = MarketAnalyzer(symbol)
                trading_manager.set_symbol(symbol)

            # 웹소켓 ticker 구독 (심볼 확정 시)
            RealMarketData.subscribe_websocket(symbol)
            # 60틱 MA60 매수 게이트용 체결가 스트림 구독.
            # 다중 심볼(command.txt에 2개 이상)일 때는 전체 심볼을 동시 구독하여
            # 폴백 시 각 심볼의 MA60을 즉시 사용할 수 있도록 함.
            command_syms = trading_manager.current_command_symbols
            if (not AUTO_SELECT and len(command_syms) >= 2):
                RealMarketData.subscribe_trade_stream_symbols(command_syms)
            else:
                RealMarketData.subscribe_trade_stream(symbol)

            # 매수 프로세스 / 미완료 사이클 재개(매수+매도 병렬)
            holding_vol = trading_manager.get_holding_volume(symbol)
            dust_hold = (holding_vol >= MIN_HOLDING_VOLUME
                         and is_dust_holding(symbol, holding_vol))
            # ★ REST 전량미완이면 먼지로 취급 금지·보유로 강제 (늦은 체결 레이스)
            rest_vol, rest_notional, rest_sellable = rest_holding_snapshot(symbol)
            if rest_sellable:
                dust_hold = False
                holding_vol = max(holding_vol, rest_vol)
                cycle_locked = True
            resume_parallel = False
            resume_skip_l1 = False
            if dust_hold and not rest_sellable:
                # 먼지진 — REST도 먼지/0일 때만 매수로 흡수
                now_d = time.time()
                if now_d - getattr(trading_manager, '_last_dust_log', 0) >= 5.0:
                    trading_manager._last_dust_log = now_d
                    print_log(LogLevel.WARNING,
                              f"먼지진 {symbol} 평가액 "
                              f"{holding_notional_krw(symbol, holding_vol):,.2f}원 "
                              f"< {MIN_ORDER_AMOUNT}원 — 매도 포기, 매수 진행")
                trading_manager.buy_orders_placed = False
                trading_manager.buy_orders_executed = False
                trading_manager.sell_orders_placed = False
                trading_manager.sell_orders_executed = False
                cycle_locked = False
            elif (cycle_locked or holding_vol >= MIN_HOLDING_VOLUME or rest_sellable):
                # 예전이 sell-only resume라 매수가 영구 차단됨 → 매수 사다리 재개 + 매도 병행
                resume_parallel = True
                trading_manager.sell_orders_executed = False
                if holding_vol >= MIN_HOLDING_VOLUME or rest_sellable:
                    trading_manager.buy_orders_executed = True
                    resume_skip_l1 = True
                # buy_orders_placed를 매번 False로 끄면 L1 삼중매수가 outer마다 재발사됨.
                # 미배치(재시작)일 때만 진입 허용. 이미 배치면 아래 reattach 경로.
                if not trading_manager.buy_orders_placed:
                    cycle_locked = False
                print_log(LogLevel.WARNING,
                          f"Resuming {symbol} (holdings={holding_vol:.6f}"
                          f"{f', REST≈{rest_notional:,.0f}원' if rest_sellable else ''}"
                          f") — buy+sell parallel (skip_l1={resume_skip_l1})")

            # while 조기종료 후 보유만 남은 경우 — L1 재주문 없이 사다리/매도만 재부착
            if (resume_parallel
                    and trading_manager.buy_orders_placed
                    and not trading_manager.is_trading_complete()
                    and (holding_vol >= MIN_HOLDING_VOLUME or rest_sellable)
                    and not dust_hold):
                now_r = time.time()
                last_r = getattr(trading_manager, '_last_resume_attach', 0)
                if now_r - last_r < 3.0:
                    # 재부착 폭주 방지 (시간차로 outer가 도는 것)
                    time.sleep(0.5)
                    continue
                trading_manager._last_resume_attach = now_r
                trading_manager.buy_orders_placed = False
                resume_skip_l1 = True
                cycle_locked = False

            # ★ 다음 사이클/신규매수 — REST 전량매도 완료 필수
            # rest_sellable이면 L1 신규매수 금지. resume(skip L1)+매도만 허용.
            if (trading_manager.should_place_buy_orders()
                    and not cycle_locked
                    and rest_sellable
                    and not resume_parallel):
                print_log(LogLevel.WARNING,
                          f"전량매도 미완({symbol}) vol={rest_vol:.8f} "
                          f"≈{rest_notional:,.0f}원 — 신규매수 차단, 매도 재개")
                trading_manager.sell_orders_executed = False
                trading_manager.buy_orders_executed = True
                trading_manager.buy_orders_placed = False
                resume_parallel = True
                resume_skip_l1 = True

            if (trading_manager.should_place_buy_orders()
                    and (holding_vol < MIN_HOLDING_VOLUME or dust_hold or resume_parallel)
                    and not cycle_locked
                    and (not rest_sellable or resume_skip_l1)):
                # command 오버라이드가 있으면 즉시 적용 (새로운 거래 시작 시에만)
                # 단, --auto-select 시에는 command.txt 심볼을 무시 (스캐너 결과 유지)
                if (not AUTO_SELECT
                        and trading_manager.pending_symbol_change
                        and not trading_manager.is_trading_in_progress()
                        and not resume_parallel
                        and not rest_sellable):
                    # 다중 심볼 폴백 모드에서는 pending으로 첫 심볼을 강제하지 않음
                    if len(trading_manager.current_command_symbols) >= 2:
                        trading_manager.pending_symbol_change = None
                    else:
                        # 현재 심볼 전량매도 확정 후에만 전환
                        if rest_holdings_cleared(symbol):
                            symbol = trading_manager.apply_pending_symbol_change()
                            print_log(LogLevel.INFO, f"Applied command override symbol: {symbol}")
                            analyzer = MarketAnalyzer(symbol)  # symbol 변경 시에만 재생성
                            RealMarketData.subscribe_websocket(symbol)
                            RealMarketData.subscribe_trade_stream(symbol)
                        else:
                            print_log(LogLevel.WARNING,
                                      "심볼 전환 보류 — 현재 코인 전량매도 미완")
                            time.sleep(0.3)
                            continue

                # rest 잔량 있을 때 resume이면 L1 절대 금지
                if rest_sellable:
                    resume_skip_l1 = True

                # 60틱 MA60 매수 진입 게이트 — 현재 체결가가 MA60 아래일 때만 매수.
                # 먼지진/재개 매수는 게이트 무시 (잔량·미완료 사이클 방치 방지).
                if not dust_hold and not resume_parallel:
                    gate_ok, gate_info = RealMarketData.check_tick_ma_gate(symbol)
                    if not gate_ok:
                        now_g = time.time()
                        if now_g - getattr(trading_manager, '_last_gate_log', 0) >= 1.0:
                            trading_manager._last_gate_log = now_g
                            print_log(LogLevel.WARNING,
                                      f"HybridMA gate block {symbol}: "
                                      f"px={gate_info.get('last_price')} "
                                      f"hybrid={(gate_info.get('hybrid') or gate_info.get('ma60'))} "
                                      f"tick={gate_info.get('ma_tick')} "
                                      f"min={gate_info.get('ma_min')} "
                                      f"w_tick={gate_info.get('w_tick')} "
                                      f"candles={gate_info.get('candle_count')}/"
                                      f"{gate_info.get('min_candle_count')}")
                        # 게이트 대기 — 사이클 카운트/배너 스팸 방지
                        cycle_count -= 1
                        time.sleep(1.0)
                        continue

                print_log(LogLevel.INFO, f"=== Trading Cycle {cycle_count} ===")
                print_log(LogLevel.INFO, f"Target Symbol: {symbol}")
                cycle_start_krw = ws_krw_total()
                print_log(LogLevel.SUCCESS,
                          f"Cycle {cycle_count} start KRW(WS): {int(cycle_start_krw):,}")

                drop_count = 7

                # 신규 사이클 시작 비프만 (while 종료마다 울리면 연속 비프/재진입처럼 보임)
                if not resume_skip_l1:
                    beep_async(440, 500)

                print_log(LogLevel.SUCCESS,
                          f"Pre-buy sync cancel then place ({symbol})"
                          f"{' [resume skip L1]' if resume_skip_l1 else ''}")

                live_px = RealMarketData.get_current_price(symbol)
                buy_base_price = live_px if live_px and live_px > 0 else analyzer.candle.current_price
                low_px = analyzer.candle.low_prices[-1]

                try:
                    if private_ws._is_initialized and private_ws.is_connected:
                        live_krw = private_ws.get_krw_balance(1)
                        live_locked = private_ws.get_krw_balance(2)
                    else:
                        live_krw = AccountChecker().get_krw_balance()
                        live_locked = -1
                except Exception as e:
                    live_krw, live_locked = S, -1
                if live_krw and live_krw > 0:
                    S = int(math.floor(float(live_krw) + 1e-9))
                # 사이클 시작 잔고 — free+locked (매수 잠금 전 스냅샷 우선 유지)
                if cycle_start_krw <= 0:
                    cycle_start_krw = float(S) + max(float(live_locked or 0), 0.0)

                dynamic_buyer = DynamicBuyOrder(symbol, buy_base_price, low_px, S, distribution_weight, 0)
                dynamic_buyer.calculate_order_plan(drop_percentage, drop_count, distribution_type)

                placed_ok = False
                with _buy_lifecycle_lock:
                    try:
                        # 재개(skip L1) 때는 열린 매수만 유지 — 전량취소 후 L1 재깔기 방지
                        if not resume_skip_l1:
                            cancel_buy_orders_sync(verify=False)
                    except Exception as e:
                        print_log(LogLevel.WARNING,
                                  f"Pre-buy cancel: {str(e)[:100]}")
                    begin_buy_placement_window()
                    placed_ok = bool(dynamic_buyer.execute_dynamic_buy_orders(
                        skip_level1=resume_skip_l1))

                if placed_ok:
                    print_log(LogLevel.SUCCESS,
                              "Dynamic buying started successfully"
                              if not resume_skip_l1 else
                              "Resume manage started (L1 skipped)")
                    trading_manager.mark_buy_orders_placed()
                    
                    # 병렬 관리: 매수 진행 중에도 매도 관리 시작
                    print_log(LogLevel.SUCCESS, "=== STARTING PARALLEL BUY/SELL MANAGEMENT ===")
                    sell_controller = SellController()
                    # 단일 매도 — 평단 × (1+수익률). last_buy+틱 가산 없음.
                    profit_targets = [float(profit_percentage)]
                    trading_manager._last_profit_pct = float(profit_percentage)

                    def _on_buy_fill_sell(avg, vol):
                        """로컬 VWAP 또는 REST 실평단으로 전량 매도. 패딩 금지."""
                        if avg <= 0 or vol <= 0:
                            return
                        is_local = bool(getattr(
                            private_ws, '_avg_sell_fire_is_local', False))
                        try:
                            # arm/correct가 넘긴 경제 평단 그대로 사용.
                            # rebuild_from_fills는 REST 스냅샷(qty0=전량)과
                            # 이중집계되므로 여기서 호출하지 않음.
                            avg_use = float(avg)
                        except (TypeError, ValueError):
                            avg_use = float(avg)
                        if avg_use <= 0 or vol * avg_use < MIN_ORDER_AMOUNT:
                            return
                        tag = "local-vwap" if is_local else "rest-avg"
                        pct = profit_targets[0] if profit_targets else 0.0
                        tgt = UpbitTickSystem.calculate_sell_price(avg_use, pct)
                        min_px = UpbitTickSystem.min_no_loss_sell_price(avg_use)
                        if min_px > 0 and tgt + 1e-12 < min_px:
                            tgt = min_px
                        # 호가 같아도 place 호출 — 내부 shortfall≥5000만 합산
                        print_log(LogLevel.INFO,
                                  f"매도기준평단 {avg_use:,.8f} ({tag}) "
                                  f"목표호가={tgt:,.8f}")
                        ok = sell_controller.place_sell_orders(
                            symbol, profit_targets, dynamic_buyer,
                            sell_base_price=avg_use, force_replace=False,
                            volume_hint=vol,
                            avg_refresh=True)
                        if not (ok and sell_controller._open_sell_count() > 0):
                            if sell_controller._adopt_open_ask_tracking(symbol):
                                ok = True
                            elif str(getattr(sell_controller, '_last_sell_skip', '') or '') in (
                                    'busy', 'cooldown', 'placing'):
                                return
                            else:
                                ok = sell_controller._force_full_sell(
                                    symbol, profit_targets, dynamic_buyer, vol,
                                    base=avg_use, min_interval=1.0)
                        if ok and (sell_controller._open_sell_count() > 0
                                   or sell_controller._adopt_open_ask_tracking(symbol)):
                            sell_controller._sell_base_provisional = is_local
                            if not is_local:
                                sell_controller._last_server_avg_seen = float(avg_use)
                                sell_controller._sell_base_provisional = False
                            trading_manager.mark_sell_orders_placed()
                            if not trading_manager.buy_orders_executed:
                                trading_manager.mark_buy_orders_executed()
                            sell_controller._sell_placed_at_buy_count = (
                                dynamic_buyer.executed_count)
                        else:
                            skip = str(getattr(sell_controller, '_last_sell_skip', '') or '')
                            if skip in ('busy', 'cooldown', 'placing'):
                                return
                            if sell_controller.has_open_sell_orders():
                                trading_manager.mark_sell_orders_placed()
                                return
                            try:
                                bal, loc, _ = private_ws.get_symbol_info(symbol)
                                held = max(float(bal or 0), 0) + max(float(loc or 0), 0)
                            except Exception:
                                held = 0.0
                            re_vol = held if held >= MIN_HOLDING_VOLUME else float(vol)
                            now_l = time.time()
                            last_l = float(getattr(sell_controller, '_last_rearm_log_t', 0) or 0)
                            if now_l - last_l >= 5.0:
                                sell_controller._last_rearm_log_t = now_l
                                print_log(LogLevel.WARNING,
                                          f"전량매도 미확인 — 재arm "
                                          f"(avg={avg_use:,.4f} vol={re_vol:.8f})")
                            try:
                                private_ws.arm_avg_sell(vol_hint=re_vol, symbol=symbol)
                            except Exception:
                                pass

                    dynamic_buyer.on_buy_fill_sell = _on_buy_fill_sell
                    private_ws.set_avg_sell_target(symbol, _on_buy_fill_sell)
                    
                    from .parallel import run_managed_cycle
                    print_log(LogLevel.SUCCESS,
                              "=== PARALLEL MODULES: market|command|buy|sell ===")
                    _ctx = run_managed_cycle(
                        symbol=symbol,
                        dynamic_buyer=dynamic_buyer,
                        sell_controller=sell_controller,
                        trading_manager=trading_manager,
                        profit_targets=profit_targets,
                        private_ws=private_ws,
                        cycle_timeout=86400,
                    )
                    command_changed_during_trading = _ctx.command_changed.is_set()
                    trading_completed = _ctx.trading_completed.is_set()

                    log_state(LogState.BUYING, symbol)
                    print_log(LogLevel.INFO, f"Buy/manage loop ended for '{symbol}'")
                else:
                    print_log(LogLevel.ERROR,
                              f"Failed to place buy orders for '{symbol}' — will retry")

            # 거래 완료 처리
            if trading_manager.is_trading_complete():
                if AUTO_SELECT and not trading_manager.stop_loss_triggered:
                    SymbolSelector.mark_symbol_as_traded(symbol)
                
                # 잔존 매수 최종 동기 스윕 (사이클 종료 경로에서 이미 했으면 빠르게 no-op)
                try:
                    cancel_buy_orders_sync(verify=True)
                except Exception:
                    cancel_buy_orders_async()

                if trading_manager.stop_loss_triggered:
                    end_krw = ws_krw_total()
                    start_ref = cycle_start_krw if cycle_start_krw > 0 else InitialBalance
                    report_cycle_pnl(cycle_count, symbol, start_ref, end_krw)
                    print_log(LogLevel.WARNING, "Trading completed due to stop loss — alarm sounding indefinitely")
                    # 스탑로스 알람 유지 — sleep 없이 메인 스레드 블록 (Ctrl+C로 종료)
                    try:
                        threading.Event().wait()
                    except KeyboardInterrupt:
                        pass
                    exit(0)
                else:
                    print_log(LogLevel.SUCCESS, "Trading completed successfully")

            # 잔고 업데이트 및 다음 사이클 준비 (WS myAsset 기준 손익)
            if trading_manager.is_trading_complete():
                end_krw = ws_krw_total()
                start_ref = cycle_start_krw if cycle_start_krw > 0 else InitialBalance
                S, _ = report_cycle_pnl(cycle_count, symbol, start_ref, end_krw)
                S = int(S)
            else:
                try:
                    S = int(ws_krw_total())
                except Exception:
                    S = int(AccountChecker().get_krw_balance())
                log_balance(S)

            # 매도완료 플래그인데 REST상 매도가능 잔량 남으면 오완료 — reset 금지
            if (trading_manager.is_trading_complete()
                    and not trading_manager.stop_loss_triggered):
                vol_l, notional_l, sellable_l = rest_holding_snapshot(symbol)
                if sellable_l:
                    print_log(LogLevel.WARNING,
                              f"False cycle-complete — REST holdings "
                              f"vol={vol_l:.6f} ≈{notional_l:,.0f}원 — reopen sell")
                    trading_manager.sell_orders_executed = False
                    trading_manager.sell_orders_placed = False
                    trading_manager.buy_orders_executed = True
                    try:
                        pcts = [float(getattr(
                            trading_manager, '_last_profit_pct', profit_percentage)
                            or profit_percentage)]
                        sc = SellController()
                        if sc.place_sell_orders(
                                symbol, pcts, None,
                                volume_hint=vol_l, force_replace=True):
                            trading_manager.mark_sell_orders_placed()
                    except Exception as e:
                        print_log(LogLevel.ERROR,
                                  f"reopen sell failed: {str(e)[:120]}")

            # ★ REST 전량미완이면 reset·심볼 전환 절대 금지
            if not rest_holdings_cleared(symbol):
                trading_manager.touch_symbol_cache()
                print_log(LogLevel.WARNING,
                          f"Cycle {cycle_count} incomplete on "
                          f"{trading_manager.current_symbol} — "
                          f"REST 전량매도 필수, no symbol switch")
            elif (trading_manager.is_trading_complete()
                    or not trading_manager.is_cycle_locked()):
                trading_manager.reset()
                cycle_start_krw = 0.0
                print_log(LogLevel.INFO,
                          f"Cycle {cycle_count} completed. Waiting for next cycle...")
            else:
                trading_manager.touch_symbol_cache()
                print_log(LogLevel.WARNING,
                          f"Cycle {cycle_count} incomplete on "
                          f"{trading_manager.current_symbol} — "
                          f"lock held, will resume (no symbol switch)")
            
    except Exception as e:
        log_state(LogState.ERROR)
        print_log(LogLevel.ERROR, f"Unexpected error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
