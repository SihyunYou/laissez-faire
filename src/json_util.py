# -*- coding: utf-8 -*-
"""Fast JSON helpers (orjson if available)."""
from __future__ import annotations

import json

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
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
