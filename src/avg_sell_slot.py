# -*- coding: utf-8 -*-
"""Per-symbol avg→sell state for parallel coin workers."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional


@dataclass
class AvgSellSlot:
    symbol: str
    cb: Any = None  # callback(avg, vol)
    armed: bool = False
    vol_hint: float = 0.0
    prev_avg: float = 0.0
    prev_t: float = 0.0
    baseline_avg: float = 0.0
    baseline_t: float = 0.0
    inflight: bool = False
    fire_gen: int = 0
    price_floor: float = 0.0
    last_fired_avg: float = 0.0
    fire_is_local: bool = False
    awaiting_rest: bool = False
    rest_correct_gen: int = 0
    pending_avg_correct: Any = None
    # local VWAP ledger
    local_qty0: float = 0.0
    local_avg0: float = 0.0
    local_fill_vol: float = 0.0
    local_fill_cost: float = 0.0
    local_uid_vol: Dict[str, float] = field(default_factory=dict)
    local_uid_cost: Dict[str, float] = field(default_factory=dict)
    local_rest_synced: bool = False
    local_max_fill: float = 0.0

    def reset_ledger(self, qty0=0.0, avg0=0.0):
        q0 = max(float(qty0 or 0), 0.0)
        a0 = max(float(avg0 or 0), 0.0)
        if a0 <= 0:
            q0 = 0.0
        self.local_qty0 = q0
        self.local_avg0 = a0
        self.local_fill_vol = 0.0
        self.local_fill_cost = 0.0
        self.local_uid_vol = {}
        self.local_uid_cost = {}
        self.local_max_fill = a0 if a0 > 0 else 0.0
        self.local_rest_synced = False

    def compute_local_avg(self) -> float:
        q0 = float(self.local_qty0 or 0)
        a0 = float(self.local_avg0 or 0)
        fv = float(self.local_fill_vol or 0)
        fc = float(self.local_fill_cost or 0)
        tot = q0 + fv
        if tot <= 1e-15:
            return 0.0
        return (a0 * q0 + fc) / tot

    def local_total_vol_hint(self, extra=0.0) -> float:
        return max(
            float(extra or 0),
            float(self.local_qty0 or 0) + float(self.local_fill_vol or 0),
            float(self.vol_hint or 0),
        )

    def note_fill(self, vol, price, uuid=None) -> float:
        try:
            vol = float(vol or 0)
            price = float(price or 0)
        except (TypeError, ValueError):
            return self.compute_local_avg()
        if vol <= 0 or price <= 0:
            return self.compute_local_avg()
        if uuid:
            uid = str(uuid)
            prev_v = float(self.local_uid_vol.get(uid, 0) or 0)
            prev_c = float(self.local_uid_cost.get(uid, 0) or 0)
            if vol + 1e-15 < prev_v:
                return self.compute_local_avg()
            self.local_fill_vol = max(0.0, self.local_fill_vol - prev_v)
            self.local_fill_cost = max(0.0, self.local_fill_cost - prev_c)
            new_c = price * vol
            self.local_uid_vol[uid] = vol
            self.local_uid_cost[uid] = new_c
            self.local_fill_vol += vol
            self.local_fill_cost += new_c
        else:
            self.local_fill_vol += vol
            self.local_fill_cost += price * vol
        if price > 0:
            self.price_floor = max(float(self.price_floor or 0), price)
            self.local_max_fill = max(float(self.local_max_fill or 0), price)
        return self.compute_local_avg()

    def clear_trading(self):
        self.cb = None
        self.armed = False
        self.vol_hint = 0.0
        self.inflight = False
        self.baseline_avg = 0.0
        self.baseline_t = 0.0
        self.price_floor = 0.0
        self.last_fired_avg = 0.0
        self.fire_is_local = False
        self.awaiting_rest = False
        self.fire_gen += 1
        self.rest_correct_gen += 1
        self.reset_ledger(0, 0)
