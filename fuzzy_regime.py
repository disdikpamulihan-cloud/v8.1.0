"""
fuzzy_regime.py — v8.2.0. Fixed: m_t start 20→25 so TRANSITION peaks ~23 not 27.
"""
from __future__ import annotations

from typing import Dict, Tuple

TRANSITION_MAP = {"TRANSITION": "WEAK_TREND"}


def _tri(x: float, a: float, b: float, c: float) -> float:
    if x <= a or x >= c:
        return 0.0
    if x <= b:
        return (x - a) / (b - a)
    return (c - x) / (c - b)


def membership(adx_value: float) -> Dict[str, float]:
    # RANGING peaks at 15, ends at 27
    m_r = _tri(adx_value, 0.0, 15.0, 27.0)
    # TRENDING peaks at 40, ends at 60 — start at 25 (was 20)
    m_t = _tri(adx_value, 25.0, 40.0, 60.0)
    raw = {"RANGING": max(m_r, 0.0), "TRENDING": max(m_t, 0.0)}
    s = sum(raw.values())
    if s > 1.0:
        for k in raw:
            raw[k] /= s
        s = 1.0
    raw["TRANSITION"] = max(1.0 - s, 0.0)
    total = sum(raw.values())
    if total < 1e-9:
        return {"RANGING": 0.0, "TRANSITION": 1.0, "TRENDING": 0.0}
    return {k: round(v / total, 4) for k, v in raw.items()}


def dominant(weights: Dict[str, float]) -> str:
    return max(weights, key=weights.get)


def blend_params(weights: Dict[str, float],
                 per_regime: Dict[str, Dict[str, float]],
                 keys: Tuple[str, ...] = ("threshold", "sl_mult", "tp1_mult", "tp2_mult", "max_hold")
                 ) -> Dict[str, float]:
    out: Dict[str, float] = {}
    fallback = next(iter(per_regime.values()), {})
    for k in keys:
        num, den = 0.0, 0.0
        for r, w in weights.items():
            if w <= 0:
                continue
            r_key = TRANSITION_MAP.get(r, r)
            val = per_regime.get(r_key, fallback).get(k)
            if val is None:
                continue
            num += w * float(val)
            den += w
        out[k] = num / den if den > 0 else float(fallback.get(k, 0.0))
    return out
