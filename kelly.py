"""
kelly.py — v8.2.0 Bayesian-shrunk fractional Kelly.
MIN_TRADES raised 20 → 50. Beta(5,5) prior on win-rate.
"""
from __future__ import annotations

import logging
import os
from typing import List, Optional, Tuple

MIN_TRADES_FOR_KELLY = 50
DEFAULT_RISK_PCT = 1.0
BAYES_ALPHA = 5.0
BAYES_BETA = 5.0


def _log(level: int, msg: str, **kw) -> None:
    lg = logging.getLogger("xauusd")
    rec = lg.makeRecord(lg.name, level, "", 0, msg, (), None)
    rec.extra_data = kw
    lg.handle(rec)


def kelly_fraction(R: List[float], fraction: float = 0.5, cap_pct: float = 3.0) -> Tuple[float, str]:
    n = len(R)
    if n < MIN_TRADES_FOR_KELLY:
        return DEFAULT_RISK_PCT, f"fallback {DEFAULT_RISK_PCT}% (n={n}<{MIN_TRADES_FOR_KELLY})"

    wins = [x for x in R if x > 0]
    losses = [abs(x) for x in R if x < 0]
    if not wins or not losses:
        return DEFAULT_RISK_PCT, f"fallback (no wins or no losses, n={n})"

    # Bayesian shrinkage
    p_raw = len(wins) / n
    p_bayes = (len(wins) + BAYES_ALPHA) / (n + BAYES_ALPHA + BAYES_BETA)

    avg_win = sum(wins) / len(wins)
    avg_loss = sum(losses) / len(losses)
    if avg_loss <= 0:
        return DEFAULT_RISK_PCT, "fallback (avg_loss<=0)"

    b = avg_win / avg_loss
    f_full = (p_bayes * b - (1.0 - p_bayes)) / b
    f_frac = max(f_full * fraction, 0.0) * 100.0
    capped = min(f_frac, cap_pct)

    reason = (f"kelly_bayes p={p_bayes:.3f} (raw={p_raw:.3f}) "
              f"b={b:.2f} full={f_full*100:.2f}% ×{fraction}={f_frac:.2f}% "
              f"cap={cap_pct}% n={n}")
    _log(logging.INFO, "kelly_computed", p_bayes=p_bayes, p_raw=p_raw,
         b=b, f_full=f_full, f_frac=f_frac/100.0, n=n)
    return round(capped, 2), reason


def risk_pct_from_env() -> Tuple[float, float]:
    return (
        float(os.getenv("KELLY_FRACTION", "0.5")),
        float(os.getenv("KELLY_CAP_PCT", "3.0")),
    )
