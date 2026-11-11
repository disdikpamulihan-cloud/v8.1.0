"""
XAUUSD Quantitative Signal Bot — v8.2.1 PRODUCTION HARDENED
============================================================
Changelog v8.2.1 (surgical fix):
  [FIX] MarketDataHandler._twelve/_yf/_csv: volume column guard
        (Twelve Data XAU/USD forex has NO volume field → previous
        `df.get("volume", 1000.0).fillna(...)` crashed on float scalar)
  [FIX] _maybe_heartbeat(): accept source param (no more '?')
"""
from __future__ import annotations

import calendar
import html
import json
import logging
import os
import re
import sqlite3
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import yfinance as yf
except ImportError:
    yf = None

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fuzzy_regime import blend_params, dominant, membership
from kelly import kelly_fraction, risk_pct_from_env
from news_calendar import NewsResult, check_news_guard as _live_news_guard
from supabase_client import SUPABASE, try_insert_signal

VERSION = "8.2.1"
RUN_ID = uuid.uuid4().hex[:8]
REGIMES = ("TRENDING", "WEAK_TREND", "RANGING")


# ============================================================================
# 1. CONFIG
# ============================================================================
@dataclass
class BotConfig:
    min_bars: int = 500
    retry_total: int = 3
    retry_backoff: float = 0.5
    http_timeout: int = 12
    telegram_timeout: int = 8
    account_size_usd: float = 10_000.0
    risk_pct: float = 1.0
    contract_size: float = 100.0
    min_sl_usd: float = 1.0
    signal_cooldown_min: int = 30
    spot_cache_min: int = 2
    macro_cache_min: int = 30
    news_enabled: bool = True
    notify_wait: bool = False
    max_daily_signals: int = 10

    trading_mode: str = "paper"
    auto_confirm_hours: int = 4
    max_concurrent_per_direction: int = 2
    max_total_open_risk_pct: float = 3.0
    daily_dd_pct_limit: float = 3.0

    daily_dd_limit_r: float = 3.0
    daily_dd_derisk_r: float = 2.0
    daily_dd_derisk_mult: float = 0.5
    cb_halt_consecutive: int = 5

    atr_spike_mult: float = 1.5
    atr_collapse_mult: float = 0.3
    macro_dxy_veto_pct: float = 0.40
    macro_10y_veto_pct: float = 1.50

    td_quota_warn_threshold: int = 100
    heartbeat_hours: int = 24
    allow_unadjusted_params: bool = False

    twelve_data_api_key: Optional[str] = None
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    csv_path: str = "data/xauusd_h1.csv"
    db_path: str = "data/quant_audit.db"

    def __post_init__(self) -> None:
        if self.trading_mode not in ("paper", "live"):
            raise ValueError(f"trading_mode={self.trading_mode!r} (must be paper|live)")
        if not (0.0 < self.risk_pct <= 5.0):
            raise ValueError(f"risk_pct={self.risk_pct} outside (0, 5]")
        if self.account_size_usd <= 0:
            raise ValueError(f"account_size_usd={self.account_size_usd} <= 0")
        if self.contract_size not in (1.0, 10.0, 100.0):
            raise ValueError(
                f"contract_size={self.contract_size} not in (1,10,100) — verify broker spec")
        if not (0.0 < self.max_total_open_risk_pct <= 10.0):
            raise ValueError(f"max_total_open_risk_pct={self.max_total_open_risk_pct} (0,10]")
        if not (0.0 < self.daily_dd_pct_limit <= 10.0):
            raise ValueError(f"daily_dd_pct_limit={self.daily_dd_pct_limit} (0,10]")
        if self.max_concurrent_per_direction < 1:
            raise ValueError("max_concurrent_per_direction must be >= 1")
        if self.daily_dd_limit_r <= 0 or self.daily_dd_derisk_r <= 0:
            raise ValueError("daily_dd_limit_r / derisk_r must be > 0")
        if self.auto_confirm_hours < 1:
            raise ValueError("auto_confirm_hours must be >= 1")
        if self.trading_mode == "live" and not SUPABASE.enabled:
            raise ValueError(
                "TRADING_MODE=live requires SUPABASE_URL + SUPABASE_KEY "
                "(manual confirmation workflow cannot run without cloud DB)")

    @classmethod
    def from_env(cls) -> "BotConfig":
        return cls(
            account_size_usd=float(os.getenv("ACCOUNT_SIZE_USD", "10000")),
            risk_pct=float(os.getenv("RISK_PCT", "1.0")),
            contract_size=float(os.getenv("XAUUSD_CONTRACT_SIZE", "100.0")),
            min_sl_usd=float(os.getenv("MIN_SL_USD", "1.0")),
            news_enabled=os.getenv("NEWS_ENABLED", "1") == "1",
            notify_wait=os.getenv("NOTIFY_WAIT", "0") == "1",
            max_daily_signals=int(os.getenv("MAX_DAILY_SIGNALS", "10")),
            trading_mode=os.getenv("TRADING_MODE", "paper").strip().lower(),
            auto_confirm_hours=int(os.getenv("AUTO_CONFIRM_HOURS", "4")),
            max_concurrent_per_direction=int(os.getenv("MAX_CONCURRENT_PER_DIRECTION", "2")),
            max_total_open_risk_pct=float(os.getenv("MAX_TOTAL_OPEN_RISK_PCT", "3.0")),
            daily_dd_pct_limit=float(os.getenv("DAILY_DD_PCT_LIMIT", "3.0")),
            daily_dd_limit_r=float(os.getenv("DAILY_DD_LIMIT_R", "3.0")),
            daily_dd_derisk_r=float(os.getenv("DAILY_DD_DERISK_R", "2.0")),
            daily_dd_derisk_mult=float(os.getenv("DAILY_DD_DERISK_MULT", "0.5")),
            cb_halt_consecutive=int(os.getenv("CB_HALT_CONSECUTIVE", "5")),
            atr_spike_mult=float(os.getenv("ATR_SPIKE_MULT", "1.5")),
            atr_collapse_mult=float(os.getenv("ATR_COLLAPSE_MULT", "0.3")),
            macro_dxy_veto_pct=float(os.getenv("MACRO_DXY_VETO_PCT", "0.40")),
            macro_10y_veto_pct=float(os.getenv("MACRO_10Y_VETO_PCT", "1.50")),
            td_quota_warn_threshold=int(os.getenv("TD_QUOTA_WARN_THRESHOLD", "100")),
            heartbeat_hours=int(os.getenv("HEARTBEAT_HOURS", "24")),
            allow_unadjusted_params=os.getenv("ALLOW_UNADJUSTED_PARAMS", "0") == "1",
            twelve_data_api_key=os.getenv("TWELVE_DATA_API_KEY"),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID"),
            csv_path=os.getenv("OHLCV_CSV_PATH", "data/xauusd_h1.csv"),
            db_path=os.getenv("AUDIT_DB_PATH", "data/quant_audit.db"),
        )

    def warnings(self) -> List[str]:
        w = []
        if not self.twelve_data_api_key:
            w.append("TWELVE_DATA_API_KEY missing → Yahoo fallback")
        if not self.telegram_bot_token or not self.telegram_chat_id:
            w.append("Telegram creds missing → stdout only")
        if self.trading_mode == "paper":
            w.append("PAPER MODE — outcomes assumed, Kelly fed by paper R (no slippage)")
        return w


# ============================================================================
# 2. LOGGING
# ============================================================================
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        p = {"ts": datetime.now(timezone.utc).isoformat(), "run_id": RUN_ID,
             "level": record.levelname, "msg": record.getMessage()}
        extra = getattr(record, "extra_data", None)
        if isinstance(extra, dict):
            p.update(extra)
        if record.exc_info:
            p["exc"] = self.formatException(record.exc_info)
        return json.dumps(p, default=str)


def get_logger(name: str = "xauusd") -> logging.Logger:
    lg = logging.getLogger(name)
    if not lg.handlers:
        lg.setLevel(logging.INFO)
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(JsonFormatter())
        lg.addHandler(h)
        lg.propagate = False
    return lg


LOG = get_logger()


def log_event(level: int = logging.INFO, event: str = "", **kw: Any) -> None:
    rec = LOG.makeRecord(LOG.name, level, "", 0, event, (), None)
    rec.extra_data = kw
    LOG.handle(rec)


# ============================================================================
# 3. INDICATORS
# ============================================================================
def ema(s: pd.Series, p: int) -> pd.Series:
    return s.ewm(span=p, adjust=False).mean()


def sma(s: pd.Series, p: int) -> pd.Series:
    return s.rolling(p, min_periods=p).mean()


def rsi(s: pd.Series, p: int = 14) -> pd.Series:
    d = s.diff()
    gain = d.clip(lower=0).ewm(alpha=1/p, adjust=False).mean()
    loss = -d.clip(upper=0).ewm(alpha=1/p, adjust=False).mean()
    return 100.0 - 100.0 / (1.0 + gain / (loss + 1e-10))


def atr(df: pd.DataFrame, p: int = 14) -> pd.Series:
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"] - df["close"].shift(1)).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1/p, adjust=False).mean()


def adx(df: pd.DataFrame, p: int = 14) -> Tuple[pd.Series, pd.Series, pd.Series]:
    up, dn = df["high"].diff(), -df["low"].diff()
    plus_dm = np.where((up > dn) & (up > 0), up, 0.0)
    minus_dm = np.where((dn > up) & (dn > 0), dn, 0.0)
    tr = atr(df, p)
    p_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1/p, adjust=False).mean() / (tr + 1e-10)
    m_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1/p, adjust=False).mean() / (tr + 1e-10)
    dx = 100 * (p_di - m_di).abs() / (p_di + m_di + 1e-10)
    return dx.ewm(alpha=1/p, adjust=False).mean(), p_di, m_di


def volume_profile_poc(df: pd.DataFrame, bins: int = 25) -> float:
    if df["volume"].sum() <= 0:
        return float(df["close"].median())
    hist, edges = np.histogram(df["close"], bins=bins, weights=df["volume"])
    i = int(np.argmax(hist))
    return float((edges[i] + edges[i + 1]) / 2.0)


# ============================================================================
# 4. HOLIDAYS
# ============================================================================
def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    weeks = calendar.monthcalendar(year, month)
    days = [w[weekday] for w in weeks if w[weekday] != 0]
    return date(year, month, days[n - 1] if n > 0 else days[n])


def _easter(year: int) -> date:
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19*a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2*e + 2*i - h - k) % 7
    m = (a + 11*h + 22*l) // 451
    month, day = divmod(h + l - 7*m + 114, 31)
    return date(year, month, day + 1)


def _holiday_set(year: int) -> Set[date]:
    h: Set[date] = {
        date(year, 1, 1),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _easter(year) - timedelta(days=2),
        _nth_weekday(year, 5, 0, -1),
        date(year, 7, 4),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        date(year, 12, 25),
    }
    if year >= 2022:
        h.add(date(year, 6, 19))
    return h


_HOLIDAYS: Dict[int, Set[date]] = {y: _holiday_set(y) for y in range(2024, 2031)}


def is_holiday(d: date) -> bool:
    return d in _HOLIDAYS.get(d.year, set())


def is_market_open(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(timezone.utc)
    if is_holiday(now.date()):
        return False
    wd, h = now.weekday(), now.hour
    if wd == 5:
        return False
    if wd == 6:
        return h >= 22
    if wd == 4:
        return h < 21
    return True


# ============================================================================
# 5. DATA QUALITY
# ============================================================================
def validate_ohlcv(df: pd.DataFrame, min_bars: int, label: str, allow_gap: bool = False) -> None:
    req = {"time", "open", "high", "low", "close", "volume"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"{label}: missing {miss}")
    if len(df) < min_bars:
        raise ValueError(f"{label}: {len(df)}<{min_bars}")
    if df[["open", "high", "low", "close"]].isna().any().any():
        raise ValueError(f"{label}: NaN OHLC")
    if (df["high"] < df["low"]).any():
        raise ValueError(f"{label}: high<low")
    if df["volume"].sum() <= 0:
        raise ValueError(f"{label}: zero volume")
    if not allow_gap and len(df) > 1:
        gaps = df["time"].diff()
        for i in range(1, len(df)):
            g = gaps.iloc[i]
            if pd.isna(g) or g <= pd.Timedelta("4h"):
                continue
            prev_t, curr_t = df["time"].iloc[i-1], df["time"].iloc[i]
            if g > pd.Timedelta("24h"):
                continue
            if prev_t.weekday() == 4 and prev_t.hour >= 20:
                continue
            if curr_t.weekday() in (6, 0):
                continue
            raise ValueError(f"{label}: internal gap {g} at {curr_t}")
    last_ts = pd.to_datetime(df["time"].iloc[-1], utc=True).tz_localize(None)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    age_h = (now - last_ts.to_pydatetime()).total_seconds() / 3600
    max_age = 6 if is_market_open() else 72
    if age_h > max_age:
        raise ValueError(f"{label}: stale {age_h:.1f}h (max {max_age})")


# ============================================================================
# 6. PARAMS
# ============================================================================
DEFAULT_PER_REGIME_PARAMS: Dict[str, Dict[str, float]] = {
    "TRENDING":   {"threshold": 65.0, "sl_mult": 1.5, "tp1_mult": 2.0, "tp2_mult": 3.0, "max_hold": 72.0},
    "WEAK_TREND": {"threshold": 70.0, "sl_mult": 1.5, "tp1_mult": 1.5, "tp2_mult": 2.5, "max_hold": 48.0},
    "RANGING":    {"threshold": 78.0, "sl_mult": 1.0, "tp1_mult": 1.0, "tp2_mult": 2.0, "max_hold": 24.0},
}

_PARAM_BOUNDS = {
    "threshold": (50.0, 95.0), "sl_mult": (0.5, 3.0),
    "tp1_mult": (0.5, 4.0), "tp2_mult": (1.0, 6.0), "max_hold": (8.0, 168.0),
}


def _validate_params(per_regime: Dict[str, Dict[str, float]]) -> Tuple[bool, str]:
    if not isinstance(per_regime, dict):
        return False, "not dict"
    for r in REGIMES:
        if r not in per_regime or not isinstance(per_regime[r], dict):
            return False, f"missing regime {r}"
        for k, (lo, hi) in _PARAM_BOUNDS.items():
            v = per_regime[r].get(k)
            if v is None or not (lo <= float(v) <= hi):
                return False, f"{r}.{k}={v} out [{lo},{hi}]"
    return True, "ok"


def load_params(cfg: BotConfig) -> Tuple[Dict[str, Dict[str, float]], str]:
    path = os.getenv("PARAMS_FILE", "params.json")
    builtin = {r: dict(DEFAULT_PER_REGIME_PARAMS[r]) for r in REGIMES}
    if not os.path.exists(path):
        return builtin, "builtin"
    if not cfg.allow_unadjusted_params:
        log_event(logging.WARNING, "params_refused",
                  reason="optimizer uses crisp regime, live uses fuzzy — "
                         "set ALLOW_UNADJUSTED_PARAMS=1 to override")
        return builtin, "builtin (mismatch guard)"
    try:
        with open(path) as f:
            j = json.load(f)
        schema = int(j.get("schema", 1))
        if schema >= 2:
            per = j.get("per_regime", {})
            merged = {r: {**DEFAULT_PER_REGIME_PARAMS[r], **per.get(r, {})} for r in REGIMES}
        else:
            flat = j.get("best_params", {})
            merged = {r: {**DEFAULT_PER_REGIME_PARAMS[r], **flat} for r in REGIMES}
        ok, why = _validate_params(merged)
        if not ok:
            log_event(logging.WARNING, "params_invalid", reason=why)
            return builtin, "builtin"
        return merged, "file (unadjusted)"
    except Exception as e:
        log_event(logging.WARNING, "params_load_fail", err=str(e)[:120])
        return builtin, "builtin"


# ============================================================================
# 7. MARKET DATA HANDLER
# ============================================================================
def _retry_session(retries: int, backoff: float) -> requests.Session:
    s = requests.Session()
    r = Retry(total=retries, backoff_factor=backoff,
              status_forcelist=[429, 500, 502, 503, 504],
              allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({"User-Agent": f"XAUUSD-Quant/{VERSION}"})
    return s


@dataclass
class MarketData:
    h1: pd.DataFrame
    m15: pd.DataFrame
    source: str
    m15_synthetic: bool = False


class MarketDataHandler:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
        self.session = _retry_session(cfg.retry_total, cfg.retry_backoff)
        self._spot_cache: Optional[Tuple[datetime, float]] = None
        self.td_remaining: Optional[int] = None

    def _check_td_quota(self, r: requests.Response) -> None:
        for hdr in ("X-RateLimit-Remaining", "x-ratelimit-remaining"):
            v = r.headers.get(hdr)
            if v is None:
                continue
            try:
                rem = int(v)
                self.td_remaining = rem
                if rem < self.cfg.td_quota_warn_threshold:
                    log_event(logging.WARNING, "td_quota_low", remaining=rem)
            except (ValueError, TypeError):
                pass
            return

    def _twelve(self, interval: str, size: int) -> Optional[pd.DataFrame]:
        if not self.cfg.twelve_data_api_key:
            return None
        url = (f"https://api.twelvedata.com/time_series?symbol=XAU/USD"
               f"&interval={interval}&outputsize={size}"
               f"&apikey={self.cfg.twelve_data_api_key}")
        r = self.session.get(url, timeout=self.cfg.http_timeout)
        self._check_td_quota(r)
        r.raise_for_status()
        j = r.json()
        if j.get("status") == "error" or j.get("code", 200) >= 400:
            raise RuntimeError(j.get("message", "TwelveData error"))
        vals = j.get("values") or []
        if not vals:
            return None
        df = pd.DataFrame(vals).rename(columns={"datetime": "time"})
        df["time"] = pd.to_datetime(df["time"], utc=True).dt.tz_localize(None)
        for c in ["open", "high", "low", "close"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        # [v8.2.1 FIX] XAU/USD forex has NO volume field
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(1000.0)
        else:
            df["volume"] = 1000.0
        return df.sort_values("time").reset_index(drop=True)

    def _yf(self) -> MarketData:
        if yf is None:
            raise RuntimeError("yfinance missing")
        tk = yf.Ticker("GC=F")
        h1_raw = tk.history(period="60d", interval="1h")
        m15_raw = tk.history(period="60d", interval="15m")
        if h1_raw.empty:
            raise RuntimeError("Yahoo H1 empty")

        def norm(d: pd.DataFrame) -> pd.DataFrame:
            d = d.reset_index()
            tcol = "Datetime" if "Datetime" in d.columns else (
                "Date" if "Date" in d.columns else d.columns[0])
            d = d.rename(columns={tcol: "time", "Open": "open", "High": "high",
                                   "Low": "low", "Close": "close", "Volume": "volume"})
            d["time"] = pd.to_datetime(d["time"], utc=True).dt.tz_localize(None)
            for c in ["open", "high", "low", "close"]:
                d[c] = pd.to_numeric(d[c], errors="coerce")
            # [v8.2.1 FIX] guard volume
            if "volume" in d.columns:
                d["volume"] = pd.to_numeric(d["volume"], errors="coerce").fillna(1000.0)
            else:
                d["volume"] = 1000.0
            return d.dropna(subset=["open", "high", "low", "close"]).sort_values("time").reset_index(drop=True)

        h1 = norm(h1_raw)
        if not m15_raw.empty:
            m15 = norm(m15_raw)
            if len(m15) >= 50:
                return MarketData(h1, m15, "yfinance", m15_synthetic=False)
        m15 = h1.set_index("time").resample("15min").ffill().reset_index()
        m15["volume"] /= 4.0
        return MarketData(h1, m15, "yfinance+synthetic", m15_synthetic=True)

    def _csv(self) -> MarketData:
        p = self.cfg.csv_path
        if not (os.path.exists(p) and os.path.getsize(p) > 0):
            raise FileNotFoundError(f"CSV fallback missing: {p}")
        df = pd.read_csv(p)
        df.columns = [c.strip().lower() for c in df.columns]
        tcol = "datetime" if "datetime" in df.columns else (
            "time" if "time" in df.columns else df.columns[0])
        df["time"] = pd.to_datetime(df[tcol], utc=True).dt.tz_localize(None)
        for c in ["open", "high", "low", "close"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        # [v8.2.1 FIX] guard volume
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(1000.0)
        else:
            df["volume"] = 1000.0
        df = df.dropna(subset=["open", "high", "low", "close"]).sort_values("time").reset_index(drop=True)
        m15 = df.set_index("time").resample("15min").ffill().reset_index()
        m15["volume"] /= 4.0
        return MarketData(df, m15, "csv", m15_synthetic=True)

    def fetch(self) -> MarketData:
        try:
            h1 = self._twelve("1h", 500)
            m15 = self._twelve("15min", 200)
            if h1 is not None and len(h1) >= self.cfg.min_bars:
                validate_ohlcv(h1, self.cfg.min_bars, "12data-H1")
                if m15 is None or len(m15) < 50:
                    m15 = h1.set_index("time").resample("15min").ffill().reset_index()
                    m15["volume"] /= 4.0
                    synth = True
                else:
                    validate_ohlcv(m15, 50, "12data-M15")
                    synth = False
                log_event(event="data_ok", source="twelvedata", m15_synthetic=synth,
                          td_remaining=self.td_remaining)
                return MarketData(h1, m15, "twelvedata", m15_synthetic=synth)
        except Exception as e:
            log_event(logging.WARNING, "twelvedata_fail", err=str(e)[:120])

        try:
            md = self._yf()
            validate_ohlcv(md.h1, self.cfg.min_bars, "yf-H1")
            validate_ohlcv(md.m15, 50, "yf-M15")
            log_event(logging.WARNING, "data_fallback",
                      source=md.source, reason="twelvedata unavailable")
            return md
        except Exception as e:
            log_event(logging.WARNING, "yfinance_fail", err=str(e)[:120])

        md = self._csv()
        validate_ohlcv(md.h1, self.cfg.min_bars, "csv-H1", allow_gap=True)
        log_event(logging.WARNING, "data_fallback", source="csv")
        return md

    def fetch_spot(self, h1: pd.DataFrame) -> float:
        if self._spot_cache:
            ts, v = self._spot_cache
            if (datetime.now(timezone.utc) - ts).total_seconds() < self.cfg.spot_cache_min * 60:
                return v
        price = None
        if self.cfg.twelve_data_api_key:
            try:
                r = self.session.get(
                    f"https://api.twelvedata.com/price?symbol=XAU/USD"
                    f"&apikey={self.cfg.twelve_data_api_key}", timeout=6)
                self._check_td_quota(r)
                r.raise_for_status()
                p = float(r.json().get("price", 0) or 0)
                if p > 0:
                    price = p
            except Exception:
                pass
        if price is None and yf is not None:
            try:
                p = float(yf.Ticker("GC=F").fast_info["last_price"])
                if p > 0:
                    price = p
            except Exception:
                pass
        if price is None:
            price = float(h1["close"].iloc[-1])
        self._spot_cache = (datetime.now(timezone.utc), price)
        return price

    def get_basis(self, md: MarketData) -> float:
        fut = float(md.h1["close"].iloc[-1])
        spot = self.fetch_spot(md.h1)
        basis = spot - fut
        if abs(basis) > 100:
            log_event(logging.WARNING, "basis_anomaly", basis=round(basis, 2))
            return 0.0
        return basis


# ============================================================================
# 8. NEWS
# ============================================================================
class NewsSafetyFilter:
    def __init__(self, enabled: bool):
        self.enabled = enabled

    def check(self) -> NewsResult:
        if not self.enabled:
            return NewsResult(allowed=True, reason="news disabled", source="bypass")
        try:
            return _live_news_guard()
        except Exception as e:
            log_event(logging.WARNING, "news_guard_fail", err=str(e)[:120])
            return NewsResult(allowed=True, reason="news guard error (bypass)", source="error")


# ============================================================================
# 9. SESSION & MACRO
# ============================================================================
def get_session_profile(cfg: BotConfig) -> Tuple[str, bool]:
    now = datetime.now(timezone.utc)
    if not is_market_open(now):
        return "CLOSED", False
    h = now.hour
    if 0 <= h < 7:   return "ASIA_RANGE", True
    if 7 <= h < 12:  return "LONDON_TREND", True
    if 12 <= h < 16: return "NY_OVERLAP", True
    if 16 <= h < 21: return "NY_PM", True
    return "OFF_HOURS", False


_macro_cache: Dict[str, Tuple[datetime, Tuple[bool, str]]] = {}


def check_macro_correlation(cfg: BotConfig) -> Tuple[bool, str]:
    now = datetime.now(timezone.utc)
    if "m" in _macro_cache:
        ts, val = _macro_cache["m"]
        if (now - ts).total_seconds() < cfg.macro_cache_min * 60:
            return val
    if yf is None:
        return True, "bypass"
    try:
        dxy = yf.Ticker("DX-Y.NYB").history(period="5d")["Close"]
        tnx = yf.Ticker("^TNX").history(period="5d")["Close"]
        notes, safe = [], True
        if len(dxy) >= 2:
            ch = (dxy.iloc[-1] - dxy.iloc[-2]) / dxy.iloc[-2] * 100
            if ch > cfg.macro_dxy_veto_pct:
                safe = False
                notes.append(f"DXY+{ch:.2f}%")
        if len(tnx) >= 2:
            ch = (tnx.iloc[-1] - tnx.iloc[-2]) / tnx.iloc[-2] * 100
            if ch > cfg.macro_10y_veto_pct:
                safe = False
                notes.append(f"10Y+{ch:.2f}%")
        res = (True, "normal") if safe else (False, "veto " + ", ".join(notes))
        _macro_cache["m"] = (now, res)
        return res
    except Exception as e:
        res = (True, f"bypass ({str(e)[:30]})")
        _macro_cache["m"] = (now, res)
        return res


# ============================================================================
# 10. SIGNAL GENERATOR
# ============================================================================
@dataclass
class EnsembleResult:
    action: str
    total_score: float
    voter_agreement: float
    forensic_score: float
    regime: str
    regime_weights: Dict[str, float]
    poc_level: float
    notes: List[str]
    atr_value: float
    atr_multiplier: float
    blended_params: Dict[str, float]
    params_source: str = "builtin"
    voters_debug: Dict[str, Any] = field(default_factory=dict)


class SignalGenerator:
    def __init__(self, params: Dict[str, Dict[str, float]], params_source: str):
        self.params = params
        self.params_source = params_source

    def _atr_regime_ok(self, atr_series: pd.Series, cfg: BotConfig) -> Tuple[bool, str]:
        s = atr_series.dropna()
        if len(s) < 100:
            return True, "insufficient_history"
        cur = float(s.iloc[-1])
        p95 = float(s.quantile(0.95))
        p05 = float(s.quantile(0.05))
        if p95 > 0 and cur > p95 * cfg.atr_spike_mult:
            return False, f"ATR_explosion {cur:.2f}>1.5×p95({p95:.2f})"
        if p05 > 0 and cur < p05 * cfg.atr_collapse_mult:
            return False, f"ATR_collapse {cur:.2f}<0.3×p05({p05:.2f})"
        return True, "ok"

    def _run_21_voters(self, df: pd.DataFrame) -> Tuple[float, int, int, Dict[str, int]]:
        close = df["close"]
        c = float(close.iloc[-1])
        v: List[int] = []
        dbg: Dict[str, int] = {}

        sma_votes = [1 if c > sma(close, 50).iloc[-1] else -1,
                     1 if c > sma(close, 200).iloc[-1] else -1]
        e9 = ema(close, 9); e21 = ema(close, 21)
        e50 = ema(close, 50); e200 = ema(close, 200)
        pad = [
            1 if e9.iloc[-1] > e21.iloc[-1] else -1,
            1 if e50.iloc[-1] > e200.iloc[-1] else -1,
            1 if c > close.iloc[-24] else -1,
            1 if float(close.std()) < float(close.rolling(50).std().iloc[-1]) else -1,
        ]
        v.extend(sma_votes + pad)
        dbg["trend"] = sum(sma_votes + pad)

        e1 = 1 if ema(close, 9).iloc[-1] > ema(close, 21).iloc[-1] else -1
        e2 = 1 if ema(close, 12).iloc[-1] > ema(close, 26).iloc[-1] else -1
        e3 = 1 if ema(close, 50).iloc[-1] > ema(close, 200).iloc[-1] else -1
        v.extend([e1, e2, e3])
        dbg["ema"] = e1 + e2 + e3

        r = float(rsi(close, 14).iloc[-1])
        r_v = [
            1 if r > 55 else (-1 if r < 45 else 0),
            -1 if r > 70 else (1 if r < 30 else 0),
            1 if r > 50 else -1,
        ]
        v.extend(r_v)
        dbg["rsi"] = sum(r_v)

        adx_v, p_di, m_di = adx(df, 14)
        m_v = [
            1 if p_di.iloc[-1] > m_di.iloc[-1] else -1,
            1 if adx_v.iloc[-1] > 20 else -1,
            1 if c > close.iloc[-2] else -1,
            1 if c > close.iloc[-5] else -1,
            1 if c > close.iloc[-10] else -1,
        ]
        v.extend(m_v)
        dbg["momentum"] = sum(m_v)

        mp = 0
        for n in (5, 10):
            hi_n, lo_n = df["high"].iloc[-n:].max(), df["low"].iloc[-n:].min()
            pos = (c - lo_n) / max(hi_n - lo_n, 1e-10)
            vote = 1 if pos > 0.5 else -1
            v.append(vote)
            mp += vote
        dbg["micro"] = mp

        vol_ma = df["volume"].rolling(20).mean().iloc[-1]
        vol_v = 1 if (not pd.isna(vol_ma) and df["volume"].iloc[-1] > vol_ma) else -1
        v.append(vol_v)
        dbg["volume"] = vol_v

        poc = volume_profile_poc(df)
        poc_v = 1 if c > poc else -1
        v.append(poc_v)
        dbg["poc"] = poc_v

        assert len(v) == 21, f"voter count {len(v)}"
        bull = sum(1 for x in v if x == 1)
        bear = 21 - bull
        return max(bull, bear) / 21.0, bull, bear, dbg

    def generate(self, md: MarketData, news: NewsResult, cfg: BotConfig) -> EnsembleResult:
        df, m15 = md.h1, md.m15
        close = df["close"]
        c_last = float(close.iloc[-1])
        atr_series = atr(df, 14)
        atr_val = float(atr_series.iloc[-1])
        adx_val = float(adx(df, 14)[0].iloc[-1])

        weights = membership(adx_val)
        dom = dominant(weights)
        blended = blend_params(weights, self.params)
        threshold = blended.get("threshold", 70.0)
        atr_mult = blended.get("sl_mult", 1.5)

        poc = volume_profile_poc(df)
        session, allowed = get_session_profile(cfg)
        corr_ok, corr_msg = check_macro_correlation(cfg)
        ema200 = float(ema(close, 200).iloc[-1])

        last, prev = df.iloc[-1], df.iloc[-21:-1]
        op, hi, lo, cl = last["open"], last["high"], last["low"], last["close"]
        rng = max(hi - lo, 0.01)
        wick_pct = (max(hi - max(op, cl), min(op, cl) - lo) / rng) * 100.0
        sweep = "NONE"
        if hi > prev["high"].max() and cl < prev["high"].max():
            sweep = "BEAR"
        elif lo < prev["low"].min() and cl > prev["low"].min():
            sweep = "BULL"
        h1_vr = last["volume"] / (df["volume"].iloc[-21:-1].mean() + 1e-10)
        m15_vr = (m15["volume"].iloc[-1] / (m15["volume"].iloc[-20:].mean() + 1e-10)
                  if not md.m15_synthetic else 1.0)
        f_score = 30.0
        if sweep != "NONE":   f_score += 35.0
        if wick_pct > 40.0:   f_score += 20.0
        if h1_vr > 1.2 or m15_vr > 1.3: f_score += 15.0
        f_score = min(f_score, 100.0)

        m15_bull = ema(m15["close"], 9).iloc[-1] > ema(m15["close"], 21).iloc[-1]
        voter_agree, bull, bear, dbg = self._run_21_voters(df)
        combined = round((voter_agree * 0.6 + (f_score / 100.0) * 0.4) * 100, 1)
        intended = "BUY" if bull > bear else "SELL"
        macro_ok = ((intended == "BUY" and c_last > ema200)
                    or (intended == "SELL" and c_last < ema200))
        atr_ok, atr_reason = self._atr_regime_ok(atr_series, cfg)

        notes, action = [], "WAIT"
        if not news.allowed:
            notes.append(f"NEWS: {news.reason}")
        elif not allowed:
            notes.append(f"SESSION: {session}")
        elif not corr_ok:
            notes.append(f"MACRO: {corr_msg}")
        elif not atr_ok:
            notes.append(f"ATR_GUARD: {atr_reason}")
        elif combined < threshold:
            notes.append(f"GATED[{dom}]: {combined}% < {threshold:.1f}%")
        elif not macro_ok:
            notes.append("VETO EMA200 H1")
        elif intended == "BUY" and m15_bull:
            action = "BUY"; notes.append(f"OK[{dom}/{session}] {combined}%")
        elif intended == "SELL" and not m15_bull:
            action = "SELL"; notes.append(f"OK[{dom}/{session}] {combined}%")
        else:
            notes.append("M15 misaligned")

        dbg["news_source"] = news.source
        return EnsembleResult(
            action=action, total_score=combined,
            voter_agreement=round(voter_agree, 4), forensic_score=round(f_score, 1),
            regime=dom, regime_weights=weights, poc_level=round(poc, 2),
            notes=notes, atr_value=round(atr_val, 4), atr_multiplier=round(atr_mult, 4),
            blended_params={k: round(v, 4) for k, v in blended.items()},
            params_source=self.params_source, voters_debug=dbg,
        )


# ============================================================================
# 11. AUDIT DB
# ============================================================================
def _to_utc_iso(dt_or_str: Any) -> str:
    if isinstance(dt_or_str, str):
        d = datetime.fromisoformat(dt_or_str)
    elif isinstance(dt_or_str, datetime):
        d = dt_or_str
    else:
        d = datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


class AuditDB:
    def __init__(self, path: str):
        self.ok = False
        try:
            d = os.path.dirname(path)
            if d:
                os.makedirs(d, exist_ok=True)
            self.conn = sqlite3.connect(path, check_same_thread=False, timeout=30.0)
            self.conn.row_factory = sqlite3.Row
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA busy_timeout=30000")
            with self.conn:
                self.conn.execute("""CREATE TABLE IF NOT EXISTS signals(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT, action TEXT, spot REAL, score REAL,
                    regime TEXT, kelly_pct REAL, lot REAL, risk REAL, notes TEXT)""")
                self.conn.execute("""CREATE TABLE IF NOT EXISTS meta(
                    key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)""")
                self._migrate()
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(ts)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_confirmed ON signals(confirmed)")
            self.conn.commit()
            self.ok = True
        except Exception as e:
            log_event(logging.WARNING, "db_init_fail", err=str(e)[:80])

    def _migrate(self) -> None:
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(signals)").fetchall()}
        new_cols = [
            ("signal_id", "TEXT"), ("entry_price", "REAL"), ("sl", "REAL"),
            ("tp1", "REAL"), ("tp2", "REAL"),
            ("status", "TEXT DEFAULT 'OPEN'"), ("r_multiple", "REAL"),
            ("closed_at", "TEXT"), ("closed_price", "REAL"),
            ("max_hold_hours", "REAL DEFAULT 48"),
            ("confirmed", "INTEGER DEFAULT 0"),
            ("confirm_ts", "TEXT"),
            ("pnl_usd", "REAL"),
        ]
        for name, ctype in new_cols:
            if name not in cols:
                try:
                    self.conn.execute(f"ALTER TABLE signals ADD COLUMN {name} {ctype}")
                except Exception as e:
                    log_event(logging.WARNING, "db_migrate_col_fail", col=name, err=str(e)[:80])
        try:
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_signals_signal_id ON signals(signal_id)")
        except Exception:
            pass
        self.conn.commit()

    def record(self, signal_id: str, e: EnsembleResult, spot: float,
               levels: dict, max_hold_hours: float, trading_mode: str) -> None:
        if not self.ok or not levels:
            return
        confirmed = 1 if trading_mode == "paper" else 0
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO signals(signal_id, ts, action, spot, entry_price, sl, tp1, tp2,"
                    " score, regime, kelly_pct, lot, risk, notes, status, max_hold_hours, confirmed)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'OPEN', ?, ?)",
                    (signal_id, _to_utc_iso(datetime.now(timezone.utc)),
                     e.action, spot, levels["entry_price"], levels["sl_price"],
                     levels["tp1_price"], levels["tp2_price"], e.total_score, e.regime,
                     levels["risk_pct"], levels["lot_size"], levels["risk_amount_usd"],
                     "; ".join(e.notes), max_hold_hours, confirmed))
        except Exception as ex:
            log_event(logging.WARNING, "db_write_fail", err=str(ex)[:80])

    def get_open_signals(self, confirmed_only: bool = False) -> List[dict]:
        if not self.ok:
            return []
        try:
            q = ("SELECT * FROM signals WHERE status='OPEN' AND action IN ('BUY','SELL')"
                 " AND sl IS NOT NULL AND tp1 IS NOT NULL AND entry_price IS NOT NULL")
            if confirmed_only:
                q += " AND confirmed=1"
            rows = self.conn.execute(q).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []

    def close_signal(self, signal_id: str, status: str, r_multiple: float,
                     exit_price: float, exit_time: Any, pnl_usd: float = 0.0) -> None:
        if not self.ok:
            return
        try:
            with self.conn:
                self.conn.execute(
                    "UPDATE signals SET status=?, r_multiple=?, closed_at=?, closed_price=?,"
                    " pnl_usd=? WHERE signal_id=?",
                    (status, round(r_multiple, 4), _to_utc_iso(exit_time),
                     round(exit_price, 2), round(pnl_usd, 2), signal_id))
        except Exception as ex:
            log_event(logging.WARNING, "db_close_fail", err=str(ex)[:80])

    def confirm_signal(self, signal_id: str, actual_entry: Optional[float] = None) -> bool:
        if not self.ok:
            return False
        try:
            with self.conn:
                if actual_entry is not None:
                    row = self.conn.execute(
                        "SELECT action, entry_price, sl, tp1, tp2 FROM signals"
                        " WHERE signal_id=?", (signal_id,)).fetchone()
                    if not row:
                        return False
                    sig = dict(row)
                    sl_d = abs(float(sig["entry_price"]) - float(sig["sl"]))
                    tp1_d = abs(float(sig["tp1"]) - float(sig["entry_price"]))
                    tp2_d = abs(float(sig["tp2"]) - float(sig["entry_price"]))
                    if sig["action"] == "BUY":
                        new_sl = actual_entry - sl_d
                        new_tp1 = actual_entry + tp1_d
                        new_tp2 = actual_entry + tp2_d
                    else:
                        new_sl = actual_entry + sl_d
                        new_tp1 = actual_entry - tp1_d
                        new_tp2 = actual_entry - tp2_d
                    self.conn.execute(
                        "UPDATE signals SET confirmed=1, confirm_ts=?, entry_price=?,"
                        " sl=?, tp1=?, tp2=? WHERE signal_id=?",
                        (_to_utc_iso(datetime.now(timezone.utc)),
                         round(actual_entry, 2), round(new_sl, 2),
                         round(new_tp1, 2), round(new_tp2, 2), signal_id))
                else:
                    self.conn.execute(
                        "UPDATE signals SET confirmed=1, confirm_ts=? WHERE signal_id=?",
                        (_to_utc_iso(datetime.now(timezone.utc)), signal_id))
            return True
        except Exception as ex:
            log_event(logging.WARNING, "db_confirm_fail", err=str(ex)[:80])
            return False

    def skip_signal(self, signal_id: str) -> bool:
        if not self.ok:
            return False
        try:
            with self.conn:
                self.conn.execute(
                    "UPDATE signals SET confirmed=2, confirm_ts=?,"
                    " status='SKIPPED', closed_at=? WHERE signal_id=?",
                    (_to_utc_iso(datetime.now(timezone.utc)),
                     _to_utc_iso(datetime.now(timezone.utc)), signal_id))
            return True
        except Exception:
            return False

    def auto_skip_stale(self, hours: int) -> int:
        if not self.ok:
            return 0
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
            with self.conn:
                cur = self.conn.execute(
                    "UPDATE signals SET confirmed=2, status='SKIPPED',"
                    " closed_at=?, notes=COALESCE(notes,'') || '; AUTO_SKIPPED'"
                    " WHERE confirmed=0 AND status='OPEN' AND ts < ?",
                    (_to_utc_iso(datetime.now(timezone.utc)), cutoff))
                return cur.rowcount or 0
        except Exception:
            return 0

    def count_open_signals(self, direction: Optional[str] = None) -> int:
        if not self.ok:
            return 0
        try:
            q = "SELECT COUNT(*) FROM signals WHERE status='OPEN' AND confirmed != 2"
            args: tuple = ()
            if direction:
                q += " AND action=?"
                args = (direction,)
            row = self.conn.execute(q, args).fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0

    def get_open_risk_usd(self) -> float:
        if not self.ok:
            return 0.0
        try:
            row = self.conn.execute(
                "SELECT COALESCE(SUM(risk),0) FROM signals"
                " WHERE status='OPEN' AND confirmed=1").fetchone()
            return float(row[0]) if row else 0.0
        except Exception:
            return 0.0

    def get_today_pnl_usd(self) -> float:
        if not self.ok:
            return 0.0
        try:
            today_start = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0).isoformat()
            row = self.conn.execute(
                "SELECT COALESCE(SUM(pnl_usd),0) FROM signals"
                " WHERE status IN ('WIN_TP1','WIN_TP2','LOSS','TIMEOUT')"
                " AND closed_at >= ?", (today_start,)).fetchone()
            return float(row[0]) if row else 0.0
        except Exception:
            return 0.0

    def get_today_r_sum(self) -> float:
        if not self.ok:
            return 0.0
        try:
            today_start = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0).isoformat()
            row = self.conn.execute(
                "SELECT COALESCE(SUM(r_multiple),0) FROM signals"
                " WHERE status IN ('WIN_TP1','WIN_TP2','LOSS','TIMEOUT')"
                " AND closed_at >= ?", (today_start,)).fetchone()
            return float(row[0]) if row else 0.0
        except Exception:
            return 0.0

    def count_today_signals(self) -> int:
        if not self.ok:
            return 0
        try:
            today_start = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0).isoformat()
            row = self.conn.execute(
                "SELECT COUNT(*) FROM signals WHERE action IN ('BUY','SELL') AND ts >= ?",
                (today_start,)).fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0

    def get_recent_closed(self, limit: int = 10) -> List[dict]:
        if not self.ok:
            return []
        try:
            rows = self.conn.execute(
                "SELECT * FROM signals WHERE status IN ('WIN_TP1','WIN_TP2','LOSS','TIMEOUT')"
                " AND confirmed=1 ORDER BY closed_at DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []

    def get_recent_r(self, limit: int = 50) -> List[float]:
        if not self.ok:
            return []
        try:
            rows = self.conn.execute(
                "SELECT r_multiple FROM signals"
                " WHERE status IN ('WIN_TP1','WIN_TP2','LOSS','TIMEOUT')"
                " AND confirmed=1 AND r_multiple IS NOT NULL"
                " ORDER BY closed_at DESC LIMIT ?", (limit,)).fetchall()
            return [float(r[0]) for r in rows]
        except Exception:
            return []

    def last_action(self) -> Optional[Tuple[str, datetime, float]]:
        if not self.ok:
            return None
        try:
            row = self.conn.execute(
                "SELECT action, ts, spot FROM signals WHERE action IN ('BUY','SELL')"
                " ORDER BY id DESC LIMIT 1").fetchone()
            if row:
                ts = datetime.fromisoformat(row[1])
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                return row[0], ts, row[2]
        except Exception:
            pass
        return None

    def is_dup(self, cooldown: int, action: str, spot: float, tol: float = 1.0) -> bool:
        last = self.last_action()
        if not last:
            return False
        a, ts, sp = last
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - ts).total_seconds() / 60
        return age < cooldown and a == action and abs(sp - spot) < tol

    def get_meta(self, key: str) -> Optional[str]:
        if not self.ok:
            return None
        try:
            row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return row[0] if row else None
        except Exception:
            return None

    def set_meta(self, key: str, value: str) -> None:
        if not self.ok:
            return
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO meta(key,value,updated_at) VALUES(?,?,?)"
                    " ON CONFLICT(key) DO UPDATE SET value=excluded.value,"
                    " updated_at=excluded.updated_at",
                    (key, value, _to_utc_iso(datetime.now(timezone.utc))))
        except Exception:
            pass


# ============================================================================
# 12. RISK ENGINE
# ============================================================================
class RiskManagementEngine:
    def __init__(self, cfg: BotConfig, db: AuditDB):
        self.cfg = cfg
        self.db = db

    def _consecutive_losses(self) -> Tuple[int, str]:
        recent = self.db.get_recent_closed(self.cfg.cb_halt_consecutive)
        n = 0
        for s in recent:
            if s.get("status") == "LOSS":
                n += 1
            else:
                break
        if n >= self.cfg.cb_halt_consecutive:
            return n, f"HALT after {n} consecutive losses"
        if n >= 3:
            return n, f"derisk 50% ({n} consec losses)"
        if n >= 2:
            return n, f"derisk 25% ({n} consec losses)"
        return n, "normal"

    def _circuit_multiplier(self) -> Tuple[float, str]:
        n, reason = self._consecutive_losses()
        if n >= self.cfg.cb_halt_consecutive:
            return 0.0, reason
        if n >= 3:
            return 0.5, reason
        if n >= 2:
            return 0.75, reason
        return 1.0, reason

    def _daily_dd_multiplier(self) -> Tuple[float, str]:
        today_r = self.db.get_today_r_sum()
        today_pnl = self.db.get_today_pnl_usd()
        pnl_limit = -self.cfg.account_size_usd * self.cfg.daily_dd_pct_limit / 100.0
        if today_pnl <= pnl_limit:
            return 0.0, f"DAILY_PNL_HALT (${today_pnl:.0f} ≤ ${pnl_limit:.0f})"
        if today_r <= -self.cfg.daily_dd_limit_r:
            return 0.0, f"DAILY_DD_HALT ({today_r:+.2f}R)"
        if today_r <= -self.cfg.daily_dd_derisk_r:
            return self.cfg.daily_dd_derisk_mult, f"DAILY_DD_DERISK ({today_r:+.2f}R)"
        return 1.0, f"daily OK (${today_pnl:+.0f}, {today_r:+.2f}R)"

    def exposure_gate(self, intended_action: str) -> Tuple[bool, str]:
        if intended_action not in ("BUY", "SELL"):
            return True, "n/a"
        n_same = self.db.count_open_signals(intended_action)
        if n_same >= self.cfg.max_concurrent_per_direction:
            return False, f"max {self.cfg.max_concurrent_per_direction} {intended_action} reached ({n_same})"
        open_risk = self.db.get_open_risk_usd()
        max_risk = self.cfg.account_size_usd * self.cfg.max_total_open_risk_pct / 100.0
        if open_risk >= max_risk:
            return False, f"open risk ${open_risk:.0f} ≥ ${max_risk:.0f}"
        return True, "ok"

    def kelly_risk_pct(self) -> Tuple[float, str]:
        frac, cap = risk_pct_from_env()
        R_history: Optional[List[float]] = None
        if SUPABASE.enabled:
            R_history = SUPABASE.fetch_recent_r(50)
        if not R_history:
            R_history = self.db.get_recent_r(50)
        if R_history and len(R_history) >= 20:
            return kelly_fraction(R_history, fraction=frac, cap_pct=cap)
        n = len(R_history) if R_history else 0
        return self.cfg.risk_pct, f"fallback {self.cfg.risk_pct}% (n={n}<20)"

    def combined_multiplier(self) -> Tuple[float, str, str]:
        cb_mult, cb_r = self._circuit_multiplier()
        dd_mult, dd_r = self._daily_dd_multiplier()
        return min(cb_mult, dd_mult), cb_r, dd_r

    def compute_levels(self, sig: str, entry: float, atr_v: float, atr_mult: float,
                       blended: dict, kelly_risk_pct: float, kelly_reason: str) -> Optional[dict]:
        if sig not in ("BUY", "SELL"):
            return None
        sl_d = max(atr_v * atr_mult, self.cfg.min_sl_usd)
        risk_usd = self.cfg.account_size_usd * kelly_risk_pct / 100.0
        cs = self.cfg.contract_size
        raw_lot = risk_usd / (sl_d * cs)
        lot = round(raw_lot, 2)
        if lot < 0.01:
            lot = 0.01
        if lot * sl_d * cs > risk_usd * 1.10 and lot > 0.01:
            lot = max(round(raw_lot - 0.01, 2), 0.01)
        actual_risk = lot * sl_d * cs

        tp1_r = blended.get("tp1_mult", 1.5) or 1.5
        tp2_r = blended.get("tp2_mult", 2.5) or 2.5
        if sig == "BUY":
            sl, tp1, tp2, be = entry - sl_d, entry + sl_d*tp1_r, entry + sl_d*tp2_r, entry + sl_d
        else:
            sl, tp1, tp2, be = entry + sl_d, entry - sl_d*tp1_r, entry - sl_d*tp2_r, entry - sl_d
        return {
            "entry_price": round(entry, 2), "sl_price": round(sl, 2),
            "tp1_price": round(tp1, 2), "tp2_price": round(tp2, 2),
            "break_even_trigger": round(be, 2),
            "lot_size": lot, "risk_amount_usd": round(risk_usd, 2),
            "actual_risk_usd": round(actual_risk, 2),
            "risk_pct": kelly_risk_pct, "kelly_reason": kelly_reason,
            "sl_distance_usd": round(sl_d, 2),
        }


# ============================================================================
# 13. OUTCOME TRACKER
# ============================================================================
class OutcomeTracker:
    def __init__(self, db: AuditDB, cfg: BotConfig):
        self.db = db
        self.cfg = cfg

    @staticmethod
    def _resolve(action: str, bar_low: float, bar_high: float,
                 sl: float, tp1: float) -> Optional[Tuple[str, float]]:
        if action == "BUY":
            if bar_low <= sl:  return "LOSS", sl
            if bar_high >= tp1: return "WIN_TP1", tp1
        else:
            if bar_high >= sl:  return "LOSS", sl
            if bar_low <= tp1:  return "WIN_TP1", tp1
        return None

    @staticmethod
    def _compute_r(sig: dict, status: str, exit_px: float) -> float:
        entry = float(sig["entry_price"])
        sl = float(sig["sl"])
        sl_d = abs(entry - sl)
        if sl_d < 1e-6:
            return 0.0
        if status == "LOSS":
            return -1.0
        if status == "WIN_TP1":
            return abs(float(sig["tp1"]) - entry) / sl_d
        return (exit_px - entry) / sl_d if sig["action"] == "BUY" else (entry - exit_px) / sl_d

    @staticmethod
    def _parse_ts(v: Any) -> datetime:
        if isinstance(v, datetime):
            d = v
        else:
            d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        if d.tzinfo is not None:
            d = d.astimezone(timezone.utc).replace(tzinfo=None)
        return d

    def process(self, df_h1: pd.DataFrame) -> int:
        open_sigs = self.db.get_open_signals(confirmed_only=True)
        if not open_sigs:
            return 0
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        df = df_h1.copy()
        if df["time"].dt.tz is not None:
            df["time"] = df["time"].dt.tz_localize(None)
        closed = 0

        for sig in open_sigs:
            try:
                entry_t = self._parse_ts(sig["ts"])
                max_hold_h = float(sig.get("max_hold_hours") or 48.0)
                expiry_t = entry_t + timedelta(hours=max_hold_h)
                window_end = min(expiry_t, now)
                bars = df[(df["time"] > entry_t) & (df["time"] <= window_end)]
                risk_usd = float(sig.get("risk") or 0.0)

                if bars.empty:
                    if now > expiry_t:
                        last_px = float(df["close"].iloc[-1]) if not df.empty else float(sig["entry_price"])
                        r = self._compute_r(sig, "TIMEOUT", last_px)
                        pnl = r * risk_usd
                        self.db.close_signal(sig["signal_id"], "TIMEOUT", r, last_px,
                                              datetime.now(timezone.utc), pnl)
                        if SUPABASE.enabled:
                            SUPABASE.update_outcome(sig["signal_id"], "TIMEOUT", r, last_px)
                        closed += 1
                    continue

                resolution = None
                resolution_bar = None
                for _, row in bars.iterrows():
                    res = self._resolve(sig["action"], float(row["low"]),
                                         float(row["high"]), float(sig["sl"]), float(sig["tp1"]))
                    if res:
                        resolution = res
                        resolution_bar = row
                        break

                if resolution:
                    status, px = resolution
                    r = self._compute_r(sig, status, float(px))
                    pnl = r * risk_usd
                    self.db.close_signal(sig["signal_id"], status, r, float(px),
                                          resolution_bar["time"], pnl)
                    if SUPABASE.enabled:
                        SUPABASE.update_outcome(sig["signal_id"], status, r, float(px))
                    log_event(logging.INFO, "outcome_resolved",
                              signal_id=sig["signal_id"], status=status,
                              r=round(r, 3), pnl_usd=round(pnl, 2))
                    closed += 1
                elif now >= expiry_t:
                    last = bars.iloc[-1]
                    exit_px = float(last["close"])
                    r = self._compute_r(sig, "TIMEOUT", exit_px)
                    pnl = r * risk_usd
                    self.db.close_signal(sig["signal_id"], "TIMEOUT", r, exit_px,
                                          last["time"], pnl)
                    if SUPABASE.enabled:
                        SUPABASE.update_outcome(sig["signal_id"], "TIMEOUT", r, exit_px)
                    closed += 1
            except Exception as e:
                log_event(logging.WARNING, "outcome_eval_fail",
                          signal_id=sig.get("signal_id"), err=str(e)[:120])
        return closed


# ============================================================================
# 14. TELEGRAM
# ============================================================================
def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text)


class TelegramNotifier:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg

    def _post(self, payload: dict, attempt: int = 0) -> bool:
        if not (self.cfg.telegram_bot_token and self.cfg.telegram_chat_id):
            return False
        if ":" not in self.cfg.telegram_bot_token:
            return False
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{self.cfg.telegram_bot_token}/sendMessage",
                json=payload, timeout=self.cfg.telegram_timeout)
            if r.status_code == 200:
                return True
            if r.status_code == 429 and attempt < 2:
                wait = min(int(r.headers.get("Retry-After", "5")), 30)
                time.sleep(wait)
                return self._post(payload, attempt + 1)
            if r.status_code == 400 and "parse entities" in r.text:
                plain = {"chat_id": payload["chat_id"],
                         "text": _strip_html(payload["text"]),
                         "disable_web_page_preview": True}
                if "reply_markup" in payload:
                    plain["reply_markup"] = payload["reply_markup"]
                r2 = requests.post(
                    f"https://api.telegram.org/bot{self.cfg.telegram_bot_token}/sendMessage",
                    json=plain, timeout=self.cfg.telegram_timeout)
                return r2.status_code == 200
            log_event(logging.ERROR, "telegram_error", code=r.status_code, body=r.text[:200])
            return False
        except Exception as e:
            log_event(logging.ERROR, "telegram_exc", err=str(e)[:120])
            return False

    def send_signal(self, e: EnsembleResult, spot: float,
                    levels: Optional[dict], signal_id: Optional[str] = None) -> bool:
        if e.action == "WAIT" and not self.cfg.notify_wait:
            log_event(event="telegram_suppressed", action="WAIT",
                      score=e.total_score, reason=e.notes[0] if e.notes else "")
            return False

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        w = e.regime_weights
        fuzzy_str = html.escape(
            f"{e.regime} (R:{w.get('RANGING', 0):.2f} "
            f"T:{w.get('TRANSITION', 0):.2f} Tr:{w.get('TRENDING', 0):.2f})")
        safe_reason = html.escape(e.notes[0]) if e.notes else ""
        safe_kelly = html.escape(levels["kelly_reason"]) if levels else ""

        if e.action == "WAIT" or not levels:
            text = (f"🟡 <b>XAUUSD v{VERSION} — WAIT</b>\n"
                    f"⏰ <code>{now}</code> | Mode: <b>{self.cfg.trading_mode.upper()}</b>\n"
                    f"Score: <b>{e.total_score}%</b> | Regime: {fuzzy_str}\n"
                    f"Reason: {safe_reason}")
        else:
            em = "🟢" if e.action == "BUY" else "🔴"
            confirm_hint = ""
            if self.cfg.trading_mode == "live" and signal_id:
                confirm_hint = (
                    f"\n\n⚠️ <b>LIVE MODE — CONFIRM REQUIRED</b>\n"
                    f"Run: <code>python confirm.py take {signal_id} &lt;fill_price&gt;</code>\n"
                    f"Auto-skip if unconfirmed after {self.cfg.auto_confirm_hours}h.")
            text = (f"{em} <b>XAUUSD — {e.action} (MARKET)</b>\n"
                    f"⏰ <code>{now}</code> | Mode: <b>{self.cfg.trading_mode.upper()}</b>\n"
                    f"Score: <b>{e.total_score}%</b> | Regime: {fuzzy_str}\n"
                    f"POC: ${e.poc_level:.2f} | ATR: ${e.atr_value:.2f}\n"
                    f"<code>ENTRY: ${levels['entry_price']:.2f}\n"
                    f"SL   : ${levels['sl_price']:.2f}  (${levels['sl_distance_usd']:.2f})\n"
                    f"TP1  : ${levels['tp1_price']:.2f}\n"
                    f"TP2  : ${levels['tp2_price']:.2f}\n"
                    f"BE @ : ${levels['break_even_trigger']:.2f}</code>\n"
                    f"Lot: <b>{levels['lot_size']}</b> | "
                    f"Risk: ${levels['actual_risk_usd']:.0f} ({levels['risk_pct']:.2f}%)"
                    f"{confirm_hint}\n"
                    f"<i>Kelly: {safe_kelly}</i>")
        return self._post({"chat_id": self.cfg.telegram_chat_id, "text": text,
                            "parse_mode": "HTML",
                            "disable_web_page_preview": True})

    def send_heartbeat(self, stats: dict) -> bool:
        text = (f"💓 <b>XAUUSD Bot Alive</b>\n"
                f"Mode: <b>{self.cfg.trading_mode.upper()}</b>\n"
                f"Open: {stats.get('open_n', 0)} | "
                f"R today: {stats.get('today_r', 0):+.2f} | "
                f"PnL today: ${stats.get('today_pnl', 0):+.0f}\n"
                f"Kelly: {stats.get('kelly_pct', 0):.2f}% | "
                f"Data: {stats.get('source', '?')}")
        return self._post({"chat_id": self.cfg.telegram_chat_id, "text": text,
                            "parse_mode": "HTML"})

    def send_failure(self, err: str) -> None:
        if not is_market_open():
            return
        self._post({
            "chat_id": self.cfg.telegram_chat_id,
            "text": f"⚠️ <b>BOT ERROR</b> [<code>{RUN_ID}</code>]\n"
                    f"<code>{html.escape(err[:400])}</code>",
            "parse_mode": "HTML",
        })


# ============================================================================
# 15. ORCHESTRATOR
# ============================================================================
class TradingBotOrchestrator:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
        self.params, self.params_source = load_params(cfg)
        self.db = AuditDB(cfg.db_path)
        self.data = MarketDataHandler(cfg)
        self.news = NewsSafetyFilter(cfg.news_enabled)
        self.signal_gen = SignalGenerator(self.params, self.params_source)
        self.risk = RiskManagementEngine(cfg, self.db)
        self.outcomes = OutcomeTracker(self.db, cfg)
        self.tg = TelegramNotifier(cfg)

    def _maybe_heartbeat(self, source: str = "?") -> None:
        try:
            last = self.db.get_meta("last_heartbeat_ts")
            if last:
                age_h = (datetime.now(timezone.utc) -
                         datetime.fromisoformat(last).replace(tzinfo=timezone.utc)
                         ).total_seconds() / 3600
                if age_h < self.cfg.heartbeat_hours:
                    return
            stats = {
                "open_n": self.db.count_open_signals(),
                "today_r": self.db.get_today_r_sum(),
                "today_pnl": self.db.get_today_pnl_usd(),
                "kelly_pct": self.risk.kelly_risk_pct()[0],
                "source": source,
            }
            self.tg.send_heartbeat(stats)
            self.db.set_meta("last_heartbeat_ts",
                              _to_utc_iso(datetime.now(timezone.utc)))
            log_event(event="heartbeat_sent", source=source)
        except Exception as e:
            log_event(logging.WARNING, "heartbeat_fail", err=str(e)[:120])

    def run(self) -> int:
        log_event(event="bot_start", version=VERSION, mode=self.cfg.trading_mode,
                  params_source=self.params_source, params_loaded=self.params,
                  contract_size=self.cfg.contract_size,
                  supabase_enabled=SUPABASE.enabled,
                  warnings=self.cfg.warnings())

        if not is_market_open():
            log_event(event="market_closed_or_holiday", skip=True)
            print("ACTION: SKIP — market closed or holiday")
            return 0

        try:
            md = self.data.fetch()
        except Exception as ex:
            err = f"{type(ex).__name__}: {ex}"
            log_event(logging.ERROR, "fatal_data", err=err)
            self.tg.send_failure(err)
            print(f"ACTION: ERROR — {err}")
            return 1

        if self.cfg.trading_mode == "live":
            n_skip = self.db.auto_skip_stale(self.cfg.auto_confirm_hours)
            if n_skip:
                log_event(event="auto_skipped_stale", count=n_skip)

        try:
            n_closed = self.outcomes.process(md.h1)
            if n_closed:
                log_event(event="outcomes_closed", count=n_closed)
        except Exception as ex:
            log_event(logging.WARNING, "outcome_tracker_fail", err=str(ex)[:120])

        try:
            news = self.news.check()
            e = self.signal_gen.generate(md, news, self.cfg)
            basis = self.data.get_basis(md)
            tradable = float(md.h1["close"].iloc[-1]) + basis

            mult, cb_r, dd_r = self.risk.combined_multiplier()
            if mult == 0.0:
                log_event(event="risk_halt", cb=cb_r, dd=dd_r)
                self.tg.send_signal(e, tradable, None)
                print(f"ACTION: HALT — CB:{cb_r} | DD:{dd_r}")
                self._maybe_heartbeat(md.source)
                return 0

            exposure_ok, exposure_reason = self.risk.exposure_gate(e.action)
            if e.action in ("BUY", "SELL") and not exposure_ok:
                log_event(event="exposure_blocked", action=e.action,
                          reason=exposure_reason)
                print(f"ACTION: {e.action} BLOCKED — {exposure_reason}")
                self._maybe_heartbeat(md.source)
                return 0

            n_today = self.db.count_today_signals()
            if n_today >= self.cfg.max_daily_signals:
                log_event(event="daily_limit", n=n_today, max=self.cfg.max_daily_signals)
                print(f"ACTION: SKIP — daily limit ({n_today}/{self.cfg.max_daily_signals})")
                self._maybe_heartbeat(md.source)
                return 0

            kelly_base, kelly_reason = self.risk.kelly_risk_pct()
            kelly_final = round(kelly_base * mult, 2)
            if mult < 1.0:
                kelly_reason = f"{kelly_reason} | mult={mult:.2f} | CB:{cb_r} | DD:{dd_r}"

            levels = self.risk.compute_levels(
                e.action, tradable, e.atr_value, e.atr_multiplier,
                e.blended_params, kelly_final, kelly_reason)

            if levels and self.db.is_dup(self.cfg.signal_cooldown_min, e.action, tradable):
                log_event(event="signal_deduped", action=e.action, spot=round(tradable, 2))
                print(f"ACTION: {e.action} (deduped) | Score: {e.total_score}%")
                self._maybe_heartbeat(md.source)
                return 0

            signal_id = None
            if levels and e.action in ("BUY", "SELL"):
                signal_id = uuid.uuid4().hex
                max_hold_h = float(e.blended_params.get("max_hold", 48.0))
                self.db.record(signal_id, e, tradable, levels, max_hold_h,
                               self.cfg.trading_mode)
                try_insert_signal(
                    signal_id=signal_id, action=e.action, spot=tradable,
                    entry_price=levels["entry_price"],
                    sl=levels["sl_price"], tp1=levels["tp1_price"], tp2=levels["tp2_price"],
                    score=e.total_score, regime=e.regime,
                    kelly_frac=kelly_final / 100.0, lot=levels["lot_size"],
                    risk_usd=levels["actual_risk_usd"], max_hold_hours=max_hold_h,
                    notes="; ".join(e.notes), trading_mode=self.cfg.trading_mode)

            sent = self.tg.send_signal(e, tradable, levels, signal_id=signal_id)

            log_event(event="signal_emitted", action=e.action, score=e.total_score,
                      spot=round(tradable, 2), source=md.source,
                      regime=e.regime, kelly_base=kelly_base, kelly_final=kelly_final,
                      cb=cb_r, dd=dd_r, mult=mult, signal_id=signal_id, tg=sent,
                      n_today=n_today, mode=self.cfg.trading_mode,
                      td_remaining=self.data.td_remaining)
            print(f"ACTION: {e.action} | Score: {e.total_score}% | "
                  f"Regime: {e.regime} | Kelly: {kelly_final:.2f}% (×{mult}) | "
                  f"Spot: {tradable:.2f} | TG: {sent} | CB: {cb_r} | DD: {dd_r} | "
                  f"Today: {n_today}/{self.cfg.max_daily_signals} | Mode: {self.cfg.trading_mode}")
            self._maybe_heartbeat(md.source)
            return 0
        except Exception as ex:
            err = f"{type(ex).__name__}: {ex}"
            log_event(logging.ERROR, "fatal_engine", err=err)
            self.tg.send_failure(err)
            print(f"ACTION: ERROR — {err}")
            return 1


# ============================================================================
# 16. MAIN
# ============================================================================
def main() -> int:
    try:
        cfg = BotConfig.from_env()
    except ValueError as e:
        log_event(logging.ERROR, "config_invalid", err=str(e))
        print(f"ACTION: CONFIG ERROR — {e}")
        return 1
    return TradingBotOrchestrator(cfg).run()


if __name__ == "__main__":
    sys.exit(main())
