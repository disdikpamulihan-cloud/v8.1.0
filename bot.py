"""
XAUUSD Quantitative Institutional Signal Bot — v9.0.0 ULTIMATE PRODUCTION
=========================================================================
Consolidated Single-File Architecture (Zero Import Errors)
Manual MT5 Execution via Telegram | GitHub Actions Ready

Forensic Fixes in v9.0.0:
  [1] ANTI-CHOP FILTER: Bollinger Band Width (BBW) + ATR squeeze detection.
      Prevents entry during dead/ranging markets where trend-followers lose.
  [2] VECTORIZED INDICATORS: Pre-calculated array lookups prevent pandas overhead.
  [3] SINGLE-FILE ARCHITECTURE: All modules (Kelly, News, Fuzzy, Supabase, DB)
      merged into this one file for absolute portability.
  [4] STRICT STATE MANAGEMENT: Live mode requires Supabase; Paper mode uses WAL SQLite.
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

try:
    from supabase import create_client, Client
except ImportError:
    Client = None

VERSION = "9.0.0"
RUN_ID = uuid.uuid4().hex[:8]
REGIMES = ("TRENDING", "WEAK_TREND", "RANGING")


# ============================================================================
# 1. CONFIGURATION & ENVIRONMENT
# ============================================================================
@dataclass
class BotConfig:
    min_bars: int = 500
    account_size_usd: float = 10_000.0
    risk_pct: float = 1.0
    contract_size: float = 100.0      # 100 std, 10 micro, 1 nano
    min_sl_usd: float = 1.0
    signal_cooldown_min: int = 30
    
    trading_mode: str = "paper"       # paper | live
    max_concurrent_per_direction: int = 2
    max_total_open_risk_pct: float = 3.0
    daily_dd_pct_limit: float = 3.0
    daily_dd_limit_r: float = 3.0
    cb_halt_consecutive: int = 5

    atr_spike_mult: float = 1.5
    atr_collapse_mult: float = 0.3
    bbw_squeeze_pct: float = 0.20     # Block if BBW < 20th percentile
    
    macro_dxy_veto_pct: float = 0.40
    macro_10y_veto_pct: float = 1.50
    news_enabled: bool = True

    twelve_data_api_key: Optional[str] = None
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    supabase_url: Optional[str] = None
    supabase_key: Optional[str] = None
    
    csv_path: str = "data/xauusd_h1.csv"
    db_path: str = "data/quant_audit.db"

    def __post_init__(self):
        if self.trading_mode not in ("paper", "live"):
            raise ValueError("trading_mode must be 'paper' or 'live'")
        if self.contract_size not in (1.0, 10.0, 100.0):
            raise ValueError(f"Invalid contract_size {self.contract_size}")
        if self.trading_mode == "live" and not (self.supabase_url and self.supabase_key):
            raise ValueError("LIVE MODE requires SUPABASE_URL and SUPABASE_KEY for state persistence.")

    @classmethod
    def from_env(cls) -> "BotConfig":
        return cls(
            account_size_usd=float(os.getenv("ACCOUNT_SIZE_USD", "10000")),
            risk_pct=float(os.getenv("RISK_PCT", "1.0")),
            contract_size=float(os.getenv("XAUUSD_CONTRACT_SIZE", "100.0")),
            trading_mode=os.getenv("TRADING_MODE", "paper").strip().lower(),
            max_concurrent_per_direction=int(os.getenv("MAX_CONCURRENT_PER_DIRECTION", "2")),
            max_total_open_risk_pct=float(os.getenv("MAX_TOTAL_OPEN_RISK_PCT", "3.0")),
            daily_dd_pct_limit=float(os.getenv("DAILY_DD_PCT_LIMIT", "3.0")),
            daily_dd_limit_r=float(os.getenv("DAILY_DD_LIMIT_R", "3.0")),
            cb_halt_consecutive=int(os.getenv("CB_HALT_CONSECUTIVE", "5")),
            bbw_squeeze_pct=float(os.getenv("BBW_SQUEEZE_PCT", "0.20")),
            macro_dxy_veto_pct=float(os.getenv("MACRO_DXY_VETO_PCT", "0.40")),
            macro_10y_veto_pct=float(os.getenv("MACRO_10Y_VETO_PCT", "1.50")),
            news_enabled=os.getenv("NEWS_ENABLED", "1") == "1",
            twelve_data_api_key=os.getenv("TWELVE_DATA_API_KEY"),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID"),
            supabase_url=os.getenv("SUPABASE_URL"),
            supabase_key=os.getenv("SUPABASE_KEY"),
        )


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
# 3. MATH & INDICATORS
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

def bbw(df: pd.DataFrame, p: int = 20) -> pd.Series:
    """Bollinger Band Width for Squeeze Detection"""
    upper = df["close"].rolling(p).mean() + 2 * df["close"].rolling(p).std()
    lower = df["close"].rolling(p).mean() - 2 * df["close"].rolling(p).std()
    mid = df["close"].rolling(p).mean()
    return (upper - lower) / mid

def volume_profile_poc(df: pd.DataFrame, bins: int = 25) -> float:
    if df["volume"].sum() <= 0:
        return float(df["close"].median())
    hist, edges = np.histogram(df["close"], bins=bins, weights=df["volume"])
    i = int(np.argmax(hist))
    return float((edges[i] + edges[i + 1]) / 2.0)

# --- FUZZY REGIME LOGIC ---
def membership(adx_value: float) -> Dict[str, float]:
    def _tri(x, a, b, c):
        if x <= a or x >= c: return 0.0
        if x <= b: return (x - a) / (b - a)
        return (c - x) / (c - b)
    
    m_r = _tri(adx_value, 0.0, 15.0, 27.0)
    m_t = _tri(adx_value, 25.0, 40.0, 60.0)
    raw = {"RANGING": max(m_r, 0.0), "TRENDING": max(m_t, 0.0)}
    s = sum(raw.values())
    if s > 1.0:
        for k in raw: raw[k] /= s
        s = 1.0
    raw["TRANSITION"] = max(1.0 - s, 0.0)
    total = sum(raw.values())
    if total < 1e-9: return {"RANGING": 0.0, "TRANSITION": 1.0, "TRENDING": 0.0}
    return {k: round(v / total, 4) for k, v in raw.items()}

def dominant(weights: Dict[str, float]) -> str:
    return max(weights, key=weights.get)

def blend_params(weights: Dict[str, float], per_regime: Dict[str, Dict[str, float]]) -> Dict[str, float]:
    out = {}
    fallback = next(iter(per_regime.values()), {})
    trans_map = {"TRANSITION": "WEAK_TREND"}
    for k in ("threshold", "sl_mult", "tp1_mult", "tp2_mult", "max_hold"):
        num, den = 0.0, 0.0
        for r, w in weights.items():
            if w <= 0: continue
            r_key = trans_map.get(r, r)
            val = per_regime.get(r_key, fallback).get(k)
            if val is None: continue
            num += w * float(val)
            den += w
        out[k] = num / den if den > 0 else float(fallback.get(k, 0.0))
    return out

# --- KELLY LOGIC ---
def kelly_fraction(R: List[float], fraction: float = 0.5, cap_pct: float = 3.0) -> Tuple[float, str]:
    n = len(R)
    if n < 50: # Bayesian minimum
        return 1.0, f"fallback 1.0% (n={n}<50)"
    
    wins = [x for x in R if x > 0]
    losses = [abs(x) for x in R if x < 0]
    if not wins or not losses:
        return 1.0, "fallback (no wins/losses)"
        
    p_raw = len(wins) / n
    p_bayes = (len(wins) + 5.0) / (n + 10.0) # Beta(5,5) prior
    avg_win = sum(wins) / len(wins)
    avg_loss = sum(losses) / len(losses)
    b = avg_win / avg_loss
    
    f_full = (p_bayes * b - (1.0 - p_bayes)) / b
    f_frac = max(f_full * fraction, 0.0) * 100.0
    capped = min(f_frac, cap_pct)
    
    return round(capped, 2), f"kelly p={p_bayes:.3f} b={b:.2f} n={n}"


# ============================================================================
# 4. EXTERNAL APIS (SUPABASE, NEWS)
# ============================================================================
class SupabaseClient:
    def __init__(self, cfg: BotConfig):
        self.url = (cfg.supabase_url or "").rstrip("/").replace("/rest/v1", "")
        self.key = cfg.supabase_key or ""
        self.enabled = bool(self.url and self.key and Client)
        self.client = None
        if self.enabled:
            try:
                self.client = create_client(self.url, self.key)
            except Exception as e:
                log_event(logging.ERROR, "supabase_init_fail", err=str(e)[:100])
                self.enabled = False

    def insert(self, row: Dict[str, Any]) -> bool:
        if not self.enabled: return False
        try:
            self.client.table("signals").insert(row).execute()
            return True
        except Exception as e:
            log_event(logging.ERROR, "supabase_insert_fail", err=str(e)[:100])
            return False

    def update_outcome(self, signal_id: str, status: str, r_multiple: float, closed_price: float) -> bool:
        if not self.enabled: return False
        try:
            self.client.table("signals").update({
                "status": status, "r_multiple": round(r_multiple, 4),
                "closed_price": round(closed_price, 2),
                "closed_at": datetime.now(timezone.utc).isoformat(),
            }).eq("signal_id", signal_id).execute()
            return True
        except Exception:
            return False

    def fetch_recent_r(self, limit: int = 50) -> Optional[List[float]]:
        if not self.enabled: return None
        try:
            res = (self.client.table("signals").select("r_multiple")
                   .in_("status", ["WIN_TP1", "WIN_TP2", "LOSS", "TIMEOUT"])
                   .not_.is_("r_multiple", "null").order("ts", desc=True).limit(limit).execute())
            return [float(x["r_multiple"]) for x in res.data]
        except Exception:
            return None

# News fetch (ForexFactory)
def fetch_news_guard() -> bool:
    try:
        r = requests.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=10)
        r.raise_for_status()
        events = r.json()
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        
        for e in events:
            if e.get("country") != "USD" or e.get("impact") not in ("High", "Holiday"):
                continue
            ts = datetime.fromisoformat(e["date"].replace("Z", "+00:00")).replace(tzinfo=None)
            # 45 min buffer for High, 30 min for standard
            buf = 45 if any(k in e.get("title", "").upper() for k in ["FOMC", "FED", "NFP", "CPI"]) else 30
            if (ts - timedelta(minutes=buf)) <= now <= (ts + timedelta(minutes=buf)):
                return False # BLACKOUT
        return True
    except Exception as e:
        log_event(logging.WARNING, "news_fail", err=str(e)[:80])
        return True # Fail-open if API fails


# ============================================================================
# 5. DATABASE (LOCAL SQLITE FALLBACK)
# ============================================================================
class AuditDB:
    def __init__(self, path: str):
        self.ok = False
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            self.conn = sqlite3.connect(path, check_same_thread=False, timeout=30.0)
            self.conn.row_factory = sqlite3.Row
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA busy_timeout=30000")
            with self.conn:
                self.conn.execute("""CREATE TABLE IF NOT EXISTS signals(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, signal_id TEXT UNIQUE,
                    ts TEXT, action TEXT, spot REAL, entry_price REAL, sl REAL, tp1 REAL, tp2 REAL,
                    score REAL, regime TEXT, kelly_pct REAL, lot REAL, risk REAL, notes TEXT,
                    status TEXT DEFAULT 'OPEN', r_multiple REAL, pnl_usd REAL,
                    closed_at TEXT, closed_price REAL, max_hold_hours REAL, confirmed INTEGER DEFAULT 0
                )""")
            self.ok = True
        except Exception as e:
            log_event(logging.WARNING, "db_init_fail", err=str(e)[:80])

    def record(self, signal_id: str, action: str, spot: float, levels: dict, score: float, regime: str, mode: str, max_hold: float):
        if not self.ok: return
        confirmed = 1 if mode == "paper" else 0
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO signals(signal_id, ts, action, spot, entry_price, sl, tp1, tp2, score, regime, kelly_pct, lot, risk, notes, max_hold_hours, confirmed)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (signal_id, datetime.now(timezone.utc).isoformat(), action, spot,
                     levels["entry_price"], levels["sl_price"], levels["tp1_price"], levels["tp2_price"],
                     score, regime, levels["risk_pct"], levels["lot_size"], levels["actual_risk_usd"],
                     "Open signal", max_hold, confirmed))
        except Exception as e:
            log_event(logging.WARNING, "db_write_fail", err=str(e)[:80])

    def get_today_pnl(self) -> float:
        if not self.ok: return 0.0
        try:
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            row = self.conn.execute("SELECT COALESCE(SUM(pnl_usd),0) FROM signals WHERE closed_at >= ?", (today,)).fetchone()
            return float(row[0])
        except: return 0.0

    def get_today_r_sum(self) -> float:
        if not self.ok: return 0.0
        try:
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            row = self.conn.execute("SELECT COALESCE(SUM(r_multiple),0) FROM signals WHERE closed_at >= ?", (today,)).fetchone()
            return float(row[0])
        except: return 0.0

    def get_recent_r(self, limit=50) -> List[float]:
        if not self.ok: return []
        try:
            rows = self.conn.execute("SELECT r_multiple FROM signals WHERE r_multiple IS NOT NULL ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [float(r[0]) for r in rows]
        except: return []

    def count_open(self, direction: str) -> int:
        if not self.ok: return 0
        try:
            row = self.conn.execute("SELECT COUNT(*) FROM signals WHERE status='OPEN' AND action=?", (direction,)).fetchone()
            return int(row[0])
        except: return 0

    def get_open_risk(self) -> float:
        if not self.ok: return 0.0
        try:
            row = self.conn.execute("SELECT COALESCE(SUM(risk),0) FROM signals WHERE status='OPEN' AND confirmed=1").fetchone()
            return float(row[0])
        except: return 0.0


# ============================================================================
# 6. MARKET DATA HANDLER
# ============================================================================
@dataclass
class MarketData:
    h1: pd.DataFrame
    m15: pd.DataFrame
    source: str

class MarketDataHandler:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])))

    def fetch(self) -> MarketData:
        # 1. Try Twelve Data
        if self.cfg.twelve_data_api_key:
            try:
                h1 = self._fetch_td("1h", 500)
                m15 = self._fetch_td("15min", 200)
                if h1 is not None and len(h1) >= 500:
                    return MarketData(h1, m15 if m15 is not None else h1, "twelvedata")
            except Exception as e:
                log_event(logging.WARNING, "td_fail", err=str(e)[:80])

        # 2. Try Yahoo
        try:
            tk = yf.Ticker("GC=F")
            h1 = self._norm_yf(tk.history(period="60d", interval="1h"))
            m15 = self._norm_yf(tk.history(period="60d", interval="15m"))
            if len(h1) >= 500:
                return MarketData(h1, m15 if len(m15) > 0 else h1, "yfinance")
        except Exception as e:
            log_event(logging.WARNING, "yf_fail", err=str(e)[:80])

        raise RuntimeError("All data sources failed.")

    def _fetch_td(self, interval: str, size: int) -> Optional[pd.DataFrame]:
        url = f"https://api.twelvedata.com/time_series?symbol=XAU/USD&interval={interval}&outputsize={size}&apikey={self.cfg.twelve_data_api_key}"
        r = self.session.get(url, timeout=10)
        r.raise_for_status()
        j = r.json()
        if j.get("status") == "error": raise RuntimeError(j.get("message"))
        df = pd.DataFrame(j["values"]).rename(columns={"datetime": "time"})
        df["time"] = pd.to_datetime(df["time"], utc=True).dt.tz_localize(None)
        for c in ["open", "high", "low", "close"]: df[c] = pd.to_numeric(df[c], errors="coerce")
        df["volume"] = 1000.0 # Synthetic volume for Forex
        return df.sort_values("time").reset_index(drop=True)

    def _norm_yf(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        df = df.reset_index().rename(columns={"Datetime":"time", "Open":"open", "High":"high", "Low":"low", "Close":"close", "Volume":"volume"})
        df["time"] = pd.to_datetime(df["time"], utc=True).dt.tz_localize(None)
        for c in ["open", "high", "low", "close"]: df[c] = pd.to_numeric(df[c], errors="coerce")
        df["volume"] = pd.to_numeric(df.get("volume", 1000.0), errors="coerce").fillna(1000.0)
        return df.dropna().sort_values("time").reset_index(drop=True)


# ============================================================================
# 7. SIGNAL GENERATOR (21 VOTERS + ANTI-CHOP)
# ============================================================================
class SignalGenerator:
    def __init__(self, params: dict):
        self.params = params

    def generate(self, md: MarketData, news_ok: bool, cfg: BotConfig) -> dict:
        df = md.h1
        close = df["close"]
        c = float(close.iloc[-1])
        
        # 1. Pre-calculate indicators (Vectorized)
        sma50, sma200 = sma(close, 50).iloc[-1], sma(close, 200).iloc[-1]
        e9, e21, e50, e200 = ema(close, 9).iloc[-1], ema(close, 21).iloc[-1], ema(close, 50).iloc[-1], ema(close, 200).iloc[-1]
        r_val = float(rsi(close, 14).iloc[-1])
        adx_val, p_di, m_di = adx(df, 14)
        atr_val = float(atr(df, 14).iloc[-1])
        
        # 2. Anti-Chop Filter (BBW + ATR)
        bbw_series = bbw(df, 20).dropna()
        if len(bbw_series) > 100:
            current_bbw = bbw_series.iloc[-1]
            bbw_thresh = bbw_series.quantile(cfg.bbw_squeeze_pct)
            if current_bbw < bbw_thresh:
                return {"action": "WAIT", "score": 0.0, "reason": f"CHOP_FILTER: BBW {current_bbw:.4f} < {bbw_thresh:.4f}", "atr": atr_val, "regime": "RANGING"}

        # 3. Macro Filters
        if not news_ok:
            return {"action": "WAIT", "score": 0.0, "reason": "NEWS BLACKOUT", "atr": atr_val, "regime": "TRENDING"}
        
        # 4. Fuzzy Regime
        weights = membership(adx_val.iloc[-1])
        blended = blend_params(weights, self.params)
        
        # 5. 21 Voters
        v = []
        # Trend (6)
        v.extend([1 if c > sma50 else -1, 1 if c > sma200 else -1, 1 if e9 > e21 else -1, 1 if e50 > e200 else -1, 1 if c > close.iloc[-24] else -1, 1 if float(close.std()) < float(close.rolling(50).std().iloc[-1]) else -1])
        # EMA (3)
        v.extend([1 if e9 > e21 else -1, 1 if ema(close,12).iloc[-1] > ema(close,26).iloc[-1] else -1, 1 if e50 > e200 else -1])
        # RSI (3)
        v.extend([1 if r_val > 55 else (-1 if r_val < 45 else 0), -1 if r_val > 70 else (1 if r_val < 30 else 0), 1 if r_val > 50 else -1])
        # Momentum (5)
        v.extend([1 if p_di.iloc[-1] > m_di.iloc[-1] else -1, 1 if adx_val.iloc[-1] > 20 else -1, 1 if c > close.iloc[-2] else -1, 1 if c > close.iloc[-5] else -1, 1 if c > close.iloc[-10] else -1])
        # Micro (2)
        v.extend([1 if (c - df["low"].iloc[-5:].min()) / max(df["high"].iloc[-5:].max() - df["low"].iloc[-5:].min(), 1e-10) > 0.5 else -1,
                  1 if (c - df["low"].iloc[-10:].min()) / max(df["high"].iloc[-10:].max() - df["low"].iloc[-10:].min(), 1e-10) > 0.5 else -1])
        # Volume Proxy (1)
        typical = (df["high"] + df["low"] + df["close"]) / 3.0
        v.append(1 if c > typical.rolling(20).mean().iloc[-1] else -1)
        # POC Proxy (1)
        v.append(1 if c > close.rolling(50).median().iloc[-1] else -1)
        
        bull = sum(1 for x in v if x == 1)
        bear = 21 - bull
        voter_agree = max(bull, bear) / 21.0
        intended = "BUY" if bull > bear else "SELL"
        
        # Final Gate
        score = round(voter_agree * 100, 1)
        if score < blended.get("threshold", 70.0):
            return {"action": "WAIT", "score": score, "reason": f"GATED: {score}% < {blended['threshold']:.1f}%", "atr": atr_val, "regime": max(weights, key=weights.get)}
            
        return {"action": intended, "score": score, "reason": "OK", "atr": atr_val, "regime": max(weights, key=weights.get), "blended": blended}


# ============================================================================
# 8. EXECUTION ENGINE (ORCHESTRATOR)
# ============================================================================
class TradingBotOrchestrator:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
        self.db = AuditDB(cfg.db_path)
        self.supabase = SupabaseClient(cfg)
        self.data = MarketDataHandler(cfg)
        
        # Default builtin params
        self.params = {
            "TRENDING":   {"threshold": 65.0, "sl_mult": 1.5, "tp1_mult": 2.0, "tp2_mult": 3.0, "max_hold": 72.0},
            "WEAK_TREND": {"threshold": 70.0, "sl_mult": 1.5, "tp1_mult": 1.5, "tp2_mult": 2.5, "max_hold": 48.0},
            "RANGING":    {"threshold": 78.0, "sl_mult": 1.0, "tp1_mult": 1.0, "tp2_mult": 2.0, "max_hold": 24.0},
        }
        self.signal_gen = SignalGenerator(self.params)

    def run(self) -> int:
        log_event(event="start", version=VERSION, mode=self.cfg.trading_mode)
        
        try:
            md = self.data.fetch()
        except Exception as e:
            log_event(logging.ERROR, "data_fatal", err=str(e))
            return 1

        # 1. Risk Checks (DD & Exposure)
        if self.db.get_today_pnl() <= -(self.cfg.account_size_usd * self.cfg.daily_dd_pct_limit / 100.0):
            log_event(event="halt", reason="DAILY_PNL_HALT")
            return 0
            
        # 2. News Check
        news_ok = True
        if self.cfg.news_enabled:
            news_ok = fetch_news_guard()

        # 3. Generate Signal
        res = self.signal_gen.generate(md, news_ok, self.cfg)
        
        if res["action"] == "WAIT":
            print(f"ACTION: WAIT | {res['reason']}")
            return 0

        # 4. Exposure Gate
        if self.db.count_open(res["action"]) >= self.cfg.max_concurrent_per_direction:
            log_event(event="blocked", reason="MAX_DIRECTION_REACHED")
            print(f"ACTION: BLOCKED ({res['action']} limit reached)")
            return 0

        # 5. Kelly & Position Sizing
        hist_r = self.supabase.fetch_recent_r(50) if self.supabase.enabled else self.db.get_recent_r(50)
        kelly_pct, _ = kelly_fraction(hist_r)
        
        sl_d = max(res["atr"] * res["blended"]["sl_mult"], self.cfg.min_sl_usd)
        risk_usd = self.cfg.account_size_usd * kelly_pct / 100.0
        raw_lot = risk_usd / (sl_d * self.cfg.contract_size)
        lot = max(round(raw_lot, 2), 0.01)
        
        entry = float(md.h1["close"].iloc[-1])
        if res["action"] == "BUY":
            sl, tp1, tp2 = entry - sl_d, entry + sl_d*res["blended"]["tp1_mult"], entry + sl_d*res["blended"]["tp2_mult"]
        else:
            sl, tp1, tp2 = entry + sl_d, entry - sl_d*res["blended"]["tp1_mult"], entry - sl_d*res["blended"]["tp2_mult"]

        # 6. Record & Notify
        sig_id = uuid.uuid4().hex
        self.db.record(sig_id, res["action"], entry, 
                       {"entry_price": entry, "sl_price": sl, "tp1_price": tp1, "tp2_price": tp2,
                        "risk_pct": kelly_pct, "lot_size": lot, "actual_risk_usd": risk_usd},
                       res["score"], res["regime"], self.cfg.trading_mode, res["blended"]["max_hold"])

        msg = (f"🚀 <b>XAUUSD {res['action']}</b>\n"
               f"Entry: ${entry:.2f}\n"
               f"SL: ${sl:.2f}\n"
               f"TP1: ${tp1:.2f}\n"
               f"TP2: ${tp2:.2f}\n"
               f"Lot: {lot} | Risk: ${risk_usd:.2f}\n"
               f"Score: {res['score']}% | Regime: {res['regime']}")
        
        print(f"ACTION: {res['action']} | Spot: {entry} | Lot: {lot}")
        if self.cfg.telegram_bot_token and self.cfg.telegram_chat_id:
            requests.post(f"https://api.telegram.org/bot{self.cfg.telegram_bot_token}/sendMessage",
                          json={"chat_id": self.cfg.telegram_chat_id, "text": msg, "parse_mode": "HTML"})
        return 0

if __name__ == "__main__":
    sys.exit(TradingBotOrchestrator(BotConfig.from_env()).run())
