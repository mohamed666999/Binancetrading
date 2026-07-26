#!/usr/bin/env python3
"""
============================================================
  AI TRADING BOT V7 - Dynamic Scanner + Smart Scoring

  Architecture:
  ┌────────────────────────────────────────────────────────┐
  │  MarketScanner (كل 5 دقائق)                           │
  │    Phase 1: Quick Scan → Top 5                        │
  │    Phase 2: Deep Analysis → Signal                    │
  │         ↓                                             │
  │  TrendEngine (Direction + Entry Quality)              │
  │         ↓                                             │
  │  SignalEngine (Score 0-10, Pullback-aware)            │
  │         ↓                                             │
  │  AIAnalyst (شرح فقط)                                  │
  │         ↓                                             │
  │  ExecutionEngine (Lock + SL Mandatory)                │
  │         ↓                                             │
  │  PositionMonitor (Real PnL + Order Status)            │
  └────────────────────────────────────────────────────────┘
============================================================
"""

import asyncio
import json
import time
import threading
import math
import os
import sqlite3
import logging
from collections import deque
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Set
from enum import Enum

import websockets
import ccxt
import requests
from flask import Flask, jsonify
from openai import OpenAI


# ============================================================
#  0. LOGGING
# ============================================================

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)-18s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            f"{LOG_DIR}/bot_{datetime.now():%Y%m%d}.log", encoding="utf-8"
        ),
    ],
)
logger = logging.getLogger("TradingBot")


# ============================================================
#  1. CONFIGURATION
# ============================================================

@dataclass
class Config:
    # --- Binance ---
    binance_api_key: str = "IX7kLH0ssWHP5TpYMUGcp0pzq4LX4Lqi7m4XtlqMkkq6DCZAsLhoeYZ3533jJFF4"
    binance_secret: str = "LmICnpSpMxL1riv4RfIf0HBGRfhDTP5JhDUYdlPSukpqV7kDTonrZ0j3DWp1a7hU"

    # --- NVIDIA / DeepSeek ---
    nvidia_api_key: str = "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"
    ai_model: str = "deepseek-ai/deepseek-v4-pro"
    ai_temperature: float = 0.0
    ai_max_tokens: int = 400

    # --- Trading ---
    dry_run: bool = True
    leverage: int = 10
    margin_usdt: float = 10.0
    max_daily_trades: int = 8
    max_open_positions: int = 2
    cooldown_seconds: int = 180

    # --- Risk ---
    max_sl_percent: float = 5.0
    max_tp_percent: float = 10.0

    # --- V7: Scoring ---
    min_score_to_enter: int = 5
    rsi_extreme_overbought: float = 82.0
    rsi_extreme_oversold: float = 18.0
    rsi_caution_overbought: float = 75.0
    rsi_caution_oversold: float = 25.0
    adx_threshold: float = 18.0
    adx_strong: float = 25.0
    stoch_overbought: float = 80.0
    stoch_oversold: float = 20.0

    # --- V7: Scanner ---
    scanner_interval: int = 300
    scanner_top_n: int = 5
    scanner_min_volume_usdt: float = 5_000_000
    scanner_min_atr_pct: float = 0.5
    scanner_max_spread_pct: float = 0.5

    # --- Position Monitor ---
    monitor_interval: int = 15

    # --- WebSocket ---
    ws_ping_interval: int = 20
    ws_ping_timeout: int = 20
    ws_reconnect_delay: int = 10
    candle_maxlen: int = 500

    # --- Server ---
    flask_port: int = 8080

    # --- Watchlist ---
    watchlist: Dict[str, str] = field(default_factory=lambda: {
        "btcusdt":      "BTC/USDT:USDT",
        "ethusdt":      "ETH/USDT:USDT",
        "solusdt":      "SOL/USDT:USDT",
        "bnbusdt":      "BNB/USDT:USDT",
        "xrpusdt":      "XRP/USDT:USDT",
        "adausdt":      "ADA/USDT:USDT",
        "linkusdt":     "LINK/USDT:USDT",
        "avaxusdt":     "AVAX/USDT:USDT",
        "dogeusdt":     "DOGE/USDT:USDT",
        "wifusdt":      "WIF/USDT:USDT",
        "1000pepeusdt": "1000PEPE/USDT:USDT",
        "suiusdt":      "SUI/USDT:USDT",
        "aaveusdt":     "AAVE/USDT:USDT",
        "nearusdt":     "NEAR/USDT:USDT",
        "arbusdt":      "ARB/USDT:USDT",
        "dotusdt":      "DOT/USDT:USDT",
        "maticusdt":    "MATIC/USDT:USDT",
        "ltcusdt":      "LTC/USDT:USDT",
        "aptusdt":      "APT/USDT:USDT",
        "opustdt":      "OP/USDT:USDT",
    })

    timeframes: List[str] = field(default_factory=lambda: ["1m", "1h", "1d"])
    db_path: str = "trades.db"

    def validate(self):
        if not self.binance_api_key or not self.binance_secret:
            raise ValueError("Binance API keys missing")
        if not self.nvidia_api_key:
            raise ValueError("NVIDIA API key missing")


CFG = Config()


# ============================================================
#  2. ENUMS & DATA CLASSES
# ============================================================

class Decision(Enum):
    BUY  = "BUY"
    SELL = "SELL"
    WAIT = "WAIT"


class TrendDirection(Enum):
    BULLISH  = "BULLISH"
    BEARISH  = "BEARISH"
    NEUTRAL  = "NEUTRAL"
    CONFLICT = "CONFLICT"


class EntryQuality(Enum):
    ALLOWED = "ALLOWED"
    CAUTION = "CAUTION"
    BLOCKED = "BLOCKED"


@dataclass
class ScannerResult:
    """نتيجة فحص عملة واحدة"""
    symbol_key: str = ""
    symbol: str = ""
    score: float = 0.0
    volume_usdt: float = 0.0
    atr_pct: float = 0.0
    change_1h_pct: float = 0.0
    trend_1h: str = ""
    rsi_1h: float = 50.0
    reasons: List[str] = field(default_factory=list)


@dataclass
class SignalResult:
    decision: Decision = Decision.WAIT
    buy_score: int = 0
    sell_score: int = 0
    max_score: int = 10
    is_pullback: bool = False
    signals: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)
    sl_percent: float = 2.0
    tp_percent: float = 4.0


@dataclass
class AIAnalysis:
    regime: str = "unknown"
    explanation: str = ""
    risk_warnings: List[str] = field(default_factory=list)
    agreement: bool = True
    raw_response: str = ""


@dataclass
class TrendConfirmation:
    direction: TrendDirection = TrendDirection.NEUTRAL
    entry_quality: EntryQuality = EntryQuality.BLOCKED
    daily_trend: str = ""
    hourly_trend: str = ""
    minute_timing: str = ""
    strength: float = 0.0
    reasons: List[str] = field(default_factory=list)


@dataclass
class TradeRecord:
    symbol: str = ""
    side: str = ""
    entry_price: float = 0.0
    quantity: float = 0.0
    sl_price: float = 0.0
    tp_price: float = 0.0
    sl_order_id: str = ""
    tp_order_id: str = ""
    entry_order_id: str = ""
    confidence: float = 0.0
    reason: str = ""
    timestamp: str = ""
    status: str = "OPEN"
    mode: str = "PAPER"


# ============================================================
#  3. DATABASE
# ============================================================

class TradeDB:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        self._create_tables()

    def _create_tables(self):
        with self.lock:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol        TEXT NOT NULL,
                    side          TEXT NOT NULL,
                    mode          TEXT DEFAULT 'PAPER',
                    entry_price   REAL,
                    quantity      REAL,
                    sl_price      REAL,
                    tp_price      REAL,
                    sl_order_id   TEXT,
                    tp_order_id   TEXT,
                    entry_order_id TEXT,
                    confidence    REAL,
                    reason        TEXT,
                    timestamp     TEXT,
                    status        TEXT DEFAULT 'OPEN',
                    exit_price    REAL,
                    realized_pnl  REAL,
                    pnl_percent   REAL,
                    commission    REAL DEFAULT 0,
                    closed_at     TEXT,
                    close_reason  TEXT
                )
            """)
            self.conn.commit()

    def insert_trade(self, t: TradeRecord) -> int:
        with self.lock:
            cur = self.conn.execute(
                """INSERT INTO trades
                   (symbol,side,mode,entry_price,quantity,sl_price,tp_price,
                    sl_order_id,tp_order_id,entry_order_id,
                    confidence,reason,timestamp,status)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (t.symbol, t.side, t.mode, t.entry_price, t.quantity,
                 t.sl_price, t.tp_price, t.sl_order_id, t.tp_order_id,
                 t.entry_order_id, t.confidence, t.reason,
                 t.timestamp, t.status),
            )
            self.conn.commit()
            return cur.lastrowid

    def close_trade(self, trade_id, exit_price, realized_pnl,
                    pnl_pct, commission, reason):
        with self.lock:
            self.conn.execute(
                """UPDATE trades SET
                   status='CLOSED', exit_price=?, realized_pnl=?,
                   pnl_percent=?, commission=?, closed_at=?, close_reason=?
                   WHERE id=?""",
                (exit_price, realized_pnl, pnl_pct, commission,
                 datetime.now(timezone.utc).isoformat(), reason, trade_id),
            )
            self.conn.commit()

    def mark_emergency(self, trade_id, reason):
        with self.lock:
            self.conn.execute(
                """UPDATE trades SET status='EMERGENCY_CLOSED',
                   closed_at=?, close_reason=? WHERE id=?""",
                (datetime.now(timezone.utc).isoformat(), reason, trade_id),
            )
            self.conn.commit()

    def get_open_trades(self) -> List[dict]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM trades WHERE status='OPEN'"
            ).fetchall()
            cols = [d[0] for d in self.conn.execute(
                "SELECT * FROM trades LIMIT 0").description]
        return [dict(zip(cols, r)) for r in rows]

    def count_today_trades(self) -> int:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self.lock:
            row = self.conn.execute(
                """SELECT COUNT(*) FROM trades
                   WHERE timestamp LIKE ? AND mode='LIVE'""",
                (f"{today}%",),
            ).fetchone()
        return row[0] if row else 0

    def get_open_count(self) -> int:
        with self.lock:
            row = self.conn.execute(
                "SELECT COUNT(*) FROM trades WHERE status='OPEN' AND mode='LIVE'"
            ).fetchone()
        return row[0] if row else 0


db = TradeDB(CFG.db_path)


# ============================================================
#  4. FLASK
# ============================================================

app = Flask(__name__)
bot_stats = {
    "status": "STARTING", "version": "V7",
    "uptime": 0, "trades_today": 0, "open_positions": 0,
    "scanner_candidates": [], "last_analysis": {},
    "errors": 0, "mode": "PAPER" if CFG.dry_run else "LIVE",
}
START_TIME = time.time()


@app.route("/")
def home():
    return f"AI TRADING BOT V7 | {'PAPER' if CFG.dry_run else 'LIVE'}"


@app.route("/health")
def health():
    bot_stats["uptime"] = int(time.time() - START_TIME)
    bot_stats["trades_today"] = db.count_today_trades()
    bot_stats["open_positions"] = db.get_open_count()
    return jsonify(bot_stats)


def run_server():
    app.run(host="0.0.0.0", port=CFG.flask_port,
            debug=False, use_reloader=False)


# ============================================================
#  5. EXCHANGE + AI
# ============================================================

exchange = ccxt.binance({
    "apiKey": CFG.binance_api_key,
    "secret": CFG.binance_secret,
    "enableRateLimit": True,
    "options": {"defaultType": "swap", "adjustForTimeDifference": True},
})

ai_client = OpenAI(
    base_url="https://integrate.api.nvidia.com/v1",
    api_key=CFG.nvidia_api_key,
)


# ============================================================
#  6. CANDLE MANAGER (Closed vs Forming)
# ============================================================

class CandleManager:
    def __init__(self, maxlen: int = 500):
        self._closed: Dict[str, Dict[str, deque]] = {}
        self._forming: Dict[str, Dict[str, Optional[list]]] = {}
        self._lock = threading.Lock()
        self._maxlen = maxlen

    def ensure_symbol(self, symbol_key: str, timeframes: List[str]):
        with self._lock:
            if symbol_key not in self._closed:
                self._closed[symbol_key] = {
                    tf: deque(maxlen=self._maxlen) for tf in timeframes
                }
                self._forming[symbol_key] = {tf: None for tf in timeframes}

    def update(self, sk: str, tf: str, candle: list, is_closed: bool):
        with self._lock:
            if sk not in self._closed or tf not in self._closed[sk]:
                return
            if is_closed:
                dq = self._closed[sk][tf]
                if dq and dq[-1][0] == candle[0]:
                    dq[-1] = candle
                else:
                    dq.append(candle)
                self._forming[sk][tf] = None
            else:
                self._forming[sk][tf] = candle

    def get_closed(self, sk: str, tf: str) -> List[list]:
        with self._lock:
            if sk not in self._closed or tf not in self._closed[sk]:
                return []
            return list(self._closed[sk][tf])

    def get_closed_count(self, sk: str, tf: str) -> int:
        with self._lock:
            if sk not in self._closed:
                return 0
            return len(self._closed[sk].get(tf, []))

    def load_initial(self, sk: str, tf: str, data: list):
        with self._lock:
            if sk not in self._closed:
                return
            if data and len(data) > 1:
                self._closed[sk][tf] = deque(data[:-1], maxlen=self._maxlen)
                self._forming[sk][tf] = data[-1]
            else:
                self._closed[sk][tf] = deque(data, maxlen=self._maxlen)


candle_mgr = CandleManager(CFG.candle_maxlen)


# ============================================================
#  7. STATE + LOCK
# ============================================================

trade_state: Dict[str, dict] = {}
execution_lock = threading.Lock()

# العملات النشطة حالياً (يحددها Scanner)
active_symbols: Dict[str, str] = {}   # key → symbol
active_lock = threading.Lock()


# ============================================================
#  8. INDICATORS (نفس V6)
# ============================================================

def closes(data):  return [float(x[4]) for x in data]
def highs(data):   return [float(x[2]) for x in data]
def lows(data):    return [float(x[3]) for x in data]
def volumes(data): return [float(x[5]) for x in data]


def sma(vals, period):
    if len(vals) < period: return None
    return sum(vals[-period:]) / period


def ema(vals, period):
    if len(vals) < period: return None
    k = 2.0 / (period + 1)
    r = sum(vals[:period]) / period
    for p in vals[period:]:
        r = (p - r) * k + r
    return r


def ema_series(vals, period):
    if len(vals) < period: return []
    k = 2.0 / (period + 1)
    r = [sum(vals[:period]) / period]
    for p in vals[period:]:
        r.append((p - r[-1]) * k + r[-1])
    return r


def calc_rsi(vals, period=14):
    if len(vals) < period + 1: return None
    gains, losses = [], []
    for i in range(1, len(vals)):
        d = vals[i] - vals[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = ((ag * (period-1)) + gains[i]) / period
        al = ((al * (period-1)) + losses[i]) / period
    if al == 0: return 100.0
    return 100.0 - (100.0 / (1.0 + ag/al))


def calc_stochastic(data, k_period=14, d_period=3):
    if len(data) < k_period + d_period: return None
    h, l, c = highs(data), lows(data), closes(data)
    k_vals = []
    for i in range(k_period-1, len(c)):
        hh = max(h[i-k_period+1:i+1])
        ll = min(l[i-k_period+1:i+1])
        k_vals.append(((c[i]-ll)/(hh-ll))*100 if hh != ll else 50.0)
    if len(k_vals) < d_period: return None
    return {"k": round(k_vals[-1], 2), "d": round(sum(k_vals[-d_period:])/d_period, 2)}


def calc_macd(vals):
    if len(vals) < 50: return None
    e12, e26 = ema_series(vals, 12), ema_series(vals, 26)
    ml = []
    for i in range(len(e26)):
        idx = i + 14
        if idx < len(e12):
            ml.append(e12[idx] - e26[i])
    if len(ml) < 9: return None
    sig = ema_series(ml, 9)
    if not sig: return None
    mv, sv = ml[-1], sig[-1]
    return {"macd": round(mv,8), "signal": round(sv,8),
            "histogram": round(mv-sv,8),
            "trend": "bullish" if mv > sv else "bearish"}


def calc_bollinger(vals, period=20):
    if len(vals) < period: return None
    mid = sma(vals, period)
    var = sum((x-mid)**2 for x in vals[-period:]) / period
    std = math.sqrt(var)
    return {"upper": round(mid+2*std,8), "middle": round(mid,8),
            "lower": round(mid-2*std,8),
            "width_pct": round((4*std)/mid*100,4) if mid else 0}


def calc_atr(data, period=14):
    if len(data) < period+1: return None
    h, l, c = highs(data), lows(data), closes(data)
    trs = [max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1]))
           for i in range(1, len(c))]
    return sum(trs[-period:]) / period if len(trs) >= period else None


def calc_adx(data, period=14) -> Optional[dict]:
    if len(data) < period * 3: return None
    h, l, c = highs(data), lows(data), closes(data)
    pdm_r, mdm_r, tr_r = [], [], []
    for i in range(1, len(c)):
        up, dn = h[i]-h[i-1], l[i-1]-l[i]
        pdm_r.append(up if (up > dn and up > 0) else 0.0)
        mdm_r.append(dn if (dn > up and dn > 0) else 0.0)
        tr_r.append(max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])))
    if len(tr_r) < period*2: return None
    atr_s, pdm_s, mdm_s = sum(tr_r[:period]), sum(pdm_r[:period]), sum(mdm_r[:period])
    pdi_s, mdi_s, dx_s = [], [], []
    for i in range(period, len(tr_r)):
        atr_s = atr_s - atr_s/period + tr_r[i]
        pdm_s = pdm_s - pdm_s/period + pdm_r[i]
        mdm_s = mdm_s - mdm_s/period + mdm_r[i]
        if atr_s == 0:
            pdi_s.append(0); mdi_s.append(0); dx_s.append(0); continue
        pdi = pdm_s/atr_s*100; mdi = mdm_s/atr_s*100
        pdi_s.append(pdi); mdi_s.append(mdi)
        ds = pdi+mdi
        dx_s.append(abs(pdi-mdi)/ds*100 if ds else 0)
    if len(dx_s) < period: return None
    adx = sum(dx_s[:period])/period
    for i in range(period, len(dx_s)):
        adx = ((adx*(period-1))+dx_s[i])/period
    cpdi, cmdi = (pdi_s[-1] if pdi_s else 0), (mdi_s[-1] if mdi_s else 0)
    return {"adx": round(adx,2), "plus_di": round(cpdi,2),
            "minus_di": round(cmdi,2),
            "trend": "bullish" if cpdi > cmdi else ("bearish" if cmdi > cpdi else "neutral"),
            "strong": adx > CFG.adx_strong, "weak": adx < CFG.adx_threshold}


def calc_ema_cross(vals, fast_p=50, slow_p=200) -> Optional[dict]:
    if len(vals) < slow_p + 2: return None
    ef, es = ema_series(vals, fast_p), ema_series(vals, slow_p)
    if len(ef) < 2 or len(es) < 2: return None
    fc, fp, sc, sp = ef[-1], ef[-2], es[-1], es[-2]
    cross = "NONE"
    if fp <= sp and fc > sc: cross = "GOLDEN_CROSS"
    elif fp >= sp and fc < sc: cross = "DEATH_CROSS"
    align = "BULLISH_ALIGNMENT" if fc > sc else "BEARISH_ALIGNMENT"
    spread = ((fc-sc)/sc)*100 if sc else 0
    return {"cross": cross, "alignment": align,
            "fast_ema": round(fc,8), "slow_ema": round(sc,8),
            "spread_pct": round(spread,4), "is_fresh_cross": cross != "NONE"}


def calc_obv(data):
    if len(data) < 2: return None
    c, v = closes(data), volumes(data)
    obv = 0
    for i in range(1, len(c)):
        if c[i] > c[i-1]: obv += v[i]
        elif c[i] < c[i-1]: obv -= v[i]
    return obv


def calc_vwap(data):
    if len(data) < 2: return None
    h, l, c, v = highs(data), lows(data), closes(data), volumes(data)
    cpv, cv = 0, 0
    for i in range(len(c)):
        tp = (h[i]+l[i]+c[i])/3; cpv += tp*v[i]; cv += v[i]
    return cpv/cv if cv else None


def calc_volume_ratio(data, period=20):
    if len(data) < period+1: return None
    v = volumes(data)
    avg = sum(v[-period-1:-1])/period
    return round(v[-1]/avg, 4) if avg else None


def calculate_indicators(data) -> Optional[dict]:
    if len(data) < 50: return None
    c = closes(data)
    price = c[-1]
    r = {"price": price, "ema9": ema(c,9), "ema21": ema(c,21),
         "ema50": ema(c,50), "ema200": ema(c,200), "sma20": sma(c,20),
         "rsi": calc_rsi(c), "macd": calc_macd(c),
         "bollinger": calc_bollinger(c), "stochastic": calc_stochastic(data),
         "atr": calc_atr(data), "adx": calc_adx(data),
         "ema_cross": calc_ema_cross(c), "obv": calc_obv(data),
         "vwap": calc_vwap(data), "volume_ratio": calc_volume_ratio(data)}
    sig = []
    if r["rsi"] is not None:
        if r["rsi"] < 30: sig.append("RSI_OVERSOLD")
        elif r["rsi"] > 70: sig.append("RSI_OVERBOUGHT")
    if r["macd"]: sig.append(f"MACD_{r['macd']['trend'].upper()}")
    if r["bollinger"]:
        if price <= r["bollinger"]["lower"]: sig.append("BELOW_BB_LOWER")
        elif price >= r["bollinger"]["upper"]: sig.append("ABOVE_BB_UPPER")
    if r["adx"]:
        if r["adx"]["strong"]: sig.append(f"STRONG_{r['adx']['trend'].upper()}")
        elif r["adx"]["weak"]: sig.append("WEAK_TREND")
    if r["ema_cross"]:
        ec = r["ema_cross"]
        if ec["is_fresh_cross"]: sig.append(ec["cross"])
        sig.append(ec["alignment"])
    if r["volume_ratio"] and r["volume_ratio"] > 2.0: sig.append("VOLUME_SPIKE")
    if r["stochastic"]:
        if r["stochastic"]["k"] < 20: sig.append("STOCH_OVERSOLD")
        elif r["stochastic"]["k"] > 80: sig.append("STOCH_OVERBOUGHT")
    r["signals"] = sig
    return r


# ============================================================
#  ✅ V7: PULLBACK DETECTION
# ============================================================

def detect_pullback(data, direction: str) -> dict:
    """
    يكشف هل السعر في pullback ضمن اتجاه:
    - BULLISH pullback: السعر هبط لـ EMA21 أو EMA50 ثم ارتد
    - BEARISH pullback: السعر صعد لـ EMA21 أو EMA50 ثم ارتد
    
    هذا أفضل من شراء القمة أو بيع القاع.
    """
    result = {"is_pullback": False, "type": "", "quality": 0, "reason": ""}

    if len(data) < 55:
        return result

    c = closes(data)
    h = highs(data)
    l = lows(data)
    price = c[-1]

    ema21 = ema(c, 21)
    ema50 = ema(c, 50)
    if not ema21 or not ema50:
        return result

    atr = calc_atr(data)
    if not atr:
        return result

    if direction == "BULLISH":
        # Pullback صاعد: السعر اقترب من EMA21/50 ثم ارتد
        touch_ema21 = l[-1] <= ema21 * 1.005  # لمس EMA21 (تسامح 0.5%)
        touch_ema50 = l[-1] <= ema50 * 1.005
        bounced = c[-1] > c[-2]  # شمعة ارتداد
        above_ema = price > ema21

        if touch_ema21 and bounced and above_ema:
            result = {"is_pullback": True, "type": "EMA21_BOUNCE",
                      "quality": 2, "reason": "ارتداد من EMA21"}
        elif touch_ema50 and bounced and price > ema50:
            result = {"is_pullback": True, "type": "EMA50_BOUNCE",
                      "quality": 3, "reason": "ارتداد من EMA50 (أقوى)"}

        # Bollinger pullback
        bb = calc_bollinger(c)
        if bb and l[-1] <= bb["lower"] * 1.002 and c[-1] > bb["lower"]:
            result = {"is_pullback": True, "type": "BB_LOWER_BOUNCE",
                      "quality": 2, "reason": "ارتداد من Bollinger السفلي"}

    elif direction == "BEARISH":
        touch_ema21 = h[-1] >= ema21 * 0.995
        touch_ema50 = h[-1] >= ema50 * 0.995
        bounced = c[-1] < c[-2]
        below_ema = price < ema21

        if touch_ema21 and bounced and below_ema:
            result = {"is_pullback": True, "type": "EMA21_REJECT",
                      "quality": 2, "reason": "رفض من EMA21"}
        elif touch_ema50 and bounced and price < ema50:
            result = {"is_pullback": True, "type": "EMA50_REJECT",
                      "quality": 3, "reason": "رفض من EMA50 (أقوى)"}

        bb = calc_bollinger(c)
        if bb and h[-1] >= bb["upper"] * 0.998 and c[-1] < bb["upper"]:
            result = {"is_pullback": True, "type": "BB_UPPER_REJECT",
                      "quality": 2, "reason": "رفض من Bollinger العلوي"}

    return result


# ============================================================
#  ✅ V7: TREND ENGINE (مخفف)
# ============================================================

def confirm_trend(i1m, i1h, i1d) -> TrendConfirmation:
    result = TrendConfirmation()
    reasons = []
    score = 0

    # ═══ 1D ═══
    if i1d:
        ec = i1d.get("ema_cross")
        adx = i1d.get("adx")
        if ec:
            if ec["alignment"] == "BULLISH_ALIGNMENT":
                result.daily_trend = "BULLISH"; score += 2
                reasons.append("1D: EMA50>EMA200")
            else:
                result.daily_trend = "BEARISH"; score -= 2
                reasons.append("1D: EMA50<EMA200")
            if ec["cross"] == "GOLDEN_CROSS":
                score += 1; reasons.append("1D: Golden Cross!")
            elif ec["cross"] == "DEATH_CROSS":
                score -= 1; reasons.append("1D: Death Cross!")
        if adx and adx["weak"]:
            # ✅ V7: لا نضرب في 0.5، فقط نضع علامة
            reasons.append(f"1D: ADX ضعيف ({adx['adx']})")

    # ═══ 1H ═══
    if i1h:
        e21, e50 = i1h.get("ema21"), i1h.get("ema50")
        price = i1h.get("price", 0)
        adx = i1h.get("adx")

        if e21 and e50:
            if e21 > e50 and price > e21:
                result.hourly_trend = "BULLISH"; score += 2
                reasons.append("1H: P>EMA21>EMA50")
            elif e21 < e50 and price < e21:
                result.hourly_trend = "BEARISH"; score -= 2
                reasons.append("1H: P<EMA21<EMA50")
            else:
                result.hourly_trend = "MIXED"
                reasons.append("1H: EMA مختلط")

        if adx:
            # ✅ V7: ADX >= 18 بدلاً من 20
            if adx["adx"] >= CFG.adx_threshold:
                if adx["trend"] == "bullish": score += 1
                elif adx["trend"] == "bearish": score -= 1
                reasons.append(f"1H: ADX={adx['adx']}")
            else:
                reasons.append(f"1H: ADX منخفض ({adx['adx']})")

    # ═══ 1M ═══
    if i1m:
        rsi = i1m.get("rsi")
        stoch = i1m.get("stochastic")
        if rsi is not None:
            if rsi < 30: result.minute_timing = "OVERSOLD"
            elif rsi > 70: result.minute_timing = "OVERBOUGHT"
            else: result.minute_timing = "NEUTRAL"
        if stoch:
            if stoch["k"] < 20 and stoch["k"] > stoch["d"]:
                reasons.append("1M: Stoch تقاطع صاعد")
            elif stoch["k"] > 80 and stoch["k"] < stoch["d"]:
                reasons.append("1M: Stoch تقاطع هابط")

    # ═══ Direction ═══
    result.reasons = reasons

    if (result.daily_trend == "BULLISH" and result.hourly_trend == "BEARISH") or \
       (result.daily_trend == "BEARISH" and result.hourly_trend == "BULLISH"):
        result.direction = TrendDirection.CONFLICT
        result.entry_quality = EntryQuality.BLOCKED
        reasons.append("🚫 تعارض 1D/1H")
        return result

    if score >= 3:
        result.direction = TrendDirection.BULLISH
        result.strength = min(score * 15, 100)
    elif score <= -3:
        result.direction = TrendDirection.BEARISH
        result.strength = min(abs(score) * 15, 100)
    else:
        result.direction = TrendDirection.NEUTRAL
        result.strength = abs(score) * 10

    # ═══ Entry Quality (✅ V7: أكثر تسامحاً) ═══
    if result.direction in (TrendDirection.BULLISH, TrendDirection.BEARISH):
        adx_1d_weak = i1d and i1d.get("adx", {}).get("weak", False)
        adx_1h_weak = i1h and i1h.get("adx", {}).get("weak", False)

        if adx_1d_weak and adx_1h_weak:
            result.entry_quality = EntryQuality.CAUTION  # ✅ كان BLOCKED
            reasons.append("⚠️ ADX ضعيف على الكل - دخول بحذر")
        else:
            result.entry_quality = EntryQuality.ALLOWED
    else:
        result.entry_quality = EntryQuality.BLOCKED

    return result


# ============================================================
#  ✅ V7: SIGNAL ENGINE (Score مخفف + Pullback)
# ============================================================

class SignalEngine:
    """
    V7 Scoring:
    ┌──────────────────────────────────────────────────────┐
    │ +2  Trend Direction متوافق                          │
    │ +2  ADX 1H >= 18                                    │
    │ +1  MACD متوافق                                     │
    │ +1  Volume Ratio > 1.2                              │
    │ +1  RSI في المنطقة الجيدة (40-68 شراء / 32-60 بيع) │
    │ +1  Price فوق/تحت EMA21                             │
    │ +1  Stochastic K > D (شراء) / K < D (بيع)          │
    │ +1  EMA Alignment أو Cross جديد                    │
    │ +2  Pullback مؤكد (بدل مطاردة القمة)               │
    │ ──────────────────────────────────────────────────── │
    │ = 12 أقصى (نطبعه كـ /10)                           │
    │                                                      │
    │ ✅ V7 Filters (مخففة):                              │
    │ - RSI > 82 = منع (ليس 75)                          │
    │ - RSI 75-82 = تحذير (-1 فقط)                       │
    │ - Stoch > 80 + K < D = تحذير (ليس منع)             │
    │ - Stoch > 80 + K > D = زخم (لا تحذير)              │
    │ - ADX < 18 = لا نقاط ADX (ليس منع)                │
    └──────────────────────────────────────────────────────┘
    """

    def evaluate(self, symbol, trend, i1m, i1h, i1d,
                 data_1h=None) -> SignalResult:

        r = SignalResult()
        buy_s, sell_s = 0, 0
        sigs, reasons = [], []

        # ─── Blockers ───
        if trend.entry_quality == EntryQuality.BLOCKED:
            r.decision = Decision.WAIT
            r.reasons = ["Entry BLOCKED"] + trend.reasons
            return r
        if trend.direction == TrendDirection.CONFLICT:
            r.decision = Decision.WAIT
            r.reasons = ["تعارض"] + trend.reasons
            return r
        if trend.direction == TrendDirection.NEUTRAL:
            r.decision = Decision.WAIT
            r.reasons = ["محايد"] + trend.reasons
            return r

        # ─── 1. Trend (+2) ───
        if trend.direction == TrendDirection.BULLISH:
            buy_s += 2; sigs.append("TREND_BULL")
        elif trend.direction == TrendDirection.BEARISH:
            sell_s += 2; sigs.append("TREND_BEAR")

        # ─── 2. ADX (+2) ───
        if i1h and i1h.get("adx"):
            adx = i1h["adx"]
            if adx["adx"] >= CFG.adx_threshold:
                if adx["trend"] == "bullish":
                    buy_s += 2; sigs.append(f"ADX_{adx['adx']}")
                elif adx["trend"] == "bearish":
                    sell_s += 2; sigs.append(f"ADX_{adx['adx']}")

        # ─── 3. MACD (+1) ───
        if i1h and i1h.get("macd"):
            if i1h["macd"]["trend"] == "bullish":
                buy_s += 1; sigs.append("MACD_BULL")
            elif i1h["macd"]["trend"] == "bearish":
                sell_s += 1; sigs.append("MACD_BEAR")

        # ─── 4. Volume (+1) ───
        if i1h and i1h.get("volume_ratio"):
            vr = i1h["volume_ratio"]
            if vr and vr > 1.2:
                if trend.direction == TrendDirection.BULLISH:
                    buy_s += 1; sigs.append(f"VOL_{vr}")
                elif trend.direction == TrendDirection.BEARISH:
                    sell_s += 1; sigs.append(f"VOL_{vr}")

        # ─── 5. RSI Zone (+1) ───
        if i1h and i1h.get("rsi") is not None:
            rsi = i1h["rsi"]
            if 40 <= rsi <= 68:
                buy_s += 1; sigs.append(f"RSI_OK_{rsi:.0f}")
            elif 32 <= rsi <= 60:
                sell_s += 1; sigs.append(f"RSI_OK_S_{rsi:.0f}")

        # ─── 6. Price vs EMA21 (+1) ───
        if i1h:
            p, e21 = i1h.get("price", 0), i1h.get("ema21")
            if e21 and p:
                if p > e21: buy_s += 1; sigs.append("P>EMA21")
                elif p < e21: sell_s += 1; sigs.append("P<EMA21")

        # ─── 7. Stochastic (+1) ───
        # ✅ V7: K > D = زخم (ليس overbought = منع)
        if i1h and i1h.get("stochastic"):
            st = i1h["stochastic"]
            if st["k"] > st["d"]:
                buy_s += 1; sigs.append("STOCH_K>D")
            elif st["k"] < st["d"]:
                sell_s += 1; sigs.append("STOCH_K<D")

        # ─── 8. EMA Cross/Alignment (+1) ───
        if i1h and i1h.get("ema_cross"):
            ec = i1h["ema_cross"]
            if ec["is_fresh_cross"]:
                if ec["cross"] == "GOLDEN_CROSS":
                    buy_s += 1; sigs.append("GOLDEN!")
                elif ec["cross"] == "DEATH_CROSS":
                    sell_s += 1; sigs.append("DEATH!")
            elif ec["alignment"] == "BULLISH_ALIGNMENT":
                buy_s += 1
            elif ec["alignment"] == "BEARISH_ALIGNMENT":
                sell_s += 1

        # ─── 9. ✅ V7: Pullback (+2) ───
        if data_1h and len(data_1h) >= 55:
            pb = detect_pullback(data_1h, trend.direction.value)
            if pb["is_pullback"]:
                r.is_pullback = True
                if trend.direction == TrendDirection.BULLISH:
                    buy_s += 2; sigs.append(f"PULLBACK_{pb['type']}")
                    reasons.append(f"✅ {pb['reason']}")
                elif trend.direction == TrendDirection.BEARISH:
                    sell_s += 2; sigs.append(f"PULLBACK_{pb['type']}")
                    reasons.append(f"✅ {pb['reason']}")

        # ═══ ✅ V7: Filters (مخففة) ═══
        if i1h and i1h.get("rsi") is not None:
            rsi = i1h["rsi"]
            stoch_k = i1h.get("stochastic", {}).get("k", 50) if i1h.get("stochastic") else 50
            adx_v = i1h.get("adx", {}).get("adx", 0) if i1h.get("adx") else 0

            # RSI > 82 = منع حقيقي
            if rsi >= CFG.rsi_extreme_overbought:
                buy_s = max(0, buy_s - 4)
                reasons.append(f"🚫 RSI={rsi:.1f} ≥ {CFG.rsi_extreme_overbought} → منع BUY")

            # RSI 75-82 = تحذير فقط (-1)
            elif rsi >= CFG.rsi_caution_overbought:
                # ✅ V7: إذا ADX قوي + Stoch K > D → لا نخصم (زخم حقيقي)
                if adx_v >= CFG.adx_strong and stoch_k > 50:
                    reasons.append(f"⚠️ RSI={rsi:.1f} مرتفع لكن زخم قوي - لا خصم")
                else:
                    buy_s = max(0, buy_s - 1)
                    reasons.append(f"⚠️ RSI={rsi:.1f} → خصم 1")

            # RSI < 18 = منع
            if rsi <= CFG.rsi_extreme_oversold:
                sell_s = max(0, sell_s - 4)
                reasons.append(f"🚫 RSI={rsi:.1f} ≤ {CFG.rsi_extreme_oversold} → منع SELL")

            elif rsi <= CFG.rsi_caution_oversold:
                if adx_v >= CFG.adx_strong and stoch_k < 50:
                    reasons.append(f"⚠️ RSI={rsi:.1f} منخفض لكن زخم هابط قوي")
                else:
                    sell_s = max(0, sell_s - 1)
                    reasons.append(f"⚠️ RSI={rsi:.1f} → خصم 1")

        # ✅ V7: Stoch > 80 ليس منعاً
        if i1h and i1h.get("stochastic"):
            st = i1h["stochastic"]
            if st["k"] > CFG.stoch_overbought:
                if st["k"] < st["d"]:
                    # K يهبط تحت D = احتمال تصحيح
                    buy_s = max(0, buy_s - 1)
                    reasons.append(f"⚠️ Stoch K<D عند {st['k']:.0f} → خصم 1")
                else:
                    # K > D = زخم مستمر، لا خصم
                    reasons.append(f"ℹ️ Stoch={st['k']:.0f} لكن K>D → زخم")
            elif st["k"] < CFG.stoch_oversold:
                if st["k"] > st["d"]:
                    sell_s = max(0, sell_s - 1)
                    reasons.append(f"⚠️ Stoch K>D عند {st['k']:.0f} → خصم 1")
                else:
                    reasons.append(f"ℹ️ Stoch={st['k']:.0f} لكن K<D → زخم هابط")

        # ═══ القرار ═══
        r.buy_score = min(buy_s, 10)
        r.sell_score = min(sell_s, 10)
        r.signals = sigs
        r.reasons = reasons + trend.reasons

        if trend.direction == TrendDirection.BULLISH:
            if buy_s >= CFG.min_score_to_enter:
                r.decision = Decision.BUY
                self._set_sl_tp(r, i1h)
            else:
                r.decision = Decision.WAIT
                reasons.append(f"BUY={buy_s}/{CFG.min_score_to_enter} غير كافٍ")
        elif trend.direction == TrendDirection.BEARISH:
            if sell_s >= CFG.min_score_to_enter:
                r.decision = Decision.SELL
                self._set_sl_tp(r, i1h)
            else:
                r.decision = Decision.WAIT
                reasons.append(f"SELL={sell_s}/{CFG.min_score_to_enter} غير كافٍ")
        else:
            r.decision = Decision.WAIT

        return r

    def _set_sl_tp(self, r, i1h):
        if i1h and i1h.get("atr") and i1h.get("price"):
            atr_pct = (i1h["atr"] / i1h["price"]) * 100
            r.sl_percent = max(0.5, min(atr_pct * 1.5, CFG.max_sl_percent))
            r.tp_percent = max(1.0, min(atr_pct * 3.0, CFG.max_tp_percent))
        else:
            r.sl_percent = 2.0
            r.tp_percent = 4.0


signal_engine = SignalEngine()


# ============================================================
#  AI ANALYST (شرح فقط)
# ============================================================

class AIAnalyst:
    def analyze(self, symbol, signal, trend, i1h, i1d) -> AIAnalysis:
        result = AIAnalysis()
        if signal.decision == Decision.WAIT:
            result.regime = "no_entry"
            return result

        prompt = f"""أنت محلل أسواق. اشرح فقط. لا تعطي قرار تداول.

العملة: {symbol}
القرار: {signal.decision.value} (Score: B={signal.buy_score} S={signal.sell_score}/10)
الاتجاه: {trend.direction.value} | Pullback: {signal.is_pullback}
RSI 1H: {i1h.get('rsi') if i1h else 'N/A'}
ADX 1H: {i1h.get('adx') if i1h else 'N/A'}
MACD: {i1h.get('macd') if i1h else 'N/A'}
Stoch: {i1h.get('stochastic') if i1h else 'N/A'}
إشارات: {signal.signals}

أجب JSON فقط:
{{"regime":"trending/ranging/volatile","explanation":"شرح بالعربية","risk_warnings":["تحذير"],"agrees_with_signal":true}}"""

        try:
            comp = ai_client.chat.completions.create(
                model=CFG.ai_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0, max_tokens=300, stream=False,
            )
            raw = comp.choices[0].message.content or ""
            cleaned = raw.strip()
            if cleaned.startswith("```"):
                lines = [l for l in cleaned.split("\n") if not l.strip().startswith("```")]
                cleaned = "\n".join(lines).strip()
            dj = json.loads(cleaned)
            result.regime = str(dj.get("regime", "unknown"))
            result.explanation = str(dj.get("explanation", ""))
            result.risk_warnings = dj.get("risk_warnings", [])
            result.agreement = bool(dj.get("agrees_with_signal", True))
            result.raw_response = raw
        except Exception as e:
            logger.warning(f"AI (non-critical): {e}")
        return result


ai_analyst = AIAnalyst()


# ============================================================
#  ✅ V7: MARKET SCANNER (ديناميكي)
# ============================================================

class MarketScanner:
    """
    Phase 1: Quick Scan (كل 5 دقائق)
      - يفحص كل العملات في watchlist
      - يفلتر: حجم، حركة، اتجاه
      - يختار Top N

    Phase 2: Deep Analysis
      - فقط للمرشحين
      - 1D + 1H + 1M + AI
    """

    def __init__(self):
        self._running = True
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("🔍 Market Scanner مُفعّل")

    def stop(self):
        self._running = False

    def _loop(self):
        # أول فحص فوري
        time.sleep(5)
        while self._running:
            try:
                self._scan_cycle()
            except Exception as e:
                logger.error(f"Scanner error: {e}", exc_info=True)
            time.sleep(CFG.scanner_interval)

    def _scan_cycle(self):
        logger.info("=" * 60)
        logger.info("🔍 بدء فحص السوق...")

        candidates = []

        for sk, sym in CFG.watchlist.items():
            try:
                result = self._quick_scan(sk, sym)
                if result:
                    candidates.append(result)
            except Exception as e:
                logger.debug(f"Scan skip {sym}: {e}")
            time.sleep(0.3)  # rate limit

        # ترتيب حسب Score
        candidates.sort(key=lambda x: x.score, reverse=True)

        # Top N
        top = candidates[:CFG.scanner_top_n]

        logger.info(f"📊 فحص {len(CFG.watchlist)} عملة → {len(candidates)} مرشح → Top {len(top)}")
        for i, c in enumerate(top):
            logger.info(f"  #{i+1} {c.symbol} | Score={c.score:.1f} | "
                        f"Vol={c.volume_usdt/1e6:.1f}M | "
                        f"ATR={c.atr_pct:.2f}% | {c.trend_1h} | "
                        f"RSI={c.rsi_1h:.1f}")

        # تحديث bot_stats
        bot_stats["scanner_candidates"] = [
            {"symbol": c.symbol, "score": c.score, "reasons": c.reasons[:3]}
            for c in top
        ]

        # ✅ تحديث العملات النشطة
        with active_lock:
            active_symbols.clear()
            for c in top:
                active_symbols[c.symbol_key] = c.symbol
                candle_mgr.ensure_symbol(c.symbol_key, CFG.timeframes)

        # Phase 2: تحليل عميق للمرشحين
        for c in top:
            # لا نحلل إذا كان هناك مركز مفتوح
            pos = get_current_position(c.symbol)
            if pos == "ERROR" or pos:
                continue

            threading.Thread(
                target=self._deep_analysis,
                args=(c.symbol_key, c.symbol),
                daemon=True,
            ).start()
            time.sleep(1)

    def _quick_scan(self, sk: str, sym: str) -> Optional[ScannerResult]:
        """فحص سريع: حجم + حركة + اتجاه"""
        result = ScannerResult(symbol_key=sk, symbol=sym)
        reasons = []

        # 1. حجم التداول
        try:
            ticker = exchange.fetch_ticker(sym)
            vol_usdt = float(ticker.get("quoteVolume", 0) or 0)
            result.volume_usdt = vol_usdt
            if vol_usdt < CFG.scanner_min_volume_usdt:
                return None  # حجم منخفض جداً
            reasons.append(f"Vol={vol_usdt/1e6:.1f}M")
        except Exception:
            return None

        # 2. ATR (حركة)
        try:
            ohlcv = exchange.fetch_ohlcv(sym, "1h", limit=50)
            if len(ohlcv) < 20:
                return None
            atr = calc_atr(ohlcv)
            price = float(ohlcv[-1][4])
            if atr and price:
                atr_pct = (atr / price) * 100
                result.atr_pct = atr_pct
                if atr_pct < CFG.scanner_min_atr_pct:
                    return None  # حركة منخفضة
                reasons.append(f"ATR={atr_pct:.2f}%")
        except Exception:
            return None

        # 3. اتجاه 1H
        try:
            c = closes(ohlcv)
            e21 = ema(c, 21)
            e50 = ema(c, 50)
            if e21 and e50:
                if e21 > e50 and price > e21:
                    result.trend_1h = "BULLISH"
                    result.score += 3
                    reasons.append("1H BULL")
                elif e21 < e50 and price < e21:
                    result.trend_1h = "BEARISH"
                    result.score += 3
                    reasons.append("1H BEAR")
                else:
                    result.trend_1h = "MIXED"
                    result.score += 1
        except Exception:
            pass

        # 4. RSI
        try:
            rsi = calc_rsi(c)
            if rsi is not None:
                result.rsi_1h = rsi
                # ✅ V7: RSI 75-82 ليس سبباً للاستبعاد
                if rsi > CFG.rsi_extreme_overbought or rsi < CFG.rsi_extreme_oversold:
                    return None  # تشبع شديد جداً فقط
                elif 40 <= rsi <= 68:
                    result.score += 2
                    reasons.append(f"RSI={rsi:.0f} جيد")
                elif rsi > CFG.rsi_caution_overbought:
                    result.score += 1
                    reasons.append(f"RSI={rsi:.0f} مرتفع")
        except Exception:
            pass

        # 5. تغير آخر ساعة
        try:
            if len(ohlcv) >= 2:
                chg = ((price - float(ohlcv[-2][4])) / float(ohlcv[-2][4])) * 100
                result.change_1h_pct = chg
                if abs(chg) > 0.3:
                    result.score += 1
                    reasons.append(f"1H Δ={chg:.2f}%")
        except Exception:
            pass

        # 6. حجم إضافي
        if result.volume_usdt > 50_000_000:
            result.score += 1
            reasons.append("High Vol")

        result.reasons = reasons
        return result

    def _deep_analysis(self, sk: str, sym: str):
        """تحليل عميق لعملة مرشحة"""
        logger.info(f"🔬 تحليل عميق: {sym}")

        # تحميل شموع إذا لم تكن موجودة
        if candle_mgr.get_closed_count(sk, "1h") < 50:
            self._load_candles(sk, sym)

        d1m = candle_mgr.get_closed(sk, "1m")
        d1h = candle_mgr.get_closed(sk, "1h")
        d1d = candle_mgr.get_closed(sk, "1d")

        if len(d1h) < 50 or len(d1d) < 50:
            logger.info(f"⏳ بيانات غير كافية: {sym}")
            return

        i1m = calculate_indicators(d1m) if len(d1m) >= 50 else None
        i1h = calculate_indicators(d1h)
        i1d = calculate_indicators(d1d)
        if not i1h or not i1d:
            return

        trend = confirm_trend(i1m, i1h, i1d)
        signal = signal_engine.evaluate(sym, trend, i1m, i1h, i1d, d1h)

        logger.info(f"📊 {sym}: {signal.decision.value} | "
                    f"B={signal.buy_score} S={signal.sell_score}/10 | "
                    f"PB={signal.is_pullback}")

        if signal.decision == Decision.WAIT:
            bot_stats["last_analysis"][sym] = {
                "decision": "WAIT", "scores": f"B:{signal.buy_score}/S:{signal.sell_score}",
                "time": datetime.now(timezone.utc).isoformat(),
            }
            return

        # AI (شرح)
        ai = ai_analyst.analyze(sym, signal, trend, i1h, i1d)

        if not ai.agreement:
            logger.warning(f"⚠️ AI لا يتفق - تخفيض {sym}")
            return

        # تنفيذ
        if signal.decision in (Decision.BUY, Decision.SELL):
            execute_trade(sym, signal, ai)

        bot_stats["last_analysis"][sym] = {
            "decision": signal.decision.value,
            "scores": f"B:{signal.buy_score}/S:{signal.sell_score}",
            "pullback": signal.is_pullback,
            "ai": ai.regime,
            "time": datetime.now(timezone.utc).isoformat(),
        }

    def _load_candles(self, sk: str, sym: str):
        for tf in CFG.timeframes:
            try:
                limit = 500 if tf == "1d" else 300
                data = exchange.fetch_ohlcv(sym, timeframe=tf, limit=limit)
                candle_mgr.load_initial(sk, tf, data)
            except Exception as e:
                logger.warning(f"Load {sym} {tf}: {e}")
            time.sleep(0.3)


# ============================================================
#  EXECUTION (نفس V6: Lock + SL Mandatory)
# ============================================================

def get_current_position(symbol):
    try:
        for p in exchange.fetch_positions([symbol]):
            ct = p.get("contracts")
            if ct and float(ct) > 0:
                return p
        return None
    except Exception as e:
        logger.error(f"Position check {symbol}: {e}")
        return "ERROR"


def check_limits() -> Tuple[bool, str]:
    if db.count_today_trades() >= CFG.max_daily_trades:
        return False, "حد يومي"
    if db.get_open_count() >= CFG.max_open_positions:
        return False, "مراكز ممتلئة"
    return True, ""


def emergency_close(symbol, reason):
    logger.critical(f"🚨 EMERGENCY: {symbol} | {reason}")
    try:
        pos = get_current_position(symbol)
        if pos and pos != "ERROR":
            ct = float(pos.get("contracts", 0))
            side = pos.get("side", "")
            if ct > 0:
                cs = "sell" if side == "long" else "buy"
                exchange.create_market_order(symbol, cs, ct,
                                             params={"reduceOnly": True})
    except Exception as e:
        logger.critical(f"🚨 Emergency failed: {e}")


def execute_trade(symbol, signal: SignalResult, ai: AIAnalysis):
    with execution_lock:
        try:
            pos = get_current_position(symbol)
            if pos == "ERROR": return
            if pos:
                logger.info(f"🛑 {symbol} مشغول"); return
            ok, reason = check_limits()
            if not ok:
                logger.info(f"⛔ {reason}"); return

            st = trade_state.get(symbol, {})
            if time.time() - st.get("last_trade_time", 0) < CFG.cooldown_seconds:
                logger.info(f"⏳ Cooldown {symbol}"); return

            ticker = exchange.fetch_ticker(symbol)
            price = ticker["last"]
            qty = float(exchange.amount_to_precision(
                symbol, (CFG.margin_usdt * CFG.leverage) / price))
            side = "buy" if signal.decision == Decision.BUY else "sell"
            pname = "LONG" if side == "buy" else "SHORT"
            mode = "PAPER" if CFG.dry_run else "LIVE"

            logger.info(f"🚀 {symbol} | {pname} | {price} | {mode} | "
                        f"PB={signal.is_pullback}")

            if CFG.dry_run:
                db.insert_trade(TradeRecord(
                    symbol=symbol, side=pname, entry_price=price,
                    quantity=qty, confidence=max(signal.buy_score, signal.sell_score),
                    reason=f"Score={signal.buy_score}/{signal.sell_score} PB={signal.is_pullback}",
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    mode="PAPER",
                ))
                st["last_trade_time"] = time.time()
                return

            # LIVE
            exchange.set_leverage(CFG.leverage, symbol)
            order = exchange.create_market_order(symbol, side, qty)
            entry_oid = order.get("id", "")
            time.sleep(1)

            p = get_current_position(symbol)
            entry = float(p.get("entryPrice", price)) if p and p != "ERROR" else price

            sl_p = max(0.5, min(signal.sl_percent, CFG.max_sl_percent))
            tp_p = max(1.0, min(signal.tp_percent, CFG.max_tp_percent))
            if side == "buy":
                sl_price = entry * (1 - sl_p/100)
                tp_price = entry * (1 + tp_p/100)
            else:
                sl_price = entry * (1 + sl_p/100)
                tp_price = entry * (1 - tp_p/100)
            sl_price = float(exchange.price_to_precision(symbol, sl_price))
            tp_price = float(exchange.price_to_precision(symbol, tp_price))
            cs = "sell" if side == "buy" else "buy"

            # SL MANDATORY
            sl_oid = ""
            try:
                sl_o = exchange.create_order(
                    symbol, "STOP_MARKET", cs, qty, None,
                    {"stopPrice": sl_price, "reduceOnly": True,
                     "workingType": "MARK_PRICE"})
                sl_oid = sl_o.get("id", "")
                logger.info(f"✅ SL: {sl_price}")
            except Exception as e:
                logger.critical(f"🚨 SL failed: {e}")
                emergency_close(symbol, "SL failed")
                return

            # TP
            tp_oid = ""
            try:
                tp_o = exchange.create_order(
                    symbol, "TAKE_PROFIT_MARKET", cs, qty, None,
                    {"stopPrice": tp_price, "reduceOnly": True,
                     "workingType": "MARK_PRICE"})
                tp_oid = tp_o.get("id", "")
                logger.info(f"✅ TP: {tp_price}")
            except Exception as e:
                logger.error(f"⚠️ TP failed: {e}")
                try: exchange.cancel_order(sl_oid, symbol)
                except: pass
                emergency_close(symbol, "TP failed")
                return

            tid = db.insert_trade(TradeRecord(
                symbol=symbol, side=pname, entry_price=entry,
                quantity=qty, sl_price=sl_price, tp_price=tp_price,
                sl_order_id=sl_oid, tp_order_id=tp_oid,
                entry_order_id=entry_oid,
                confidence=max(signal.buy_score, signal.sell_score),
                reason=f"Score={signal.buy_score}/{signal.sell_score} PB={signal.is_pullback}",
                timestamp=datetime.now(timezone.utc).isoformat(),
                mode="LIVE",
            ))
            logger.info(f"💾 Trade #{tid}")
            st["last_trade_time"] = time.time()

        except Exception as e:
            logger.error(f"❌ Execute: {e}", exc_info=True)
            emergency_close(symbol, str(e))


# ============================================================
#  POSITION MONITOR (نفس V6)
# ============================================================

class PositionMonitor:
    def __init__(self):
        self._running = True

    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()
        logger.info("👁️ Monitor مُفعّل")

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            try:
                for t in db.get_open_trades():
                    if t.get("mode") == "PAPER": continue
                    self._check(t)
            except Exception as e:
                logger.error(f"Monitor: {e}")
            time.sleep(CFG.monitor_interval)

    def _check(self, trade):
        sym = trade["symbol"]
        sl_st = self._order_status(sym, trade.get("sl_order_id"))
        tp_st = self._order_status(sym, trade.get("tp_order_id"))
        pos = get_current_position(sym)
        if pos == "ERROR": return

        if pos is None:
            reason = "STOP_LOSS" if sl_st == "closed" else \
                     "TAKE_PROFIT" if tp_st == "closed" else "MANUAL"
            exit_p, rpnl, comm = self._real_exit(sym, trade)
            entry = trade["entry_price"]
            if exit_p == 0: exit_p = entry
            qty = trade["quantity"]
            if rpnl == 0:
                rpnl = (exit_p - entry) * qty if trade["side"] == "LONG" \
                       else (entry - exit_p) * qty
            pnl_pct = (rpnl / (entry * qty)) * 100 if entry * qty else 0
            db.close_trade(trade["id"], exit_p, rpnl, pnl_pct, comm, reason)
            logger.info(f"📊 {sym} | {reason} | PnL={rpnl:.4f} ({pnl_pct:.2f}%)")
            self._cancel_remaining(sym, trade)

    def _order_status(self, sym, oid):
        if not oid: return "unknown"
        try:
            return exchange.fetch_order(oid, sym).get("status", "unknown")
        except: return "unknown"

    def _real_exit(self, sym, trade):
        ep, rpnl, comm = 0, 0, 0
        try:
            trades = exchange.fetch_my_trades(sym, limit=20)
            for t in reversed(trades):
                if t.get("reduceOnly"):
                    ep = float(t.get("price", 0))
                    comm = float(t.get("fee", {}).get("cost", 0))
                    break
        except: pass
        if ep == 0:
            try: ep = exchange.fetch_ticker(sym)["last"]
            except: pass
        return ep, rpnl, comm

    def _cancel_remaining(self, sym, trade):
        for oid in [trade.get("sl_order_id"), trade.get("tp_order_id")]:
            if not oid: continue
            try:
                if self._order_status(sym, oid) == "open":
                    exchange.cancel_order(oid, sym)
            except: pass


# ============================================================
#  WEBSOCKET (ديناميكي - للمرشحين فقط)
# ============================================================

async def websocket_worker():
    delay = CFG.ws_reconnect_delay

    while True:
        # بناء قائمةStreams من العملات النشطة
        with active_lock:
            current = dict(active_symbols)

        if not current:
            await asyncio.sleep(10)
            continue

        streams = []
        for sk in current:
            for tf in CFG.timeframes:
                streams.append(f"{sk}@kline_{tf}")
        url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)

        try:
            async with websockets.connect(
                url, ping_interval=CFG.ws_ping_interval,
                ping_timeout=CFG.ws_ping_timeout,
            ) as ws:
                logger.info(f"✅ WS متصل ({len(current)} عملات)")
                delay = CFG.ws_reconnect_delay

                async for msg in ws:
                    data = json.loads(msg)
                    k = data.get("data", {}).get("k")
                    if not k: continue

                    sk = k["s"].lower()
                    tf = k["i"]
                    candle = [k["t"], float(k["o"]), float(k["h"]),
                              float(k["l"]), float(k["c"]), float(k["v"])]

                    candle_mgr.update(sk, tf, candle, k["x"])

        except Exception as e:
            logger.error(f"❌ WS: {e}")

        await asyncio.sleep(delay)
        delay = min(delay * 2, 120)


# ============================================================
#  MAIN
# ============================================================

def main():
    logger.info("=" * 60)
    logger.info("🤖 AI TRADING BOT V7 - Dynamic Scanner")
    logger.info(f"   Mode: {'📝 PAPER' if CFG.dry_run else '💰 LIVE'}")
    logger.info(f"   Watchlist: {len(CFG.watchlist)} coins")
    logger.info(f"   Scanner: every {CFG.scanner_interval}s → Top {CFG.scanner_top_n}")
    logger.info(f"   MinScore: {CFG.min_score_to_enter}/10")
    logger.info(f"   RSI Block: >{CFG.rsi_extreme_overbought} / <{CFG.rsi_extreme_oversold}")
    logger.info(f"   ADX Threshold: {CFG.adx_threshold}")
    logger.info(f"   MaxOpen: {CFG.max_open_positions} | MaxDaily: {CFG.max_daily_trades}")
    logger.info("=" * 60)

    try:
        CFG.validate()
    except ValueError as e:
        logger.critical(f"⛔ {e}"); return

    try:
        ip = requests.get("https://api.ipify.org", timeout=10).text
        logger.info(f"🌐 IP: {ip}")
    except: pass

    threading.Thread(target=run_server, daemon=True).start()
    time.sleep(2)

    # اختبار Binance
    try:
        t = exchange.fetch_ticker("BTC/USDT:USDT")
        logger.info(f"✅ Binance OK | BTC: {t['last']}")
    except Exception as e:
        logger.critical(f"❌ Binance: {e}"); return

    # تحميل أولي لكل العملات (Scanner يحتاج بيانات)
    logger.info("📥 تحميل بيانات أولية...")
    for sk, sym in CFG.watchlist.items():
        candle_mgr.ensure_symbol(sk, CFG.timeframes)
        for tf in CFG.timeframes:
            try:
                limit = 500 if tf == "1d" else 300
                data = exchange.fetch_ohlcv(sym, timeframe=tf, limit=limit)
                candle_mgr.load_initial(sk, tf, data)
            except Exception as e:
                logger.debug(f"  {sym} {tf}: {e}")
            time.sleep(0.2)
    logger.info("✅ البيانات الأولية جاهزة")

    # تشغيل المكونات
    monitor = PositionMonitor()
    monitor.start()

    scanner = MarketScanner()
    scanner.start()

    bot_stats["status"] = "RUNNING"

    try:
        asyncio.run(websocket_worker())
    except KeyboardInterrupt:
        logger.info("👋 إيقاف...")
        scanner.stop()
        monitor.stop()


if __name__ == "__main__":
    main()
