#!/usr/bin/env python3
"""
============================================================
  AI TRADING BOT V4 - Production Grade
  Memecoin Futures Trading with Multi-Timeframe AI Analysis
============================================================
"""

import asyncio
import json
import time
import threading
import math
import sqlite3
import logging
import signal
import sys
from collections import deque
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional, Dict, List
from enum import Enum
from pathlib import Path

import websockets
import ccxt
import requests
from flask import Flask, jsonify
from openai import OpenAI


# ============================================================
#  0. LOGGING
# ============================================================

LOG_DIR = "logs"
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

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
    ai_max_tokens: int = 300

    # --- Trading ---
    dry_run: bool = True
    leverage: int = 10
    margin_usdt: float = 10.0
    min_confidence: float = 70.0
    max_daily_trades: int = 6
    max_open_positions: int = 2
    cooldown_seconds: int = 300

    # --- Risk ---
    default_sl_percent: float = 2.0
    default_tp_percent: float = 4.0
    max_sl_percent: float = 5.0
    max_tp_percent: float = 10.0
    trailing_stop_percent: float = 1.5

    # --- WebSocket ---
    ws_ping_interval: int = 20
    ws_ping_timeout: int = 20
    ws_reconnect_delay: int = 10
    candle_maxlen: int = 300

    # --- Server ---
    flask_port: int = 8080

    # --- Symbols ---
    symbols: Dict[str, str] = field(default_factory=lambda: {
        "wifusdt":      "WIF/USDT:USDT",
        "1000pepeusdt": "1000PEPE/USDT:USDT",
        "dogeusdt":     "DOGE/USDT:USDT",
    })

    timeframes: List[str] = field(default_factory=lambda: ["1m", "1h", "1d"])
    db_path: str = "trades.db"


CFG = Config()


# ============================================================
#  2. ENUMS & DATA CLASSES
# ============================================================

class Decision(Enum):
    BUY  = "BUY"
    SELL = "SELL"
    WAIT = "WAIT"


@dataclass
class AIResult:
    decision: Decision = Decision.WAIT
    confidence: float = 0.0
    sl_percent: float = 2.0
    tp_percent: float = 4.0
    reason: str = ""
    raw_response: str = ""


@dataclass
class TradeRecord:
    symbol: str = ""
    side: str = ""
    entry_price: float = 0.0
    quantity: float = 0.0
    sl_price: float = 0.0
    tp_price: float = 0.0
    confidence: float = 0.0
    reason: str = ""
    timestamp: str = ""
    status: str = "OPEN"


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
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol      TEXT NOT NULL,
                    side        TEXT NOT NULL,
                    entry_price REAL,
                    quantity    REAL,
                    sl_price    REAL,
                    tp_price    REAL,
                    confidence  REAL,
                    reason      TEXT,
                    timestamp   TEXT,
                    status      TEXT DEFAULT 'OPEN',
                    exit_price  REAL,
                    pnl         REAL,
                    closed_at   TEXT
                )
            """)
            self.conn.commit()

    def insert_trade(self, t: TradeRecord):
        with self.lock:
            self.conn.execute(
                """INSERT INTO trades
                   (symbol,side,entry_price,quantity,sl_price,tp_price,
                    confidence,reason,timestamp,status)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (t.symbol, t.side, t.entry_price, t.quantity,
                 t.sl_price, t.tp_price, t.confidence, t.reason,
                 t.timestamp, t.status),
            )
            self.conn.commit()

    def count_today_trades(self) -> int:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self.lock:
            row = self.conn.execute(
                "SELECT COUNT(*) FROM trades WHERE timestamp LIKE ?",
                (f"{today}%",),
            ).fetchone()
        return row[0] if row else 0

    def get_open_count(self) -> int:
        with self.lock:
            row = self.conn.execute(
                "SELECT COUNT(*) FROM trades WHERE status='OPEN'"
            ).fetchone()
        return row[0] if row else 0


db = TradeDB(CFG.db_path)


# ============================================================
#  4. FLASK SERVER
# ============================================================

app = Flask(__name__)
bot_stats = {
    "status": "STARTING",
    "uptime": 0,
    "trades_today": 0,
    "open_positions": 0,
    "last_analysis": {},
    "errors": 0,
}
START_TIME = time.time()


@app.route("/")
def home():
    return "AI TRADING BOT V4 IS LIVE"


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
#  5. EXCHANGE + AI CLIENT
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
#  6. CANDLE STORAGE
# ============================================================

candles: Dict[str, Dict[str, deque]] = {}
for _sk in CFG.symbols:
    candles[_sk] = {tf: deque(maxlen=CFG.candle_maxlen) for tf in CFG.timeframes}


# ============================================================
#  7. TRADE STATE
# ============================================================

trade_state: Dict[str, dict] = {}
for _sv in CFG.symbols.values():
    trade_state[_sv] = {
        "ai_busy": False,
        "last_trade_time": 0,
        "last_decision": None,
    }


# ============================================================
#  8. TECHNICAL INDICATORS
# ============================================================

def closes(data):  return [float(x[4]) for x in data]
def highs(data):   return [float(x[2]) for x in data]
def lows(data):    return [float(x[3]) for x in data]
def volumes(data): return [float(x[5]) for x in data]
def opens(data):   return [float(x[1]) for x in data]


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
    """Wilder's Smoothing RSI"""
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
    e12 = ema_series(vals, 12)
    e26 = ema_series(vals, 26)
    offset = 14
    ml = []
    for i in range(len(e26)):
        idx = i + offset
        if idx < len(e12):
            ml.append(e12[idx] - e26[i])
    if len(ml) < 9: return None
    sig = ema_series(ml, 9)
    if not sig: return None
    mv, sv = ml[-1], sig[-1]
    return {
        "macd": round(mv, 8), "signal": round(sv, 8),
        "histogram": round(mv-sv, 8),
        "trend": "bullish" if mv > sv else "bearish",
    }


def calc_bollinger(vals, period=20):
    if len(vals) < period: return None
    mid = sma(vals, period)
    var = sum((x-mid)**2 for x in vals[-period:]) / period
    std = math.sqrt(var)
    return {
        "upper": round(mid+2*std, 8), "middle": round(mid, 8),
        "lower": round(mid-2*std, 8),
        "width_pct": round((4*std)/mid*100, 4) if mid else 0,
    }


def calc_atr(data, period=14):
    if len(data) < period+1: return None
    h, l, c = highs(data), lows(data), closes(data)
    trs = []
    for i in range(1, len(c)):
        trs.append(max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])))
    if len(trs) < period: return None
    return sum(trs[-period:]) / period


def calc_adx(data, period=14):
    if len(data) < period*2: return None
    h, l, c = highs(data), lows(data), closes(data)
    pdm, mdm, trv = [], [], []
    for i in range(1, len(c)):
        up = h[i]-h[i-1]
        dn = l[i-1]-l[i]
        pdm.append(up if (up > dn and up > 0) else 0)
        mdm.append(dn if (dn > up and dn > 0) else 0)
        trv.append(max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])))
    atr_v = sum(trv[-period:]) / period
    if atr_v == 0: return None
    pdi = (sum(pdm[-period:])/period) / atr_v * 100
    mdi = (sum(mdm[-period:])/period) / atr_v * 100
    if pdi+mdi == 0: return 0.0
    return round(abs(pdi-mdi)/(pdi+mdi)*100, 2)


def calc_obv(data):
    if len(data) < 2: return None
    c, v = closes(data), volumes(data)
    obv = 0
    for i in range(1, len(c)):
        if c[i] > c[i-1]:   obv += v[i]
        elif c[i] < c[i-1]: obv -= v[i]
    return obv


def calc_vwap(data):
    if len(data) < 2: return None
    h, l, c, v = highs(data), lows(data), closes(data), volumes(data)
    cpv, cv = 0, 0
    for i in range(len(c)):
        tp = (h[i]+l[i]+c[i]) / 3
        cpv += tp * v[i]
        cv  += v[i]
    return cpv/cv if cv else None


def calc_volume_ratio(data, period=20):
    if len(data) < period+1: return None
    v = volumes(data)
    avg = sum(v[-period-1:-1]) / period
    return round(v[-1]/avg, 4) if avg else None


# ============================================================
#  9. AGGREGATE INDICATORS + DERIVED SIGNALS
# ============================================================

def calculate_indicators(data) -> Optional[dict]:
    if len(data) < 50: return None
    c = closes(data)
    price = c[-1]

    r = {
        "price": price,
        "ema9": ema(c,9), "ema21": ema(c,21),
        "ema50": ema(c,50), "ema200": ema(c,200),
        "sma20": sma(c,20),
        "rsi": calc_rsi(c),
        "macd": calc_macd(c),
        "bollinger": calc_bollinger(c),
        "stochastic": calc_stochastic(data),
        "atr": calc_atr(data),
        "adx": calc_adx(data),
        "obv": calc_obv(data),
        "vwap": calc_vwap(data),
        "volume_ratio": calc_volume_ratio(data),
    }

    # --- إشارات مشتقة ---
    sig = []
    if r["rsi"] is not None:
        if r["rsi"] < 30:   sig.append("RSI_OVERSOLD")
        elif r["rsi"] > 70: sig.append("RSI_OVERBOUGHT")

    if r["macd"]:
        sig.append(f"MACD_{r['macd']['trend'].upper()}")

    if r["bollinger"]:
        if price <= r["bollinger"]["lower"]: sig.append("BELOW_BB_LOWER")
        elif price >= r["bollinger"]["upper"]: sig.append("ABOVE_BB_UPPER")

    if r["adx"] is not None:
        sig.append("STRONG_TREND" if r["adx"] > 25 else "WEAK_TREND")

    if r["volume_ratio"] and r["volume_ratio"] > 2.0:
        sig.append("VOLUME_SPIKE")

    if r["ema50"] and r["ema200"]:
        sig.append("GOLDEN_CROSS" if r["ema50"] > r["ema200"] else "DEATH_CROSS")

    if r["stochastic"]:
        if r["stochastic"]["k"] < 20: sig.append("STOCH_OVERSOLD")
        elif r["stochastic"]["k"] > 80: sig.append("STOCH_OVERBOUGHT")

    r["signals"] = sig
    return r


# ============================================================
#  10. MARKET DATA
# ============================================================

def get_market_data(symbol):
    res = {"funding_rate": None, "open_interest": None}
    try:
        res["funding_rate"] = exchange.fetch_funding_rate(symbol).get("fundingRate")
    except Exception as e:
        logger.warning(f"Funding failed {symbol}: {e}")
    try:
        res["open_interest"] = exchange.fetch_open_interest(symbol).get("openInterest")
    except Exception as e:
        logger.warning(f"OI failed {symbol}: {e}")
    return res


# ============================================================
#  11. POSITION MANAGEMENT
# ============================================================

def get_current_position(symbol):
    try:
        for p in exchange.fetch_positions([symbol]):
            ct = p.get("contracts")
            if ct and float(ct) > 0:
                logger.info(f"مركز مفتوح: {symbol} | {p.get('side')} | {ct}")
                return p
        return None
    except Exception as e:
        logger.error(f"فحص مركز فشل {symbol}: {e}")
        return "ERROR"


def check_limits() -> bool:
    if db.count_today_trades() >= CFG.max_daily_trades:
        logger.warning("⛔ بلوغ الحد اليومي")
        return False
    if db.get_open_count() >= CFG.max_open_positions:
        logger.warning("⛔ مراكز مفتوحة كثيرة")
        return False
    return True


# ============================================================
#  12. TRADE EXECUTION
# ============================================================

def execute_trade(symbol: str, ai: AIResult):
    logger.info("=" * 60)
    logger.info(f"🚀 تنفيذ: {symbol} | {ai.decision.value} | ثقة {ai.confidence}%")

    try:
        pos = get_current_position(symbol)
        if pos == "ERROR":
            logger.error("🛑 تعذر فحص المركز"); return
        if pos:
            logger.info("🛑 مركز مفتوح مسبقاً"); return
        if not check_limits():
            return

        st = trade_state.get(symbol, {})
        if time.time() - st.get("last_trade_time", 0) < CFG.cooldown_seconds:
            logger.info("⏳ Cooldown نشط"); return

        ticker = exchange.fetch_ticker(symbol)
        price = ticker["last"]
        qty = float(exchange.amount_to_precision(
            symbol, (CFG.margin_usdt * CFG.leverage) / price
        ))
        side = "buy" if ai.decision == Decision.BUY else "sell"
        pname = "LONG" if side == "buy" else "SHORT"

        logger.info(f"💰 {price} | 📦 {qty} | {pname} | x{CFG.leverage}")

        # --- DRY RUN ---
        if CFG.dry_run:
            logger.info("🧪 DRY RUN - لا صفقة حقيقية")
            db.insert_trade(TradeRecord(
                symbol=symbol, side=pname, entry_price=price,
                quantity=qty, confidence=ai.confidence, reason=ai.reason,
                timestamp=datetime.now(timezone.utc).isoformat(),
                status="DRY_RUN",
            ))
            st["last_trade_time"] = time.time()
            return

        # --- تنفيذ حقيقي ---
        exchange.set_leverage(CFG.leverage, symbol)
        order = exchange.create_market_order(symbol, side, qty)
        logger.info(f"✅ Order: {order.get('id')}")

        time.sleep(1)
        p = get_current_position(symbol)
        entry = float(p.get("entryPrice", price)) if p and p != "ERROR" else price

        sl_p = max(0.5, min(ai.sl_percent, CFG.max_sl_percent))
        tp_p = max(1.0, min(ai.tp_percent, CFG.max_tp_percent))

        if side == "buy":
            sl_price = entry * (1 - sl_p/100)
            tp_price = entry * (1 + tp_p/100)
        else:
            sl_price = entry * (1 + sl_p/100)
            tp_price = entry * (1 - tp_p/100)

        sl_price = float(exchange.price_to_precision(symbol, sl_price))
        tp_price = float(exchange.price_to_precision(symbol, tp_price))
        cs = "sell" if side == "buy" else "buy"

        for otype, oprice, label in [
            ("STOP_MARKET", sl_price, "SL"),
            ("TAKE_PROFIT_MARKET", tp_price, "TP"),
        ]:
            try:
                exchange.create_order(
                    symbol, otype, cs, qty, None,
                    {"stopPrice": oprice, "reduceOnly": True},
                )
                logger.info(f"✅ {label}: {oprice}")
            except Exception as e:
                logger.error(f"⚠️ فشل {label}: {e}")

        db.insert_trade(TradeRecord(
            symbol=symbol, side=pname, entry_price=entry,
            quantity=qty, sl_price=sl_price, tp_price=tp_price,
            confidence=ai.confidence, reason=ai.reason,
            timestamp=datetime.now(timezone.utc).isoformat(),
        ))
        st["last_trade_time"] = time.time()
        st["last_decision"] = ai.decision.value

    except Exception as e:
        logger.error(f"❌ فشل التنفيذ: {e}", exc_info=True)
        bot_stats["errors"] += 1


# ============================================================
#  13. AI ANALYSIS
# ============================================================

def _fmt(v, d=6):
    if v is None: return "N/A"
    if isinstance(v, float): return f"{v:.{d}f}"
    return str(v)


def build_prompt(symbol, i1m, i1h, i1d, mkt):
    return f"""أنت نظام تداول آلي احترافي. حلل البيانات المعطاة فقط. لا تخترع معلومات.

العملة: {symbol}
السعر: {_fmt(i1h['price'] if i1h else None)}

=== 1D (اتجاه عام) ===
EMA50: {_fmt(i1d.get('ema50') if i1d else None)}
EMA200: {_fmt(i1d.get('ema200') if i1d else None)}
RSI: {_fmt(i1d.get('rsi') if i1d else None, 2)}
MACD: {i1d.get('macd') if i1d else 'N/A'}
ADX: {_fmt(i1d.get('adx') if i1d else None, 2)}
Bollinger: {i1d.get('bollinger') if i1d else 'N/A'}
إشارات: {i1d.get('signals') if i1d else []}

=== 1H (اتجاه رئيسي) ===
RSI: {_fmt(i1h.get('rsi') if i1h else None, 2)}
MACD: {i1h.get('macd') if i1h else 'N/A'}
Stochastic: {i1h.get('stochastic') if i1h else 'N/A'}
EMA9: {_fmt(i1h.get('ema9') if i1h else None)}
EMA21: {_fmt(i1h.get('ema21') if i1h else None)}
EMA50: {_fmt(i1h.get('ema50') if i1h else None)}
EMA200: {_fmt(i1h.get('ema200') if i1h else None)}
Bollinger: {i1h.get('bollinger') if i1h else 'N/A'}
ATR: {_fmt(i1h.get('atr') if i1h else None)}
ADX: {_fmt(i1h.get('adx') if i1h else None, 2)}
VWAP: {_fmt(i1h.get('vwap') if i1h else None)}
OBV: {_fmt(i1h.get('obv') if i1h else None, 0)}
VolRatio: {_fmt(i1h.get('volume_ratio') if i1h else None, 2)}
إشارات: {i1h.get('signals') if i1h else []}

=== 1M (توقيت) ===
RSI: {_fmt(i1m.get('rsi') if i1m else None, 2)}
MACD: {i1m.get('macd') if i1m else 'N/A'}
Stochastic: {i1m.get('stochastic') if i1m else 'N/A'}
إشارات: {i1m.get('signals') if i1m else []}

=== سوق ===
Funding: {mkt.get('funding_rate', 'N/A')}
OI: {mkt.get('open_interest', 'N/A')}

=== قواعد إلزامية ===
1. 1D للاتجاه العام، 1H للدخول، 1M للتوقيت.
2. لا BUY إذا 1D+1H هابطين. لا SELL إذا 1D+1H صاعدين.
3. تعارض 1D مع 1H = WAIT.
4. RSI>75 على 1H = لا BUY. RSI<25 = لا SELL.
5. ADX<20 = WAIT.
6. Funding سالب جداً + صاعد = احتمال انعكاس.
7. VolRatio>3 + شمعة كبيرة = تأكيد.
8. تحتاج 3 إشارات متوافقة على الأقل.

أجب JSON فقط:
{{"decision":"BUY/SELL/WAIT","confidence":0-100,"stop_loss_percent":0.5-5,"take_profit_percent":1-10,"reason":"سبب مختصر"}}"""


def analyze_with_ai(symbol) -> Optional[AIResult]:
    logger.info(f"🧠 تحليل AI: {symbol}")

    sym_key = None
    for k, v in CFG.symbols.items():
        if v == symbol:
            sym_key = k; break
    if not sym_key:
        return None

    d = candles[sym_key]
    d1m, d1h, d1d = list(d["1m"]), list(d["1h"]), list(d["1d"])

    if len(d1h) < 50:
        logger.info(f"⏳ 1H: {len(d1h)}/50"); return None
    if len(d1d) < 50:
        logger.info(f"⏳ 1D: {len(d1d)}/50"); return None

    i1m = calculate_indicators(d1m) if len(d1m) >= 50 else None
    i1h = calculate_indicators(d1h)
    i1d = calculate_indicators(d1d)
    if not i1h or not i1d:
        return None

    # فحص تناقض
    e50d, e50h = i1d.get("ema50"), i1h.get("ema50")
    if e50d and e50h:
        t1d = "bull" if i1d["price"] > e50d else "bear"
        t1h = "bull" if i1h["price"] > e50h else "bear"
        if t1d != t1h:
            logger.info(f"⏳ تناقض 1D:{t1d} vs 1H:{t1h}")
            return AIResult(decision=Decision.WAIT, reason="تناقض اتجاهي")

    mkt = get_market_data(symbol)
    prompt = build_prompt(symbol, i1m, i1h, i1d, mkt)

    logger.info("📤 إرسال إلى AI...")
    t0 = time.time()
    try:
        comp = ai_client.chat.completions.create(
            model=CFG.ai_model,
            messages=[{"role": "user", "content": prompt}],
            temperature=CFG.ai_temperature,
            max_tokens=CFG.ai_max_tokens,
            stream=False,
        )
        raw = comp.choices[0].message.content or ""
        logger.info(f"⚡ رد AI: {time.time()-t0:.2f}s")

        # تنظيف markdown fences
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            cleaned = "\n".join(lines).strip()

        dj = json.loads(cleaned)

        try:
            dec = Decision(str(dj.get("decision","WAIT")).upper())
        except ValueError:
            dec = Decision.WAIT

        result = AIResult(
            decision=dec,
            confidence=max(0, min(float(dj.get("confidence",0)), 100)),
            sl_percent=max(0.5, min(float(dj.get("stop_loss_percent",2)), 5)),
            tp_percent=max(1.0, min(float(dj.get("take_profit_percent",4)), 10)),
            reason=str(dj.get("reason","")),
            raw_response=raw,
        )

        logger.info(f"🎯 {result.decision.value} | {result.confidence}% | {result.reason}")

        if result.decision in (Decision.BUY, Decision.SELL):
            if result.confidence < CFG.min_confidence:
                logger.info(f"⛔ ثقة منخفضة {result.confidence}%")
                return AIResult(decision=Decision.WAIT, reason="ثقة منخفضة")

        bot_stats["last_analysis"][symbol] = {
            "decision": result.decision.value,
            "confidence": result.confidence,
            "time": datetime.now(timezone.utc).isoformat(),
        }
        return result

    except json.JSONDecodeError:
        logger.error("⚠️ JSON غير صحيح من AI")
        return None
    except Exception as e:
        logger.error(f"❌ خطأ AI: {e}", exc_info=True)
        bot_stats["errors"] += 1
        return None


# ============================================================
#  14. SAFE WRAPPER
# ============================================================

def analyze_safe(symbol):
    st = trade_state.get(symbol)
    if not st or st["ai_busy"]:
        return

    pos = get_current_position(symbol)
    if pos == "ERROR":
        logger.error(f"🛑 تعذر فحص {symbol}"); return
    if pos:
        logger.info(f"📌 {symbol} مشغول"); return

    st["ai_busy"] = True
    try:
        r = analyze_with_ai(symbol)
        if r and r.decision in (Decision.BUY, Decision.SELL):
            execute_trade(symbol, r)
    except Exception as e:
        logger.error(f"❌ {symbol}: {e}", exc_info=True)
    finally:
        st["ai_busy"] = False


# ============================================================
#  15. WEBSOCKET (مع Exponential Backoff)
# ============================================================

async def websocket_worker():
    streams = []
    for sk in CFG.symbols:
        for tf in CFG.timeframes:
            streams.append(f"{sk}@kline_{tf}")
    url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
    delay = CFG.ws_reconnect_delay

    while True:
        try:
            async with websockets.connect(
                url, ping_interval=CFG.ws_ping_interval,
                ping_timeout=CFG.ws_ping_timeout,
            ) as ws:
                logger.info("✅ WebSocket متصل")
                delay = CFG.ws_reconnect_delay

                async for msg in ws:
                    data = json.loads(msg)
                    k = data.get("data", {}).get("k")
                    if not k:
                        continue

                    sk = k["s"].lower()
                    tf = k["i"]
                    if sk not in candles or tf not in candles[sk]:
                        continue

                    candle = [
                        k["t"], float(k["o"]), float(k["h"]),
                        float(k["l"]), float(k["c"]), float(k["v"]),
                    ]

                    dq = candles[sk][tf]
                    if dq and dq[-1][0] == candle[0]:
                        dq[-1] = candle
                    else:
                        dq.append(candle)

                    if k["x"] and tf == "1h":
                        sym = CFG.symbols.get(sk)
                        if sym:
                            logger.info(f"🕯️ شمعة 1H: {sym}")
                            threading.Thread(
                                target=analyze_safe, args=(sym,), daemon=True
                            ).start()

        except Exception as e:
            logger.error(f"❌ WS: {e}")

        logger.info(f"🔄 إعادة اتصال بعد {delay}s")
        await asyncio.sleep(delay)
        delay = min(delay * 2, 120)


# ============================================================
#  16. INITIAL LOAD + TEST
# ============================================================

def load_initial_candles():
    logger.info("📥 تحميل شموع أولية...")
    for sk, sym in CFG.symbols.items():
        for tf in CFG.timeframes:
            try:
                data = exchange.fetch_ohlcv(sym, timeframe=tf, limit=250)
                candles[sk][tf] = deque(data, maxlen=CFG.candle_maxlen)
                logger.info(f"  ✅ {sym} {tf}: {len(data)}")
            except Exception as e:
                logger.error(f"  ❌ {sym} {tf}: {e}")
            time.sleep(0.5)


def test_binance():
    try:
        t = exchange.fetch_ticker("BTC/USDT:USDT")
        logger.info(f"✅ Binance OK | BTC: {t['last']}")
        return True
    except Exception as e:
        logger.error(f"❌ Binance: {e}")
        return False


# ============================================================
#  17. MAIN
# ============================================================

def main():
    logger.info("=" * 60)
    logger.info("🤖 AI TRADING BOT V4")
    logger.info(f"   DRY_RUN={CFG.dry_run} | Lev=x{CFG.leverage}")
    logger.info(f"   Margin={CFG.margin_usdt} | MinConf={CFG.min_confidence}%")
    logger.info(f"   MaxDaily={CFG.max_daily_trades} | Cooldown={CFG.cooldown_seconds}s")
    logger.info("=" * 60)

    try:
        ip = requests.get("https://api.ipify.org", timeout=10).text
        logger.info(f"🌐 IP: {ip}")
    except Exception:
        pass

    threading.Thread(target=run_server, daemon=True).start()
    logger.info(f"🌐 Flask :{CFG.flask_port}")
    time.sleep(2)

    if not test_binance():
        logger.error("🛑 إيقاف - Binance غير متصل")
        return

    load_initial_candles()

    logger.info("🧠 تحليل أولي...")
    for sym in CFG.symbols.values():
        threading.Thread(target=analyze_safe, args=(sym,), daemon=True).start()
        time.sleep(1)

    bot_stats["status"] = "RUNNING"
    logger.info("🚀 بدء WebSocket...")
    asyncio.run(websocket_worker())


if __name__ == "__main__":
    main()
