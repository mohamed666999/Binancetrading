#!/usr/bin/env python3
"""
  AI TRADING BOT V8 FINAL
  Build: V8-2026-07-26
"""

V8_BUILD = "V8-FINAL-2026-07-26"

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
from typing import Optional, Dict, List, Tuple
from enum import Enum

import websockets
import ccxt
import requests
from flask import Flask, jsonify
from openai import OpenAI

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


@dataclass
class Config:
    binance_api_key: str = "IX7kLH0ssWHP5TpYMUGcp0pzq4LX4Lqi7m4XtlqMkkq6DCZAsLhoeYZ3533jJFF4"
    binance_secret: str = "LmICnpSpMxL1riv4RfIf0HBGRfhDTP5JhDUYdlPSukpqV7kDTonrZ0j3DWp1a7hU"
    nvidia_api_key: str = "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"
    ai_model: str = "deepseek-ai/deepseek-v4-pro"
    ai_temperature: float = 0.0
    ai_max_tokens: int = 400
    dry_run: bool = False
    leverage: int = 10
    margin_usdt: float = 10.0
    max_daily_trades: int = 8
    max_open_positions: int = 2
    cooldown_seconds: int = 180
    max_sl_percent: float = 5.0
    max_tp_percent: float = 10.0
    min_score_to_enter: int = 5
    rsi_extreme_overbought: float = 82.0
    rsi_extreme_oversold: float = 18.0
    rsi_caution_overbought: float = 75.0
    rsi_caution_oversold: float = 25.0
    adx_threshold: float = 18.0
    adx_strong: float = 25.0
    stoch_overbought: float = 80.0
    stoch_oversold: float = 20.0
    scanner_interval: int = 300
    scanner_top_n: int = 5
    scanner_min_volume_usdt: float = 5_000_000
    scanner_min_atr_pct: float = 0.5
    monitor_interval: int = 15
    ws_ping_interval: int = 20
    ws_ping_timeout: int = 20
    ws_reconnect_delay: int = 10
    candle_maxlen: int = 500
    flask_port: int = 8080
    watchlist: Dict[str, str] = field(default_factory=lambda: {
        "btcusdt": "BTC/USDT:USDT", "ethusdt": "ETH/USDT:USDT",
        "solusdt": "SOL/USDT:USDT", "bnbusdt": "BNB/USDT:USDT",
        "xrpusdt": "XRP/USDT:USDT", "adausdt": "ADA/USDT:USDT",
        "linkusdt": "LINK/USDT:USDT", "avaxusdt": "AVAX/USDT:USDT",
        "dogeusdt": "DOGE/USDT:USDT", "wifusdt": "WIF/USDT:USDT",
        "1000pepeusdt": "1000PEPE/USDT:USDT", "suiusdt": "SUI/USDT:USDT",
        "aaveusdt": "AAVE/USDT:USDT", "nearusdt": "NEAR/USDT:USDT",
        "arbusdt": "ARB/USDT:USDT", "dotusdt": "DOT/USDT:USDT",
        "maticusdt": "MATIC/USDT:USDT", "ltcusdt": "LTC/USDT:USDT",
        "aptusdt": "APT/USDT:USDT", "opustdt": "OP/USDT:USDT",
    })
    timeframes: List[str] = field(default_factory=lambda: ["1m", "1h", "1d"])
    db_path: str = "trades.db"

    def validate(self):
        if not self.binance_api_key or not self.binance_secret:
            raise ValueError("Binance keys missing")
        if not self.nvidia_api_key:
            raise ValueError("NVIDIA key missing")


CFG = Config()


class Decision(Enum):
    BUY = "BUY"; SELL = "SELL"; WAIT = "WAIT"

class TrendDirection(Enum):
    BULLISH = "BULLISH"; BEARISH = "BEARISH"; NEUTRAL = "NEUTRAL"; CONFLICT = "CONFLICT"

class EntryQuality(Enum):
    ALLOWED = "ALLOWED"; CAUTION = "CAUTION"; BLOCKED = "BLOCKED"


@dataclass
class ScannerResult:
    symbol_key: str = ""; symbol: str = ""; score: float = 0.0
    volume_usdt: float = 0.0; atr_pct: float = 0.0
    change_1h_pct: float = 0.0; trend_1h: str = ""; rsi_1h: float = 50.0
    reasons: List[str] = field(default_factory=list)

@dataclass
class SignalResult:
    decision: Decision = Decision.WAIT
    buy_score: int = 0; sell_score: int = 0; max_score: int = 12
    is_pullback: bool = False
    signals: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)
    sl_percent: float = 2.0; tp_percent: float = 4.0

@dataclass
class AIAnalysis:
    regime: str = "unknown"; explanation: str = ""
    risk_warnings: List[str] = field(default_factory=list)
    agreement: bool = True; raw_response: str = ""

@dataclass
class TrendConfirmation:
    direction: TrendDirection = TrendDirection.NEUTRAL
    entry_quality: EntryQuality = EntryQuality.BLOCKED
    daily_trend: str = ""; hourly_trend: str = ""; minute_timing: str = ""
    strength: float = 0.0
    reasons: List[str] = field(default_factory=list)

@dataclass
class TradeRecord:
    symbol: str = ""; side: str = ""; entry_price: float = 0.0
    quantity: float = 0.0; sl_price: float = 0.0; tp_price: float = 0.0
    sl_order_id: str = ""; tp_order_id: str = ""; entry_order_id: str = ""
    confidence: float = 0.0; reason: str = ""; timestamp: str = ""
    status: str = "OPEN"; mode: str = "PAPER"


class TradeDB:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        with self.lock:
            self.conn.execute("""CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, side TEXT,
                mode TEXT DEFAULT 'PAPER', entry_price REAL, quantity REAL,
                sl_price REAL, tp_price REAL, sl_order_id TEXT, tp_order_id TEXT,
                entry_order_id TEXT, confidence REAL, reason TEXT, timestamp TEXT,
                status TEXT DEFAULT 'OPEN', exit_price REAL, realized_pnl REAL,
                pnl_percent REAL, commission REAL DEFAULT 0, closed_at TEXT,
                close_reason TEXT)""")
            self.conn.commit()

    def insert_trade(self, t):
        with self.lock:
            cur = self.conn.execute(
                "INSERT INTO trades (symbol,side,mode,entry_price,quantity,"
                "sl_price,tp_price,sl_order_id,tp_order_id,entry_order_id,"
                "confidence,reason,timestamp,status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (t.symbol,t.side,t.mode,t.entry_price,t.quantity,t.sl_price,
                 t.tp_price,t.sl_order_id,t.tp_order_id,t.entry_order_id,
                 t.confidence,t.reason,t.timestamp,t.status))
            self.conn.commit()
            return cur.lastrowid

    def close_trade(self, tid, ep, rpnl, pp, comm, reason):
        with self.lock:
            self.conn.execute(
                "UPDATE trades SET status='CLOSED',exit_price=?,realized_pnl=?,"
                "pnl_percent=?,commission=?,closed_at=?,close_reason=? WHERE id=?",
                (ep,rpnl,pp,comm,datetime.now(timezone.utc).isoformat(),reason,tid))
            self.conn.commit()

    def get_open_trades(self):
        with self.lock:
            rows = self.conn.execute("SELECT * FROM trades WHERE status='OPEN'").fetchall()
            cols = [d[0] for d in self.conn.execute("SELECT * FROM trades LIMIT 0").description]
        return [dict(zip(cols, r)) for r in rows]

    def count_today_trades(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self.lock:
            r = self.conn.execute("SELECT COUNT(*) FROM trades WHERE timestamp LIKE ? AND mode='LIVE'",
                                  (f"{today}%",)).fetchone()
        return r[0] if r else 0

    def get_open_count(self):
        with self.lock:
            r = self.conn.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN' AND mode='LIVE'").fetchone()
        return r[0] if r else 0


db = TradeDB(CFG.db_path)

app = Flask(__name__)
bot_stats = {"status":"STARTING","version":V8_BUILD,"uptime":0,"trades_today":0,
             "open_positions":0,"scanner_candidates":[],"last_analysis":{},
             "errors":0,"mode":"PAPER" if CFG.dry_run else "LIVE"}
START_TIME = time.time()

@app.route("/")
def home():
    return f"AI TRADING BOT {V8_BUILD} | {'PAPER' if CFG.dry_run else 'LIVE'}"

@app.route("/health")
def health():
    bot_stats["uptime"] = int(time.time() - START_TIME)
    bot_stats["trades_today"] = db.count_today_trades()
    bot_stats["open_positions"] = db.get_open_count()
    return jsonify(bot_stats)

def run_server():
    app.run(host="0.0.0.0", port=CFG.flask_port, debug=False, use_reloader=False)


exchange = ccxt.binance({
    "apiKey": CFG.binance_api_key, "secret": CFG.binance_secret,
    "enableRateLimit": True,
    "options": {"defaultType": "swap", "adjustForTimeDifference": True},
})

ai_client = OpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=CFG.nvidia_api_key)


class CandleManager:
    def __init__(self, maxlen=500):
        self._closed = {}; self._forming = {}; self._lock = threading.Lock(); self._maxlen = maxlen

    def ensure_symbol(self, sk, tfs):
        with self._lock:
            if sk not in self._closed:
                self._closed[sk] = {tf: deque(maxlen=self._maxlen) for tf in tfs}
                self._forming[sk] = {tf: None for tf in tfs}

    def update(self, sk, tf, candle, is_closed):
        with self._lock:
            if sk not in self._closed or tf not in self._closed[sk]: return
            if is_closed:
                dq = self._closed[sk][tf]
                if dq and dq[-1][0] == candle[0]: dq[-1] = candle
                else: dq.append(candle)
                self._forming[sk][tf] = None
            else:
                self._forming[sk][tf] = candle

    def get_closed(self, sk, tf):
        with self._lock:
            if sk not in self._closed or tf not in self._closed[sk]: return []
            return list(self._closed[sk][tf])

    def get_closed_count(self, sk, tf):
        with self._lock:
            return len(self._closed.get(sk, {}).get(tf, []))

    def load_initial(self, sk, tf, data):
        with self._lock:
            if sk not in self._closed: return
            if data and len(data) > 1:
                self._closed[sk][tf] = deque(data[:-1], maxlen=self._maxlen)
                self._forming[sk][tf] = data[-1]
            else:
                self._closed[sk][tf] = deque(data, maxlen=self._maxlen)


candle_mgr = CandleManager(CFG.candle_maxlen)
trade_state = {}
execution_lock = threading.Lock()
active_symbols = {}
active_lock = threading.Lock()


# === INDICATORS ===

def closes(d): return [float(x[4]) for x in d]
def highs(d): return [float(x[2]) for x in d]
def lows(d): return [float(x[3]) for x in d]
def volumes(d): return [float(x[5]) for x in d]

def sma(v, p):
    if len(v) < p: return None
    return sum(v[-p:]) / p

def ema(v, p):
    if len(v) < p: return None
    k = 2.0/(p+1); r = sum(v[:p])/p
    for x in v[p:]: r = (x-r)*k+r
    return r

def ema_series(v, p):
    if len(v) < p: return []
    k = 2.0/(p+1); r = [sum(v[:p])/p]
    for x in v[p:]: r.append((x-r[-1])*k+r[-1])
    return r

def calc_rsi(v, p=14):
    if len(v) < p+1: return None
    g, l = [], []
    for i in range(1, len(v)):
        d = v[i]-v[i-1]; g.append(max(d,0)); l.append(max(-d,0))
    ag = sum(g[:p])/p; al = sum(l[:p])/p
    for i in range(p, len(g)):
        ag = ((ag*(p-1))+g[i])/p; al = ((al*(p-1))+l[i])/p
    if al == 0: return 100.0
    return 100.0 - (100.0/(1.0+ag/al))

def calc_stochastic(data, kp=14, dp=3):
    if len(data) < kp+dp: return None
    h, l, c = highs(data), lows(data), closes(data)
    kv = []
    for i in range(kp-1, len(c)):
        hh = max(h[i-kp+1:i+1]); ll = min(l[i-kp+1:i+1])
        kv.append(((c[i]-ll)/(hh-ll))*100 if hh != ll else 50.0)
    if len(kv) < dp: return None
    return {"k": round(kv[-1],2), "d": round(sum(kv[-dp:])/dp,2)}

def calc_macd(v):
    if len(v) < 50: return None
    e12, e26 = ema_series(v,12), ema_series(v,26)
    ml = []
    for i in range(len(e26)):
        idx = i+14
        if idx < len(e12): ml.append(e12[idx]-e26[i])
    if len(ml) < 9: return None
    sig = ema_series(ml, 9)
    if not sig: return None
    mv, sv = ml[-1], sig[-1]
    return {"macd":round(mv,8),"signal":round(sv,8),"histogram":round(mv-sv,8),
            "trend":"bullish" if mv > sv else "bearish"}

def calc_bollinger(v, p=20):
    if len(v) < p: return None
    mid = sma(v,p); var = sum((x-mid)**2 for x in v[-p:])/p; std = math.sqrt(var)
    return {"upper":round(mid+2*std,8),"middle":round(mid,8),"lower":round(mid-2*std,8),
            "width_pct":round((4*std)/mid*100,4) if mid else 0}

def calc_atr(data, p=14):
    if len(data) < p+1: return None
    h, l, c = highs(data), lows(data), closes(data)
    trs = [max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])) for i in range(1,len(c))]
    return sum(trs[-p:])/p if len(trs) >= p else None

def calc_adx(data, p=14):
    if len(data) < p*3: return None
    h, l, c = highs(data), lows(data), closes(data)
    pr, mr, tr = [], [], []
    for i in range(1, len(c)):
        up, dn = h[i]-h[i-1], l[i-1]-l[i]
        pr.append(up if (up > dn and up > 0) else 0.0)
        mr.append(dn if (dn > up and dn > 0) else 0.0)
        tr.append(max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])))
    if len(tr) < p*2: return None
    ats, pds, mds = sum(tr[:p]), sum(pr[:p]), sum(mr[:p])
    pdis, mdis, dxs = [], [], []
    for i in range(p, len(tr)):
        ats = ats - ats/p + tr[i]; pds = pds - pds/p + pr[i]; mds = mds - mds/p + mr[i]
        if ats == 0: pdis.append(0); mdis.append(0); dxs.append(0); continue
        pdi = pds/ats*100; mdi = mds/ats*100
        pdis.append(pdi); mdis.append(mdi)
        ds = pdi+mdi; dxs.append(abs(pdi-mdi)/ds*100 if ds else 0)
    if len(dxs) < p: return None
    adx = sum(dxs[:p])/p
    for i in range(p, len(dxs)): adx = ((adx*(p-1))+dxs[i])/p
    cpdi = pdis[-1] if pdis else 0; cmdi = mdis[-1] if mdis else 0
    return {"adx":round(adx,2),"plus_di":round(cpdi,2),"minus_di":round(cmdi,2),
            "trend":"bullish" if cpdi > cmdi else ("bearish" if cmdi > cpdi else "neutral"),
            "strong":adx > CFG.adx_strong,"weak":adx < CFG.adx_threshold}

def calc_ema_cross(v, fp=50, sp=200):
    if len(v) < sp+2: return None
    ef, es = ema_series(v,fp), ema_series(v,sp)
    if len(ef) < 2 or len(es) < 2: return None
    fc, fprev, sc, sprev = ef[-1], ef[-2], es[-1], es[-2]
    cross = "NONE"
    if fprev <= sprev and fc > sc: cross = "GOLDEN_CROSS"
    elif fprev >= sprev and fc < sc: cross = "DEATH_CROSS"
    align = "BULLISH_ALIGNMENT" if fc > sc else "BEARISH_ALIGNMENT"
    spread = ((fc-sc)/sc)*100 if sc else 0
    return {"cross":cross,"alignment":align,"fast_ema":round(fc,8),"slow_ema":round(sc,8),
            "spread_pct":round(spread,4),"is_fresh_cross":cross != "NONE"}

def calc_volume_ratio(data, p=20):
    if len(data) < p+1: return None
    v = volumes(data); avg = sum(v[-p-1:-1])/p
    return round(v[-1]/avg, 4) if avg else None

def calculate_indicators(data):
    if len(data) < 50: return None
    c = closes(data); price = c[-1]
    r = {"price":price,"ema9":ema(c,9),"ema21":ema(c,21),"ema50":ema(c,50),
         "ema200":ema(c,200),"sma20":sma(c,20),"rsi":calc_rsi(c),"macd":calc_macd(c),
         "bollinger":calc_bollinger(c),"stochastic":calc_stochastic(data),
         "atr":calc_atr(data),"adx":calc_adx(data),"ema_cross":calc_ema_cross(c),
         "volume_ratio":calc_volume_ratio(data)}
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


# === V8 PULLBACK (3 candles) ===

def detect_pullback(data, direction):
    result = {"is_pullback":False,"type":"","quality":0,"reason":""}
    if len(data) < 55: return result
    c = closes(data); h = highs(data); l = lows(data); price = c[-1]
    ema21 = ema(c,21); ema50 = ema(c,50)
    if not ema21 or not ema50: return result
    rl = min(l[-3:]); rh = max(h[-3:])
    bu = c[-1] > c[-2] and c[-1] > c[-3]
    bd = c[-1] < c[-2] and c[-1] < c[-3]

    if direction == "BULLISH":
        if rl <= ema50*1.008 and bu and price > ema50:
            result = {"is_pullback":True,"type":"EMA50_BOUNCE","quality":3,"reason":"ارتداد من EMA50"}
        elif rl <= ema21*1.008 and bu and price > ema21:
            result = {"is_pullback":True,"type":"EMA21_BOUNCE","quality":2,"reason":"ارتداد من EMA21"}
        bb = calc_bollinger(c)
        if bb and rl <= bb["lower"]*1.005 and c[-1] > bb["lower"] and not result["is_pullback"]:
            result = {"is_pullback":True,"type":"BB_BOUNCE","quality":2,"reason":"ارتداد من BB"}
    elif direction == "BEARISH":
        if rh >= ema50*0.992 and bd and price < ema50:
            result = {"is_pullback":True,"type":"EMA50_REJECT","quality":3,"reason":"رفض من EMA50"}
        elif rh >= ema21*0.992 and bd and price < ema21:
            result = {"is_pullback":True,"type":"EMA21_REJECT","quality":2,"reason":"رفض من EMA21"}
        bb = calc_bollinger(c)
        if bb and rh >= bb["upper"]*0.995 and c[-1] < bb["upper"] and not result["is_pullback"]:
            result = {"is_pullback":True,"type":"BB_REJECT","quality":2,"reason":"رفض من BB"}
    return result


# === V8 TREND ENGINE ===

def confirm_trend(i1m, i1h, i1d):
    result = TrendConfirmation(); reasons = []; score = 0

    if i1d:
        ec = i1d.get("ema_cross"); adx = i1d.get("adx")
        if ec:
            if ec["alignment"] == "BULLISH_ALIGNMENT":
                result.daily_trend = "BULLISH"; score += 2; reasons.append("1D: EMA50>EMA200")
            else:
                result.daily_trend = "BEARISH"; score -= 2; reasons.append("1D: EMA50<EMA200")
            if ec["cross"] == "GOLDEN_CROSS": score += 1; reasons.append("1D: Golden Cross!")
            elif ec["cross"] == "DEATH_CROSS": score -= 1; reasons.append("1D: Death Cross!")
        else:
            e50 = i1d.get("ema50"); p = i1d.get("price",0)
            if e50 and p:
                if p > e50: result.daily_trend = "BULLISH"; score += 1; reasons.append("1D: P>EMA50")
                else: result.daily_trend = "BEARISH"; score -= 1; reasons.append("1D: P<EMA50")
        if adx:
            if adx["weak"]: reasons.append(f"1D: ADX weak ({adx['adx']})")
            elif adx["strong"]: reasons.append(f"1D: ADX strong ({adx['adx']})")

    if i1h:
        e21 = i1h.get("ema21"); e50 = i1h.get("ema50"); p = i1h.get("price",0); adx = i1h.get("adx")
        if e21 and e50:
            if e21 > e50 and p > e21:
                result.hourly_trend = "BULLISH"; score += 2; reasons.append("1H: P>EMA21>EMA50")
            elif e21 < e50 and p < e21:
                result.hourly_trend = "BEARISH"; score -= 2; reasons.append("1H: P<EMA21<EMA50")
            elif p > e21:
                result.hourly_trend = "WEAK_BULLISH"; score += 1; reasons.append("1H: P>EMA21 (weak)")
            elif p < e21:
                result.hourly_trend = "WEAK_BEARISH"; score -= 1; reasons.append("1H: P<EMA21 (weak)")
            else:
                result.hourly_trend = "MIXED"; reasons.append("1H: mixed")
        if adx:
            if adx["adx"] >= CFG.adx_threshold:
                if adx["trend"] == "bullish": score += 1; reasons.append(f"1H: +DI>-DI (ADX={adx['adx']})")
                elif adx["trend"] == "bearish": score -= 1; reasons.append(f"1H: -DI>+DI (ADX={adx['adx']})")
            else:
                reasons.append(f"1H: ADX={adx['adx']} (low)")

    if i1m:
        rsi = i1m.get("rsi"); stoch = i1m.get("stochastic")
        if rsi is not None:
            if rsi < 30: result.minute_timing = "OVERSOLD"; reasons.append(f"1M: RSI={rsi:.0f} oversold")
            elif rsi > 70: result.minute_timing = "OVERBOUGHT"; reasons.append(f"1M: RSI={rsi:.0f} overbought")
            else: result.minute_timing = "NEUTRAL"
        if stoch:
            if stoch["k"] < 20 and stoch["k"] > stoch["d"]: reasons.append("1M: Stoch bull cross")
            elif stoch["k"] > 80 and stoch["k"] < stoch["d"]: reasons.append("1M: Stoch bear cross")

    result.reasons = reasons

    db_ = result.daily_trend == "BULLISH"
    dbe = result.daily_trend == "BEARISH"
    hb = result.hourly_trend in ("BULLISH","WEAK_BULLISH")
    hbe = result.hourly_trend in ("BEARISH","WEAK_BEARISH")

    if db_ and hbe:
        result.direction = TrendDirection.NEUTRAL; result.entry_quality = EntryQuality.CAUTION
        result.strength = 20; reasons.append("1D bull + 1H bear -> caution")
    elif dbe and hb:
        result.direction = TrendDirection.NEUTRAL; result.entry_quality = EntryQuality.CAUTION
        result.strength = 20; reasons.append("1D bear + 1H bull -> caution")
    elif score >= 3:
        result.direction = TrendDirection.BULLISH; result.strength = min(score*15, 100)
    elif score <= -3:
        result.direction = TrendDirection.BEARISH; result.strength = min(abs(score)*15, 100)
    elif score >= 1:
        result.direction = TrendDirection.BULLISH; result.strength = score*10
        result.entry_quality = EntryQuality.CAUTION; reasons.append("weak bull")
    elif score <= -1:
        result.direction = TrendDirection.BEARISH; result.strength = abs(score)*10
        result.entry_quality = EntryQuality.CAUTION; reasons.append("weak bear")
    else:
        result.direction = TrendDirection.NEUTRAL; result.strength = 0
        result.entry_quality = EntryQuality.BLOCKED; reasons.append("no direction")

    if result.direction in (TrendDirection.BULLISH, TrendDirection.BEARISH):
        if result.entry_quality != EntryQuality.CAUTION:
            w1 = i1d and i1d.get("adx",{}).get("weak",False)
            w2 = i1h and i1h.get("adx",{}).get("weak",False)
            if w1 and w2:
                result.entry_quality = EntryQuality.CAUTION; reasons.append("ADX weak both")
            else:
                result.entry_quality = EntryQuality.ALLOWED
    return result


# === V8 SIGNAL ENGINE (MAX=12) ===

class SignalEngine:
    MAX_SCORE = 12

    def evaluate(self, symbol, trend, i1m, i1h, i1d, data_1h=None):
        r = SignalResult(); r.max_score = self.MAX_SCORE
        bs, ss = 0, 0; sigs = []; reasons = []

        if trend.entry_quality == EntryQuality.BLOCKED:
            r.decision = Decision.WAIT; r.reasons = ["BLOCKED"] + trend.reasons; return r

        if trend.direction == TrendDirection.NEUTRAL:
            if trend.entry_quality == EntryQuality.CAUTION:
                reasons.append("neutral/caution")
            else:
                r.decision = Decision.WAIT; r.reasons = ["neutral"] + trend.reasons; return r

        if trend.direction == TrendDirection.BULLISH: bs += 2; sigs.append("TREND_BULL")
        elif trend.direction == TrendDirection.BEARISH: ss += 2; sigs.append("TREND_BEAR")
        elif trend.direction == TrendDirection.NEUTRAL:
            if trend.hourly_trend in ("BULLISH","WEAK_BULLISH"): bs += 1; sigs.append("WEAK_BULL")
            elif trend.hourly_trend in ("BEARISH","WEAK_BEARISH"): ss += 1; sigs.append("WEAK_BEAR")

        if i1h and i1h.get("adx"):
            a = i1h["adx"]
            if a["adx"] >= CFG.adx_threshold:
                if a["trend"] == "bullish": bs += 2; sigs.append(f"ADX_{a['adx']}")
                elif a["trend"] == "bearish": ss += 2; sigs.append(f"ADX_{a['adx']}")

        if i1h and i1h.get("macd"):
            if i1h["macd"]["trend"] == "bullish": bs += 1; sigs.append("MACD_B")
            elif i1h["macd"]["trend"] == "bearish": ss += 1; sigs.append("MACD_S")

        if i1h and i1h.get("volume_ratio"):
            vr = i1h["volume_ratio"]
            if vr and vr > 1.2:
                if trend.direction == TrendDirection.BULLISH or trend.hourly_trend in ("BULLISH","WEAK_BULLISH"):
                    bs += 1; sigs.append(f"VOL_{vr}")
                elif trend.direction == TrendDirection.BEARISH or trend.hourly_trend in ("BEARISH","WEAK_BEARISH"):
                    ss += 1; sigs.append(f"VOL_{vr}")

        if i1h and i1h.get("rsi") is not None:
            rsi = i1h["rsi"]
            ib = trend.direction == TrendDirection.BULLISH or trend.hourly_trend in ("BULLISH","WEAK_BULLISH")
            ise = trend.direction == TrendDirection.BEARISH or trend.hourly_trend in ("BEARISH","WEAK_BEARISH")
            if ib and 40 <= rsi <= 68: bs += 1; sigs.append(f"RSI_B_{rsi:.0f}")
            elif ise and 32 <= rsi <= 60: ss += 1; sigs.append(f"RSI_S_{rsi:.0f}")

        if i1h:
            p, e21 = i1h.get("price",0), i1h.get("ema21")
            if e21 and p:
                if p > e21: bs += 1; sigs.append("P>EMA21")
                elif p < e21: ss += 1; sigs.append("P<EMA21")

        if i1h and i1h.get("stochastic"):
            st = i1h["stochastic"]
            if st["k"] > st["d"]: bs += 1; sigs.append("STOCH_K>D")
            elif st["k"] < st["d"]: ss += 1; sigs.append("STOCH_K<D")

        if i1h and i1h.get("ema_cross"):
            ec = i1h["ema_cross"]
            if ec["is_fresh_cross"]:
                if ec["cross"] == "GOLDEN_CROSS": bs += 1; sigs.append("GOLDEN!")
                elif ec["cross"] == "DEATH_CROSS": ss += 1; sigs.append("DEATH!")
            elif ec["alignment"] == "BULLISH_ALIGNMENT": bs += 1
            elif ec["alignment"] == "BEARISH_ALIGNMENT": ss += 1

        if data_1h and len(data_1h) >= 55:
            dpb = trend.direction.value
            if dpb == "NEUTRAL":
                if trend.hourly_trend in ("BULLISH","WEAK_BULLISH"): dpb = "BULLISH"
                elif trend.hourly_trend in ("BEARISH","WEAK_BEARISH"): dpb = "BEARISH"
            pb = detect_pullback(data_1h, dpb)
            if pb["is_pullback"]:
                r.is_pullback = True
                if dpb == "BULLISH": bs += 2; sigs.append(f"PB_{pb['type']}"); reasons.append(pb["reason"])
                elif dpb == "BEARISH": ss += 2; sigs.append(f"PB_{pb['type']}"); reasons.append(pb["reason"])

        if i1h and i1h.get("rsi") is not None:
            rsi = i1h["rsi"]
            sk = i1h.get("stochastic",{}).get("k",50) if i1h.get("stochastic") else 50
            av = i1h.get("adx",{}).get("adx",0) if i1h.get("adx") else 0
            if rsi >= CFG.rsi_extreme_overbought:
                bs = max(0, bs-4); reasons.append(f"RSI={rsi:.1f} BLOCK")
            elif rsi >= CFG.rsi_caution_overbought:
                if av >= CFG.adx_strong and sk > 50: reasons.append(f"RSI={rsi:.1f} momentum ok")
                else: bs = max(0, bs-1); reasons.append(f"RSI={rsi:.1f} -1")
            if rsi <= CFG.rsi_extreme_oversold:
                ss = max(0, ss-4); reasons.append(f"RSI={rsi:.1f} BLOCK SELL")
            elif rsi <= CFG.rsi_caution_oversold:
                if av >= CFG.adx_strong and sk < 50: reasons.append(f"RSI={rsi:.1f} bear momentum")
                else: ss = max(0, ss-1); reasons.append(f"RSI={rsi:.1f} -1")

        if i1h and i1h.get("stochastic"):
            st = i1h["stochastic"]
            if st["k"] > CFG.stoch_overbought and st["k"] < st["d"]:
                bs = max(0, bs-1); reasons.append(f"Stoch K<D at {st['k']:.0f}")

        r.buy_score = min(bs, self.MAX_SCORE); r.sell_score = min(ss, self.MAX_SCORE)
        r.signals = sigs; r.reasons = reasons + trend.reasons

        ibe = trend.direction == TrendDirection.BULLISH or \
              (trend.direction == TrendDirection.NEUTRAL and trend.hourly_trend in ("BULLISH","WEAK_BULLISH"))
        ise = trend.direction == TrendDirection.BEARISH or \
              (trend.direction == TrendDirection.NEUTRAL and trend.hourly_trend in ("BEARISH","WEAK_BEARISH"))

        if ibe:
            if bs >= CFG.min_score_to_enter: r.decision = Decision.BUY; self._sltp(r, i1h)
            else: r.decision = Decision.WAIT; reasons.append(f"BUY={bs}/{CFG.min_score_to_enter} low")
        elif ise:
            if ss >= CFG.min_score_to_enter: r.decision = Decision.SELL; self._sltp(r, i1h)
            else: r.decision = Decision.WAIT; reasons.append(f"SELL={ss}/{CFG.min_score_to_enter} low")
        else:
            r.decision = Decision.WAIT
        return r

    def _sltp(self, r, i1h):
        if i1h and i1h.get("atr") and i1h.get("price"):
            ap = (i1h["atr"]/i1h["price"])*100
            r.sl_percent = max(0.5, min(ap*1.5, CFG.max_sl_percent))
            r.tp_percent = max(1.0, min(ap*3.0, CFG.max_tp_percent))
        else:
            r.sl_percent = 2.0; r.tp_percent = 4.0


signal_engine = SignalEngine()


class AIAnalyst:
    def analyze(self, symbol, signal, trend, i1h, i1d):
        result = AIAnalysis()
        if signal.decision == Decision.WAIT: result.regime = "no_entry"; return result
        prompt = f"اشرح فقط. العملة:{symbol} القرار:{signal.decision.value} Score:B={signal.buy_score} S={signal.sell_score}/{signal.max_score} RSI:{i1h.get('rsi') if i1h else 'N/A'} ADX:{i1h.get('adx') if i1h else 'N/A'} أجب JSON: {{\"regime\":\"...\",\"explanation\":\"...\",\"risk_warnings\":[],\"agrees_with_signal\":true}}"
        try:
            comp = ai_client.chat.completions.create(model=CFG.ai_model,
                messages=[{"role":"user","content":prompt}], temperature=0, max_tokens=300, stream=False)
            raw = comp.choices[0].message.content or ""
            cleaned = raw.strip()
            if cleaned.startswith("```"):
                lines = [l for l in cleaned.split("\n") if not l.strip().startswith("```")]
                cleaned = "\n".join(lines).strip()
            dj = json.loads(cleaned)
            result.regime = str(dj.get("regime","unknown"))
            result.explanation = str(dj.get("explanation",""))
            result.risk_warnings = dj.get("risk_warnings",[])
            result.agreement = bool(dj.get("agrees_with_signal",True))
            result.raw_response = raw
        except Exception as e:
            logger.warning(f"AI: {e}")
        return result

ai_analyst = AIAnalyst()


class MarketScanner:
    def __init__(self): self._running = True

    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()
        logger.info("Scanner V8")

    def stop(self): self._running = False

    def _loop(self):
        time.sleep(5)
        while self._running:
            try: self._scan_cycle()
            except Exception as e: logger.error(f"Scanner: {e}", exc_info=True)
            time.sleep(CFG.scanner_interval)

    def _scan_cycle(self):
        logger.info("=" * 60)
        logger.info("Scanning...")
        candidates = []
        for sk, sym in CFG.watchlist.items():
            try:
                r = self._quick_scan(sk, sym)
                if r: candidates.append(r)
            except: pass
            time.sleep(0.3)
        candidates.sort(key=lambda x: x.score, reverse=True)
        top = candidates[:CFG.scanner_top_n]
        logger.info(f"Scanned {len(CFG.watchlist)} -> {len(candidates)} -> Top {len(top)}")
        for i, c in enumerate(top):
            logger.info(f"  #{i+1} {c.symbol} | Score={c.score:.1f} | Vol={c.volume_usdt/1e6:.1f}M | ATR={c.atr_pct:.2f}% | {c.trend_1h} | RSI={c.rsi_1h:.1f}")
        bot_stats["scanner_candidates"] = [{"symbol":c.symbol,"score":c.score} for c in top]
        with active_lock:
            active_symbols.clear()
            for c in top:
                active_symbols[c.symbol_key] = c.symbol
                candle_mgr.ensure_symbol(c.symbol_key, CFG.timeframes)
        for c in top:
            pos = get_current_position(c.symbol)
            if pos == "ERROR" or pos: continue
            threading.Thread(target=self._deep_analysis, args=(c.symbol_key,c.symbol), daemon=True).start()
            time.sleep(1)

    def _quick_scan(self, sk, sym):
        result = ScannerResult(symbol_key=sk, symbol=sym); reasons = []
        try:
            ticker = exchange.fetch_ticker(sym)
            vol = float(ticker.get("quoteVolume",0) or 0)
            result.volume_usdt = vol
            if vol < CFG.scanner_min_volume_usdt: return None
            reasons.append(f"Vol={vol/1e6:.1f}M")
        except: return None
        try:
            ohlcv = exchange.fetch_ohlcv(sym, "1h", limit=50)
            if len(ohlcv) < 20: return None
            atr = calc_atr(ohlcv); price = float(ohlcv[-1][4])
            if atr and price:
                ap = (atr/price)*100; result.atr_pct = ap
                if ap < CFG.scanner_min_atr_pct: return None
                reasons.append(f"ATR={ap:.2f}%")
        except: return None
        try:
            c = closes(ohlcv); e21 = ema(c,21); e50 = ema(c,50)
            if e21 and e50:
                if e21 > e50 and price > e21: result.trend_1h = "BULLISH"; result.score += 3; reasons.append("1H BULL")
                elif e21 < e50 and price < e21: result.trend_1h = "BEARISH"; result.score += 3; reasons.append("1H BEAR")
                else: result.trend_1h = "MIXED"; result.score += 1
        except: pass
        try:
            rsi = calc_rsi(c)
            if rsi is not None:
                result.rsi_1h = rsi
                if rsi > CFG.rsi_extreme_overbought or rsi < CFG.rsi_extreme_oversold: return None
                elif 40 <= rsi <= 68: result.score += 2; reasons.append(f"RSI={rsi:.0f}")
                elif rsi > CFG.rsi_caution_overbought: result.score += 1; reasons.append(f"RSI={rsi:.0f} high")
        except: pass
        try:
            if len(ohlcv) >= 2:
                chg = ((price - float(ohlcv[-2][4]))/float(ohlcv[-2][4]))*100
                result.change_1h_pct = chg
                if abs(chg) > 0.3: result.score += 1; reasons.append(f"1H d={chg:.2f}%")
        except: pass
        if result.volume_usdt > 50_000_000: result.score += 1; reasons.append("HighVol")
        result.reasons = reasons
        return result

    def _deep_analysis(self, sk, sym):
        logger.info(f"Deep: {sym}")
        if candle_mgr.get_closed_count(sk, "1h") < 50:
            self._load_candles(sk, sym)
        d1m = candle_mgr.get_closed(sk,"1m"); d1h = candle_mgr.get_closed(sk,"1h"); d1d = candle_mgr.get_closed(sk,"1d")
        if len(d1h) < 50 or len(d1d) < 50:
            logger.info(f"Not enough data: {sym} (1H={len(d1h)},1D={len(d1d)})"); return
        i1m = calculate_indicators(d1m) if len(d1m) >= 50 else None
        i1h = calculate_indicators(d1h); i1d = calculate_indicators(d1d)
        if not i1h or not i1d: logger.info(f"Indicators fail: {sym}"); return

        trend = confirm_trend(i1m, i1h, i1d)

        logger.info(f">>> {sym} | 1D={trend.daily_trend} | 1H={trend.hourly_trend} | 1M={trend.minute_timing} | DIR={trend.direction.value} | QUALITY={trend.entry_quality.value} | STR={trend.strength}%")
        for reason in trend.reasons:
            logger.info(f"   - {reason}")

        signal = signal_engine.evaluate(sym, trend, i1m, i1h, i1d, d1h)

        logger.info(f"<<< {sym}: {signal.decision.value} | B={signal.buy_score} S={signal.sell_score}/{signal.max_score} | PB={signal.is_pullback} | Signals={signal.signals}")

        if signal.decision == Decision.WAIT:
            bot_stats["last_analysis"][sym] = {"decision":"WAIT",
                "scores":f"B:{signal.buy_score}/S:{signal.sell_score}/{signal.max_score}",
                "trend":trend.direction.value,"quality":trend.entry_quality.value,
                "time":datetime.now(timezone.utc).isoformat()}
            return

        ai = ai_analyst.analyze(sym, signal, trend, i1h, i1d)
        if not ai.agreement:
            logger.warning(f"AI disagrees: {sym}"); return

        if signal.decision in (Decision.BUY, Decision.SELL):
            execute_trade(sym, signal, ai)

        bot_stats["last_analysis"][sym] = {"decision":signal.decision.value,
            "scores":f"B:{signal.buy_score}/S:{signal.sell_score}/{signal.max_score}",
            "pullback":signal.is_pullback,"ai":ai.regime,
            "time":datetime.now(timezone.utc).isoformat()}

    def _load_candles(self, sk, sym):
        for tf in CFG.timeframes:
            try:
                limit = 500 if tf == "1d" else 300
                data = exchange.fetch_ohlcv(sym, timeframe=tf, limit=limit)
                candle_mgr.load_initial(sk, tf, data)
            except Exception as e: logger.warning(f"Load {sym} {tf}: {e}")
            time.sleep(0.3)


def get_current_position(symbol):
    try:
        for p in exchange.fetch_positions([symbol]):
            ct = p.get("contracts")
            if ct and float(ct) > 0: return p
        return None
    except Exception as e:
        logger.error(f"Pos check {symbol}: {e}"); return "ERROR"

def check_limits():
    if db.count_today_trades() >= CFG.max_daily_trades: return False, "daily limit"
    if db.get_open_count() >= CFG.max_open_positions: return False, "positions full"
    return True, ""

def emergency_close(symbol, reason):
    logger.critical(f"EMERGENCY: {symbol} | {reason}")
    try:
        pos = get_current_position(symbol)
        if pos and pos != "ERROR":
            ct = float(pos.get("contracts",0)); side = pos.get("side","")
            if ct > 0:
                cs = "sell" if side == "long" else "buy"
                exchange.create_market_order(symbol, cs, ct, params={"reduceOnly":True})
    except Exception as e: logger.critical(f"Emergency fail: {e}")


def execute_trade(symbol, signal, ai):
    with execution_lock:
        try:
            pos = get_current_position(symbol)
            if pos == "ERROR": return
            if pos: logger.info(f"Busy: {symbol}"); return
            ok, reason = check_limits()
            if not ok: logger.info(f"Limited: {reason}"); return
            st = trade_state.get(symbol, {})
            if time.time() - st.get("last_trade_time",0) < CFG.cooldown_seconds:
                logger.info(f"Cooldown: {symbol}"); return

            ticker = exchange.fetch_ticker(symbol); price = ticker["last"]
            raw_qty = (CFG.margin_usdt * CFG.leverage) / price
            qty = float(exchange.amount_to_precision(symbol, raw_qty))
            side = "buy" if signal.decision == Decision.BUY else "sell"
            pname = "LONG" if side == "buy" else "SHORT"
            mode = "PAPER" if CFG.dry_run else "LIVE"

            logger.info(f"TRADE {symbol} | {pname} | {price} | {mode} | PB={signal.is_pullback} | Score={signal.buy_score}/{signal.sell_score}/{signal.max_score}")

            if CFG.dry_run:
                db.insert_trade(TradeRecord(symbol=symbol, side=pname, entry_price=price,
                    quantity=qty, confidence=max(signal.buy_score, signal.sell_score),
                    reason=f"B={signal.buy_score}/S={signal.sell_score}/{signal.max_score} PB={signal.is_pullback}",
                    timestamp=datetime.now(timezone.utc).isoformat(), mode="PAPER"))
                st["last_trade_time"] = time.time(); return

            exchange.set_leverage(CFG.leverage, symbol)
            order = exchange.create_market_order(symbol, side, qty)
            entry_oid = order.get("id",""); time.sleep(1)

            p = get_current_position(symbol)
            if p == "ERROR" or p is None:
                logger.critical(f"No position after order: {symbol}"); return
            entry = float(p.get("entryPrice", price))
            actual_qty = abs(float(p.get("contracts",0)))
            if actual_qty <= 0:
                logger.critical(f"Zero qty: {symbol}"); emergency_close(symbol,"zero qty"); return
            logger.info(f"actual_qty={actual_qty}")

            sl_p = max(0.5, min(signal.sl_percent, CFG.max_sl_percent))
            tp_p = max(1.0, min(signal.tp_percent, CFG.max_tp_percent))
            if side == "buy":
                sl_price = entry*(1-sl_p/100); tp_price = entry*(1+tp_p/100)
            else:
                sl_price = entry*(1+sl_p/100); tp_price = entry*(1-tp_p/100)
            sl_price = float(exchange.price_to_precision(symbol, sl_price))
            tp_price = float(exchange.price_to_precision(symbol, tp_price))
            cs = "sell" if side == "buy" else "buy"

            sl_oid = ""
            try:
                sl_o = exchange.create_order(symbol,"STOP_MARKET",cs,actual_qty,None,
                    {"stopPrice":sl_price,"reduceOnly":True,"workingType":"MARK_PRICE"})
                sl_oid = sl_o.get("id",""); logger.info(f"SL: {sl_price}")
            except Exception as e:
                logger.critical(f"SL fail: {e}"); emergency_close(symbol,"SL fail"); return

            tp_oid = ""
            try:
                tp_o = exchange.create_order(symbol,"TAKE_PROFIT_MARKET",cs,actual_qty,None,
                    {"stopPrice":tp_price,"reduceOnly":True,"workingType":"MARK_PRICE"})
                tp_oid = tp_o.get("id",""); logger.info(f"TP: {tp_price}")
            except Exception as e:
                logger.error(f"TP fail: {e}")
                try: exchange.cancel_order(sl_oid, symbol)
                except: pass
                emergency_close(symbol,"TP fail"); return

            tid = db.insert_trade(TradeRecord(symbol=symbol, side=pname, entry_price=entry,
                quantity=actual_qty, sl_price=sl_price, tp_price=tp_price,
                sl_order_id=sl_oid, tp_order_id=tp_oid, entry_order_id=entry_oid,
                confidence=max(signal.buy_score, signal.sell_score),
                reason=f"B={signal.buy_score}/S={signal.sell_score}/{signal.max_score} PB={signal.is_pullback}",
                timestamp=datetime.now(timezone.utc).isoformat(), mode="LIVE"))
            logger.info(f"Trade #{tid}"); st["last_trade_time"] = time.time()
        except Exception as e:
            logger.error(f"Exec: {e}", exc_info=True); emergency_close(symbol, str(e))


class PositionMonitor:
    def __init__(self): self._running = True
    def start(self): threading.Thread(target=self._loop, daemon=True).start(); logger.info("Monitor V8")
    def stop(self): self._running = False

    def _loop(self):
        while self._running:
            try:
                for t in db.get_open_trades():
                    if t.get("mode") == "PAPER": continue
                    self._check(t)
            except Exception as e: logger.error(f"Monitor: {e}")
            time.sleep(CFG.monitor_interval)

    def _check(self, trade):
        sym = trade["symbol"]
        sl_st = self._ost(sym, trade.get("sl_order_id"))
        tp_st = self._ost(sym, trade.get("tp_order_id"))
        pos = get_current_position(sym)
        if pos == "ERROR": return
        if pos is None:
            reason = "STOP_LOSS" if sl_st == "closed" else "TAKE_PROFIT" if tp_st == "closed" else "MANUAL"
            ep, rpnl, comm = self._rexit(sym, trade)
            entry = trade["entry_price"]; qty = trade["quantity"]
            if ep == 0: ep = entry
            if rpnl == 0:
                cs = self._csz(sym); rq = qty * cs
                rpnl = (ep-entry)*rq if trade["side"] == "LONG" else (entry-ep)*rq
            notional = entry*qty if entry*qty else 1
            pp = (rpnl/notional)*100
            db.close_trade(trade["id"], ep, rpnl, pp, comm, reason)
            logger.info(f"CLOSED {sym} | {reason} | PnL={rpnl:.4f} ({pp:.2f}%) | Comm={comm:.4f}")
            self._cancel(sym, trade)

    def _csz(self, sym):
        try: return float(exchange.market(sym).get("contractSize",1) or 1)
        except: return 1.0

    def _ost(self, sym, oid):
        if not oid: return "unknown"
        try: return exchange.fetch_order(oid, sym).get("status","unknown")
        except: return "unknown"

    def _rexit(self, sym, trade):
        ep, rpnl, comm = 0, 0, 0
        try:
            trades = exchange.fetch_my_trades(sym, limit=30)
            for t in reversed(trades):
                if t.get("reduceOnly") or (t.get("side") == "sell" and trade["side"] == "LONG") or (t.get("side") == "buy" and trade["side"] == "SHORT"):
                    ep = float(t.get("price",0) or t.get("average",0))
                    comm = float(t.get("fee",{}).get("cost",0) or 0)
                    info = t.get("info",{}); rs = info.get("realizedPnl","0")
                    rpnl = float(rs) if rs else 0; break
        except: pass
        if ep == 0:
            try: ep = exchange.fetch_ticker(sym)["last"]
            except: pass
        return ep, rpnl, comm

    def _cancel(self, sym, trade):
        for oid in [trade.get("sl_order_id"), trade.get("tp_order_id")]:
            if not oid: continue
            try:
                if self._ost(sym, oid) == "open": exchange.cancel_order(oid, sym)
            except: pass


async def websocket_worker():
    delay = CFG.ws_reconnect_delay
    while True:
        with active_lock: current = dict(active_symbols)
        if not current: await asyncio.sleep(10); continue
        streams = []
        for sk in current:
            for tf in CFG.timeframes: streams.append(f"{sk}@kline_{tf}")
        url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
        try:
            async with websockets.connect(url, ping_interval=CFG.ws_ping_interval, ping_timeout=CFG.ws_ping_timeout) as ws:
                logger.info(f"WS connected ({len(current)} coins)"); delay = CFG.ws_reconnect_delay
                async for msg in ws:
                    data = json.loads(msg); k = data.get("data",{}).get("k")
                    if not k: continue
                    sk = k["s"].lower(); tf = k["i"]
                    candle = [k["t"],float(k["o"]),float(k["h"]),float(k["l"]),float(k["c"]),float(k["v"])]
                    candle_mgr.update(sk, tf, candle, k["x"])
        except Exception as e: logger.error(f"WS: {e}")
        await asyncio.sleep(delay); delay = min(delay*2, 120)


def main():
    logger.critical(f"BUILD: {V8_BUILD} | MAX_SCORE={SignalEngine.MAX_SCORE} | dry_run={CFG.dry_run}")
    logger.info("=" * 60)
    logger.info(f"AI TRADING BOT {V8_BUILD}")
    logger.info(f"   Mode: {'PAPER' if CFG.dry_run else 'LIVE'}")
    logger.info(f"   Watchlist: {len(CFG.watchlist)} | Scanner: {CFG.scanner_interval}s -> Top {CFG.scanner_top_n}")
    logger.info(f"   MinScore: {CFG.min_score_to_enter}/{SignalEngine.MAX_SCORE}")
    logger.info(f"   RSI Block: >{CFG.rsi_extreme_overbought} / <{CFG.rsi_extreme_oversold}")
    logger.info(f"   ADX: {CFG.adx_threshold} | MaxOpen: {CFG.max_open_positions} | MaxDaily: {CFG.max_daily_trades}")
    logger.info("=" * 60)

    try: CFG.validate()
    except ValueError as e: logger.critical(f"Config: {e}"); return

    try:
        ip = requests.get("https://api.ipify.org", timeout=10).text
        logger.info(f"IP: {ip}")
    except: pass

    threading.Thread(target=run_server, daemon=True).start(); time.sleep(2)

    try:
        t = exchange.fetch_ticker("BTC/USDT:USDT")
        logger.info(f"Binance OK | BTC: {t['last']}")
    except Exception as e: logger.critical(f"Binance: {e}"); return

    logger.info("Loading initial data...")
    for sk, sym in CFG.watchlist.items():
        candle_mgr.ensure_symbol(sk, CFG.timeframes)
        for tf in CFG.timeframes:
            try:
                limit = 500 if tf == "1d" else 300
                data = exchange.fetch_ohlcv(sym, timeframe=tf, limit=limit)
                candle_mgr.load_initial(sk, tf, data)
            except Exception as e: logger.debug(f"  {sym} {tf}: {e}")
            time.sleep(0.2)
    logger.info("Data ready")

    monitor = PositionMonitor(); monitor.start()
    scanner = MarketScanner(); scanner.start()
    bot_stats["status"] = "RUNNING"

    try: asyncio.run(websocket_worker())
    except KeyboardInterrupt:
        logger.info("Shutdown"); scanner.stop(); monitor.stop()


if __name__ == "__main__":
    main()
