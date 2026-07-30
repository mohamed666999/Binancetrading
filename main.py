#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║     APEX TRADING BOT v3.0 — Multi-Layer Signal Fusion        ║
║                                                              ║
║  Architecture:                                               ║
║  • Layer 1: 9 Independent Signal Modules (APEX Classic)      ║
║  • Layer 2: Technical Indicators Engine (RSI/MACD/EMA/BB)    ║
║  • Layer 3: Market Structure (S/R, Volume Profile, HH/HL)    ║
║  • Layer 4: Derivatives Intelligence (OI/Funding/LSR/Flow)   ║
║  • Layer 5: Regime Classifier (9 regimes, adaptive weights)  ║
║  • Layer 6: Multi-Timeframe Alignment                        ║
║  • Layer 7: AI Veto / Explainer (15% weight)                 ║
║                                                              ║
║  Merged from: APEX v1 + MSSI v2 + APEX v3 Technical Layer    ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio, json, time, threading, math, os, sqlite3, logging
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Any
from enum import Enum
import websockets, ccxt, requests
from flask import Flask, jsonify, render_template_string
from openai import OpenAI

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"{LOG_DIR}/apex_{datetime.now():%Y%m%d}.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("APEX")

FAPI = "https://fapi.binance.com"


def clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, float(x)))

def safe_div(a, b, default=0.0):
    return a / b if abs(b) > 1e-12 else default

def _mean(v):
    return sum(v) / len(v) if v else 0.0

def _std(v):
    if len(v) < 2:
        return 0.0
    m = _mean(v)
    return math.sqrt(sum((x - m) ** 2 for x in v) / len(v))

def _zscore(val, arr):
    s = _std(arr)
    return (val - _mean(arr)) / s if s > 0 else 0.0

def _sigmoid(x):
    if x >= 0:
        z = math.exp(-x)
        return 1 / (1 + z)
    z = math.exp(x)
    return z / (1 + z)

def scale_signed_to_100(x):
    return clamp((x + 1.0) * 50.0, 0.0, 100.0)

def _ema(data, period):
    if not data or period <= 0:
        return []
    k = 2.0 / (period + 1)
    result = [data[0]]
    for i in range(1, len(data)):
        result.append(data[i] * k + result[-1] * (1 - k))
    return result

def _sma(data, period):
    if len(data) < period:
        return _mean(data)
    return _mean(data[-period:])

def _rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = _mean(gains[-period:])
    avg_loss = _mean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def _macd(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow + signal:
        return 0.0, 0.0, 0.0
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    min_len = min(len(ema_fast), len(ema_slow))
    if min_len < signal:
        return 0.0, 0.0, 0.0
    macd_line = [ema_fast[-(min_len - i)] - ema_slow[-(min_len - i)] for i in range(min_len)]
    signal_line = _ema(macd_line, signal)
    if not signal_line:
        return 0.0, 0.0, 0.0
    m = macd_line[-1]
    s = signal_line[-1]
    return m, s, m - s

def _bollinger(closes, period=20, std_mult=2.0):
    if len(closes) < period:
        mid = _mean(closes)
        return mid, mid, mid, 0.5, 0.0
    recent = closes[-period:]
    mid = _mean(recent)
    std = _std(recent)
    upper = mid + std_mult * std
    lower = mid - std_mult * std
    price = closes[-1]
    bw = safe_div(upper - lower, mid) * 100
    pct_b = safe_div(price - lower, upper - lower) if upper != lower else 0.5
    return upper, mid, lower, clamp(pct_b, -0.5, 1.5), bw

def _atr(highs, lows, closes, period=14):
    if len(closes) < 2:
        return 0.0
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    return _mean(trs[-period:]) if trs else 0.0

def _stochastic(highs, lows, closes, k_period=14):
    if len(closes) < k_period:
        return 50.0, 50.0
    hh = max(highs[-k_period:])
    ll = min(lows[-k_period:])
    if hh == ll:
        return 50.0, 50.0
    k = (closes[-1] - ll) / (hh - ll) * 100
    k_vals = []
    for j in range(min(3, len(closes) - k_period + 1)):
        idx = len(closes) - j
        hh_j = max(highs[idx - k_period:idx])
        ll_j = min(lows[idx - k_period:idx])
        if hh_j != ll_j:
            k_vals.append((closes[idx - 1] - ll_j) / (hh_j - ll_j) * 100)
    d = _mean(k_vals) if k_vals else k
    return clamp(k, 0, 100), clamp(d, 0, 100)

def _adx(highs, lows, closes, period=14):
    if len(closes) < period + 2:
        return 20.0, 0.0, 0.0
    plus_dms, minus_dms, trs = [], [], []
    for i in range(1, len(closes)):
        up_move = highs[i] - highs[i - 1]
        down_move = lows[i - 1] - lows[i]
        plus_dms.append(up_move if up_move > down_move and up_move > 0 else 0)
        minus_dms.append(down_move if down_move > up_move and down_move > 0 else 0)
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    atr_val = _mean(trs[-period:]) or 1e-12
    plus_di = 100 * _mean(plus_dms[-period:]) / atr_val
    minus_di = 100 * _mean(minus_dms[-period:]) / atr_val
    di_sum = plus_di + minus_di
    dx = 100 * abs(plus_di - minus_di) / di_sum if di_sum > 0 else 0
    return clamp(dx, 0, 100), plus_di, minus_di

def _vwap(highs, lows, closes, volumes):
    n = min(24, len(closes))
    tp = [(highs[-i] + lows[-i] + closes[-i]) / 3 for i in range(n, 0, -1)]
    vols = volumes[-n:]
    total_vol = sum(vols)
    if total_vol == 0:
        return closes[-1]
    return sum(tp[i] * vols[i] for i in range(n)) / total_vol

def _pivot_points(high, low, close):
    pp = (high + low + close) / 3
    r1 = 2 * pp - low
    s1 = 2 * pp - high
    r2 = pp + (high - low)
    s2 = pp - (high - low)
    r3 = high + 2 * (pp - low)
    s3 = low - 2 * (high - pp)
    return {"pp": pp, "r1": r1, "r2": r2, "r3": r3, "s1": s1, "s2": s2, "s3": s3}

def _detect_candle_pattern(opens, highs, lows, closes):
    patterns = {}
    if len(closes) < 3:
        return patterns
    o, h, l, c = opens[-1], highs[-1], lows[-1], closes[-1]
    po, ph, pl, pc = opens[-2], highs[-2], lows[-2], closes[-2]
    body = abs(c - o)
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    full_range = max(h - l, 1e-12)
    if lower_wick > body * 2 and upper_wick < body * 0.3:
        patterns["hammer"] = 1.0
    if upper_wick > body * 2 and lower_wick < body * 0.3:
        patterns["shooting_star"] = 1.0
    if body < full_range * 0.1:
        patterns["doji"] = 1.0
    prev_body = abs(pc - po)
    if body > prev_body * 1.3:
        if c > o and pc > po and o < pc and c > po:
            patterns["bullish_engulfing"] = 1.0
        if c < o and pc < po and o > pc and c < po:
            patterns["bearish_engulfing"] = 1.0
    if all(closes[-3 + i] > opens[-3 + i] for i in range(3)):
        patterns["three_white_soldiers"] = 1.0
    if all(closes[-3 + i] < opens[-3 + i] for i in range(3)):
        patterns["three_black_crows"] = 1.0
    bull_score = sum([patterns.get("hammer", 0), patterns.get("bullish_engulfing", 0), patterns.get("three_white_soldiers", 0) * 1.5])
    bear_score = sum([patterns.get("shooting_star", 0), patterns.get("bearish_engulfing", 0), patterns.get("three_black_crows", 0) * 1.5])
    patterns["bull_bias"] = clamp(bull_score * 30, 0, 100)
    patterns["bear_bias"] = clamp(bear_score * 30, 0, 100)
    return patterns

def _ichimoku(highs, lows, closes):
    def mid(h, l):
        return (max(h) + min(l)) / 2
    n = len(closes)
    if n < 52:
        return {"above_cloud": 0, "bullish_cloud": 0, "tk_cross": 0}
    tenkan = mid(highs[-9:], lows[-9:])
    kijun = mid(highs[-26:], lows[-26:])
    senkou_a = (tenkan + kijun) / 2
    senkou_b = mid(highs[-52:], lows[-52:])
    price = closes[-1]
    cloud_top = max(senkou_a, senkou_b)
    cloud_bottom = min(senkou_a, senkou_b)
    above_cloud = 1 if price > cloud_top else (-1 if price < cloud_bottom else 0)
    bullish_cloud = 1 if senkou_a > senkou_b else -1
    tk_cross = 1 if tenkan > kijun else (-1 if tenkan < kijun else 0)
    return {"above_cloud": above_cloud, "bullish_cloud": bullish_cloud, "tk_cross": tk_cross, "tenkan": tenkan, "kijun": kijun, "senkou_a": senkou_a, "senkou_b": senkou_b}


class DerivativesFeed:
    def __init__(self, ttl=90):
        self._cache = {}
        self._ttl = ttl
        self._lock = threading.Lock()

    def _get(self, path, params):
        try:
            r = requests.get(f"{FAPI}{path}", params=params, timeout=8)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        return None

    def _cached(self, key, fn):
        with self._lock:
            hit = self._cache.get(key)
            if hit and time.time() - hit[0] < self._ttl:
                return hit[1]
        val = fn()
        with self._lock:
            self._cache[key] = (time.time(), val)
        return val

    @staticmethod
    def binance_sym(ccxt_sym):
        return ccxt_sym.split(":")[0].replace("/", "")

    def open_interest(self, symbol):
        s = self.binance_sym(symbol)
        data = self._cached(f"oi:{s}", lambda: self._get("/futures/data/openInterestHist", {"symbol": s, "period": "1h", "limit": 48}))
        defaults = {"oi_change_1h": 0.0, "oi_change_6h": 0.0, "oi_zscore": 0.0, "oi_trend": 0.0, "oi_acceleration": 0.0}
        if not data or len(data) < 6:
            return defaults
        vals = [float(d["sumOpenInterestValue"]) for d in data]
        last = vals[-1]
        ch1 = safe_div(last - vals[-2], vals[-2])
        ref6 = vals[-7] if len(vals) >= 7 else vals[0]
        ch6 = safe_div(last - ref6, ref6)
        mean_v = _mean(vals)
        std_v = _std(vals)
        zscore = (last - mean_v) / std_v if std_v > 0 else 0.0
        x = list(range(len(vals[-12:])))
        y = vals[-12:]
        n = len(x)
        mx, my = _mean(x), _mean(y)
        slope = safe_div(sum((x[i] - mx) * (y[i] - my) for i in range(n)), sum((x[i] - mx) ** 2 for i in range(n)))
        oi_trend = clamp(slope / (mean_v + 1) * 1000, -1, 1)
        accel = 0.0
        if len(vals) >= 4:
            accel = (vals[-1] - vals[-2]) - (vals[-3] - vals[-4])
            accel = clamp(safe_div(accel, mean_v + 1) * 1000, -1, 1)
        return {"oi_change_1h": ch1, "oi_change_6h": ch6, "oi_zscore": zscore, "oi_trend": oi_trend, "oi_acceleration": accel}

    def funding(self, symbol):
        s = self.binance_sym(symbol)
        cur = self._cached(f"pi:{s}", lambda: self._get("/fapi/v1/premiumIndex", {"symbol": s}))
        hist = self._cached(f"fh:{s}", lambda: self._get("/fapi/v1/fundingRate", {"symbol": s, "limit": 48}))
        rate, avg, std_rate = 0.0, 0.0, 0.0
        if isinstance(cur, dict):
            rate = float(cur.get("lastFundingRate", 0) or 0)
        if hist and isinstance(hist, list):
            rs = [float(h["fundingRate"]) for h in hist if h.get("fundingRate")]
            if rs:
                avg = _mean(rs)
                std_rate = _std(rs)
        annual = rate * 3 * 365
        deviation = safe_div(rate - avg, std_rate + 1e-9)
        extreme = abs(annual) > 0.3
        return {"rate": rate, "annual": annual, "deviation": deviation, "extreme": 1.0 if extreme else 0.0, "stress": clamp(abs(annual) / 0.5 * 100, 0, 100), "direction": 1.0 if rate > 0 else (-1.0 if rate < 0 else 0.0)}

    def long_short_ratio(self, symbol):
        s = self.binance_sym(symbol)
        top = self._cached(f"tlsp:{s}", lambda: self._get("/futures/data/topLongShortPositionRatio", {"symbol": s, "period": "1h", "limit": 12}))
        glob = self._cached(f"gls:{s}", lambda: self._get("/futures/data/globalLongShortAccountRatio", {"symbol": s, "period": "1h", "limit": 12}))
        result = {"top_ls": 1.0, "global_ls": 1.0, "smart_money_bias": 0.0, "retail_overcrowded_long": 0.0, "retail_overcrowded_short": 0.0}
        top_r, glob_r = 1.0, 1.0
        if top and len(top) >= 2:
            top_r = float(top[-1].get("longShortRatio", 1.0))
            result["top_ls"] = top_r
        if glob and glob:
            glob_r = float(glob[-1].get("longShortRatio", 1.0))
            result["global_ls"] = glob_r
        gap = top_r - glob_r
        result["smart_money_bias"] = clamp(gap * 20, -100, 100)
        result["retail_overcrowded_long"] = 1.0 if glob_r > 2.5 else 0.0
        result["retail_overcrowded_short"] = 1.0 if glob_r < 0.4 else 0.0
        return result

    def taker_flow(self, symbol):
        s = self.binance_sym(symbol)
        data = self._cached(f"tk:{s}", lambda: self._get("/futures/data/takerlongshortRatio", {"symbol": s, "period": "5m", "limit": 24}))
        if not data or not isinstance(data, list):
            return {"ratio": 1.0, "imbalance": 0.0, "momentum": 0.0}
        try:
            recent = data[-6:]
            prev = data[-12:-6]
            buy_r = sum(float(d.get("buyVol", 0)) for d in recent)
            sell_r = sum(float(d.get("sellVol", 0)) for d in recent)
            buy_p = sum(float(d.get("buyVol", 0)) for d in prev)
            sell_p = sum(float(d.get("sellVol", 0)) for d in prev)
            total_r = buy_r + sell_r
            total_p = buy_p + sell_p
            ratio = buy_r / sell_r if sell_r > 0 else 1.0
            imb = safe_div(buy_r - sell_r, total_r)
            imb_prev = safe_div(buy_p - sell_p, total_p)
            momentum = imb - imb_prev
            return {"ratio": ratio, "imbalance": imb, "momentum": momentum}
        except Exception:
            return {"ratio": 1.0, "imbalance": 0.0, "momentum": 0.0}

    def orderbook(self, exchange_pub, symbol):
        try:
            ob = exchange_pub.fetch_order_book(symbol, limit=50)
            bids = ob.get("bids", [])
            asks = ob.get("asks", [])
            if not bids or not asks:
                return {"imbalance": 0.0, "spread_bps": 5.0, "depth_ratio": 1.0}
            mid = (bids[0][0] + asks[0][0]) / 2
            bid_vol = sum(p * q * (1 - abs(p - mid) / mid) for p, q in bids[:20])
            ask_vol = sum(p * q * (1 - abs(p - mid) / mid) for p, q in asks[:20])
            total = bid_vol + ask_vol
            imb = safe_div(bid_vol - ask_vol, total)
            spread = safe_div(asks[0][0] - bids[0][0], mid) * 10000
            depth_ratio = safe_div(bid_vol, ask_vol)
            return {"imbalance": imb, "spread_bps": spread, "depth_ratio": depth_ratio}
        except Exception:
            return {"imbalance": 0.0, "spread_bps": 5.0, "depth_ratio": 1.0}

    def liquidation_heatmap(self, symbol, price):
        try:
            data = self._cached(f"liq:{symbol}", lambda: self._get("/futures/data/openInterestHist", {"symbol": self.binance_sym(symbol), "period": "5m", "limit": 50}))
            if not data:
                return {"liq_pressure_long": 0.0, "liq_pressure_short": 0.0}
            oi_vals = [float(d["sumOpenInterest"]) for d in data]
            drops = [max(oi_vals[i - 1] - oi_vals[i], 0) for i in range(1, len(oi_vals))]
            avg_drop = _mean(drops[-10:])
            recent_drop = _mean(drops[-3:])
            liq_pressure = clamp(safe_div(recent_drop, avg_drop + 1) - 1, 0, 1)
            price_up = sum(1 for i in range(1, len(data)) if float(data[i].get("sumOpenInterest", 0)) < float(data[i - 1].get("sumOpenInterest", 0)))
            return {"liq_pressure_long": liq_pressure if price_up > 5 else 0.0, "liq_pressure_short": liq_pressure if price_up <= 5 else 0.0}
        except Exception:
            return {"liq_pressure_long": 0.0, "liq_pressure_short": 0.0}



@dataclass
class Config:
    binance_api_key: str = os.getenv("BINANCE_API_KEY", "IX7kLH0ssWHP5TpYMUGcp0pzq4LX4Lqi7m4XtlqMkkq6DCZAsLhoeYZ3533jJFF4")
    binance_secret: str = os.getenv("BINANCE_SECRET", "LmICnpSpMxL1riv4RfIf0HBGRfhDTP5JhDUYdlPSukpqV7kDTonrZ0j3DWp1a7hU")
    nvidia_api_key: str = os.getenv("NVIDIA_API_KEY", "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")
    ai_model: str = "nvidia/llama-3.3-nemotron-super-49b-v1"
    dry_run: bool = True
    leverage: int = 50
    risk_per_trade_pct: float = 50.0
    max_daily_trades: int = 12
    max_open_positions: int = 2
    cooldown_seconds: int = 120
    max_sl_percent: float = 2.0
    max_tp_percent: float = 5.0
    min_rr_ratio: float = 2.0
    max_daily_loss_pct: float = 4.0
    max_consecutive_losses: int = 4
    min_signal_score: float = 58.0
    min_confidence: float = 52.0
    min_module_agreement: int = 4
    min_entry_quality: float = 52.0
    max_risk_for_entry: float = 48.0
    min_momentum_score: float = 45.0
    min_trend_alignment: int = 2
    use_ai_veto: bool = False
    use_ai_explainer: bool = True
    ai_min_veto_confidence: float = 80.0
    scanner_interval: int = 45
    scanner_top_n: int = 12
    scanner_min_volume_usdt: float = 3_000_000
    scanner_min_atr_pct: float = 0.3
    primary_tf: str = "1h"
    trend_tf: str = "4h"
    confirm_tf: str = "15m"
    timeframes: List[str] = field(default_factory=lambda: ["15m", "1h", "4h"])
    candle_maxlen: int = 600
    monitor_interval: int = 10
    trailing_stop_pct: float = 1.2
    flask_port: int = 8080
    watchlist: Dict[str, str] = field(default_factory=lambda: {
        "btcusdt": "BTC/USDT:USDT", "ethusdt": "ETH/USDT:USDT", "solusdt": "SOL/USDT:USDT",
        "bnbusdt": "BNB/USDT:USDT", "xrpusdt": "XRP/USDT:USDT", "adausdt": "ADA/USDT:USDT",
        "linkusdt": "LINK/USDT:USDT", "avaxusdt": "AVAX/USDT:USDT", "dogeusdt": "DOGE/USDT:USDT",
        "wifusdt": "WIF/USDT:USDT", "1000pepeusdt": "1000PEPE/USDT:USDT", "suiusdt": "SUI/USDT:USDT",
        "aaveusdt": "AAVE/USDT:USDT", "nearusdt": "NEAR/USDT:USDT", "arbusdt": "ARB/USDT:USDT",
        "dotusdt": "DOT/USDT:USDT", "ltcusdt": "LTC/USDT:USDT", "aptusdt": "APT/USDT:USDT",
        "opusdt": "OP/USDT:USDT", "jupusdt": "JUP/USDT:USDT", "tiausdt": "TIA/USDT:USDT",
    })
        db_path: str = "apex_trades.db"

    ws_ping_interval: int = 20
    ws_ping_timeout: int = 20
    ws_reconnect_delay: int = 8
    

CFG = Config()


class Direction(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"

class Decision(Enum):
    BUY = "BUY"
    SELL = "SELL"
    WAIT = "WAIT"

class Regime(Enum):
    TRENDING_UP = "TRENDING_UP"
    TRENDING_DOWN = "TRENDING_DOWN"
    RANGING = "RANGING"
    BREAKOUT_UP = "BREAKOUT_UP"
    BREAKOUT_DOWN = "BREAKOUT_DOWN"
    REVERSAL_UP = "REVERSAL_UP"
    REVERSAL_DOWN = "REVERSAL_DOWN"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    ACCUMULATION = "ACCUMULATION"
    DISTRIBUTION = "DISTRIBUTION"


@dataclass
class OHLCV:
    opens: List[float] = field(default_factory=list)
    highs: List[float] = field(default_factory=list)
    lows: List[float] = field(default_factory=list)
    closes: List[float] = field(default_factory=list)
    volumes: List[float] = field(default_factory=list)

    @classmethod
    def from_raw(cls, data):
        obj = cls()
        for c in data:
            obj.opens.append(float(c[1]))
            obj.highs.append(float(c[2]))
            obj.lows.append(float(c[3]))
            obj.closes.append(float(c[4]))
            obj.volumes.append(float(c[5]))
        return obj

    def __len__(self):
        return len(self.closes)


@dataclass
class ModuleSignal:
    name: str
    score: float
    confidence: float
    direction: Direction = Direction.NEUTRAL
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def bull_signal(self):
        return self.score > 55 and self.confidence > 40

    @property
    def bear_signal(self):
        return self.score < 45 and self.confidence > 40


@dataclass
class APEXOutput:
    decision: Decision = Decision.WAIT
    direction: Direction = Direction.NEUTRAL
    regime: Regime = Regime.RANGING
    composite_score: float = 50.0
    confidence: float = 0.0
    bull_modules: int = 0
    bear_modules: int = 0
    total_modules: int = 0
    sl_percent: float = 1.5
    tp_percent: float = 3.0
    rr_ratio: float = 2.0
    module_signals: List[ModuleSignal] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    rsi: float = 50.0
    trend_strength: float = 0.0
    volatility_pct: float = 0.0
    volume_spike: bool = False


@dataclass
class FinalDecision:
    decision: Decision = Decision.WAIT
    final_score: float = 0.0
    apex_score: float = 0.0
    ai_score: float = 0.0
    ai_explanation: str = ""
    sl_percent: float = 1.5
    tp_percent: float = 3.0
    regime: str = "RANGING"
    reasons: List[str] = field(default_factory=list)
    symbol: str = ""
    tf_alignment: int = 0
    risk_score: float = 50.0
    entry_quality: float = 0.0


class APEXEngine:
    BASE_WEIGHTS = {
        "trend": 0.18, "momentum": 0.15, "volume": 0.12, "structure": 0.12,
        "candle": 0.08, "deriv": 0.15, "ichimoku": 0.08, "sr_levels": 0.07, "volatility": 0.05,
    }

    def analyze(self, data_primary, data_trend=None, data_fast=None, symbol=None, exchange_pub=None):
        if len(data_primary) < 50:
            return None
        primary = OHLCV.from_raw(data_primary)
        trend_d = OHLCV.from_raw(data_trend) if data_trend and len(data_trend) >= 50 else None
        fast_d = OHLCV.from_raw(data_fast) if data_fast and len(data_fast) >= 30 else None
        out = APEXOutput()
        deriv_data = {}
        if symbol and exchange_pub:
            try:
                deriv_data = {
                    "oi": deriv.open_interest(symbol), "fund": deriv.funding(symbol),
                    "lsr": deriv.long_short_ratio(symbol), "tf": deriv.taker_flow(symbol),
                    "ob": deriv.orderbook(exchange_pub, symbol),
                    "liq": deriv.liquidation_heatmap(symbol, primary.closes[-1]),
                }
            except Exception as e:
                logger.warning(f"Deriv fetch error {symbol}: {e}")
                deriv_data = {}
        signals = [
            self._module_trend(primary, trend_d), self._module_momentum(primary),
            self._module_volume(primary), self._module_structure(primary),
            self._module_candle(primary), self._module_deriv(primary, deriv_data),
            self._module_ichimoku(primary), self._module_sr_levels(primary),
            self._module_volatility(primary),
        ]
        out.module_signals = signals
        out.total_modules = len(signals)
        regime = self._detect_regime(primary, deriv_data)
        out.regime = regime
        weights = self._adaptive_weights(regime)
        weighted_score = 0.0
        total_weight = 0.0
        bull_count = 0
        bear_count = 0
        for sig, (name, w) in zip(signals, weights.items()):
            effective_weight = w * (sig.confidence / 100.0)
            weighted_score += sig.score * effective_weight
            total_weight += effective_weight
            if sig.bull_signal:
                bull_count += 1
            elif sig.bear_signal:
                bear_count += 1
        composite = safe_div(weighted_score, total_weight, 50.0)
        out.composite_score = clamp(composite, 0, 100)
        out.bull_modules = bull_count
        out.bear_modules = bear_count
        if trend_d and len(trend_d) >= 50:
            trend_score = self._multi_tf_alignment(primary, trend_d, fast_d)
            out.composite_score = out.composite_score * 0.70 + trend_score * 0.30
        out.rsi = _rsi(primary.closes)
        atr_val = _atr(primary.highs, primary.lows, primary.closes)
        out.volatility_pct = safe_div(atr_val, primary.closes[-1]) * 100
        out.volume_spike = (len(primary.volumes) >= 20 and primary.volumes[-1] > _mean(primary.volumes[-20:]) * 1.5)
        adx_val, plus_di, minus_di = _adx(primary.highs, primary.lows, primary.closes)
        out.trend_strength = adx_val
        if out.composite_score > 55:
            out.direction = Direction.LONG
        elif out.composite_score < 45:
            out.direction = Direction.SHORT
        else:
            out.direction = Direction.NEUTRAL
        sl_mult = 1.5 if out.trend_strength > 25 else 2.0
        tp_mult = max(CFG.min_rr_ratio, 3.0 if out.trend_strength > 30 else 2.5)
        out.sl_percent = clamp(out.volatility_pct * sl_mult, 0.5, CFG.max_sl_percent)
        out.tp_percent = clamp(out.volatility_pct * tp_mult * sl_mult, 1.0, CFG.max_tp_percent)
        out.rr_ratio = safe_div(out.tp_percent, out.sl_percent, 2.0)
        block_reason = self._smart_filters(primary, out, deriv_data)
        if block_reason:
            out.warnings.append(f"FILTER: {block_reason}")
            logger.info(f"🔶 SOFT FILTER [{symbol}]: {block_reason}")
            out.composite_score = (out.composite_score - 50) * 0.5 + 50
        out.confidence = self._calc_confidence(out, adx_val, deriv_data)
        bull_ok = (out.composite_score >= CFG.min_signal_score and out.bull_modules >= CFG.min_module_agreement and out.confidence >= CFG.min_confidence and out.direction == Direction.LONG)
        bear_ok = (out.composite_score <= (100 - CFG.min_signal_score) and out.bear_modules >= CFG.min_module_agreement and out.confidence >= CFG.min_confidence and out.direction == Direction.SHORT)
        if bull_ok:
            out.decision = Decision.BUY
        elif bear_ok:
            out.decision = Decision.SELL
        else:
            out.decision = Decision.WAIT
        out.reasons = self._build_reasons(out, signals, adx_val, plus_di, minus_di)
        return out

    def _module_trend(self, d, trend_d=None):
        closes = d.closes
        price = closes[-1]
        ema9 = _ema(closes, 9)[-1] if len(closes) >= 9 else price
        ema21 = _ema(closes, 21)[-1] if len(closes) >= 21 else price
        ema50 = _ema(closes, 50)[-1] if len(closes) >= 50 else price
        ema200 = _ema(closes, 200)[-1] if len(closes) >= 200 else ema50
        aligned_bull = price > ema9 > ema21 > ema50
        aligned_bear = price < ema9 < ema21 < ema50
        partial_bull = sum([price > ema9, ema9 > ema21, ema21 > ema50])
        partial_bear = sum([price < ema9, ema9 < ema21, ema21 < ema50])
        ema_score = 50 + (partial_bull - partial_bear) * 12
        if aligned_bull: ema_score += 10
        if aligned_bear: ema_score -= 10
        macd_line, signal_line, hist = _macd(closes)
        macd_bull = macd_line > signal_line and hist > 0
        macd_bear = macd_line < signal_line and hist < 0
        macd_score = 65 if macd_bull else (35 if macd_bear else 50)
        if len(closes) > 36:
            _, _, hist_prev = _macd(closes[:-1])
            if hist > hist_prev: macd_score += 5
            if hist < hist_prev: macd_score -= 5
        htf_score = 50.0
        if trend_d and len(trend_d) >= 50:
            htf_price = trend_d.closes[-1]
            htf_ema21 = _ema(trend_d.closes, 21)[-1]
            htf_ema50 = _ema(trend_d.closes, 50)[-1]
            if htf_price > htf_ema21 > htf_ema50: htf_score = 70.0
            elif htf_price < htf_ema21 < htf_ema50: htf_score = 30.0
        score = clamp(ema_score * 0.50 + macd_score * 0.30 + htf_score * 0.20, 0, 100)
        confidence = 70 if (aligned_bull or aligned_bear) else 50
        direction = Direction.LONG if score > 55 else (Direction.SHORT if score < 45 else Direction.NEUTRAL)
        return ModuleSignal(name="trend", score=score, confidence=confidence, direction=direction,
                            details={"ema9": ema9, "ema21": ema21, "ema50": ema50, "macd": macd_line, "signal": signal_line, "hist": hist})

    def _module_momentum(self, d):
        closes = d.closes
        rsi = _rsi(closes)
        stoch_k, stoch_d = _stochastic(d.highs, d.lows, closes)
        if rsi < 30: rsi_score = 80
        elif rsi > 70: rsi_score = 20
        elif rsi > 60: rsi_score = 62
        elif rsi < 40: rsi_score = 38
        else: rsi_score = 50
        if len(closes) >= 15:
            rsi_prev = _rsi(closes[:-1])
            rsi_change = rsi - rsi_prev
            rsi_score += rsi_change * 0.5
        if stoch_k < 20 and stoch_k > stoch_d: stoch_score = 75
        elif stoch_k > 80 and stoch_k < stoch_d: stoch_score = 25
        elif stoch_k > stoch_d: stoch_score = 60
        elif stoch_k < stoch_d: stoch_score = 40
        else: stoch_score = 50
        if len(closes) >= 11:
            roc = safe_div(closes[-1] - closes[-11], closes[-11], 0) * 100
            roc_score = clamp(50 + roc * 10, 0, 100)
        else: roc_score = 50
        score = clamp(rsi_score * 0.45 + stoch_score * 0.35 + roc_score * 0.20, 0, 100)
        confidence = 70 if (rsi < 30 or rsi > 70) else (60 if abs(rsi - 50) > 10 else 45)
        direction = Direction.LONG if score > 55 else (Direction.SHORT if score < 45 else Direction.NEUTRAL)
        return ModuleSignal(name="momentum", score=score, confidence=confidence, direction=direction,
                            details={"rsi": rsi, "stoch_k": stoch_k, "stoch_d": stoch_d})

    def _module_volume(self, d):
        closes = d.closes
        vols = d.volumes
        price = closes[-1]
        n = min(20, len(vols))
        vol_z = _zscore(vols[-1], vols[-n:]) if n >= 5 else 0
        vol_rising = sum(1 for i in range(-5, 0) if len(vols) >= abs(i) and vols[i] > vols[i - 1]) >= 3
        vwap = _vwap(d.highs, d.lows, closes, vols)
        above_vwap = price > vwap
        n20 = min(20, len(closes) - 1)
        buy_vol = sum(vols[-(n20 - i)] for i in range(n20) if closes[-(n20 - i)] >= closes[-(n20 - i) - 1])
        sell_vol = sum(vols[-(n20 - i)] for i in range(n20) if closes[-(n20 - i)] < closes[-(n20 - i) - 1])
        total_vol = buy_vol + sell_vol
        vol_bias = safe_div(buy_vol - sell_vol, total_vol)
        obv_vals = [0.0]
        for i in range(1, len(closes)):
            if closes[i] > closes[i - 1]: obv_vals.append(obv_vals[-1] + vols[i])
            elif closes[i] < closes[i - 1]: obv_vals.append(obv_vals[-1] - vols[i])
            else: obv_vals.append(obv_vals[-1])
        obv_trend = 0.0
        if len(obv_vals) >= 10:
            obv_trend = safe_div(obv_vals[-1] - _mean(obv_vals[-10:]), max(abs(obv_vals[-1]), 1))
        score = 50 + vol_bias * 20 + (10 if above_vwap else -10) + clamp(obv_trend * 100, -15, 15) + (5 if vol_rising else -5)
        score = clamp(score, 0, 100)
        confidence = clamp(abs(vol_z) * 20 + 40, 30, 80)
        direction = Direction.LONG if score > 55 else (Direction.SHORT if score < 45 else Direction.NEUTRAL)
        return ModuleSignal(name="volume", score=score, confidence=confidence, direction=direction,
                            details={"vwap": vwap, "vol_z": vol_z, "vol_bias": vol_bias, "above_vwap": above_vwap, "obv_trend": obv_trend})

    def _module_structure(self, d):
        closes = d.closes
        highs = d.highs
        lows = d.lows
        price = closes[-1]
        if len(closes) < 20:
            return ModuleSignal("structure", 50, 30, Direction.NEUTRAL)
        pp = _pivot_points(highs[-2], lows[-2], closes[-2])
        supports = [pp["s1"], pp["s2"], pp["s3"]]
        resistances = [pp["r1"], pp["r2"], pp["r3"]]
        nearest_support = min((abs(price - s) for s in supports), default=float('inf'))
        nearest_resist = min((abs(price - r) for r in resistances), default=float('inf'))
        near_support = nearest_support < price * 0.01
        near_resist = nearest_resist < price * 0.01
        n = min(20, len(highs))
        hh = sum(1 for i in range(-n + 1, 0) if highs[i] > highs[i - 1])
        hl = sum(1 for i in range(-n + 1, 0) if lows[i] > lows[i - 1])
        lh = sum(1 for i in range(-n + 1, 0) if highs[i] < highs[i - 1])
        ll = sum(1 for i in range(-n + 1, 0) if lows[i] < lows[i - 1])
        bull_struct = safe_div(hh + hl, n * 2)
        bear_struct = safe_div(lh + ll, n * 2)
        score = 50 + (bull_struct - bear_struct) * 50
        if near_support: score += 10
        if near_resist: score -= 10
        high20 = max(highs[-20:])
        low20 = min(lows[-20:])
        range20 = high20 - low20
        if range20 > 0:
            pos = (price - low20) / range20
            score += (pos - 0.5) * 20
        score = clamp(score, 0, 100)
        confidence = 60 if (near_support or near_resist) else 45
        direction = Direction.LONG if score > 55 else (Direction.SHORT if score < 45 else Direction.NEUTRAL)
        return ModuleSignal(name="structure", score=score, confidence=confidence, direction=direction,
                            details={"pp": pp["pp"], "r1": pp["r1"], "s1": pp["s1"], "near_support": near_support, "near_resist": near_resist})

    def _module_candle(self, d):
        if len(d.closes) < 5:
            return ModuleSignal("candle", 50, 20, Direction.NEUTRAL)
        patterns = _detect_candle_pattern(d.opens, d.highs, d.lows, d.closes)
        upper, mid_bb, lower, pct_b, bw = _bollinger(d.closes)
        price = d.closes[-1]
        score = 50
        if patterns.get("hammer") and pct_b < 0.3: score += 20
        if patterns.get("shooting_star") and pct_b > 0.7: score -= 20
        if patterns.get("bullish_engulfing"): score += 18
        if patterns.get("bearish_engulfing"): score -= 18
        if patterns.get("three_white_soldiers"): score += 22
        if patterns.get("three_black_crows"): score -= 22
        if pct_b < 0.1: score += 8
        elif pct_b > 0.9: score -= 8
        if d.closes[-1] > d.opens[-1]:
            body_ratio = safe_div(d.closes[-1] - d.opens[-1], d.highs[-1] - d.lows[-1] + 1e-10)
            score += body_ratio * 10
        else:
            body_ratio = safe_div(d.opens[-1] - d.closes[-1], d.highs[-1] - d.lows[-1] + 1e-10)
            score -= body_ratio * 10
        score = clamp(score, 0, 100)
        has_pattern = any(k in patterns for k in ["hammer", "shooting_star", "bullish_engulfing", "bearish_engulfing", "three_white_soldiers", "three_black_crows"])
        confidence = 65 if has_pattern else 35
        direction = Direction.LONG if score > 55 else (Direction.SHORT if score < 45 else Direction.NEUTRAL)
        return ModuleSignal(name="candle", score=score, confidence=confidence, direction=direction,
                            details={"pct_b": pct_b, "bw": bw, "patterns": list(patterns.keys())})

    def _module_deriv(self, d, deriv_data):
        if not deriv_data:
            return ModuleSignal("deriv", 50, 20, Direction.NEUTRAL)
        oi = deriv_data.get("oi", {})
        fund = deriv_data.get("fund", {})
        lsr = deriv_data.get("lsr", {})
        tf = deriv_data.get("tf", {})
        ob = deriv_data.get("ob", {})
        score = 50
        confidence_factors = []
        oi_change = oi.get("oi_change_1h", 0)
        price_change = safe_div(d.closes[-1] - d.closes[-2], d.closes[-2])
        if oi_change > 0.005 and price_change > 0:
            score += 12; confidence_factors.append(15)
        elif oi_change < -0.005 and price_change < 0:
            score -= 12; confidence_factors.append(15)
        elif oi_change > 0 and price_change < 0:
            score -= 8
        elif oi_change < 0 and price_change > 0:
            score += 5
        oi_trend = oi.get("oi_trend", 0)
        score += oi_trend * 10
        fund_rate = fund.get("rate", 0)
        fund_extreme = fund.get("extreme", 0)
        if fund_rate > 0.0005: score -= 8 if not fund_extreme else 15
        elif fund_rate < -0.0003: score += 8 if not fund_extreme else 15
        smart_bias = lsr.get("smart_money_bias", 0)
        score += clamp(smart_bias * 0.15, -10, 10)
        if lsr.get("retail_overcrowded_long"): score -= 12; confidence_factors.append(20)
        if lsr.get("retail_overcrowded_short"): score += 12; confidence_factors.append(20)
        taker_imb = tf.get("imbalance", 0)
        taker_mom = tf.get("momentum", 0)
        score += taker_imb * 15 + taker_mom * 10
        ob_imb = ob.get("imbalance", 0)
        spread = ob.get("spread_bps", 5)
        score += ob_imb * 8
        if spread > 10: score = (score - 50) * 0.7 + 50
        score = clamp(score, 0, 100)
        confidence = _mean(confidence_factors) + 40 if confidence_factors else 45
        confidence = clamp(confidence, 0, 85)
        direction = Direction.LONG if score > 55 else (Direction.SHORT if score < 45 else Direction.NEUTRAL)
        return ModuleSignal(name="deriv", score=score, confidence=confidence, direction=direction,
                            details={"oi_change": oi_change, "funding": fund_rate, "smart_bias": smart_bias, "taker_imb": taker_imb})

    def _module_ichimoku(self, d):
        if len(d.closes) < 52:
            return ModuleSignal("ichimoku", 50, 20, Direction.NEUTRAL)
        ichi = _ichimoku(d.highs, d.lows, d.closes)
        score = 50
        above_cloud = ichi.get("above_cloud", 0)
        bull_cloud = ichi.get("bullish_cloud", 0)
        tk_cross = ichi.get("tk_cross", 0)
        score += above_cloud * 20 + bull_cloud * 10 + tk_cross * 12
        price = d.closes[-1]
        kijun = ichi.get("kijun", price)
        kijun_dist = safe_div(price - kijun, kijun) * 100
        score += clamp(kijun_dist * 2, -10, 10)
        if len(d.closes) >= 27:
            chikou_price = d.closes[-26]
            score += 8 if price > chikou_price else -8
        score = clamp(score, 0, 100)
        confidence = 65 if (above_cloud != 0 and tk_cross != 0) else 40
        direction = Direction.LONG if score > 55 else (Direction.SHORT if score < 45 else Direction.NEUTRAL)
        return ModuleSignal(name="ichimoku", score=score, confidence=confidence, direction=direction, details=ichi)

    def _module_sr_levels(self, d):
        closes = d.closes
        highs = d.highs
        lows = d.lows
        price = closes[-1]
        if len(closes) < 30:
            return ModuleSignal("sr_levels", 50, 25, Direction.NEUTRAL)
        n = min(50, len(closes))
        swing_highs, swing_lows = [], []
        for i in range(2, n - 2):
            idx = len(highs) - n + i
            if highs[idx] > highs[idx - 1] and highs[idx] > highs[idx + 1]: swing_highs.append(highs[idx])
            if lows[idx] < lows[idx - 1] and lows[idx] < lows[idx + 1]: swing_lows.append(lows[idx])
        if swing_highs and swing_lows:
            recent_high = max(swing_highs[-3:]) if len(swing_highs) >= 3 else max(swing_highs)
            recent_low = min(swing_lows[-3:]) if len(swing_lows) >= 3 else min(swing_lows)
            fib_range = recent_high - recent_low
            fib_levels = {"fib_236": recent_low + fib_range * 0.236, "fib_382": recent_low + fib_range * 0.382,
                          "fib_500": recent_low + fib_range * 0.500, "fib_618": recent_low + fib_range * 0.618,
                          "fib_786": recent_low + fib_range * 0.786}
            tolerance = price * 0.005
            near_fib_support = any(abs(price - lv) < tolerance and price >= lv - tolerance for lv in fib_levels.values())
            near_fib_resist = any(abs(price - lv) < tolerance and price <= lv + tolerance for lv in fib_levels.values())
            fib_position = clamp(safe_div(price - recent_low, fib_range), 0, 1)
        else:
            fib_position = 0.5
            near_fib_support = False
            near_fib_resist = False
        score = 50 + (fib_position - 0.5) * 30
        if near_fib_support: score += 15
        if near_fib_resist: score -= 15
        cluster_score = 0
        for level in swing_lows[-5:]:
            if abs(price - level) < price * 0.015: cluster_score += 1
        for level in swing_highs[-5:]:
            if abs(price - level) < price * 0.015: cluster_score -= 1
        score += cluster_score * 8
        score = clamp(score, 0, 100)
        confidence = 60 if (near_fib_support or near_fib_resist) else 40
        direction = Direction.LONG if score > 55 else (Direction.SHORT if score < 45 else Direction.NEUTRAL)
        return ModuleSignal(name="sr_levels", score=score, confidence=confidence, direction=direction,
                            details={"fib_pos": fib_position, "near_fib_support": near_fib_support, "near_fib_resist": near_fib_resist})

    def _module_volatility(self, d):
        closes = d.closes
        atr = _atr(d.highs, d.lows, closes)
        atr_pct = safe_div(atr, closes[-1]) * 100
        _, _, _, pct_b, bw = _bollinger(closes)
        if len(closes) >= 20:
            rets = [safe_div(closes[i] - closes[i - 1], closes[i - 1]) for i in range(1, len(closes))]
            hv = _std(rets[-20:]) * math.sqrt(365) * 100
        else: hv = 50
        if atr_pct < 0.3:
            score = 55; vol_regime = "COMPRESSED"; confidence = 50
        elif atr_pct > 3.0:
            score = 50; vol_regime = "EXPLOSIVE"; confidence = 35
        else:
            score = 50 + (pct_b - 0.5) * 20; vol_regime = "NORMAL"; confidence = 55
        if bw < 3.0:
            score = (score - 50) * 0.5 + 50; confidence = max(confidence - 10, 20)
        score = clamp(score, 0, 100)
        direction = Direction.LONG if score > 55 else (Direction.SHORT if score < 45 else Direction.NEUTRAL)
        return ModuleSignal(name="volatility", score=score, confidence=confidence, direction=direction,
                            details={"atr_pct": atr_pct, "bw": bw, "pct_b": pct_b, "hv": hv, "vol_regime": vol_regime})

    def _detect_regime(self, d, deriv_data):
        closes = d.closes
        if len(closes) < 30: return Regime.RANGING
        adx_val, plus_di, minus_di = _adx(d.highs, d.lows, closes)
        _, _, _, pct_b, bw = _bollinger(closes)
        atr = _atr(d.highs, d.lows, closes)
        atr_pct = safe_div(atr, closes[-1]) * 100
        if adx_val > 25 and plus_di > minus_di: return Regime.TRENDING_UP
        if adx_val > 25 and minus_di > plus_di: return Regime.TRENDING_DOWN
        if bw > 8 and atr_pct > 1.5:
            ema_fast = _ema(closes, 9)[-1]
            ema_slow = _ema(closes, 21)[-1]
            return Regime.BREAKOUT_UP if ema_fast > ema_slow else Regime.BREAKOUT_DOWN
        rsi = _rsi(closes)
        if rsi < 28: return Regime.REVERSAL_UP
        if rsi > 72: return Regime.REVERSAL_DOWN
        if atr_pct > 2.5: return Regime.HIGH_VOLATILITY
        oi = deriv_data.get("oi", {})
        oi_change = oi.get("oi_change_1h", 0)
        if bw < 4.0 and abs(oi_change) < 0.01: return Regime.ACCUMULATION
        return Regime.RANGING

    def _adaptive_weights(self, regime):
        w = dict(self.BASE_WEIGHTS)
        if regime in (Regime.TRENDING_UP, Regime.TRENDING_DOWN):
            w["trend"] = 0.25; w["momentum"] = 0.18; w["ichimoku"] = 0.12; w["sr_levels"] = 0.05
        elif regime in (Regime.BREAKOUT_UP, Regime.BREAKOUT_DOWN):
            w["volume"] = 0.20; w["volatility"] = 0.12; w["momentum"] = 0.18; w["deriv"] = 0.18
        elif regime in (Regime.REVERSAL_UP, Regime.REVERSAL_DOWN):
            w["candle"] = 0.18; w["momentum"] = 0.20; w["sr_levels"] = 0.15; w["deriv"] = 0.18
        elif regime == Regime.HIGH_VOLATILITY:
            w["deriv"] = 0.22; w["volatility"] = 0.08; w["trend"] = 0.12
        total = sum(w.values())
        return {k: v / total for k, v in w.items()}

    def _multi_tf_alignment(self, primary, trend_tf, fast_tf=None):
        tr_ema21 = _ema(trend_tf.closes, 21)[-1] if len(trend_tf.closes) >= 21 else trend_tf.closes[-1]
        tr_ema50 = _ema(trend_tf.closes, 50)[-1] if len(trend_tf.closes) >= 50 else trend_tf.closes[-1]
        tr_price = trend_tf.closes[-1]
        tr_rsi = _rsi(trend_tf.closes)
        trend_score = 50
        if tr_price > tr_ema21 > tr_ema50: trend_score = 70
        elif tr_price < tr_ema21 < tr_ema50: trend_score = 30
        if tr_rsi > 60: trend_score += 5
        if tr_rsi < 40: trend_score -= 5
        fast_score = 50
        if fast_tf and len(fast_tf.closes) >= 21:
            f_ema9 = _ema(fast_tf.closes, 9)[-1]
            f_ema21 = _ema(fast_tf.closes, 21)[-1]
            fast_score = 65 if f_ema9 > f_ema21 else (35 if f_ema9 < f_ema21 else 50)
        return clamp(trend_score * 0.65 + fast_score * 0.35, 0, 100)

    def _smart_filters(self, d, out, deriv_data):
        fund = deriv_data.get("fund", {})
        ob = deriv_data.get("ob", {})
        if ob.get("spread_bps", 0) > 15:
            return f"سبريد واسع جداً {ob['spread_bps']:.1f}bps"
        if fund.get("extreme") and out.direction == Direction.LONG and fund.get("rate", 0) > 0.002:
            return "تمويل متطرف إيجابي — خطر تصفية لونغ"
        if fund.get("extreme") and out.direction == Direction.SHORT and fund.get("rate", 0) < -0.001:
            return "تمويل متطرف سلبي — خطر short squeeze"
        rsi = _rsi(d.closes)
        if len(d.closes) >= 20:
            if d.closes[-1] >= max(d.closes[-20:-1]) and rsi < _rsi(d.closes[:-1]):
                return "تباعد هبوطي في RSI"
            if d.closes[-1] <= min(d.closes[-20:-1]) and rsi > _rsi(d.closes[:-1]):
                return "تباعد صعودي في RSI"
        return None

    def _calc_confidence(self, out, adx, deriv_data):
        active_modules = out.bull_modules + out.bear_modules
        agreement = max(out.bull_modules, out.bear_modules) / max(active_modules, 1)
        base = agreement * 70
        if adx > 30: base += 15
        elif adx > 20: base += 8
        oi = deriv_data.get("oi", {})
        if abs(oi.get("oi_change_1h", 0)) > 0.005: base += 8
        score_extremity = abs(out.composite_score - 50) / 50
        base += score_extremity * 15
        return clamp(base, 0, 95)

    def _build_reasons(self, out, signals, adx, plus_di, minus_di):
        reasons = [
            f"Regime={out.regime.value} Dir={out.direction.value} Score={out.composite_score:.1f}",
            f"Modules: Bull={out.bull_modules}/{out.total_modules} Bear={out.bear_modules}/{out.total_modules}",
            f"ADX={adx:.1f} +DI={plus_di:.1f} -DI={minus_di:.1f} | RSI={out.rsi:.1f}",
            f"Volatility={out.volatility_pct:.2f}% | VolumeSpike={out.volume_spike}",
        ]
        for sig in signals:
            emoji = "🟢" if sig.bull_signal else ("🔴" if sig.bear_signal else "⚪")
            reasons.append(f"  {emoji} [{sig.name:10s}] Score={sig.score:5.1f} Conf={sig.confidence:.0f}%")
        return reasons


apex_engine = APEXEngine()
deriv = DerivativesFeed()


class TradeDB:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        self._init_tables()

    def _init_tables(self):
        with self.lock:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT, side TEXT, mode TEXT,
                    entry_price REAL, quantity REAL,
                    sl_price REAL, tp_price REAL,
                    sl_order_id TEXT DEFAULT '', tp_order_id TEXT DEFAULT '',
                    entry_order_id TEXT DEFAULT '',
                    confidence REAL, entry_quality REAL, risk_score REAL,
                    regime TEXT, reason TEXT, timestamp TEXT,
                    status TEXT DEFAULT 'OPEN',
                    exit_price REAL, realized_pnl REAL,
                    pnl_percent REAL, commission REAL DEFAULT 0,
                    closed_at TEXT, close_reason TEXT,
                    ai_explanation TEXT, tf_alignment INTEGER,
                    final_score REAL
                );
                CREATE INDEX IF NOT EXISTS idx_status ON trades(status);
                CREATE INDEX IF NOT EXISTS idx_symbol ON trades(symbol);
                CREATE INDEX IF NOT EXISTS idx_timestamp ON trades(timestamp);
            """)
            self.conn.commit()

    def insert_trade(self, **kw):
        with self.lock:
            cur = self.conn.execute(
                """INSERT INTO trades
                (symbol, side, mode, entry_price, quantity, sl_price, tp_price,
                sl_order_id, tp_order_id, entry_order_id, confidence, entry_quality,
                risk_score, regime, reason, timestamp, status, ai_explanation,
                tf_alignment, final_score)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (kw.get("symbol"), kw.get("side"), kw.get("mode"),
                 kw.get("entry_price"), kw.get("quantity"),
                 kw.get("sl_price"), kw.get("tp_price"),
                 kw.get("sl_order_id", ""), kw.get("tp_order_id", ""),
                 kw.get("entry_order_id", ""), kw.get("confidence", 0),
                 kw.get("entry_quality", 0), kw.get("risk_score", 50),
                 kw.get("regime", ""), kw.get("reason", ""),
                 kw.get("timestamp", ""), kw.get("status", "OPEN"),
                 kw.get("ai_explanation", ""), kw.get("tf_alignment", 0),
                 kw.get("final_score", 0)))
            self.conn.commit()
            return cur.lastrowid

    def close_trade(self, tid, ep, rpnl, pp, comm, reason):
        with self.lock:
            self.conn.execute(
                """UPDATE trades SET status='CLOSED', exit_price=?,
                realized_pnl=?, pnl_percent=?, commission=?,
                closed_at=?, close_reason=? WHERE id=?""",
                (ep, rpnl, pp, comm,
                 datetime.now(timezone.utc).isoformat(), reason, tid))
            self.conn.commit()

        def get_open_trades(self):
        with self.lock:
            rows = self.conn.execute("SELECT * FROM trades WHERE status='OPEN'").fetchall()
            # التعديل هنا لتجنب خطأ KeyError: 'symbol'
            cursor = self.conn.execute("SELECT * FROM trades LIMIT 0")
            cols = [description[0] for description in cursor.description]
        return [dict(zip(cols, r)) for r in rows]


    def open_count(self):
        with self.lock:
            r = self.conn.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN'").fetchone()
        return r[0] if r else 0

    def consecutive_losses(self):
        with self.lock:
            rows = self.conn.execute(
                "SELECT realized_pnl FROM trades WHERE status='CLOSED' ORDER BY closed_at DESC LIMIT ?",
                (CFG.max_consecutive_losses,)).fetchall()
        if len(rows) < CFG.max_consecutive_losses:
            return 0
        return sum(1 for r in rows if r[0] is not None and r[0] < 0)

    def daily_pnl(self):
        t = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self.lock:
            r = self.conn.execute(
                "SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE status='CLOSED' AND closed_at LIKE ?",
                (f"{t}%",)).fetchone()
        return r[0] if r else 0.0

    def get_stats(self):
        with self.lock:
            total = self.conn.execute("SELECT COUNT(*) FROM trades WHERE status='CLOSED'").fetchone()[0]
            wins = self.conn.execute(
                "SELECT COUNT(*) FROM trades WHERE status='CLOSED' AND realized_pnl > 0").fetchone()[0]
            pnl = self.conn.execute(
                "SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE status='CLOSED'").fetchone()[0]
        winrate = wins / total * 100 if total > 0 else 0
        return {"total": total, "wins": wins, "winrate": winrate, "total_pnl": pnl}

db = TradeDB(CFG.db_path)



app = Flask(__name__)
bot_stats = {
    "status": "STARTING", "version": "APEX-v3.0-Fusion", "uptime": 0,
    "trades_today": 0, "open_positions": 0, "scanner": [],
    "last_analysis": {}, "mode": "DRY_RUN" if CFG.dry_run else "LIVE",
    "current_ip": "", "performance": {}
}
T0 = time.time()


@app.route("/")
def home():
    stats = db.get_stats()
    rows = ""
    for s, v in bot_stats["last_analysis"].items():
        dc = v.get("decision", "WAIT")
        cls = "buy" if dc == "BUY" else ("sell" if dc == "SELL" else "wait")
        rows += f"""<tr>
            <td>{s}</td>
            <td class='{cls}'>{dc}</td>
            <td>{v.get('score', 0):.1f}</td>
            <td>{v.get('regime', '')}</td>
            <td>{v.get('tf_align', 0)}/3</td>
            <td>{v.get('time', '')[-8:]}</td>
        </tr>"""
    return f"""<!DOCTYPE html>
<html>
<head><title>APEX Trading Bot</title>
<style>
  body {{ font-family: monospace; background: #0a0a0a; color: #00ff88; padding: 20px; }}
  h1 {{ color: #00ccff; }}
  table {{ border-collapse: collapse; width: 100%; }}
  td, th {{ border: 1px solid #333; padding: 8px; }}
  .buy {{ color: #00ff88; }} .sell {{ color: #ff4466; }} .wait {{ color: #888; }}
  .stat {{ background: #111; padding: 10px; margin: 5px; display: inline-block; border-radius: 4px; }}
</style>
</head>
<body>
<h1>APEX TRADING ENGINE v3.0 Fusion</h1>
<div>
  <span class="stat">Mode: <b>{bot_stats['mode']}</b></span>
  <span class="stat">IP: {bot_stats['current_ip']}</span>
  <span class="stat">Uptime: {int(time.time()-T0)}s</span>
  <span class="stat">Today: {bot_stats['trades_today']} trades</span>
  <span class="stat">Open: {bot_stats['open_positions']}</span>
  <span class="stat">Total: {stats['total']} | WR: {stats['winrate']:.1f}%</span>
</div>
<h2>Last Analysis</h2>
<table>
  <tr><th>Symbol</th><th>Decision</th><th>Score</th><th>Regime</th><th>TF Align</th><th>Time</th></tr>
  {rows}
</table>
</body></html>"""


@app.route("/health")
def health():
    bot_stats["uptime"] = int(time.time() - T0)
    bot_stats["trades_today"] = db.count_today()
    bot_stats["open_positions"] = db.open_count()
    bot_stats["performance"] = db.get_stats()
    return jsonify(bot_stats)


@app.route("/positions")
def positions():
    return jsonify(db.get_open_trades())


def run_server():
    app.run(host="0.0.0.0", port=CFG.flask_port, debug=False, use_reloader=False)


exchange_public = ccxt.binance({
    "enableRateLimit": True,
    "options": {"defaultType": "swap", "adjustForTimeDifference": True}
})
exchange = ccxt.binance({
    "apiKey": CFG.binance_api_key, "secret": CFG.binance_secret,
    "enableRateLimit": True,
    "options": {"defaultType": "swap", "adjustForTimeDifference": True}
})


def get_ip():
    try:
        return requests.get("https://api.ipify.org", timeout=10).text
    except Exception:
        return "UNKNOWN"


def show_deploy_ip():
    ip = get_ip()
    bot_stats["current_ip"] = ip
    logger.critical("=" * 60)
    logger.critical(f"  DEPLOY IP:  {ip}")
    logger.critical("  Add in Binance API Management -> IP Whitelist")
    logger.critical("=" * 60)
    return ip


def get_balance():
    try:
        bal = exchange.fetch_balance({"type": "future"})
        usdt = bal.get("USDT", {})
        return float(usdt.get("free", 0) or 0)
    except Exception:
        return 0.0


def daily_pnl_pct():
    bal = get_balance() or 1.0
    return db.daily_pnl() / bal * 100


def position_size(balance, entry, sl):
    risk_usdt = balance * CFG.risk_per_trade_pct / 100
    sl_dist = abs(entry - sl)
    if sl_dist <= 0:
        return 0.0
    return risk_usdt / sl_dist


class AIAnalyst:
    def analyze(self, symbol, apex_out):
        result = {"decision": "WAIT", "confidence": 0.0, "explanation": "", "risk_warnings": [], "error": False}
        if not CFG.use_ai_veto and not CFG.use_ai_explainer:
            return result
        reasons_text = "\n".join(apex_out.reasons[:6])
        prompt = f"""أنت محلل تداول. اقرأ نتائج محرك APEX التالي وأعطِ رأيك.

العملة: {symbol}
قرار APEX: {apex_out.decision.value}
Regime: {apex_out.regime.value}
Composite Score: {apex_out.composite_score:.1f}/100
Confidence: {apex_out.confidence:.1f}/100
Direction: {apex_out.direction.value}
RSI: {apex_out.rsi:.1f}
ADX: {apex_out.trend_strength:.1f}
Volatility: {apex_out.volatility_pct:.2f}%
Modules Bull: {apex_out.bull_modules} | Bear: {apex_out.bear_modules}

القواعد:
1. إذا ترى خطر حقيقي لا يراه APEX: اعترض (WAIT)
2. إذا APEX صحيح: وافق
3. confidence = مدى ثقتك (0-100)

أجب JSON فقط:
{{"decision":"BUY أو SELL أو WAIT","confidence":75,"explanation":"شرح مختصر بالعربية","risk_warnings":[]}}"""
        try:
            ai_client = OpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=CFG.nvidia_api_key)
            comp = ai_client.chat.completions.create(
                model=CFG.ai_model,
                messages=[{"role": "system", "content": "/think"}, {"role": "user", "content": prompt}],
                temperature=0.6, top_p=0.95, max_tokens=1024, stream=False)
            raw = comp.choices[0].message.content or ""
            cleaned = raw.strip()
            if cleaned.startswith("```"):
                lines = [l for l in cleaned.split("\n") if not l.strip().startswith("```")]
                cleaned = "\n".join(lines).strip()
            js = cleaned.find("{"); je = cleaned.rfind("}") + 1
            if js >= 0 and je > js:
                cleaned = cleaned[js:je]
            dj = json.loads(cleaned)
            result["decision"] = str(dj.get("decision", "WAIT")).upper()
            if result["decision"] not in ("BUY", "SELL", "WAIT"):
                result["decision"] = "WAIT"
            result["confidence"] = max(0, min(100, float(dj.get("confidence", 0))))
            result["explanation"] = str(dj.get("explanation", ""))
            result["risk_warnings"] = dj.get("risk_warnings", [])
            logger.info(f"AI {symbol}: {result['decision']} | Conf={result['confidence']} | {result['explanation'][:80]}")
        except Exception as e:
            logger.warning(f"AI ERROR {symbol}: {e}")
            result["decision"] = "WAIT"; result["confidence"] = 0; result["error"] = True
            result["explanation"] = f"AI_ERROR: {str(e)[:100]}"
        return result


ai_analyst = AIAnalyst()


class CandleManager:
    def __init__(self, maxlen=500):
        self._c = {}
        self._f = {}
        self._lock = threading.Lock()
        self._m = maxlen

    def ensure(self, sk, tfs):
        with self._lock:
            if sk not in self._c:
                self._c[sk] = {tf: deque(maxlen=self._m) for tf in tfs}
                self._f[sk] = {tf: None for tf in tfs}

    def update(self, sk, tf, candle, closed):
        with self._lock:
            if sk not in self._c or tf not in self._c[sk]:
                return
            if closed:
                dq = self._c[sk][tf]
                if dq and dq[-1][0] == candle[0]:
                    dq[-1] = candle
                else:
                    dq.append(candle)
                self._f[sk][tf] = None
            else:
                self._f[sk][tf] = candle

    def get(self, sk, tf):
        with self._lock:
            if sk not in self._c or tf not in self._c[sk]:
                return []
            return list(self._c[sk][tf])

    def count(self, sk, tf):
        with self._lock:
            return len(self._c.get(sk, {}).get(tf, []))

    def load(self, sk, tf, data):
        with self._lock:
            if sk not in self._c:
                return
            if data and len(data) > 1:
                self._c[sk][tf] = deque(data[:-1], maxlen=self._m)
                self._f[sk][tf] = data[-1]
            else:
                self._c[sk][tf] = deque(data, maxlen=self._m)


cm = CandleManager(CFG.candle_maxlen)
trade_state = {}
execution_lock = threading.Lock()
active_symbols = {}
active_lock = threading.Lock()


class MarketScanner:
    def __init__(self):
        self._run = True

    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()
        logger.info("Scanner started")

    def stop(self):
        self._run = False

    def _loop(self):
        time.sleep(5)
        while self._run:
            try:
                self._cycle()
            except Exception as e:
                logger.error(f"Scanner: {e}", exc_info=True)
            time.sleep(CFG.scanner_interval)

    def _cycle(self):
        logger.info("=" * 60)
        logger.info("Scanning...")
        candidates = []
        for sk, sym in CFG.watchlist.items():
            try:
                r = self._quick(sk, sym)
                if r:
                    candidates.append(r)
            except Exception:
                pass
            time.sleep(0.3)
        candidates.sort(key=lambda x: x.get("score", 0), reverse=True)
        top = candidates[:CFG.scanner_top_n]
        logger.info(f"Scanned {len(CFG.watchlist)} -> {len(candidates)} -> Top {len(top)}")
        for i, c in enumerate(top):
            logger.info(f"  #{i+1} {c['symbol']} | Score={c['score']:.1f} | Vol={c['volume_usdt']/1e6:.1f}M | ATR={c['atr_pct']:.2f}%")
        bot_stats["scanner"] = [{"symbol": c["symbol"], "score": c["score"]} for c in top]
        with active_lock:
            active_symbols.clear()
            for c in top:
                active_symbols[c["symbol_key"]] = c["symbol"]
                cm.ensure(c["symbol_key"], CFG.timeframes)
        for c in top:
            pos = get_pos(c["symbol"])
            if pos == "ERROR" or pos:
                continue
            threading.Thread(target=self._deep, args=(c["symbol_key"], c["symbol"]), daemon=True).start()
            time.sleep(1)

    def _quick(self, sk, sym):
        result = {"symbol_key": sk, "symbol": sym, "score": 0.0, "volume_usdt": 0.0, "atr_pct": 0.0, "reasons": []}
        try:
            ticker = exchange_public.fetch_ticker(sym)
            vol = float(ticker.get("quoteVolume", 0) or 0)
            result["volume_usdt"] = vol
            if vol < CFG.scanner_min_volume_usdt:
                return None
        except Exception:
            return None
        try:
            ohlcv = exchange_public.fetch_ohlcv(sym, "1h", limit=50)
            if len(ohlcv) < 20:
                return None
            h = [float(x[2]) for x in ohlcv]
            l = [float(x[3]) for x in ohlcv]
            c = [float(x[4]) for x in ohlcv]
            trs = [max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])) for i in range(1, len(c))]
            atr = sum(trs[-14:]) / 14 if len(trs) >= 14 else 0
            price = c[-1]
            if atr and price:
                ap = (atr / price) * 100
                result["atr_pct"] = ap
                if ap < CFG.scanner_min_atr_pct:
                    return None
        except Exception:
            return None
        if len(c) >= 20:
            net = abs(c[-1] - c[-20])
            path = sum(abs(c[i] - c[i - 1]) for i in range(len(c) - 20, len(c)))
            eff = net / path if path > 0 else 0
            result["score"] = eff * 5 + min(vol / 50_000_000, 1) * 2 + min(ap / 1.5, 1) * 2
        return result

    def _deep(self, sk, sym):
        logger.info(f"Deep: {sym}")
        if cm.count(sk, CFG.primary_tf) < 50:
            self._load(sk, sym)
        d_primary = cm.get(sk, CFG.primary_tf)
        d_trend = cm.get(sk, CFG.trend_tf)
        d_fast = cm.get(sk, CFG.confirm_tf)
        if len(d_primary) < 50:
            logger.info(f"Not enough data: {sym}")
            return
        apex = apex_engine.analyze(
            data_primary=d_primary,
            data_trend=d_trend if len(d_trend) >= 50 else None,
            data_fast=d_fast if len(d_fast) >= 30 else None,
            symbol=sym,
            exchange_pub=exchange_public,
        )
        if not apex:
            logger.info(f"APEX fail: {sym}")
            return
        logger.info(f">>> APEX {sym}: {apex.decision.value} | Regime={apex.regime.value} | Score={apex.composite_score:.1f} | Conf={apex.confidence:.1f}")
        for r in apex.reasons:
            logger.info(f"   {r}")
        ai = ai_analyst.analyze(sym, apex)
        final = FinalDecision()
        final.sl_percent = apex.sl_percent
        final.tp_percent = apex.tp_percent
        final.regime = apex.regime.value
        final.apex_score = apex.confidence
        final.ai_score = ai["confidence"]
        final.ai_explanation = ai["explanation"]
        final.risk_score = 50.0
        final.entry_quality = apex.composite_score
        final.tf_alignment = apex.bull_modules if apex.direction == Direction.LONG else apex.bear_modules
        if apex.decision == Decision.WAIT:
            final.decision = Decision.WAIT
            final.final_score = apex.confidence
            final.reasons = [f"APEX WAIT | Regime={apex.regime.value}"] + apex.reasons
        elif ai["error"]:
            final.decision = Decision.BUY if apex.decision == Decision.BUY else Decision.SELL
            final.final_score = apex.confidence
            final.reasons = [f"APEX {apex.decision.value} (AI_ERROR) | Conf={apex.confidence:.1f}"] + apex.reasons
        elif CFG.use_ai_veto and ai["decision"] == "WAIT" and ai["confidence"] >= CFG.ai_min_veto_confidence:
            final.decision = Decision.WAIT
            final.final_score = apex.confidence
            final.reasons = [f"AI VETO (conf={ai['confidence']}) | APEX was {apex.decision.value}"] + apex.reasons
        else:
            final.decision = Decision.BUY if apex.decision == Decision.BUY else Decision.SELL
            final.final_score = apex.confidence * 0.85 + ai["confidence"] * 0.15
            final.reasons = [f"APEX {apex.decision.value} | Score={final.final_score:.1f} | AI={ai['decision']}({ai['confidence']})"] + apex.reasons
        logger.info(f"FINAL {sym}: {final.decision.value} | Score={final.final_score:.1f} | {final.reasons[0] if final.reasons else ''}")
        bot_stats["last_analysis"][sym] = {
            "decision": final.decision.value,
            "score": round(final.final_score, 1),
            "regime": apex.regime.value,
            "tf_align": final.tf_alignment,
            "ai": f"{ai['decision']}({ai['confidence']}){'[ERR]' if ai['error'] else ''}",
            "time": datetime.now(timezone.utc).isoformat(),
        }
        if final.decision == Decision.WAIT:
            return
        if final.final_score < CFG.min_confidence:
            logger.info(f"Score too low: {final.final_score:.1f} < {CFG.min_confidence}")
            return
        execute_trade(sym, final)

    def _load(self, sk, sym):
        for tf in CFG.timeframes:
            try:
                limit = 500 if tf == "1d" else 300
                data = exchange_public.fetch_ohlcv(sym, timeframe=tf, limit=limit)
                cm.load(sk, tf, data)
            except Exception as e:
                logger.warning(f"Load {sym} {tf}: {e}")
            time.sleep(0.3)


def get_pos(sym):
    try:
        for p in exchange.fetch_positions([sym]):
            ct = p.get("contracts")
            if ct and float(ct) > 0:
                return p
        return None
    except Exception as e:
        logger.error(f"Pos {sym}: {e}")
        return "ERROR"


def emergency_close(sym, reason):
    logger.critical(f"EMERGENCY CLOSE: {sym} | {reason}")
    trade_to_close = None
    for t in db.get_open_trades():
        if t["symbol"] == sym:
            trade_to_close = t
            break
    if trade_to_close:
        for oid in [trade_to_close.get("sl_order_id"), trade_to_close.get("tp_order_id")]:
            if oid:
                try:
                    exchange.cancel_order(oid, sym)
                    logger.info(f"🧹 تم حذف الطلب المرتبط بالصفقة: {oid}")
                except Exception as e:
                    if "Unknown order" not in str(e) and "Order does not exist" not in str(e):
                        logger.warning(f"⚠️ فشل حذف الطلب {oid}: {e}")
    try:
        pos = get_pos(sym)
        if pos and pos != "ERROR":
            ct = float(pos.get("contracts", 0))
            side = pos.get("side", "")
            if ct > 0:
                cs = "sell" if side == "long" else "buy"
                exchange.create_market_order(sym, cs, ct, params={"reduceOnly": True})
                logger.info(f"✅ تم إغلاق صفقة {sym} بالكامل.")
    except Exception as e:
        logger.critical(f"❌ فشل إغلاق الصفقة (Emergency fail): {e}")


def execute_trade(sym, final):
    with execution_lock:
        try:
            dpnl = daily_pnl_pct()
            if dpnl <= -CFG.max_daily_loss_pct:
                logger.critical(f"🛑 تجاوز حد الخسارة اليومي ({dpnl:.2f}%) — إيقاف التداول")
                return
            if db.consecutive_losses() >= CFG.max_consecutive_losses:
                logger.critical(f"🛑 {CFG.max_consecutive_losses} خسائر متتالية — إيقاف مؤقت")
                return
            open_db_trades = [t for t in db.get_open_trades() if t["symbol"] == sym]
            if open_db_trades:
                logger.info(f"⏳ تجاهل الإشارة: {sym} لديه صفقة مفتوحة بالفعل في قاعدة البيانات.")
                return
            st = trade_state.setdefault(sym, {})
            if time.time() - st.get("t", 0) < CFG.cooldown_seconds:
                logger.info(f"⏳ تجاهل الإشارة: {sym} في فترة التبريد (Cooldown).")
                return
            pos = get_pos(sym)
            if pos == "ERROR":
                return
            if pos:
                logger.info(f"⏳ تجاهل الإشارة: {sym} مشغول (Busy) على المنصة.")
                return
            if db.count_today() >= CFG.max_daily_trades:
                logger.info("🚫 تم الوصول للحد اليومي للصفقات")
                return
            if db.open_count() >= CFG.max_open_positions:
                logger.info("🚫 تم الوصول للحد الأقصى للصفقات المفتوحة")
                return
            ticker = exchange_public.fetch_ticker(sym)
            price = ticker["last"]
            sl_p = max(0.5, min(final.sl_percent, CFG.max_sl_percent))
            tp_p = max(1.0, min(final.tp_percent, CFG.max_tp_percent))
            side = "buy" if final.decision == Decision.BUY else "sell"
            pname = "LONG" if side == "buy" else "SHORT"
            if side == "buy":
                sl_price = price * (1 - sl_p / 100)
                tp_price = price * (1 + tp_p / 100)
            else:
                sl_price = price * (1 + sl_p / 100)
                tp_price = price * (1 - tp_p / 100)
            balance = get_balance()
            if balance <= 0:
                logger.critical("❌ الرصيد صفر أو غير متاح")
                return
            qty = position_size(balance, price, sl_price)
            qty = float(exchange.amount_to_precision(sym, qty))
            if qty <= 0:
                logger.info(f"⏳ حجم الصفقة صفر بعد حساب المخاطرة لـ {sym}")
                return
            logger.info(f"🚀 بدء التنفيذ {sym} | {pname} | السعر: {price} | Qty: {qty} | Bal: {balance:.2f}")
            st["t"] = time.time()
            if CFG.dry_run:
                logger.info(f"📝 DRY RUN — لم يتم تنفيذ صفقة حقيقية لـ {sym}")
                tid = db.insert_trade(
                    symbol=sym, side=pname, mode="DRY_RUN", entry_price=price,
                    quantity=qty, sl_price=sl_price, tp_price=tp_price,
                    sl_order_id="", tp_order_id="", entry_order_id="",
                    confidence=final.final_score,
                    reason=f"DRY | APEX={final.apex_score:.0f} AI={final.ai_score:.0f} Final={final.final_score:.1f}",
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    ai_explanation=final.ai_explanation, final_score=final.final_score,
                    regime=final.regime, entry_quality=final.entry_quality,
                    risk_score=final.risk_score, tf_alignment=final.tf_alignment)
                logger.info(f"✅ DRY RUN Trade #{tid}")
                return
            exchange.set_leverage(CFG.leverage, sym)
            order = exchange.create_market_order(sym, side, qty)
            eoid = order.get("id", "")
            time.sleep(2)
            p = get_pos(sym)
            if p == "ERROR" or p is None:
                logger.critical(f"❌ لم يتم العثور على المركز بعد فتحه لـ {sym}")
                return
            entry = float(p.get("entryPrice", price))
            aqty = abs(float(p.get("contracts", 0)))
            if aqty <= 0:
                emergency_close(sym, "zero qty")
                return
            if side == "buy":
                sl_price = entry * (1 - sl_p / 100)
                tp_price = entry * (1 + tp_p / 100)
            else:
                sl_price = entry * (1 + sl_p / 100)
                tp_price = entry * (1 - tp_p / 100)
            sl_price = float(exchange.price_to_precision(sym, sl_price))
            tp_price = float(exchange.price_to_precision(sym, tp_price))
            cs = "sell" if side == "buy" else "buy"
            sloid, tpoid = "", ""
            try:
                slo = exchange.create_order(sym, "STOP_MARKET", cs, aqty, None,
                                            {"stopPrice": sl_price, "reduceOnly": True, "workingType": "MARK_PRICE"})
                sloid = slo.get("id", "")
            except Exception as e:
                logger.critical(f"SL fail: {e}")
                emergency_close(sym, "SL fail")
                return
            try:
                tpo = exchange.create_order(sym, "TAKE_PROFIT_MARKET", cs, aqty, None,
                                            {"stopPrice": tp_price, "reduceOnly": True, "workingType": "MARK_PRICE"})
                tpoid = tpo.get("id", "")
            except Exception as e:
                logger.error(f"TP fail: {e}")
                if sloid:
                    try:
                        exchange.cancel_order(sloid, sym)
                    except Exception:
                        pass
                emergency_close(sym, "TP fail")
                return
            tid = db.insert_trade(
                symbol=sym, side=pname, mode="LIVE", entry_price=entry,
                quantity=aqty, sl_price=sl_price, tp_price=tp_price,
                sl_order_id=sloid, tp_order_id=tpoid, entry_order_id=eoid,
                confidence=final.final_score,
                reason=f"APEX={final.apex_score:.0f} AI={final.ai_score:.0f} Final={final.final_score:.1f}",
                timestamp=datetime.now(timezone.utc).isoformat(),
                ai_explanation=final.ai_explanation, final_score=final.final_score,
                regime=final.regime, entry_quality=final.entry_quality,
                risk_score=final.risk_score, tf_alignment=final.tf_alignment)
            logger.info(f"✅ تمت الصفقة بنجاح #{tid}")
        except Exception as e:
            logger.error(f"Exec Error: {e}", exc_info=True)
            emergency_close(sym, str(e))


class PositionMonitor:
    def __init__(self):
        self._run = True

    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()
        logger.info("Monitor started")

    def stop(self):
        self._run = False

    def _loop(self):
        while self._run:
            try:
                for t in db.get_open_trades():
                    self._check(t)
            except Exception as e:
                logger.error(f"Monitor: {e}")
            time.sleep(CFG.monitor_interval)

    def _check(self, trade):
        sym = trade["symbol"]
        try:
            trade_time = datetime.fromisoformat(trade["timestamp"].replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - trade_time).total_seconds() < 60:
                return
        except Exception:
            pass
        sl_st = self._ost(sym, trade.get("sl_order_id"))
        tp_st = self._ost(sym, trade.get("tp_order_id"))
        pos = get_pos(sym)
        if pos == "ERROR":
            return
        if pos is None:
            reason = "STOP_LOSS" if sl_st == "closed" else "TAKE_PROFIT" if tp_st == "closed" else "MANUAL"
            ep, rpnl, comm = self._rexit(sym, trade)
            entry = trade["entry_price"]
            qty = trade["quantity"]
            if ep == 0:
                ep = entry
            if rpnl == 0:
                cs = self._csz(sym)
                rq = qty * cs
                rpnl = (ep - entry) * rq if trade["side"] == "LONG" else (entry - ep) * rq
            notional = entry * qty if entry * qty else 1
            pp = (rpnl / notional) * 100
            db.close_trade(trade["id"], ep, rpnl, pp, comm, reason)
            logger.info(f"CLOSED {sym} | {reason} | PnL={rpnl:.4f} ({pp:.2f}%)")
            self._cancel(sym, trade)

    def _csz(self, sym):
        try:
            return float(exchange.market(sym).get("contractSize", 1) or 1)
        except Exception:
            return 1.0

    def _ost(self, sym, oid):
        if not oid:
            return "unknown"
        try:
            return exchange.fetch_order(oid, sym).get("status", "unknown")
        except Exception:
            return "unknown"

    def _rexit(self, sym, trade):
        ep, rpnl, comm = 0, 0, 0
        try:
            trades = exchange.fetch_my_trades(sym, limit=30)
            for t in reversed(trades):
                if t.get("reduceOnly") or (t.get("side") == "sell" and trade["side"] == "LONG") or (t.get("side") == "buy" and trade["side"] == "SHORT"):
                    ep = float(t.get("price", 0) or t.get("average", 0))
                    comm = float(t.get("fee", {}).get("cost", 0) or 0)
                    info = t.get("info", {})
                    rs = info.get("realizedPnl", "0")
                    rpnl = float(rs) if rs else 0
                    break
        except Exception:
            pass
        if ep == 0:
            try:
                ep = exchange_public.fetch_ticker(sym)["last"]
            except Exception:
                pass
        return ep, rpnl, comm

    def _cancel(self, sym, trade):
        logger.info(f"🧹 جاري تنظيف الطلبات المرتبطة بالصفقة المنتهية {sym}...")
        for order_type, oid in [("SL", trade.get("sl_order_id")), ("TP", trade.get("tp_order_id"))]:
            if not oid:
                continue
            try:
                exchange.cancel_order(oid, sym)
                logger.info(f"✅ تم حذف طلب {order_type} الخاص بالبوت بنجاح (ID: {oid}).")
            except Exception as e:
                if "-2011" in str(e) or "Unknown order" in str(e) or "Order does not exist" in str(e):
                    logger.info(f"ℹ️ طلب {order_type} غير موجود (تم تنفيذه أو حذفه مسبقاً).")
                else:
                    logger.error(f"⚠️ خطأ غير متوقع أثناء حذف طلب {order_type}: {e}")


async def ws_worker():
    delay = CFG.ws_reconnect_delay
    while True:
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
            async with websockets.connect(url, ping_interval=CFG.ws_ping_interval, ping_timeout=CFG.ws_ping_timeout) as ws:
                logger.info(f"WS connected ({len(current)} symbols)")
                delay = CFG.ws_reconnect_delay
                async for msg in ws:
                    data = json.loads(msg)
                    k = data.get("data", {}).get("k")
                    if not k:
                        continue
                    sk = k["s"].lower()
                    tf = k["i"]
                    candle = [k["t"], float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]), float(k["v"])]
                    cm.update(sk, tf, candle, k["x"])
        except Exception as e:
            logger.error(f"WS error: {e}")
        await asyncio.sleep(delay)
        delay = min(delay * 2, 120)


def main():
    ip = show_deploy_ip()
    logger.info("=" * 60)
    logger.info("APEX TRADING BOT v3.0 — Multi-Layer Fusion")
    logger.info(f"   IP: {ip}")
    logger.info(f"   Mode: {'DRY_RUN 📝' if CFG.dry_run else 'LIVE 🚀'}")
    logger.info(f"   Scanner every {CFG.scanner_interval}s → Top {CFG.scanner_top_n}")
    logger.info(f"   Min Signal Score: {CFG.min_signal_score} | Min Confidence: {CFG.min_confidence}")
    logger.info(f"   Min Module Agreement: {CFG.min_module_agreement}")
    logger.info(f"   Max Risk Score: {CFG.max_risk_for_entry} | Open Positions: {CFG.max_open_positions}")
    logger.info(f"   Leverage: x{CFG.leverage} | Risk/Trade: {CFG.risk_per_trade_pct}%")
    logger.info(f"   SL: {CFG.max_sl_percent}% | TP: {CFG.max_tp_percent}% | Ratio: 1:{CFG.max_tp_percent/CFG.max_sl_percent:.1f}")
    logger.info(f"   Max Daily Loss: {CFG.max_daily_loss_pct}% | Max Consec Losses: {CFG.max_consecutive_losses}")
    logger.info(f"   DB: {CFG.db_path}")
    logger.info("=" * 60)
    threading.Thread(target=run_server, daemon=True).start()
    time.sleep(2)
    try:
        t = exchange_public.fetch_ticker("BTC/USDT:USDT")
        logger.info(f"Binance OK | BTC: {t['last']}")
    except Exception as e:
        logger.critical(f"Binance connection failed: {e}")
        return
    logger.info("Loading historical data...")
    for sk, sym in CFG.watchlist.items():
        cm.ensure(sk, CFG.timeframes)
        for tf in CFG.timeframes:
            try:
                limit = 500 if tf == "1d" else 300
                data = exchange_public.fetch_ohlcv(sym, timeframe=tf, limit=limit)
                cm.load(sk, tf, data)
            except Exception as e:
                logger.warning(f"Load {sym} {tf}: {e}")
            time.sleep(0.2)
    logger.info("Data ready")
    monitor = PositionMonitor()
    monitor.start()
    scanner = MarketScanner()
    scanner.start()
    bot_stats["status"] = "RUNNING"
    try:
        asyncio.run(ws_worker())
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
        scanner.stop()
        monitor.stop()


if __name__ == "__main__":
    main()
