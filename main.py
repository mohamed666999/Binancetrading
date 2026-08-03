#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║     APEX TRADING BOT v3.1 — Multi-Layer Signal Fusion        ║
║                                                              ║
║  Architecture:                                               ║
║  • Layer 1: 9 Independent Signal Modules (APEX Classic)      ║
║  • Layer 2: Technical Indicators Engine (RSI/MACD/EMA/BB)    ║
║  • Layer 3: Market Structure (S/R, Volume Profile, HH/HL)    ║
║  • Layer 4: Derivatives Intelligence (OI/Funding/LSR/Flow)   ║
║  • Layer 5: Regime Classifier (9 regimes, adaptive weights)  ║
║  • Layer 6: Multi-Timeframe Alignment                        ║
║  • Layer 7: AI Veto / Explainer (15% weight)                 ║
║  • Layer 8: External Strategies Veto (conor19w)              ║
║  • Layer 9: ISS Quantum (Information Spacetime Singularity)  ║
║  • Layer 10: AMF — Adaptive Momentum Fusion [NEW]            ║
║                                                              ║
║  5-Slot System: Slot1-2=Normal | Slot3=Strong (+Lev)         ║
║                 Slot4=VeryStrong | Slot5=SNIPER APEX         ║
║                                                              ║
║  Merged from: APEX v1 + MSSI v2 + APEX v3 Technical Layer   ║
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

# --- External Strategies Integration (Project B / conor19w) ---
try:
    from adapters.conor19w_adapter import call_strategy_by_name
    from signals.aggregator import aggregate_signals
    EXTERNAL_AVAILABLE = True
except ImportError:
    EXTERNAL_AVAILABLE = False

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
    return {"above_cloud": above_cloud, "bullish_cloud": bullish_cloud, "tk_cross": tk_cross,
            "tenkan": tenkan, "kijun": kijun, "senkou_a": senkou_a, "senkou_b": senkou_b}


# ══════════════════════════════════════════════════════════════
# ✅ دوال مساعدة لموديول ISS (بدون numpy/scipy — math فقط)
# ══════════════════════════════════════════════════════════════
def _entropy_manual(values):
    if not values:
        return 0.0
    min_v = min(values)
    max_v = max(values)
    range_v = max_v - min_v
    if range_v < 1e-12:
        return 0.0
    bins = 10
    counts = [0] * bins
    for v in values:
        idx = int((v - min_v) / range_v * bins)
        idx = min(idx, bins - 1)
        counts[idx] += 1
    total = len(values)
    ent = 0.0
    for c in counts:
        if c > 0:
            p = c / total
            ent -= p * math.log(p + 1e-10)
    return ent

def _gradient_manual(values):
    n = len(values)
    if n < 2:
        return [0.0]
    grad = [0.0] * n
    grad[0] = values[1] - values[0]
    grad[-1] = values[-1] - values[-2]
    for i in range(1, n - 1):
        grad[i] = (values[i + 1] - values[i - 1]) / 2.0
    return grad


# ══════════════════════════════════════════════════════════════
# NEW: دوال مساعدة لموديول AMF (Adaptive Momentum Fusion)
# ══════════════════════════════════════════════════════════════
def _heikin_ashi(opens, highs, lows, closes):
    """حساب شموع Heikin-Ashi يدوياً"""  # NEW
    ha_o, ha_c, ha_h, ha_l = [], [], [], []
    for i in range(len(closes)):
        ha_close = (opens[i] + highs[i] + lows[i] + closes[i]) / 4
        ha_open = (opens[i - 1] + closes[i - 1]) / 2 if i > 0 else opens[i]
        ha_high = max(highs[i], ha_open, ha_close)
        ha_low = min(lows[i], ha_open, ha_close)
        ha_o.append(ha_open)
        ha_c.append(ha_close)
        ha_h.append(ha_high)
        ha_l.append(ha_low)
    return ha_o, ha_h, ha_l, ha_c

def _calc_cvd(closes, volumes, lookback=20):
    """CVD — Cumulative Volume Delta: فرق ضغط الشراء والبيع"""  # NEW
    n = min(lookback, len(closes))
    cvd_vals = []
    running = 0.0
    start = len(closes) - n
    for i in range(start, len(closes)):
        if i == 0:
            running += volumes[i]
        else:
            if closes[i] > closes[i - 1]:
                running += volumes[i]
            elif closes[i] < closes[i - 1]:
                running -= volumes[i]
        cvd_vals.append(running)
    return cvd_vals

def _detect_fvg(highs, lows, lookback=10):
    """Fair Value Gap (ICT): فجوات القيمة العادلة"""  # NEW
    bull_fvg = []
    bear_fvg = []
    n = min(lookback, len(highs) - 2)
    for i in range(n):
        idx = len(highs) - n + i
        if idx < 2:
            continue
        # Bull FVG: high[idx-2] < low[idx] => gap up
        if highs[idx - 2] < lows[idx]:
            bull_fvg.append((highs[idx - 2], lows[idx]))
        # Bear FVG: low[idx-2] > high[idx] => gap down
        if lows[idx - 2] > highs[idx]:
            bear_fvg.append((lows[idx - 2], highs[idx]))
    return bull_fvg, bear_fvg

def _ema_compression_score(closes, periods=(5, 8, 13, 21)):
    """قياس انضغاط EMA — كلما انضغط أكثر = انفجار محتمل"""  # NEW
    n = len(closes)
    emas = {}
    for p in periods:
        if n >= p:
            emas[p] = _ema(closes, p)
    if len(emas) < 2:
        return 0.0, False, False
    vals = [emas[p][-1] for p in sorted(emas.keys())]
    price = closes[-1]
    spread = (max(vals) - min(vals)) / price * 100
    # تاريخ الانضغاط قبل 5 شموع
    vals_prev = []
    for p in sorted(emas.keys()):
        if len(emas[p]) >= 5:
            vals_prev.append(emas[p][-5])
    spread_prev = (max(vals_prev) - min(vals_prev)) / closes[-5] * 100 if len(vals_prev) == len(vals) and len(closes) >= 5 else spread
    compressing = spread < spread_prev * 0.65  # انضغاط بأكثر من 35%
    # اتجاه الانفجار
    sorted_periods = sorted(emas.keys())
    breakout_up = all(emas[sorted_periods[i]][-1] > emas[sorted_periods[i + 1]][-1] for i in range(len(sorted_periods) - 1))
    breakout_down = all(emas[sorted_periods[i]][-1] < emas[sorted_periods[i + 1]][-1] for i in range(len(sorted_periods) - 1))
    return spread, compressing, breakout_up, breakout_down


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
    ai_model: str = "mistralai/mistral-medium-3.5-128b"
    dry_run: bool = False
    leverage: int = 10
    risk_per_trade_pct: float = 3.0
    tier_levels_enabled: bool = True
    trailing_enabled: bool = True
    trailing_activation: float = 80.0
    trailing_drop: float = 9.0
    max_daily_trades: int = 12
    max_open_positions: int = 5          # MODIFIED: من 2 إلى 5 صفقات متزامنة
    cooldown_seconds: int = 120
    max_sl_percent: float = 2.0
    max_tp_percent: float = 5.0
    min_rr_ratio: float = 2.0
    max_daily_loss_pct: float = 4.0
    max_consecutive_losses: int = 4
    min_signal_score: float = 52.0
    min_confidence: float = 45.0
    min_module_agreement: int = 3
    min_entry_quality: float = 48.0
    max_risk_for_entry: float = 48.0
    min_momentum_score: float = 45.0
    min_trend_alignment: int = 2
    use_ai_veto: bool = False
    use_ai_explainer: bool = True
    ai_min_veto_confidence: float = 80.0
    use_external_strategies: bool = True
    external_strategies_list: List[str] = field(default_factory=lambda: ["candle_wick", "EMA_cross", "stochBB", "StochRSIMACD"])
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
    # ══════════════════════════════════════════════════════
    # NEW: نظام 5 فتحات — كل فتحة لها شروطها ورافعتها
    # Slot 1-2: شروط عادية (min_signal_score / min_confidence)
    # Slot 3:   إشارة أقوى + رافعة x1.5
    # Slot 4:   إشارة أقوى جداً + رافعة x2.0
    # Slot 5:   SNIPER فقط — أشد الشروط + رافعة x2.5
    # ══════════════════════════════════════════════════════
    slot3_min_score: float = 68.0         # NEW: الحد الأدنى للسكور في الفتحة 3
    slot3_min_confidence: float = 60.0    # NEW: الحد الأدنى للثقة في الفتحة 3
    slot3_leverage_mult: float = 1.5      # NEW: مضاعف الرافعة للفتحة 3

    slot4_min_score: float = 78.0         # NEW: الحد الأدنى للسكور في الفتحة 4
    slot4_min_confidence: float = 70.0    # NEW: الحد الأدنى للثقة في الفتحة 4
    slot4_leverage_mult: float = 2.0      # NEW: مضاعف الرافعة للفتحة 4

    slot5_min_score: float = 88.0         # NEW: الحد الأدنى للسكور في الفتحة 5 (SNIPER)
    slot5_min_confidence: float = 82.0    # NEW: الحد الأدنى للثقة في الفتحة 5
    slot5_leverage_mult: float = 2.5      # NEW: مضاعف الرافعة للفتحة 5 (SNIPER)
    slot5_min_modules: int = 7            # NEW: الحد الأدنى للموديولات المتفقة في الفتحة 5
    max_leverage_cap: int = 75            # NEW: الحد الأقصى للرافعة عبر كل الفتحات
    # ══════════════════════════════════════════════════════
    watchlist: Dict[str, str] = field(default_factory=lambda: {
        "btcusdt": "BTC/USDT:USDT", "ethusdt": "ETH/USDT:USDT", "solusdt": "SOL/USDT:USDT",
        "bnbusdt": "BNB/USDT:USDT", "xrpusdt": "XRP/USDT:USDT", "adausdt": "ADA/USDT:USDT",
        "linkusdt": "LINK/USDT:USDT", "avaxusdt": "AVAX/USDT:USDT", "dogeusdt": "DOGE/USDT:USDT",
        "wifusdt": "WIF/USDT:USDT", "1000pepeusdt": "1000PEPE/USDT:USDT", "suiusdt": "SUI/USDT:USDT",
        "aaveusdt": "AAVE/USDT:USDT", "nearusdt": "NEAR/USDT:USDT", "arbusdt": "ARB/USDT:USDT",
        "aptusdt": "APT/USDT:USDT",
        "opusdt": "OP/USDT:USDT",
        "jupusdt": "JUP/USDT:USDT",
        "tiausdt": "TIA/USDT:USDT",
    })

    db_path: str = "apex_trades_v3.db"
    ws_ping_interval: int = 20
    ws_ping_timeout: int = 20
    ws_reconnect_delay: int = 8


CFG = Config()

# MODIFIED: لا تستخدم مفاتيح API داخل الملف.
# احذف أي مفاتيح قديمة موجودة في بداية Config وألغها من المنصات فوراً.
CFG.binance_api_key = os.getenv("BINANCE_API_KEY", "").strip()
CFG.binance_secret = os.getenv("BINANCE_SECRET", "").strip()
CFG.nvidia_api_key = os.getenv("NVIDIA_API_KEY", "").strip()

# MODIFIED: الوضع التجريبي هو الافتراضي لأمان الحساب.
CFG.dry_run = os.getenv("APEX_DRY_RUN", "true").lower() in (
    "1", "true", "yes", "on"
)

# MODIFIED: خمس صفقات كحد أقصى.
CFG.max_open_positions = 5

# NEW: إذا لم يوجد مفتاح NVIDIA فلا نرسل طلبات AI.
if not CFG.nvidia_api_key:
    CFG.use_ai_veto = False
    CFG.use_ai_explainer = False


# NEW: إصلاح دالة EMA compression بحيث تعيد دائماً أربع قيم.
def _ema_compression_score(closes, periods=(5, 8, 13, 21)):
    if len(closes) < max(periods):
        return 0.0, False, False, False

    emas = {p: _ema(closes, p) for p in periods}
    current = [emas[p][-1] for p in periods]
    price = max(abs(closes[-1]), 1e-12)

    spread = (max(current) - min(current)) / price * 100.0

    previous = [emas[p][-5] for p in periods]
    previous_price = max(abs(closes[-5]), 1e-12)
    previous_spread = (
        (max(previous) - min(previous)) / previous_price * 100.0
    )

    compressing = (
        previous_spread > 0 and spread <= previous_spread * 0.65
    )

    breakout_up = all(
        emas[periods[i]][-1] > emas[periods[i + 1]][-1]
        for i in range(len(periods) - 1)
    )

    breakout_down = all(
        emas[periods[i]][-1] < emas[periods[i + 1]][-1]
        for i in range(len(periods) - 1)
    )

    return spread, compressing, breakout_up, breakout_down


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
        for candle in data:
            obj.opens.append(float(candle[1]))
            obj.highs.append(float(candle[2]))
            obj.lows.append(float(candle[3]))
            obj.closes.append(float(candle[4]))
            obj.volumes.append(float(candle[5]))
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
        return self.score > 55.0 and self.confidence > 40.0

    @property
    def bear_signal(self):
        return self.score < 45.0 and self.confidence > 40.0


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
    signal_score: float = 0.0
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
    # MODIFIED: تمت إضافة AMF مع إبقاء الأوزان مجموعها قريباً من 1.
    BASE_WEIGHTS = {
        "trend": 0.13,
        "momentum": 0.11,
        "volume": 0.09,
        "structure": 0.09,
        "candle": 0.06,
        "deriv": 0.11,
        "ichimoku": 0.06,
        "sr_levels": 0.05,
        "volatility": 0.04,
        "iss_quantum": 0.10,
        "amf": 0.16,
    }

    def analyze(
        self,
        data_primary,
        data_trend=None,
        data_fast=None,
        symbol=None,
        exchange_pub=None,
    ):
        if not data_primary or len(data_primary) < 50:
            return None

        primary = OHLCV.from_raw(data_primary)

        trend_d = (
            OHLCV.from_raw(data_trend)
            if data_trend and len(data_trend) >= 50
            else None
        )

        fast_d = (
            OHLCV.from_raw(data_fast)
            if data_fast and len(data_fast) >= 30
            else None
        )

        out = APEXOutput()
        deriv_data = {}

        if symbol and exchange_pub:
            try:
                deriv_data = {
                    "oi": deriv.open_interest(symbol),
                    "fund": deriv.funding(symbol),
                    "lsr": deriv.long_short_ratio(symbol),
                    "tf": deriv.taker_flow(symbol),
                    "ob": deriv.orderbook(exchange_pub, symbol),
                    "liq": deriv.liquidation_heatmap(
                        symbol, primary.closes[-1]
                    ),
                }
            except Exception as exc:
                logger.warning("Derivatives error %s: %s", symbol, exc)
                deriv_data = {}

        signals = [
            self._module_trend(primary, trend_d),
            self._module_momentum(primary),
            self._module_volume(primary),
            self._module_structure(primary),
            self._module_candle(primary),
            self._module_deriv(primary, deriv_data),
            self._module_ichimoku(primary),
            self._module_sr_levels(primary),
            self._module_volatility(primary),
            self._module_ethereal_iss(primary),
            self._module_adaptive_momentum_fusion(primary),
        ]

        out.module_signals = signals
        out.total_modules = len(signals)
        out.regime = self._detect_regime(primary, deriv_data)
        weights = self._adaptive_weights(out.regime)

        weighted_score = 0.0
        total_weight = 0.0
        bull_count = 0
        bear_count = 0

        for signal in signals:
            weight = weights.get(signal.name, 0.0)
            effective_weight = weight * signal.confidence / 100.0
            weighted_score += signal.score * effective_weight
            total_weight += effective_weight

            if signal.bull_signal:
                bull_count += 1
            elif signal.bear_signal:
                bear_count += 1

        out.composite_score = clamp(
            safe_div(weighted_score, total_weight, 50.0)
        )
        out.bull_modules = bull_count
        out.bear_modules = bear_count

        if trend_d is not None:
            tf_score = self._multi_tf_alignment(
                primary, trend_d, fast_d
            )
            out.composite_score = (
                out.composite_score * 0.70 + tf_score * 0.30
            )

        out.rsi = _rsi(primary.closes)

        atr_value = _atr(
            primary.highs,
            primary.lows,
            primary.closes,
        )

        out.volatility_pct = safe_div(
            atr_value, primary.closes[-1]
        ) * 100.0

        out.volume_spike = (
            len(primary.volumes) >= 20
            and primary.volumes[-1] > _mean(primary.volumes[-20:]) * 1.5
        )

        adx_value, plus_di, minus_di = _adx(
            primary.highs,
            primary.lows,
            primary.closes,
        )
        out.trend_strength = adx_value

        self._refresh_direction(out)

        sl_multiplier = 1.5 if adx_value > 25.0 else 2.0
        tp_multiplier = 3.0 if adx_value > 30.0 else 2.5
        tp_multiplier = max(CFG.min_rr_ratio, tp_multiplier)

        out.sl_percent = clamp(
            out.volatility_pct * sl_multiplier,
            0.5,
            CFG.max_sl_percent,
        )

        out.tp_percent = clamp(
            out.volatility_pct * tp_multiplier * sl_multiplier,
            1.0,
            CFG.max_tp_percent,
        )

        out.rr_ratio = safe_div(
            out.tp_percent,
            out.sl_percent,
            2.0,
        )

        filter_reason = self._smart_filters(
            primary, out, deriv_data
        )

        if filter_reason:
            out.warnings.append("FILTER: " + filter_reason)
            out.composite_score = (
                out.composite_score - 50.0
            ) * 0.50 + 50.0
            self._refresh_direction(out)

        out.confidence = self._calc_confidence(
            out, adx_value, deriv_data
        )

        bull_ok = (
            out.composite_score >= CFG.min_signal_score
            and out.bull_modules >= CFG.min_module_agreement
            and out.confidence >= CFG.min_confidence
            and out.direction == Direction.LONG
        )

        bear_ok = (
            out.composite_score <= 100.0 - CFG.min_signal_score
            and out.bear_modules >= CFG.min_module_agreement
            and out.confidence >= CFG.min_confidence
            and out.direction == Direction.SHORT
        )

        if bull_ok:
            out.decision = Decision.BUY
        elif bear_ok:
            out.decision = Decision.SELL
        else:
            out.decision = Decision.WAIT

        out.reasons = self._build_reasons(
            out, signals, adx_value, plus_di, minus_di
        )

        return out

    @staticmethod
    def _refresh_direction(out):
        if out.composite_score > 55.0:
            out.direction = Direction.LONG
        elif out.composite_score < 45.0:
            out.direction = Direction.SHORT
        else:
            out.direction = Direction.NEUTRAL

    def _module_trend(self, data, trend_data=None):
        closes = data.closes
        price = closes[-1]

        ema9 = _ema(closes, 9)[-1]
        ema21 = _ema(closes, 21)[-1]
        ema50 = _ema(closes, 50)[-1]
        ema200 = _ema(closes, 200)[-1] if len(closes) >= 200 else ema50

        bullish = price > ema9 > ema21 > ema50
        bearish = price < ema9 < ema21 < ema50

        bull_parts = sum([
            price > ema9,
            ema9 > ema21,
            ema21 > ema50,
        ])

        bear_parts = sum([
            price < ema9,
            ema9 < ema21,
            ema21 < ema50,
        ])

        ema_score = 50.0 + (bull_parts - bear_parts) * 12.0

        if bullish:
            ema_score += 10.0
        if bearish:
            ema_score -= 10.0

        macd_line, signal_line, histogram = _macd(closes)

        if macd_line > signal_line and histogram > 0:
            macd_score = 65.0
        elif macd_line < signal_line and histogram < 0:
            macd_score = 35.0
        else:
            macd_score = 50.0

        htf_score = 50.0

        if trend_data is not None:
            htf_price = trend_data.closes[-1]
            htf_ema21 = _ema(trend_data.closes, 21)[-1]
            htf_ema50 = _ema(trend_data.closes, 50)[-1]

            if htf_price > htf_ema21 > htf_ema50:
                htf_score = 70.0
            elif htf_price < htf_ema21 < htf_ema50:
                htf_score = 30.0

        score = clamp(
            ema_score * 0.50
            + macd_score * 0.30
            + htf_score * 0.20
        )

        confidence = 72.0 if bullish or bearish else 50.0
        direction = (
            Direction.LONG
            if score > 55
            else Direction.SHORT
            if score < 45
            else Direction.NEUTRAL
        )

        return ModuleSignal(
            name="trend",
            score=score,
            confidence=confidence,
            direction=direction,
            details={
                "ema9": ema9,
                "ema21": ema21,
                "ema50": ema50,
                "ema200": ema200,
                "macd": macd_line,
                "signal": signal_line,
                "hist": histogram,
            },
        )

    def _module_momentum(self, data):
        closes = data.closes
        rsi = _rsi(closes)
        stoch_k, stoch_d = _stochastic(
            data.highs,
            data.lows,
            closes,
        )

        if rsi < 30:
            rsi_score = 80.0
        elif rsi > 70:
            rsi_score = 20.0
        elif rsi > 60:
            rsi_score = 62.0
        elif rsi < 40:
            rsi_score = 38.0
        else:
            rsi_score = 50.0

        if len(closes) >= 15:
            rsi_score += (
                _rsi(closes) - _rsi(closes[:-1])
            ) * 0.5

        if stoch_k < 20 and stoch_k > stoch_d:
            stoch_score = 75.0
        elif stoch_k > 80 and stoch_k < stoch_d:
            stoch_score = 25.0
        elif stoch_k > stoch_d:
            stoch_score = 60.0
        elif stoch_k < stoch_d:
            stoch_score = 40.0
        else:
            stoch_score = 50.0

        if len(closes) >= 11:
            roc = safe_div(
                closes[-1] - closes[-11],
                closes[-11],
            ) * 100.0
            roc_score = clamp(50.0 + roc * 10.0)
        else:
            roc_score = 50.0

        score = clamp(
            rsi_score * 0.45
            + stoch_score * 0.35
            + roc_score * 0.20
        )

        confidence = (
            70.0
            if rsi < 30 or rsi > 70
            else 60.0
            if abs(rsi - 50.0) > 10
            else 45.0
        )

        direction = (
            Direction.LONG
            if score > 55
            else Direction.SHORT
            if score < 45
            else Direction.NEUTRAL
        )

        return ModuleSignal(
            name="momentum",
            score=score,
            confidence=confidence,
            direction=direction,
            details={
                "rsi": rsi,
                "stoch_k": stoch_k,
                "stoch_d": stoch_d,
            },
        )

    def _module_volume(self, data):
        closes = data.closes
        volumes = data.volumes

        n = min(20, len(volumes))
        volume_z = _zscore(volumes[-1], volumes[-n:]) if n >= 5 else 0.0

        rising = sum(
            1
            for i in range(-5, 0)
            if volumes[i] > volumes[i - 1]
        ) >= 3

        vwap_value = _vwap(
            data.highs,
            data.lows,
            closes,
            volumes,
        )

        price = closes[-1]
        above_vwap = price > vwap_value

        buy_volume = 0.0
        sell_volume = 0.0

        for i in range(max(1, len(closes) - 20), len(closes)):
            if closes[i] >= closes[i - 1]:
                buy_volume += volumes[i]
            else:
                sell_volume += volumes[i]

        volume_bias = safe_div(
            buy_volume - sell_volume,
            buy_volume + sell_volume,
        )

        score = 50.0
        score += volume_bias * 20.0
        score += 10.0 if above_vwap else -10.0
        score += 5.0 if rising else -5.0
        score += clamp(volume_z * 2.0, -10.0, 10.0)
        score = clamp(score)

        confidence = clamp(
            abs(volume_z) * 20.0 + 40.0,
            30.0,
            80.0,
        )

        direction = (
            Direction.LONG
            if score > 55
            else Direction.SHORT
            if score < 45
            else Direction.NEUTRAL
        )

        return ModuleSignal(
            name="volume",
            score=score,
            confidence=confidence,
            direction=direction,
            details={
                "vwap": vwap_value,
                "volume_z": volume_z,
                "volume_bias": volume_bias,
                "above_vwap": above_vwap,
            },
        )

    def _module_structure(self, data):
        price = data.closes[-1]

        if len(data.closes) < 20:
            return ModuleSignal(
                "structure", 50.0, 30.0, Direction.NEUTRAL
            )

        pivot = _pivot_points(
            data.highs[-2],
            data.lows[-2],
            data.closes[-2],
        )

        supports = [pivot["s1"], pivot["s2"], pivot["s3"]]
        resistances = [pivot["r1"], pivot["r2"], pivot["r3"]]

        nearest_support = min(
            abs(price - value) for value in supports
        )
        nearest_resistance = min(
            abs(price - value) for value in resistances
        )

        near_support = nearest_support <= price * 0.01
        near_resistance = nearest_resistance <= price * 0.01

        n = min(20, len(data.closes))
        higher_highs = 0
        higher_lows = 0
        lower_highs = 0
        lower_lows = 0

        for i in range(-n + 1, 0):
            if data.highs[i] > data.highs[i - 1]:
                higher_highs += 1
            if data.lows[i] > data.lows[i - 1]:
                higher_lows += 1
            if data.highs[i] < data.highs[i - 1]:
                lower_highs += 1
            if data.lows[i] < data.lows[i - 1]:
                lower_lows += 1

        bullish_structure = safe_div(
            higher_highs + higher_lows,
            n * 2,
        )
        bearish_structure = safe_div(
            lower_highs + lower_lows,
            n * 2,
        )

        score = 50.0 + (
            bullish_structure - bearish_structure
        ) * 50.0

        if near_support:
            score += 10.0
        if near_resistance:
            score -= 10.0

        high20 = max(data.highs[-20:])
        low20 = min(data.lows[-20:])
        range20 = high20 - low20

        if range20 > 0:
            position = (price - low20) / range20
            score += (position - 0.5) * 20.0

        score = clamp(score)
        confidence = 60.0 if near_support or near_resistance else 45.0

        direction = (
            Direction.LONG
            if score > 55
            else Direction.SHORT
            if score < 45
            else Direction.NEUTRAL
        )

        return ModuleSignal(
            name="structure",
            score=score,
            confidence=confidence,
            direction=direction,
            details={
                "pp": pivot["pp"],
                "r1": pivot["r1"],
                "s1": pivot["s1"],
                "near_support": near_support,
                "near_resistance": near_resistance,
            },
        )

    def _module_candle(self, data):
        patterns = _detect_candle_pattern(
            data.opens,
            data.highs,
            data.lows,
            data.closes,
        )

        _, _, _, percent_b, bandwidth = _bollinger(
            data.closes
        )

        score = 50.0

        if patterns.get("hammer") and percent_b < 0.30:
            score += 20.0

        if patterns.get("shooting_star") and percent_b > 0.70:
            score -= 20.0

        if patterns.get("bullish_engulfing"):
            score += 18.0

        if patterns.get("bearish_engulfing"):
            score -= 18.0

        if patterns.get("three_white_soldiers"):
            score += 22.0

        if patterns.get("three_black_crows"):
            score -= 22.0

        if percent_b < 0.10:
            score += 8.0
        elif percent_b > 0.90:
            score -= 8.0

        candle_range = max(
            data.highs[-1] - data.lows[-1],
            1e-12,
        )

        body_direction = (
            data.closes[-1] - data.opens[-1]
        ) / candle_range

        score += body_direction * 10.0
        score = clamp(score)

        has_pattern = any(
            key in patterns
            for key in (
                "hammer",
                "shooting_star",
                "bullish_engulfing",
                "bearish_engulfing",
                "three_white_soldiers",
                "three_black_crows",
            )
        )

        confidence = 65.0 if has_pattern else 35.0
        direction = (
            Direction.LONG
            if score > 55
            else Direction.SHORT
            if score < 45
            else Direction.NEUTRAL
        )

        return ModuleSignal(
            name="candle",
            score=score,
            confidence=confidence,
            direction=direction,
            details={
                "percent_b": percent_b,
                "bandwidth": bandwidth,
                "patterns": list(patterns.keys()),
            },
        )

    def _module_deriv(self, data, deriv_data):
        if not deriv_data:
            return ModuleSignal(
                "deriv", 50.0, 20.0, Direction.NEUTRAL
            )

        oi = deriv_data.get("oi", {})
        funding = deriv_data.get("fund", {})
        lsr = deriv_data.get("lsr", {})
        taker = deriv_data.get("tf", {})
        orderbook = deriv_data.get("ob", {})

        score = 50.0
        confidence_factors = []

        oi_change = oi.get("oi_change_1h", 0.0)
        price_change = safe_div(
            data.closes[-1] - data.closes[-2],
            data.closes[-2],
        )

        if oi_change > 0.005 and price_change > 0:
            score += 12.0
            confidence_factors.append(15.0)
        elif oi_change < -0.005 and price_change < 0:
            score -= 12.0
            confidence_factors.append(15.0)
        elif oi_change > 0 and price_change < 0:
            score -= 8.0
        elif oi_change < 0 and price_change > 0:
            score += 5.0

        score += oi.get("oi_trend", 0.0) * 10.0

        funding_rate = funding.get("rate", 0.0)
        funding_extreme = funding.get("extreme", 0.0)

        if funding_rate > 0.0005:
            score -= 15.0 if funding_extreme else 8.0
        elif funding_rate < -0.0003:
            score += 15.0 if funding_extreme else 8.0

        score += clamp(
            lsr.get("smart_money_bias", 0.0) * 0.15,
            -10.0,
            10.0,
        )

        if lsr.get("retail_overcrowded_long"):
            score -= 12.0
            confidence_factors.append(20.0)

        if lsr.get("retail_overcrowded_short"):
            score += 12.0
            confidence_factors.append(20.0)

        score += taker.get("imbalance", 0.0) * 15.0
        score += taker.get("momentum", 0.0) * 10.0
        score += orderbook.get("imbalance", 0.0) * 8.0

        if orderbook.get("spread_bps", 5.0) > 10.0:
            score = (score - 50.0) * 0.70 + 50.0

        score = clamp(score)

        confidence = (
            _mean(confidence_factors) + 40.0
            if confidence_factors
            else 45.0
        )
        confidence = clamp(confidence, 0.0, 85.0)

        direction = (
            Direction.LONG
            if score > 55
            else Direction.SHORT
            if score < 45
            else Direction.NEUTRAL
        )

        return ModuleSignal(
            name="deriv",
            score=score,
            confidence=confidence,
            direction=direction,
            details={
                "oi_change": oi_change,
                "funding": funding_rate,
                "smart_bias": lsr.get("smart_money_bias", 0.0),
                "taker_imbalance": taker.get("imbalance", 0.0),
            },
        )

    def _module_ichimoku(self, data):
        if len(data.closes) < 52:
            return ModuleSignal(
                "ichimoku", 50.0, 20.0, Direction.NEUTRAL
            )

        ichi = _ichimoku(
            data.highs,
            data.lows,
            data.closes,
        )

        score = 50.0
        score += ichi.get("above_cloud", 0) * 20.0
        score += ichi.get("bullish_cloud", 0) * 10.0
        score += ichi.get("tk_cross", 0) * 12.0

        price = data.closes[-1]
        kijun = ichi.get("kijun", price)

        score += clamp(
            safe_div(price - kijun, kijun) * 200.0,
            -10.0,
            10.0,
        )

        score = clamp(score)

        confidence = (
            65.0
            if ichi.get("above_cloud", 0) != 0
            and ichi.get("tk_cross", 0) != 0
            else 40.0
        )

        direction = (
            Direction.LONG
            if score > 55
            else Direction.SHORT
            if score < 45
            else Direction.NEUTRAL
        )

        return ModuleSignal(
            name="ichimoku",
            score=score,
            confidence=confidence,
            direction=direction,
            details=ichi,
        )

    def _module_sr_levels(self, data):
        if len(data.closes) < 30:
            return ModuleSignal(
                "sr_levels", 50.0, 25.0, Direction.NEUTRAL
            )

        price = data.closes[-1]
        n = min(50, len(data.closes))
        swing_highs = []
        swing_lows = []

        for i in range(2, n - 2):
            index = len(data.highs) - n + i

            if (
                data.highs[index] > data.highs[index - 1]
                and data.highs[index] > data.highs[index + 1]
            ):
                swing_highs.append(data.highs[index])

            if (
                data.lows[index] < data.lows[index - 1]
                and data.lows[index] < data.lows[index + 1]
            ):
                swing_lows.append(data.lows[index])

        fib_position = 0.5
        near_support = False
        near_resistance = False

        if swing_highs and swing_lows:
            recent_high = max(swing_highs[-3:])
            recent_low = min(swing_lows[-3:])
            fib_range = max(recent_high - recent_low, 1e-12)

            fib_levels = [
                recent_low + fib_range * 0.236,
                recent_low + fib_range * 0.382,
                recent_low + fib_range * 0.500,
                recent_low + fib_range * 0.618,
                recent_low + fib_range * 0.786,
            ]

            tolerance = price * 0.005
            fib_position = clamp(
                safe_div(price - recent_low, fib_range),
                0.0,
                1.0,
            )

            near_support = any(
                abs(price - level) < tolerance
                and price >= level - tolerance
                for level in fib_levels
            )

            near_resistance = any(
                abs(price - level) < tolerance
                and price <= level + tolerance
                for level in fib_levels
            )

        score = 50.0 + (fib_position - 0.5) * 30.0

        if near_support:
            score += 15.0
        if near_resistance:
            score -= 15.0

        for level in swing_lows[-5:]:
            if abs(price - level) < price * 0.015:
                score += 8.0

        for level in swing_highs[-5:]:
            if abs(price - level) < price * 0.015:
                score -= 8.0

        score = clamp(score)
        confidence = 60.0 if near_support or near_resistance else 40.0

        direction = (
            Direction.LONG
            if score > 55
            else Direction.SHORT
            if score < 45
            else Direction.NEUTRAL
        )

        return ModuleSignal(
            name="sr_levels",
            score=score,
            confidence=confidence,
            direction=direction,
            details={
                "fib_position": fib_position,
                "near_support": near_support,
                "near_resistance": near_resistance,
            },
        )

    def _module_volatility(self, data):
        atr_value = _atr(
            data.highs,
            data.lows,
            data.closes,
        )

        atr_pct = safe_div(
            atr_value,
            data.closes[-1],
        ) * 100.0

        _, _, _, percent_b, bandwidth = _bollinger(data.closes)

        if atr_pct < 0.3:
            score = 55.0
            regime = "COMPRESSED"
            confidence = 50.0
        elif atr_pct > 3.0:
            score = 50.0
            regime = "EXPLOSIVE"
            confidence = 35.0
        else:
            score = 50.0 + (percent_b - 0.5) * 20.0
            regime = "NORMAL"
            confidence = 55.0

        if bandwidth < 3.0:
            score = (score - 50.0) * 0.5 + 50.0
            confidence = max(confidence - 10.0, 20.0)

        score = clamp(score)

        direction = (
            Direction.LONG
            if score > 55
            else Direction.SHORT
            if score < 45
            else Direction.NEUTRAL
        )

        return ModuleSignal(
            name="volatility",
            score=score,
            confidence=confidence,
            direction=direction,
            details={
                "atr_pct": atr_pct,
                "bandwidth": bandwidth,
                "percent_b": percent_b,
                "volatility_regime": regime,
            },
        )

    def _module_ethereal_iss(self, data):
        closes = data.closes

        if len(closes) < 30:
            return ModuleSignal(
                "iss_quantum", 50.0, 10.0, Direction.NEUTRAL
            )

        changes = [
            closes[i] - closes[i - 1]
            for i in range(1, len(closes))
        ]

        entropy = _entropy_manual(changes)
        standard_deviation = _std(closes[-14:])
        singularity = entropy / (standard_deviation + 1e-10)

        recent_flux = _gradient_manual(closes[-5:])
        bias = _mean(recent_flux)

        score = 50.0 + safe_div(
            bias,
            closes[-1] * 0.001,
        ) * 10.0

        score = clamp(score)
        confidence = clamp(
            singularity * 20.0,
            30.0,
            98.0,
        )

        direction = (
            Direction.LONG
            if score > 55
            else Direction.SHORT
            if score < 45
            else Direction.NEUTRAL
        )

        return ModuleSignal(
            name="iss_quantum",
            score=score,
            confidence=confidence,
            direction=direction,
            details={
                "entropy": entropy,
                "singularity": singularity,
            },
        )

    # NEW: خوارزمية AMF تجمع الاتجاه والزخم والحجم وCVD والـ FVG.
    def _module_adaptive_momentum_fusion(self, data):
        closes = data.closes
        highs = data.highs
        lows = data.lows
        volumes = data.volumes

        if len(closes) < 60:
            return ModuleSignal(
                "amf", 50.0, 20.0, Direction.NEUTRAL
            )

        ema9 = _ema(closes, 9)[-1]
        ema21 = _ema(closes, 21)[-1]
        ema55 = _ema(closes, 55)[-1]

        trend_component = 0.0

        if ema9 > ema21 > ema55:
            trend_component = 1.0
        elif ema9 < ema21 < ema55:
            trend_component = -1.0
        else:
            trend_component = safe_div(
                ema9 - ema55,
                abs(ema55) * 0.01,
            )
            trend_component = clamp(
                trend_component,
                -1.0,
                1.0,
            )

        atr_value = _atr(highs, lows, closes)
        _, _, macd_hist = _macd(closes)

        macd_component = math.tanh(
            safe_div(macd_hist, atr_value) * 2.0
        )

        rsi_value = _rsi(closes)
        rsi_component = clamp(
            (rsi_value - 50.0) / 25.0,
            -1.0,
            1.0,
        )

        volume_z = _zscore(
            volumes[-1],
            volumes[-20:],
        )

        price_volume_component = clamp(
            volume_z / 3.0,
            -1.0,
            1.0,
        )

        cvd_values = _calc_cvd(
            closes,
            volumes,
            lookback=20,
        )

        cvd_component = 0.0
        if len(cvd_values) >= 10:
            cvd_component = math.tanh(
                safe_div(
                    cvd_values[-1] - cvd_values[-10],
                    _mean(volumes[-20:]) * 10.0,
                )
            )

        ha_o, _, _, ha_c = _heikin_ashi(
            data.opens,
            data.highs,
            data.lows,
            data.closes,
        )

        ha_bull = sum(
            1
            for i in range(-3, 0)
            if ha_c[i] > ha_o[i]
        )

        ha_component = (ha_bull - 1.5) / 1.5
        ha_component = clamp(ha_component, -1.0, 1.0)

        bull_fvg, bear_fvg = _detect_fvg(
            highs,
            lows,
            lookback=12,
        )

        fvg_component = 0.0
        current_price = closes[-1]

        if bull_fvg:
            low_edge = bull_fvg[-1][0]
            high_edge = bull_fvg[-1][1]
            if low_edge <= current_price <= high_edge * 1.01:
                fvg_component += 0.50

        if bear_fvg:
            low_edge = bear_fvg[-1][1]
            high_edge = bear_fvg[-1][0]
            if low_edge * 0.99 <= current_price <= high_edge:
                fvg_component -= 0.50

        spread, compressing, breakout_up, breakout_down = (
            _ema_compression_score(closes)
        )

        breakout_component = 0.0

        if breakout_up:
            breakout_component += 0.60
        elif breakout_down:
            breakout_component -= 0.60

        if compressing:
            breakout_component *= 0.50

        raw = (
            trend_component * 0.30
            + macd_component * 0.18
            + rsi_component * 0.12
            + price_volume_component * 0.12
            + cvd_component * 0.12
            + ha_component * 0.08
            + fvg_component * 0.04
            + breakout_component * 0.04
        )

        score = clamp(50.0 + raw * 35.0)

        directional_parts = [
            trend_component,
            macd_component,
            rsi_component,
            cvd_component,
            ha_component,
        ]

        positive = sum(value > 0.15 for value in directional_parts)
        negative = sum(value < -0.15 for value in directional_parts)
        agreement = max(positive, negative) / len(directional_parts)

        confidence = clamp(
            35.0
            + agreement * 45.0
            + min(abs(raw) * 20.0, 18.0),
            25.0,
            95.0,
        )

        direction = (
            Direction.LONG
            if score > 55.0
            else Direction.SHORT
            if score < 45.0
            else Direction.NEUTRAL
        )

        return ModuleSignal(
            name="amf",
            score=score,
            confidence=confidence,
            direction=direction,
            details={
                "trend_component": trend_component,
                "macd_component": macd_component,
                "rsi": rsi_value,
                "volume_z": volume_z,
                "cvd_component": cvd_component,
                "ema_spread": spread,
                "compressing": compressing,
                "breakout_up": breakout_up,
                "breakout_down": breakout_down,
            },
        )

    def _detect_regime(self, data, deriv_data):
        adx_value, plus_di, minus_di = _adx(
            data.highs,
            data.lows,
            data.closes,
        )

        _, _, _, _, bandwidth = _bollinger(data.closes)

        atr_value = _atr(
            data.highs,
            data.lows,
            data.closes,
        )

        atr_pct = safe_div(
            atr_value,
            data.closes[-1],
        ) * 100.0

        if adx_value > 25 and plus_di > minus_di:
            return Regime.TRENDING_UP

        if adx_value > 25 and minus_di > plus_di:
            return Regime.TRENDING_DOWN

        if bandwidth > 8 and atr_pct > 1.5:
            ema_fast = _ema(data.closes, 9)[-1]
            ema_slow = _ema(data.closes, 21)[-1]
            return (
                Regime.BREAKOUT_UP
                if ema_fast > ema_slow
                else Regime.BREAKOUT_DOWN
            )

        rsi_value = _rsi(data.closes)

        if rsi_value < 28:
            return Regime.REVERSAL_UP

        if rsi_value > 72:
            return Regime.REVERSAL_DOWN

        if atr_pct > 2.5:
            return Regime.HIGH_VOLATILITY

        oi_change = deriv_data.get(
            "oi", {}
        ).get("oi_change_1h", 0.0)

        if bandwidth < 4.0 and abs(oi_change) < 0.01:
            return Regime.ACCUMULATION

        return Regime.RANGING

    def _adaptive_weights(self, regime):
        weights = dict(self.BASE_WEIGHTS)

        if regime in (
            Regime.TRENDING_UP,
            Regime.TRENDING_DOWN,
        ):
            weights["trend"] += 0.05
            weights["ichimoku"] += 0.03
            weights["amf"] += 0.03
            weights["sr_levels"] -= 0.03

        elif regime in (
            Regime.BREAKOUT_UP,
            Regime.BREAKOUT_DOWN,
        ):
            weights["volume"] += 0.05
            weights["deriv"] += 0.03
            weights["amf"] += 0.04
            weights["sr_levels"] -= 0.02

        elif regime in (
            Regime.REVERSAL_UP,
            Regime.REVERSAL_DOWN,
        ):
            weights["momentum"] += 0.04
            weights["candle"] += 0.03
            weights["sr_levels"] += 0.03
            weights["trend"] -= 0.04

        elif regime == Regime.HIGH_VOLATILITY:
            weights["deriv"] += 0.05
            weights["amf"] += 0.04
            weights["trend"] -= 0.03
            weights["volatility"] -= 0.02

        for key in list(weights):
            weights[key] = max(weights[key], 0.01)

        total = sum(weights.values())
        return {
            key: value / total
            for key, value in weights.items()
        }

    def _multi_tf_alignment(self, primary, trend_data, fast_data=None):
        trend_price = trend_data.closes[-1]
        trend_ema21 = _ema(trend_data.closes, 21)[-1]
        trend_ema50 = _ema(trend_data.closes, 50)[-1]
        trend_rsi = _rsi(trend_data.closes)

        trend_score = 50.0

        if trend_price > trend_ema21 > trend_ema50:
            trend_score = 70.0
        elif trend_price < trend_ema21 < trend_ema50:
            trend_score = 30.0

        if trend_rsi > 60:
            trend_score += 5.0
        elif trend_rsi < 40:
            trend_score -= 5.0

        fast_score = 50.0

        if fast_data and len(fast_data.closes) >= 21:
            fast_ema9 = _ema(fast_data.closes, 9)[-1]
            fast_ema21 = _ema(fast_data.closes, 21)[-1]

            if fast_ema9 > fast_ema21:
                fast_score = 65.0
            elif fast_ema9 < fast_ema21:
                fast_score = 35.0

        return clamp(
            trend_score * 0.65 + fast_score * 0.35
        )

    def _smart_filters(self, data, output, deriv_data):
        orderbook = deriv_data.get("ob", {})
        funding = deriv_data.get("fund", {})

        if orderbook.get("spread_bps", 0.0) > 15.0:
            return "spread is too wide"

        funding_rate = funding.get("rate", 0.0)
        funding_extreme = funding.get("extreme", 0.0)

        if (
            funding_extreme
            and output.direction == Direction.LONG
            and funding_rate > 0.002
        ):
            return "positive funding is extreme"

        if (
            funding_extreme
            and output.direction == Direction.SHORT
            and funding_rate < -0.001
        ):
            return "negative funding may cause short squeeze"

        rsi_now = _rsi(data.closes)

        if len(data.closes) >= 20:
            rsi_previous = _rsi(data.closes[:-1])

            if (
                data.closes[-1] >= max(data.closes[-20:-1])
                and rsi_now < rsi_previous
            ):
                return "possible bearish RSI divergence"

            if (
                data.closes[-1] <= min(data.closes[-20:-1])
                and rsi_now > rsi_previous
            ):
                return "possible bullish RSI divergence"

        return None

    def _calc_confidence(self, output, adx_value, deriv_data):
        active = output.bull_modules + output.bear_modules

        if active <= 0:
            agreement = 0.0
        else:
            agreement = max(
                output.bull_modules,
                output.bear_modules,
            ) / active

        confidence = agreement * 70.0

        if adx_value > 30:
            confidence += 15.0
        elif adx_value > 20:
            confidence += 8.0

        oi_change = deriv_data.get(
            "oi", {}
        ).get("oi_change_1h", 0.0)

        if abs(oi_change) > 0.005:
            confidence += 8.0

        confidence += (
            abs(output.composite_score - 50.0) / 50.0
        ) * 15.0

        return clamp(confidence, 0.0, 95.0)

    def _build_reasons(
        self,
        output,
        signals,
        adx_value,
        plus_di,
        minus_di,
    ):
        reasons = [
            (
                f"Regime={output.regime.value} "
                f"Direction={output.direction.value} "
                f"Score={output.composite_score:.1f}"
            ),
            (
                f"Modules: Bull={output.bull_modules}/"
                f"{output.total_modules} "
                f"Bear={output.bear_modules}/"
                f"{output.total_modules}"
            ),
            (
                f"ADX={adx_value:.1f} "
                f"+DI={plus_di:.1f} "
                f"-DI={minus_di:.1f} "
                f"RSI={output.rsi:.1f}"
            ),
            (
                f"Volatility={output.volatility_pct:.2f}% "
                f"VolumeSpike={output.volume_spike}"
            ),
        ]

        for signal in signals:
            marker = (
                "BULL"
                if signal.bull_signal
                else "BEAR"
                if signal.bear_signal
                else "NEUTRAL"
            )

            reasons.append(
                f"{marker} [{signal.name}] "
                f"Score={signal.score:.1f} "
                f"Conf={signal.confidence:.1f}%"
            )

        return reasons


deriv = DerivativesFeed()
apex_engine = APEXEngine()


class TradeDB:
    def __init__(self, path):
        self.conn = sqlite3.connect(
            path,
            check_same_thread=False,
        )
        self.lock = threading.Lock()
        self._init_tables()

    def _init_tables(self):
        with self.lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    side TEXT,
                    mode TEXT,
                    entry_price REAL,
                    quantity REAL,
                    sl_price REAL,
                    tp_price REAL,
                    sl_order_id TEXT DEFAULT '',
                    tp_order_id TEXT DEFAULT '',
                    entry_order_id TEXT DEFAULT '',
                    confidence REAL,
                    entry_quality REAL,
                    risk_score REAL,
                    regime TEXT,
                    reason TEXT,
                    timestamp TEXT,
                    status TEXT DEFAULT 'OPEN',
                    exit_price REAL,
                    realized_pnl REAL,
                    pnl_percent REAL,
                    commission REAL DEFAULT 0,
                    closed_at TEXT,
                    close_reason TEXT,
                    ai_explanation TEXT,
                    tf_alignment INTEGER,
                    final_score REAL
                );

                CREATE INDEX IF NOT EXISTS idx_status
                ON trades(status);

                CREATE INDEX IF NOT EXISTS idx_symbol
                ON trades(symbol);

                CREATE INDEX IF NOT EXISTS idx_timestamp
                ON trades(timestamp);
                """
            )
            self.conn.commit()

    def insert_trade(self, **kwargs):
        with self.lock:
            cursor = self.conn.execute(
                """
                INSERT INTO trades (
                    symbol,
                    side,
                    mode,
                    entry_price,
                    quantity,
                    sl_price,
                    tp_price,
                    sl_order_id,
                    tp_order_id,
                    entry_order_id,
                    confidence,
                    entry_quality,
                    risk_score,
                    regime,
                    reason,
                    timestamp,
                    status,
                    ai_explanation,
                    tf_alignment,
                    final_score
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    kwargs.get("symbol", ""),
                    kwargs.get("side", ""),
                    kwargs.get("mode", ""),
                    kwargs.get("entry_price", 0.0),
                    kwargs.get("quantity", 0.0),
                    kwargs.get("sl_price", 0.0),
                    kwargs.get("tp_price", 0.0),
                    kwargs.get("sl_order_id", ""),
                    kwargs.get("tp_order_id", ""),
                    kwargs.get("entry_order_id", ""),
                    kwargs.get("confidence", 0.0),
                    kwargs.get("entry_quality", 0.0),
                    kwargs.get("risk_score", 50.0),
                    kwargs.get("regime", ""),
                    kwargs.get("reason", ""),
                    kwargs.get(
                        "timestamp",
                        datetime.now(timezone.utc).isoformat(),
                    ),
                    kwargs.get("status", "OPEN"),
                    kwargs.get("ai_explanation", ""),
                    kwargs.get("tf_alignment", 0),
                    kwargs.get("final_score", 0.0),
                ),
            )

            self.conn.commit()
            return cursor.lastrowid

    def close_trade(
        self,
        trade_id,
        exit_price,
        realized_pnl,
        pnl_percent,
        commission,
        reason,
    ):
        with self.lock:
            self.conn.execute(
                """
                UPDATE trades
                SET status='CLOSED',
                    exit_price=?,
                    realized_pnl=?,
                    pnl_percent=?,
                    commission=?,
                    closed_at=?,
                    close_reason=?
                WHERE id=?
                """,
                (
                    exit_price,
                    realized_pnl,
                    pnl_percent,
                    commission,
                    datetime.now(timezone.utc).isoformat(),
                    reason,
                    trade_id,
                ),
            )
            self.conn.commit()

    def get_open_trades(self):
        with self.lock:
            cursor = self.conn.execute(
                "SELECT * FROM trades WHERE status='OPEN'"
            )
            rows = cursor.fetchall()
            columns = [
                description[0]
                for description in cursor.description
            ]

        return [
            dict(zip(columns, row))
            for row in rows
        ]

    def count_today(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        with self.lock:
            result = self.conn.execute(
                """
                SELECT COUNT(*)
                FROM trades
                WHERE timestamp LIKE ?
                """,
                (today + "%",),
            ).fetchone()

        return int(result[0] if result else 0)

    def open_count(self):
        with self.lock:
            result = self.conn.execute(
                """
                SELECT COUNT(*)
                FROM trades
                WHERE status='OPEN'
                """
            ).fetchone()

        return int(result[0] if result else 0)

    def consecutive_losses(self):
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT realized_pnl
                FROM trades
                WHERE status='CLOSED'
                ORDER BY closed_at DESC
                LIMIT ?
                """,
                (CFG.max_consecutive_losses,),
            ).fetchall()

        if len(rows) < CFG.max_consecutive_losses:
            return 0

        return sum(
            1
            for row in rows
            if row[0] is not None and row[0] < 0
        )

    def daily_pnl(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        with self.lock:
            result = self.conn.execute(
                """
                SELECT COALESCE(SUM(realized_pnl), 0)
                FROM trades
                WHERE status='CLOSED'
                AND closed_at LIKE ?
                """,
                (today + "%",),
            ).fetchone()

        return float(result[0] if result else 0.0)

    def get_stats(self):
        with self.lock:
            total = self.conn.execute(
                """
                SELECT COUNT(*)
                FROM trades
                WHERE status='CLOSED'
                """
            ).fetchone()[0]

            wins = self.conn.execute(
                """
                SELECT COUNT(*)
                FROM trades
                WHERE status='CLOSED'
                AND realized_pnl > 0
                """
            ).fetchone()[0]

            pnl = self.conn.execute(
                """
                SELECT COALESCE(SUM(realized_pnl), 0)
                FROM trades
                WHERE status='CLOSED'
                """
            ).fetchone()[0]

        return {
            "total": int(total),
            "wins": int(wins),
            "winrate": wins / total * 100.0 if total else 0.0,
            "total_pnl": float(pnl or 0.0),
        }


db = TradeDB(CFG.db_path)


app = Flask(__name__)

bot_stats = {
    "status": "STARTING",
    "version": "APEX-v3.1-AMF-5SLOTS",
    "uptime": 0,
    "trades_today": 0,
    "open_positions": 0,
    "scanner": [],
    "last_analysis": {},
    "mode": "DRY_RUN" if CFG.dry_run else "LIVE",
    "performance": {},
}

T0 = time.time()


@app.route("/")
def home():
    stats = db.get_stats()
    rows = ""

    for symbol, value in bot_stats["last_analysis"].items():
        decision = value.get("decision", "WAIT")
        css_class = (
            "buy"
            if decision == "BUY"
            else "sell"
            if decision == "SELL"
            else "wait"
        )

        rows += (
            "<tr>"
            f"<td>{symbol}</td>"
            f"<td class='{css_class}'>{decision}</td>"
            f"<td>{value.get('score', 0):.1f}</td>"
            f"<td>{value.get('regime', '')}</td>"
            f"<td>{value.get('tf_align', 0)}</td>"
            f"<td>{value.get('slot', '-')}</td>"
            f"<td>{value.get('time', '')[-8:]}</td>"
            "</tr>"
        )

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>APEX Bot</title>
        <style>
            body {{
                font-family: monospace;
                background: #0a0a0a;
                color: #00ff88;
                padding: 20px;
            }}
            h1 {{ color: #00ccff; }}
            table {{ border-collapse: collapse; width: 100%; }}
            td, th {{ border: 1px solid #333; padding: 8px; }}
            .buy {{ color: #00ff88; }}
            .sell {{ color: #ff4466; }}
            .wait {{ color: #888; }}
            .stat {{
                background: #111;
                padding: 10px;
                margin: 5px;
                display: inline-block;
                border-radius: 4px;
            }}
        </style>
    </head>
    <body>
        <h1>APEX v3.1 + AMF + 5 Slots</h1>
        <div>
            <span class="stat">
                Mode: <b>{bot_stats["mode"]}</b>
            </span>
            <span class="stat">
                Uptime: {int(time.time() - T0)}s
            </span>
            <span class="stat">
                Today: {bot_stats["trades_today"]}
            </span>
            <span class="stat">
                Open: {bot_stats["open_positions"]}
            </span>
            <span class="stat">
                WR: {stats["winrate"]:.1f}%
            </span>
        </div>

        <h2>Last Analysis</h2>
        <table>
            <tr>
                <th>Symbol</th>
                <th>Decision</th>
                <th>Score</th>
                <th>Regime</th>
                <th>TF Alignment</th>
                <th>Slot</th>
                <th>Time</th>
            </tr>
            {rows}
        </table>
    </body>
    </html>
    """


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
    app.run(
        host="127.0.0.1",
        port=CFG.flask_port,
        debug=False,
        use_reloader=False,
    )


exchange_public = ccxt.binance(
    {
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap",
            "adjustForTimeDifference": True,
        },
    }
)

exchange = ccxt.binance(
    {
        "apiKey": CFG.binance_api_key,
        "secret": CFG.binance_secret,
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap",
            "adjustForTimeDifference": True,
        },
    }
)


def get_balance():
    try:
        balance = exchange.fetch_balance(
            {"type": "future"}
        )
        return float(
            balance.get("USDT", {}).get("free", 0.0) or 0.0
        )
    except Exception as exc:
        logger.error("Balance error: %s", exc)
        return 0.0


def daily_pnl_pct():
    balance = get_balance()

    if balance <= 0:
        return 0.0

    return db.daily_pnl() / balance * 100.0


def get_pos(symbol):
    try:
        positions_data = exchange.fetch_positions([symbol])

        for position in positions_data:
            contracts = float(
                position.get("contracts", 0.0) or 0.0
            )

            if abs(contracts) > 0:
                return position

        return None

    except Exception as exc:
        logger.error("Position error %s: %s", symbol, exc)
        return "ERROR"


def live_open_position_count():
    try:
        positions_data = exchange.fetch_positions()

        return sum(
            1
            for position in positions_data
            if abs(
                float(position.get("contracts", 0.0) or 0.0)
            ) > 0
        )

    except Exception as exc:
        logger.error("Open positions error: %s", exc)
        return -1


# NEW: تحديد الفتحة والرافعة بدون زيادة نسبة المخاطرة.
def get_slot_configuration(final):
    current_positions = live_open_position_count()

    if current_positions < 0:
        return None

    slot = current_positions + 1

    if slot > 5:
        return None

    # الفتحتان الأولى والثانية بالشروط الأساسية.
    if slot <= 2:
        if (
            final.final_score < CFG.min_confidence
            or final.entry_quality < CFG.min_signal_score
        ):
            return None

        leverage_multiplier = 1.0
        slot_name = f"SLOT_{slot}_NORMAL"

    # الفتحة الثالثة: إشارة أقوى ورافعة أعلى.
    elif slot == 3:
        if (
            final.final_score < CFG.slot3_min_confidence
            or final.entry_quality < CFG.slot3_min_score
            or final.tf_alignment < 5
        ):
            return None

        leverage_multiplier = CFG.slot3_leverage_mult
        slot_name = "SLOT_3_STRONG"

    # الفتحة الرابعة: توافق أكبر.
    elif slot == 4:
        if (
            final.final_score < CFG.slot4_min_confidence
            or final.entry_quality < CFG.slot4_min_score
            or final.tf_alignment < 6
        ):
            return None

        leverage_multiplier = CFG.slot4_leverage_mult
        slot_name = "SLOT_4_VERY_STRONG"

    # الفتحة الخامسة: أعلى شروط، وليست دخولاً عادياً.
    else:
        if (
            final.final_score < CFG.slot5_min_confidence
            or final.entry_quality < CFG.slot5_min_score
            or final.tf_alignment < CFG.slot5_min_modules
        ):
            return None

        leverage_multiplier = CFG.slot5_leverage_mult
        slot_name = "SLOT_5_SNIPER"

    leverage = int(
        round(CFG.leverage * leverage_multiplier)
    )

    leverage = max(1, min(
        leverage,
        CFG.max_leverage_cap,
    ))

    return {
        "slot": slot,
        "name": slot_name,
        "leverage": leverage,
        # MODIFIED: المخاطرة لا تتضاعف مع الرافعة.
        "risk_multiplier": 1.0,
    }


class AIAnalyst:
    def analyze(self, symbol, apex_output):
        result = {
            "decision": "WAIT",
            "confidence": 0.0,
            "explanation": "",
            "risk_warnings": [],
            "error": False,
        }

        if not CFG.nvidia_api_key:
            result["error"] = True
            result["explanation"] = "NVIDIA_API_KEY is not configured"
            return result

        if not CFG.use_ai_veto and not CFG.use_ai_explainer:
            return result

        prompt = f"""
You are a conservative trading risk analyst.

Symbol: {symbol}
APEX decision: {apex_output.decision.value}
Regime: {apex_output.regime.value}
Composite score: {apex_output.composite_score:.2f}
Confidence: {apex_output.confidence:.2f}
Direction: {apex_output.direction.value}
RSI: {apex_output.rsi:.2f}
ADX: {apex_output.trend_strength:.2f}
Volatility: {apex_output.volatility_pct:.2f}
Bull modules: {apex_output.bull_modules}
Bear modules: {apex_output.bear_modules}

Return JSON only:
{{
  "decision": "BUY or SELL or WAIT",
  "confidence": 0,
  "explanation": "short explanation",
  "risk_warnings": []
}}
"""

        try:
            response = requests.post(
                "https://integrate.api.nvidia.com/v1/chat/completions",
                headers={
                    "Authorization": (
                        f"Bearer {CFG.nvidia_api_key}"
                    ),
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                json={
                    "model": CFG.ai_model,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "Return valid JSON only. "
                                "Be conservative."
                            ),
                        },
                        {
                            "role": "user",
                            "content": prompt,
                        },
                    ],
                    "max_tokens": 2048,
                    "temperature": 0.2,
                    "stream": False,
                },
                timeout=60,
            )

            response.raise_for_status()
            payload = response.json()
            raw = payload["choices"][0]["message"]["content"]
            raw = raw.strip()

            if raw.startswith("```"):
                raw = raw.replace("```json", "")
                raw = raw.replace("```", "")
                raw = raw.strip()

            start = raw.find("{")
            end = raw.rfind("}") + 1

            if start < 0 or end <= start:
                raise ValueError("AI did not return JSON")

            parsed = json.loads(raw[start:end])

            decision = str(
                parsed.get("decision", "WAIT")
            ).upper()

            if decision not in ("BUY", "SELL", "WAIT"):
                decision = "WAIT"

            result["decision"] = decision
            result["confidence"] = clamp(
                float(parsed.get("confidence", 0.0)),
                0.0,
                100.0,
            )
            result["explanation"] = str(
                parsed.get("explanation", "")
            )
            result["risk_warnings"] = parsed.get(
                "risk_warnings", []
            )

        except Exception as exc:
            result["error"] = True
            result["explanation"] = f"AI_ERROR: {str(exc)[:150]}"
            logger.warning("AI error %s: %s", symbol, exc)

        return result


ai_analyst = AIAnalyst()


class CandleManager:
    def __init__(self, maxlen=500):
        self._candles = {}
        self._forming = {}
        self._lock = threading.Lock()
        self._maxlen = maxlen

    def ensure(self, symbol_key, timeframes):
        with self._lock:
            if symbol_key not in self._candles:
                self._candles[symbol_key] = {
                    tf: deque(maxlen=self._maxlen)
                    for tf in timeframes
                }

                self._forming[symbol_key] = {
                    tf: None
                    for tf in timeframes
                }

    def update(self, symbol_key, timeframe, candle, closed):
        with self._lock:
            if (
                symbol_key not in self._candles
                or timeframe not in self._candles[symbol_key]
            ):
                return

            if closed:
                candles = self._candles[symbol_key][timeframe]

                if candles and candles[-1][0] == candle[0]:
                    candles[-1] = candle
                else:
                    candles.append(candle)

                self._forming[symbol_key][timeframe] = None
            else:
                self._forming[symbol_key][timeframe] = candle

    def get(self, symbol_key, timeframe):
        with self._lock:
            if (
                symbol_key not in self._candles
                or timeframe not in self._candles[symbol_key]
            ):
                return []

            return list(
                self._candles[symbol_key][timeframe]
            )

    def count(self, symbol_key, timeframe):
        with self._lock:
            return len(
                self._candles.get(
                    symbol_key,
                    {},
                ).get(timeframe, [])
            )

    def load(self, symbol_key, timeframe, data):
        with self._lock:
            if symbol_key not in self._candles:
                return

            if data and len(data) > 1:
                self._candles[symbol_key][timeframe] = deque(
                    data[:-1],
                    maxlen=self._maxlen,
                )
                self._forming[symbol_key][timeframe] = data[-1]
            else:
                self._candles[symbol_key][timeframe] = deque(
                    data or [],
                    maxlen=self._maxlen,
                )


cm = CandleManager(CFG.candle_maxlen)
trade_state = {}
execution_lock = threading.Lock()
active_symbols = {}
active_lock = threading.Lock()


class OpportunityPool:
    def __init__(self, max_size=5, ttl_seconds=300):
        self.pool = []
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.lock = threading.Lock()

    def add_or_update(self, symbol, final, apex_output):
        with self.lock:
            self.pool = [
                item
                for item in self.pool
                if item["symbol"] != symbol
            ]

            rr_score = clamp(
                safe_div(apex_output.rr_ratio, 4.0) * 100.0
            )

            volume_quality = (
                70.0
                if apex_output.volume_spike
                else 50.0
            )

            opportunity_score = (
                final.final_score * 0.45
                + apex_output.confidence * 0.25
                + rr_score * 0.15
                + volume_quality * 0.10
                + final.entry_quality * 0.05
            )

            self.pool.append(
                {
                    "symbol": symbol,
                    "final": final,
                    "apex": apex_output,
                    "opp_score": opportunity_score,
                    "timestamp": time.time(),
                }
            )

            self.pool.sort(
                key=lambda item: item["opp_score"],
                reverse=True,
            )

            self.pool = self.pool[:self.max_size]

    def get_best_opportunity(self):
        with self.lock:
            now = time.time()

            self.pool = [
                item
                for item in self.pool
                if now - item["timestamp"] < self.ttl_seconds
            ]

            if not self.pool:
                return None

            self.pool.sort(
                key=lambda item: item["opp_score"],
                reverse=True,
            )

            return self.pool[0]

    def remove(self, symbol):
        with self.lock:
            self.pool = [
                item
                for item in self.pool
                if item["symbol"] != symbol
            ]


opp_pool = OpportunityPool(
    max_size=5,
    ttl_seconds=300,
)


def emergency_close(symbol, reason):
    logger.critical(
        "EMERGENCY CLOSE %s: %s",
        symbol,
        reason,
    )

    try:
        position = get_pos(symbol)

        if position and position != "ERROR":
            contracts = abs(
                float(position.get("contracts", 0.0) or 0.0)
            )

            side = str(
                position.get("side", "")
            ).lower()

            if contracts > 0:
                close_side = (
                    "sell"
                    if side == "long"
                    else "buy"
                )

                exchange.create_market_order(
                    symbol,
                    close_side,
                    contracts,
                    params={"reduceOnly": True},
                )

        try:
            exchange.cancel_all_orders(symbol)
        except Exception:
            pass

    except Exception as exc:
        logger.critical(
            "Emergency close failed %s: %s",
            symbol,
            exc,
        )


def execute_trade(symbol, final):
    state = trade_state.setdefault(symbol, {})

    if state.get("executing", False):
        return False

    with execution_lock:
        try:
            state["executing"] = True

            if db.count_today() >= CFG.max_daily_trades:
                logger.warning(
                    "Daily trade limit reached"
                )
                return False

            if daily_pnl_pct() <= -CFG.max_daily_loss_pct:
                logger.warning(
                    "Daily loss limit reached"
                )
                return False

            if (
                db.consecutive_losses()
                >= CFG.max_consecutive_losses
            ):
                logger.warning(
                    "Consecutive loss limit reached"
                )
                return False

            current_position = get_pos(symbol)

            if current_position == "ERROR":
                return False

            if current_position:
                logger.info(
                    "Position already open: %s",
                    symbol,
                )
                return False

            live_count = live_open_position_count()

            if live_count < 0:
                return False

            if live_count >= CFG.max_open_positions:
                return False

            # MODIFIED: لا نلغي أوامر يدوية تلقائياً.
            try:
                pending_orders = exchange.fetch_open_orders(
                    symbol
                )

                if pending_orders:
                    logger.warning(
                        "Pending orders found for %s",
                        symbol,
                    )
                    return False

            except Exception:
                pass

            if (
                time.time() - state.get("last_trade_time", 0)
                < CFG.cooldown_seconds
            ):
                return False

            slot_config = get_slot_configuration(final)

            if not slot_config:
                logger.info(
                    "Slot requirements not met for %s",
                    symbol,
                )
                return False

            ticker = exchange_public.fetch_ticker(symbol)
            price = float(ticker["last"])

            side = (
                "buy"
                if final.decision == Decision.BUY
                else "sell"
            )

            balance = get_balance()

            if balance <= 0:
                logger.error(
                    "No available balance"
                )
                return False

            stop_price = (
                price * (1.0 - final.sl_percent / 100.0)
                if side == "buy"
                else price * (1.0 + final.sl_percent / 100.0)
            )

            # MODIFIED: الرافعة لا تضاعف حجم المخاطرة.
            risk_amount = (
                balance * CFG.risk_per_trade_pct / 100.0
            )

            price_distance = abs(price - stop_price)

            if price_distance <= 0:
                return False

            quantity = risk_amount / price_distance
            quantity = float(
                exchange.amount_to_precision(
                    symbol,
                    quantity,
                )
            )

            if quantity <= 0:
                return False

            leverage = slot_config["leverage"]

            logger.info(
                "Preparing %s | %s | slot=%s | leverage=%sx",
                symbol,
                slot_config["name"],
                slot_config["slot"],
                leverage,
            )

            if CFG.dry_run:
                take_profit = (
                    price * (1.0 + final.tp_percent / 100.0)
                    if side == "buy"
                    else price * (1.0 - final.tp_percent / 100.0)
                )

                state["last_trade_time"] = time.time()

                db.insert_trade(
                    symbol=symbol,
                    side="LONG" if side == "buy" else "SHORT",
                    mode="DRY_RUN",
                    entry_price=price,
                    quantity=quantity,
                    sl_price=stop_price,
                    tp_price=take_profit,
                    confidence=final.final_score,
                    entry_quality=final.entry_quality,
                    risk_score=final.risk_score,
                    regime=final.regime,
                    reason=(
                        f"{slot_config['name']} | "
                        f"APEX={final.apex_score:.1f}"
                    ),
                    timestamp=datetime.now(
                        timezone.utc
                    ).isoformat(),
                    status="OPEN",
                    ai_explanation=final.ai_explanation,
                    tf_alignment=final.tf_alignment,
                    final_score=final.final_score,
                )

                logger.info(
                    "DRY RUN trade registered: %s",
                    symbol,
                )
                return True

            try:
                exchange.set_leverage(
                    leverage,
                    symbol,
                )
            except Exception as exc:
                logger.warning(
                    "Could not set leverage %s: %s",
                    symbol,
                    exc,
                )

            order = exchange.create_market_order(
                symbol,
                side,
                quantity,
            )

            entry_order_id = order.get("id", "")
            position = None

            for _ in range(10):
                position = get_pos(symbol)

                if position and position != "ERROR":
                    break

                time.sleep(0.5)

            if not position or position == "ERROR":
                emergency_close(
                    symbol,
                    "position not confirmed",
                )
                return False

            entry_price = float(
                position.get("entryPrice", price)
                or price
            )

            actual_quantity = abs(
                float(
                    position.get("contracts", quantity)
                    or quantity
                )
            )

            if actual_quantity <= 0:
                emergency_close(
                    symbol,
                    "zero position quantity",
                )
                return False

            stop_price = (
                entry_price
                * (1.0 - final.sl_percent / 100.0)
                if side == "buy"
                else entry_price
                * (1.0 + final.sl_percent / 100.0)
            )

            take_profit = (
                entry_price
                * (1.0 + final.tp_percent / 100.0)
                if side == "buy"
                else entry_price
                * (1.0 - final.tp_percent / 100.0)
            )

            stop_price = float(
                exchange.price_to_precision(
                    symbol,
                    stop_price,
                )
            )

            take_profit = float(
                exchange.price_to_precision(
                    symbol,
                    take_profit,
                )
            )

            close_side = (
                "sell"
                if side == "buy"
                else "buy"
            )

            try:
                stop_order = exchange.create_order(
                    symbol,
                    "STOP_MARKET",
                    close_side,
                    actual_quantity,
                    None,
                    {
                        "stopPrice": stop_price,
                        "reduceOnly": True,
                        "workingType": "MARK_PRICE",
                    },
                )

                stop_order_id = stop_order.get("id", "")

            except Exception as exc:
                logger.critical(
                    "Stop-loss creation failed: %s",
                    exc,
                )
                emergency_close(
                    symbol,
                    "stop-loss creation failed",
                )
                return False

            try:
                take_profit_order = exchange.create_order(
                    symbol,
                    "TAKE_PROFIT_MARKET",
                    close_side,
                    actual_quantity,
                    None,
                    {
                        "stopPrice": take_profit,
                        "reduceOnly": True,
                        "workingType": "MARK_PRICE",
                    },
                )

                take_profit_order_id = (
                    take_profit_order.get("id", "")
                )

            except Exception as exc:
                logger.critical(
                    "Take-profit creation failed: %s",
                    exc,
                )

                try:
                    exchange.cancel_order(
                        stop_order_id,
                        symbol,
                    )
                except Exception:
                    pass

                emergency_close(
                    symbol,
                    "take-profit creation failed",
                )
                return False

            state["last_trade_time"] = time.time()

            trade_id = db.insert_trade(
                symbol=symbol,
                side="LONG" if side == "buy" else "SHORT",
                mode="LIVE",
                entry_price=entry_price,
                quantity=actual_quantity,
                sl_price=stop_price,
                tp_price=take_profit,
                sl_order_id=stop_order_id,
                tp_order_id=take_profit_order_id,
                entry_order_id=entry_order_id,
                confidence=final.final_score,
                entry_quality=final.entry_quality,
                risk_score=final.risk_score,
                regime=final.regime,
                reason=(
                    f"{slot_config['name']} | "
                    f"leverage={leverage}x"
                ),
                timestamp=datetime.now(
                    timezone.utc
                ).isoformat(),
                status="OPEN",
                ai_explanation=final.ai_explanation,
                tf_alignment=final.tf_alignment,
                final_score=final.final_score,
            )

            logger.info(
                "LIVE trade #%s opened on %s",
                trade_id,
                symbol,
            )

            return True

        except Exception as exc:
            logger.error(
                "Execution error %s: %s",
                symbol,
                exc,
                exc_info=True,
            )
            return False

        finally:
            state["executing"] = False


class PositionMonitor:
    def __init__(self, exchange_instance, database, config):
        self.exchange = exchange_instance
        self.db = database
        self.cfg = config
        self.trailing_peaks = {}
        self.running = True

    def start(self):
        threading.Thread(
            target=self._loop,
            daemon=True,
        ).start()

        logger.info(
            "Position monitor started"
        )

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            try:
                self.monitor_live_positions()
            except Exception as exc:
                logger.error(
                    "Monitor error: %s",
                    exc,
                )

            time.sleep(self.cfg.monitor_interval)

    def monitor_live_positions(self):
        if self.cfg.dry_run:
            return

        try:
            positions = self.exchange.fetch_positions()

        except Exception as exc:
            logger.error(
                "Fetch positions failed: %s",
                exc,
            )
            return

        active = [
            position
            for position in positions
            if abs(
                float(position.get("contracts", 0.0) or 0.0)
            ) > 0
        ]

        for position in active:
            symbol = position["symbol"]
            side = str(
                position.get("side", "")
            ).upper()

            quantity = abs(
                float(
                    position.get("contracts", 0.0)
                    or 0.0
                )
            )

            entry_price = float(
                position.get("entryPrice", 0.0)
                or 0.0
            )

            if quantity <= 0 or entry_price <= 0:
                continue

            try:
                current_price = float(
                    self.exchange.fetch_ticker(
                        symbol
                    )["last"]
                )
            except Exception:
                continue

            max_take_profit_price = (
                entry_price
                * (1.0 + self.cfg.max_tp_percent / 100.0)
                if side == "LONG"
                else entry_price
                * (1.0 - self.cfg.max_tp_percent / 100.0)
            )

            if side == "LONG":
                current_distance = current_price - entry_price
                target_distance = (
                    max_take_profit_price - entry_price
                )
            else:
                current_distance = entry_price - current_price
                target_distance = (
                    entry_price - max_take_profit_price
                )

            if target_distance <= 0:
                continue

            progress = (
                current_distance / target_distance
            ) * 100.0

            peak_key = f"{symbol}:{side}"
            previous_peak = self.trailing_peaks.get(
                peak_key,
                0.0,
            )

            self.trailing_peaks[peak_key] = max(
                previous_peak,
                progress,
            )

            peak = self.trailing_peaks[peak_key]

            if (
                self.cfg.trailing_enabled
                and peak >= self.cfg.trailing_activation
                and progress
                <= peak - self.cfg.trailing_drop
            ):
                self.close_position_direct(
                    symbol,
                    side,
                    quantity,
                    current_price,
                    "TRAILING_TAKE_PROFIT",
                )

    def close_position_direct(
        self,
        symbol,
        side,
        quantity,
        exit_price,
        reason,
    ):
        try:
            close_side = (
                "sell"
                if side == "LONG"
                else "buy"
            )

            self.exchange.create_market_order(
                symbol,
                close_side,
                quantity,
                params={"reduceOnly": True},
            )

            try:
                self.exchange.cancel_all_orders(symbol)
            except Exception:
                pass

            for trade in self.db.get_open_trades():
                if trade["symbol"] != symbol:
                    continue

                trade_side = str(
                    trade.get("side", "")
                ).upper()

                trade_quantity = float(
                    trade.get("quantity", quantity)
                    or quantity
                )

                entry = float(
                    trade.get("entry_price", exit_price)
                )

                if trade_side == "LONG":
                    pnl = (
                        exit_price - entry
                    ) * trade_quantity
                    pnl_percent = (
                        exit_price - entry
                    ) / entry * 100.0
                else:
                    pnl = (
                        entry - exit_price
                    ) * trade_quantity
                    pnl_percent = (
                        entry - exit_price
                    ) / entry * 100.0

                self.db.close_trade(
                    trade["id"],
                    exit_price,
                    pnl,
                    pnl_percent,
                    0.0,
                    reason,
                )

        except Exception as exc:
            logger.error(
                "Close position failed %s: %s",
                symbol,
                exc,
            )


class MarketScanner:
    def __init__(self):
        self.running = True

    def start(self):
        threading.Thread(
            target=self._loop,
            daemon=True,
        ).start()

        logger.info("Scanner started")

    def stop(self):
        self.running = False

    def _loop(self):
        time.sleep(5)

        while self.running:
            try:
                self._cycle()
            except Exception as exc:
                logger.error(
                    "Scanner cycle error: %s",
                    exc,
                    exc_info=True,
                )

            time.sleep(CFG.scanner_interval)

    def _cycle(self):
        candidates = []

        for symbol_key, symbol in CFG.watchlist.items():
            try:
                result = self._quick(
                    symbol_key,
                    symbol,
                )

                if result:
                    candidates.append(result)

            except Exception as exc:
                logger.debug(
                    "Quick scan failed %s: %s",
                    symbol,
                    exc,
                )

            time.sleep(0.25)

        candidates.sort(
            key=lambda item: item["score"],
            reverse=True,
        )

        selected = candidates[:CFG.scanner_top_n]

        bot_stats["scanner"] = [
            {
                "symbol": item["symbol"],
                "score": round(item["score"], 2),
            }
            for item in selected
        ]

        with active_lock:
            active_symbols.clear()

            for item in selected:
                active_symbols[item["symbol_key"]] = item["symbol"]
                cm.ensure(
                    item["symbol_key"],
                    CFG.timeframes,
                )

        for item in selected:
            position = get_pos(item["symbol"])

            if position == "ERROR" or position:
                continue

            threading.Thread(
                target=self._deep,
                args=(
                    item["symbol_key"],
                    item["symbol"],
                ),
                daemon=True,
            ).start()

            time.sleep(0.5)

    def _quick(self, symbol_key, symbol):
        ticker = exchange_public.fetch_ticker(symbol)

        quote_volume = float(
            ticker.get("quoteVolume", 0.0)
            or 0.0
        )

        if quote_volume < CFG.scanner_min_volume_usdt:
            return None

        candles = exchange_public.fetch_ohlcv(
            symbol,
            timeframe="1h",
            limit=60,
        )

        if len(candles) < 30:
            return None

        highs = [float(item[2]) for item in candles]
        lows = [float(item[3]) for item in candles]
        closes = [float(item[4]) for item in candles]

        atr_value = _atr(
            highs,
            lows,
            closes,
        )

        price = closes[-1]

        if price <= 0:
            return None

        atr_pct = atr_value / price * 100.0

        if atr_pct < CFG.scanner_min_atr_pct:
            return None

        net_move = abs(
            closes[-1] - closes[-20]
        )

        path = sum(
            abs(closes[i] - closes[i - 1])
            for i in range(
                max(1, len(closes) - 20),
                len(closes),
            )
        )

        efficiency = (
            safe_div(net_move, path)
            if path > 0
            else 0.0
        )

        score = (
            efficiency * 50.0
            + min(quote_volume / 50_000_000.0, 1.0)
            * 25.0
            + min(atr_pct / 1.5, 1.0)
            * 25.0
        )

        return {
            "symbol_key": symbol_key,
            "symbol": symbol,
            "score": score,
            "volume_usdt": quote_volume,
            "atr_pct": atr_pct,
        }

    def _deep(self, symbol_key, symbol):
        try:
            if cm.count(
                symbol_key,
                CFG.primary_tf,
            ) < 50:
                self._load(symbol_key, symbol)

            primary = cm.get(
                symbol_key,
                CFG.primary_tf,
            )

            trend = cm.get(
                symbol_key,
                CFG.trend_tf,
            )

            fast = cm.get(
                symbol_key,
                CFG.confirm_tf,
            )

            if len(primary) < 50:
                return

            apex = apex_engine.analyze(
                data_primary=primary,
                data_trend=trend
                if len(trend) >= 50
                else None,
                data_fast=fast
                if len(fast) >= 30
                else None,
                symbol=symbol,
                exchange_pub=exchange_public,
            )

            if not apex:
                return

            ai = ai_analyst.analyze(
                symbol,
                apex,
            )

            ext_decision = "HOLD"
            ext_confidence = 0.0

            if (
                CFG.use_external_strategies
                and EXTERNAL_AVAILABLE
            ):
                adapter_candles = [
                    {
                        "open": float(candle[1]),
                        "high": float(candle[2]),
                        "low": float(candle[3]),
                        "close": float(candle[4]),
                        "volume": float(candle[5]),
                    }
                    for candle in primary
                ]

                signals = [
                    call_strategy_by_name(
                        strategy_name,
                        adapter_candles,
                    )
                    for strategy_name
                    in CFG.external_strategies_list
                ]

                ext_decision, ext_confidence, _ = (
                    aggregate_signals(signals)
                )

                ext_decision = str(
                    ext_decision
                ).upper()

            final = FinalDecision()
            final.symbol = symbol
            final.sl_percent = apex.sl_percent
            final.tp_percent = apex.tp_percent
            final.regime = apex.regime.value
            final.apex_score = apex.confidence
            final.signal_score = apex.composite_score
            final.entry_quality = apex.composite_score
            final.ai_score = ai["confidence"]
            final.ai_explanation = ai["explanation"]

            if apex.direction == Direction.LONG:
                final.tf_alignment = apex.bull_modules
            elif apex.direction == Direction.SHORT:
                final.tf_alignment = apex.bear_modules
            else:
                final.tf_alignment = 0

            final.risk_score = clamp(
                100.0 - apex.confidence
            )

            external_veto = (
                CFG.use_external_strategies
                and EXTERNAL_AVAILABLE
                and ext_decision not in ("HOLD", "WAIT")
                and ext_confidence >= 50.0
                and apex.decision != Decision.WAIT
                and ext_decision != apex.decision.value
            )

            if apex.decision == Decision.WAIT:
                final.decision = Decision.WAIT
                final.final_score = apex.confidence
                final.reasons = [
                    "APEX WAIT"
                ] + apex.reasons

            elif external_veto:
                final.decision = Decision.WAIT
                final.final_score = apex.confidence
                final.reasons = [
                    "EXTERNAL VETO"
                ] + apex.reasons

            elif (
                CFG.use_ai_veto
                and ai["decision"] == "WAIT"
                and ai["confidence"]
                >= CFG.ai_min_veto_confidence
            ):
                final.decision = Decision.WAIT
                final.final_score = apex.confidence
                final.reasons = [
                    "AI VETO"
                ] + apex.reasons

            else:
                final.decision = apex.decision

                # MODIFIED: تطبيع الأوزان إذا كان AI أو external غير متاح.
                score_sum = apex.confidence * 0.75
                weight_sum = 0.75

                if (
                    CFG.use_ai_explainer
                    and not ai["error"]
                ):
                    score_sum += ai["confidence"] * 0.15
                    weight_sum += 0.15

                if (
                    CFG.use_external_strategies
                    and EXTERNAL_AVAILABLE
                ):
                    score_sum += ext_confidence * 0.10
                    weight_sum += 0.10

                final.final_score = safe_div(
                    score_sum,
                    weight_sum,
                    apex.confidence,
                )

                final.reasons = [
                    (
                        f"APEX={apex.decision.value} "
                        f"AI={ai['decision']} "
                        f"EXT={ext_decision}"
                    )
                ] + apex.reasons

            bot_stats["last_analysis"][symbol] = {
                "decision": final.decision.value,
                "score": round(final.final_score, 2),
                "entry_quality": round(
                    final.entry_quality,
                    2,
                ),
                "regime": final.regime,
                "tf_align": final.tf_alignment,
                "slot": "-",
                "time": datetime.now(
                    timezone.utc
                ).isoformat(),
            }

            if final.decision == Decision.WAIT:
                return

            if final.final_score < CFG.min_confidence:
                return

            opp_pool.add_or_update(
                symbol,
                final,
                apex,
            )

            best = opp_pool.get_best_opportunity()

            if not best:
                return

            slot_info = get_slot_configuration(
                best["final"]
            )

            if slot_info:
                bot_stats["last_analysis"][
                    best["symbol"]
                ]["slot"] = slot_info["slot"]

            if live_open_position_count() >= CFG.max_open_positions:
                return

            success = execute_trade(
                best["symbol"],
                best["final"],
            )

            if success:
                opp_pool.remove(best["symbol"])
            else:
                # لا نحاول تنفيذ المرشح الضعيف مرة أخرى في نفس الدورة.
                opp_pool.remove(best["symbol"])

        except Exception as exc:
            logger.error(
                "Deep analysis failed %s: %s",
                symbol,
                exc,
                exc_info=True,
            )

    def _load(self, symbol_key, symbol):
        for timeframe in CFG.timeframes:
            try:
                candles = exchange_public.fetch_ohlcv(
                    symbol,
                    timeframe=timeframe,
                    limit=300,
                )

                cm.load(
                    symbol_key,
                    timeframe,
                    candles,
                )

            except Exception as exc:
                logger.warning(
                    "Load failed %s %s: %s",
                    symbol,
                    timeframe,
                    exc,
                )

            time.sleep(0.25)


async def ws_worker():
    delay = CFG.ws_reconnect_delay

    while True:
        with active_lock:
            current_symbols = dict(active_symbols)

        if not current_symbols:
            await asyncio.sleep(10)
            continue

        streams = [
            f"{symbol_key}@kline_{timeframe}"
            for symbol_key in current_symbols
            for timeframe in CFG.timeframes
        ]

        url = (
            "wss://fstream.binance.com/stream?streams="
            + "/".join(streams)
        )

        try:
            async with websockets.connect(
                url,
                ping_interval=CFG.ws_ping_interval,
                ping_timeout=CFG.ws_ping_timeout,
            ) as websocket:
                logger.info(
                    "WebSocket connected: %s symbols",
                    len(current_symbols),
                )

                delay = CFG.ws_reconnect_delay

                async for message in websocket:
                    payload = json.loads(message)
                    kline = payload.get("data", {}).get("k")

                    if not kline:
                        continue

                    symbol_key = kline["s"].lower()
                    timeframe = kline["i"]

                    candle = [
                        kline["t"],
                        float(kline["o"]),
                        float(kline["h"]),
                        float(kline["l"]),
                        float(kline["c"]),
                        float(kline["v"]),
                    ]

                    cm.update(
                        symbol_key,
                        timeframe,
                        candle,
                        bool(kline["x"]),
                    )

        except Exception as exc:
            logger.error(
                "WebSocket error: %s",
                exc,
            )

        await asyncio.sleep(delay)
        delay = min(delay * 2, 120)


def main():
    logger.info(
        "APEX v3.1 AMF starting | mode=%s | max_positions=%s",
        "DRY_RUN" if CFG.dry_run else "LIVE",
        CFG.max_open_positions,
    )

    threading.Thread(
        target=run_server,
        daemon=True,
    ).start()

    time.sleep(2)

    try:
        ticker = exchange_public.fetch_ticker(
            "BTC/USDT:USDT"
        )

        logger.info(
            "Binance public API OK | BTC=%s",
            ticker.get("last"),
        )

    except Exception as exc:
        logger.critical(
            "Binance connection failed: %s",
            exc,
        )
        return

    logger.info("Loading historical candles...")

    for symbol_key, symbol in CFG.watchlist.items():
        cm.ensure(
            symbol_key,
            CFG.timeframes,
        )

        for timeframe in CFG.timeframes:
            try:
                candles = exchange_public.fetch_ohlcv(
                    symbol,
                    timeframe=timeframe,
                    limit=300,
                )

                cm.load(
                    symbol_key,
                    timeframe,
                    candles,
                )

            except Exception as exc:
                logger.warning(
                    "Historical load failed %s %s: %s",
                    symbol,
                    timeframe,
                    exc,
                )

            time.sleep(0.20)

    monitor = PositionMonitor(
        exchange,
        db,
        CFG,
    )
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
