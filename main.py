#!/usr/bin/env python3
"""
  MSSI BOT V1 — Market State & Signal Intelligence
  No RSI. No MACD. No EMA. No ADX. No Stochastic. No Bollinger.
  Pure market state inference from raw OHLCV.
"""

BUILD = "MSSI-V1-2026-07-26"

import asyncio, json, time, threading, math, os, sqlite3, logging
from collections import deque
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from enum import Enum
import websockets, ccxt, requests
from flask import Flask, jsonify
from openai import OpenAI

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(),
              logging.FileHandler(f"{LOG_DIR}/mssi_{datetime.now():%Y%m%d}.log", encoding="utf-8")])
logger = logging.getLogger("MSSI")


# ================================================================
#  CONFIG
# ================================================================
@dataclass
class Config:
    binance_api_key: str = "IX7kLH0ssWHP5TpYMUGcp0pzq4LX4Lqi7m4XtlqMkkq6DCZAsLhoeYZ3533jJFF4"
    binance_secret: str = "LmICnpSpMxL1riv4RfIf0HBGRfhDTP5JhDUYdlPSukpqV7kDTonrZ0j3DWp1a7hU"
    nvidia_api_key: str = "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"
    ai_model: str = "deepseek-ai/deepseek-v4-pro"
    dry_run: bool = False
    leverage: int = 10
    margin_usdt: float = 10.0
    max_daily_trades: int = 8
    max_open_positions: int = 2
    cooldown_seconds: int = 180
    max_sl_percent: float = 5.0
    max_tp_percent: float = 10.0
    # MSSI Decision Thresholds
    min_entry_quality: float = 55.0
    min_direction_bias: float = 25.0
    min_continuation_edge: float = 8.0
    max_risk: float = 70.0
    # Scanner
    scanner_interval: int = 300
    scanner_top_n: int = 5
    scanner_min_volume: float = 5_000_000
    scanner_min_atr_pct: float = 0.5
    # Monitor
    monitor_interval: int = 15
    # WS
    ws_ping_interval: int = 20
    ws_ping_timeout: int = 20
    ws_reconnect_delay: int = 10
    candle_maxlen: int = 500
    flask_port: int = 8080
    watchlist: Dict[str,str] = field(default_factory=lambda: {
        "btcusdt":"BTC/USDT:USDT","ethusdt":"ETH/USDT:USDT",
        "solusdt":"SOL/USDT:USDT","bnbusdt":"BNB/USDT:USDT",
        "xrpusdt":"XRP/USDT:USDT","adausdt":"ADA/USDT:USDT",
        "linkusdt":"LINK/USDT:USDT","avaxusdt":"AVAX/USDT:USDT",
        "dogeusdt":"DOGE/USDT:USDT","wifusdt":"WIF/USDT:USDT",
        "1000pepeusdt":"1000PEPE/USDT:USDT","suiusdt":"SUI/USDT:USDT",
        "aaveusdt":"AAVE/USDT:USDT","nearusdt":"NEAR/USDT:USDT",
        "arbusdt":"ARB/USDT:USDT","dotusdt":"DOT/USDT:USDT",
        "maticusdt":"MATIC/USDT:USDT","ltcusdt":"LTC/USDT:USDT",
        "aptusdt":"APT/USDT:USDT","opustdt":"OP/USDT:USDT",
    })
    timeframes: List[str] = field(default_factory=lambda: ["1m","1h","1d"])
    db_path: str = "trades.db"

CFG = Config()


# ================================================================
#  DATABASE
# ================================================================
class TradeDB:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        with self.lock:
            self.conn.execute("""CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, side TEXT,
                mode TEXT, entry_price REAL, quantity REAL, sl_price REAL,
                tp_price REAL, sl_order_id TEXT, tp_order_id TEXT,
                entry_order_id TEXT, confidence REAL, reason TEXT,
                timestamp TEXT, status TEXT DEFAULT 'OPEN', exit_price REAL,
                realized_pnl REAL, pnl_percent REAL, commission REAL DEFAULT 0,
                closed_at TEXT, close_reason TEXT, regime TEXT, mssi_scores TEXT)""")
            self.conn.commit()
    def insert_trade(self, **kw):
        with self.lock:
            cur = self.conn.execute(
                "INSERT INTO trades (symbol,side,mode,entry_price,quantity,sl_price,tp_price,"
                "sl_order_id,tp_order_id,entry_order_id,confidence,reason,timestamp,status,regime,mssi_scores)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (kw.get("symbol"),kw.get("side"),kw.get("mode"),kw.get("entry_price"),
                 kw.get("quantity"),kw.get("sl_price"),kw.get("tp_price"),
                 kw.get("sl_order_id",""),kw.get("tp_order_id",""),kw.get("entry_order_id",""),
                 kw.get("confidence",0),kw.get("reason",""),kw.get("timestamp",""),
                 kw.get("status","OPEN"),kw.get("regime",""),kw.get("mssi_scores","")))
            self.conn.commit(); return cur.lastrowid
    def close_trade(self, tid, ep, rpnl, pp, comm, reason):
        with self.lock:
            self.conn.execute("UPDATE trades SET status='CLOSED',exit_price=?,realized_pnl=?,"
                "pnl_percent=?,commission=?,closed_at=?,close_reason=? WHERE id=?",
                (ep,rpnl,pp,comm,datetime.now(timezone.utc).isoformat(),reason,tid))
            self.conn.commit()
    def get_open_trades(self):
        with self.lock:
            rows = self.conn.execute("SELECT * FROM trades WHERE status='OPEN'").fetchall()
            cols = [d[0] for d in self.conn.execute("SELECT * FROM trades LIMIT 0").description]
        return [dict(zip(cols,r)) for r in rows]
    def count_today(self):
        t = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self.lock:
            r = self.conn.execute("SELECT COUNT(*) FROM trades WHERE timestamp LIKE ? AND mode='LIVE'",(f"{t}%",)).fetchone()
        return r[0] if r else 0
    def open_count(self):
        with self.lock:
            r = self.conn.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN' AND mode='LIVE'").fetchone()
        return r[0] if r else 0

db = TradeDB(CFG.db_path)


# ================================================================
#  FLASK
# ================================================================
app = Flask(__name__)
bot_stats = {"status":"STARTING","version":BUILD,"uptime":0,"trades_today":0,
             "open_positions":0,"scanner":[],"last_analysis":{},
             "mode":"PAPER" if CFG.dry_run else "LIVE"}
T0 = time.time()

@app.route("/")
def home(): return f"MSSI BOT {BUILD} | {'PAPER' if CFG.dry_run else 'LIVE'}"
@app.route("/health")
def health():
    bot_stats["uptime"] = int(time.time()-T0)
    bot_stats["trades_today"] = db.count_today()
    bot_stats["open_positions"] = db.open_count()
    return jsonify(bot_stats)
def run_server(): app.run(host="0.0.0.0",port=CFG.flask_port,debug=False,use_reloader=False)


# ================================================================
#  EXCHANGE + AI
# ================================================================
exchange = ccxt.binance({"apiKey":CFG.binance_api_key,"secret":CFG.binance_secret,
    "enableRateLimit":True,"options":{"defaultType":"swap","adjustForTimeDifference":True}})
ai_client = OpenAI(base_url="https://integrate.api.nvidia.com/v1",api_key=CFG.nvidia_api_key)


# ================================================================
#  CANDLE MANAGER
# ================================================================
class CandleManager:
    def __init__(self, maxlen=500):
        self._c={}; self._f={}; self._lock=threading.Lock(); self._m=maxlen
    def ensure(self,sk,tfs):
        with self._lock:
            if sk not in self._c:
                self._c[sk]={tf:deque(maxlen=self._m) for tf in tfs}
                self._f[sk]={tf:None for tf in tfs}
    def update(self,sk,tf,candle,closed):
        with self._lock:
            if sk not in self._c or tf not in self._c[sk]: return
            if closed:
                dq=self._c[sk][tf]
                if dq and dq[-1][0]==candle[0]: dq[-1]=candle
                else: dq.append(candle)
                self._f[sk][tf]=None
            else: self._f[sk][tf]=candle
    def get(self,sk,tf):
        with self._lock:
            if sk not in self._c or tf not in self._c[sk]: return []
            return list(self._c[sk][tf])
    def count(self,sk,tf):
        with self._lock: return len(self._c.get(sk,{}).get(tf,[]))
    def load(self,sk,tf,data):
        with self._lock:
            if sk not in self._c: return
            if data and len(data)>1:
                self._c[sk][tf]=deque(data[:-1],maxlen=self._m); self._f[sk][tf]=data[-1]
            else: self._c[sk][tf]=deque(data,maxlen=self._m)

cm = CandleManager(CFG.candle_maxlen)
trade_state = {}; execution_lock = threading.Lock()
active_symbols = {}; active_lock = threading.Lock()


# ================================================================
#  MSSI ENGINE — Market State & Signal Intelligence
#  No RSI. No MACD. No EMA. No ADX. No Stochastic. No Bollinger.
# ================================================================

def _sigmoid(x): return 1.0/(1.0+math.exp(-max(-500,min(500,x))))
def _map_s(x): return 100.0*math.tanh(0.85*x)
def _map_u(x): return 100.0*_sigmoid(1.35*x-0.10)
def _clip(x,lo,hi): return max(lo,min(hi,x))
EPS = 1e-8

class RollingNorm:
    """Robust z-score: median + MAD"""
    def __init__(self, maxlen=500):
        self.buf = deque(maxlen=maxlen)
    def push(self, v):
        self.buf.append(v)
    def z(self, v):
        if len(self.buf) < 30: return 0.0
        s = sorted(self.buf)
        n = len(s); med = s[n//2]
        mad = sorted([abs(x-med) for x in s])[n//2]
        denom = 1.4826*mad + EPS
        return _clip((v-med)/denom, -3.5, 3.5)


class MSSIEngine:
    """
    Multi-Scale Market State & Signal Intelligence
    Windows: 16, 48, 144
    Weights: 0.20, 0.35, 0.45
    """
    WINDOWS = [16, 48, 144]
    WEIGHTS = [0.20, 0.35, 0.45]

    def __init__(self):
        self.norms: Dict[str, Dict[int, RollingNorm]] = {}
        self._init_norms()

    def _init_norms(self):
        keys = ["SDE","DE","IRI","SSI","FP","VP","ACC2","VER","EX2","DIV",
                "NOISE2","WS","CLV_mean","COMP","PB","ret3","abs_IRI","abs_SSI",
                "abs_FP","abs_VP","vol_ratio","VER_excess"]
        for k in keys:
            self.norms[k] = {w: RollingNorm(500) for w in self.WINDOWS}

    def _extract_raw(self, data):
        """Extract raw arrays from OHLCV"""
        n = len(data)
        o = [float(x[1]) for x in data]
        h = [float(x[2]) for x in data]
        l = [float(x[3]) for x in data]
        c = [float(x[4]) for x in data]
        v = [float(x[5]) for x in data]
        r = [0.0]*n
        tr = [0.0]*n
        clv = [0.0]*n
        ofi = [0.0]*n
        for i in range(1, n):
            r[i] = math.log(c[i]/c[i-1]) if c[i-1] > 0 else 0.0
            tr[i] = max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1]))
            rng = max(h[i]-l[i], EPS)
            clv[i] = (2*c[i]-h[i]-l[i]) / rng
            ofi[i] = clv[i] * math.log(1+v[i])
        tr[0] = h[0]-l[0] if n > 0 else 0
        return o, h, l, c, v, r, tr, clv, ofi

    def _calc_features(self, o, h, l, c, v, r, tr, clv, ofi, n_win):
        """Calculate all primitive features for a given window"""
        n = len(c)
        if n < n_win + 2:
            return None
        t = n - 1
        s = t - n_win + 1

        # Directional Efficiency
        path = sum(abs(c[i]-c[i-1]) for i in range(s+1, t+1)) + EPS
        disp = abs(c[t] - c[s])
        DE = disp / path

        # Signed Directional Efficiency
        SDE = (1.0 if c[t] >= c[s] else -1.0) * DE

        # Return-to-Range Impulse
        tr_sum = sum(tr[i] for i in range(s+1, t+1)) + EPS
        IRI = (c[t] - c[s]) / tr_sum

        # Volume Participation
        v_sum = sum(v[i] for i in range(s+1, t+1)) + EPS
        VP = sum(v[i]*(1.0 if r[i]>=0 else -1.0) for i in range(s+1, t+1)) / v_sum

        # Flow Proxy
        FP = sum(ofi[i] for i in range(s+1, t+1)) / n_win

        # Acceptance
        direction_sign = 1.0 if c[t] >= c[s] else -1.0
        ACC = sum(clv[i]*direction_sign for i in range(s+1, t+1)) / n_win
        ACC2 = ACC * DE

        # Volatility Expansion Ratio
        half = n_win // 2
        if t - 2*n_win + 1 >= 0:
            recent_tr = sum(tr[i] for i in range(t-n_win+1, t+1)) / n_win
            older_tr = sum(tr[i] for i in range(t-2*n_win+1, t-n_win+1)) / n_win + EPS
            VER = recent_tr / older_tr
        else:
            VER = 1.0

        # Structure Integrity
        hh = sum(1 for i in range(s+1, t+1) if h[i] > h[i-1])
        hl = sum(1 for i in range(s+1, t+1) if l[i] > l[i-1])
        lh = sum(1 for i in range(s+1, t+1) if h[i] < h[i-1])
        ll = sum(1 for i in range(s+1, t+1) if l[i] < l[i-1])
        cnt = max(n_win - 1, 1)
        SI_bull = 0.5*(hh + hl) / cnt
        SI_bear = 0.5*(lh + ll) / cnt
        SSI = SI_bull - SI_bear

        # Exhaustion
        half_s = t - half + 1
        if half_s > s:
            path_half = sum(abs(c[i]-c[i-1]) for i in range(half_s+1, t+1)) + EPS
            IRI_half = (c[t] - c[half_s]) / (sum(tr[i] for i in range(half_s+1, t+1)) + EPS)
            EXH = max(0, abs(IRI) - abs(IRI_half))
        else:
            EXH = 0
        DIV = max(0, DE - abs(VP))
        EX2 = 0.6*EXH + 0.4*DIV

        # Noise
        NOISE = 1.0 - DE
        sign_changes = sum(1 for i in range(s+2, t+1)
                          if (r[i] >= 0) != (r[i-1] >= 0))
        WS = sign_changes / max(n_win - 2, 1)
        NOISE2 = 0.55*NOISE + 0.45*WS

        # CLV mean
        CLV_mean = sum(clv[i] for i in range(s+1, t+1)) / n_win

        # Compression
        COMP = max(0, 1.0 - VER)

        # Pullback depth
        max_c = max(c[s:t+1])
        PB = (max_c - c[t]) / (tr_sum) if direction_sign > 0 else (c[t] - min(c[s:t+1])) / tr_sum

        # 3-bar return
        ret3 = abs(r[t] + r[t-1] + r[t-2]) if t >= 2 else abs(r[t])

        # Volume ratio short/long
        v_short = sum(v[i] for i in range(max(0,t-15), t+1)) / 16
        v_long = sum(v[i] for i in range(max(0,t-n_win+1), t+1)) / n_win
        vol_ratio = math.log(1 + v_short / (v_long + EPS))

        return {
            "DE":DE, "SDE":SDE, "IRI":IRI, "VP":VP, "FP":FP,
            "ACC2":ACC2, "VER":VER, "SSI":SSI, "EX2":EX2, "DIV":DIV,
            "NOISE2":NOISE2, "WS":WS, "CLV_mean":CLV_mean, "COMP":COMP,
            "PB":PB, "ret3":ret3, "abs_IRI":abs(IRI), "abs_SSI":abs(SSI),
            "abs_FP":abs(FP), "abs_VP":abs(VP), "vol_ratio":vol_ratio,
            "VER_excess":max(0, VER-1.25),
        }

    def analyze(self, data) -> Optional[dict]:
        """Full MSSI analysis on candle data. Returns 14 scores + regime + decision."""
        if len(data) < 150:
            return None

        o, h, l, c, v, r, tr, clv, ofi = self._extract_raw(data)

        # Calculate features at each scale
        feats = {}
        for w in self.WINDOWS:
            f = self._calc_features(o, h, l, c, v, r, tr, clv, ofi, w)
            if f is None:
                return None
            feats[w] = f

        # Push to rolling normalizers + get z-scores
        z = {}
        for w_idx, w in enumerate(self.WINDOWS):
            for key in feats[w]:
                self.norms[key][w].push(feats[w][key])
                z[(key, w)] = self.norms[key][w].z(feats[w][key])

        # Multi-scale blend
        def blend(key, signed=False):
            val = 0.0
            for wi, w in enumerate(self.WINDOWS):
                zv = z.get((key, w), 0.0)
                mapped = _map_s(zv) if signed else _map_u(zv)
                val += self.WEIGHTS[wi] * mapped
            return val

        # === 14 SCORES ===

        # 1. direction_bias [-100, +100]
        db_raw = (0.34*blend("SDE",True) + 0.26*blend("IRI",True) +
                  0.22*blend("SSI",True) + 0.18*blend("FP",True))
        direction_bias = _clip(db_raw, -100, 100)

        # 2. trend_strength [0, 100]
        trend_strength = _clip(
            0.40*blend("DE") + 0.25*blend("abs_IRI") +
            0.20*blend("abs_SSI") + 0.15*blend("VER"), 0, 100)

        # 3. momentum_score [0, 100]
        momentum_score = _clip(
            0.45*blend("ret3") + 0.35*blend("abs_IRI") +
            0.20*blend("abs_FP"), 0, 100)

        # 4. market_structure_score [0, 100]
        market_structure_score = _clip(
            0.55*blend("abs_SSI") + 0.25*blend("DE") +
            0.20*blend("ACC2"), 0, 100)

        # 5. acceptance_score [-100, +100]
        acceptance_score = _clip(
            0.60*blend("ACC2",True) + 0.25*blend("FP",True) +
            0.15*blend("SDE",True), -100, 100)

        # 6. participation_score [0, 100]
        participation_score = _clip(
            0.50*blend("abs_FP") + 0.30*blend("abs_VP") +
            0.20*blend("vol_ratio"), 0, 100)

        # 11. exhaustion_score [0, 100] (needed before continuation)
        exhaustion_score = _clip(
            0.45*blend("EX2") + 0.30*blend("DIV") +
            0.25*blend("VER_excess"), 0, 100)

        # 12. noise_score [0, 100]
        noise_score = _clip(
            0.60*blend("NOISE2") + 0.25*blend("WS") +
            0.15*blend("CLV_mean"), 0, 100)

        # 10. pullback_quality [0, 100]
        pb_z = blend("PB")
        pullback_quality = _clip(
            35*(blend("DE")/100) + 25*(blend("ACC2")/100) +
            20*(100 - abs(pb_z - 0.65)*100/3.5)/100 +
            20*((100 - exhaustion_score)/100), 0, 100) * 100
        pullback_quality = _clip(pullback_quality, 0, 100)

        # 13. risk_score [0, 100] (partial, needs reversal)
        # Calculate reversal first (circular dependency - use partial)
        RPx_partial = (-0.015*direction_bias - 0.020*trend_strength -
                       0.010*acceptance_score + 0.028*exhaustion_score +
                       0.022*noise_score)
        reversal_probability = 100*_sigmoid(RPx_partial - 1.10)

        risk_score = _clip(
            0.30*blend("VER") + 0.25*reversal_probability +
            0.20*noise_score + 0.15*exhaustion_score +
            0.10*(100 - pullback_quality), 0, 100)

        # Recalculate reversal with risk
        RPx = (-0.015*direction_bias - 0.020*trend_strength -
               0.010*acceptance_score + 0.028*exhaustion_score +
               0.022*noise_score + 0.018*risk_score)
        reversal_probability = 100*_sigmoid(RPx - 1.10)

        # 7. continuation_probability [0, 100]
        CPx = (0.018*direction_bias + 0.022*trend_strength +
               0.015*momentum_score + 0.014*market_structure_score +
               0.017*acceptance_score + 0.013*participation_score -
               0.020*exhaustion_score - 0.018*noise_score)
        continuation_probability = 100*_sigmoid(CPx - 1.85)

        # 8. (done above)

        # 9. breakout_probability [0, 100]
        BOx = (0.024*momentum_score + 0.020*participation_score +
               0.016*trend_strength + 0.018*abs(acceptance_score) +
               0.022*blend("COMP") - 0.015*noise_score)
        breakout_probability = 100*_sigmoid(BOx - 2.20)

        # 14. entry_quality [0, 100]
        entry_quality = _clip(
            0.24*trend_strength + 0.16*momentum_score +
            0.15*market_structure_score + 0.14*participation_score +
            0.14*continuation_probability + 0.09*pullback_quality -
            0.08*reversal_probability - 0.14*risk_score, 0, 100)

        # === REGIME CLASSIFICATION ===
        regime = "UNKNOWN"
        ver_blend = sum(self.WEIGHTS[i]*feats[w]["VER"] for i,w in enumerate(self.WINDOWS))

        if reversal_probability >= 64 and exhaustion_score >= 58 and abs(direction_bias) >= 35:
            regime = "REVERSAL"
        elif exhaustion_score >= 66 and trend_strength >= 50:
            regime = "EXHAUSTION"
        elif breakout_probability >= 68 and participation_score >= 55 and momentum_score >= 57:
            regime = "BREAKOUT"
        elif trend_strength >= 62 and noise_score <= 42 and market_structure_score >= 58:
            regime = "TRENDING"
        elif trend_strength < 42 and noise_score >= 52 and abs(direction_bias) < 18:
            regime = "RANGING"
        elif (abs(direction_bias) < 22 and trend_strength < 45 and
              participation_score >= 58 and acceptance_score > 12 and ver_blend < 0.92):
            regime = "ACCUMULATION"
        elif (abs(direction_bias) < 22 and trend_strength < 45 and
              participation_score >= 58 and acceptance_score < -12 and ver_blend < 0.92):
            regime = "DISTRIBUTION"
        elif ver_blend >= 1.28:
            regime = "HIGH_VOLATILITY"
        elif ver_blend <= 0.82:
            regime = "LOW_VOLATILITY"

        # === DECISION ===
        decision = "WAIT"
        confidence = 0.0

        cont_edge = continuation_probability - reversal_probability

        if (direction_bias > CFG.min_direction_bias and
            entry_quality >= CFG.min_entry_quality and
            cont_edge >= CFG.min_continuation_edge and
            risk_score <= CFG.max_risk):
            decision = "LONG"
            confidence = entry_quality * 0.4 + continuation_probability * 0.3 + \
                         trend_strength * 0.2 + participation_score * 0.1

        elif (direction_bias < -CFG.min_direction_bias and
              entry_quality >= CFG.min_entry_quality and
              cont_edge >= CFG.min_continuation_edge and
              risk_score <= CFG.max_risk):
            decision = "SHORT"
            confidence = entry_quality * 0.4 + continuation_probability * 0.3 + \
                         trend_strength * 0.2 + participation_score * 0.1

        confidence = _clip(confidence, 0, 100)

        # SL/TP from volatility
        atr_proxy = sum(self.WEIGHTS[i]*feats[w]["VER"] for i,w in enumerate(self.WINDOWS))
        base_sl = _clip(1.0 + atr_proxy * 1.5, 0.5, CFG.max_sl_percent)
        base_tp = _clip(2.0 + atr_proxy * 3.0, 1.0, CFG.max_tp_percent)

        return {
            "direction_bias": round(direction_bias, 2),
            "trend_strength": round(trend_strength, 2),
            "momentum_score": round(momentum_score, 2),
            "market_structure_score": round(market_structure_score, 2),
            "acceptance_score": round(acceptance_score, 2),
            "participation_score": round(participation_score, 2),
            "continuation_probability": round(continuation_probability, 2),
            "reversal_probability": round(reversal_probability, 2),
            "breakout_probability": round(breakout_probability, 2),
            "pullback_quality": round(pullback_quality, 2),
            "exhaustion_score": round(exhaustion_score, 2),
            "noise_score": round(noise_score, 2),
            "risk_score": round(risk_score, 2),
            "entry_quality": round(entry_quality, 2),
            "regime": regime,
            "decision": decision,
            "confidence": round(confidence, 2),
            "sl_percent": round(base_sl, 2),
            "tp_percent": round(base_tp, 2),
            "cont_edge": round(cont_edge, 2),
        }


mssi = MSSIEngine()


# ================================================================
#  AI ANALYST (explains only)
# ================================================================
def ai_explain(symbol, m):
    if m["decision"] == "WAIT": return ""
    prompt = f"""اشرح فقط. العملة:{symbol} القرار:{m['decision']} Regime:{m['regime']}
direction_bias:{m['direction_bias']} trend:{m['trend_strength']} momentum:{m['momentum_score']}
entry_quality:{m['entry_quality']} continuation:{m['continuation_probability']} reversal:{m['reversal_probability']}
exhaustion:{m['exhaustion_score']} noise:{m['noise_score']} risk:{m['risk_score']}
أجب JSON: {{"explanation":"شرح بالعربية","risk_warnings":["تحذير"],"agrees":true}}"""
    try:
        comp = ai_client.chat.completions.create(model=CFG.ai_model,
            messages=[{"role":"user","content":prompt}], temperature=0, max_tokens=300, stream=False)
        raw = comp.choices[0].message.content or ""
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            lines = [l for l in cleaned.split("\n") if not l.strip().startswith("```")]
            cleaned = "\n".join(lines).strip()
        dj = json.loads(cleaned)
        if not dj.get("agrees", True):
            return "AI_DISAGREES"
        return dj.get("explanation", "")
    except: return ""


# ================================================================
#  SCANNER
# ================================================================
class Scanner:
    def __init__(self): self._run = True
    def start(self): threading.Thread(target=self._loop, daemon=True).start(); logger.info("Scanner MSSI")
    def stop(self): self._run = False

    def _loop(self):
        time.sleep(5)
        while self._run:
            try: self._cycle()
            except Exception as e: logger.error(f"Scanner: {e}", exc_info=True)
            time.sleep(CFG.scanner_interval)

    def _cycle(self):
        logger.info("="*60)
        logger.info("MSSI Scanning...")
        candidates = []
        for sk, sym in CFG.watchlist.items():
            try:
                r = self._quick(sk, sym)
                if r: candidates.append(r)
            except: pass
            time.sleep(0.3)
        candidates.sort(key=lambda x: x["score"], reverse=True)
        top = candidates[:CFG.scanner_top_n]
        logger.info(f"Scanned {len(CFG.watchlist)} -> {len(candidates)} -> Top {len(top)}")
        for i, c in enumerate(top):
            logger.info(f"  #{i+1} {c['sym']} | Score={c['score']:.1f} | Vol={c['vol']/1e6:.1f}M | ATR={c['atr']:.2f}%")
        bot_stats["scanner"] = [{"symbol":c["sym"],"score":c["score"]} for c in top]
        with active_lock:
            active_symbols.clear()
            for c in top:
                active_symbols[c["sk"]] = c["sym"]
                cm.ensure(c["sk"], CFG.timeframes)
        for c in top:
            pos = get_pos(c["sym"])
            if pos == "ERROR" or pos: continue
            threading.Thread(target=self._deep, args=(c["sk"],c["sym"]), daemon=True).start()
            time.sleep(1)

    def _quick(self, sk, sym):
        try:
            ticker = exchange.fetch_ticker(sym)
            vol = float(ticker.get("quoteVolume",0) or 0)
            if vol < CFG.scanner_min_volume: return None
        except: return None
        try:
            ohlcv = exchange.fetch_ohlcv(sym, "1h", limit=50)
            if len(ohlcv) < 20: return None
            h = [float(x[2]) for x in ohlcv]; l = [float(x[3]) for x in ohlcv]
            c = [float(x[4]) for x in ohlcv]
            trs = [max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])) for i in range(1,len(c))]
            atr = sum(trs[-14:])/14 if len(trs)>=14 else 0
            price = c[-1]
            atr_pct = (atr/price)*100 if price else 0
            if atr_pct < CFG.scanner_min_atr_pct: return None
        except: return None
        score = 0
        if vol > 50_000_000: score += 2
        elif vol > 10_000_000: score += 1
        if atr_pct > 1.5: score += 2
        elif atr_pct > 0.8: score += 1
        if len(c) >= 2:
            chg = abs((c[-1]-c[-2])/c[-2])*100
            if chg > 1.0: score += 2
            elif chg > 0.3: score += 1
        # Directional efficiency quick check
        if len(c) >= 20:
            path = sum(abs(c[i]-c[i-1]) for i in range(len(c)-20, len(c))) + EPS
            disp = abs(c[-1]-c[-20])
            de = disp/path
            if de > 0.4: score += 2
            elif de > 0.2: score += 1
        return {"sk":sk, "sym":sym, "score":score, "vol":vol, "atr":atr_pct}

    def _deep(self, sk, sym):
        logger.info(f"MSSI Deep: {sym}")
        if cm.count(sk, "1h") < 150:
            self._load(sk, sym)
        d1h = cm.get(sk, "1h")
        d1d = cm.get(sk, "1d")
        if len(d1h) < 150:
            logger.info(f"Not enough 1H: {sym} ({len(d1h)})"); return

        # MSSI on 1H (primary)
        m1h = mssi.analyze(d1h)
        if not m1h:
            logger.info(f"MSSI fail 1H: {sym}"); return

        # MSSI on 1D (context)
        m1d = mssi.analyze(d1d) if len(d1d) >= 150 else None

        # Blend 1H (70%) + 1D (30%) for final decision
        if m1d:
            final = {}
            for key in m1h:
                if isinstance(m1h[key], (int, float)):
                    final[key] = m1h[key]*0.70 + m1d[key]*0.30
                else:
                    final[key] = m1h[key]
            final["regime"] = m1h["regime"]
            # Re-evaluate decision with blended scores
            cont_edge = final["continuation_probability"] - final["reversal_probability"]
            if (final["direction_bias"] > CFG.min_direction_bias and
                final["entry_quality"] >= CFG.min_entry_quality and
                cont_edge >= CFG.min_continuation_edge and
                final["risk_score"] <= CFG.max_risk):
                final["decision"] = "LONG"
            elif (final["direction_bias"] < -CFG.min_direction_bias and
                  final["entry_quality"] >= CFG.min_entry_quality and
                  cont_edge >= CFG.min_continuation_edge and
                  final["risk_score"] <= CFG.max_risk):
                final["decision"] = "SHORT"
            else:
                final["decision"] = "WAIT"
            final["cont_edge"] = round(cont_edge, 2)
        else:
            final = m1h

        logger.info(
            f">>> {sym} | Regime={final['regime']} | "
            f"Dir={final['direction_bias']:.1f} | Trend={final['trend_strength']:.1f} | "
            f"Mom={final['momentum_score']:.1f} | Entry={final['entry_quality']:.1f} | "
            f"Cont={final['continuation_probability']:.1f} | Rev={final['reversal_probability']:.1f} | "
            f"Exh={final['exhaustion_score']:.1f} | Noise={final['noise_score']:.1f} | "
            f"Risk={final['risk_score']:.1f}"
        )
        logger.info(
            f"<<< {sym}: {final['decision']} | Conf={final.get('confidence',0):.1f} | "
            f"Edge={final.get('cont_edge',0):.1f} | "
            f"Accept={final['acceptance_score']:.1f} | Part={final['participation_score']:.1f} | "
            f"PB={final['pullback_quality']:.1f} | BO={final['breakout_probability']:.1f}"
        )

        bot_stats["last_analysis"][sym] = {
            "decision": final["decision"], "regime": final["regime"],
            "dir": final["direction_bias"], "entry": final["entry_quality"],
            "time": datetime.now(timezone.utc).isoformat()
        }

        if final["decision"] == "WAIT": return

        # AI explain
        explanation = ai_explain(sym, final)
        if explanation == "AI_DISAGREES":
            logger.warning(f"AI disagrees: {sym}"); return

        # Execute
        execute(sym, final, explanation)

    def _load(self, sk, sym):
        for tf in CFG.timeframes:
            try:
                limit = 500 if tf == "1d" else 300
                data = exchange.fetch_ohlcv(sym, timeframe=tf, limit=limit)
                cm.load(sk, tf, data)
            except Exception as e: logger.warning(f"Load {sym} {tf}: {e}")
            time.sleep(0.3)


# ================================================================
#  EXECUTION
# ================================================================
def get_pos(sym):
    try:
        for p in exchange.fetch_positions([sym]):
            ct = p.get("contracts")
            if ct and float(ct) > 0: return p
        return None
    except Exception as e:
        logger.error(f"Pos {sym}: {e}"); return "ERROR"

def emergency_close(sym, reason):
    logger.critical(f"EMERGENCY: {sym} | {reason}")
    try:
        pos = get_pos(sym)
        if pos and pos != "ERROR":
            ct = float(pos.get("contracts",0)); side = pos.get("side","")
            if ct > 0:
                cs = "sell" if side == "long" else "buy"
                exchange.create_market_order(sym, cs, ct, params={"reduceOnly":True})
    except Exception as e: logger.critical(f"Emergency fail: {e}")

def execute(sym, m, explanation):
    with execution_lock:
        try:
            pos = get_pos(sym)
            if pos == "ERROR": return
            if pos: logger.info(f"Busy: {sym}"); return
            if db.count_today() >= CFG.max_daily_trades: logger.info("Daily limit"); return
            if db.open_count() >= CFG.max_open_positions: logger.info("Positions full"); return
            st = trade_state.get(sym, {})
            if time.time() - st.get("t",0) < CFG.cooldown_seconds: logger.info(f"Cooldown: {sym}"); return

            ticker = exchange.fetch_ticker(sym); price = ticker["last"]
            raw_qty = (CFG.margin_usdt * CFG.leverage) / price
            qty = float(exchange.amount_to_precision(sym, raw_qty))
            side = "buy" if m["decision"] == "LONG" else "sell"
            pname = "LONG" if side == "buy" else "SHORT"
            mode = "PAPER" if CFG.dry_run else "LIVE"
            scores_json = json.dumps({k:v for k,v in m.items() if isinstance(v,(int,float))})

            logger.info(f"TRADE {sym} | {pname} | {price} | {mode} | Regime={m['regime']} | "
                        f"Entry={m['entry_quality']:.1f} | Conf={m.get('confidence',0):.1f}")

            if CFG.dry_run:
                db.insert_trade(symbol=sym, side=pname, mode="PAPER", entry_price=price,
                    quantity=qty, confidence=m.get("confidence",0),
                    reason=f"MSSI {m['regime']} | Dir={m['direction_bias']:.1f} | EQ={m['entry_quality']:.1f}",
                    timestamp=datetime.now(timezone.utc).isoformat(), regime=m["regime"],
                    mssi_scores=scores_json)
                st["t"] = time.time(); return

            # LIVE
            exchange.set_leverage(CFG.leverage, sym)
            order = exchange.create_market_order(sym, side, qty)
            eoid = order.get("id",""); time.sleep(1)
            p = get_pos(sym)
            if p == "ERROR" or p is None: logger.critical(f"No pos: {sym}"); return
            entry = float(p.get("entryPrice", price))
            aqty = abs(float(p.get("contracts",0)))
            if aqty <= 0: emergency_close(sym,"zero qty"); return

            sl_p = _clip(m.get("sl_percent",2.0), 0.5, CFG.max_sl_percent)
            tp_p = _clip(m.get("tp_percent",4.0), 1.0, CFG.max_tp_percent)
            if side == "buy": sl_price = entry*(1-sl_p/100); tp_price = entry*(1+tp_p/100)
            else: sl_price = entry*(1+sl_p/100); tp_price = entry*(1-tp_p/100)
            sl_price = float(exchange.price_to_precision(sym, sl_price))
            tp_price = float(exchange.price_to_precision(sym, tp_price))
            cs = "sell" if side == "buy" else "buy"

            sloid = ""
            try:
                slo = exchange.create_order(sym,"STOP_MARKET",cs,aqty,None,
                    {"stopPrice":sl_price,"reduceOnly":True,"workingType":"MARK_PRICE"})
                sloid = slo.get("id",""); logger.info(f"SL: {sl_price}")
            except Exception as e:
                logger.critical(f"SL fail: {e}"); emergency_close(sym,"SL fail"); return

            tpoid = ""
            try:
                tpo = exchange.create_order(sym,"TAKE_PROFIT_MARKET",cs,aqty,None,
                    {"stopPrice":tp_price,"reduceOnly":True,"workingType":"MARK_PRICE"})
                tpoid = tpo.get("id",""); logger.info(f"TP: {tp_price}")
            except Exception as e:
                logger.error(f"TP fail: {e}")
                try: exchange.cancel_order(sloid, sym)
                except: pass
                emergency_close(sym,"TP fail"); return

            tid = db.insert_trade(symbol=sym, side=pname, mode="LIVE", entry_price=entry,
                quantity=aqty, sl_price=sl_price, tp_price=tp_price,
                sl_order_id=sloid, tp_order_id=tpoid, entry_order_id=eoid,
                confidence=m.get("confidence",0),
                reason=f"MSSI {m['regime']} | Dir={m['direction_bias']:.1f} | EQ={m['entry_quality']:.1f}",
                timestamp=datetime.now(timezone.utc).isoformat(), regime=m["regime"],
                mssi_scores=scores_json)
            logger.info(f"Trade #{tid}"); st["t"] = time.time()
        except Exception as e:
            logger.error(f"Exec: {e}", exc_info=True); emergency_close(sym, str(e))


# ================================================================
#  POSITION MONITOR
# ================================================================
class Monitor:
    def __init__(self): self._run = True
    def start(self): threading.Thread(target=self._loop, daemon=True).start(); logger.info("Monitor MSSI")
    def stop(self): self._run = False
    def _loop(self):
        while self._run:
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
        pos = get_pos(sym)
        if pos == "ERROR": return
        if pos is None:
            reason = "STOP_LOSS" if sl_st == "closed" else "TAKE_PROFIT" if tp_st == "closed" else "MANUAL"
            ep, rpnl, comm = self._rexit(sym, trade)
            entry = trade["entry_price"]; qty = trade["quantity"]
            if ep == 0: ep = entry
            if rpnl == 0:
                cs = self._csz(sym); rq = qty*cs
                rpnl = (ep-entry)*rq if trade["side"]=="LONG" else (entry-ep)*rq
            notional = entry*qty if entry*qty else 1
            pp = (rpnl/notional)*100
            db.close_trade(trade["id"], ep, rpnl, pp, comm, reason)
            logger.info(f"CLOSED {sym} | {reason} | PnL={rpnl:.4f} ({pp:.2f}%)")
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
                if t.get("reduceOnly") or (t.get("side")=="sell" and trade["side"]=="LONG") or (t.get("side")=="buy" and trade["side"]=="SHORT"):
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


# ================================================================
#  WEBSOCKET
# ================================================================
async def ws_worker():
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
                logger.info(f"WS connected ({len(current)})"); delay = CFG.ws_reconnect_delay
                async for msg in ws:
                    data = json.loads(msg); k = data.get("data",{}).get("k")
                    if not k: continue
                    sk = k["s"].lower(); tf = k["i"]
                    candle = [k["t"],float(k["o"]),float(k["h"]),float(k["l"]),float(k["c"]),float(k["v"])]
                    cm.update(sk, tf, candle, k["x"])
        except Exception as e: logger.error(f"WS: {e}")
        await asyncio.sleep(delay); delay = min(delay*2, 120)


# ================================================================
#  MAIN
# ================================================================
def main():
    logger.critical(f"BUILD: {BUILD} | dry_run={CFG.dry_run}")
    logger.critical(f"MSSI Windows: {MSSIEngine.WINDOWS} | Weights: {MSSIEngine.WEIGHTS}")
    logger.info("="*60)
    logger.info(f"MSSI BOT {BUILD}")
    logger.info(f"   Mode: {'PAPER' if CFG.dry_run else 'LIVE'}")
    logger.info(f"   Watchlist: {len(CFG.watchlist)} | Scanner: {CFG.scanner_interval}s -> Top {CFG.scanner_top_n}")
    logger.info(f"   MinEntryQuality: {CFG.min_entry_quality} | MinDirBias: {CFG.min_direction_bias}")
    logger.info(f"   MinContEdge: {CFG.min_continuation_edge} | MaxRisk: {CFG.max_risk}")
    logger.info(f"   MaxOpen: {CFG.max_open_positions} | MaxDaily: {CFG.max_daily_trades}")
    logger.info("="*60)

    threading.Thread(target=run_server, daemon=True).start(); time.sleep(2)

    try:
        t = exchange.fetch_ticker("BTC/USDT:USDT")
        logger.info(f"Binance OK | BTC: {t['last']}")
    except Exception as e: logger.critical(f"Binance: {e}"); return

    logger.info("Loading data...")
    for sk, sym in CFG.watchlist.items():
        cm.ensure(sk, CFG.timeframes)
        for tf in CFG.timeframes:
            try:
                limit = 500 if tf == "1d" else 300
                data = exchange.fetch_ohlcv(sym, timeframe=tf, limit=limit)
                cm.load(sk, tf, data)
            except: pass
            time.sleep(0.2)
    logger.info("Data ready")

    monitor = Monitor(); monitor.start()
    scanner = Scanner(); scanner.start()
    bot_stats["status"] = "RUNNING"

    try: asyncio.run(ws_worker())
    except KeyboardInterrupt: logger.info("Shutdown"); scanner.stop(); monitor.stop()


if __name__ == "__main__":
    main()
