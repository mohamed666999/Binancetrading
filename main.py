#!/usr/bin/env python3
"""
  MSSI TRADING BOT — Market State & Signal Intelligence
  MSSI = 85% decision | AI = 15% veto/filter

⚠️ تنبيه: المفاتيح في الكود الأصلي كانت مكشوفة.
   لا تستخدم هذا الملف مباشرة في البيئة الحقيقية دون استبدال المفاتيح بـ os.getenv().
   أوصي بإنشاء ملف .env وقراءة المفاتيح منه.
"""

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
              logging.FileHandler(f"{LOG_DIR}/bot_{datetime.now():%Y%m%d}.log", encoding="utf-8")])
logger = logging.getLogger("BOT")


@dataclass
class Config:
    # ⚠️ لا تضع المفاتيح هنا في الاستخدام الحقيقي — استخدم os.getenv()
    binance_api_key: str = "IX7kLH0ssWHP5TpYMUGcp0pzq4LX4Lqi7m4XtlqMkkq6DCZAsLhoeYZ3533jJFF4"
    binance_secret: str = "LmICnpSpMxL1riv4RfIf0HBGRfhDTP5JhDUYdlPSukpqV7kDTonrZ0j3DWp1a7hU"
    nvidia_api_key: str = "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"
    ai_model: str = "nvidia/llama-3.3-nemotron-super-49b-v1.5"
    dry_run: bool = False
    leverage: int = 10
    margin_usdt: float = 10.0
    max_daily_trades: int = 8
    max_open_positions: int = 1  # ✅ خُفّض إلى 1 لزيادة الفرص
    cooldown_seconds: int = 180
    max_sl_percent: float = 5.0
    max_tp_percent: float = 10.0
    mssi_weight: float = 0.85
    ai_weight: float = 0.15
    min_long_score: float = 58.0
    min_short_score: float = 42.0
    max_risk_for_entry: float = 82.0  # ✅ رُفع إلى 82
    min_entry_quality: float = 28.0   # ✅ خُفّض إلى 28
    min_confidence: float = 32.0     # ✅ خُفّض إلى 32
    use_ai_veto: bool = False
    use_ai_explainer: bool = True
    scanner_interval: int = 60       # ✅ خُفّض إلى 60 ثانية
    scanner_top_n: int = 10          # ✅ رُفع إلى 10 عملات
    scanner_min_volume_usdt: float = 5_000_000
    scanner_min_atr_pct: float = 0.5
    monitor_interval: int = 15
    ws_ping_interval: int = 20
    ws_ping_timeout: int = 20
    ws_reconnect_delay: int = 10
    candle_maxlen: int = 500
    flask_port: int = 8080
    watchlist: Dict[str, str] = field(default_factory=lambda: {
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


class Decision(Enum):
    BUY="BUY"; SELL="SELL"; WAIT="WAIT"
class TrendDirection(Enum):
    BULLISH="BULLISH"; BEARISH="BEARISH"; NEUTRAL="NEUTRAL"
class EntryQuality(Enum):
    ALLOWED="ALLOWED"; CAUTION="CAUTION"; BLOCKED="BLOCKED"
class MarketRegime(Enum):
    TRENDING="TRENDING"; RANGING="RANGING"; BREAKOUT="BREAKOUT"
    ACCUMULATION="ACCUMULATION"; DISTRIBUTION="DISTRIBUTION"
    HIGH_VOLATILITY="HIGH_VOLATILITY"; LOW_VOLATILITY="LOW_VOLATILITY"
    EXHAUSTION="EXHAUSTION"; REVERSAL="REVERSAL"

@dataclass
class ScannerResult:
    symbol_key:str=""; symbol:str=""; score:float=0.0; volume_usdt:float=0.0
    atr_pct:float=0.0; trend_1h:str=""; rsi_1h:float=50.0
    reasons:List[str]=field(default_factory=list)

@dataclass
class RawFeatures:
    ret_1:float=0; ret_5:float=0; ret_20:float=0
    true_range:float=0; atr_ratio:float=0; realized_vol:float=0
    efficiency:float=0; range_expansion:float=0; compression:float=0
    volume_zscore:float=0; participation:float=0
    buy_sell_imbalance:float=0; delta_pressure:float=0
    oi_change:float=0; oi_price_agreement:float=0; funding_stress:float=0
    wick_rejection:float=0; close_location:float=0
    breakout_pressure:float=0; reversal_pressure:float=0; noise:float=0
    persistence:float=0; price:float=0

@dataclass
class MSSIOutput:
    direction_bias:float=0; trend_strength:float=0; momentum_score:float=0
    market_structure_score:float=0; acceptance_score:float=0; participation_score:float=0
    continuation_probability:float=0; reversal_probability:float=0
    breakout_probability:float=0; pullback_quality:float=0
    exhaustion_score:float=0; noise_score:float=0; risk_score:float=0
    entry_quality:float=0; regime:str="UNKNOWN"; direction:str="NEUTRAL"
    decision:str="WAIT"; confidence:float=0; final_score:float=0
    sl_percent:float=2.0; tp_percent:float=4.0
    reasons:List[str]=field(default_factory=list)

@dataclass
class AIResult:
    decision:str="WAIT"; confidence:float=0.0; explanation:str=""
    risk_warnings:List[str]=field(default_factory=list); regime:str="unknown"
    error:bool=False

@dataclass
class FinalDecision:
    decision:Decision=Decision.WAIT; final_score:float=0.0; mssi_score:float=0.0
    ai_score:float=0.0; ai_explanation:str=""; ai_regime:str=""
    sl_percent:float=2.0; tp_percent:float=4.0; is_pullback:bool=False
    regime:str="UNKNOWN"; signals:List[str]=field(default_factory=list)
    reasons:List[str]=field(default_factory=list)

@dataclass
class TrendConfirmation:
    direction:TrendDirection=TrendDirection.NEUTRAL; entry_quality:EntryQuality=EntryQuality.BLOCKED
    daily_trend:str=""; hourly_trend:str=""; minute_timing:str=""; strength:float=0.0
    reasons:List[str]=field(default_factory=list)


def clamp(x, lo=0.0, hi=100.0): return max(lo, min(hi, x))
def safe_div(a, b): return a/b if abs(b) > 1e-12 else 0.0
def _mean(v): return sum(v)/len(v) if v else 0.0
def _std(v):
    if len(v)<2: return 0.0
    m=_mean(v); return (sum((x-m)**2 for x in v)/len(v))**0.5
def _zscore(val, arr):
    s=_std(arr)
    return (val-_mean(arr))/s if s>0 else 0.0
def _sigmoid(x):
    if x>=0: z=math.exp(-x); return 1/(1+z)
    z=math.exp(x); return z/(1+z)
def scale_signed_to_100(x): return clamp((x+1.0)*50.0, 0.0, 100.0)


class MSSIEngine:

    def extract(self, data) -> Optional[RawFeatures]:
        if len(data) < 30: return None
        o=[float(x[1]) for x in data]; h=[float(x[2]) for x in data]
        l=[float(x[3]) for x in data]; c=[float(x[4]) for x in data]
        v=[float(x[5]) for x in data]
        n=len(c); t=n-1
        f=RawFeatures(); f.price=c[t]
        f.ret_1 = safe_div(c[t]-c[t-1], c[t-1]) if t>=1 else 0
        f.ret_5 = safe_div(c[t]-c[t-5], c[t-5]) if t>=5 else 0
        f.ret_20 = safe_div(c[t]-c[t-20], c[t-20]) if t>=20 else 0
        trs=[max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])) for i in range(1,n)]
        f.true_range = trs[-1] if trs else 0
        atr20 = _mean(trs[-20:]) if len(trs)>=20 else _mean(trs)
        f.atr_ratio = safe_div(atr20, c[t])
        rets=[safe_div(c[i]-c[i-1], c[i-1]) for i in range(1,n)]
        f.realized_vol = _std(rets[-20:]) if len(rets)>=20 else _std(rets)
        net = abs(c[t]-c[max(0,t-20)])
        path = sum(abs(c[i]-c[i-1]) for i in range(max(1,t-19), t+1))
        f.efficiency = min(safe_div(net, path), 1.0)
        f.noise = 1.0 - f.efficiency
        signs=[1 if c[i]>=c[i-1] else -1 for i in range(max(1,t-19), t+1)]
        f.persistence = max(sum(1 for i in range(1,len(signs)) if signs[i]==signs[i-1]),0)/max(len(signs)-1,1) if len(signs)>1 else 0
        f.range_expansion = safe_div(trs[-1], atr20) if atr20>0 else 1.0
        short_rng = max(h[-5:])-min(l[-5:]) if n>=5 else 0
        long_rng = max(h[-20:])-min(l[-20:]) if n>=20 else 1
        f.compression = clamp(1.0 - safe_div(short_rng, long_rng), 0, 1)
        vol_z = _zscore(v[t], v[-20:]) if n>=20 else 0
        f.volume_zscore = vol_z
        f.participation = 0.6*_sigmoid(vol_z) + 0.4*_sigmoid(0)
        rng = max(h[t]-l[t], 1e-12)
        f.close_location = safe_div(c[t]-l[t], rng)
        f.buy_sell_imbalance = (f.close_location - 0.5) * 2 * min(abs(vol_z)/2, 1.0)
        f.delta_pressure = f.buy_sell_imbalance * (1 if f.ret_1>=0 else -1)
        f.oi_change = 0; f.oi_price_agreement = 0; f.funding_stress = 0
        uw = h[t]-max(o[t],c[t]); lw = min(o[t],c[t])-l[t]
        f.wick_rejection = safe_div(max(uw,lw), rng)
        f.breakout_pressure = (0.35*min(f.range_expansion/2,1) + 0.25*f.efficiency +
                               0.20*f.participation + 0.20*max(f.buy_sell_imbalance,0))
        f.reversal_pressure = (0.30*f.wick_rejection + 0.25*max(-f.delta_pressure,0) +
                               0.25*min(f.funding_stress/20,1) + 0.20*(1-f.efficiency))
        return f

    def infer_direction(self, f: RawFeatures) -> TrendDirection:
        ds = (0.40*max(min(f.ret_20*20,1),-1) + 0.25*max(min(f.ret_5*30,1),-1) +
              0.20*max(min(f.buy_sell_imbalance,1),-1) + 0.15*max(min(f.oi_price_agreement,1),-1))
        if ds > 0.12: return TrendDirection.BULLISH
        if ds < -0.12: return TrendDirection.BEARISH
        return TrendDirection.NEUTRAL

    def detect_regime(self, f: RawFeatures, direction: TrendDirection) -> MarketRegime:
        if f.reversal_pressure > 0.72 and f.range_expansion > 1.4: return MarketRegime.REVERSAL
        if f.breakout_pressure > 0.68 and f.compression > 0.35: return MarketRegime.BREAKOUT
        if f.efficiency > 0.62 and f.participation > 0.56: return MarketRegime.TRENDING
        if f.noise > 0.58 and f.range_expansion < 1.05: return MarketRegime.RANGING
        if f.realized_vol > 0.018: return MarketRegime.HIGH_VOLATILITY
        if f.realized_vol < 0.005: return MarketRegime.LOW_VOLATILITY
        if direction==TrendDirection.BULLISH and f.compression>0.45 and f.participation<0.52: return MarketRegime.ACCUMULATION
        if direction==TrendDirection.BEARISH and f.compression>0.45 and f.participation<0.52: return MarketRegime.DISTRIBUTION
        if f.reversal_pressure > 0.60: return MarketRegime.EXHAUSTION
        return MarketRegime.RANGING

    def score(self, f: RawFeatures, regime: MarketRegime, direction: TrendDirection) -> MSSIOutput:
        m = MSSIOutput()
        m.regime = regime.value
        m.direction = direction.value
        db_raw = (0.35*max(min(f.ret_20*15,1),-1) + 0.20*max(min(f.ret_5*20,1),-1) +
                  0.20*f.buy_sell_imbalance + 0.15*f.oi_price_agreement +
                  0.10*max(min((f.close_location-0.5)*2,1),-1))
        m.direction_bias = scale_signed_to_100(db_raw)
        m.trend_strength = clamp(35*f.efficiency + 25*min(f.range_expansion/1.8,1) +
                                 20*f.participation + 20*max(f.oi_price_agreement,0))
        m.momentum_score = clamp(30*abs(max(min(f.ret_1*80,1),-1)) +
                                 35*abs(max(min(f.ret_5*25,1),-1)) +
                                 35*(f.buy_sell_imbalance+1)/2)
        m.market_structure_score = clamp(40*f.efficiency + 25*(1-f.noise) +
                                         20*max(min((f.close_location-0.5)*2,1),0) +
                                         15*max(f.oi_price_agreement,0))
        m.acceptance_score = clamp(35*max(min((f.close_location-0.5)*2,1),0) +
                                   25*(1-f.wick_rejection) + 20*f.efficiency +
                                   20*min(f.range_expansion/1.5,1))
        m.participation_score = clamp(45*f.participation +
                                      30*min(max(f.volume_zscore,0)/3,1) +
                                      25*min(abs(f.buy_sell_imbalance),1))
        m.continuation_probability = clamp(30*f.efficiency + 20*max(f.oi_price_agreement,0) +
                                           20*f.participation + 15*m.acceptance_score/100 +
                                           15*f.persistence)
        m.reversal_probability = clamp(30*f.wick_rejection + 25*f.noise +
                                       20*max(0,f.range_expansion-1.3)/0.7 +
                                       15*(1-f.efficiency) + 10*(1-f.persistence))
        m.breakout_probability = clamp(25*m.momentum_score/100 + 20*f.participation +
                                       20*f.compression + 20*f.efficiency + 15*(1-f.noise))
        m.pullback_quality = clamp(35*f.efficiency + 25*m.acceptance_score/100 +
                                   20*(1-m.exhaustion_score/100) + 20*f.persistence)
        m.exhaustion_score = clamp(35*f.wick_rejection + 30*max(0,f.range_expansion-1.2)/0.8 +
                                   20*(1-f.efficiency) + 15*max(0,-f.buy_sell_imbalance))
        m.noise_score = clamp(55*f.noise + 25*(1-f.persistence) +
                              20*(1-abs(f.close_location-0.5)*2))
        m.risk_score = clamp(30*min(f.range_expansion/2,1) + 25*m.reversal_probability/100 +
                             20*m.noise_score/100 + 15*m.exhaustion_score/100 +
                             10*(1-m.pullback_quality/100))
        eq_raw = (0.24*m.trend_strength + 0.16*m.momentum_score +
                  0.15*m.market_structure_score + 0.14*m.participation_score +
                  0.14*m.continuation_probability + 0.09*m.pullback_quality -
                  0.08*m.reversal_probability - 0.14*m.risk_score)
        m.entry_quality = clamp(eq_raw)
        m.sl_percent = max(0.5, min(f.atr_ratio*100*1.5, CFG.max_sl_percent))
        m.tp_percent = max(1.0, min(f.atr_ratio*100*3.0, CFG.max_tp_percent))

        is_bull = direction == TrendDirection.BULLISH
        is_bear = direction == TrendDirection.BEARISH

        # ✅ تم إزالة "+ 3" من شرط التوازن
        if is_bull and m.entry_quality >= CFG.min_entry_quality and m.risk_score <= CFG.max_risk_for_entry and m.continuation_probability > m.reversal_probability and m.direction_bias >= CFG.min_long_score:
            m.decision = "BUY"
            m.confidence = m.entry_quality*0.35 + m.continuation_probability*0.25 + m.trend_strength*0.20 + m.participation_score*0.10 + m.acceptance_score*0.10
            m.final_score = m.confidence
        elif is_bear and m.entry_quality >= CFG.min_entry_quality and m.risk_score <= CFG.max_risk_for_entry and m.continuation_probability > m.reversal_probability and m.direction_bias <= (100 - CFG.min_short_score):
            m.decision = "SELL"
            m.confidence = m.entry_quality*0.35 + m.continuation_probability*0.25 + m.trend_strength*0.20 + m.participation_score*0.10 + m.acceptance_score*0.10
            m.final_score = m.confidence
        else:
            m.decision = "WAIT"
            # ✅ لا نصفر الثقة — نحافظ عليها لتحليل أفضل لاحقاً
            m.final_score = m.confidence  # not 0

        m.reasons = [
            f"Regime={regime.value} Dir={direction.value}",
            f"DE={f.efficiency:.3f} Noise={f.noise:.3f} Persist={f.persistence:.2f}",
            f"RE={f.range_expansion:.2f} CL={f.close_location:.2f} WR={f.wick_rejection:.2f}",
            f"VolZ={f.volume_zscore:.2f} Part={f.participation:.2f} BSI={f.buy_sell_imbalance:.3f}",
            f"DirBias={m.direction_bias:.1f} Trend={m.trend_strength:.1f} Mom={m.momentum_score:.1f}",
            f"Entry={m.entry_quality:.1f} Cont={m.continuation_probability:.1f} Rev={m.reversal_probability:.1f}",
            f"Exh={m.exhaustion_score:.1f} Risk={m.risk_score:.1f} PB={m.pullback_quality:.1f}",
        ]
        return m

    def analyze(self, data_1h, data_1d=None) -> Optional[MSSIOutput]:
        f = self.extract(data_1h)
        if not f: return None
        direction = self.infer_direction(f)
        regime = self.detect_regime(f, direction)
        m = self.score(f, regime, direction)
        if data_1d and len(data_1d) >= 30:
            fd = self.extract(data_1d)
            if fd:
                dd = self.infer_direction(fd)
                rd = self.detect_regime(fd, dd)
                md = self.score(fd, rd, dd)
                m.direction_bias = m.direction_bias*0.7 + md.direction_bias*0.3
                m.trend_strength = m.trend_strength*0.7 + md.trend_strength*0.3
                m.entry_quality = m.entry_quality*0.7 + md.entry_quality*0.3
                m.continuation_probability = m.continuation_probability*0.7 + md.continuation_probability*0.3
                m.reversal_probability = m.reversal_probability*0.7 + md.reversal_probability*0.3
                m.risk_score = m.risk_score*0.7 + md.risk_score*0.3
                m.confidence = m.confidence*0.7 + md.confidence*0.3
                m.final_score = m.confidence
                is_bull = m.direction_bias > 50
                is_bear = m.direction_bias < 50
                # ✅ نفس التعديل: إزالة "+ 3"
                if is_bull and m.entry_quality >= CFG.min_entry_quality and m.risk_score <= CFG.max_risk_for_entry and m.continuation_probability > m.reversal_probability and m.direction_bias >= CFG.min_long_score:
                    m.decision = "BUY"
                elif is_bear and m.entry_quality >= CFG.min_entry_quality and m.risk_score <= CFG.max_risk_for_entry and m.continuation_probability > m.reversal_probability and m.direction_bias <= (100 - CFG.min_short_score):
                    m.decision = "SELL"
                else:
                    m.decision = "WAIT"
                    m.final_score = m.confidence  # ✅ لا نصفر
        return m

mssi_engine = MSSIEngine()


class TradeDB:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        with self.lock:
            self.conn.execute("""CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, side TEXT,
                mode TEXT, entry_price REAL, quantity REAL, sl_price REAL,
                tp_price REAL, sl_order_id TEXT, tp_order_id TEXT,
                entry_order_id TEXT, confidence REAL, reason TEXT, timestamp TEXT,
                status TEXT DEFAULT 'OPEN', exit_price REAL, realized_pnl REAL,
                pnl_percent REAL, commission REAL DEFAULT 0, closed_at TEXT,
                close_reason TEXT, ai_explanation TEXT, final_score REAL, regime TEXT)""")
            self.conn.commit()
    def insert_trade(self, **kw):
        with self.lock:
            cur = self.conn.execute(
                "INSERT INTO trades (symbol,side,mode,entry_price,quantity,sl_price,tp_price,"
                "sl_order_id,tp_order_id,entry_order_id,confidence,reason,timestamp,status,"
                "ai_explanation,final_score,regime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (kw.get("symbol"),kw.get("side"),kw.get("mode"),kw.get("entry_price"),
                 kw.get("quantity"),kw.get("sl_price"),kw.get("tp_price"),
                 kw.get("sl_order_id",""),kw.get("tp_order_id",""),kw.get("entry_order_id",""),
                 kw.get("confidence",0),kw.get("reason",""),kw.get("timestamp",""),
                 kw.get("status","OPEN"),kw.get("ai_explanation",""),kw.get("final_score",0),
                 kw.get("regime","")))
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
            r = self.conn.execute("SELECT COUNT(*) FROM trades WHERE timestamp LIKE ?",(f"{t}%",)).fetchone()
        return r[0] if r else 0
    def open_count(self):
        with self.lock:
            r = self.conn.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN'").fetchone()
        return r[0] if r else 0

db = TradeDB(CFG.db_path)

app = Flask(__name__)
bot_stats = {"status":"STARTING","version":"MSSI-V1","uptime":0,"trades_today":0,
             "open_positions":0,"scanner":[],"last_analysis":{},
             "mode":"LIVE","current_ip":"","deploy_ip":""}
T0 = time.time()

@app.route("/")
def home():
    return (f"<h2>MSSI TRADING BOT</h2>"
            f"<p>IP: <b>{bot_stats['current_ip']}</b></p>"
            f"<p>Trades: {bot_stats['trades_today']} | Open: {bot_stats['open_positions']}</p>")
@app.route("/health")
def health():
    bot_stats["uptime"] = int(time.time()-T0)
    bot_stats["trades_today"] = db.count_today()
    bot_stats["open_positions"] = db.open_count()
    return jsonify(bot_stats)
def run_server(): app.run(host="0.0.0.0",port=CFG.flask_port,debug=False,use_reloader=False)

exchange_public = ccxt.binance({"enableRateLimit":True,"options":{"defaultType":"swap","adjustForTimeDifference":True}})
exchange = ccxt.binance({"apiKey":CFG.binance_api_key,"secret":CFG.binance_secret,"enableRateLimit":True,"options":{"defaultType":"swap","adjustForTimeDifference":True}})
ai_client = OpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=CFG.nvidia_api_key)

def get_ip():
    try: return requests.get("https://api.ipify.org", timeout=10).text
    except: return "UNKNOWN"

def show_deploy_ip():
    ip = get_ip(); bot_stats["current_ip"] = ip; bot_stats["deploy_ip"] = ip
    logger.critical("="*60)
    logger.critical(f"  DEPLOY IP:  {ip}")
    logger.critical(f"  Add in Binance API Management -> IP Whitelist")
    logger.critical("="*60)
    return ip

class IPMonitor:
    def __init__(self): self._run=True; self.ip=""
    def start(self): threading.Thread(target=self._loop,daemon=True).start()
    def stop(self): self._run=False
    def _loop(self):
        self.ip=show_deploy_ip()
        while self._run:
            time.sleep(60)
            try:
                ip=get_ip()
                if ip!=self.ip and ip!="UNKNOWN":
                    logger.critical(f"  IP CHANGED: {self.ip} -> {ip}")
                    self.ip=ip; bot_stats["current_ip"]=ip
            except: pass

ip_monitor = IPMonitor()

class CandleManager:
    def __init__(self, maxlen=500):
        self._c={}; self._f={}; self._lock=threading.Lock(); self._m=maxlen
    def ensure(self, sk, tfs):
        with self._lock:
            if sk not in self._c:
                self._c[sk]={tf:deque(maxlen=self._m) for tf in tfs}
                self._f[sk]={tf:None for tf in tfs}
    def update(self, sk, tf, candle, closed):
        with self._lock:
            if sk not in self._c or tf not in self._c[sk]: return
            if closed:
                dq=self._c[sk][tf]
                if dq and dq[-1][0]==candle[0]: dq[-1]=candle
                else: dq.append(candle)
                self._f[sk][tf]=None
            else: self._f[sk][tf]=candle
    def get(self, sk, tf):
        with self._lock:
            if sk not in self._c or tf not in self._c[sk]: return []
            return list(self._c[sk][tf])
    def count(self, sk, tf):
        with self._lock: return len(self._c.get(sk,{}).get(tf,[]))
    def load(self, sk, tf, data):
        with self._lock:
            if sk not in self._c: return
            if data and len(data)>1:
                self._c[sk][tf]=deque(data[:-1],maxlen=self._m); self._f[sk][tf]=data[-1]
            else: self._c[sk][tf]=deque(data,maxlen=self._m)

cm = CandleManager(CFG.candle_maxlen)
trade_state = {}; execution_lock = threading.Lock()
active_symbols = {}; active_lock = threading.Lock()


class AIAnalyst:
    def analyze(self, symbol, mssi: MSSIOutput) -> AIResult:
        result = AIResult()
        if not CFG.use_ai_veto and not CFG.use_ai_explainer:
            return result
        prompt = f"""أنت محلل تداول. اقرأ نتائج محرك MSSI التالي وأعطِ رأيك.

العملة: {symbol}
قرار MSSI: {mssi.decision}
Regime: {mssi.regime}
Direction Bias: {mssi.direction_bias:.1f}/100
Trend Strength: {mssi.trend_strength:.1f}/100
Momentum: {mssi.momentum_score:.1f}/100
Entry Quality: {mssi.entry_quality:.1f}/100
Continuation: {mssi.continuation_probability:.1f}/100
Reversal: {mssi.reversal_probability:.1f}/100
Exhaustion: {mssi.exhaustion_score:.1f}/100
Risk: {mssi.risk_score:.1f}/100
Noise: {mssi.noise_score:.1f}/100

القواعد:
1. إذا ترى خطر حقيقي لا يراه MSSI: اعترض (WAIT)
2. إذا MSSI صحيح: وافق
3. confidence = مدى ثقتك (0-100)

أجب JSON فقط:
{{"decision":"BUY أو SELL أو WAIT","confidence":75,"regime":"...","explanation":"شرح مختصر بالعربية","risk_warnings":[]}}"""
        try:
            comp = ai_client.chat.completions.create(
                model=CFG.ai_model,
                messages=[{"role":"system","content":"/think"},{"role":"user","content":prompt}],
                temperature=0.6, top_p=0.95, max_tokens=1024, stream=False)
            raw = comp.choices[0].message.content or ""
            cleaned = raw.strip()
            if cleaned.startswith("```"):
                lines=[l for l in cleaned.split("\n") if not l.strip().startswith("```")]
                cleaned="\n".join(lines).strip()
            js=cleaned.find("{"); je=cleaned.rfind("}")+1
            if js>=0 and je>js: cleaned=cleaned[js:je]
            dj=json.loads(cleaned)
            result.decision=str(dj.get("decision","WAIT")).upper()
            if result.decision not in ("BUY","SELL","WAIT"): result.decision="WAIT"
            result.confidence=max(0,min(100,float(dj.get("confidence",0))))
            result.explanation=str(dj.get("explanation",""))
            result.risk_warnings=dj.get("risk_warnings",[])
            result.regime=str(dj.get("regime","unknown"))
            logger.info(f"AI {symbol}: {result.decision} | Conf={result.confidence} | {result.explanation[:80]}")
        except Exception as e:
            logger.warning(f"AI ERROR {symbol}: {e}")
            result.decision="WAIT"; result.confidence=0; result.error=True
            result.explanation=f"AI_ERROR: {str(e)[:100]}"
        return result

ai_analyst = AIAnalyst()


class MarketScanner:
    def __init__(self): self._run=True
    def start(self): threading.Thread(target=self._loop,daemon=True).start(); logger.info("Scanner MSSI")
    def stop(self): self._run=False
    def _loop(self):
        time.sleep(5)
        while self._run:
            try: self._cycle()
            except Exception as e: logger.error(f"Scanner: {e}",exc_info=True)
            time.sleep(CFG.scanner_interval)
    def _cycle(self):
        logger.info("="*60); logger.info("Scanning..."); candidates=[]
        for sk,sym in CFG.watchlist.items():
            try:
                r=self._quick(sk,sym)
                if r: candidates.append(r)
            except: pass
            time.sleep(0.3)
        candidates.sort(key=lambda x:x.score,reverse=True); top=candidates[:CFG.scanner_top_n]
        logger.info(f"Scanned {len(CFG.watchlist)} -> {len(candidates)} -> Top {len(top)}")
        for i,c in enumerate(top):
            logger.info(f"  #{i+1} {c.symbol} | Score={c.score:.1f} | Vol={c.volume_usdt/1e6:.1f}M | ATR={c.atr_pct:.2f}%")
        bot_stats["scanner"]=[{"symbol":c.symbol,"score":c.score} for c in top]
        with active_lock:
            active_symbols.clear()
            for c in top: active_symbols[c.symbol_key]=c.symbol; cm.ensure(c.symbol_key,CFG.timeframes)
        for c in top:
            pos=get_pos(c.symbol)
            if pos=="ERROR" or pos: continue
            threading.Thread(target=self._deep,args=(c.symbol_key,c.symbol),daemon=True).start()
            time.sleep(1)
    def _quick(self,sk,sym):
        result=ScannerResult(symbol_key=sk,symbol=sym); reasons=[]
        try:
            ticker=exchange_public.fetch_ticker(sym)
            vol=float(ticker.get("quoteVolume",0) or 0); result.volume_usdt=vol
            if vol<CFG.scanner_min_volume_usdt: return None
        except: return None
        try:
            ohlcv=exchange_public.fetch_ohlcv(sym,"1h",limit=50)
            if len(ohlcv)<20: return None
            h=[float(x[2]) for x in ohlcv]; l=[float(x[3]) for x in ohlcv]; c=[float(x[4]) for x in ohlcv]
            trs=[max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])) for i in range(1,len(c))]
            atr=sum(trs[-14:])/14 if len(trs)>=14 else 0
            price=c[-1]
            if atr and price:
                ap=(atr/price)*100; result.atr_pct=ap
                if ap<CFG.scanner_min_atr_pct: return None
        except: return None
        if len(c)>=20:
            net=abs(c[-1]-c[-20]); path=sum(abs(c[i]-c[i-1]) for i in range(len(c)-20,len(c)))
            eff=net/path if path>0 else 0
            result.score = eff*5 + min(vol/50_000_000,1)*2 + min(ap/1.5,1)*2
        result.reasons=reasons; return result
    def _deep(self,sk,sym):
        logger.info(f"Deep: {sym}")
        if cm.count(sk,"1h")<50: self._load(sk,sym)
        d1h=cm.get(sk,"1h"); d1d=cm.get(sk,"1d")
        if len(d1h)<50: logger.info(f"Not enough: {sym}"); return
        mssi = mssi_engine.analyze(d1h, d1d if len(d1d)>=30 else None)
        if not mssi: logger.info(f"MSSI fail: {sym}"); return
        logger.info(f">>> MSSI {sym}: {mssi.decision} | Regime={mssi.regime} | Dir={mssi.direction}")
        for r in mssi.reasons: logger.info(f"   {r}")
        ai = ai_analyst.analyze(sym, mssi)
        final = FinalDecision()
        final.sl_percent = mssi.sl_percent; final.tp_percent = mssi.tp_percent
        final.regime = mssi.regime; final.is_pullback = mssi.pullback_quality > 50
        final.mssi_score = mssi.confidence; final.ai_score = ai.confidence
        final.ai_explanation = ai.explanation; final.ai_regime = ai.regime
        if mssi.decision == "WAIT":
            final.decision = Decision.WAIT; final.final_score = mssi.confidence  # ✅ لا نصفر
            final.reasons = [f"MSSI WAIT | Regime={mssi.regime}"] + mssi.reasons
        elif ai.error:
            final.decision = Decision.BUY if mssi.decision=="BUY" else Decision.SELL
            final.final_score = mssi.confidence * CFG.mssi_weight
            final.reasons = [f"MSSI {mssi.decision} (AI_ERROR) | Conf={mssi.confidence:.1f}"] + mssi.reasons
        elif CFG.use_ai_veto and ai.decision == "WAIT" and ai.confidence >= 75:
            final.decision = Decision.WAIT; final.final_score = mssi.confidence  # ✅ لا نصفر
            final.reasons = [f"AI VETO (conf={ai.confidence}) | MSSI was {mssi.decision}"] + mssi.reasons
        elif CFG.use_ai_veto and ai.decision != mssi.decision and ai.decision != "WAIT" and ai.confidence >= 70:
            final.final_score = mssi.confidence * CFG.mssi_weight * 0.7
            if final.final_score >= CFG.min_confidence:
                final.decision = Decision.BUY if mssi.decision=="BUY" else Decision.SELL
                final.reasons = [f"MSSI {mssi.decision} (AI disagrees, reduced) | Score={final.final_score:.1f}"] + mssi.reasons
            else:
                final.decision = Decision.WAIT
                final.reasons = [f"MSSI {mssi.decision} REDUCED below threshold"] + mssi.reasons
        else:
            final.decision = Decision.BUY if mssi.decision=="BUY" else Decision.SELL
            final.final_score = mssi.confidence * CFG.mssi_weight + ai.confidence * CFG.ai_weight
            final.reasons = [f"MSSI {mssi.decision} | Score={final.final_score:.1f} | AI={ai.decision}({ai.confidence})"] + mssi.reasons
        logger.info(f"FINAL {sym}: {final.decision.value} | Score={final.final_score:.1f} | {final.reasons[0] if final.reasons else ''}")
        bot_stats["last_analysis"][sym]={"decision":final.decision.value,"final_score":round(final.final_score,1),
            "regime":mssi.regime,"ai":f"{ai.decision}({ai.confidence}){'[ERR]' if ai.error else ''}",
            "time":datetime.now(timezone.utc).isoformat()}
        if final.decision==Decision.WAIT: return
        if final.final_score<CFG.min_confidence:
            logger.info(f"Score too low: {final.final_score:.1f} < {CFG.min_confidence}"); return
        execute_trade(sym, final)
    def _load(self,sk,sym):
        for tf in CFG.timeframes:
            try:
                limit=500 if tf=="1d" else 300
                data=exchange_public.fetch_ohlcv(sym,timeframe=tf,limit=limit)
                cm.load(sk,tf,data)
            except Exception as e: logger.warning(f"Load {sym} {tf}: {e}")
            time.sleep(0.3)


def get_pos(sym):
    try:
        for p in exchange.fetch_positions([sym]):
            ct=p.get("contracts")
            if ct and float(ct)>0: return p
        return None
    except Exception as e: logger.error(f"Pos {sym}: {e}"); return "ERROR"

# ✅ التعديل 1: emergency_close مع حذف الأوامر المعلقة أولاً
def emergency_close(sym, reason):
    logger.critical(f"EMERGENCY CLOSE: {sym} | {reason}")
    
    # 1. جلب الصفقة من قاعدة البيانات لمعرفة أرقام طلبات SL و TP الخاصة بها فقط
    trade_to_close = None
    for t in db.get_open_trades():
        if t["symbol"] == sym:
            trade_to_close = t
            break

    # 2. حذف الطلبات المحددة الخاصة بهذه الصفقة فقط
    if trade_to_close:
        for oid in [trade_to_close.get("sl_order_id"), trade_to_close.get("tp_order_id")]:
            if oid:
                try:
                    exchange.cancel_order(oid, sym)
                    logger.info(f"🧹 تم حذف الطلب المرتبط بالصفقة: {oid}")
                except Exception as e:
                    if "Unknown order" not in str(e) and "Order does not exist" not in str(e):
                        logger.warning(f"⚠️ فشل حذف الطلب {oid}: {e}")

    # 3. إغلاق الكمية المفتوحة بسعر السوق
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
            pos=get_pos(sym)
            if pos=="ERROR": return
            if pos: logger.info(f"Busy: {sym}"); return
            if db.count_today()>=CFG.max_daily_trades: logger.info("Daily limit"); return
            if db.open_count()>=CFG.max_open_positions: logger.info("Positions full"); return
            st=trade_state.get(sym,{})
            if time.time()-st.get("t",0)<CFG.cooldown_seconds: logger.info(f"Cooldown: {sym}"); return
            ticker=exchange_public.fetch_ticker(sym); price=ticker["last"]
            raw_qty=(CFG.margin_usdt*CFG.leverage)/price
            qty=float(exchange.amount_to_precision(sym,raw_qty))
            side="buy" if final.decision==Decision.BUY else "sell"
            pname="LONG" if side=="buy" else "SHORT"
            logger.info(f"LIVE TRADE {sym} | {pname} | {price} | Score={final.final_score:.1f} | Regime={final.regime}")
            exchange.set_leverage(CFG.leverage,sym)
            order=exchange.create_market_order(sym,side,qty)
            eoid=order.get("id",""); time.sleep(1)
            p=get_pos(sym)
            if p=="ERROR" or p is None: logger.critical(f"No pos: {sym}"); return
            entry=float(p.get("entryPrice",price)); aqty=abs(float(p.get("contracts",0)))
            if aqty<=0: emergency_close(sym,"zero qty"); return
            logger.info(f"qty={aqty} entry={entry}")
            sl_p=max(0.5,min(final.sl_percent,CFG.max_sl_percent))
            tp_p=max(1.0,min(final.tp_percent,CFG.max_tp_percent))
            if side=="buy": sl_price=entry*(1-sl_p/100); tp_price=entry*(1+tp_p/100)
            else: sl_price=entry*(1+sl_p/100); tp_price=entry*(1-tp_p/100)
            sl_price=float(exchange.price_to_precision(sym,sl_price))
            tp_price=float(exchange.price_to_precision(sym,tp_price))
            cs="sell" if side=="buy" else "buy"
            sloid=""
            try:
                slo=exchange.create_order(sym,"STOP_MARKET",cs,aqty,None,
                    {"stopPrice":sl_price,"reduceOnly":True,"workingType":"MARK_PRICE"})
                sloid=slo.get("id",""); logger.info(f"SL: {sl_price}")
            except Exception as e:
                logger.critical(f"SL fail: {e}"); emergency_close(sym,"SL fail"); return
            tpoid=""
            try:
                tpo=exchange.create_order(sym,"TAKE_PROFIT_MARKET",cs,aqty,None,
                    {"stopPrice":tp_price,"reduceOnly":True,"workingType":"MARK_PRICE"})
                tpoid=tpo.get("id",""); logger.info(f"TP: {tp_price}")
            except Exception as e:
                logger.error(f"TP fail: {e}")
                try: exchange.cancel_order(sloid,sym)
                except: pass
                emergency_close(sym,"TP fail"); return
            tid=db.insert_trade(symbol=sym,side=pname,mode="LIVE",entry_price=entry,
                quantity=aqty,sl_price=sl_price,tp_price=tp_price,
                sl_order_id=sloid,tp_order_id=tpoid,entry_order_id=eoid,
                confidence=final.final_score,
                reason=f"MSSI={final.mssi_score:.0f} AI={final.ai_score:.0f} Final={final.final_score:.1f} | {final.ai_explanation[:100]}",
                timestamp=datetime.now(timezone.utc).isoformat(),
                ai_explanation=final.ai_explanation, final_score=final.final_score, regime=final.regime)
            logger.info(f"Trade #{tid}"); st["t"]=time.time()
        except Exception as e:
            logger.error(f"Exec: {e}",exc_info=True); emergency_close(sym,str(e))


class PositionMonitor:
    def __init__(self): self._run=True
    def start(self): threading.Thread(target=self._loop,daemon=True).start(); logger.info("Monitor")
    def stop(self): self._run=False
    def _loop(self):
        while self._run:
            try:
                for t in db.get_open_trades(): self._check(t)
            except Exception as e: logger.error(f"Monitor: {e}")
            time.sleep(CFG.monitor_interval)
    def _check(self,trade):
        sym=trade["symbol"]
        sl_st=self._ost(sym,trade.get("sl_order_id")); tp_st=self._ost(sym,trade.get("tp_order_id"))
        pos=get_pos(sym)
        if pos=="ERROR": return
        if pos is None:
            reason="STOP_LOSS" if sl_st=="closed" else "TAKE_PROFIT" if tp_st=="closed" else "MANUAL"
            ep,rpnl,comm=self._rexit(sym,trade); entry=trade["entry_price"]; qty=trade["quantity"]
            if ep==0: ep=entry
            if rpnl==0:
                cs=self._csz(sym); rq=qty*cs
                rpnl=(ep-entry)*rq if trade["side"]=="LONG" else (entry-ep)*rq
            notional=entry*qty if entry*qty else 1; pp=(rpnl/notional)*100
            db.close_trade(trade["id"],ep,rpnl,pp,comm,reason)
            logger.info(f"CLOSED {sym} | {reason} | PnL={rpnl:.4f} ({pp:.2f}%)")
            self._cancel(sym,trade)
    def _csz(self,sym):
        try: return float(exchange.market(sym).get("contractSize",1) or 1)
        except: return 1.0
    def _ost(self,sym,oid):
        if not oid: return "unknown"
        try: return exchange.fetch_order(oid,sym).get("status","unknown")
        except: return "unknown"
    def _rexit(self,sym,trade):
        ep,rpnl,comm=0,0,0
        try:
            trades=exchange.fetch_my_trades(sym,limit=30)
            for t in reversed(trades):
                if t.get("reduceOnly") or (t.get("side")=="sell" and trade["side"]=="LONG") or (t.get("side")=="buy" and trade["side"]=="SHORT"):
                    ep=float(t.get("price",0) or t.get("average",0)); comm=float(t.get("fee",{}).get("cost",0) or 0)
                    info=t.get("info",{}); rs=info.get("realizedPnl","0"); rpnl=float(rs) if rs else 0; break
        except: pass
        if ep==0:
            try: ep=exchange_public.fetch_ticker(sym)["last"]
            except: pass
        return ep,rpnl,comm
    # ✅ التعديل 2: _cancel مع تتبع الأخطاء والتنظيف الجراحي
    def _cancel(self, sym, trade):
        logger.info(f"🧹 جاري تنظيف الطلبات المرتبطة بالصفقة المنتهية {sym}...")
        
        # استخراج أرقام الطلبات من قاعدة بيانات البوت (SL و TP)
        orders_to_cancel = [
            ("SL", trade.get("sl_order_id")), 
            ("TP", trade.get("tp_order_id"))
        ]
        
        for order_type, oid in orders_to_cancel:
            if not oid: 
                continue
                
            try:
                # حذف الطلب المحدد فقط
                exchange.cancel_order(oid, sym)
                logger.info(f"✅ تم حذف طلب {order_type} الخاص بالبوت بنجاح (ID: {oid}).")
            except Exception as e:
                # إذا ظهر هذا الخطأ يعني أن الطلب تنفذ (ضرب الهدف/الوقف) أو تم حذفه مسبقاً
                if "-2011" in str(e) or "Unknown order" in str(e) or "Order does not exist" in str(e):
                    logger.info(f"ℹ️ طلب {order_type} غير موجود (تم تنفيذه أو حذفه مسبقاً).")
                else:
                    logger.error(f"⚠️ خطأ غير متوقع أثناء حذف طلب {order_type}: {e}")


async def ws_worker():
    delay=CFG.ws_reconnect_delay
    while True:
        with active_lock: current=dict(active_symbols)
        if not current: await asyncio.sleep(10); continue
        streams=[]
        for sk in current:
            for tf in CFG.timeframes: streams.append(f"{sk}@kline_{tf}")
        url="wss://fstream.binance.com/stream?streams="+"/".join(streams)
        try:
            async with websockets.connect(url,ping_interval=CFG.ws_ping_interval, ping_timeout=CFG.ws_ping_timeout) as ws:
                logger.info(f"WS connected ({len(current)})"); delay=CFG.ws_reconnect_delay
                async for msg in ws:
                    data=json.loads(msg); k=data.get("data",{}).get("k")
                    if not k: continue
                    sk=k["s"].lower(); tf=k["i"]
                    candle=[k["t"],float(k["o"]),float(k["h"]),float(k["l"]),float(k["c"]),float(k["v"])]
                    cm.update(sk,tf,candle,k["x"])
        except Exception as e: logger.error(f"WS: {e}")
        await asyncio.sleep(delay); delay=min(delay*2,120)


def main():
    ip = show_deploy_ip()
    logger.info("="*60)
    logger.info("MSSI TRADING BOT — LIVE (Opportunity Mode)")
    logger.info(f"   IP: {ip}")
    logger.info(f"   Scanner every {CFG.scanner_interval}s → Top {CFG.scanner_top_n}")
    logger.info(f"   Min Entry Quality: {CFG.min_entry_quality} | Min Confidence: {CFG.min_confidence}")
    logger.info(f"   Max Risk: {CFG.max_risk_for_entry} | Open Positions: {CFG.max_open_positions}")
    logger.info(f"   Leverage: x{CFG.leverage} | Margin: {CFG.margin_usdt} USDT")
    logger.info("="*60)
    ip_monitor.start()
    threading.Thread(target=run_server,daemon=True).start(); time.sleep(2)
    try:
        t=exchange_public.fetch_ticker("BTC/USDT:USDT")
        logger.info(f"Binance OK | BTC: {t['last']}")
    except Exception as e: logger.critical(f"Binance: {e}"); return
    logger.info("Loading data...")
    for sk,sym in CFG.watchlist.items():
        cm.ensure(sk,CFG.timeframes)
        for tf in CFG.timeframes:
            try:
                limit=500 if tf=="1d" else 300
                data=exchange_public.fetch_ohlcv(sym,timeframe=tf,limit=limit)
                cm.load(sk,tf,data)
            except: pass
            time.sleep(0.2)
    logger.info("Data ready")
    monitor=PositionMonitor(); monitor.start()
    scanner=MarketScanner(); scanner.start()
    bot_stats["status"]="RUNNING"
    try: asyncio.run(ws_worker())
    except KeyboardInterrupt: logger.info("Shutdown"); scanner.stop(); monitor.stop(); ip_monitor.stop()

if __name__=="__main__":
    main()
