#!/usr/bin/env python3
"""
  AI TRADING BOT V8 LIVE
  - LIVE trading (not paper)
  - AI = 55% of final decision
  - Signal Engine = 45%
  - IP shown at deploy for Binance whitelist
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


# ================================================================
#  CONFIG
# ================================================================
@dataclass
class Config:
    binance_api_key: str = "IX7kLH0ssWHP5TpYMUGcp0pzq4LX4Lqi7m4XtlqMkkq6DCZAsLhoeYZ3533jJFF4"
    binance_secret: str = "LmICnpSpMxL1riv4RfIf0HBGRfhDTP5JhDUYdlPSukpqV7kDTonrZ0j3DWp1a7hU"
    nvidia_api_key: str = "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"
    ai_model: str = "https://integrate.api.nvidia.com/v1"

    # ✅ LIVE
    dry_run: bool = False

    leverage: int = 10
    margin_usdt: float = 10.0
    max_daily_trades: int = 8
    max_open_positions: int = 2
    cooldown_seconds: int = 180
    max_sl_percent: float = 5.0
    max_tp_percent: float = 10.0

    # ✅ AI Weight
    ai_weight: float = 0.55
    signal_weight: float = 0.45
    min_final_score: float = 45.0

    # Signal thresholds
    min_score_to_enter: int = 4
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
#  ENUMS & DATA
# ================================================================
class Decision(Enum):
    BUY="BUY"; SELL="SELL"; WAIT="WAIT"
class TrendDirection(Enum):
    BULLISH="BULLISH"; BEARISH="BEARISH"; NEUTRAL="NEUTRAL"
class EntryQuality(Enum):
    ALLOWED="ALLOWED"; CAUTION="CAUTION"; BLOCKED="BLOCKED"

@dataclass
class ScannerResult:
    symbol_key:str=""; symbol:str=""; score:float=0.0; volume_usdt:float=0.0
    atr_pct:float=0.0; trend_1h:str=""; rsi_1h:float=50.0
    reasons:List[str]=field(default_factory=list)

@dataclass
class SignalResult:
    decision:Decision=Decision.WAIT; buy_score:int=0; sell_score:int=0; max_score:int=12
    is_pullback:bool=False; signals:List[str]=field(default_factory=list)
    reasons:List[str]=field(default_factory=list); sl_percent:float=2.0; tp_percent:float=4.0

@dataclass
class AIResult:
    decision:str="WAIT"; confidence:float=0.0; explanation:str=""
    risk_warnings:List[str]=field(default_factory=list); regime:str="unknown"

@dataclass
class FinalDecision:
    decision:Decision=Decision.WAIT
    final_score:float=0.0
    signal_score:float=0.0
    ai_score:float=0.0
    ai_explanation:str=""
    ai_regime:str=""
    sl_percent:float=2.0; tp_percent:float=4.0
    is_pullback:bool=False
    signals:List[str]=field(default_factory=list)
    reasons:List[str]=field(default_factory=list)

@dataclass
class TrendConfirmation:
    direction:TrendDirection=TrendDirection.NEUTRAL; entry_quality:EntryQuality=EntryQuality.BLOCKED
    daily_trend:str=""; hourly_trend:str=""; minute_timing:str=""; strength:float=0.0
    reasons:List[str]=field(default_factory=list)

@dataclass
class TradeRecord:
    symbol:str=""; side:str=""; entry_price:float=0.0; quantity:float=0.0
    sl_price:float=0.0; tp_price:float=0.0; sl_order_id:str=""; tp_order_id:str=""
    entry_order_id:str=""; confidence:float=0.0; reason:str=""; timestamp:str=""
    status:str="OPEN"; mode:str="LIVE"


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
                entry_order_id TEXT, confidence REAL, reason TEXT, timestamp TEXT,
                status TEXT DEFAULT 'OPEN', exit_price REAL, realized_pnl REAL,
                pnl_percent REAL, commission REAL DEFAULT 0, closed_at TEXT,
                close_reason TEXT, ai_explanation TEXT, final_score REAL)""")
            self.conn.commit()
    def insert_trade(self, **kw):
        with self.lock:
            cur = self.conn.execute(
                "INSERT INTO trades (symbol,side,mode,entry_price,quantity,sl_price,tp_price,"
                "sl_order_id,tp_order_id,entry_order_id,confidence,reason,timestamp,status,"
                "ai_explanation,final_score) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (kw.get("symbol"),kw.get("side"),kw.get("mode"),kw.get("entry_price"),
                 kw.get("quantity"),kw.get("sl_price"),kw.get("tp_price"),
                 kw.get("sl_order_id",""),kw.get("tp_order_id",""),kw.get("entry_order_id",""),
                 kw.get("confidence",0),kw.get("reason",""),kw.get("timestamp",""),
                 kw.get("status","OPEN"),kw.get("ai_explanation",""),kw.get("final_score",0)))
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


# ================================================================
#  FLASK
# ================================================================
app = Flask(__name__)
bot_stats = {"status":"STARTING","version":"V8-LIVE","uptime":0,"trades_today":0,
             "open_positions":0,"scanner":[],"last_analysis":{},
             "mode":"LIVE","current_ip":"","deploy_ip":""}
T0 = time.time()

@app.route("/")
def home():
    return (f"<h2>AI TRADING BOT V8 LIVE</h2>"
            f"<p>IP: <b>{bot_stats['current_ip']}</b></p>"
            f"<p>Deploy IP: <b>{bot_stats['deploy_ip']}</b></p>"
            f"<p>Trades: {bot_stats['trades_today']} | Open: {bot_stats['open_positions']}</p>")
@app.route("/health")
def health():
    bot_stats["uptime"] = int(time.time()-T0)
    bot_stats["trades_today"] = db.count_today()
    bot_stats["open_positions"] = db.open_count()
    return jsonify(bot_stats)
def run_server(): app.run(host="0.0.0.0",port=CFG.flask_port,debug=False,use_reloader=False)


# ================================================================
#  EXCHANGE
# ================================================================
exchange_public = ccxt.binance({
    "enableRateLimit": True,
    "options": {"defaultType": "swap", "adjustForTimeDifference": True},
})
exchange = ccxt.binance({
    "apiKey": CFG.binance_api_key, "secret": CFG.binance_secret,
    "enableRateLimit": True,
    "options": {"defaultType": "swap", "adjustForTimeDifference": True},
})
ai_client = OpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=CFG.nvidia_api_key)


# ================================================================
#  ✅ IP MONITOR - يظهر بوضوح عند Deploy
# ================================================================
def get_ip():
    try: return requests.get("https://api.ipify.org", timeout=10).text
    except: return "UNKNOWN"

def show_deploy_ip():
    """✅ يعرض IP بشكل بارز جداً عند أول تشغيل"""
    ip = get_ip()
    bot_stats["current_ip"] = ip
    bot_stats["deploy_ip"] = ip
    logger.critical("=" * 60)
    logger.critical(f"  🌐🌐🌐  DEPLOY IP:  {ip}  🌐🌐🌐")
    logger.critical(f"  👉 أضف هذا IP في Binance API Management")
    logger.critical(f"  👉 Binance -> API Management -> Edit -> IP Whitelist")
    logger.critical(f"  👉 أضف: {ip}")
    logger.critical("=" * 60)
    return ip

class IPMonitor:
    def __init__(self): self._run = True; self.ip = ""
    def start(self): threading.Thread(target=self._loop, daemon=True).start()
    def stop(self): self._run = False
    def _loop(self):
        self.ip = show_deploy_ip()
        while self._run:
            time.sleep(60)
            try:
                ip = get_ip()
                if ip != self.ip and ip != "UNKNOWN":
                    logger.critical("=" * 60)
                    logger.critical(f"  🔄🔄🔄  IP CHANGED: {self.ip} -> {ip}")
                    logger.critical(f"  👉 أضف IP الجديد في Binance: {ip}")
                    logger.critical("=" * 60)
                    self.ip = ip
                    bot_stats["current_ip"] = ip
            except: pass

ip_monitor = IPMonitor()


# ================================================================
#  CANDLE MANAGER
# ================================================================
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


# ================================================================
#  INDICATORS
# ================================================================
def closes(d): return [float(x[4]) for x in d]
def highs(d): return [float(x[2]) for x in d]
def lows(d): return [float(x[3]) for x in d]
def volumes(d): return [float(x[5]) for x in d]
def sma(v,p):
    if len(v)<p: return None
    return sum(v[-p:])/p
def ema(v,p):
    if len(v)<p: return None
    k=2.0/(p+1); r=sum(v[:p])/p
    for x in v[p:]: r=(x-r)*k+r
    return r
def ema_series(v,p):
    if len(v)<p: return []
    k=2.0/(p+1); r=[sum(v[:p])/p]
    for x in v[p:]: r.append((x-r[-1])*k+r[-1])
    return r
def calc_rsi(v,p=14):
    if len(v)<p+1: return None
    g,l=[],[]
    for i in range(1,len(v)):
        d=v[i]-v[i-1]; g.append(max(d,0)); l.append(max(-d,0))
    ag=sum(g[:p])/p; al=sum(l[:p])/p
    for i in range(p,len(g)):
        ag=((ag*(p-1))+g[i])/p; al=((al*(p-1))+l[i])/p
    if al==0: return 100.0
    return 100.0-(100.0/(1.0+ag/al))
def calc_stochastic(data,kp=14,dp=3):
    if len(data)<kp+dp: return None
    h,l,c=highs(data),lows(data),closes(data); kv=[]
    for i in range(kp-1,len(c)):
        hh=max(h[i-kp+1:i+1]); ll=min(l[i-kp+1:i+1])
        kv.append(((c[i]-ll)/(hh-ll))*100 if hh!=ll else 50.0)
    if len(kv)<dp: return None
    return {"k":round(kv[-1],2),"d":round(sum(kv[-dp:])/dp,2)}
def calc_macd(v):
    if len(v)<50: return None
    e12,e26=ema_series(v,12),ema_series(v,26); ml=[]
    for i in range(len(e26)):
        idx=i+14
        if idx<len(e12): ml.append(e12[idx]-e26[i])
    if len(ml)<9: return None
    sig=ema_series(ml,9)
    if not sig: return None
    mv,sv=ml[-1],sig[-1]
    return {"macd":round(mv,8),"signal":round(sv,8),"histogram":round(mv-sv,8),
            "trend":"bullish" if mv>sv else "bearish"}
def calc_bollinger(v,p=20):
    if len(v)<p: return None
    mid=sma(v,p); var=sum((x-mid)**2 for x in v[-p:])/p; std=math.sqrt(var)
    return {"upper":round(mid+2*std,8),"middle":round(mid,8),"lower":round(mid-2*std,8)}
def calc_atr(data,p=14):
    if len(data)<p+1: return None
    h,l,c=highs(data),lows(data),closes(data)
    trs=[max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])) for i in range(1,len(c))]
    return sum(trs[-p:])/p if len(trs)>=p else None
def calc_adx(data,p=14):
    if len(data)<p*3: return None
    h,l,c=highs(data),lows(data),closes(data)
    pr,mr,tr=[],[],[]
    for i in range(1,len(c)):
        up,dn=h[i]-h[i-1],l[i-1]-l[i]
        pr.append(up if (up>dn and up>0) else 0.0)
        mr.append(dn if (dn>up and dn>0) else 0.0)
        tr.append(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
    if len(tr)<p*2: return None
    ats,pds,mds=sum(tr[:p]),sum(pr[:p]),sum(mr[:p])
    pdis,mdis,dxs=[],[],[]
    for i in range(p,len(tr)):
        ats=ats-ats/p+tr[i]; pds=pds-pds/p+pr[i]; mds=mds-mds/p+mr[i]
        if ats==0: pdis.append(0); mdis.append(0); dxs.append(0); continue
        pdi=pds/ats*100; mdi=mds/ats*100
        pdis.append(pdi); mdis.append(mdi)
        ds=pdi+mdi; dxs.append(abs(pdi-mdi)/ds*100 if ds else 0)
    if len(dxs)<p: return None
    adx=sum(dxs[:p])/p
    for i in range(p,len(dxs)): adx=((adx*(p-1))+dxs[i])/p
    cpdi=pdis[-1] if pdis else 0; cmdi=mdis[-1] if mdis else 0
    return {"adx":round(adx,2),"plus_di":round(cpdi,2),"minus_di":round(cmdi,2),
            "trend":"bullish" if cpdi>cmdi else ("bearish" if cmdi>cpdi else "neutral"),
            "strong":adx>CFG.adx_strong,"weak":adx<CFG.adx_threshold}
def calc_ema_cross(v,fp=50,sp=200):
    if len(v)<sp+2: return None
    ef,es=ema_series(v,fp),ema_series(v,sp)
    if len(ef)<2 or len(es)<2: return None
    fc,fprev,sc,sprev=ef[-1],ef[-2],es[-1],es[-2]
    cross="NONE"
    if fprev<=sprev and fc>sc: cross="GOLDEN_CROSS"
    elif fprev>=sprev and fc<sc: cross="DEATH_CROSS"
    align="BULLISH_ALIGNMENT" if fc>sc else "BEARISH_ALIGNMENT"
    return {"cross":cross,"alignment":align,"is_fresh_cross":cross!="NONE"}
def calc_volume_ratio(data,p=20):
    if len(data)<p+1: return None
    v=volumes(data); avg=sum(v[-p-1:-1])/p
    return round(v[-1]/avg,4) if avg else None

def calculate_indicators(data):
    if len(data)<50: return None
    c=closes(data); price=c[-1]
    r={"price":price,"ema9":ema(c,9),"ema21":ema(c,21),"ema50":ema(c,50),
       "ema200":ema(c,200),"rsi":calc_rsi(c),"macd":calc_macd(c),
       "bollinger":calc_bollinger(c),"stochastic":calc_stochastic(data),
       "atr":calc_atr(data),"adx":calc_adx(data),"ema_cross":calc_ema_cross(c),
       "volume_ratio":calc_volume_ratio(data)}
    return r


# ================================================================
#  PULLBACK
# ================================================================
def detect_pullback(data, direction):
    result={"is_pullback":False,"type":"","reason":""}
    if len(data)<55: return result
    c=closes(data); h=highs(data); l=lows(data); price=c[-1]
    ema21=ema(c,21); ema50=ema(c,50)
    if not ema21 or not ema50: return result
    rl=min(l[-3:]); rh=max(h[-3:])
    bu=c[-1]>c[-2] and c[-1]>c[-3]; bd=c[-1]<c[-2] and c[-1]<c[-3]
    if direction=="BULLISH":
        if rl<=ema50*1.008 and bu and price>ema50:
            result={"is_pullback":True,"type":"EMA50_BOUNCE","reason":"EMA50 bounce"}
        elif rl<=ema21*1.008 and bu and price>ema21:
            result={"is_pullback":True,"type":"EMA21_BOUNCE","reason":"EMA21 bounce"}
    elif direction=="BEARISH":
        if rh>=ema50*0.992 and bd and price<ema50:
            result={"is_pullback":True,"type":"EMA50_REJECT","reason":"EMA50 reject"}
        elif rh>=ema21*0.992 and bd and price<ema21:
            result={"is_pullback":True,"type":"EMA21_REJECT","reason":"EMA21 reject"}
    return result


# ================================================================
#  TREND ENGINE
# ================================================================
def confirm_trend(i1m, i1h, i1d):
    result=TrendConfirmation(); reasons=[]; score=0
    if i1d:
        ec=i1d.get("ema_cross"); adx=i1d.get("adx")
        if ec:
            if ec["alignment"]=="BULLISH_ALIGNMENT": result.daily_trend="BULLISH"; score+=2; reasons.append("1D:EMA50>200")
            else: result.daily_trend="BEARISH"; score-=2; reasons.append("1D:EMA50<200")
            if ec["cross"]=="GOLDEN_CROSS": score+=1; reasons.append("1D:GoldenCross")
            elif ec["cross"]=="DEATH_CROSS": score-=1; reasons.append("1D:DeathCross")
        else:
            e50=i1d.get("ema50"); p=i1d.get("price",0)
            if e50 and p:
                if p>e50: result.daily_trend="BULLISH"; score+=1; reasons.append("1D:P>EMA50")
                else: result.daily_trend="BEARISH"; score-=1; reasons.append("1D:P<EMA50")
        if adx:
            if adx["weak"]: reasons.append(f"1D:ADX weak({adx['adx']})")
            elif adx["strong"]: reasons.append(f"1D:ADX strong({adx['adx']})")
    if i1h:
        e21=i1h.get("ema21"); e50=i1h.get("ema50"); p=i1h.get("price",0); adx=i1h.get("adx")
        if e21 and e50:
            if e21>e50 and p>e21: result.hourly_trend="BULLISH"; score+=2; reasons.append("1H:P>EMA21>50")
            elif e21<e50 and p<e21: result.hourly_trend="BEARISH"; score-=2; reasons.append("1H:P<EMA21<50")
            elif p>e21: result.hourly_trend="WEAK_BULLISH"; score+=1; reasons.append("1H:P>EMA21")
            elif p<e21: result.hourly_trend="WEAK_BEARISH"; score-=1; reasons.append("1H:P<EMA21")
            else: result.hourly_trend="MIXED"; reasons.append("1H:mixed")
        if adx:
            if adx["adx"]>=CFG.adx_threshold:
                if adx["trend"]=="bullish": score+=1; reasons.append(f"1H:+DI>-DI({adx['adx']})")
                elif adx["trend"]=="bearish": score-=1; reasons.append(f"1H:-DI>+DI({adx['adx']})")
            else: reasons.append(f"1H:ADX low({adx['adx']})")
    if i1m:
        rsi=i1m.get("rsi"); stoch=i1m.get("stochastic")
        if rsi is not None:
            if rsi<30: result.minute_timing="OVERSOLD"
            elif rsi>70: result.minute_timing="OVERBOUGHT"
            else: result.minute_timing="NEUTRAL"
        if stoch:
            if stoch["k"]<20 and stoch["k"]>stoch["d"]: reasons.append("1M:Stoch bull")
            elif stoch["k"]>80 and stoch["k"]<stoch["d"]: reasons.append("1M:Stoch bear")
    result.reasons=reasons
    db_=result.daily_trend=="BULLISH"; dbe=result.daily_trend=="BEARISH"
    hb=result.hourly_trend in ("BULLISH","WEAK_BULLISH"); hbe=result.hourly_trend in ("BEARISH","WEAK_BEARISH")
    if db_ and hbe:
        result.direction=TrendDirection.NEUTRAL; result.entry_quality=EntryQuality.CAUTION
        result.strength=20; reasons.append("1D bull+1H bear->caution")
    elif dbe and hb:
        result.direction=TrendDirection.NEUTRAL; result.entry_quality=EntryQuality.CAUTION
        result.strength=20; reasons.append("1D bear+1H bull->caution")
    elif score>=3: result.direction=TrendDirection.BULLISH; result.strength=min(score*15,100)
    elif score<=-3: result.direction=TrendDirection.BEARISH; result.strength=min(abs(score)*15,100)
    elif score>=1: result.direction=TrendDirection.BULLISH; result.strength=score*10; result.entry_quality=EntryQuality.CAUTION
    elif score<=-1: result.direction=TrendDirection.BEARISH; result.strength=abs(score)*10; result.entry_quality=EntryQuality.CAUTION
    else: result.direction=TrendDirection.NEUTRAL; result.strength=0; result.entry_quality=EntryQuality.BLOCKED; reasons.append("no direction")
    if result.direction in (TrendDirection.BULLISH,TrendDirection.BEARISH):
        if result.entry_quality!=EntryQuality.CAUTION:
            w1=i1d and i1d.get("adx",{}).get("weak",False); w2=i1h and i1h.get("adx",{}).get("weak",False)
            if w1 and w2: result.entry_quality=EntryQuality.CAUTION
            else: result.entry_quality=EntryQuality.ALLOWED
    return result


# ================================================================
#  SIGNAL ENGINE (45%)
# ================================================================
class SignalEngine:
    MAX_SCORE=12
    def evaluate(self, symbol, trend, i1m, i1h, i1d, data_1h=None):
        r=SignalResult(); r.max_score=self.MAX_SCORE; bs,ss=0,0; sigs=[]; reasons=[]
        if trend.entry_quality==EntryQuality.BLOCKED:
            r.decision=Decision.WAIT; r.reasons=["BLOCKED"]+trend.reasons; return r
        if trend.direction==TrendDirection.NEUTRAL:
            if trend.entry_quality==EntryQuality.CAUTION: reasons.append("caution")
            else: r.decision=Decision.WAIT; r.reasons=["neutral"]+trend.reasons; return r
        if trend.direction==TrendDirection.BULLISH: bs+=2; sigs.append("TREND_B")
        elif trend.direction==TrendDirection.BEARISH: ss+=2; sigs.append("TREND_S")
        elif trend.direction==TrendDirection.NEUTRAL:
            if trend.hourly_trend in ("BULLISH","WEAK_BULLISH"): bs+=1
            elif trend.hourly_trend in ("BEARISH","WEAK_BEARISH"): ss+=1
        if i1h and i1h.get("adx"):
            a=i1h["adx"]
            if a["adx"]>=CFG.adx_threshold:
                if a["trend"]=="bullish": bs+=2; sigs.append(f"ADX_{a['adx']}")
                elif a["trend"]=="bearish": ss+=2; sigs.append(f"ADX_{a['adx']}")
        if i1h and i1h.get("macd"):
            if i1h["macd"]["trend"]=="bullish": bs+=1; sigs.append("MACD_B")
            elif i1h["macd"]["trend"]=="bearish": ss+=1; sigs.append("MACD_S")
        if i1h and i1h.get("volume_ratio"):
            vr=i1h["volume_ratio"]
            if vr and vr>1.2:
                if trend.direction==TrendDirection.BULLISH or trend.hourly_trend in ("BULLISH","WEAK_BULLISH"): bs+=1
                elif trend.direction==TrendDirection.BEARISH or trend.hourly_trend in ("BEARISH","WEAK_BEARISH"): ss+=1
        if i1h and i1h.get("rsi") is not None:
            rsi=i1h["rsi"]
            ib=trend.direction==TrendDirection.BULLISH or trend.hourly_trend in ("BULLISH","WEAK_BULLISH")
            ise=trend.direction==TrendDirection.BEARISH or trend.hourly_trend in ("BEARISH","WEAK_BEARISH")
            if ib and 40<=rsi<=68: bs+=1; sigs.append(f"RSI_{rsi:.0f}")
            elif ise and 32<=rsi<=60: ss+=1; sigs.append(f"RSI_{rsi:.0f}")
        if i1h:
            p,e21=i1h.get("price",0),i1h.get("ema21")
            if e21 and p:
                if p>e21: bs+=1; sigs.append("P>EMA21")
                elif p<e21: ss+=1; sigs.append("P<EMA21")
        if i1h and i1h.get("stochastic"):
            st=i1h["stochastic"]
            if st["k"]>st["d"]: bs+=1; sigs.append("STOCH_K>D")
            elif st["k"]<st["d"]: ss+=1; sigs.append("STOCH_K<D")
        if i1h and i1h.get("ema_cross"):
            ec=i1h["ema_cross"]
            if ec["is_fresh_cross"]:
                if ec["cross"]=="GOLDEN_CROSS": bs+=1; sigs.append("GOLDEN")
                elif ec["cross"]=="DEATH_CROSS": ss+=1; sigs.append("DEATH")
            elif ec["alignment"]=="BULLISH_ALIGNMENT": bs+=1
            elif ec["alignment"]=="BEARISH_ALIGNMENT": ss+=1
        if data_1h and len(data_1h)>=55:
            dpb=trend.direction.value
            if dpb=="NEUTRAL":
                if trend.hourly_trend in ("BULLISH","WEAK_BULLISH"): dpb="BULLISH"
                elif trend.hourly_trend in ("BEARISH","WEAK_BEARISH"): dpb="BEARISH"
            pb=detect_pullback(data_1h,dpb)
            if pb["is_pullback"]:
                r.is_pullback=True
                if dpb=="BULLISH": bs+=2; sigs.append(f"PB_{pb['type']}")
                elif dpb=="BEARISH": ss+=2; sigs.append(f"PB_{pb['type']}")
        # Filters
        if i1h and i1h.get("rsi") is not None:
            rsi=i1h["rsi"]; sk=i1h.get("stochastic",{}).get("k",50) if i1h.get("stochastic") else 50
            av=i1h.get("adx",{}).get("adx",0) if i1h.get("adx") else 0
            if rsi>=CFG.rsi_extreme_overbought: bs=max(0,bs-4)
            elif rsi>=CFG.rsi_caution_overbought:
                if not (av>=CFG.adx_strong and sk>50): bs=max(0,bs-1)
            if rsi<=CFG.rsi_extreme_oversold: ss=max(0,ss-4)
            elif rsi<=CFG.rsi_caution_oversold:
                if not (av>=CFG.adx_strong and sk<50): ss=max(0,ss-1)
        r.buy_score=min(bs,self.MAX_SCORE); r.sell_score=min(ss,self.MAX_SCORE)
        r.signals=sigs; r.reasons=reasons+trend.reasons
        ibe=trend.direction==TrendDirection.BULLISH or (trend.direction==TrendDirection.NEUTRAL and trend.hourly_trend in ("BULLISH","WEAK_BULLISH"))
        ise=trend.direction==TrendDirection.BEARISH or (trend.direction==TrendDirection.NEUTRAL and trend.hourly_trend in ("BEARISH","WEAK_BEARISH"))
        if ibe:
            if bs>=CFG.min_score_to_enter: r.decision=Decision.BUY; self._sltp(r,i1h)
            else: r.decision=Decision.WAIT
        elif ise:
            if ss>=CFG.min_score_to_enter: r.decision=Decision.SELL; self._sltp(r,i1h)
            else: r.decision=Decision.WAIT
        else: r.decision=Decision.WAIT
        return r
    def _sltp(self,r,i1h):
        if i1h and i1h.get("atr") and i1h.get("price"):
            ap=(i1h["atr"]/i1h["price"])*100
            r.sl_percent=max(0.5,min(ap*1.5,CFG.max_sl_percent)); r.tp_percent=max(1.0,min(ap*3.0,CFG.max_tp_percent))
        else: r.sl_percent=2.0; r.tp_percent=4.0

signal_engine=SignalEngine()


# ================================================================
#  ✅ AI ANALYST (55% من القرار) - يقرأ المؤشرات فقط
# ================================================================
class AIAnalyst:
    def analyze(self, symbol, i1h, i1d, trend) -> AIResult:
        """AI يقرأ كل المؤشرات ويعطي قراره المستقل"""
        result = AIResult()

        # بناء وصف شامل للمؤشرات
        ind_summary = []
        if i1h:
            if i1h.get("rsi") is not None: ind_summary.append(f"RSI_1H={i1h['rsi']:.1f}")
            if i1h.get("adx"): ind_summary.append(f"ADX_1H={i1h['adx']['adx']} +DI={i1h['adx']['plus_di']} -DI={i1h['adx']['minus_di']}")
            if i1h.get("macd"): ind_summary.append(f"MACD_1H={i1h['macd']['trend']} hist={i1h['macd']['histogram']}")
            if i1h.get("stochastic"): ind_summary.append(f"Stoch_1H K={i1h['stochastic']['k']} D={i1h['stochastic']['d']}")
            if i1h.get("ema21"): ind_summary.append(f"EMA21_1H={i1h['ema21']:.6f}")
            if i1h.get("ema50"): ind_summary.append(f"EMA50_1H={i1h['ema50']:.6f}")
            if i1h.get("ema200"): ind_summary.append(f"EMA200_1H={i1h['ema200']:.6f}")
            if i1h.get("price"): ind_summary.append(f"Price={i1h['price']}")
            if i1h.get("volume_ratio"): ind_summary.append(f"VolRatio={i1h['volume_ratio']}")
            if i1h.get("atr"): ind_summary.append(f"ATR={i1h['atr']:.6f}")
            if i1h.get("ema_cross"): ind_summary.append(f"EMACross_1H={i1h['ema_cross']['alignment']}")
            if i1h.get("bollinger"): ind_summary.append(f"BB_1H upper={i1h['bollinger']['upper']} lower={i1h['bollinger']['lower']}")
        if i1d:
            if i1d.get("ema_cross"): ind_summary.append(f"EMACross_1D={i1d['ema_cross']['alignment']} cross={i1d['ema_cross']['cross']}")
            if i1d.get("adx"): ind_summary.append(f"ADX_1D={i1d['adx']['adx']}")
            if i1d.get("rsi") is not None: ind_summary.append(f"RSI_1D={i1d['rsi']:.1f}")

        trend_info = f"1D={trend.daily_trend} 1H={trend.hourly_trend} DIR={trend.direction.value} STR={trend.strength}%"

        prompt = f"""أنت محلل تداول محترف. اقرأ المؤشرات التالية وأعطِ قرارك.

العملة: {symbol}
الاتجاه: {trend_info}

المؤشرات:
{chr(10).join(ind_summary)}

القواعد:
1. اقرأ المؤشرات فقط. لا تخترع بيانات.
2. إذا كانت المؤشرات تدعم الشراء بقوة: BUY
3. إذا كانت تدعم البيع بقوة: SELL
4. إذا كانت غير واضحة أو متضاربة: WAIT
5. confidence = مدى ثقتك بالقرار (0-100)

أجب JSON فقط بدون أي نص آخر:
{{"decision":"BUY أو SELL أو WAIT","confidence":75,"regime":"trending أو ranging أو volatile","explanation":"شرح مختصر بالعربية","risk_warnings":["تحذير1"]}}"""

        try:
            comp = ai_client.chat.completions.create(
                model=CFG.ai_model,
                messages=[{"role":"user","content":prompt}],
                temperature=0, max_tokens=400, stream=False)
            raw = comp.choices[0].message.content or ""
            cleaned = raw.strip()
            if cleaned.startswith("```"):
                lines = [l for l in cleaned.split("\n") if not l.strip().startswith("```")]
                cleaned = "\n".join(lines).strip()
            dj = json.loads(cleaned)
            result.decision = str(dj.get("decision","WAIT")).upper()
            if result.decision not in ("BUY","SELL","WAIT"): result.decision = "WAIT"
            result.confidence = max(0, min(100, float(dj.get("confidence",0))))
            result.explanation = str(dj.get("explanation",""))
            result.risk_warnings = dj.get("risk_warnings",[])
            result.regime = str(dj.get("regime","unknown"))
            logger.info(f"🤖 AI {symbol}: {result.decision} | Conf={result.confidence} | {result.regime} | {result.explanation[:80]}")
        except Exception as e:
            logger.warning(f"AI fail {symbol}: {e}")
            result.decision = "WAIT"; result.confidence = 0
        return result


ai_analyst = AIAnalyst()


# ================================================================
#  ✅ FINAL DECISION = 55% AI + 45% Signal
# ================================================================
def merge_decision(signal: SignalResult, ai: AIResult, trend: TrendConfirmation) -> FinalDecision:
    final = FinalDecision()
    final.is_pullback = signal.is_pullback
    final.signals = signal.signals
    final.sl_percent = signal.sl_percent
    final.tp_percent = signal.tp_percent

    # Signal strength (0-100)
    sig_dir = "WAIT"
    sig_strength = 0
    if signal.decision == Decision.BUY:
        sig_dir = "BUY"; sig_strength = (signal.buy_score / signal.max_score) * 100
    elif signal.decision == Decision.SELL:
        sig_dir = "SELL"; sig_strength = (signal.sell_score / signal.max_score) * 100

    # AI strength
    ai_dir = ai.decision
    ai_strength = ai.confidence

    final.signal_score = sig_strength
    final.ai_score = ai_strength
    final.ai_explanation = ai.explanation
    final.ai_regime = ai.regime

    # ✅ إذا AI يقول WAIT بثقة عالية → WAIT (AI له 55%)
    if ai_dir == "WAIT" and ai_strength >= 60:
        final.decision = Decision.WAIT
        final.final_score = 0
        final.reasons = [f"AI WAIT (conf={ai_strength})"] + signal.reasons
        return final

    # ✅ إذا AI و Signal متفقان
    if ai_dir == sig_dir and ai_dir != "WAIT":
        final.final_score = CFG.ai_weight * ai_strength + CFG.signal_weight * sig_strength
        final.decision = Decision.BUY if ai_dir == "BUY" else Decision.SELL
        final.reasons = [f"AGREED {ai_dir} | AI={ai_strength} SIG={sig_strength:.0f}"] + signal.reasons
        return final

    # ✅ إذا مختلفان → AI يقرر (55%)
    if ai_dir in ("BUY","SELL") and ai_strength >= 55:
        final.final_score = CFG.ai_weight * ai_strength + CFG.signal_weight * sig_strength
        final.decision = Decision.BUY if ai_dir == "BUY" else Decision.SELL
        final.reasons = [f"AI OVERRIDE {ai_dir} | AI={ai_strength} SIG_dir={sig_dir} SIG={sig_strength:.0f}"] + signal.reasons
        return final

    # ✅ إذا Signal قوي لكن AI غير واثق
    if sig_dir in ("BUY","SELL") and sig_strength >= 70 and ai_strength < 40:
        final.final_score = CFG.ai_weight * ai_strength + CFG.signal_weight * sig_strength
        final.decision = Decision.BUY if sig_dir == "BUY" else Decision.SELL
        final.reasons = [f"SIGNAL STRONG {sig_dir} | SIG={sig_strength:.0f} AI_low={ai_strength}"] + signal.reasons
        return final

    # لا اتفاق كافٍ
    final.decision = Decision.WAIT
    final.final_score = CFG.ai_weight * ai_strength + CFG.signal_weight * sig_strength
    final.reasons = [f"NO CONSENSUS | AI={ai_dir}({ai_strength}) SIG={sig_dir}({sig_strength:.0f})"] + signal.reasons
    return final


# ================================================================
#  SCANNER
# ================================================================
class MarketScanner:
    def __init__(self): self._run=True
    def start(self): threading.Thread(target=self._loop,daemon=True).start(); logger.info("Scanner LIVE")
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
            logger.info(f"  #{i+1} {c.symbol} | Score={c.score:.1f} | Vol={c.volume_usdt/1e6:.1f}M | ATR={c.atr_pct:.2f}% | {c.trend_1h} | RSI={c.rsi_1h:.1f}")
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
            reasons.append(f"Vol={vol/1e6:.1f}M")
        except: return None
        try:
            ohlcv=exchange_public.fetch_ohlcv(sym,"1h",limit=50)
            if len(ohlcv)<20: return None
            atr=calc_atr(ohlcv); price=float(ohlcv[-1][4])
            if atr and price:
                ap=(atr/price)*100; result.atr_pct=ap
                if ap<CFG.scanner_min_atr_pct: return None
                reasons.append(f"ATR={ap:.2f}%")
        except: return None
        try:
            c=closes(ohlcv); e21=ema(c,21); e50=ema(c,50)
            if e21 and e50:
                if e21>e50 and price>e21: result.trend_1h="BULLISH"; result.score+=3; reasons.append("BULL")
                elif e21<e50 and price<e21: result.trend_1h="BEARISH"; result.score+=3; reasons.append("BEAR")
                else: result.trend_1h="MIXED"; result.score+=1
        except: pass
        try:
            rsi=calc_rsi(c)
            if rsi is not None:
                result.rsi_1h=rsi
                if rsi>CFG.rsi_extreme_overbought or rsi<CFG.rsi_extreme_oversold: return None
                elif 40<=rsi<=68: result.score+=2
                elif rsi>CFG.rsi_caution_overbought: result.score+=1
        except: pass
        if result.volume_usdt>50_000_000: result.score+=1
        result.reasons=reasons; return result
    def _deep(self,sk,sym):
        logger.info(f"Deep: {sym}")
        if cm.count(sk,"1h")<50: self._load(sk,sym)
        d1m=cm.get(sk,"1m"); d1h=cm.get(sk,"1h"); d1d=cm.get(sk,"1d")
        if len(d1h)<50 or len(d1d)<50: logger.info(f"Not enough: {sym}"); return
        i1m=calculate_indicators(d1m) if len(d1m)>=50 else None
        i1h=calculate_indicators(d1h); i1d=calculate_indicators(d1d)
        if not i1h or not i1d: return
        trend=confirm_trend(i1m,i1h,i1d)
        logger.info(f">>> {sym} | 1D={trend.daily_trend} | 1H={trend.hourly_trend} | DIR={trend.direction.value} | QUALITY={trend.entry_quality.value} | STR={trend.strength}%")
        for reason in trend.reasons: logger.info(f"   - {reason}")

        # Signal Engine (45%)
        signal=signal_engine.evaluate(sym,trend,i1m,i1h,i1d,d1h)
        logger.info(f"<<< SIGNAL {sym}: {signal.decision.value} | B={signal.buy_score} S={signal.sell_score}/{signal.max_score} | PB={signal.is_pullback} | {signal.signals}")

        # ✅ AI يقرأ المؤشرات (55%)
        ai=ai_analyst.analyze(sym,i1h,i1d,trend)

        # ✅ الدمج: 55% AI + 45% Signal
        final=merge_decision(signal,ai,trend)

        logger.info(f"🎯 FINAL {sym}: {final.decision.value} | FinalScore={final.final_score:.1f} | AI={ai.decision}({ai.confidence}) SIG={signal.decision.value}(B:{signal.buy_score}/S:{signal.sell_score}) | {final.reasons[0] if final.reasons else ''}")

        bot_stats["last_analysis"][sym]={
            "decision":final.decision.value,"final_score":round(final.final_score,1),
            "ai":f"{ai.decision}({ai.confidence})","signal":f"B:{signal.buy_score}/S:{signal.sell_score}",
            "regime":ai.regime,"time":datetime.now(timezone.utc).isoformat()}

        if final.decision==Decision.WAIT: return
        if final.final_score<CFG.min_final_score:
            logger.info(f"Score too low: {final.final_score:.1f} < {CFG.min_final_score}"); return

        execute_trade(sym,final)

    def _load(self,sk,sym):
        for tf in CFG.timeframes:
            try:
                limit=500 if tf=="1d" else 300
                data=exchange_public.fetch_ohlcv(sym,timeframe=tf,limit=limit)
                cm.load(sk,tf,data)
            except Exception as e: logger.warning(f"Load {sym} {tf}: {e}")
            time.sleep(0.3)


# ================================================================
#  EXECUTION (LIVE)
# ================================================================
def get_pos(sym):
    try:
        for p in exchange.fetch_positions([sym]):
            ct=p.get("contracts")
            if ct and float(ct)>0: return p
        return None
    except Exception as e: logger.error(f"Pos {sym}: {e}"); return "ERROR"

def emergency_close(sym,reason):
    logger.critical(f"🚨 EMERGENCY: {sym} | {reason}")
    try:
        pos=get_pos(sym)
        if pos and pos!="ERROR":
            ct=float(pos.get("contracts",0)); side=pos.get("side","")
            if ct>0:
                cs="sell" if side=="long" else "buy"
                exchange.create_market_order(sym,cs,ct,params={"reduceOnly":True})
    except Exception as e: logger.critical(f"Emergency fail: {e}")

def execute_trade(sym, final: FinalDecision):
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

            logger.info(f"🚀 LIVE TRADE {sym} | {pname} | {price} | FinalScore={final.final_score:.1f} | AI={final.ai_explanation[:60]}")

            exchange.set_leverage(CFG.leverage,sym)
            order=exchange.create_market_order(sym,side,qty)
            eoid=order.get("id",""); time.sleep(1)
            p=get_pos(sym)
            if p=="ERROR" or p is None: logger.critical(f"No pos: {sym}"); return
            entry=float(p.get("entryPrice",price)); aqty=abs(float(p.get("contracts",0)))
            if aqty<=0: emergency_close(sym,"zero qty"); return
            logger.info(f"📦 qty={aqty} entry={entry}")

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
                sloid=slo.get("id",""); logger.info(f"✅ SL: {sl_price}")
            except Exception as e:
                logger.critical(f"🚨 SL fail: {e}"); emergency_close(sym,"SL fail"); return

            tpoid=""
            try:
                tpo=exchange.create_order(sym,"TAKE_PROFIT_MARKET",cs,aqty,None,
                    {"stopPrice":tp_price,"reduceOnly":True,"workingType":"MARK_PRICE"})
                tpoid=tpo.get("id",""); logger.info(f"✅ TP: {tp_price}")
            except Exception as e:
                logger.error(f"TP fail: {e}")
                try: exchange.cancel_order(sloid,sym)
                except: pass
                emergency_close(sym,"TP fail"); return

            tid=db.insert_trade(symbol=sym,side=pname,mode="LIVE",entry_price=entry,
                quantity=aqty,sl_price=sl_price,tp_price=tp_price,
                sl_order_id=sloid,tp_order_id=tpoid,entry_order_id=eoid,
                confidence=final.final_score,
                reason=f"AI={final.ai_score:.0f} SIG={final.signal_score:.0f} Final={final.final_score:.1f} | {final.ai_explanation[:100]}",
                timestamp=datetime.now(timezone.utc).isoformat(),
                ai_explanation=final.ai_explanation, final_score=final.final_score)
            logger.info(f"💾 Trade #{tid}"); st["t"]=time.time()
        except Exception as e:
            logger.error(f"Exec: {e}",exc_info=True); emergency_close(sym,str(e))


# ================================================================
#  POSITION MONITOR
# ================================================================
class PositionMonitor:
    def __init__(self): self._run=True
    def start(self): threading.Thread(target=self._loop,daemon=True).start(); logger.info("Monitor LIVE")
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
            logger.info(f"📊 CLOSED {sym} | {reason} | PnL={rpnl:.4f} ({pp:.2f}%) | Comm={comm:.4f}")
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
    def _cancel(self,sym,trade):
        for oid in [trade.get("sl_order_id"),trade.get("tp_order_id")]:
            if not oid: continue
            try:
                if self._ost(sym,oid)=="open": exchange.cancel_order(oid,sym)
            except: pass


# ================================================================
#  WEBSOCKET
# ================================================================
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
            async with websockets.connect(url,ping_interval=CFG.ws_ping_interval,ping_timeout=CFG.ws_ping_timeout) as ws:
                logger.info(f"WS connected ({len(current)})"); delay=CFG.ws_reconnect_delay
                async for msg in ws:
                    data=json.loads(msg); k=data.get("data",{}).get("k")
                    if not k: continue
                    sk=k["s"].lower(); tf=k["i"]
                    candle=[k["t"],float(k["o"]),float(k["h"]),float(k["l"]),float(k["c"]),float(k["v"])]
                    cm.update(sk,tf,candle,k["x"])
        except Exception as e: logger.error(f"WS: {e}")
        await asyncio.sleep(delay); delay=min(delay*2,120)


# ================================================================
#  MAIN
# ================================================================
def main():
    # ✅ أول شيء: عرض IP
    ip = show_deploy_ip()

    logger.info("="*60)
    logger.info("🤖 AI TRADING BOT V8 — LIVE MODE")
    logger.info(f"   ⚠️  MODE: 💰 LIVE (صفقات حقيقية)")
    logger.info(f"   🌐 IP: {ip}")
    logger.info(f"   🤖 AI Weight: {CFG.ai_weight*100:.0f}% | Signal Weight: {CFG.signal_weight*100:.0f}%")
    logger.info(f"   Min Final Score: {CFG.min_final_score}")
    logger.info(f"   Watchlist: {len(CFG.watchlist)} | Scanner: {CFG.scanner_interval}s -> Top {CFG.scanner_top_n}")
    logger.info(f"   Leverage: x{CFG.leverage} | Margin: {CFG.margin_usdt} USDT")
    logger.info(f"   MaxOpen: {CFG.max_open_positions} | MaxDaily: {CFG.max_daily_trades}")
    logger.info("="*60)

    ip_monitor.start()
    threading.Thread(target=run_server,daemon=True).start(); time.sleep(2)

    try:
        t=exchange_public.fetch_ticker("BTC/USDT:USDT")
        logger.info(f"✅ Binance OK | BTC: {t['last']}")
    except Exception as e: logger.critical(f"❌ Binance: {e}"); return

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
    logger.info("✅ Data ready")

    monitor=PositionMonitor(); monitor.start()
    scanner=MarketScanner(); scanner.start()
    bot_stats["status"]="RUNNING"

    try: asyncio.run(ws_worker())
    except KeyboardInterrupt: logger.info("Shutdown"); scanner.stop(); monitor.stop(); ip_monitor.stop()

if __name__=="__main__":
    main()
