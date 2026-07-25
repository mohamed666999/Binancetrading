import asyncio
import json
import time
import threading
import websockets
import ccxt
import requests
import math

from flask import Flask
from openai import OpenAI


# =====================================================
# 1. KEEP ALIVE SERVER
# =====================================================

app = Flask(__name__)

@app.route("/")
def home():
    return "AI TRADING BOT IS LIVE"


def run_server():
    app.run(
        host="0.0.0.0",
        port=8080,
        debug=False,
        use_reloader=False
    )


# =====================================================
# 2. BINANCE API
# =====================================================

BINANCE_API_KEY = "IX7kLH0ssWHP5TpYMUGcp0pzq4LX4Lqi7m4XtlqMkkq6DCZAsLhoeYZ3533jJFF4"
BINANCE_SECRET = "LmICnpSpMxL1riv4RfIf0HBGRfhDTP5JhDUYdlPSukpqV7kDTonrZ0j3DWp1a7hU"


exchange = ccxt.binance({
    "apiKey": BINANCE_API_KEY,
    "secret": BINANCE_SECRET,
    "enableRateLimit": True,
    "options": {
        "defaultType": "swap",
        "adjustForTimeDifference": True
    }
})


# =====================================================
# 3. DEEPSEEK / NVIDIA API
# =====================================================

client = OpenAI(
    base_url="https://integrate.api.nvidia.com/v1",
    api_key="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"
)


# =====================================================
# 4. العملات (تم استبدالها بعملات سريعة التقلب)
# =====================================================

SYMBOLS = {
    "wifusdt": "WIF/USDT:USDT",
    "1000pepeusdt": "1000PEPE/USDT:USDT",
    "dogeusdt": "DOGE/USDT:USDT"
}


# =====================================================
# 5. تخزين الشموع القادمة من WebSocket
# =====================================================

candles = {
    "wifusdt": {
        "1m": [],
        "1h": [],
        "1d": []
    },
    "1000pepeusdt": {
        "1m": [],
        "1h": [],
        "1d": []
    },
    "dogeusdt": {
        "1m": [],
        "1h": [],
        "1d": []
    }
}


# =====================================================
# TECHNICAL INDICATORS
# =====================================================

def closes(data):
    return [float(x[4]) for x in data]


def highs(data):
    return [float(x[2]) for x in data]


def lows(data):
    return [float(x[3]) for x in data]


def volumes(data):
    return [float(x[5]) for x in data]


def ema(values, period):
    if len(values) < period:
        return None
    multiplier = 2 / (period + 1)
    result = sum(values[:period]) / period
    for price in values[period:]:
        result = (price - result) * multiplier + result
    return result


def sma(values, period):
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def calculate_rsi(values, period=14):
    if len(values) < period + 1:
        return None
    gains = []
    losses = []
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_macd(values):
    """حساب MACD line + Signal line + Histogram"""
    if len(values) < 35:
        return None

    # EMA12 و EMA26 لكل النقاط
    ema12_vals = []
    ema26_vals = []
    multiplier_12 = 2 / (12 + 1)
    multiplier_26 = 2 / (26 + 1)

    ema12 = sum(values[:12]) / 12
    ema26 = sum(values[:26]) / 26
    ema12_vals.append(ema12)
    ema26_vals.append(ema26)

    for price in values[12:]:
        ema12 = (price - ema12) * multiplier_12 + ema12
        ema12_vals.append(ema12)

    for price in values[26:]:
        ema26 = (price - ema26) * multiplier_26 + ema26
        ema26_vals.append(ema26)

    # محاذاة الأطوال
    min_len = min(len(ema12_vals), len(ema26_vals))
    macd_line = [ema12_vals[i] - ema26_vals[i] for i in range(min_len)]

    if len(macd_line) < 9:
        return None

    signal_line = ema(macd_line, 9)
    if signal_line is None:
        return None

    histogram = macd_line[-1] - signal_line
    trend = "bullish" if macd_line[-1] > signal_line else "bearish"

    return {
        "macd": macd_line[-1],
        "signal": signal_line,
        "histogram": histogram,
        "trend": trend
    }


def calculate_bollinger(values, period=20):
    if len(values) < period:
        return None
    middle = sma(values, period)
    recent = values[-period:]
    variance = sum((x - middle) ** 2 for x in recent) / period
    std = math.sqrt(variance)
    upper = middle + (2 * std)
    lower = middle - (2 * std)
    return {"upper": upper, "middle": middle, "lower": lower}


def calculate_atr(data, period=14):
    if len(data) < period + 1:
        return None
    true_ranges = []
    for i in range(1, len(data)):
        high = float(data[i][2])
        low = float(data[i][3])
        previous_close = float(data[i - 1][4])
        tr = max(high - low, abs(high - previous_close), abs(low - previous_close))
        true_ranges.append(tr)
    return sum(true_ranges[-period:]) / period


def calculate_adx(data, period=14):
    if len(data) < period * 2:
        return None
    plus_dm = []
    minus_dm = []
    tr_values = []
    for i in range(1, len(data)):
        high = float(data[i][2])
        low = float(data[i][3])
        previous_high = float(data[i - 1][2])
        previous_low = float(data[i - 1][3])
        previous_close = float(data[i - 1][4])
        up_move = high - previous_high
        down_move = previous_low - low
        if up_move > down_move and up_move > 0:
            plus = up_move
        else:
            plus = 0
        if down_move > up_move and down_move > 0:
            minus = down_move
        else:
            minus = 0
        tr = max(high - low, abs(high - previous_close), abs(low - previous_close))
        plus_dm.append(plus)
        minus_dm.append(minus)
        tr_values.append(tr)
    atr = sum(tr_values[-period:]) / period
    if atr == 0:
        return None
    plus_di = (sum(plus_dm[-period:]) / period) / atr * 100
    minus_di = (sum(minus_dm[-period:]) / period) / atr * 100
    if plus_di + minus_di == 0:
        return 0
    dx = abs(plus_di - minus_di) / (plus_di + minus_di) * 100
    return dx


def calculate_volume_ratio(data, period=20):
    if len(data) < period + 1:
        return None
    current_volume = float(data[-1][5])
    previous_volumes = [float(x[5]) for x in data[-period - 1:-1]]
    average_volume = sum(previous_volumes) / len(previous_volumes)
    if average_volume == 0:
        return None
    return current_volume / average_volume


# =====================================================
# BUILD TECHNICAL ANALYSIS
# =====================================================

def calculate_indicators(data):
    if len(data) < 50:
        return None
    price_data = closes(data)
    current_price = price_data[-1]
    ema9 = ema(price_data, 9)
    ema21 = ema(price_data, 21)
    ema50 = ema(price_data, 50)
    ema200 = ema(price_data, 200)
    rsi = calculate_rsi(price_data)
    macd = calculate_macd(price_data)  # الآن يعيد dict
    bollinger = calculate_bollinger(price_data)
    atr = calculate_atr(data)
    adx = calculate_adx(data)
    volume_ratio = calculate_volume_ratio(data)
    return {
        "price": current_price,
        "ema9": ema9,
        "ema21": ema21,
        "ema50": ema50,
        "ema200": ema200,
        "rsi": rsi,
        "macd": macd,
        "bollinger": bollinger,
        "atr": atr,
        "adx": adx,
        "volume_ratio": volume_ratio
    }


# =====================================================
# 6. منع تكرار الصفقات + إدارة مستقلة لكل عملة
# =====================================================

trade_state = {
    "WIF/USDT:USDT": {
        "last_decision": None,
        "ai_busy": False
    },
    "1000PEPE/USDT:USDT": {
        "last_decision": None,
        "ai_busy": False
    },
    "DOGE/USDT:USDT": {
        "last_decision": None,
        "ai_busy": False
    }
}


# =====================================================
# 7. اختبار Binance
# =====================================================

def test_binance():
    print("")
    print("=" * 60)
    print("🔌 [1] اختبار الاتصال بـ Binance")
    print("=" * 60)
    try:
        ticker = exchange.fetch_ticker("BTC/USDT:USDT")
        price = ticker["last"]
        print("✅ Binance متصل بنجاح")
        print(f"💰 سعر BTC الحالي: {price}")
        return True
    except Exception as e:
        print("❌ فشل الاتصال بـ Binance")
        print(e)
        return False


# =====================================================
# 8. الحصول على المركز الحالي
# =====================================================

def get_current_position(symbol):
    try:
        print(f"🔎 فحص المركز الحالي: {symbol}")
        positions = exchange.fetch_positions([symbol])
        for position in positions:
            contracts = position.get("contracts")
            if contracts and float(contracts) > 0:
                side = position.get("side")
                print(f"⚠️ يوجد مركز مفتوح بالفعل: {side} | {contracts}")
                return position
        print("✅ لا يوجد مركز مفتوح")
        return None
    except Exception as e:
        print("⚠️ تعذر فحص المركز")
        print(e)
        return None


# =====================================================
# MARKET DATA
# =====================================================

def get_market_data(symbol):
    result = {"funding_rate": None, "open_interest": None}
    try:
        funding = exchange.fetch_funding_rate(symbol)
        result["funding_rate"] = funding.get("fundingRate")
        print(f"💰 Funding {symbol}: {result['funding_rate']}")
    except Exception as e:
        print(f"⚠️ فشل Funding: {e}")
    try:
        oi = exchange.fetch_open_interest(symbol)
        result["open_interest"] = oi.get("openInterest")
        print(f"📊 Open Interest {symbol}: {result['open_interest']}")
    except Exception as e:
        print(f"⚠️ فشل Open Interest: {e}")
    return result


# =====================================================
# 9. تنفيذ الصفقة + وضع SL/TP فوراً (مع دقة الأسعار والكميات)
# =====================================================

def execute_trade(symbol, decision, sl_percent=2.0, tp_percent=4.0):
    print("")
    print("=" * 60)
    print("🚀 بدء تنفيذ الصفقة")
    print("=" * 60)
    try:
        current_position = get_current_position(symbol)
        if current_position:
            print("🛑 تم إلغاء الصفقة: يوجد مركز مفتوح بالفعل")
            return

        LEVERAGE = 10
        MARGIN_USDT = 10
        print(f"💵 الهامش: {MARGIN_USDT} USDT")
        print(f"⚡ الرافعة: x{LEVERAGE}")

        ticker = exchange.fetch_ticker(symbol)
        price = ticker["last"]
        notional = MARGIN_USDT * LEVERAGE
        raw_quantity = notional / price
        # ✅ استخدام دقة الكمية المناسبة
        quantity = float(exchange.amount_to_precision(symbol, raw_quantity))

        if decision == "BUY":
            side = "buy"
            position_name = "LONG"
        elif decision == "SELL":
            side = "sell"
            position_name = "SHORT"
        else:
            print("⏳ WAIT")
            return

        print(f"📊 الاتجاه: {position_name}")
        print(f"💰 السعر: {price}")
        print(f"📦 الكمية: {quantity}")

        print("⚙️ ضبط الرافعة...")
        exchange.set_leverage(LEVERAGE, symbol)

        print("🚀 إرسال أمر السوق إلى Binance...")
        order = exchange.create_market_order(symbol, side, quantity)

        print("")
        print("✅✅✅ تم فتح الصفقة بنجاح ✅✅✅")
        print(f"🆔 Order ID: {order.get('id')}")
        print(f"📌 الاتجاه: {position_name}")
        print(f"📌 العملة: {symbol}")

        # ✅ وضع SL و TP فوراً بعد فتح الصفقة
        time.sleep(1)
        position = get_current_position(symbol)
        if position:
            entry_price = float(position.get("entryPrice", price))
        else:
            entry_price = price

        sl_percent = max(0.5, min(sl_percent, 5.0))
        tp_percent = max(1.0, min(tp_percent, 10.0))

        if side == "buy":
            stop_loss = entry_price * (1 - sl_percent / 100)
            take_profit = entry_price * (1 + tp_percent / 100)
        else:
            stop_loss = entry_price * (1 + sl_percent / 100)
            take_profit = entry_price * (1 - tp_percent / 100)

        # ✅ استخدام دقة السعر المناسبة
        stop_loss = float(exchange.price_to_precision(symbol, stop_loss))
        take_profit = float(exchange.price_to_precision(symbol, take_profit))

        print(f"🛑 SL: {stop_loss} ({sl_percent}%)")
        print(f"🎯 TP: {take_profit} ({tp_percent}%)")

        try:
            exchange.create_order(
                symbol,
                "STOP_MARKET",
                "sell" if side == "buy" else "buy",
                quantity,
                None,
                {"stopPrice": stop_loss, "reduceOnly": True}
            )
            print("✅ SL موضوع بنجاح")
        except Exception as sl_err:
            print(f"⚠️ فشل وضع SL: {sl_err}")

        try:
            exchange.create_order(
                symbol,
                "TAKE_PROFIT_MARKET",
                "sell" if side == "buy" else "buy",
                quantity,
                None,
                {"stopPrice": take_profit, "reduceOnly": True}
            )
            print("✅ TP موضوع بنجاح")
        except Exception as tp_err:
            print(f"⚠️ فشل وضع TP: {tp_err}")

        # ✅ التأكد من وجود أوامر SL/TP على المنصة
        print("🛡️ فحص أوامر الحماية...")
        try:
            open_orders = exchange.fetch_open_orders(symbol)
            for o in open_orders:
                if o.get('reduceOnly'):
                    print(f"🛡️ Order: {o['type']} | Side: {o['side']} | Stop: {o.get('stopPrice')} | ID: {o['id']}")
        except Exception as e:
            print(f"⚠️ تعذر فحص أوامر SL/TP: {e}")

    except Exception as e:
        print("")
        print("❌❌❌ فشل تنفيذ الصفقة ❌❌❌")
        print(e)


# =====================================================
# AI ANALYSIS V3 (مع تحسينات MACD وفلتر الاتجاه)
# =====================================================

def analyze_with_ai(symbol):
    print("")
    print("=" * 60)
    print(f"🧠 بدء تحليل V3: {symbol}")
    print("=" * 60)

    market_data = None
    for key, value in SYMBOLS.items():
        if value == symbol:
            market_data = candles[key]
            break
    if not market_data:
        print("❌ لا توجد بيانات")
        return None, None, None

    one_minute = market_data["1m"]
    one_hour = market_data["1h"]
    one_day = market_data["1d"]

    if len(one_hour) < 50:
        print(f"⏳ شموع الساعة غير كافية: {len(one_hour)}/50")
        return None, None, None

    if len(one_day) < 50:  # ✅ تعديل الحد إلى 50 بدلاً من 5
        print(f"⏳ شموع اليوم غير كافية: {len(one_day)}/50")
        return None, None, None

    print("📊 حساب المؤشرات...")
    indicators_1m = calculate_indicators(one_minute)
    indicators_1h = calculate_indicators(one_hour)
    indicators_1d = calculate_indicators(one_day)

    if not indicators_1h or not indicators_1d:
        print("❌ فشل حساب مؤشرات 1H أو 1D")
        return None, None, None

    market_info = get_market_data(symbol)
    price = indicators_1h["price"]
    print(f"💰 السعر: {price}")
    print(f"📈 RSI 1H: {indicators_1h['rsi']}")
    print(f"📊 MACD 1H: {indicators_1h['macd']}")
    print(f"📉 ADX 1H: {indicators_1h['adx']}")
    print(f"📦 Volume Ratio: {indicators_1h['volume_ratio']}")

    # ✅ فلتر الاتجاه: إذا كان الإطار اليومي والساعة متناقضين → WAIT
    ema50_1d = indicators_1d.get("ema50")
    ema50_1h = indicators_1h.get("ema50")
    if ema50_1d is not None and ema50_1h is not None:
        daily_trend = "bullish" if indicators_1d["price"] > ema50_1d else "bearish"
        hourly_trend = "bullish" if indicators_1h["price"] > ema50_1h else "bearish"
        if daily_trend != hourly_trend:
            print(f"⏳ تناقض اتجاهي (1D: {daily_trend}, 1H: {hourly_trend}) - WAIT")
            return None, None, None

    prompt = f"""
أنت نظام تداول آلي احترافي.
مهمتك تحليل البيانات وليس اختراع معلومات.

العملة:
{symbol}
السعر الحالي:
{price}

====================
مؤشرات 1M
====================
{indicators_1m}

====================
مؤشرات 1H
====================
RSI: {indicators_1h['rsi']}
MACD: {indicators_1h['macd']}
EMA 9: {indicators_1h['ema9']}
EMA 21: {indicators_1h['ema21']}
EMA 50: {indicators_1h['ema50']}
EMA 200: {indicators_1h['ema200']}
Bollinger: {indicators_1h['bollinger']}
ATR: {indicators_1h['atr']}
ADX: {indicators_1h['adx']}
Volume Ratio: {indicators_1h['volume_ratio']}

====================
مؤشرات 1D
====================
{indicators_1d}

====================
بيانات السوق
====================
Funding Rate: {market_info['funding_rate']}
Open Interest: {market_info['open_interest']}

====================
قواعد القرار (مهم جداً):
- 1D يحدد الاتجاه العام، 1H يحدد الاتجاه الرئيسي، 1M للتوقيت فقط.
- لا تدخل BUY إذا كان 1D و 1H هابطين.
- لا تدخل SELL إذا كان 1D و 1H صاعدين.
- إذا كانت الأطر الزمنية متناقضة، أجب WAIT.
- لا تتداول لمجرد حركة صغيرة.

أجب JSON فقط:
{{
  "decision": "BUY",
  "stop_loss_percent": 2.0,
  "take_profit_percent": 4.0,
  "confidence": 75,
  "reason": "short reason"
}}
decision يجب أن يكون واحدًا فقط:
BUY
SELL
WAIT
stop_loss_percent: بين 0.5 و 5
take_profit_percent: بين 1 و 10
confidence: بين 0 و 100
لا تكتب أي شيء خارج JSON.
"""

    print("📤 إرسال المؤشرات إلى DeepSeek...")
    start_time = time.time()
    try:
        completion = client.chat.completions.create(
            model="deepseek-ai/deepseek-v4-pro",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=200,
            extra_body={"chat_template_kwargs": {"thinking": False}},
            stream=False
        )
        elapsed = time.time() - start_time
        response = completion.choices[0].message.content or ""
        print(f"⚡ زمن رد AI: {elapsed:.2f} ثانية")
        print("🤖 رد DeepSeek:")
        print(response)

        try:
            data = json.loads(response)
        except Exception:
            print("⚠️ AI لم يرسل JSON صحيح")
            return None, None, None

        decision = str(data.get("decision", "WAIT")).upper()
        sl_percent = float(data.get("stop_loss_percent", 2.0))
        tp_percent = float(data.get("take_profit_percent", 4.0))
        confidence = float(data.get("confidence", 0))

        sl_percent = max(0.5, min(sl_percent, 5.0))
        tp_percent = max(1.0, min(tp_percent, 10.0))
        confidence = max(0, min(confidence, 100))

        print("")
        print(f"🎯 القرار: {decision}")
        print(f"📊 الثقة: {confidence}%")
        print(f"🛑 SL: {sl_percent}%")
        print(f"🎯 TP: {tp_percent}%")

        MIN_CONFIDENCE = 65
        if decision in ["BUY", "SELL"]:
            if confidence < MIN_CONFIDENCE:
                print(f"⛔ الثقة منخفضة ({confidence}%)")
                return None, None, None

        if decision == "WAIT":
            print("⏳ WAIT")
            return None, None, None

        return decision, sl_percent, tp_percent

    except Exception as e:
        print("❌ خطأ DeepSeek:")
        print(e)
        return None, None, None


# =====================================================
# 11. تحليل آمن لكل عملة (مع فحص المركز قبل استدعاء AI)
# =====================================================

def analyze_with_ai_safe(symbol):
    state = trade_state.get(symbol)
    if not state:
        return
    if state["ai_busy"]:
        print(f"⏳ AI مازال يحلل {symbol}")
        return

    # ✅ فحص المركز الحالي قبل إرسال الطلب إلى AI
    position = get_current_position(symbol)
    if position:
        print(f"📌 {symbol} لديه صفقة مفتوحة - تخطي التحليل")
        return

    state["ai_busy"] = True
    try:
        decision, sl_percent, tp_percent = analyze_with_ai(symbol)
        if decision in ["BUY", "SELL"]:
            execute_trade(symbol, decision, sl_percent, tp_percent)
    finally:
        state["ai_busy"] = False


# =====================================================
# 12. Binance WebSocket (Futures)
# =====================================================

async def websocket_worker():
    streams = []
    for symbol in SYMBOLS.keys():
        streams.append(f"{symbol}@kline_1m")
        streams.append(f"{symbol}@kline_1h")
        streams.append(f"{symbol}@kline_1d")
    # ✅ استخدام fstream للعقود الآجلة
    stream_url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)

    print("")
    print("=" * 60)
    print("🔌 الاتصال بـ Binance Futures WebSocket")
    print("=" * 60)

    while True:
        try:
            async with websockets.connect(stream_url, ping_interval=20, ping_timeout=20) as websocket:
                print("✅ WebSocket متصل بنجاح")
                async for message in websocket:
                    data = json.loads(message)
                    payload = data.get("data", {})
                    kline = payload.get("k")
                    if not kline:
                        continue

                    symbol_key = kline["s"].lower()
                    interval = kline["i"]
                    is_closed = kline["x"]

                    candle = [
                        kline["t"],
                        float(kline["o"]),
                        float(kline["h"]),
                        float(kline["l"]),
                        float(kline["c"]),
                        float(kline["v"])
                    ]

                    # تحديث أو إضافة الشمعة (عدم تكرار نفس الشمعة)
                    existing = candles[symbol_key][interval]
                    if existing and existing[-1][0] == candle[0]:
                        existing[-1] = candle
                    else:
                        existing.append(candle)
                    if len(existing) > 300:
                        candles[symbol_key][interval] = existing[-300:]

                    print(f"📡 {symbol_key.upper()} {interval} السعر: {candle[4]}", end="\r")

                    if is_closed and interval == "1h":
                        symbol = SYMBOLS[symbol_key]
                        print("")
                        print(f"🕯️ شمعة ساعة جديدة أغلقت: {symbol}")
                        threading.Thread(target=analyze_with_ai_safe, args=(symbol,), daemon=True).start()

        except Exception as e:
            print("")
            print("❌ WebSocket انقطع")
            print(e)
            print("🔄 إعادة الاتصال بعد 10 ثوانٍ")
            await asyncio.sleep(10)


# =====================================================
# INITIAL CANDLE LOADING
# =====================================================

def load_initial_candles():
    print("")
    print("📥 تحميل الشموع الأولية...")
    for key, symbol in SYMBOLS.items():
        try:
            for timeframe in ["1m", "1h", "1d"]:
                limit = 250
                data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
                candles[key][timeframe] = data
                print(f"✅ {symbol} {timeframe}: {len(data)} شمعة")
        except Exception as e:
            print(f"❌ فشل تحميل {symbol}: {e}")


# =====================================================
# 13. التشغيل
# =====================================================

def start_websocket():
    asyncio.run(websocket_worker())


if __name__ == "__main__":
    print("")
    print("=" * 60)
    print("🤖 AI TRADING BOT V3 - MEME COINS (مؤشرات + AI متطور)")
    print("=" * 60)

    try:
        outbound_ip = requests.get("https://api.ipify.org", timeout=10).text
        print(f"🌐 OUTBOUND IP: {outbound_ip}")
    except Exception as e:
        print(f"⚠️ تعذر جلب الـ IP الخارجي: {e}")

    threading.Thread(target=run_server, daemon=True).start()
    time.sleep(3)

    print("🔌 اختبار Binance قبل التشغيل...")
    if test_binance():
        print("")
        print("📥 تحميل البيانات الأولية...")
        load_initial_candles()
        print("")
        print("🚀 بدء Binance WebSocket...")
        start_websocket()
    else:
        print("")
        print("🛑 تم إيقاف البوت لأن Binance غير متصل")
