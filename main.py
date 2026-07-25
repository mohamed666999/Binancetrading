import asyncio
import json
import time
import threading
import websockets
import ccxt
import requests

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

                print(
                    f"⚠️ يوجد مركز مفتوح بالفعل: "
                    f"{side} | {contracts}"
                )

                return position

        print("✅ لا يوجد مركز مفتوح")

        return None

    except Exception as e:

        print("⚠️ تعذر فحص المركز")

        print(e)

        return None


# =====================================================
# 9. تنفيذ الصفقة + وضع SL/TP فوراً
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

        quantity = notional / price


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

        exchange.set_leverage(
            LEVERAGE,
            symbol
        )


        print("🚀 إرسال أمر السوق إلى Binance...")


        order = exchange.create_market_order(
            symbol,
            side,
            quantity
        )


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

        print(f"🛑 SL: {stop_loss:.6f} ({sl_percent}%)")
        print(f"🎯 TP: {take_profit:.6f} ({tp_percent}%)")

        try:
            exchange.create_order(
                symbol,
                "STOP_MARKET",
                "sell" if side == "buy" else "buy",
                quantity,
                None,
                {
                    "stopPrice": round(stop_loss, 6),
                    "reduceOnly": True
                }
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
                {
                    "stopPrice": round(take_profit, 6),
                    "reduceOnly": True
                }
            )
            print("✅ TP موضوع بنجاح")
        except Exception as tp_err:
            print(f"⚠️ فشل وضع TP: {tp_err}")


    except Exception as e:

        print("")
        print("❌❌❌ فشل تنفيذ الصفقة ❌❌❌")

        print(e)


# =====================================================
# 10. إرسال البيانات إلى DeepSeek (معدّل: JSON سريع)
# =====================================================

def analyze_with_ai(symbol):

    print("")
    print("=" * 60)
    print(f"🧠 بدء تحليل الذكاء الاصطناعي: {symbol}")
    print("=" * 60)


    market_data = None


    for key, value in SYMBOLS.items():

        if value == symbol:

            market_data = candles[key]

            break


    if not market_data:

        print("❌ لا توجد بيانات")

        return None, None, None


    one_minute = market_data["1m"][-20:]
    one_hour = market_data["1h"][-10:]
    one_day = market_data["1d"][-5:]


    if len(one_hour) < 5:

        print(
            f"⏳ لا توجد شموع كافية بعد "
            f"({len(one_hour)}/5)"
        )

        return None, None, None


    print("📊 البيانات المتوفرة:")

    print(f"1m: {len(one_minute)} شمعة")

    print(f"1h: {len(one_hour)} شمعة")

    print(f"1d: {len(one_day)} شمعة")


    prompt = f"""
أنت نظام تداول آلي سريع.

الرمز: {symbol}

بيانات 1m:
{one_minute}

بيانات 1h:
{one_hour}

بيانات 1d:
{one_day}

أجب JSON فقط بدون شرح:

{{
  "decision": "BUY أو SELL أو WAIT",
  "stop_loss_percent": رقم بين 0.5 و 5,
  "take_profit_percent": رقم بين 1 و 10,
  "confidence": رقم بين 0 و 100
}}

لا تكتب أي شيء خارج JSON.
"""


    print("📤 إرسال البيانات إلى DeepSeek...")

    try:

        completion = client.chat.completions.create(

            model="deepseek-ai/deepseek-v4-pro",

            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ],

            temperature=0,

            max_tokens=200,

            extra_body={
                "chat_template_kwargs": {
                    "thinking": False
                }
            },

            stream=False

        )


        response = completion.choices[0].message.content or ""


        print("")
        print("🤖 رد DeepSeek الكامل:")

        print(response)


        # ✅ محاولة قراءة JSON
        try:
            data = json.loads(response)
            decision = str(data.get("decision", "WAIT")).upper()
            sl_percent = float(data.get("stop_loss_percent", 2.0))
            tp_percent = float(data.get("take_profit_percent", 4.0))
            confidence = float(data.get("confidence", 50))
        except:
            # fallback قديم
            text = response.upper().strip()
            lines = text.splitlines()
            decision = "WAIT"
            for line in reversed(lines):
                line = line.strip()
                if line in ["BUY", "SELL", "WAIT"]:
                    decision = line
                    break
            sl_percent = 2.0
            tp_percent = 4.0
            confidence = 50


        # ✅ تطبيق الحدود
        sl_percent = max(0.5, min(sl_percent, 5.0))
        tp_percent = max(1.0, min(tp_percent, 10.0))
        confidence = max(0, min(confidence, 100))

        print("")
        print(f"🎯 القرار النهائي: {decision}")
        print(f"📊 الثقة: {confidence}%")
        print(f"🛑 SL: {sl_percent}%")
        print(f"🎯 TP: {tp_percent}%")


        state = trade_state.get(symbol)


        if decision == "WAIT":
            print("⏳ AI قال WAIT")
            return None, None, None


        if state and state["last_decision"] == decision:
            print(f"🛑 نفس القرار السابق ({decision}) - لن نكرر الصفقة")
            return None, None, None


        if state:
            state["last_decision"] = decision


        print(f"🚨 قرار جديد: {decision}")

        return decision, sl_percent, tp_percent


    except Exception as e:

        print("❌ فشل الاتصال بـ DeepSeek")
        print(e)
        return None, None, None


# =====================================================
# 11. تحليل آمن لكل عملة بشكل مستقل
# =====================================================

def analyze_with_ai_safe(symbol):
    state = trade_state.get(symbol)
    if not state:
        return

    if state["ai_busy"]:
        print(f"⏳ AI مازال يحلل {symbol}")
        return

    state["ai_busy"] = True
    try:
        decision, sl_percent, tp_percent = analyze_with_ai(symbol)
        if decision in ["BUY", "SELL"]:
            execute_trade(symbol, decision, sl_percent, tp_percent)
    finally:
        state["ai_busy"] = False


# =====================================================
# 12. Binance WebSocket
# =====================================================

async def websocket_worker():

    streams = []

    for symbol in SYMBOLS.keys():

        streams.append(
            f"{symbol}@kline_1m"
        )

        streams.append(
            f"{symbol}@kline_1h"
        )

        streams.append(
            f"{symbol}@kline_1d"
        )


    stream_url = (
        "wss://stream.binance.com:9443/stream?streams="
        + "/".join(streams)
    )


    print("")
    print("=" * 60)
    print("🔌 الاتصال بـ Binance WebSocket")
    print("=" * 60)


    while True:

        try:

            async with websockets.connect(
                stream_url,
                ping_interval=20,
                ping_timeout=20
            ) as websocket:

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


                    candles[symbol_key][interval].append(candle)


                    if len(
                        candles[symbol_key][interval]
                    ) > 200:

                        candles[symbol_key][interval] = \
                            candles[symbol_key][interval][-200:]


                    print(
                        f"📡 {symbol_key.upper()} "
                        f"{interval} "
                        f"السعر: {candle[4]}",
                        end="\r"
                    )


                    if is_closed and interval == "1h":

                        symbol = SYMBOLS[symbol_key]

                        print("")

                        print(
                            f"🕯️ شمعة ساعة جديدة أغلقت: "
                            f"{symbol}"
                        )


                        threading.Thread(

                            target=analyze_with_ai_safe,

                            args=(symbol,),

                            daemon=True

                        ).start()


        except Exception as e:

            print("")

            print("❌ WebSocket انقطع")

            print(e)

            print("🔄 إعادة الاتصال بعد 10 ثوانٍ")

            await asyncio.sleep(10)


# =====================================================
# 13. التشغيل
# =====================================================

def start_websocket():

    asyncio.run(
        websocket_worker()
    )


if __name__ == "__main__":

    print("")
    print("=" * 60)
    print("🤖 AI TRADING BOT V2 - MEME COINS (AI SL/TP)")
    print("=" * 60)

    # ✅ إضافة طباعة الـ IP الخارجي لمعرفة العنوان الذي يراه Binance
    try:
        outbound_ip = requests.get("https://api.ipify.org", timeout=10).text
        print(f"🌐 OUTBOUND IP: {outbound_ip}")
    except Exception as e:
        print(f"⚠️ تعذر جلب الـ IP الخارجي: {e}")


    threading.Thread(

        target=run_server,

        daemon=True

    ).start()


    time.sleep(3)


    print("🔌 اختبار Binance قبل التشغيل...")


    if test_binance():

        print("")

        print("🚀 بدء Binance WebSocket...")

        start_websocket()

    else:

        print("")

        print(
            "🛑 تم إيقاف البوت "
            "لأن Binance غير متصل"
        )
