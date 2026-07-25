import asyncio
import json
import time
import threading
import websockets
import ccxt

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
# 4. العملات
# =====================================================

SYMBOLS = {
    "btcusdt": "BTC/USDT:USDT",
    "ethusdt": "ETH/USDT:USDT"
}


# =====================================================
# 5. تخزين الشموع القادمة من WebSocket
# =====================================================

candles = {
    "btcusdt": {
        "1m": [],
        "1h": [],
        "1d": []
    },
    "ethusdt": {
        "1m": [],
        "1h": [],
        "1d": []
    }
}


# =====================================================
# 6. منع تكرار الصفقات
# =====================================================

last_decision = {
    "BTC/USDT:USDT": None,
    "ETH/USDT:USDT": None
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
# 9. تنفيذ الصفقة
# =====================================================

def execute_trade(symbol, decision):

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


    except Exception as e:

        print("")
        print("❌❌❌ فشل تنفيذ الصفقة ❌❌❌")

        print(e)


# =====================================================
# 10. إرسال البيانات إلى DeepSeek
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

        return


    one_minute = market_data["1m"][-100:]

    one_hour = market_data["1h"][-24:]

    one_day = market_data["1d"][-7:]


    if len(one_hour) < 5:

        print(
            f"⏳ لا توجد شموع كافية بعد "
            f"({len(one_hour)}/5)"
        )

        return


    print("📊 البيانات المتوفرة:")

    print(f"1m: {len(one_minute)} شمعة")

    print(f"1h: {len(one_hour)} شمعة")

    print(f"1d: {len(one_day)} شمعة")


    prompt = f"""
أنت نظام تحليل تداول.

حلل العملة:

{symbol}

بيانات 1m:
{one_minute}

بيانات 1h:
{one_hour}

بيانات 1d:
{one_day}

أعطني القرار النهائي فقط في آخر سطر.

يجب أن يكون آخر سطر حرفيًا واحدًا من:

BUY
SELL
WAIT

لا تكتب أي كلمة بعد القرار.
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


        text = response.upper().strip()


        lines = text.splitlines()


        decision = "WAIT"


        for line in reversed(lines):

            line = line.strip()

            if line in ["BUY", "SELL", "WAIT"]:

                decision = line

                break


        print("")

        print(f"🎯 القرار النهائي: {decision}")


        previous = last_decision.get(symbol)


        if decision == "WAIT":

            print("⏳ AI قال WAIT")

            return


        if previous == decision:

            print(
                f"🛑 نفس القرار السابق "
                f"({decision}) - لن نكرر الصفقة"
            )

            return


        last_decision[symbol] = decision


        print(
            f"🚨 قرار جديد: {decision}"
        )


        execute_trade(
            symbol,
            decision
        )


    except Exception as e:

        print("❌ فشل الاتصال بـ DeepSeek")

        print(e)


# =====================================================
# 11. Binance WebSocket
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

                            target=analyze_with_ai,

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
# 12. التشغيل
# =====================================================

def start_websocket():

    asyncio.run(
        websocket_worker()
    )


if __name__ == "__main__":

    print("")
    print("=" * 60)
    print("🤖 AI TRADING BOT V2")
    print("=" * 60)


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
