import ccxt
import time
from openai import OpenAI
from flask import Flask
from threading import Thread
import asyncio
import websockets
import json

# ==================================================
# 1. Flask لإبقاء Render مستيقظاً
# ==================================================

app = Flask(__name__)

@app.route("/")
def home():
    return "AI Trading Bot is ONLINE"

def run_server():
    app.run(
        host="0.0.0.0",
        port=8080
    )

# ==================================================
# 2. Binance
# ==================================================

print("======================================")
print("🚀 بدء تشغيل البوت")
print("======================================")

BINANCE_API_KEY = "IX7kLH0ssWHP5TpYMUGcp0pzq4LX4Lqi7m4XtlqMkkq6DCZAsLhoeYZ3533jJFF4"
BINANCE_SECRET = "LmICnpSpMxL1riv4RfIf0HBGRfhDTP5JhDUYdlPSukpqV7kDTonrZ0j3DWp1a7hU"

if not BINANCE_API_KEY or not BINANCE_SECRET:
    print("❌ BINANCE API KEYS غير موجودة في الكود")
else:
    print("✅ مفاتيح Binance موجودة")

exchange = ccxt.binance({
    "apiKey": BINANCE_API_KEY,
    "secret": BINANCE_SECRET,
    "enableRateLimit": True,
    "options": {
        "defaultType": "swap",
        "adjustForTimeDifference": True
    }
})

# ==================================================
# 3. DeepSeek / NVIDIA
# ==================================================

NVIDIA_API_KEY = "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"

if not NVIDIA_API_KEY:
    print("❌ NVIDIA_API_KEY غير موجود")
else:
    print("✅ مفتاح NVIDIA موجود")

client = OpenAI(
    base_url="https://integrate.api.nvidia.com/v1",
    api_key=NVIDIA_API_KEY
)

# ==================================================
# 4. العملات
# ==================================================

SYMBOLS = [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT"
]

# ==================================================
# 5. WebSocket للأسعار الحية (جديد)
# ==================================================

async def listen_binance_prices(symbols):
    """تستمع لبث الأسعار المباشر دون استهلاك طلبات"""
    # تحويل BTC/USDT:USDT -> btcusdt
    streams = "/".join([
        s.split(":")[0].replace("/", "").lower() + "@ticker"
        for s in symbols
    ])
    uri = f"wss://fstream.binance.com/stream?streams={streams}"
    
    async with websockets.connect(uri) as websocket:
        print("✅ تم فتح قناة WebSocket للأسعار الحية")
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                if "data" in data:
                    ticker = data["data"]
                    symbol = ticker["s"]
                    price = ticker["c"]
                    print(f"📡 [{symbol}] السعر المباشر: {price}")
            except Exception as e:
                print(f"⚠️ خطأ WebSocket: {e}")
                await asyncio.sleep(1)

def run_websocket():
    """تشغيل WebSocket في thread منفصل"""
    asyncio.run(listen_binance_prices(SYMBOLS))

# ==================================================
# 6. اختبار اتصال Binance
# ==================================================

def test_binance():
    print("")
    print("🔌 [1] اختبار الاتصال بـ Binance...")
    try:
        ticker = exchange.fetch_ticker("BTC/USDT:USDT")
        print("✅ Binance متصل بنجاح")
        print(f"💰 BTC السعر الحالي: {ticker['last']}")
        return True
    except Exception as e:
        print("❌ فشل الاتصال بـ Binance")
        print("ERROR:", e)
        return False

# ==================================================
# 7. جلب الشموع
# ==================================================

def get_candles(symbol):
    print("")
    print(f"📊 [{symbol}] جلب البيانات...")
    try:
        daily = exchange.fetch_ohlcv(symbol, timeframe="1d", limit=7)
        hourly = exchange.fetch_ohlcv(symbol, timeframe="1h", limit=24)
        print(f"✅ [{symbol}] تم جلب البيانات")
        print(f"📅 شموع يومية: {len(daily)}")
        print(f"⏱ شموع ساعية: {len(hourly)}")
        return daily, hourly
    except Exception as e:
        print(f"❌ فشل جلب بيانات {symbol}")
        print("ERROR:", e)
        return None, None

# ==================================================
# 8. تحليل DeepSeek
# ==================================================

def ask_ai(symbol, daily, hourly):
    print("")
    print(f"🧠 [{symbol}] إرسال البيانات إلى DeepSeek...")

    prompt = f"""
أنت محلل تداول آلي.

حلل العملة:
{symbol}

البيانات اليومية:
{daily}

البيانات الساعية:
{hourly}

أجب في النهاية بكلمة واحدة فقط:

BUY
SELL
WAIT
"""
    try:
        response = client.chat.completions.create(
            model="deepseek-ai/deepseek-v4-pro",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=50,
            extra_body={"chat_template_kwargs": {"thinking": False}}
        )
        result = response.choices[0].message.content or ""
        print("✅ DeepSeek رد بنجاح")
        print("🤖 رد الذكاء الاصطناعي:")
        print(result)
        text = result.upper()
        if "BUY" in text:
            decision = "BUY"
        elif "SELL" in text:
            decision = "SELL"
        else:
            decision = "WAIT"
        print(f"🎯 القرار النهائي: {decision}")
        return decision
    except Exception as e:
        print("❌ فشل الاتصال بـ DeepSeek")
        print("ERROR:", e)
        return "WAIT"

# ==================================================
# 9. تنفيذ الصفقة
# ==================================================

def execute_trade(symbol, decision):
    print("")
    print(f"💥 محاولة فتح صفقة {decision} على {symbol}")
    LEVERAGE = 10
    MARGIN = 10
    try:
        ticker = exchange.fetch_ticker(symbol)
        price = ticker["last"]
        notional = MARGIN * LEVERAGE
        amount = notional / price
        print(f"💰 السعر: {price}")
        print(f"💵 الهامش: {MARGIN} USDT")
        print(f"⚡ الرافعة: x{LEVERAGE}")
        print(f"📦 حجم المركز: {notional} USDT")
        print(f"🔢 الكمية: {amount}")
        exchange.set_leverage(LEVERAGE, symbol)
        if decision == "BUY":
            side = "buy"
            position = "LONG"
        elif decision == "SELL":
            side = "sell"
            position = "SHORT"
        else:
            return
        print(f"🚀 إرسال أمر {position} إلى Binance...")
        order = exchange.create_market_order(symbol, side, amount)
        print("")
        print("======================================")
        print("✅ تم فتح الصفقة بنجاح")
        print(f"📌 المركز: {position}")
        print(f"📌 العملة: {symbol}")
        print(f"📌 Order ID: {order.get('id')}")
        print("======================================")
    except Exception as e:
        print("")
        print("❌ فشل تنفيذ الصفقة")
        print("ERROR:", e)

# ==================================================
# 10. دورة واحدة
# ==================================================

def run_cycle():
    print("")
    print("======================================")
    print("🔄 بدء دورة تداول جديدة")
    print("======================================")
    if not test_binance():
        print("⛔ إيقاف الدورة لأن Binance غير متصل")
        return
    for symbol in SYMBOLS:
        print("")
        print(f"🔍 تحليل {symbol}")
        daily, hourly = get_candles(symbol)
        if daily is None:
            continue
        decision = ask_ai(symbol, daily, hourly)
        if decision in ["BUY", "SELL"]:
            execute_trade(symbol, decision)
        else:
            print(f"⏳ لا توجد صفقة على {symbol}")
        time.sleep(5)

# ==================================================
# 11. التشغيل
# ==================================================

if __name__ == "__main__":
    Thread(target=run_server, daemon=True).start()
    Thread(target=run_websocket, daemon=True).start()  # تشغيل WebSocket في الخلفية
    time.sleep(3)
    while True:
        try:
            run_cycle()
        except Exception as e:
            print("🔥 خطأ عام:")
            print(e)
        print("")
        print("😴 انتظار 5 دقائق قبل الدورة التالية...")
        print("")
        time.sleep(300)
