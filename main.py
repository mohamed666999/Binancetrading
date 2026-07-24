import ccxt
import time
from openai import OpenAI
from flask import Flask
from threading import Thread

# ==========================================
# 1. إعداد السيرفر الوهمي
# ==========================================
app = Flask(__name__)

@app.route('/')
def keep_alive():
    return "🤖 رادار الذكاء الاصطناعي يعمل بنجاح ولا ينام!"

def run_server():
    app.run(host='0.0.0.0', port=8080)

# ==========================================
# 2. إعدادات الاتصال (بينانس + وكيل - اختياري)
# ==========================================
PROXY_URL = ""  # اتركه فارغاً أو ضع وكيل مثل: http://user:pass@ip:port

exchange_params = {
    'apiKey': "IX7kLH0ssWHP5TpYMUGcp0pzq4LX4Lqi7m4XtlqMkkq6DCZAsLhoeYZ3533jJFF4",
    'secret': "LmICnpSpMxL1riv4RfIf0HBGRfhDTP5JhDUYdlPSukpqV7kDTonrZ0j3DWp1a7hU",
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap',
        'adjustForTimeDifference': True,
    }
}

if PROXY_URL:
    exchange_params['proxies'] = {
        'http': PROXY_URL,
        'https': PROXY_URL,
    }
    print(f"🔌 تم تفعيل الوكيل: {PROXY_URL}")

exchange = ccxt.binance(exchange_params)

client = OpenAI(
    base_url="https://integrate.api.nvidia.com/v1",
    api_key="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"
)

# ==========================================
# 3. ذراع التنفيذ الآمن
# ==========================================
def execute_trade(symbol, decision):
    LEVERAGE = 10
    TRADE_MARGIN = 10

    try:
        print(f"🚀 تنفيذ {decision} على {symbol} الآن...", flush=True)
        exchange.load_markets()
        market = exchange.market(symbol)
        ticker = exchange.fetch_ticker(symbol)
        current_price = ticker['last']

        notional_value = TRADE_MARGIN * LEVERAGE
        raw_amount = notional_value / current_price
        position_size = float(exchange.amount_to_precision(symbol, raw_amount))

        side = "buy" if decision == "BUY" else "sell"

        print(f"💰 السعر: {current_price}", flush=True)
        print(f"💵 قيمة المركز: {notional_value} USDT", flush=True)
        print(f"📦 الكمية: {position_size}", flush=True)
        print("⚙️ ضبط الرافعة...", flush=True)
        exchange.set_leverage(LEVERAGE, symbol)
        print("📤 إرسال أمر السوق...", flush=True)
        order = exchange.create_market_order(symbol, side, position_size)
        print("✅ تم فتح المركز بنجاح!", flush=True)
        print(order, flush=True)

    except Exception as e:
        print(f"❌ فشل فتح الصفقة {symbol}", flush=True)
        print(f"نوع الخطأ: {type(e).__name__}", flush=True)
        print(f"التفاصيل: {e}", flush=True)

# ==========================================
# 4. تحميل الأسواق (مرة واحدة فقط)
# ==========================================
ALL_SYMBOLS = []

def load_symbols_once():
    global ALL_SYMBOLS
    if ALL_SYMBOLS:
        return ALL_SYMBOLS

    try:
        print("📡 جاري تحميل الأسواق لأول مرة...", flush=True)
        markets = exchange.load_markets()
        symbols = []
        for symbol, market in markets.items():
            if (
                market.get("active")
                and market.get("swap")
                and market.get("linear")
                and market.get("settle") == "USDT"
            ):
                symbols.append(symbol)

        ALL_SYMBOLS = symbols[:2]  # BTC و ETH للاختبار
        print(f"✅ تم تحميل {len(ALL_SYMBOLS)} عملة: {ALL_SYMBOLS}", flush=True)
        return ALL_SYMBOLS

    except Exception as e:
        print(f"⚠️ فشل تحميل الأسواق: {e}", flush=True)
        return []

# ==========================================
# 5. دورة التحليل (مع تتبع شامل للذكاء الاصطناعي)
# ==========================================
def fetch_and_analyze():
    symbols = load_symbols_once()
    if not symbols:
        print("❌ لا توجد عملات متاحة. تخطي هذه الدورة.", flush=True)
        return

    print(f"🔍 بدأ تحليل {len(symbols)} عملة...\n", flush=True)
    for symbol in symbols:
        try:
            print(f"\n[{symbol}] 1️⃣ بدء سحب البيانات...", flush=True)
            print(f"[{symbol}] 2️⃣ سحب البيانات اليومية...", flush=True)
            daily_candles = exchange.fetch_ohlcv(symbol, timeframe='1d', limit=7)
            print(f"[{symbol}] 3️⃣ تم جلب البيانات اليومية", flush=True)

            print(f"[{symbol}] 4️⃣ سحب البيانات الساعية...", flush=True)
            hourly_candles = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=24)
            print(f"[{symbol}] 5️⃣ تم جلب البيانات الساعية", flush=True)

            # --- إرسال إلى الذكاء الاصطناعي (مع timeout ومعالجة الخطأ) ---
            print(f"[{symbol}] 6️⃣ إرسال البيانات إلى الذكاء الاصطناعي...", flush=True)

            try:
                completion = client.chat.completions.create(
                    model="deepseek-ai/deepseek-v4-pro",
                    messages=[
                        {
                            "role": "user",
                            "content": f"""
اختر قراراً واحداً فقط بناءً على البيانات التالية.

العملة: {symbol}
اليومي: {daily_candles}
الساعي: {hourly_candles}

أجب بكلمة واحدة فقط:
BUY
SELL
WAIT
"""
                        }
                    ],
                    temperature=0,
                    max_tokens=5,
                    stream=False,
                    timeout=30,  # انتظار أقصى 30 ثانية للرد
                    extra_body={
                        "chat_template_kwargs": {
                            "thinking": False
                        }
                    }
                )

                analysis_result = completion.choices[0].message.content or ""
                print(f"[{symbol}] 7️⃣ وصل رد الذكاء الاصطناعي: {analysis_result}", flush=True)

            except Exception as ai_error:
                print(f"[{symbol}] ❌ خطأ NVIDIA AI: {type(ai_error).__name__}: {ai_error}", flush=True)
                continue  # تخطي هذه العملة والانتقال للتالية

            # --- تحليل الرد ---
            text = analysis_result.upper().strip()
            if "BUY" in text:
                decision = "BUY"
            elif "SELL" in text:
                decision = "SELL"
            else:
                decision = "WAIT"

            print(f"[{symbol}] 🎯 القرار النهائي: {decision}", flush=True)

            if decision in ["BUY", "SELL"]:
                print(f"🚀 محاولة فتح مركز {decision} على {symbol}", flush=True)
                execute_trade(symbol, decision)
            else:
                print(f"⏳ لا توجد صفقة على {symbol}", flush=True)

            time.sleep(3)

        except Exception as e:
            print(f"❌ خطأ حقيقي في {symbol}: {type(e).__name__}: {e}", flush=True)

# ==========================================
# 6. نقطة الانطلاق
# ==========================================
if __name__ == "__main__":
    # تشغيل السيرفر الوهمي
    Thread(target=run_server).start()

    # تحميل الأسواق أول مرة
    load_symbols_once()

    # الدورة الرئيسية
    while True:
        print("\n🔄 بدء دورة مسح رادارية جديدة...", flush=True)
        try:
            fetch_and_analyze()
        except Exception as e:
            print(f"خطأ أثناء الدورة: {e}", flush=True)
        print("✅ البوت سيرتاح لمدة 10 دقائق...", flush=True)
        time.sleep(600)
