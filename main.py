import ccxt
import time
from openai import OpenAI
from flask import Flask
from threading import Thread

# ==========================================
# 1. إعداد السيرفر الوهمي (لإبقاء البوت مستيقظاً 24/7)
# ==========================================

app = Flask(__name__)

@app.route('/')
def keep_alive():
    return "🤖 رادار الذكاء الاصطناعي يعمل بنجاح ولا ينام!"

def run_server():
    app.run(host='0.0.0.0', port=8080)

# ==========================================
# 2. إعدادات الاتصال (بينانس + الذكاء الاصطناعي)
# ==========================================

exchange = ccxt.binance({
    'apiKey': 'IX7kLH0ssWHP5TpYMUGcp0pzq4LX4Lqi7m4XtlqMkkq6DCZAsLhoeYZ3533jJFF4',
    'secret': 'LmICnpSpMxL1riv4RfIf0HBGRfhDTP5JhDUYdlPSukpqV7kDTonrZ0j3DWp1a7hU',
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap',
        'adjustForTimeDifference': True,
    }
})

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
        ticker = exchange.fetch_ticker(symbol)
        current_price = ticker['last']

        notional_value = TRADE_MARGIN * LEVERAGE
        position_size = notional_value / current_price

        if decision == "BUY":
            side = "buy"
            position_name = "LONG"
        elif decision == "SELL":
            side = "sell"
            position_name = "SHORT"
        else:
            return

        print(f"🚀 {symbol}")
        print(f"📊 القرار: {decision} ({position_name})")
        print(f"💰 السعر: {current_price}")
        print(f"💵 قيمة المركز: {notional_value} USDT")
        print(f"📦 الكمية: {position_size}")

        exchange.set_leverage(LEVERAGE, symbol)

        order = exchange.create_market_order(
            symbol,
            side,
            position_size
        )

        print("✅ تم فتح المركز بنجاح!")
        print(order)

    except Exception as e:
        print(f"❌ فشل فتح الصفقة {symbol}")
        print(e)

# ==========================================
# 4. محرك البحث والتحليل (متوافق مع FAPI لتجنب حظر بينانس)
# ==========================================

def get_all_usdt_symbols():
    try:
        markets = exchange.fetch_markets({'type': 'swap'})
        symbols = []

        for market in markets:
            if (
                market.get("active")
                and market.get("linear")
                and market.get("quote") == "USDT"
            ):
                symbols.append(market['symbol'])

        # قصر القائمة على أول عملتين للاختبار الآمن وعدم تجاوز حدود الحظر
        return symbols[:2]
    except Exception as e:
        print(f"⚠️ خطأ في تحميل الأسواق: {e}")
        return []

def fetch_and_analyze():
    symbols = get_all_usdt_symbols()
    print(f"تم العثور على {len(symbols)} عملة للعقود الدائمة! بدأ المسح الراداري...\n")
    
    for symbol in symbols:
        try:
            print(f"[{symbol}] جاري سحب البيانات...")
            daily_candles = exchange.fetch_ohlcv(symbol, timeframe='1d', limit=7)
            hourly_candles = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=24)

            prompt = f"""
حلل {symbol} بسرعة.

البيانات اليومية:
{daily_candles}

البيانات الساعية:
{hourly_candles}

أجب بكلمة واحدة فقط:
BUY أو SELL أو WAIT
"""  
              
            completion = client.chat.completions.create(  
              model="deepseek-ai/deepseek-v4-pro",  
              messages=[{"role": "user", "content": prompt}],  
              temperature=0,  
              max_tokens=10,  
              extra_body={"chat_template_kwargs": {"thinking": False}},  
              stream=False  
            )  
              
            analysis_result = completion.choices[0].message.content or ""
            print(f"🤖 رد AI لـ {symbol}:")
            print(analysis_result[:500])

            text = analysis_result.upper()

            if "BUY" in text:
                decision = "BUY"
            elif "SELL" in text:
                decision = "SELL"
            else:
                decision = "WAIT"

            if decision in ["BUY", "SELL"]:
                print(f"🚀 القرار: {decision}")
                execute_trade(symbol, decision)
            else:
                print("⏳ القرار: WAIT")
                  
            time.sleep(1)   
        except Exception as e:  
            print(f"⚠️ تخطي {symbol} بسبب خطأ: {e}")

# ==========================================
# 5. نقطة الانطلاق الشاملة
# ==========================================

if __name__ == "__main__":
    # تشغيل السيرفر الوهمي في مسار جانبي
    Thread(target=run_server).start()

    # حلقة التشغيل الرئيسية للبوت
    while True:  
        print("🔄 بدء دورة مسح رادارية جديدة...")  
        try:  
            fetch_and_analyze()  
        except Exception as e:  
            print(f"خطأ أثناء الدورة: {e}")  
        print("✅ البوت سيرتاح لمدة 5 دقائق...")  
        time.sleep(300)
