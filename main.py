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
    # تشغيل السيرفر على المنفذ 8080
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
    }
})

client = OpenAI(
  base_url="https://integrate.api.nvidia.com/v1",
  api_key="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"
)

# ==========================================
# 3. ذراع التنفيذ
# ==========================================
def execute_trade(symbol, decision):
    LEVERAGE = 10         
    TRADE_MARGIN = 10     
    TP_PERCENT = 0.05     
    SL_PERCENT = 0.03     

    try:
        exchange.set_leverage(LEVERAGE, symbol)
        ticker = exchange.fetch_ticker(symbol)
        current_price = ticker['last']
        position_size = (TRADE_MARGIN * LEVERAGE) / current_price
        
        if decision == "BUY":
            tp_price = current_price * (1 + TP_PERCENT)
            sl_price = current_price * (1 - SL_PERCENT)
            side = 'buy'
            exit_side = 'sell'
        elif decision == "SELL":
            tp_price = current_price * (1 - TP_PERCENT)
            sl_price = current_price * (1 + SL_PERCENT)
            side = 'sell'
            exit_side = 'buy'
        else:
            return 

        print(f"🚀 جاري تنفيذ صفقة {decision} لعملة {symbol} بحجم {TRADE_MARGIN}$...")
        exchange.create_market_order(symbol, side, position_size)
        exchange.create_order(symbol, 'stop_market', exit_side, position_size, params={'stopPrice': sl_price, 'reduceOnly': True})
        exchange.create_order(symbol, 'take_profit_market', exit_side, position_size, params={'stopPrice': tp_price, 'reduceOnly': True})
        print("✅ تم فتح الصفقة بنجاح!")
    except Exception as e:
        print(f"❌ فشل التنفيذ لعملة {symbol}: {e}")

# ==========================================
# 4. محرك البحث والتحليل
# ==========================================
def get_all_usdt_symbols():
    markets = exchange.load_markets()
    symbols = [symbol for symbol in markets if symbol.endswith('/USDT') and markets[symbol]['active']]
    return symbols

def fetch_and_analyze():
    symbols = get_all_usdt_symbols()
    print(f"تم العثور على {len(symbols)} عملة! بدأ المسح الراداري...\n")
    for symbol in symbols:
        try:
            print(f"[{symbol}] جاري سحب البيانات...")
            daily_candles = exchange.fetch_ohlcv(symbol, timeframe='1d', limit=7)
            hourly_candles = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=24)
            
            prompt = f"أنت محلل مالي محترف. حلل {symbol}.\nيومي: {daily_candles}\nساعي: {hourly_candles}\nأعطني توصية واضحة. يجب أن تنتهي إجابتك بكلمة واحدة فقط في السطر الأخير: (BUY أو SELL أو WAIT)."
            
            completion = client.chat.completions.create(
              model="deepseek-ai/deepseek-v4-pro",
              messages=[{"role": "user", "content": prompt}],
              temperature=1,
              top_p=0.95,
              max_tokens=16384,
              extra_body={"chat_template_kwargs": {"thinking": False}},
              stream=False
            )
            
            analysis_result = completion.choices[0].message.content
            words = analysis_result.strip().split()
            decision = words[-1].upper().replace(".", "").replace(",", "") if words else "WAIT"
            
            if decision in ["BUY", "SELL"]:
                execute_trade(symbol, decision)
            else:
                print("⏳ توصية بالانتظار.")
                
            time.sleep(1) 
        except Exception as e:
            print(f"⚠️ تخطي {symbol} بسبب خطأ: {e}")

# ==========================================
# 5. نقطة الانطلاق الشاملة
# ==========================================
if __name__ == "__main__":
    # تشغيل السيرفر الوهمي في مسار جانبي (Thread)
    Thread(target=run_server).start()
    
    # تشغيل البوت في المسار الرئيسي
    while True:
        print("🔄 بدء دورة مسح رادارية جديدة...")
        try:
            fetch_and_analyze()
        except Exception as e:
            print(f"خطأ أثناء الدورة: {e}")
        print("✅ البوت سيرتاح لمدة 5 دقائق...")
        time.sleep(300)
