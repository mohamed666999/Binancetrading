import ccxt
import time
from openai import OpenAI

# ==========================================
# 1. إعدادات الاتصال (بينانس + الذكاء الاصطناعي)
# ==========================================

# إعداد بينانس للعقود الآجلة (Futures) مع المفاتيح المدمجة
exchange = ccxt.binance({
    'apiKey': 'IX7kLH0ssWHP5TpYMUGcp0pzq4LX4Lqi7m4XtlqMkkq6DCZAsLhoeYZ3533jJFF4',
    'secret': 'LmICnpSpMxL1riv4RfIf0HBGRfhDTP5JhDUYdlPSukpqV7kDTonrZ0j3DWp1a7hU',
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap', # تفعيل سوق العقود الآجلة Futures
    }
})

# إعداد NVIDIA DeepSeek
client = OpenAI(
  base_url="https://integrate.api.nvidia.com/v1",
  api_key="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"
)

# ==========================================
# 2. ذراع التنفيذ (الدخول ووضع الدرع الواقي)
# ==========================================

def execute_trade(symbol, decision):
    LEVERAGE = 10         # رافعة 10x لتكبير الأرباح
    TRADE_MARGIN = 10     # سحب 10 دولار من المحفظة لكل صفقة
    TP_PERCENT = 0.05     # جني الأرباح عند 5%
    SL_PERCENT = 0.03     # وقف الخسارة عند 3% (الدرع الواقي)

    try:
        # ضبط الرافعة المالية للعملة
        exchange.set_leverage(LEVERAGE, symbol)
        
        # جلب السعر الحالي لحساب الكمية
        ticker = exchange.fetch_ticker(symbol)
        current_price = ticker['last']
        
        # حساب حجم العقد بناءً على الرافعة المالية
        position_size = (TRADE_MARGIN * LEVERAGE) / current_price
        
        # تحديد أسعار الهدف والدرع بناءً على القرار
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
            return # في حال الانتظار لا تفعل شيئاً

        print(f"🚀 جاري تنفيذ صفقة {decision} لعملة {symbol} بحجم {TRADE_MARGIN}$ ورافعة {LEVERAGE}x...")

        # الدخول في الصفقة (أمر السوق السريع)
        exchange.create_market_order(symbol, side, position_size)

        # وضع وقف الخسارة (Stop Loss)
        exchange.create_order(
            symbol, 'stop_market', exit_side, position_size, 
            params={'stopPrice': sl_price, 'reduceOnly': True}
        )

        # وضع أمر جني الأرباح (Take Profit)
        exchange.create_order(
            symbol, 'take_profit_market', exit_side, position_size, 
            params={'stopPrice': tp_price, 'reduceOnly': True}
        )

        print("✅ تم فتح الصفقة بنجاح وتفعيل الدرع الواقي وأهداف الربح!")
        
    except Exception as e:
        print(f"❌ فشل التنفيذ لعملة {symbol}: {e}")

# ==========================================
# 3. محرك البحث والتحليل (عقل البوت)
# ==========================================

def get_all_usdt_symbols():
    print("جاري الاتصال بالسوق لمعرفة العملات المتاحة...")
    markets = exchange.load_markets()
    # جلب العملات النشطة مقابل الدولار
    symbols = [symbol for symbol in markets if symbol.endswith('/USDT') and markets[symbol]['active']]
    return symbols

def fetch_and_analyze():
    symbols = get_all_usdt_symbols()
    print(f"تم العثور على {len(symbols)} عملة! بدأ المسح الراداري...\n")
    
    for symbol in symbols:
        try:
            print(f"[{symbol}] جاري سحب البيانات والتحليل...")
            
            # جلب البيانات
            daily_candles = exchange.fetch_ohlcv(symbol, timeframe='1d', limit=7)
            hourly_candles = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=24)
            
            # صياغة رسالة الذكاء الاصطناعي
            prompt = f"""
            أنت محلل مالي محترف. قم بتحليل حركة السعر لعملة {symbol}.
            بيانات الشموع اليومية (آخر 7 أيام): {daily_candles}
            بيانات الشموع كل ساعة (آخر 24 ساعة): {hourly_candles}
            استخرج النماذج الفنية الدقيقة، حدد اتجاه السوق، وأعطني توصية واضحة.
            يجب أن تنتهي إجابتك بكلمة واحدة فقط في السطر الأخير: (BUY أو SELL أو WAIT).
            """
            
            # إرسال البيانات للتحليل
            completion = client.chat.completions.create(
              model="deepseek-ai/deepseek-v4-pro",
              messages=[{"role": "user", "content": prompt}],
              temperature=1,
              top_p=0.95,
              max_tokens=16384,
              extra_body={"chat_template_kwargs": {"thinking": False}},
              stream=False
            )
            
            # استخراج النتيجة
            analysis_result = completion.choices[0].message.content
            print(f"التحليل:\n{analysis_result}")
            
            # استخراج الكلمة الأخيرة من رد الذكاء الاصطناعي لمعرفة القرار
            words = analysis_result.strip().split()
            decision = words[-1].upper().replace(".", "").replace(",", "") if words else "WAIT"
            
            # تنفيذ القرار
            if decision in ["BUY", "SELL"]:
                execute_trade(symbol, decision)
            else:
                print("⏳ الذكاء الاصطناعي يوصي بالانتظار (WAIT).")
                
            print("-" * 50)
            time.sleep(1) # استراحة لحماية الاتصال من الحظر
            
        except Exception as e:
            print(f"⚠️ تخطي {symbol} بسبب خطأ: {e}")

# ==========================================
# 4. نقطة الانطلاق (التشغيل المستمر)
# ==========================================
if __name__ == "__main__":
    while True:
        print("🔄 بدء دورة مسح رادارية جديدة للسوق بالكامل...")
        fetch_and_analyze()
        print("✅ انتهت الدورة. البوت سيرتاح لمدة 5 دقائق قبل المسح التالي...")
        time.sleep(300) # استراحة 5 دقائق قبل إعادة مسح السوق مرة أخرى
