import json
import ccxt
import time
from flask import Flask, request, jsonify
from openai import OpenAI

app = Flask(__name__)

# ==========================================
# 1. إعداد سيرفر keep‑alive
# ==========================================
@app.route('/')
def keep_alive():
    return "🤖 رادار الذكاء الاصطناعي يعمل بنجاح ولا ينام!"

# ==========================================
# 2. إعدادات الاتصال (Binance + وكيل)
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
    print(f"🔌 تم تفعيل الوكيل: {PROXY_URL}", flush=True)

exchange = ccxt.binance(exchange_params)

# ==========================================
# 3. إعداد عميل NVIDIA AI
# ==========================================
client = OpenAI(
    base_url="https://integrate.api.nvidia.com/v1",
    api_key="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"
)

MODEL_NAME = "deepseek-ai/deepseek-v4-pro"   # موديل NVIDIA المتاح

# ==========================================
# 4. دوال مساعدة
# ==========================================

def normalize_symbol(symbol: str) -> str:
    """تحويل الرمز إلى صيغة Binance للعقود الدائمة (مثلاً BTCUSDT -> BTC/USDT:USDT)"""
    if ":" in symbol:
        return symbol
    # إذا كان مثل BTCUSDT
    if "/" not in symbol:
        symbol = symbol.upper().replace("USDT", "/USDT")
        return symbol + ":USDT"
    return symbol

def has_open_position(symbol: str) -> bool:
    """التحقق من وجود مركز مفتوح على العملة"""
    try:
        positions = exchange.fetch_positions([symbol])
        for pos in positions:
            if float(pos['contracts']) != 0:
                print(f"⚠️ يوجد مركز مفتوح بالفعل على {symbol} ({pos['side']} {pos['contracts']})", flush=True)
                return True
        return False
    except Exception as e:
        print(f"⚠️ تعذر التحقق من المراكز: {e}", flush=True)
        return False

# ==========================================
# 5. بناء الـ prompt وطلب التحليل من NVIDIA
# ==========================================
def build_prompt(payload: dict) -> str:
    return f"""
أنت محلل تداول فني محترف.

حلّل البيانات التالية وأعد قرارًا واحدًا فقط من:
LONG
SHORT
WAIT

أعد JSON فقط بهذا الشكل:
{{
  "decision": "LONG|SHORT|WAIT",
  "confidence": 0-100,
  "reason": "short reason"
}}

البيانات:
symbol: {payload.get("symbol")}
timeframe: {payload.get("timeframe")}
lookback: {payload.get("lookback")}
emaFast: {payload.get("emaFast")}
emaSlow: {payload.get("emaSlow")}
rsi: {payload.get("rsi")}

open: {payload.get("open")}
high: {payload.get("high")}
low: {payload.get("low")}
close: {payload.get("close")}
volume: {payload.get("volume")}
""".strip()

def ask_ai(payload: dict) -> dict:
    prompt = build_prompt(payload)
    print("🧠 إرسال البيانات إلى AI...", flush=True)
    try:
        resp = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": prompt},
            ],
            stream=False,
            temperature=0,
            max_tokens=120,
            timeout=30,
            extra_body={"chat_template_kwargs": {"thinking": False}}
        )
        text = (resp.choices[0].message.content or "").strip()
        result = json.loads(text)
        print(f"✅ AI رد: {result}", flush=True)
        return result
    except Exception as e:
        print(f"⚠️ خطأ في AI أو JSON: {e}", flush=True)
        # fallback
        upper = text.upper() if 'text' in locals() else ""
        if "LONG" in upper:
            decision = "LONG"
        elif "SHORT" in upper:
            decision = "SHORT"
        else:
            decision = "WAIT"
        return {"decision": decision, "confidence": 50, "reason": "fallback"}

# ==========================================
# 6. تنفيذ الصفقة + TP/SL
# ==========================================
def execute_trade(symbol: str, decision: str):
    LEVERAGE = 10
    TRADE_MARGIN = 10      # 10 USDT هامش
    TP_PERCENT = 2.0       # جني ربح 2%
    SL_PERCENT = 1.0       # وقف خسارة 1%

    try:
        print(f"🔌 الاتصال بـ Binance للتنفيذ...", flush=True)
        exchange.load_markets()
        ticker = exchange.fetch_ticker(symbol)
        current_price = ticker['last']
        print(f"💰 السعر الحالي: {current_price}", flush=True)

        notional_value = TRADE_MARGIN * LEVERAGE
        raw_amount = notional_value / current_price
        position_size = float(exchange.amount_to_precision(symbol, raw_amount))
        side = "buy" if decision == "LONG" else "sell"

        print(f"📦 الكمية: {position_size} | قيمة المركز: {notional_value} USDT", flush=True)

        # ضبط الرافعة
        exchange.set_leverage(LEVERAGE, symbol)

        # فتح المركز
        print("🚀 إرسال أمر فتح المركز...", flush=True)
        order = exchange.create_market_order(symbol, side, position_size)
        print(f"✅ تم فتح {decision} بنجاح!", flush=True)
        print(order, flush=True)

        # --- إعداد TP/SL ---
        try:
            if decision == "LONG":
                tp_price = current_price * (1 + TP_PERCENT / 100)
                sl_price = current_price * (1 - SL_PERCENT / 100)
            else:  # SHORT
                tp_price = current_price * (1 - TP_PERCENT / 100)
                sl_price = current_price * (1 + SL_PERCENT / 100)

            # جني الأرباح (TAKE_PROFIT_MARKET)
            exchange.create_order(
                symbol,
                'TAKE_PROFIT_MARKET',
                'sell' if decision == 'LONG' else 'buy',  # إغلاق
                position_size,
                None,
                {
                    'stopPrice': round(tp_price, 2),
                    'reduceOnly': True
                }
            )
            print(f"✅ تم وضع TP عند {tp_price:.2f}", flush=True)

            # وقف الخسارة (STOP_MARKET)
            exchange.create_order(
                symbol,
                'STOP_MARKET',
                'sell' if decision == 'LONG' else 'buy',
                position_size,
                None,
                {
                    'stopPrice': round(sl_price, 2),
                    'reduceOnly': True
                }
            )
            print(f"✅ تم وضع SL عند {sl_price:.2f}", flush=True)

        except Exception as tp_err:
            print(f"⚠️ فشل في تعيين TP/SL: {tp_err}", flush=True)

        return {"executed": True, "order": order}

    except Exception as e:
        print(f"❌ فشل فتح الصفقة: {e}", flush=True)
        return {"executed": False, "error": str(e)}

# ==========================================
# 7. نقطة نهاية الويبهوك (TradingView → AI → Binance)
# ==========================================
@app.post("/webhook")
def webhook():
    print("\n📡 Webhook من TradingView وصل", flush=True)
    try:
        payload = request.get_json(force=True, silent=False)
        print(f"📊 البيانات المستلمة: {payload}", flush=True)

        # تطبيع الرمز
        raw_symbol = payload.get("symbol", "BTC/USDT:USDT")
        symbol = normalize_symbol(raw_symbol)
        print(f"🎯 الرمز المستخدم: {symbol}", flush=True)

        # 1. منع تكرار الصفقة
        if has_open_position(symbol):
            print("⛔ تخطي: يوجد مركز مفتوح بالفعل.", flush=True)
            return jsonify({"success": True, "reason": "position_already_open"})

        # 2. تحليل AI
        ai_result = ask_ai(payload)
        decision = str(ai_result.get("decision", "WAIT")).upper()
        print(f"🧠 قرار AI: {decision} (الثقة: {ai_result.get('confidence', '?')}%)", flush=True)

        # 3. تنفيذ الصفقة إذا لم تكن WAIT
        trade_result = None
        if decision in ("LONG", "SHORT"):
            trade_result = execute_trade(symbol, decision)
        else:
            print("⏳ القرار WAIT - لا توجد صفقة.", flush=True)

        return jsonify({
            "success": True,
            "received": payload,
            "ai_result": ai_result,
            "trade_result": trade_result
        })

    except Exception as e:
        print(f"❌ خطأ عام: {e}", flush=True)
        return jsonify({"success": False, "error": str(e)}), 500

# ==========================================
# 8. تشغيل التطبيق
# ==========================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
