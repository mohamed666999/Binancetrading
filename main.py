import json
import ccxt
import time
from threading import Thread
from flask import Flask, request, jsonify
from openai import OpenAI

app = Flask(__name__)

# ==========================================
# 1. إعداد سيرفر keep‑alive (مدمج مع الويبهوك)
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
    print(f"🔌 تم تفعيل الوكيل: {PROXY_URL}")

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
# 4. دالة بناء الـ prompt وطلب التحليل من NVIDIA
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
            extra_body={"chat_template_kwargs": {"thinking": False}}  # حسب إعدادات NVIDIA
        )
        text = (resp.choices[0].message.content or "").strip()
        return json.loads(text)
    except Exception as e:
        print(f"NVIDIA AI parsing error: {e}")
        # fallback بسيط
        upper = text.upper() if 'text' in locals() else ""
        if "LONG" in upper:
            decision = "LONG"
        elif "SHORT" in upper:
            decision = "SHORT"
        else:
            decision = "WAIT"
        return {"decision": decision, "confidence": 50, "reason": f"fallback: {text[:200]}"}

# ==========================================
# 5. دالة تنفيذ الصفقة على Binance
# ==========================================
def execute_trade(symbol: str, decision: str):
    LEVERAGE = 10
    TRADE_MARGIN = 10  # 10 USDT هامش

    try:
        print(f"🚀 فتح مركز {decision} على {symbol}")
        ticker = exchange.fetch_ticker(symbol)
        current_price = ticker['last']

        notional_value = TRADE_MARGIN * LEVERAGE
        raw_amount = notional_value / current_price
        position_size = float(exchange.amount_to_precision(symbol, raw_amount))

        side = "buy" if decision == "LONG" else "sell"

        print(f"💰 السعر: {current_price} | 📦 الكمية: {position_size}")
        exchange.set_leverage(LEVERAGE, symbol)
        order = exchange.create_market_order(symbol, side, position_size)

        print("✅ تم فتح المركز بنجاح!")
        return {"executed": True, "order": order}

    except Exception as e:
        print(f"❌ فشل فتح الصفقة: {e}")
        return {"executed": False, "error": str(e)}

# ==========================================
# 6. نقطة نهاية الويبهوك
# ==========================================
@app.post("/webhook")
def webhook():
    try:
        payload = request.get_json(force=True, silent=False)
        print(f"📩 إشارة واردة: {payload.get('symbol')}")

        # 1. اسأل NVIDIA AI عن القرار
        ai_result = ask_ai(payload)
        decision = str(ai_result.get("decision", "WAIT")).upper()

        # 2. نفذ الصفقة إذا لم تكن WAIT
        trade_result = None
        if decision in ("LONG", "SHORT"):
            symbol = payload.get("symbol", "BTC/USDT:USDT")
            trade_result = execute_trade(symbol, decision)

        return jsonify({
            "success": True,
            "received": payload,
            "ai_result": ai_result,
            "trade_result": trade_result
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

# ==========================================
# 7. تشغيل التطبيق
# ==========================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
