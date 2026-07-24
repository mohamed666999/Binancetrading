import re
import json
import time
import hmac
import hashlib
import logging
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlencode

import requests
from flask import Flask, request, jsonify
from openai import OpenAI

# =========================================================
# إعداد السجلات
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

app = Flask(__name__)

# =========================================================
# المفاتيح الثابتة (مباشرة بدلاً من os.getenv)
# =========================================================
BINANCE_API_KEY = "IX7kLH0ssWHP5TpYMUGcp0pzq4LX4Lqi7m4XtlqMkkq6DCZAsLhoeYZ3533jJFF4"
BINANCE_API_SECRET = "LmICnpSpMxL1riv4RfIf0HBGRfhDTP5JhDUYdlPSukpqV7kDTonrZ0j3DWp1a7hU"
DEEPSEEK_API_KEY = "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"
DEEPSEEK_MODEL = "deepseek-ai/deepseek-v4-pro"

# اختياري (يمكن تركه فارغاً)
WEBHOOK_SECRET = ""
BINANCE_HEDGE_MODE = False

# =========================================================
# الاتصالات
# =========================================================
DEEPSEEK_CLIENT = OpenAI(
    api_key=DEEPSEEK_API_KEY,
    base_url="https://integrate.api.nvidia.com/v1"  # استخدام NVIDIA API
)

BINANCE_BASE_URL = "https://fapi.binance.com"

LEVERAGE = 10
DEFAULT_AMOUNT_USDT = Decimal("10")
MIN_NOTIONAL_USDT = Decimal("5")

# =========================================================
# أدوات مساعدة
# =========================================================
def log_step(trace, message, data=None):
    line = message
    if data is not None:
        try:
            line += " | " + json.dumps(data, ensure_ascii=False, default=str)[:1200]
        except Exception:
            line += f" | {str(data)[:1200]}"
    logging.info(line)
    trace.append(line)


def normalize_symbol(raw_symbol: str) -> str:
    """
    يقبل:
    - BTCUSDT
    - BINANCE:BTCUSDT
    - BINANCE:BTCUSDT.P
    - BTC/USDT
    - BTC/USDT:USDT

    ويرجع:
    - BTCUSDT
    """
    s = (raw_symbol or "BTCUSDT").upper().strip()
    if ":" in s:
        s = s.split(":")[-1]
    s = s.replace(".P", "").replace(".PINE", "")
    s = s.replace("/", "")
    return s


def public_binance_get(path, params=None):
    params = params or {}
    url = f"{BINANCE_BASE_URL}{path}"
    r = requests.get(url, params=params, timeout=15)
    try:
        return r.json()
    except Exception:
        return {"raw": r.text, "status_code": r.status_code}


def signed_binance_request(method: str, path: str, params=None):
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        raise RuntimeError("Binance API key/secret missing")

    params = dict(params or {})
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000

    query = urlencode(params, doseq=True)
    signature = hmac.new(
        BINANCE_API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    url = f"{BINANCE_BASE_URL}{path}?{query}&signature={signature}"
    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY
    }

    r = requests.request(method.upper(), url, headers=headers, timeout=15)

    try:
        return r.json()
    except Exception:
        return {
            "raw": r.text,
            "status_code": r.status_code
        }


def get_symbol_info(symbol: str):
    data = public_binance_get("/fapi/v1/exchangeInfo", {"symbol": symbol})
    symbols = data.get("symbols", [])
    if not symbols:
        return None
    return symbols[0]


def get_current_price(symbol: str):
    data = public_binance_get("/fapi/v1/ticker/price", {"symbol": symbol})
    if "price" not in data:
        raise RuntimeError(f"فشل جلب السعر: {data}")
    return Decimal(str(data["price"]))


def get_position(symbol: str, trace):
    """
    يرجع:
    {
      "side": None | "LONG" | "SHORT",
      "qty": Decimal,
      "raw": dict|None
    }
    """
    data = signed_binance_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol})
    log_step(trace, "📥 رد Binance للمركز الحالي", data)

    if isinstance(data, dict) and data.get("code") not in (None, 200, 0):
        raise RuntimeError(f"فشل جلب المركز: {data}")

    rows = data if isinstance(data, list) else data.get("rows") or data.get("result") or []
    if isinstance(rows, dict):
        rows = [rows]

    for row in rows:
        if row.get("symbol") == symbol:
            amt = Decimal(str(row.get("positionAmt", "0")))
            if amt > 0:
                return {"side": "LONG", "qty": amt, "raw": row}
            if amt < 0:
                return {"side": "SHORT", "qty": abs(amt), "raw": row}

    return {"side": None, "qty": Decimal("0"), "raw": None}


def set_leverage(symbol: str, leverage: int, trace):
    log_step(trace, f"⚙️ ضبط الرافعة على {leverage}x")
    data = signed_binance_request("POST", "/fapi/v1/leverage", {
        "symbol": symbol,
        "leverage": leverage
    })
    log_step(trace, "📥 رد ضبط الرافعة", data)
    return data


def calculate_quantity(symbol: str, amount_usdt: Decimal, price: Decimal, trace):
    info = get_symbol_info(symbol)
    if not info:
        raise RuntimeError(f"لم يتم العثور على معلومات الرمز: {symbol}")

    lot_filter = None
    for f in info.get("filters", []):
        if f.get("filterType") == "LOT_SIZE":
            lot_filter = f
            break

    if not lot_filter:
        raise RuntimeError(f"لم يتم العثور على LOT_SIZE للرمز: {symbol}")

    step_size = Decimal(str(lot_filter["stepSize"]))
    min_qty = Decimal(str(lot_filter["minQty"]))

    notional = amount_usdt * Decimal(str(LEVERAGE))
    raw_qty = notional / price

    steps = (raw_qty / step_size).to_integral_value(rounding=ROUND_DOWN)
    qty = steps * step_size

    if qty < min_qty:
        qty = min_qty

    order_value = qty * price

    # ضمان الحد الأدنى المالي
    if order_value < MIN_NOTIONAL_USDT:
        qty = ((MIN_NOTIONAL_USDT / price) / step_size).to_integral_value(rounding=ROUND_DOWN) * step_size
        if qty < min_qty:
            qty = min_qty
        order_value = qty * price

    log_step(trace, "📦 حساب الكمية", {
        "amount_usdt": str(amount_usdt),
        "leverage": LEVERAGE,
        "price": str(price),
        "qty": str(qty),
        "order_value_usdt": str(order_value),
        "min_qty": str(min_qty),
        "step_size": str(step_size)
    })

    return qty, order_value


def open_market_order(symbol: str, action: str, qty: Decimal, trace):
    side = "BUY" if action == "LONG" else "SELL"

    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": format(qty, "f"),
        "newOrderRespType": "RESULT"
    }

    if BINANCE_HEDGE_MODE:
        params["positionSide"] = "LONG" if action == "LONG" else "SHORT"

    log_step(trace, "📤 إرسال أمر السوق إلى Binance", params)
    data = signed_binance_request("POST", "/fapi/v1/order", params)
    log_step(trace, "📥 رد Binance لأمر السوق", data)
    return data


def close_position(symbol: str, current_side: str, qty: Decimal, trace):
    # يغلق Long ببيع، ويغلق Short بشراء
    close_side = "SELL" if current_side == "LONG" else "BUY"

    params = {
        "symbol": symbol,
        "side": close_side,
        "type": "MARKET",
        "quantity": format(qty, "f"),
        "reduceOnly": "true",
        "newOrderRespType": "RESULT"
    }

    if BINANCE_HEDGE_MODE:
        params["positionSide"] = "LONG" if current_side == "LONG" else "SHORT"

    log_step(trace, "🔄 إغلاق المركز الحالي", params)
    data = signed_binance_request("POST", "/fapi/v1/order", params)
    log_step(trace, "📥 رد Binance عند الإغلاق", data)
    return data


def build_deepseek_prompt(payload: dict) -> str:
    candles = payload.get("candles")
    if isinstance(candles, list) and candles:
        candles_text = json.dumps(candles[-20:], ensure_ascii=False, default=str)
    else:
        candles_text = json.dumps({
            "open": payload.get("open"),
            "high": payload.get("high"),
            "low": payload.get("low"),
            "close": payload.get("close"),
            "volume": payload.get("volume")
        }, ensure_ascii=False, default=str)

    indicators = {
        "emaFast": payload.get("emaFast"),
        "emaSlow": payload.get("emaSlow"),
        "rsi": payload.get("rsi"),
        "macd": payload.get("macd"),
        "signal": payload.get("signal"),
        "atr": payload.get("atr")
    }

    return f"""
أنت محلل تداول فني محترف لعقود USDT Perpetual.
حلل البيانات القادمة من TradingView ثم أعطِ قرارًا واحدًا فقط من:
LONG
SHORT
WAIT

أعد JSON فقط وبالشكل التالي:
{{
  "decision": "LONG|SHORT|WAIT",
  "confidence": 0-100,
  "reason": "short reason"
}}

ممنوع الشرح الطويل.
ممنوع أي نص خارج JSON.

المدخلات:
symbol: {payload.get("symbol")}
timeframe: {payload.get("timeframe")}
tradingview_action_hint: {payload.get("action")}
amount_usdt: {payload.get("amount_usdt")}

indicators:
{json.dumps(indicators, ensure_ascii=False, default=str)}

candles_snapshot:
{candles_text}
""".strip()


def parse_deepseek_output(text: str) -> dict:
    raw = (text or "").strip()

    try:
        obj = json.loads(raw)
        decision = str(obj.get("decision", "WAIT")).upper()
        if decision not in ("LONG", "SHORT", "WAIT"):
            decision = "WAIT"
        obj["decision"] = decision
        return obj
    except Exception:
        pass

    m = re.search(r"\{.*\}", raw, re.S)
    if m:
        try:
            obj = json.loads(m.group(0))
            decision = str(obj.get("decision", "WAIT")).upper()
            if decision not in ("LONG", "SHORT", "WAIT"):
                decision = "WAIT"
            obj["decision"] = decision
            return obj
        except Exception:
            pass

    upper = raw.upper()
    if "LONG" in upper:
        decision = "LONG"
    elif "SHORT" in upper:
        decision = "SHORT"
    else:
        decision = "WAIT"

    return {
        "decision": decision,
        "confidence": 50,
        "reason": raw[:400]
    }


def ask_deepseek(payload: dict, trace):
    if not DEEPSEEK_CLIENT:
        raise RuntimeError("DEEPSEEK_API_KEY غير موجود")

    prompt = build_deepseek_prompt(payload)
    log_step(trace, "🤖 إرسال البيانات إلى DeepSeek", {
        "symbol": payload.get("symbol"),
        "timeframe": payload.get("timeframe"),
        "has_candles": bool(payload.get("candles")),
    })

    resp = DEEPSEEK_CLIENT.chat.completions.create(
        model=DEEPSEEK_MODEL,
        messages=[
            {"role": "system", "content": "Return JSON only."},
            {"role": "user", "content": prompt}
        ],
        temperature=0,
        max_tokens=120,
        stream=False,
        extra_body={"chat_template_kwargs": {"thinking": False}}
    )

    raw = resp.choices[0].message.content or ""
    log_step(trace, "✅ وصل رد DeepSeek", {"raw_preview": raw[:800]})
    parsed = parse_deepseek_output(raw)
    log_step(trace, "🧠 قرار DeepSeek النهائي", parsed)
    return parsed, raw


def execute_trade(symbol: str, decision: str, amount_usdt: Decimal, trace):
    current_position = get_position(symbol, trace)
    log_step(trace, "📌 حالة المركز قبل التنفيذ", current_position)

    if current_position["side"] == decision:
        log_step(trace, "⏭️ يوجد مركز من نفس الاتجاه بالفعل، لن نكرر الصفقة")
        return {
            "skipped": True,
            "reason": "same_direction_position_exists",
            "position": {
                "side": current_position["side"],
                "qty": str(current_position["qty"])
            }
        }

    if current_position["side"] and current_position["side"] != decision and current_position["qty"] > 0:
        close_position(symbol, current_position["side"], current_position["qty"], trace)
        time.sleep(1)

    price = get_current_price(symbol)
    log_step(trace, "💲 السعر الحالي", {"symbol": symbol, "price": str(price)})

    qty, order_value = calculate_quantity(symbol, amount_usdt, price, trace)

    set_leverage(symbol, LEVERAGE, trace)

    order = open_market_order(symbol, decision, qty, trace)

    return {
        "opened": True,
        "symbol": symbol,
        "decision": decision,
        "price": str(price),
        "qty": str(qty),
        "order_value_usdt": str(order_value),
        "binance_order": order
    }

# =========================================================
# Routes
# =========================================================
@app.get("/")
def home():
    return {
        "status": "online",
        "service": "TradingView -> DeepSeek -> Binance Futures"
    }


@app.get("/health")
def health():
    return jsonify({
        "status": "ok",
        "env": {
            "binance_api_key": bool(BINANCE_API_KEY),
            "binance_api_secret": bool(BINANCE_API_SECRET),
            "deepseek_api_key": bool(DEEPSEEK_API_KEY),
            "webhook_secret": bool(WEBHOOK_SECRET)
        },
        "settings": {
            "leverage": LEVERAGE,
            "default_amount_usdt": str(DEFAULT_AMOUNT_USDT),
            "binance_hedge_mode": BINANCE_HEDGE_MODE,
            "deepseek_model": DEEPSEEK_MODEL
        }
    })


@app.post("/webhook")
def webhook():
    trace = []

    try:
        payload = request.get_json(silent=True) or {}
        log_step(trace, "📩 وصول Webhook من TradingView", payload)

        # تحقق اختياري
        if WEBHOOK_SECRET:
            incoming_secret = (
                payload.get("secret")
                or request.headers.get("X-Webhook-Secret")
                or ""
            )
            if incoming_secret != WEBHOOK_SECRET:
                log_step(trace, "⛔ فشل التحقق من السر")
                return jsonify({
                    "success": False,
                    "error": "unauthorized",
                    "trace": trace
                }), 401

        symbol = normalize_symbol(payload.get("symbol", "BTCUSDT"))
        action_hint = str(payload.get("action", "ANALYZE")).upper()
        amount_usdt = Decimal(str(payload.get("amount_usdt", DEFAULT_AMOUNT_USDT)))

        if amount_usdt <= 0:
            amount_usdt = DEFAULT_AMOUNT_USDT

        payload["symbol"] = symbol
        payload["amount_usdt"] = str(amount_usdt)
        payload["action"] = action_hint

        log_step(trace, "🧹 بعد التطبيع", {
            "symbol": symbol,
            "action_hint": action_hint,
            "amount_usdt": str(amount_usdt)
        })

        # إرسال البيانات إلى DeepSeek
        ds_result, ds_raw = ask_deepseek(payload, trace)

        decision = str(ds_result.get("decision", "WAIT")).upper()

        result = {
            "success": True,
            "received": payload,
            "deepseek": {
                "decision": decision,
                "confidence": ds_result.get("confidence"),
                "reason": ds_result.get("reason"),
                "raw": ds_raw[:2000]
            },
            "trace": trace
        }

        # تنفيذ الصفقة فقط إذا كانت LONG/SHORT
        if decision in ("LONG", "SHORT"):
            log_step(trace, f"🚀 قرار تنفيذ: {decision}")
            trade_result = execute_trade(
                symbol=symbol,
                decision=decision,
                amount_usdt=amount_usdt,
                trace=trace
            )
            result["trade"] = trade_result
        else:
            log_step(trace, "⏳ DeepSeek أعطى WAIT، لن يتم فتح صفقة")
            result["trade"] = {"skipped": True, "reason": "WAIT"}

        result["trace"] = trace
        return jsonify(result)

    except Exception as e:
        log_step(trace, "❌ خطأ في /webhook", {
            "type": type(e).__name__,
            "error": str(e)
        })
        return jsonify({
            "success": False,
            "error": str(e),
            "trace": trace
        }), 500


@app.get("/test-connections")
def test_connections():
    trace = []
    try:
        log_step(trace, "🔎 فحص DeepSeek credentials", {
            "deepseek_key": bool(DEEPSEEK_API_KEY),
            "model": DEEPSEEK_MODEL
        })

        log_step(trace, "🔎 فحص Binance public ping")
        ping = public_binance_get("/fapi/v1/ping")
        time_api = public_binance_get("/fapi/v1/time")

        log_step(trace, "📥 رد Binance ping", ping)
        log_step(trace, "📥 رد Binance time", time_api)

        deepseek_ok = False
        deepseek_preview = None

        if DEEPSEEK_CLIENT:
            log_step(trace, "🤖 تجربة DeepSeek صغيرة")
            resp = DEEPSEEK_CLIENT.chat.completions.create(
                model=DEEPSEEK_MODEL,
                messages=[{"role": "user", "content": "Reply with OK only."}],
                temperature=0,
                max_tokens=5,
                stream=False,
                extra_body={"chat_template_kwargs": {"thinking": False}}
            )
            deepseek_preview = resp.choices[0].message.content or ""
            deepseek_ok = True
            log_step(trace, "✅ رد DeepSeek التجريبي", {"raw": deepseek_preview})

        return jsonify({
            "success": True,
            "deepseek_ok": deepseek_ok,
            "binance_public_ping": ping,
            "binance_time": time_api,
            "deepseek_preview": deepseek_preview,
            "trace": trace
        })

    except Exception as e:
        log_step(trace, "❌ فشل test-connections", {
            "type": type(e).__name__,
            "error": str(e)
        })
        return jsonify({
            "success": False,
            "error": str(e),
            "trace": trace
        }), 500


# =========================================================
# تشغيل محلي
# =========================================================
if __name__ == "__main__":
    port = 8080
    logging.info("========== STARTUP ==========")
    logging.info(f"DeepSeek key present: {bool(DEEPSEEK_API_KEY)}")
    logging.info(f"Binance key present: {bool(BINANCE_API_KEY)}")
    logging.info(f"Binance secret present: {bool(BINANCE_API_SECRET)}")
    logging.info(f"Webhook secret present: {bool(WEBHOOK_SECRET)}")
    logging.info(f"Hedge mode: {BINANCE_HEDGE_MODE}")
    logging.info(f"Leverage: {LEVERAGE}")
    logging.info(f"Default amount_usdt: {DEFAULT_AMOUNT_USDT}")
    logging.info("================================")
    app.run(host="0.0.0.0", port=port)
