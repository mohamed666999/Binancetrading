"""
Adapter layer for conor19w strategies (Project B).

Why: keep all external-strategy handling isolated. Adapters map Project B
strategy functions to the standardized Signal object and never perform
order execution. The adapter tolerates differing function signatures in
Project B by attempting common calling patterns and returning a
HOLD signal on any errors.

Important:
- This file only depends on the curated strategies/external/conor19w/TradingStrats.py
  which is a sandboxed copy of strategy logic (no execution code copied).
- Default behavior: lightweight heuristics for confidence mapping. You can
  refine per-strategy confidence later.
"""
from typing import List, Dict, Any

from signals.signal import Signal

# import the sandboxed, isolated copy of Project B strategies
from strategies.external.conor19w import TradingStrats


def _prepare_arrays(candles: List[Dict[str, Any]]):
    # candles: list of {open,high,low,close,volume,timestamp}
    closes = [c["close"] for c in candles]
    opens = [c["open"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c.get("volume", 0) for c in candles]
    return opens, highs, lows, closes, volumes


def _map_raw_direction(raw):
    # Project B historically uses: 1 for BUY, 0 for SELL, -99 for no-signal
    try:
        if raw is None:
            return "HOLD"
        if isinstance(raw, str):
            r = raw.upper()
            if r in ("BUY", "LONG"): return "BUY"
            if r in ("SELL", "SHORT"): return "SELL"
            return "HOLD"
        if isinstance(raw, (int, float)):
            if int(raw) == 1:
                return "BUY"
            if int(raw) == 0:
                return "SELL"
            return "HOLD"
        return "HOLD"
    except Exception:
        return "HOLD"


def _default_confidence(mapped_decision: str) -> int:
    if mapped_decision == "HOLD":
        return 10
    return 60


def call_strategy_by_name(name: str, candles: List[Dict], extra: Dict = None) -> Signal:
    """Generic caller that tries several common signatures found in TradingStrats.py.

    - name: function name in strategies.external.conor19w.TradingStrats
    - candles: OHLCV list with last element as current candle
    - extra: optional dict for additional arrays (ema arrays, indicators) if precomputed
    """
    extra = extra or {}
    opens, highs, lows, closes, volumes = _prepare_arrays(candles)
    idx = len(candles) - 1

    fn = getattr(TradingStrats, name, None)
    if fn is None:
        return Signal(decision="HOLD", confidence=0, strategy_name=f"conor19w.{name}", reason="not_found")

    # Try common calling patterns from the original file. We keep attempts limited and safe.
    attempts = []
    # Pattern 1: (Trade_Direction, Close, Open, High, Low, current_index)
    attempts.append(("pattern1", (0, closes, opens, highs, lows, idx)))
    # Pattern 2: (Trade_Direction, EMAshort, EMAlong, current_index) or similar - skip for generic
    attempts.append(("pattern2", (closes, idx)))
    # Pattern 3: (Close, Trade_Direction, EMA50, EMA14, EMA8, fastd, fastk, current_index)
    attempts.append(("pattern3", (closes, 0, [], [], [], [], [], idx)))
    # Pattern 4: (Trade_Direction, fastd, fastk, RSI, MACD, macdsignal, current_index)
    attempts.append(("pattern4", (0, [], [], [], [], [], idx)))

    raw = None
    last_exc = None
    for tag, args in attempts:
        try:
            res = fn(*args)
            raw = res
            break
        except TypeError as te:
            last_exc = te
            continue
        except Exception as e:
            # Runtime error inside a strategy should not crash the master bot
            return Signal(decision="HOLD", confidence=0, strategy_name=f"conor19w.{name}", reason=f"runtime_error: {e}")

    # Interpret raw result
    # Many functions return either an int direction or (direction, sl, tp) tuples.
    mapped = "HOLD"
    if raw is None:
        mapped = "HOLD"
    else:
        if isinstance(raw, tuple) or isinstance(raw, list):
            if len(raw) >= 1:
                mapped = _map_raw_direction(raw[0])
        else:
            mapped = _map_raw_direction(raw)

    conf = _default_confidence(mapped)
    reason = f"wrapped from conor19w.{name} raw={raw}"

    return Signal(decision=mapped, confidence=conf, strategy_name=f"conor19w.{name}", reason=reason)


def list_available_strategies() -> List[str]:
    # Return public callables found in TradingStrats module (best-effort)
    return [n for n in dir(TradingStrats) if not n.startswith("_") and callable(getattr(TradingStrats, n))]
