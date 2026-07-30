"""
Basic smoke test for the conor19w adapter and aggregator.
This test creates synthetic candle data and checks that the adapter
and aggregator run without throwing errors.

Why: provides a quick safety check before enabling external strategies in
production. It is intentionally simple and does not assert trading logic.
"""
from adapters.conor19w_adapter import call_strategy_by_name, list_available_strategies
from signals.aggregator import aggregate_signals


def make_synthetic_candles(n=100, start_price=100.0):
    candles = []
    p = start_price
    for i in range(n):
        o = p
        c = p + (0.5 - (i % 3) * 0.01)  # small variation
        h = max(o, c) + 0.2
        l = min(o, c) - 0.2
        candles.append({"open": o, "high": h, "low": l, "close": c, "volume": 1000 + i})
        p = c
    return candles


if __name__ == "__main__":
    candles = make_synthetic_candles()
    # list strategies available in sandbox
    print("available:", list_available_strategies())

    # call a couple of strategies
    s1 = call_strategy_by_name("candle_wick", candles)
    s2 = call_strategy_by_name("EMA_cross", candles)
    print(s1)
    print(s2)

    # aggregate
    dec, conf, details = aggregate_signals([s1, s2])
    print("aggregated:", dec, conf)
