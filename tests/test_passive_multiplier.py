from adapters.conor19w_adapter import call_strategy_by_name, PASSIVE_WEIGHT_MULTIPLIER


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


def test_passive_multiplier_effect():
    """Ensure the passive multiplier increases or preserves confidence.

    The test toggles the global multiplier between 1.0 and the configured value
    and checks that confidence does not decrease when multiplier is larger.
    """
    import adapters.conor19w_adapter as adapter

    candles = make_synthetic_candles()

    # Pick a strategy that exists in the sandbox (best-effort)
    strategies = adapter.list_available_strategies()
    if not strategies:
        # Nothing to test
        return
    sname = strategies[0]

    # Save and restore multiplier
    old = adapter.PASSIVE_WEIGHT_MULTIPLIER
    try:
        adapter.PASSIVE_WEIGHT_MULTIPLIER = 1.0
        s1 = call_strategy_by_name(sname, candles)

        adapter.PASSIVE_WEIGHT_MULTIPLIER = max(1.0, PASSIVE_WEIGHT_MULTIPLIER)
        s2 = call_strategy_by_name(sname, candles)

        assert s2.confidence >= s1.confidence
    finally:
        adapter.PASSIVE_WEIGHT_MULTIPLIER = old
