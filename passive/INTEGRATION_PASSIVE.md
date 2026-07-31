# Integration notes for passive features added into Binancetrading

This small document describes the passive integration that was added on branch
`add/passive-integration`.

Summary
- Passive/external strategies coming from the `conor19w` sandbox are given a
  heavier influence on the aggregated decision by applying a confidence
  multiplier.
- The multiplier is hard-coded to 1.8 as requested and applied inside the
  `adapters/conor19w_adapter.py` module.

Where to look
- adapters/conor19w_adapter.py  -> adapter that wraps external strategies.
- strategies/external/conor19w/ -> sandboxed strategy implementations.
- signals/aggregator.py         -> aggregator used to combine signals (unchanged).

How the multiplier is applied
- The adapter computes the strategy's nominal confidence (10 for HOLD, 60 for
  BUY/SELL by default) and then multiplies it by the passive multiplier.
- Values are clamped to the 0-100 range.

Disabling / customization
- To disable weighting or change weights edit `adapters/conor19w_adapter.py`:
  - `PASSIVE_WEIGHT_MULTIPLIER` constant (currently 1.8)
  - `PASSIVE_WEIGHTS` dict for per-strategy fine-tuning

Testing
- A smoke test was added under `tests/test_passive_multiplier.py` to ensure the
  multiplier is applied.

Notes
- No trading logic, API keys, or live execution code were modified.
- All changes are localized to this repository and the new branch.
