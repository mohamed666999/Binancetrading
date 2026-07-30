# Integration notes and safe rollout plan

This document explains what was added to the repository and how to enable the
external Project B strategies in a safe, non-invasive way.

Files added (new):
- signals/signal.py                -> Signal dataclass and validators
- signals/aggregator.py           -> Weighted-majority aggregator
- adapters/conor19w_adapter.py    -> Adapter layer exposing call_strategy_by_name()
- strategies/external/conor19w/   -> Sandbox copy of selected strategy functions
  - TradingStrats.py
  - README.md
- INTEGRATION.md                   -> (this file)

Design goals followed:
- Project A remains master controller: no execution, risk manager or AI code
  was modified. New code is isolated under strategies/ and adapters/.
- Default state is "disabled". Enabling requires explicit configuration.
- Every adapter returns a standardized Signal object: {decision, confidence, strategy_name, reason}

How to enable (manual steps):
1. Edit your Project A config to add an "external_strategies" block. Example:

external_strategies:
  conor19w:
    enabled: false  # set true to enable
    aggregator: weighted_majority
    min_confidence: 30
    strategies: ["candle_wick","EMA_cross"]

2. In the scan/analysis path in your bot (before final execution) call:

from adapters.conor19w_adapter import call_strategy_by_name, list_available_strategies
from signals.aggregator import aggregate_signals

# collect signals
signals = []
for sname in enabled_list:
    sig = call_strategy_by_name(sname, candles_for_symbol)
    signals.append(sig)

pb_decision, pb_conf, details = aggregate_signals(signals)

# Then apply consensus rule with AI decision (existing code)

3. Use dry-run and logging to validate results before allowing live trades.

Rollback
- To disable the feature simply set external_strategies.conor19w.enabled=false
  or remove the package folders. No other files are modified by the integration.

License / attribution
- Strategy code was extracted from the conor19w repository. Please ensure
  any license obligations are observed. A short attribution header is included
  in strategies/external/conor19w/TradingStrats.py

