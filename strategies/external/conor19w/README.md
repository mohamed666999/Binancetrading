# Sandbox strategy package for Project B (conor19w)
#
# This directory holds an isolated copy of strategy logic extracted from
# conor19w/Binance-Futures-Trading-Bot. It is intentionally namespaced to
# avoid accidental import of execution, logging, or config components.
#
# Attribution:
#   Original: https://github.com/conor19w/Binance-Futures-Trading-Bot
#   Please retain this notice when editing or extending these strategies.

# List of example strategy entry points present in TradingStrats.py
# - candle_wick
# - EMA_cross
# - stochBB
# - breakout
# - StochRSIMACD
# - tripleEMAStochasticRSIATR

# WARNING: These functions are for signal generation only. They must NOT
# perform any trading actions, HTTP calls, or file writes. Any such code
# was intentionally removed during extraction.
