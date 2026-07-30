# Sandbox copy of selected Project B strategy implementations.
#
# WHY: This file is a curated, isolated copy of strategy functions from
# conor19w/Binance-Futures-Trading-Bot TradingStrats.py. We removed
# references to LiveTradingConfig, Logger and any execution code so the
# master bot can safely import and use these functions only for signal
# generation.
#
# Attribution: original code from https://github.com/conor19w/Binance-Futures-Trading-Bot
# Please preserve this header when modifying.

# --- helpers (trimmed to the indicator helpers actually used by strategies) ---
import math


def clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, float(x)))


def _mean(v):
    return sum(v) / len(v) if v else 0.0


def _std(v):
    if len(v) < 2:
        return 0.0
    m = _mean(v)
    return math.sqrt(sum((x - m) ** 2 for x in v) / len(v))


def _ema(data, period):
    if not data or period <= 0:
        return []
    k = 2.0 / (period + 1)
    result = [data[0]]
    for i in range(1, len(data)):
        result.append(data[i] * k + result[-1] * (1 - k))
    return result


def _sma(data, period):
    if len(data) < period:
        return _mean(data)
    return _mean(data[-period:])


def _rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = _mean(gains[-period:])
    avg_loss = _mean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _macd(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow + signal:
        return 0.0, 0.0, 0.0
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    min_len = min(len(ema_fast), len(ema_slow))
    if min_len < signal:
        return 0.0, 0.0, 0.0
    macd_line = [ema_fast[-(min_len - i)] - ema_slow[-(min_len - i)] for i in range(min_len)]
    signal_line = _ema(macd_line, signal)
    if not signal_line:
        return 0.0, 0.0, 0.0
    m = macd_line[-1]
    s = signal_line[-1]
    return m, s, m - s


# --- Strategy functions (extracted and lightly adapted) ---
# For brevity we include a representative selection of the strategies found
# in the original TradingStrats.py. Adapters should call these functions and
# interpret their outputs.


def candle_wick(Trade_Direction, Close, Open, High, Low, current_index):
    try:
        if Close[current_index - 4] < Close[current_index - 3] < Close[current_index - 2] and Close[current_index - 1] < Open[current_index - 1] and (
                High[current_index - 1] - Open[current_index - 1] + Close[current_index - 1] - Low[current_index - 1]) > 10 * (Open[current_index - 1] - Close[current_index - 1]):
            Trade_Direction = 0
        elif Close[current_index - 4] > Close[current_index - 3] > Close[current_index - 2] and Close[current_index - 1] > Open[current_index - 1] and (
                High[current_index - 1] - Close[current_index - 1] + Open[current_index - 1] - Low[current_index - 1]) > 10 * (Close[current_index - 1] - Open[current_index - 1]):
            Trade_Direction = 1
    except Exception:
        # On small input lengths or index errors, return neutral
        Trade_Direction = -99
    return Trade_Direction


def EMA_cross(Trade_Direction, EMA_short, EMA_long, current_index):
    try:
        if EMA_short[current_index - 4] > EMA_long[current_index - 4] \
                and EMA_short[current_index - 3] > EMA_long[current_index - 3] \
                and EMA_short[current_index - 2] > EMA_long[current_index - 2] \
                and EMA_short[current_index - 1] > EMA_long[current_index - 1] \
                and EMA_short[current_index] < EMA_long[current_index]:
            Trade_Direction = 0

        if EMA_short[current_index - 4] < EMA_long[current_index - 4] \
                and EMA_short[current_index - 3] < EMA_long[current_index - 3] \
                and EMA_short[current_index - 2] < EMA_long[current_index - 2] \
                and EMA_short[current_index - 1] < EMA_long[current_index - 1] \
                and EMA_short[current_index] > EMA_long[current_index]:
            Trade_Direction = 1
    except Exception:
        Trade_Direction = -99
    return Trade_Direction


def stochBB(Trade_Direction, fastd, fastk, percent_B, current_index):
    try:
        percent_B1 = percent_B[current_index]
        percent_B2 = percent_B[current_index - 1]
        percent_B3 = percent_B[current_index - 2]
        if fastk[current_index] < .2 and fastd[current_index] < .2 and (fastk[current_index] > fastd[current_index] and fastk[current_index - 1] < fastd[current_index - 1]) and (
                percent_B1 < 0 or percent_B2 < 0 or percent_B3 < 0):
            Trade_Direction = 1
        elif fastk[current_index] > .8 and fastd[current_index] > .8 and (fastk[current_index] < fastd[current_index] and fastk[current_index - 1] > fastd[current_index - 1]) and (
                percent_B1 > 1 or percent_B2 > 1 or percent_B3 > 1):
            Trade_Direction = 0
    except Exception:
        Trade_Direction = -99
    return Trade_Direction


def breakout(Trade_Direction, Close, VolumeStream, max_Close, min_Close, max_Vol, current_index):
    try:
        invert = 0
        if invert:
            if Close[current_index] >= max_Close.iloc[current_index] and VolumeStream[current_index] >= max_Vol.iloc[current_index]:
                Trade_Direction = 0
            elif Close[current_index] <= min_Close.iloc[current_index] and VolumeStream[current_index] >= max_Vol.iloc[current_index]:
                Trade_Direction = 1
        else:
            if Close[current_index] >= max_Close.iloc[current_index] and VolumeStream[current_index] >= max_Vol.iloc[current_index]:
                Trade_Direction = 1
            elif Close[current_index] <= min_Close.iloc[current_index] and VolumeStream[current_index] >= max_Vol.iloc[current_index]:
                Trade_Direction = 0
    except Exception:
        Trade_Direction = -99
    return Trade_Direction


def StochRSIMACD(Trade_Direction, fastd, fastk, RSI, MACD, macdsignal, current_index):
    try:
        if ((fastd[current_index] < 20 and fastk[current_index] < 20 and RSI[current_index] > 50 and MACD[current_index] > macdsignal[current_index] and MACD[current_index - 1] < macdsignal[current_index - 1]) or
                (fastd[current_index - 1] < 20 and fastk[current_index - 1] < 20 and RSI[current_index] > 50 and MACD[current_index] > macdsignal[current_index] and MACD[current_index - 2] < macdsignal[current_index - 2] and fastd[current_index] < 80 and fastk[current_index] < 80) or
                (fastd[current_index - 2] < 20 and fastk[current_index - 2] < 20 and RSI[current_index] > 50 and MACD[current_index] > macdsignal[current_index] and MACD[current_index - 1] < macdsignal[current_index - 1] and fastd[current_index] < 80 and fastk[current_index] < 80) or
                (fastd[current_index - 3] < 20 and fastk[current_index - 3] < 20 and RSI[current_index] > 50 and MACD[current_index] > macdsignal[current_index] and MACD[current_index - 2] < macdsignal[current_index - 2] and fastd[current_index] < 80 and fastk[current_index] < 80)):
            Trade_Direction = 1
        elif ((fastd[current_index] > 80 and fastk[current_index] > 80 and RSI[current_index] < 50 and MACD[current_index] < macdsignal[current_index] and MACD[current_index - 1] > macdsignal[current_index - 1]) or
              (fastd[current_index - 1] > 80 and fastk[current_index - 1] > 80 and RSI[current_index] < 50 and MACD[current_index] < macdsignal[current_index] and MACD[current_index - 2] > macdsignal[current_index - 2] and fastd[current_index] > 20 and fastk[current_index] > 20) or
              (fastd[current_index - 2] > 80 and fastk[current_index - 2] > 80 and RSI[current_index] < 50 and MACD[current_index] < macdsignal[current_index] and MACD[current_index - 1] > macdsignal[current_index - 1] and fastd[current_index] > 20 and fastk[current_index] > 20) or
              (fastd[current_index - 3] > 80 and fastk[current_index - 3] > 80 and RSI[current_index] < 50 and MACD[current_index] < macdsignal[current_index] and MACD[current_index - 2] > macdsignal[current_index - 2] and fastd[current_index] > 20 and fastk[current_index] > 20)):
            Trade_Direction = 0
    except Exception:
        Trade_Direction = -99
    return Trade_Direction


def tripleEMAStochasticRSIATR(Close, Trade_Direction, EMA50, EMA14, EMA8, fastd, fastk, current_index):
    try:
        if (Close[current_index] > EMA8[current_index] > EMA14[current_index] > EMA50[current_index]) and \
                ((fastk[current_index] > fastd[current_index]) and (fastk[current_index - 1] < fastd[current_index - 1])):
            Trade_Direction = 1
        elif (Close[current_index] < EMA8[current_index] < EMA14[current_index] < EMA50[current_index]) and \
                ((fastk[current_index] < fastd[current_index]) and (fastk[current_index - 1] > fastd[current_index - 1])):
            Trade_Direction = 0
    except Exception:
        Trade_Direction = -99
    return Trade_Direction


# End of sandboxed strategy module
