"""
Signal aggregation utilities.

Why: provide a deterministic, configurable way to aggregate many optional
external strategy signals into a single ProjectB decision (BUY/SELL/HOLD)
that the adapter layer returns to the master bot for consensus checking.

Notes:
- Default aggregator is weighted majority where each signal weight = confidence.
- Ties produce HOLD.
- This module returns (decision:str, confidence:int, details:dict)
"""
from typing import List, Tuple, Dict
from collections import defaultdict

from signals.signal import Signal


def aggregate_signals(signals: List[Signal], mode: str = "weighted_majority") -> Tuple[str, int, Dict]:
    """Aggregate a list of Signal objects into one decision.

    Returns:
      decision: "BUY"|"SELL"|"HOLD"
      confidence: aggregated confidence (0-100)
      details: debug info
    """
    if not signals:
        return "HOLD", 0, {"reason": "no_signals"}

    # Normalize and validate
    votes = defaultdict(float)  # vote weight sum per decision
    supporters = {"BUY": [], "SELL": [], "HOLD": []}
    total_weight = 0.0

    for s in signals:
        try:
            s.validate()
        except Exception as e:
            # Skip invalid signals but keep record
            continue
        w = float(s.confidence)
        votes[s.decision] += w
        supporters[s.decision].append((s.strategy_name, s.confidence, s.reason))
        total_weight += w

    # Weighted majority
    buy_w = votes.get("BUY", 0.0)
    sell_w = votes.get("SELL", 0.0)
    hold_w = votes.get("HOLD", 0.0)

    # Determine winner
    winner = max(("BUY", buy_w), ("SELL", sell_w), ("HOLD", hold_w), key=lambda x: x[1])
    winner_decision, winner_weight = winner

    # Tie handling: if two top weights are equal (within tiny eps), return HOLD
    sorted_weights = sorted([(k, v) for k, v in votes.items()], key=lambda x: x[1], reverse=True)
    if len(sorted_weights) >= 2 and abs(sorted_weights[0][1] - sorted_weights[1][1]) < 1e-9:
        return "HOLD", 0, {"reason": "tie", "votes": dict(votes)}

    # aggregated confidence: mean confidence among supporters of winner weighted by confidence
    if supporters[winner_decision]:
        w_sum = sum(c for (_, c, _) in supporters[winner_decision])
        if w_sum > 0:
            agg_conf = int(round(sum(c * c for (_, c, _) in supporters[winner_decision]) / w_sum))
        else:
            agg_conf = int(min(100, round(winner_weight)))
    else:
        agg_conf = 0

    details = {
        "votes": dict(votes),
        "supporters": supporters,
        "total_weight": total_weight,
    }

    return winner_decision, int(max(0, min(100, agg_conf))), details
