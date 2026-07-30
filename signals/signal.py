"""
Standard Signal object used by adapters and aggregator.

Why: enforces a stable contract between external strategies and the master bot.
All adapters must return this shape. This file is intentionally small and
commented so future contributors understand the contract.
"""
from dataclasses import dataclass, asdict


@dataclass
class Signal:
    decision: str  # "BUY"|"SELL"|"HOLD"
    confidence: int  # 0-100
    strategy_name: str
    reason: str = ""

    def to_dict(self):
        return asdict(self)

    def validate(self):
        # Basic validation to catch bad adapters early.
        if self.decision not in ("BUY", "SELL", "HOLD"):
            raise ValueError(f"invalid decision: {self.decision}")
        if not (0 <= int(self.confidence) <= 100):
            raise ValueError(f"confidence must be 0-100: {self.confidence}")
        if not isinstance(self.strategy_name, str) or not self.strategy_name:
            raise ValueError("strategy_name required")
