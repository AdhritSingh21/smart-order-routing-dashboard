from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True, frozen=True)
class MarketTick:
    symbol: str
    price: float
    event_type: str
    event_time: str
    source: str
    arrival_ns: int


@dataclass(slots=True, frozen=True)
class FeatureFrame:
    tick: MarketTick
    rolling_return: float
    volatility: float
    momentum: float

    def as_vector(self) -> list[float]:
        return [self.rolling_return, self.volatility, self.momentum]

    def as_dict(self) -> dict[str, float]:
        return {
            "rolling_return": self.rolling_return,
            "volatility": self.volatility,
            "momentum": self.momentum,
        }


PredictionPayload = dict[str, Any]
