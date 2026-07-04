"""Shared feature engineering for the execution-quality models.

The SAME rolling-state implementation builds offline training rows (from
logged per-order metrics) and online serving-time feature vectors (from
/ingest metric frames). One code path is what prevents train/serve skew:
a feature row always describes a venue's state strictly BEFORE the next
order's outcome is known, so there is no target leakage by construction.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any

import numpy as np

SHORT_WINDOW = 20  # orders — reacts to bursts / regime shifts
LONG_WINDOW = 100  # orders — stable venue baseline (matches the aggregator window)
MIN_HISTORY = 10   # orders required before a venue's features are usable

VENUE_COLUMN = "venue"
NUMERIC_FEATURES = [
    "orders_seen",
    "fill_rate_short",
    "fill_rate_long",
    "slippage_mean_short",
    "slippage_mean_long",
    "slippage_std_short",
    "slippage_p95_long",
    "slippage_trend",
    "latency_p50_short",
    "latency_p95_long",
    "latency_trend",
    "seconds_since_last_order",
]
FEATURE_COLUMNS = [VENUE_COLUMN, *NUMERIC_FEATURES]

# Naive references the model must beat: "assume the next order looks like the
# venue's recent average". Both are ordinary feature columns, so offline and
# live validation compare against the exact same baseline.
BASELINE_SLIPPAGE_COLUMN = "slippage_mean_short"
BASELINE_FILL_COLUMN = "fill_rate_short"
BASELINE_LATENCY_COLUMN = "latency_p95_long"


@dataclass(slots=True)
class OrderOutcome:
    """Realized outcome of one order, as reported by the MetricsEngine."""

    slippage_bps: float
    latency_ms: float
    filled: bool
    fill_ratio: float
    timestamp: float


def outcome_from_metric(payload: dict[str, Any]) -> OrderOutcome:
    """Build an OrderOutcome from a MetricMsg-shaped dict (bridge/ingest format)."""
    return OrderOutcome(
        slippage_bps=float(payload["slippage_bps"]),
        latency_ms=float(payload["latency_ms"]),
        filled=bool(payload["filled"]),
        fill_ratio=float(payload["fill_ratio"]),
        timestamp=float(payload["timestamp"]),
    )


class VenueRollingState:
    """Rolling per-venue execution history.

    Mirrors the aggregator's documented semantics: fill rate counts fully
    filled orders over ALL orders in the window, while slippage/latency
    statistics only include orders that actually executed (fill_ratio > 0).
    """

    def __init__(self, venue: str) -> None:
        self.venue = venue
        self._orders: deque[OrderOutcome] = deque(maxlen=LONG_WINDOW)

    def __len__(self) -> int:
        return len(self._orders)

    @property
    def ready(self) -> bool:
        return len(self._orders) >= MIN_HISTORY

    def update(self, outcome: OrderOutcome) -> None:
        self._orders.append(outcome)

    def features(self, as_of: float) -> dict[str, Any] | None:
        """Feature row describing this venue BEFORE the next order.

        ``as_of`` is the submission time of the order being predicted; only
        already-observed orders contribute. Returns None while the venue has
        too little history (or no executed orders) to describe.
        """
        if not self.ready:
            return None

        orders = list(self._orders)
        short = orders[-SHORT_WINDOW:]
        executed = [o for o in orders if o.fill_ratio > 0.0]
        executed_short = [o for o in short if o.fill_ratio > 0.0]
        if not executed or not executed_short:
            return None

        slip_long = np.asarray([o.slippage_bps for o in executed])
        slip_short = np.asarray([o.slippage_bps for o in executed_short])
        lat_long = np.asarray([o.latency_ms for o in executed])
        lat_short = np.asarray([o.latency_ms for o in executed_short])

        lat_p50_short = float(np.percentile(lat_short, 50))
        lat_p50_long = float(np.percentile(lat_long, 50))

        return {
            VENUE_COLUMN: self.venue,
            "orders_seen": float(len(orders)),
            "fill_rate_short": sum(o.filled for o in short) / len(short),
            "fill_rate_long": sum(o.filled for o in orders) / len(orders),
            "slippage_mean_short": float(slip_short.mean()),
            "slippage_mean_long": float(slip_long.mean()),
            "slippage_std_short": float(slip_short.std(ddof=0)),
            "slippage_p95_long": float(np.percentile(slip_long, 95)),
            "slippage_trend": float(slip_short.mean() - slip_long.mean()),
            "latency_p50_short": lat_p50_short,
            "latency_p95_long": float(np.percentile(lat_long, 95)),
            "latency_trend": lat_p50_short / max(lat_p50_long, 1e-9),
            "seconds_since_last_order": max(0.0, as_of - orders[-1].timestamp),
        }
