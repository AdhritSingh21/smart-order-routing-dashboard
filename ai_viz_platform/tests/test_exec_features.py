"""Unit tests for execution-quality feature engineering and dataset building."""
from __future__ import annotations

import numpy as np

from exec_ml.dataset import TARGET_FILLED, TARGET_SLIPPAGE, build_dataset
from exec_ml.features import (
    FEATURE_COLUMNS,
    MIN_HISTORY,
    OrderOutcome,
    VenueRollingState,
)


def outcome(
    slippage: float = 1.0,
    latency: float = 100.0,
    filled: bool = True,
    fill_ratio: float = 1.0,
    timestamp: float = 0.0,
) -> OrderOutcome:
    return OrderOutcome(
        slippage_bps=slippage,
        latency_ms=latency,
        filled=filled,
        fill_ratio=fill_ratio,
        timestamp=timestamp,
    )


def metric(venue: str, index: int, slippage: float = 1.0, filled: bool = True) -> dict:
    return {
        "order_id": f"{venue}-{index}",
        "venue": venue,
        "slippage_bps": slippage if filled else 0.0,
        "latency_ms": 100.0,
        "fill_ratio": 1.0 if filled else 0.0,
        "filled": filled,
        "timestamp": float(index),
    }


def test_features_none_until_min_history():
    state = VenueRollingState("alpaca")
    for i in range(MIN_HISTORY - 1):
        assert state.features(as_of=float(i)) is None
        state.update(outcome(timestamp=float(i)))
    assert state.features(as_of=float(MIN_HISTORY)) is None  # one short
    state.update(outcome(timestamp=float(MIN_HISTORY)))
    features = state.features(as_of=float(MIN_HISTORY + 1))
    assert features is not None
    assert set(features) == set(FEATURE_COLUMNS)


def test_features_describe_state_before_the_next_order():
    """An outcome must not influence features until update() is called."""
    state = VenueRollingState("alpaca")
    for i in range(MIN_HISTORY):
        state.update(outcome(slippage=2.0, timestamp=float(i)))

    before = state.features(as_of=float(MIN_HISTORY))
    assert before is not None
    assert before["slippage_mean_short"] == 2.0

    # A wild outlier arrives: features computed for it are still unchanged.
    still_before = state.features(as_of=float(MIN_HISTORY))
    assert still_before == before

    state.update(outcome(slippage=100.0, timestamp=float(MIN_HISTORY)))
    after = state.features(as_of=float(MIN_HISTORY + 1))
    assert after is not None
    assert after["slippage_mean_short"] > 2.0


def test_unexecuted_orders_lower_fill_rate_but_not_slippage_stats():
    state = VenueRollingState("alpaca")
    for i in range(MIN_HISTORY):
        state.update(outcome(slippage=3.0, timestamp=float(i)))
    for i in range(5):
        state.update(
            outcome(slippage=0.0, filled=False, fill_ratio=0.0, timestamp=float(MIN_HISTORY + i))
        )

    features = state.features(as_of=100.0)
    assert features is not None
    # Rejected orders (slippage recorded as 0) are excluded from slippage stats...
    assert features["slippage_mean_short"] == 3.0
    # ...but do count against the fill rate.
    assert features["fill_rate_short"] < 1.0


def test_build_dataset_targets_align_with_the_predicted_order():
    """Row i's target is metric i's own outcome; features come only from prior ones."""
    venue = "binance_testnet"
    slippages = [float(i) for i in range(MIN_HISTORY + 5)]
    metrics = [metric(venue, i, slippage=s) for i, s in enumerate(slippages)]

    frame = build_dataset(metrics)
    # The first MIN_HISTORY orders are warm-up only.
    assert len(frame) == len(metrics) - MIN_HISTORY

    first = frame.iloc[0]
    assert first[TARGET_SLIPPAGE] == slippages[MIN_HISTORY]
    assert first["slippage_mean_short"] == np.mean(slippages[:MIN_HISTORY])
    assert bool(first[TARGET_FILLED]) is True


def test_build_dataset_orders_streams_chronologically_per_venue():
    interleaved = []
    for i in range(MIN_HISTORY + 3):
        interleaved.append(metric("alpaca", 2 * i))
        interleaved.append(metric("coinbase_sandbox", 2 * i + 1))
    frame = build_dataset(interleaved)
    assert set(frame["venue"]) == {"alpaca", "coinbase_sandbox"}
    assert (frame.groupby("venue").size() == 3).all()
    assert frame["timestamp"].is_monotonic_increasing


def test_build_dataset_rejected_orders_have_no_slippage_target():
    venue = "alpaca"
    metrics = [metric(venue, i) for i in range(MIN_HISTORY)]
    metrics.append(metric(venue, MIN_HISTORY, filled=False))
    frame = build_dataset(metrics)
    assert len(frame) == 1
    assert bool(frame.iloc[0][TARGET_FILLED]) is False
    assert np.isnan(frame.iloc[0][TARGET_SLIPPAGE])
