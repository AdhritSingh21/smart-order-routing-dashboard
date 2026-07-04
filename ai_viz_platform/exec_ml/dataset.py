"""Build a supervised dataset from per-order execution metric logs.

Input: JSONL files where each line is either the /ingest envelope
({"type": "metric", "data": {...}}) or a bare MetricMsg-shaped dict.
venue_report envelopes are skipped. The backend can capture these logs
live with ``python main.py --exec-log FILE`` — the same builder then turns
real logged executions into training data.

Each output row is one order: features describe the venue's rolling state
strictly before that order executed, targets are the order's realized
outcome (next-order prediction, exactly how the model is used live).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Iterator

import numpy as np
import pandas as pd

from .features import VenueRollingState, outcome_from_metric

TARGET_SLIPPAGE = "target_slippage_bps"
TARGET_FILLED = "target_filled"
TARGET_LATENCY = "target_latency_ms"

REQUIRED_METRIC_FIELDS = (
    "order_id",
    "venue",
    "slippage_bps",
    "latency_ms",
    "fill_ratio",
    "filled",
    "timestamp",
)


def iter_metric_payloads(paths: Iterable[Path]) -> Iterator[dict[str, Any]]:
    """Yield metric dicts from JSONL logs, tolerating blank/foreign lines."""
    for path in paths:
        with Path(path).open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, dict):
                    continue
                if record.get("type") == "metric" and isinstance(record.get("data"), dict):
                    record = record["data"]
                elif "type" in record:
                    continue  # venue_report or other envelope
                if all(field in record for field in REQUIRED_METRIC_FIELDS):
                    yield record


def build_dataset(metrics: Iterable[dict[str, Any]]) -> pd.DataFrame:
    """Turn a chronological metric stream into feature/target rows.

    Rows are only emitted once a venue has MIN_HISTORY observed orders, and
    the rolling state is updated AFTER the row is captured — features can
    never see the order they are predicting.
    """
    ordered = sorted(metrics, key=lambda m: float(m["timestamp"]))
    states: dict[str, VenueRollingState] = {}
    rows: list[dict[str, Any]] = []

    for metric in ordered:
        venue = str(metric["venue"])
        state = states.setdefault(venue, VenueRollingState(venue))
        outcome = outcome_from_metric(metric)

        features = state.features(as_of=outcome.timestamp)
        if features is not None:
            executed = outcome.fill_ratio > 0.0
            rows.append(
                {
                    **features,
                    TARGET_SLIPPAGE: outcome.slippage_bps if executed else np.nan,
                    TARGET_FILLED: outcome.filled,
                    TARGET_LATENCY: outcome.latency_ms if executed else np.nan,
                    "timestamp": outcome.timestamp,
                    "order_id": str(metric["order_id"]),
                }
            )
        state.update(outcome)

    return pd.DataFrame(rows)


def dataset_from_logs(paths: Iterable[Path]) -> pd.DataFrame:
    return build_dataset(iter_metric_payloads(paths))
