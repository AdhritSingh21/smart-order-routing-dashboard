"""Live execution-quality prediction and smart-order-routing state.

Consumes per-order MetricMsg payloads as they arrive at /ingest, maintains
the exact rolling per-venue state the models were trained on
(exec_ml.features — one shared code path, no train/serve skew), and after
every completed order:

1. scores the previous prediction for that venue against the realized
   outcome (rolling live validation, model vs naive baseline),
2. refreshes the venue's next-order prediction,
3. re-ranks venues into a routing recommendation,
4. broadcasts an ``execution_prediction`` frame on the shared /ws stream.

Every frame carries ``model_status`` ("real" or "demo") so simulated-data
models are never presented as production-quality.
"""
from __future__ import annotations

import time
from collections import defaultdict, deque
from typing import Any

from exec_ml.features import (
    BASELINE_FILL_COLUMN,
    BASELINE_SLIPPAGE_COLUMN,
    OrderOutcome,
    VenueRollingState,
    outcome_from_metric,
)
from exec_ml.serving import ExecModelBundle

from .broadcast import ConnectionManager
from .state import RuntimeState, utc_now_iso

# Routing composite, aligned with the observed-metrics panel heuristic:
# reward fill probability, penalize expected slippage and tail latency.
ROUTING_SLIPPAGE_WEIGHT = 1.7
ROUTING_LATENCY_WEIGHT = 1.0 / 24.0

VALIDATION_WINDOW = 100  # completed orders in the rolling live-error window
RECO_HISTORY = 50        # scored recommendation windows to keep
RECO_WINDOW_SEC = 10.0   # how long a recommendation is held before scoring
RECO_MIN_VENUES = 2      # realized comparison needs at least this many venues
RECO_MIN_ORDERS = 3      # ... each with at least this many orders in the window


def routing_score(
    fill_probability: float | None,
    slippage_bps: float,
    latency_ms_p95: float,
) -> float:
    fill_component = 100.0 * (fill_probability if fill_probability is not None else 1.0)
    score = (
        fill_component
        - ROUTING_SLIPPAGE_WEIGHT * max(0.0, slippage_bps)
        - ROUTING_LATENCY_WEIGHT * max(0.0, latency_ms_p95)
    )
    return max(0.0, min(100.0, score))


def _mean(values: deque[float]) -> float | None:
    return sum(values) / len(values) if values else None


class ExecutionQualityPredictor:
    def __init__(
        self,
        bundle: ExecModelBundle,
        state: RuntimeState,
        manager: ConnectionManager,
        model_status: str,
    ) -> None:
        self._bundle = bundle
        self._state = state
        self._manager = manager
        self.model_status = model_status

        self._venues: dict[str, VenueRollingState] = {}
        self._latest: dict[str, dict[str, Any]] = {}   # venue -> latest prediction payload
        self._pending: dict[str, dict[str, Any]] = {}  # venue -> prediction awaiting outcome
        self._recommended: str | None = None

        # Rolling live validation (last VALIDATION_WINDOW completed orders).
        self._slip_abs_err: deque[float] = deque(maxlen=VALIDATION_WINDOW)
        self._slip_baseline_abs_err: deque[float] = deque(maxlen=VALIDATION_WINDOW)
        self._fill_correct: deque[float] = deque(maxlen=VALIDATION_WINDOW)
        self._fill_brier: deque[float] = deque(maxlen=VALIDATION_WINDOW)
        self._latency_covered: deque[float] = deque(maxlen=VALIDATION_WINDOW)
        self._orders_scored = 0

        # Recommendation scoring: hold each recommendation for a window of
        # metric time, then check it against realized per-venue outcomes.
        self._reco_hits: deque[float] = deque(maxlen=RECO_HISTORY)
        self._window_started: float | None = None
        self._window_reco: str | None = None
        self._window_outcomes: dict[str, list[OrderOutcome]] = defaultdict(list)

    # ------------------------------------------------------------- ingestion

    async def on_metric(self, payload: dict[str, Any]) -> None:
        """Process one completed order; broadcast an updated routing frame."""
        venue = str(payload["venue"])
        outcome = outcome_from_metric(payload)

        self._score_pending(venue, outcome)
        self._advance_reco_window(outcome.timestamp)
        self._window_outcomes[venue].append(outcome)

        state = self._venues.setdefault(venue, VenueRollingState(venue))
        state.update(outcome)

        self._refresh_prediction(venue, as_of=outcome.timestamp)
        self._rank()

        frame = self.frame()
        self._state.latest_exec_prediction = frame
        self._state.exec_predictions_published += 1
        self._state.last_exec_prediction_at = utc_now_iso()
        await self._manager.broadcast_json(frame)

    # ------------------------------------------------------- live validation

    def _score_pending(self, venue: str, outcome: OrderOutcome) -> None:
        pending = self._pending.pop(venue, None)
        if pending is None:
            return

        if outcome.fill_ratio > 0.0:
            predicted = pending["predicted_slippage_bps"]
            baseline = pending["baseline_slippage_bps"]
            self._slip_abs_err.append(abs(predicted - outcome.slippage_bps))
            self._slip_baseline_abs_err.append(abs(baseline - outcome.slippage_bps))
            bound = pending["predicted_latency_ms_p95"]
            self._latency_covered.append(1.0 if outcome.latency_ms <= bound else 0.0)

        probability = pending.get("predicted_fill_probability")
        if probability is not None:
            realized = 1.0 if outcome.filled else 0.0
            self._fill_correct.append(1.0 if (probability >= 0.5) == outcome.filled else 0.0)
            self._fill_brier.append((probability - realized) ** 2)

        self._orders_scored += 1

    def _advance_reco_window(self, now_ts: float) -> None:
        if self._window_started is None:
            self._window_started = now_ts
            self._window_reco = self._recommended
            return
        if now_ts - self._window_started < RECO_WINDOW_SEC:
            return

        # Close the window: was the held recommendation realized-best?
        realized: dict[str, float] = {}
        for venue, outcomes in self._window_outcomes.items():
            if len(outcomes) < RECO_MIN_ORDERS:
                continue
            executed = [o for o in outcomes if o.fill_ratio > 0.0]
            fill_share = sum(o.filled for o in outcomes) / len(outcomes)
            mean_slip = (
                sum(o.slippage_bps for o in executed) / len(executed) if executed else 0.0
            )
            mean_latency = (
                sum(o.latency_ms for o in executed) / len(executed) if executed else 0.0
            )
            realized[venue] = routing_score(fill_share, mean_slip, mean_latency)

        if self._window_reco is not None and len(realized) >= RECO_MIN_VENUES:
            best = max(realized, key=realized.get)  # type: ignore[arg-type]
            self._reco_hits.append(1.0 if best == self._window_reco else 0.0)

        self._window_started = now_ts
        self._window_reco = self._recommended
        self._window_outcomes.clear()

    # ----------------------------------------------------------- predictions

    def _refresh_prediction(self, venue: str, as_of: float) -> None:
        state = self._venues[venue]
        features = state.features(as_of=as_of)
        if features is None:
            self._latest[venue] = {
                "venue": venue,
                "status": "warming_up",
                "orders_observed": len(state),
                "recommended": False,
            }
            return

        prediction = self._bundle.predict_rows([features])[0]
        score = routing_score(
            prediction["predicted_fill_probability"],
            prediction["predicted_slippage_bps"],
            prediction["predicted_latency_ms_p95"],
        )
        payload = {
            "venue": venue,
            "status": "ok",
            "predicted_slippage_bps": round(prediction["predicted_slippage_bps"], 4),
            "predicted_fill_probability": (
                None
                if prediction["predicted_fill_probability"] is None
                else round(prediction["predicted_fill_probability"], 4)
            ),
            "predicted_latency_ms_p95": round(prediction["predicted_latency_ms_p95"], 2),
            "routing_score": round(score, 2),
            "recommended": False,
            "orders_observed": len(state),
        }
        self._latest[venue] = payload
        self._pending[venue] = {
            "predicted_slippage_bps": prediction["predicted_slippage_bps"],
            "predicted_fill_probability": prediction["predicted_fill_probability"],
            "predicted_latency_ms_p95": prediction["predicted_latency_ms_p95"],
            "baseline_slippage_bps": features[BASELINE_SLIPPAGE_COLUMN],
            "baseline_fill_probability": features[BASELINE_FILL_COLUMN],
        }

    def _rank(self) -> None:
        scored = [p for p in self._latest.values() if p.get("status") == "ok"]
        for payload in self._latest.values():
            payload["recommended"] = False
        if not scored:
            self._recommended = None
            return
        best = max(scored, key=lambda p: p["routing_score"])
        best["recommended"] = True
        self._recommended = best["venue"]

    # -------------------------------------------------------------- snapshot

    def validation(self) -> dict[str, Any]:
        return {
            "window": VALIDATION_WINDOW,
            "orders_scored": self._orders_scored,
            "slippage_mae": _mean(self._slip_abs_err),
            "slippage_baseline_mae": _mean(self._slip_baseline_abs_err),
            "fill_accuracy": _mean(self._fill_correct),
            "fill_brier": _mean(self._fill_brier),
            "latency_p95_coverage": _mean(self._latency_covered),
            "recommendation_hit_rate": _mean(self._reco_hits),
            "recommendation_windows": len(self._reco_hits),
        }

    def frame(self) -> dict[str, Any]:
        venues = sorted(
            self._latest.values(),
            key=lambda p: p.get("routing_score", -1.0),
            reverse=True,
        )
        return {
            "type": "execution_prediction",
            "data": {
                "generated_at": time.time(),
                "model_status": self.model_status,
                "recommended_venue": self._recommended,
                "venues": venues,
                "validation": self.validation(),
            },
        }

    def snapshot(self) -> dict[str, Any]:
        """Full snapshot for GET /predict_execution."""
        metadata = self._bundle.metadata
        return {
            "enabled": True,
            "model_status": self.model_status,
            "model": {
                "data_source": metadata.get("data_source"),
                "is_demo": metadata.get("is_demo"),
                "trained_at": metadata.get("trained_at"),
                "feature_columns": metadata.get("feature_columns"),
                "offline_test_metrics": {
                    "slippage_bps": metadata.get("metrics", {}).get("slippage_bps"),
                    "fill_probability": metadata.get("metrics", {}).get("fill_probability"),
                },
            },
            **self.frame()["data"],
        }
