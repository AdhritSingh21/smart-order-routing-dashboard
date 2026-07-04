from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class RuntimeState:
    mode: str
    model_path: str
    started_at: str = field(default_factory=utc_now_iso)
    model_loaded: bool = False
    last_tick_at: str | None = None
    last_prediction_at: str | None = None
    last_venue_report_at: str | None = None
    last_metric_at: str | None = None
    ticks_received: int = 0
    predictions_published: int = 0
    venue_reports_ingested: int = 0
    metrics_ingested: int = 0
    clients_connected: int = 0
    last_error: str | None = None
    stopping: bool = False
    tick_queue: asyncio.Queue[Any] | None = None
    feature_queue: asyncio.Queue[Any] | None = None
    prediction_queue: asyncio.Queue[Any] | None = None
    latest_venue_reports: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Execution-quality routing model (None/disabled when not trained or refused).
    exec_model_status: str | None = None
    exec_model_disabled_reason: str | None = None
    exec_predictions_published: int = 0
    last_exec_prediction_at: str | None = None
    latest_exec_prediction: dict[str, Any] | None = None

    def snapshot(self) -> dict[str, Any]:
        if self.stopping:
            status = "stopping"
        elif self.last_error and not self.model_loaded:
            status = "error"
        elif self.model_loaded and self.predictions_published > 0:
            status = "ok"
        elif self.model_loaded:
            status = "warming_up"
        else:
            status = "starting"

        return {
            "status": status,
            "mode": self.mode,
            "model_loaded": self.model_loaded,
            "model_path": self.model_path,
            "started_at": self.started_at,
            "last_tick_at": self.last_tick_at,
            "last_prediction_at": self.last_prediction_at,
            "last_venue_report_at": self.last_venue_report_at,
            "last_metric_at": self.last_metric_at,
            "ticks_received": self.ticks_received,
            "predictions_published": self.predictions_published,
            "venue_reports_ingested": self.venue_reports_ingested,
            "metrics_ingested": self.metrics_ingested,
            "venues_available": sorted(self.latest_venue_reports),
            "exec_model_status": self.exec_model_status,
            "exec_model_disabled_reason": self.exec_model_disabled_reason,
            "exec_predictions_published": self.exec_predictions_published,
            "last_exec_prediction_at": self.last_exec_prediction_at,
            "clients_connected": self.clients_connected,
            "queues": {
                "ticks": self.tick_queue.qsize() if self.tick_queue else None,
                "features": self.feature_queue.qsize() if self.feature_queue else None,
                "predictions": self.prediction_queue.qsize() if self.prediction_queue else None,
            },
            "last_error": self.last_error,
        }
