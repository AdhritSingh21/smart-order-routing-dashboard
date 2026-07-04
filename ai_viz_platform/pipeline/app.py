from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from .broadcast import ConnectionManager
from .ingest import IngestEnvelope, VenueReportEnvelope
from .state import RuntimeState, utc_now_iso

if TYPE_CHECKING:
    from .exec_predictor import ExecutionQualityPredictor


class MetricJsonlLog:
    """Append ingested metric envelopes to a JSONL file.

    The captured file feeds ``train_exec_model.py --from-logs``, turning a
    live session into future training data.
    """

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = path.open("a", encoding="utf-8")
        self._lock = threading.Lock()

    def write(self, payload: dict) -> None:
        line = json.dumps({"type": "metric", "data": payload})
        with self._lock:
            self._handle.write(line + "\n")
            self._handle.flush()

    def close(self) -> None:
        with self._lock:
            self._handle.close()


def create_app(
    state: RuntimeState,
    manager: ConnectionManager,
    exec_predictor: "ExecutionQualityPredictor | None" = None,
    metric_log: MetricJsonlLog | None = None,
) -> FastAPI:
    app = FastAPI(title="AI Viz Market Data Pipeline", version="1.2.0")

    @app.get("/health")
    async def health() -> dict:
        return state.snapshot()

    @app.post("/ingest")
    async def ingest(message: IngestEnvelope) -> dict[str, object]:
        payload = jsonable_encoder(message.data)

        if isinstance(message, VenueReportEnvelope):
            state.venue_reports_ingested += 1
            state.last_venue_report_at = utc_now_iso()
            state.latest_venue_reports[message.data.venue] = payload
        else:
            state.metrics_ingested += 1
            state.last_metric_at = utc_now_iso()
            if metric_log is not None:
                metric_log.write(payload)

        # Exactly one broadcast per accepted message; clients filter by type
        # (the execution panel keeps venue_report and ignores metric frames).
        await manager.broadcast_json({"type": message.type, "data": payload})

        # Each completed order also refreshes the routing model's view of its
        # venue and broadcasts an execution_prediction frame.
        if exec_predictor is not None and not isinstance(message, VenueReportEnvelope):
            await exec_predictor.on_metric(payload)

        return {"accepted": True, "type": message.type}

    @app.get("/predict_execution")
    async def predict_execution() -> JSONResponse:
        if exec_predictor is None:
            return JSONResponse(
                status_code=503,
                content={
                    "enabled": False,
                    "reason": state.exec_model_disabled_reason
                    or "execution-quality model not loaded",
                },
            )
        return JSONResponse(content=jsonable_encoder(exec_predictor.snapshot()))

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await manager.connect(websocket)
        try:
            # Keeping a receive loop lets us detect normal client disconnects.
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    break
        except WebSocketDisconnect:
            pass
        finally:
            await manager.disconnect(websocket)

    return app
