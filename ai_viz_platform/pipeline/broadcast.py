from __future__ import annotations

import asyncio
from typing import Any

from fastapi import WebSocket

from .state import RuntimeState, utc_now_iso


class ConnectionManager:
    def __init__(self, state: RuntimeState) -> None:
        self._connections: set[WebSocket] = set()
        self._lock = asyncio.Lock()
        self._state = state

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)
            self._state.clients_connected = len(self._connections)
            venue_snapshots = list(self._state.latest_venue_reports.values())
            exec_prediction = self._state.latest_exec_prediction

        # Give newly connected dashboards the latest execution snapshot instead
        # of making them wait for the aggregator's next reporting interval.
        for report in venue_snapshots:
            await websocket.send_json({"type": "venue_report", "data": report})
        if exec_prediction is not None:
            await websocket.send_json(exec_prediction)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._connections.discard(websocket)
            self._state.clients_connected = len(self._connections)

    async def broadcast_json(self, payload: dict[str, Any]) -> None:
        async with self._lock:
            connections = list(self._connections)

        if not connections:
            return

        results = await asyncio.gather(
            *(connection.send_json(payload) for connection in connections),
            return_exceptions=True,
        )
        failed = [
            connection
            for connection, result in zip(connections, results, strict=True)
            if isinstance(result, Exception)
        ]
        if failed:
            async with self._lock:
                for connection in failed:
                    self._connections.discard(connection)
                self._state.clients_connected = len(self._connections)


async def prediction_broadcaster(
    prediction_queue: asyncio.Queue[dict[str, Any]],
    manager: ConnectionManager,
    state: RuntimeState,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        try:
            payload = await asyncio.wait_for(prediction_queue.get(), timeout=0.5)
        except TimeoutError:
            continue

        try:
            state.predictions_published += 1
            state.last_prediction_at = utc_now_iso()
            await manager.broadcast_json(payload)
        finally:
            prediction_queue.task_done()
