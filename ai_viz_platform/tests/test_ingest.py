"""In-process tests for POST /ingest validation, caching, and /ws rebroadcast."""
from __future__ import annotations

import time

from fastapi.testclient import TestClient

from pipeline.app import create_app
from pipeline.broadcast import ConnectionManager
from pipeline.state import RuntimeState


def make_client() -> tuple[TestClient, RuntimeState]:
    state = RuntimeState(mode="sim", model_path="unused")
    manager = ConnectionManager(state)
    return TestClient(create_app(state, manager)), state


def venue_report(venue: str = "binance_testnet", **overrides) -> dict:
    report = {
        "venue": venue,
        "window_orders": 50,
        "fill_rate": 0.96,
        "slippage_bps_p50": 0.7,
        "slippage_bps_p95": 2.5,
        "latency_ms_p50": 17.0,
        "latency_ms_p95": 44.0,
        "latency_ms_p99": 70.0,
        "comparable": True,
        "timestamp": time.time(),
    }
    report.update(overrides)
    return report


def metric(**overrides) -> dict:
    payload = {
        "order_id": "order-1",
        "venue": "alpaca",
        "symbol": "BTC/USDT",
        "status": "filled",
        "slippage_bps": 0.4,
        "latency_ms": 12.0,
        "fill_ratio": 1.0,
        "filled": True,
        "requested_quantity": 0.001,
        "filled_quantity": 0.001,
        "avg_fill_price": 65000.0,
        "reference_price": 64997.4,
        "exchange_status": "closed",
        "terminal_reason": "fully filled",
        "quote_mode": "sim",
        "execution_mode": "sim",
        "comparable": True,
        "timestamp": time.time(),
    }
    payload.update(overrides)
    return payload


def test_accepts_valid_venue_report_and_caches_it():
    client, state = make_client()
    report = venue_report()
    response = client.post("/ingest", json={"type": "venue_report", "data": report})
    assert response.status_code == 200
    assert response.json() == {"accepted": True, "type": "venue_report"}
    assert state.venue_reports_ingested == 1
    assert state.latest_venue_reports["binance_testnet"] == report


def test_accepts_valid_metric():
    client, state = make_client()
    response = client.post("/ingest", json={"type": "metric", "data": metric()})
    assert response.status_code == 200
    assert response.json() == {"accepted": True, "type": "metric"}
    assert state.metrics_ingested == 1
    assert state.latest_venue_reports == {}


def test_rejects_unsupported_message_type():
    client, _ = make_client()
    response = client.post("/ingest", json={"type": "order", "data": {}})
    assert response.status_code == 422


def test_rejects_malformed_venue_report():
    client, state = make_client()
    bad_payloads = [
        venue_report(fill_rate=1.5),                    # out of range
        venue_report(venue=""),                         # empty venue
        {k: v for k, v in venue_report().items() if k != "latency_ms_p99"},
        venue_report(window_orders=-1),
    ]
    for bad in bad_payloads:
        response = client.post("/ingest", json={"type": "venue_report", "data": bad})
        assert response.status_code == 422, bad
    assert state.venue_reports_ingested == 0


def test_rejects_non_envelope_bodies():
    client, _ = make_client()
    assert client.post("/ingest", json={"venue": "x"}).status_code == 422
    assert client.post("/ingest", json=[1, 2, 3]).status_code == 422
    assert client.post(
        "/ingest",
        content=b"not json",
        headers={"content-type": "application/json"},
    ).status_code == 422


def test_rebroadcasts_each_message_exactly_once():
    client, _ = make_client()
    with client.websocket_connect("/ws") as ws:
        client.post("/ingest", json={"type": "venue_report", "data": venue_report()})
        first = ws.receive_json()
        assert first["type"] == "venue_report"
        assert first["data"]["venue"] == "binance_testnet"

        # The very next frame must be the metric: the venue report above was
        # broadcast exactly once, and metric envelopes are rebroadcast too.
        client.post("/ingest", json={"type": "metric", "data": metric()})
        second = ws.receive_json()
        assert second["type"] == "metric"
        assert second["data"]["order_id"] == "order-1"


def test_new_client_receives_cached_venue_reports():
    client, _ = make_client()
    client.post("/ingest", json={"type": "venue_report", "data": venue_report("alpaca")})
    client.post("/ingest", json={"type": "venue_report", "data": venue_report("coinbase_sandbox")})
    # Later report for the same venue replaces the cached one (upsert).
    updated = venue_report("alpaca", fill_rate=0.5)
    client.post("/ingest", json={"type": "venue_report", "data": updated})

    with client.websocket_connect("/ws") as ws:
        snapshots = {frame["data"]["venue"]: frame["data"]
                     for frame in (ws.receive_json(), ws.receive_json())}
    assert set(snapshots) == {"alpaca", "coinbase_sandbox"}
    assert snapshots["alpaca"]["fill_rate"] == 0.5


def test_disconnected_client_is_removed_and_broadcast_survives():
    client, state = make_client()
    with client.websocket_connect("/ws"):
        assert state.clients_connected == 1
    deadline = time.monotonic() + 5
    while state.clients_connected != 0 and time.monotonic() < deadline:
        time.sleep(0.02)
    assert state.clients_connected == 0

    response = client.post(
        "/ingest", json={"type": "venue_report", "data": venue_report()}
    )
    assert response.status_code == 200
    assert state.venue_reports_ingested == 1
