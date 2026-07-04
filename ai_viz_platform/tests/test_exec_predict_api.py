"""In-process tests for the ingest -> routing-prediction -> broadcast path."""
from __future__ import annotations

from fastapi.testclient import TestClient

from exec_ml.features import MIN_HISTORY
from exec_ml.serving import load_exec_bundle
from pipeline.app import create_app
from pipeline.broadcast import ConnectionManager
from pipeline.exec_predictor import ExecutionQualityPredictor, RECO_WINDOW_SEC
from pipeline.state import RuntimeState


def make_client(trained_bundle_dir) -> tuple[TestClient, RuntimeState, ExecutionQualityPredictor]:
    state = RuntimeState(mode="sim", model_path="unused")
    state.exec_model_status = "demo"
    manager = ConnectionManager(state)
    bundle = load_exec_bundle(trained_bundle_dir)
    predictor = ExecutionQualityPredictor(bundle, state, manager, model_status="demo")
    client = TestClient(create_app(state, manager, exec_predictor=predictor))
    return client, state, predictor


def metric(venue: str, timestamp: float, slippage: float = 1.0, filled: bool = True) -> dict:
    return {
        "order_id": f"{venue}-{timestamp}",
        "venue": venue,
        "symbol": "BTC/USDT",
        "status": "filled" if filled else "rejected",
        "slippage_bps": slippage if filled else 0.0,
        "latency_ms": 90.0,
        "fill_ratio": 1.0 if filled else 0.0,
        "filled": filled,
        "requested_quantity": 0.001,
        "filled_quantity": 0.001 if filled else 0.0,
        "avg_fill_price": 65000.0 if filled else 0.0,
        "reference_price": 64997.4,
        "exchange_status": "closed" if filled else "rejected",
        "terminal_reason": "fully filled" if filled else "sim: rejected",
        "quote_mode": "sim",
        "execution_mode": "sim",
        "comparable": True,
        "timestamp": timestamp,
    }


def post_metric(client: TestClient, payload: dict) -> None:
    response = client.post("/ingest", json={"type": "metric", "data": payload})
    assert response.status_code == 200


def warm_up(client: TestClient, venues: list[str], start: float = 0.0) -> float:
    """Send MIN_HISTORY orders per venue; returns the next free timestamp."""
    timestamp = start
    for _ in range(MIN_HISTORY):
        for venue in venues:
            post_metric(client, metric(venue, timestamp, slippage=1.0 + timestamp % 3))
            timestamp += 0.1
    return timestamp


def test_predict_endpoint_disabled_without_model():
    state = RuntimeState(mode="sim", model_path="unused")
    state.exec_model_disabled_reason = "not trained"
    client = TestClient(create_app(state, ConnectionManager(state)))
    response = client.get("/predict_execution")
    assert response.status_code == 503
    assert response.json() == {"enabled": False, "reason": "not trained"}


def test_metrics_produce_predictions_and_recommendation(trained_bundle_dir):
    client, state, _ = make_client(trained_bundle_dir)
    venues = ["alpaca", "binance_testnet"]
    timestamp = warm_up(client, venues)
    post_metric(client, metric("alpaca", timestamp))

    snapshot = client.get("/predict_execution").json()
    assert snapshot["enabled"] is True
    assert snapshot["model_status"] == "demo"
    assert snapshot["model"]["is_demo"] is True
    assert snapshot["recommended_venue"] in venues

    ok_rows = [row for row in snapshot["venues"] if row["status"] == "ok"]
    assert {row["venue"] for row in ok_rows} == set(venues)
    for row in ok_rows:
        assert isinstance(row["predicted_slippage_bps"], float)
        assert 0.0 <= row["predicted_fill_probability"] <= 1.0
        assert row["predicted_latency_ms_p95"] > 0.0
        assert 0.0 <= row["routing_score"] <= 100.0
    assert sum(row["recommended"] for row in ok_rows) == 1
    assert state.exec_predictions_published > 0


def test_execution_prediction_frames_broadcast_on_ws(trained_bundle_dir):
    client, _, _ = make_client(trained_bundle_dir)
    timestamp = warm_up(client, ["alpaca"])

    with client.websocket_connect("/ws") as ws:
        # Connecting replays the prediction snapshot cached during warm-up.
        replay = ws.receive_json()
        assert replay["type"] == "execution_prediction"
        post_metric(client, metric("alpaca", timestamp))
        # The metric envelope is rebroadcast first, then the prediction frame.
        frames = [ws.receive_json(), ws.receive_json()]
    types = [frame["type"] for frame in frames]
    assert types == ["metric", "execution_prediction"]
    data = frames[1]["data"]
    assert data["model_status"] == "demo"
    assert data["recommended_venue"] == "alpaca"
    assert data["validation"]["window"] == 100


def test_new_ws_client_receives_cached_prediction_frame(trained_bundle_dir):
    client, _, _ = make_client(trained_bundle_dir)
    timestamp = warm_up(client, ["alpaca"])
    post_metric(client, metric("alpaca", timestamp))

    with client.websocket_connect("/ws") as ws:
        frame = ws.receive_json()
    assert frame["type"] == "execution_prediction"
    assert frame["data"]["recommended_venue"] == "alpaca"


def test_live_validation_tracks_errors_and_recommendation_hits(trained_bundle_dir):
    client, _, predictor = make_client(trained_bundle_dir)
    venues = ["alpaca", "binance_testnet"]
    timestamp = warm_up(client, venues)

    # Each subsequent order scores the previous prediction for its venue.
    for i in range(8):
        for venue in venues:
            post_metric(client, metric(venue, timestamp, slippage=1.5))
            timestamp += 0.1

    validation = predictor.validation()
    assert validation["orders_scored"] > 0
    assert validation["slippage_mae"] is not None
    assert validation["slippage_baseline_mae"] is not None
    assert 0.0 <= validation["fill_accuracy"] <= 1.0
    assert 0.0 <= validation["fill_brier"] <= 1.0

    # Jump metric-time forward twice to close recommendation windows
    # (each window needs >= 3 orders on >= 2 venues to be scored).
    for _ in range(2):
        timestamp += RECO_WINDOW_SEC + 1.0
        for _ in range(3):
            for venue in venues:
                post_metric(client, metric(venue, timestamp))
                timestamp += 0.1

    timestamp += RECO_WINDOW_SEC + 1.0
    post_metric(client, metric("alpaca", timestamp))

    validation = predictor.validation()
    assert validation["recommendation_windows"] >= 1
    assert 0.0 <= validation["recommendation_hit_rate"] <= 1.0
