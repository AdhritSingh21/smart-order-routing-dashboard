from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import websockets

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def receive_prediction(port: int) -> dict:
    async with websockets.connect(f"ws://127.0.0.1:{port}/ws") as websocket:
        return json.loads(await asyncio.wait_for(websocket.recv(), timeout=10))


async def post_and_receive_venue_report(port: int, report: dict) -> dict:
    async with websockets.connect(f"ws://127.0.0.1:{port}/ws") as websocket:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"http://127.0.0.1:{port}/ingest",
                json={"type": "venue_report", "data": report},
                timeout=2,
            )
            response.raise_for_status()

        deadline = asyncio.get_running_loop().time() + 5
        while asyncio.get_running_loop().time() < deadline:
            payload = json.loads(
                await asyncio.wait_for(websocket.recv(), timeout=2)
            )
            if payload.get("type") == "venue_report":
                return payload
        raise AssertionError("venue report was not rebroadcast")


async def receive_cached_venue_report(port: int, venue: str) -> dict:
    async with websockets.connect(f"ws://127.0.0.1:{port}/ws") as websocket:
        deadline = asyncio.get_running_loop().time() + 5
        while asyncio.get_running_loop().time() < deadline:
            payload = json.loads(
                await asyncio.wait_for(websocket.recv(), timeout=2)
            )
            if payload.get("type") == "venue_report" and payload.get("data", {}).get("venue") == venue:
                return payload
        raise AssertionError("cached venue report was not sent to new client")


def test_sim_pipeline_end_to_end() -> None:
    port = free_port()
    process = subprocess.Popen(
        [
            sys.executable,
            "main.py",
            "--mode",
            "sim",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--sim-interval",
            "0.02",
            "--run-seconds",
            "15",
            "--log-level",
            "warning",
        ],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        health = None
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                response = httpx.get(f"http://127.0.0.1:{port}/health", timeout=1)
                if response.status_code == 200:
                    health = response.json()
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
        assert health is not None, "API did not become ready"
        assert health["mode"] == "sim"
        assert health["model_loaded"] is True

        payload = asyncio.run(receive_prediction(port))
        assert payload["symbol"] == "BTC/USD"
        assert payload["source"] == "sim"
        assert payload["prediction"] in {"up", "down"}
        assert isinstance(payload["latest_price"], float)
        assert 0.0 <= payload["confidence"] <= 1.0
        assert payload["pipeline_latency_ms"] >= 0.0
        assert set(payload["features"]) == {
            "rolling_return",
            "volatility",
            "momentum",
        }

        venue_report = {
            "venue": "binance_testnet",
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
        venue_payload = asyncio.run(
            post_and_receive_venue_report(port, venue_report)
        )
        assert venue_payload == {"type": "venue_report", "data": venue_report}
        cached_payload = asyncio.run(
            receive_cached_venue_report(port, "binance_testnet")
        )
        assert cached_payload == venue_payload

        metric_response = httpx.post(
            f"http://127.0.0.1:{port}/ingest",
            json={
                "type": "metric",
                "data": {
                    "order_id": "order-1",
                    "venue": "binance_testnet",
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
                },
            },
            timeout=2,
        )
        assert metric_response.status_code == 200

        health_after = httpx.get(
            f"http://127.0.0.1:{port}/health", timeout=2
        ).json()
        assert health_after["status"] == "ok"
        assert health_after["predictions_published"] >= 1
        assert health_after["venue_reports_ingested"] == 1
        assert health_after["metrics_ingested"] == 1
        assert health_after["venues_available"] == ["binance_testnet"]
    finally:
        process.terminate()
        try:
            stdout, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate(timeout=5)
        # POSIX: 0 (clean) or -SIGTERM. Windows: TerminateProcess exits 1.
        allowed = (0, -15, 1) if os.name == "nt" else (0, -15)
        if process.returncode not in allowed:
            raise AssertionError(
                f"pipeline exited with {process.returncode}\nstdout:\n{stdout}\nstderr:\n{stderr}"
            )
