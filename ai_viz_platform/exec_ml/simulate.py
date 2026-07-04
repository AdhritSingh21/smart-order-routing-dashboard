"""DEMO execution-log generator — clearly simulated, never real market data.

Emits per-order execution metrics in the same envelope the dashboard_bridge
POSTs to /ingest ({"type": "metric", "data": {...MetricMsg fields...}}), so
the dataset builder consumes captured real logs and simulated ones through
one code path. Every artifact trained from this generator is stamped
``data_source="simulated-replay"`` and ``is_demo=true`` in its metadata —
the serving guard refuses to present it as production-quality.

Unlike the static per-venue gaussians in the ROS sim listeners, this
generator adds latent regime dynamics so there is genuinely something for a
model to learn — and for rolling features to reveal:

- per-venue congestion: mean-reverting AR(1) with occasional spikes. It
  raises latency (with LOW noise) and slippage (with HIGH noise) together
  and lowers fill probability — so recent latency is a much cleaner read of
  the venue's current state than the noisy recent-slippage average, and a
  model that uses it can beat the naive baseline.
- shared market stress: a slower AR(1) common factor that widens slippage
  on every venue at once.
- order size: larger orders slip more (square-root impact).
"""
from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

DATA_SOURCE_LABEL = "simulated-replay"

# Base venue personalities, aligned with exec_quality_ws sim parameters.
VENUE_PROFILES: dict[str, dict[str, float]] = {
    "alpaca": {
        "latency_ms_mean": 120.0,
        "latency_ms_std": 40.0,
        "slippage_bps_mean": 1.0,
        "slippage_bps_std": 1.5,
        "reject_prob": 0.02,
        "stress_beta": 0.6,
    },
    "binance_testnet": {
        "latency_ms_mean": 60.0,
        "latency_ms_std": 20.0,
        "slippage_bps_mean": 2.5,
        "slippage_bps_std": 3.0,
        "reject_prob": 0.05,
        "stress_beta": 1.2,
    },
    "coinbase_sandbox": {
        "latency_ms_mean": 95.0,
        "latency_ms_std": 30.0,
        "slippage_bps_mean": 1.8,
        "slippage_bps_std": 2.0,
        "reject_prob": 0.03,
        "stress_beta": 0.9,
    },
}


@dataclass
class _VenueState:
    congestion: float = 0.0


def generate_metrics(
    orders: int = 30_000,
    seed: int = 7,
    start_time: float = 1_700_000_000.0,
    order_interval_sec: float = 0.5,
    venues: dict[str, dict[str, float]] | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield MetricMsg-shaped dicts, chronologically, round-robin across venues."""
    rng = random.Random(seed)
    profiles = venues or VENUE_PROFILES
    names = sorted(profiles)
    states = {name: _VenueState() for name in names}
    market_stress = 0.0
    now = start_time
    reference_price = 65_000.0

    for index in range(orders):
        # Shared market factor: slow mean reversion + rare stress bursts.
        market_stress = 0.99 * market_stress + rng.gauss(0.0, 0.04)
        if rng.random() < 0.003:
            market_stress += rng.uniform(0.8, 2.0)
        stress = max(0.0, market_stress)

        venue = names[index % len(names)]
        profile = profiles[venue]
        state = states[venue]

        # Per-venue congestion: regime shifts fast enough that a trailing
        # 20-order average goes stale, which is the edge the model must find.
        state.congestion = 0.9 * state.congestion + rng.gauss(0.0, 0.15)
        if rng.random() < 0.02:
            state.congestion += rng.uniform(2.0, 6.0)
        congestion = max(0.0, state.congestion)

        now += rng.uniform(0.5, 1.5) * order_interval_sec / len(names)
        reference_price *= 1.0 + rng.gauss(0.0, 1e-5)

        # Latency tracks congestion with LOW noise (a clean state signal)...
        latency_ms = max(
            1.0,
            rng.gauss(
                profile["latency_ms_mean"] * (1.0 + 1.0 * congestion),
                profile["latency_ms_std"] * 0.4,
            ),
        )
        reject_prob = min(
            0.6,
            profile["reject_prob"] * (1.0 + 3.0 * congestion + 1.5 * stress),
        )
        quantity = rng.lognormvariate(-6.9, 0.6)  # around 0.001 BTC

        order_id = uuid.UUID(int=rng.getrandbits(128)).hex
        if rng.random() < reject_prob:
            payload: dict[str, Any] = {
                "status": "rejected",
                "slippage_bps": 0.0,
                "fill_ratio": 0.0,
                "filled": False,
                "filled_quantity": 0.0,
                "avg_fill_price": 0.0,
                "exchange_status": "rejected",
                "terminal_reason": "sim: venue rejected order",
            }
        else:
            # ...while slippage tracks the same state with HIGH noise.
            size_impact = 0.4 * (quantity / 0.001) ** 0.5
            slippage_bps = rng.gauss(
                profile["slippage_bps_mean"]
                * (1.0 + 2.0 * congestion + profile["stress_beta"] * stress)
                + size_impact,
                profile["slippage_bps_std"],
            )
            payload = {
                "status": "filled",
                "slippage_bps": slippage_bps,
                "fill_ratio": 1.0,
                "filled": True,
                "filled_quantity": quantity,
                "avg_fill_price": reference_price * (1.0 + slippage_bps / 1e4),
                "exchange_status": "closed",
                "terminal_reason": "fully filled",
            }

        yield {
            "order_id": order_id,
            "venue": venue,
            "symbol": "BTC/USDT",
            "latency_ms": latency_ms,
            "requested_quantity": quantity,
            "reference_price": reference_price,
            "quote_mode": "sim",
            "execution_mode": "sim",
            "comparable": True,
            "timestamp": now,
            **payload,
        }


def write_demo_log(path: Path, orders: int = 30_000, seed: int = 7) -> int:
    """Write a demo JSONL log in the /ingest envelope format. Returns row count."""
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for metric in generate_metrics(orders=orders, seed=seed):
            handle.write(json.dumps({"type": "metric", "data": metric}) + "\n")
            count += 1
    return count
