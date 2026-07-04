"""Live verification helper: watch /ws and report what flows through it.

Not a pytest test — run it against a live backend (+ analyzer/bridge) to
verify predictions, venue reports, and metrics share the socket, and that a
fresh connection immediately receives cached venue reports.

    python tests/ws_live_check.py [--url ws://127.0.0.1:8000/ws] [--seconds 12]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time

import websockets


async def watch(url: str, seconds: float) -> dict:
    counts: dict[str, int] = {"prediction": 0, "venue_report": 0, "metric": 0, "other": 0}
    venues: dict[str, dict] = {}
    first_frame_ms: float | None = None

    async with websockets.connect(url) as ws:
        start = time.monotonic()
        while time.monotonic() - start < seconds:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2)
            except TimeoutError:
                continue
            payload = json.loads(raw)
            if first_frame_ms is None:
                first_frame_ms = (time.monotonic() - start) * 1000
            kind = payload.get("type")
            if kind == "venue_report":
                counts["venue_report"] += 1
                venues[payload["data"]["venue"]] = payload["data"]
            elif kind == "metric":
                counts["metric"] += 1
            elif "prediction" in payload:
                counts["prediction"] += 1
            else:
                counts["other"] += 1

    return {"counts": counts, "venues": venues, "first_frame_ms": first_frame_ms}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="ws://127.0.0.1:8000/ws")
    parser.add_argument("--seconds", type=float, default=12.0)
    args = parser.parse_args()

    print(f"watching {args.url} for {args.seconds:.0f}s ...")
    result = await watch(args.url, args.seconds)
    print(json.dumps({
        "counts": result["counts"],
        "venues_seen": sorted(result["venues"]),
        "sample_venue_report": next(iter(result["venues"].values()), None),
    }, indent=2))

    print("reconnecting to check cached venue snapshot ...")
    snapshot = await watch(args.url, 1.0)
    print(json.dumps({
        "cached_venues_on_connect": sorted(snapshot["venues"]),
        "first_frame_ms": snapshot["first_frame_ms"],
    }, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
