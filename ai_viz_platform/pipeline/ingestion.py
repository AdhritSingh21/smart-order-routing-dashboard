from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import time
from datetime import datetime, timezone
from typing import Any

import websockets

from .state import RuntimeState, utc_now_iso
from .types import MarketTick

LOGGER = logging.getLogger(__name__)
ALPACA_CRYPTO_WS_URL = "wss://stream.data.alpaca.markets/v1beta3/crypto/us"


def _event_time(message: dict[str, Any]) -> str:
    value = message.get("t")
    return str(value) if value else datetime.now(timezone.utc).isoformat()


async def simulated_ingestion(
    tick_queue: asyncio.Queue[MarketTick],
    state: RuntimeState,
    stop_event: asyncio.Event,
    symbol: str,
    interval_seconds: float,
    start_price: float,
    seed: int,
) -> None:
    rng = random.Random(seed)
    price = start_price
    tick_number = 0

    while not stop_event.is_set():
        arrival_ns = time.perf_counter_ns()
        shock = rng.gauss(0.0, 0.00035)
        mean_reversion = (start_price - price) / start_price * 0.00002
        price *= math.exp(shock + mean_reversion)
        event_type = "trade" if tick_number % 2 == 0 else "quote"
        tick_number += 1

        tick = MarketTick(
            symbol=symbol,
            price=price,
            event_type=event_type,
            event_time=datetime.now(timezone.utc).isoformat(),
            source="sim",
            arrival_ns=arrival_ns,
        )
        await tick_queue.put(tick)
        state.ticks_received += 1
        state.last_tick_at = utc_now_iso()
        await asyncio.sleep(interval_seconds)


async def alpaca_ingestion(
    tick_queue: asyncio.Queue[MarketTick],
    state: RuntimeState,
    stop_event: asyncio.Event,
    symbol: str,
    api_key: str,
    api_secret: str,
    websocket_url: str = ALPACA_CRYPTO_WS_URL,
) -> None:
    if not api_key or not api_secret:
        raise ValueError(
            "ALPACA_API_KEY and ALPACA_API_SECRET are required in alpaca mode"
        )

    backoff_seconds = 1.0
    while not stop_event.is_set():
        try:
            async with websockets.connect(
                websocket_url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_queue=2048,
            ) as websocket:
                connected = json.loads(await asyncio.wait_for(websocket.recv(), timeout=10))
                LOGGER.info("Alpaca connection response: %s", connected)

                await websocket.send(
                    json.dumps(
                        {"action": "auth", "key": api_key, "secret": api_secret}
                    )
                )
                auth_response = json.loads(
                    await asyncio.wait_for(websocket.recv(), timeout=10)
                )
                if not any(
                    item.get("T") == "success" and item.get("msg") == "authenticated"
                    for item in auth_response
                ):
                    raise RuntimeError(f"Alpaca authentication failed: {auth_response}")

                await websocket.send(
                    json.dumps(
                        {
                            "action": "subscribe",
                            "trades": [symbol],
                            "quotes": [symbol],
                        }
                    )
                )
                LOGGER.info("Subscribed to Alpaca trades and quotes for %s", symbol)
                backoff_seconds = 1.0
                state.last_error = None

                while not stop_event.is_set():
                    try:
                        raw_message = await asyncio.wait_for(
                            websocket.recv(), timeout=1.0
                        )
                    except TimeoutError:
                        continue
                    arrival_ns = time.perf_counter_ns()
                    messages = json.loads(raw_message)
                    for message in messages:
                        message_type = message.get("T")
                        if message_type == "t":
                            price = float(message["p"])
                            event_type = "trade"
                        elif message_type == "q":
                            bid = float(message["bp"])
                            ask = float(message["ap"])
                            price = (bid + ask) / 2.0
                            event_type = "quote"
                        else:
                            continue

                        tick = MarketTick(
                            symbol=str(message.get("S", symbol)),
                            price=price,
                            event_type=event_type,
                            event_time=_event_time(message),
                            source="alpaca",
                            arrival_ns=arrival_ns,
                        )
                        await tick_queue.put(tick)
                        state.ticks_received += 1
                        state.last_tick_at = utc_now_iso()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state.last_error = f"ingestion: {type(exc).__name__}: {exc}"
            LOGGER.exception("Alpaca stream error; reconnecting in %.1fs", backoff_seconds)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=backoff_seconds)
            except TimeoutError:
                pass
            backoff_seconds = min(backoff_seconds * 2.0, 30.0)
