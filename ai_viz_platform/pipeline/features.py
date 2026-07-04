from __future__ import annotations

import asyncio
from collections import deque

import numpy as np

from .types import FeatureFrame, MarketTick


async def feature_engineering_worker(
    tick_queue: asyncio.Queue[MarketTick],
    feature_queue: asyncio.Queue[FeatureFrame],
    stop_event: asyncio.Event,
    window: int,
) -> None:
    if window < 3:
        raise ValueError("feature window must be at least 3")

    prices: deque[float] = deque(maxlen=window + 1)

    while not stop_event.is_set():
        try:
            tick = await asyncio.wait_for(tick_queue.get(), timeout=0.5)
        except TimeoutError:
            continue

        try:
            prices.append(tick.price)
            if len(prices) < window + 1:
                continue

            price_array = np.asarray(prices, dtype=np.float64)
            log_returns = np.diff(np.log(price_array))
            feature_frame = FeatureFrame(
                tick=tick,
                rolling_return=float(log_returns[-1]),
                volatility=float(np.std(log_returns, ddof=1)),
                momentum=float(price_array[-1] / price_array[0] - 1.0),
            )
            await feature_queue.put(feature_frame)
        finally:
            tick_queue.task_done()
