"""Post-submission order monitoring.

Some exchanges acknowledge a market order as {status: open, filled: 0} and
only report the real outcome on a later fetch. OrderPoller watches every
non-terminal submission until it reaches a terminal state, the polling
deadline expires, or the node shuts down.

Design
------
- One daemon worker thread per tracked order (order rate here is a few per
  second at most). The ROS executor is never blocked; rclpy publishers are
  thread-safe.
- The exchange is polled via VenueAdapter.fetch_order_status (read-only,
  exchange-specific fallbacks live in the adapter). create_order is NEVER
  called from here — status polling can retry, order submission cannot.
- CCXT 'filled' is CUMULATIVE. The poller converts it to incremental deltas
  before publishing, so the metrics engine never double-counts quantity.
  The incremental price for a delta is derived from the change in cumulative
  notional (avg * filled), falling back to the venue's cumulative average.
- Temporary fetch failures get limited exponential backoff
  (poll_fetch_retries consecutive failures, then a terminal 'error' result
  that PRESERVES any partial fills already published).
- Deadline expiry publishes 'timeout' / 'timeout_partial' with a clear
  reason; previously observed partial fills remain accounted.
- Finalization is idempotent: a per-order flag guarantees at most one
  terminal FillMsg from the poller, and the listener only starts tracking
  for non-terminal submissions, so polling and the submission callback can
  never both finalize the same order.
- shutdown() sets an event every sleep observes, so workers stop promptly
  (without publishing fabricated results) when the node shuts down.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable

from exec_quality_interfaces.msg import FillMsg

from exec_quality_analyzer.order_status import (
    OrderStatus, is_terminal, parse_ccxt_order, with_fill_qualifier,
)

_EPS = 1e-12
_MAX_BACKOFF_SEC = 10.0


class OrderPoller:
    def __init__(self, adapter: Any, publish_fill: Callable[[FillMsg], None],
                 logger: Any):
        self.adapter = adapter
        self.cfg = adapter.config
        self.publish_fill = publish_fill
        self.log = logger
        self._shutdown = threading.Event()
        self._lock = threading.Lock()
        self._threads: dict[str, threading.Thread] = {}

    # ------------------------------------------------------------------ api
    def track(self, order: Any, initial_parsed: dict) -> None:
        """Begin polling a non-terminal order.

        `initial_parsed` is the normalized submission response; its
        cumulative filled quantity is the baseline (the listener has already
        published that portion), so deltas published here never double-count.
        """
        if is_terminal(initial_parsed['status']):
            return  # nothing to poll; the listener already finalized
        exchange_order_id = initial_parsed.get('exchange_order_id', '')
        if not exchange_order_id:
            # Without an id the state can never be fetched: finalize honestly
            # now rather than poll a hopeless case until the deadline.
            self._publish(order, with_fill_qualifier(
                OrderStatus.ERROR, initial_parsed['filled_quantity']),
                0.0, 0.0, '',
                'venue returned no order id; status cannot be polled',
                initial_parsed.get('exchange_status', ''))
            return
        worker = threading.Thread(
            target=self._poll_loop,
            args=(order, initial_parsed, exchange_order_id),
            name=f'order-poller-{order.order_id[:8]}', daemon=True)
        with self._lock:
            if self._shutdown.is_set():
                return
            self._threads[order.order_id] = worker
        worker.start()

    def shutdown(self, join_timeout_sec: float = 5.0) -> None:
        """Stop all polling promptly (used on node shutdown)."""
        self._shutdown.set()
        with self._lock:
            threads = list(self._threads.values())
        for t in threads:
            t.join(timeout=join_timeout_sec)

    def active_count(self) -> int:
        with self._lock:
            return sum(1 for t in self._threads.values() if t.is_alive())

    # ---------------------------------------------------------------- worker
    def _poll_loop(self, order: Any, initial_parsed: dict,
                   exchange_order_id: str) -> None:
        requested = float(order.quantity)
        cum_filled = float(initial_parsed['filled_quantity'])
        avg0 = float(initial_parsed['average_price'])
        cum_notional = cum_filled * avg0 if avg0 > 0 else 0.0
        deadline = time.monotonic() + self.cfg.poll_max_duration_sec
        consecutive_failures = 0
        finalized = False

        try:
            while not finalized:
                if self._shutdown.wait(self.cfg.poll_interval_sec):
                    self.log.info(
                        f'{self.cfg.venue}: polling of {order.order_id[:8]} '
                        f'stopped by shutdown')
                    return
                if time.monotonic() >= deadline:
                    status = with_fill_qualifier(OrderStatus.TIMEOUT,
                                                 cum_filled)
                    self._publish(
                        order, status, 0.0, 0.0, exchange_order_id,
                        f'polling deadline '
                        f'{self.cfg.poll_max_duration_sec}s exceeded; '
                        f'observed cumulative fill {cum_filled}/{requested}',
                        '')
                    finalized = True
                    return

                try:
                    raw = self.adapter.fetch_order_status(
                        exchange_order_id, order.symbol)
                    parsed = parse_ccxt_order(raw, requested)
                    consecutive_failures = 0
                except Exception as exc:
                    consecutive_failures += 1
                    if consecutive_failures > self.cfg.poll_fetch_retries:
                        status = with_fill_qualifier(OrderStatus.ERROR,
                                                     cum_filled)
                        self._publish(
                            order, status, 0.0, 0.0, exchange_order_id,
                            f'status fetch failed '
                            f'{consecutive_failures} consecutive times; '
                            f'giving up: {type(exc).__name__}: {exc}', '')
                        finalized = True
                        return
                    backoff = min(
                        _MAX_BACKOFF_SEC,
                        self.cfg.poll_fetch_backoff_sec
                        * (2 ** (consecutive_failures - 1)))
                    self.log.warning(
                        f'{self.cfg.venue}: status fetch '
                        f'{consecutive_failures} failed for '
                        f'{order.order_id[:8]} ({exc}); backing off '
                        f'{backoff:.1f}s')
                    if self._shutdown.wait(backoff):
                        return
                    continue

                # CCXT reports cumulative fills: derive the new delta only.
                new_cum = parsed['filled_quantity']
                if new_cum < cum_filled - _EPS:
                    self.log.warning(
                        f'{self.cfg.venue}: cumulative fill went backwards '
                        f'for {order.order_id[:8]} '
                        f'({cum_filled} -> {new_cum}); keeping prior value')
                    new_cum = cum_filled
                delta_qty = new_cum - cum_filled
                delta_price = 0.0
                if delta_qty > _EPS:
                    avg = parsed['average_price']
                    if avg > 0:
                        new_notional = avg * new_cum
                        delta_price = (new_notional - cum_notional) / delta_qty
                        if delta_price <= 0:
                            delta_price = avg
                        cum_notional = new_notional
                    else:
                        self.log.warning(
                            f'{self.cfg.venue}: fill without usable price '
                            f'for {order.order_id[:8]}')
                    cum_filled = new_cum

                if is_terminal(parsed['status']):
                    self._publish(
                        order, parsed['status'], delta_price, delta_qty,
                        parsed['exchange_order_id'] or exchange_order_id,
                        parsed['error_reason'], parsed['exchange_status'])
                    finalized = True
                    return
                if delta_qty > _EPS:
                    self._publish(
                        order, OrderStatus.PARTIAL, delta_price, delta_qty,
                        parsed['exchange_order_id'] or exchange_order_id,
                        parsed['error_reason'], parsed['exchange_status'])
        finally:
            with self._lock:
                self._threads.pop(order.order_id, None)

    # --------------------------------------------------------------- helpers
    def _publish(self, order: Any, status: str, fill_price: float,
                 filled_quantity: float, exchange_order_id: str,
                 error_reason: str, exchange_status: str) -> None:
        fill = FillMsg()
        fill.order_id = order.order_id
        fill.venue = order.venue
        fill.symbol = order.symbol
        fill.status = status
        fill.fill_price = float(fill_price)
        fill.filled_quantity = float(filled_quantity)
        fill.fill_time = time.time()
        fill.error_reason = error_reason
        fill.exchange_order_id = exchange_order_id
        fill.exchange_status = exchange_status
        self.publish_fill(fill)
        if is_terminal(status):
            self.log.info(
                f'{self.cfg.venue}: poller finalized {order.order_id[:8]} '
                f'as {status} ({error_reason or "ok"})')
