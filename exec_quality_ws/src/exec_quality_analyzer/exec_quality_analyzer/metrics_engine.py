"""MetricsEngine node.

Joins /orders/submit with /orders/fills by order_id and computes per-order
execution quality metrics.

Sign convention (documented)
----------------------------
slippage_bps is POSITIVE when execution was WORSE than the reference and
NEGATIVE for price improvement:
  buy : (fill_price - reference) / reference * 1e4   (reference = best ask)
  sell: (reference - fill_price) / reference * 1e4   (reference = best bid)
The reference is the venue-specific quoted_price captured at submission and
carried on the OrderMsg (reference_source tells you whether it was ask, bid,
last, or sim).

Status model
------------
Statuses come from exec_quality_analyzer.order_status — the single source of
truth for which statuses are terminal. Key guarantees:
- A terminal venue status with partial fills (canceled_partial,
  rejected_partial, expired_partial, ...) finalizes IMMEDIATELY and keeps
  its executed quantity, weighted average price, slippage, and latency. It
  is never downgraded to non-terminal 'partial' and never waits for a fill
  that cannot arrive.
- An order whose cumulative fill reaches the requested quantity is 'filled'
  even if the venue's last report was a canceled/closed-like status.
- A zero-filled cancellation stays a terminal unfilled 'canceled'.
- Once finalized, an order id is remembered; late or duplicate terminal
  updates (e.g. poller and another callback reporting the same outcome)
  are dropped — exactly one metric per order.
- Timeout finalization preserves observed partial fills and their slippage
  (status 'timeout_partial'); it never overwrites measured slippage with 0.

Fill-rate semantics: MetricMsg.filled is true only for complete fills;
terminal partials report fill_ratio in (0,1) and filled=false. The
Aggregator counts fill_rate as fully-filled / all terminal orders.

Race-condition handling
-----------------------
Orders and fills travel on separate topics, so a fill can arrive first.
Unknown fills are buffered by order_id and replayed when the order arrives;
unmatched fills expire after `unmatched_fill_timeout_sec` (logged). Duplicate
fill events and duplicate OrderMsgs are detected and dropped. Multiple
incremental partial fills accumulate into a quantity-weighted average
execution price. All shared state is guarded by a lock because ROS callbacks
and poller worker threads run concurrently.

Latency: wall-clock (last fill_time) - submit_time, clamped at >= 0 with a
warning on clock skew. (A monotonic clock cannot be used across processes;
the sweep timer uses monotonic time internally for expiry measurement.)
"""
import time
import threading
from dataclasses import dataclass, field

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy

from exec_quality_interfaces.msg import OrderMsg, FillMsg, MetricMsg

from exec_quality_analyzer.order_status import (
    OrderStatus, is_terminal, with_fill_qualifier,
)

_EPS = 1e-12


@dataclass
class OrderState:
    order: OrderMsg
    filled_qty: float = 0.0
    notional: float = 0.0          # sum(price * qty) for weighted average
    last_fill_time: float = 0.0
    last_exchange_status: str = ''
    seen_fill_keys: set = field(default_factory=set)


class MetricsEngine(Node):
    def __init__(self):
        super().__init__('metrics_engine')

        self.declare_parameter('order_timeout_sec', 10.0)
        self.declare_parameter('unmatched_fill_timeout_sec', 5.0)
        self.timeout = float(self.get_parameter('order_timeout_sec').value)
        self.unmatched_timeout = float(
            self.get_parameter('unmatched_fill_timeout_sec').value)
        if self.timeout <= 0 or self.unmatched_timeout <= 0:
            raise ValueError('metrics_engine: timeouts must be positive')

        self._lock = threading.Lock()
        self.pending: dict[str, OrderState] = {}
        # order_id -> list[(monotonic_arrival, FillMsg)]
        self.unmatched_fills: dict[str, list] = {}
        # order_id -> monotonic finalize time; guarantees one metric per
        # order even when duplicate terminal updates race in.
        self.finalized: dict[str, float] = {}

        qos = QoSProfile(reliability=ReliabilityPolicy.RELIABLE,
                         history=HistoryPolicy.KEEP_LAST, depth=200)
        self.create_subscription(OrderMsg, '/orders/submit', self.on_order, qos)
        self.create_subscription(FillMsg, '/orders/fills', self.on_fill, qos)
        self.pub = self.create_publisher(MetricMsg, '/metrics/per_order', qos)
        self.create_timer(1.0, self.sweep)

        self.get_logger().info(
            f'MetricsEngine up | order_timeout={self.timeout}s '
            f'unmatched_fill_timeout={self.unmatched_timeout}s')

    # ------------------------------------------------------------- callbacks
    def on_order(self, order: OrderMsg):
        if not getattr(order, 'order_id', ''):
            self.get_logger().warning('malformed OrderMsg without order_id dropped')
            return
        with self._lock:
            if order.order_id in self.pending or order.order_id in self.finalized:
                self.get_logger().warning(
                    f'duplicate OrderMsg ignored for {order.order_id[:8]}')
                return
            self.pending[order.order_id] = OrderState(order=order)
            buffered = self.unmatched_fills.pop(order.order_id, [])
        for _, fill in buffered:
            self._process_fill(fill)

    def on_fill(self, fill: FillMsg):
        if not getattr(fill, 'order_id', ''):
            self.get_logger().warning('malformed FillMsg without order_id dropped')
            return
        with self._lock:
            if fill.order_id in self.finalized:
                return  # late/duplicate update after the final metric
            if fill.order_id not in self.pending:
                # Fill raced ahead of its order: buffer, don't discard.
                self.unmatched_fills.setdefault(fill.order_id, []).append(
                    (time.monotonic(), fill))
                return
        self._process_fill(fill)

    # ------------------------------------------------------------ core join
    def _process_fill(self, fill: FillMsg):
        with self._lock:
            state = self.pending.get(fill.order_id)
            if state is None:
                return  # already finalized (e.g. duplicate after terminal)
            # Duplicate detection: same event delivered twice.
            key = (fill.status, round(fill.fill_price, 10),
                   round(fill.filled_quantity, 12), round(fill.fill_time, 6))
            if key in state.seen_fill_keys:
                self.get_logger().warning(
                    f'duplicate fill event ignored for {fill.order_id[:8]}')
                return
            state.seen_fill_keys.add(key)

            # FillMsg quantities are incremental per event; accumulate.
            if fill.filled_quantity > 0 and fill.fill_price > 0:
                state.filled_qty += fill.filled_quantity
                state.notional += fill.fill_price * fill.filled_quantity
                state.last_fill_time = max(state.last_fill_time, fill.fill_time)
            if getattr(fill, 'exchange_status', ''):
                state.last_exchange_status = fill.exchange_status

            requested = state.order.quantity
            complete = requested > 0 and state.filled_qty + _EPS >= requested
            terminal = is_terminal(fill.status)

            if not (complete or terminal):
                return  # await more partial fills, a terminal event, or sweep

            if complete:
                status = OrderStatus.FILLED
            else:
                if fill.status == OrderStatus.FILLED:
                    # Venue claims complete but our accumulated quantity
                    # disagrees (lost event or inconsistent venue report).
                    # Finalize with the honest fill_ratio and flag it.
                    self.get_logger().warning(
                        f"terminal 'filled' for {fill.order_id[:8]} but "
                        f'accumulated {state.filled_qty}/{requested} — '
                        f'finalizing with observed quantities')
                # Terminal with partial fills -> the *_partial variant; the
                # executed quantity and its quality are preserved, never
                # downgraded to a non-terminal generic 'partial'.
                status = with_fill_qualifier(fill.status, state.filled_qty)
            metric = self._build_metric(
                state, status=status, complete=complete,
                end_time=state.last_fill_time or fill.fill_time,
                terminal_reason=getattr(fill, 'error_reason', ''),
                now_wall=time.time())
            del self.pending[fill.order_id]
            self.finalized[fill.order_id] = time.monotonic()
        self.pub.publish(metric)

    def _build_metric(self, state: OrderState, status: str, complete: bool,
                      end_time: float, terminal_reason: str,
                      now_wall: float) -> MetricMsg:
        order = state.order
        m = MetricMsg()
        m.order_id = order.order_id
        m.venue = order.venue
        m.symbol = order.symbol
        m.timestamp = now_wall
        m.status = status
        m.filled = bool(complete)
        requested = order.quantity

        m.requested_quantity = float(requested)
        m.filled_quantity = float(state.filled_qty)
        m.fill_ratio = (min(1.0, state.filled_qty / requested)
                        if requested > 0 else 0.0)
        m.avg_fill_price = (state.notional / state.filled_qty
                            if state.filled_qty > 0 else 0.0)
        m.reference_price = float(order.quoted_price)
        m.exchange_status = state.last_exchange_status
        m.terminal_reason = terminal_reason
        m.quote_mode = getattr(order, 'quote_mode', '')
        m.execution_mode = getattr(order, 'execution_mode', '')
        m.comparable = bool(getattr(order, 'comparable', True))

        # Quantity-weighted average execution price over all partial fills.
        # Slippage is preserved for terminal partials (never reset to 0.0
        # when there was measurable execution).
        if state.filled_qty > 0 and order.quoted_price > 0:
            direction = 1.0 if order.side == 'buy' else -1.0
            m.slippage_bps = direction * (m.avg_fill_price - order.quoted_price) \
                / order.quoted_price * 1e4
        else:
            m.slippage_bps = 0.0

        latency_ms = (end_time - order.submit_time) * 1000.0
        if latency_ms < 0:
            self.get_logger().warning(
                f'negative latency clamped for {order.order_id[:8]} '
                f'({latency_ms:.1f} ms) — check clock skew')
            latency_ms = 0.0
        m.latency_ms = latency_ms
        return m

    # ----------------------------------------------------------------- sweep
    def sweep(self):
        now_wall = time.time()
        now_mono = time.monotonic()
        timed_out, expired, metrics = [], [], []
        with self._lock:
            for oid, state in list(self.pending.items()):
                if now_wall - state.order.submit_time > self.timeout:
                    timed_out.append(self.pending.pop(oid))
                    self.finalized[oid] = now_mono
            for oid, fills in list(self.unmatched_fills.items()):
                fresh = [(t, f) for t, f in fills
                         if now_mono - t <= self.unmatched_timeout]
                dropped = len(fills) - len(fresh)
                if dropped:
                    expired.append((oid, dropped))
                if fresh:
                    self.unmatched_fills[oid] = fresh
                else:
                    del self.unmatched_fills[oid]
            # Forget finalized ids once no late duplicate can be in flight.
            for oid, t in list(self.finalized.items()):
                if now_mono - t > max(self.timeout, self.unmatched_timeout) * 2:
                    del self.finalized[oid]

            for state in timed_out:
                # Timeout preserves observed partial fills AND their
                # slippage; with fills the latency is the last real fill,
                # otherwise the timeout horizon itself.
                status = with_fill_qualifier(OrderStatus.TIMEOUT,
                                             state.filled_qty)
                end_time = (state.last_fill_time if state.filled_qty > 0
                            else state.order.submit_time + self.timeout)
                metrics.append(self._build_metric(
                    state, status=status, complete=False, end_time=end_time,
                    terminal_reason=(
                        f'no terminal venue status within {self.timeout}s '
                        f'(observed fill '
                        f'{state.filled_qty}/{state.order.quantity})'),
                    now_wall=now_wall))

        for oid, n in expired:
            self.get_logger().warning(
                f'expired {n} unmatched fill(s) for unknown order {oid[:8]}')
        for m in metrics:
            self.pub.publish(m)
            self.get_logger().warning(f'TIMEOUT {m.venue} {m.order_id[:8]} '
                                      f'({m.status})')


def main(args=None):
    rclpy.init(args=args)
    node = MetricsEngine()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
