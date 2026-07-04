"""ExecutionListener node (one instance per venue).

Subscribes to /orders/submit, filters for its own venue, executes the order,
and publishes the result on /orders/fills.

Modes (strict, validated by VenueConfig):
  sim        : no network; per-venue latency/slippage/reject distributions.
  sandbox    : paper/testnet via the VenueAdapter. Sandbox activation must be
               positively confirmed or the node refuses to start.
  production : refused unless allow_production_trading is explicitly true.
dry_run (default true outside sim) constructs but never submits orders; the
result is published with the explicit terminal status "dry_run".

Asynchronous orders: when a submission comes back non-terminal (e.g.
status=open, filled=0), the listener publishes any initial partial fill and
hands the order to an OrderPoller, which polls the venue (configurable
interval/deadline) on a worker thread until the order is terminal. The ROS
executor is never blocked and create_order is never called twice.

Topology guard: a non-sim listener refuses orders that carry SIMULATED
reference prices (quote_mode == "sim") unless the explicitly named testing
override allow_sim_reference_for_live_testing is set. The OrderSubmitter
validates the topology at startup; this is defense in depth at runtime.
"""
import time
import random

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy

from exec_quality_interfaces.msg import OrderMsg, FillMsg

from exec_quality_analyzer.adapters import (
    VenueAdapter, venue_config_from_params,
)
from exec_quality_analyzer.order_poller import OrderPoller
from exec_quality_analyzer.order_status import OrderStatus, is_terminal


class ExecutionListener(Node):
    def __init__(self, adapter: VenueAdapter | None = None):
        super().__init__('execution_listener')

        self.declare_parameter('venue', 'alpaca')
        self.declare_parameter('mode', 'sim')             # sim | sandbox | production
        self.declare_parameter('dry_run', True)
        self.declare_parameter('allow_production_trading', False)
        self.declare_parameter('allow_sim_reference_for_live_testing', False)
        self.declare_parameter('ccxt_exchange_id', '')
        self.declare_parameter('symbol_override', '')
        self.declare_parameter('quote_timeout_sec', 5.0)
        self.declare_parameter('quote_max_age_sec', 2.0)
        self.declare_parameter('quote_retries', 2)
        self.declare_parameter('quote_retry_backoff_sec', 0.5)
        self.declare_parameter('max_future_timestamp_skew_sec', 5.0)
        # Market-data / execution separation (e.g. Coinbase sandbox)
        self.declare_parameter('market_data_mode', 'venue')   # venue | public
        self.declare_parameter('market_data_exchange_id', '')
        self.declare_parameter('synthetic_execution', False)
        self.declare_parameter('include_in_ranking', '')      # '' | 'true' | 'false'
        # Post-submission polling
        self.declare_parameter('poll_interval_sec', 1.0)
        self.declare_parameter('poll_max_duration_sec', 30.0)
        self.declare_parameter('poll_request_timeout_sec', 5.0)
        self.declare_parameter('poll_fetch_retries', 5)
        self.declare_parameter('poll_fetch_backoff_sec', 0.5)
        # Sim-mode venue personality
        self.declare_parameter('sim_latency_ms_mean', 80.0)
        self.declare_parameter('sim_latency_ms_std', 25.0)
        self.declare_parameter('sim_slippage_bps_mean', 1.5)
        self.declare_parameter('sim_slippage_bps_std', 2.0)
        self.declare_parameter('sim_reject_prob', 0.03)

        g = lambda k, d=None: self.get_parameter(k).value
        self.venue = g('venue')
        self.mode = g('mode')
        self.allow_sim_reference = bool(
            g('allow_sim_reference_for_live_testing'))
        self.lat_mean = float(g('sim_latency_ms_mean'))
        self.lat_std = float(g('sim_latency_ms_std'))
        self.slip_mean = float(g('sim_slippage_bps_mean'))
        self.slip_std = float(g('sim_slippage_bps_std'))
        self.reject_prob = float(g('sim_reject_prob'))

        self.adapter = adapter
        if self.mode != 'sim' and self.adapter is None:
            cfg = venue_config_from_params(
                lambda k, d: self.get_parameter(k).value if self.has_parameter(k) else d)
            # Raises AdapterConfigError / SandboxActivationError on unsafe
            # config — the node must NOT start in an ambiguous trading state.
            self.adapter = VenueAdapter(cfg, logger=self.get_logger())

        qos = QoSProfile(reliability=ReliabilityPolicy.RELIABLE,
                         history=HistoryPolicy.KEEP_LAST, depth=100)
        self.pub = self.create_publisher(FillMsg, '/orders/fills', qos)

        self.poller: OrderPoller | None = None
        if self.adapter is not None and self.mode != 'sim':
            self.poller = OrderPoller(self.adapter, self.pub.publish,
                                      self.get_logger())

        # Duplicate-delivery guard: a replayed OrderMsg must never reach
        # create_order a second time. Insertion-ordered; pruned by size.
        self._seen_order_ids: dict[str, None] = {}
        self._seen_order_ids_max = 10000

        # Subscribe last: everything on_order touches now exists.
        self.sub = self.create_subscription(OrderMsg, '/orders/submit', self.on_order, qos)

        self.get_logger().info(
            f'ExecutionListener[{self.venue}] up | mode={self.mode}')

    # -------------------------------------------------------------- callback
    def on_order(self, order: OrderMsg):
        try:
            if not order.order_id or order.venue != self.venue:
                return
            if order.reference_source == 'none':
                return  # quote-failed orders are never executed
            if order.quantity <= 0:
                self.get_logger().warning(
                    f'malformed order {order.order_id[:8]}: quantity '
                    f'{order.quantity} <= 0; ignoring')
                return
            quote_mode = getattr(order, 'quote_mode', '')
        except AttributeError as exc:
            self.get_logger().warning(f'malformed OrderMsg dropped: {exc}')
            return

        if order.order_id in self._seen_order_ids:
            self.get_logger().warning(
                f'duplicate OrderMsg {order.order_id[:8]} ignored — '
                f'an order is never executed twice')
            return
        self._seen_order_ids[order.order_id] = None
        if len(self._seen_order_ids) > self._seen_order_ids_max:
            for oid in list(self._seen_order_ids)[:self._seen_order_ids_max // 2]:
                del self._seen_order_ids[oid]

        if (self.mode != 'sim' and quote_mode == 'sim'
                and not self.allow_sim_reference):
            # Defense in depth: never execute a live/sandbox order against a
            # simulated reference price (the submitter validates this at
            # startup; refuse here too in case of mismatched configs).
            self.get_logger().error(
                f'REFUSED order {order.order_id[:8]}: listener mode '
                f'{self.mode} but reference price is simulated '
                f'(quote_mode=sim); set allow_sim_reference_for_live_testing '
                f'to permit this in tests')
            self._publish_local_failure(
                order, OrderStatus.REJECTED,
                'listener refused simulated reference price in '
                f'{self.mode} mode')
            return

        if self.mode == 'sim':
            fill = self._simulate_fill(order)
            self.pub.publish(fill)
            self.get_logger().info(
                f'FILL[{self.venue}] {fill.status} {order.order_id[:8]} '
                f'@ {fill.fill_price:.2f}')
        else:
            self._execute_via_adapter(order)

    # ------------------------------------------------------------------- sim
    def _simulate_fill(self, order: OrderMsg) -> FillMsg:
        fill = FillMsg()
        fill.order_id = order.order_id
        fill.venue = self.venue
        fill.symbol = order.symbol
        fill.error_reason = ''

        # Simulated venue latency is expressed through the fill timestamp
        # instead of a blocking sleep, so the ROS callback returns instantly.
        latency_s = max(0.001, random.gauss(self.lat_mean, self.lat_std) / 1000.0)

        if random.random() < self.reject_prob:
            fill.status = OrderStatus.REJECTED
            fill.fill_price = 0.0
            fill.filled_quantity = 0.0
            fill.error_reason = 'sim: venue rejected order'
        else:
            slip_bps = random.gauss(self.slip_mean, self.slip_std)
            direction = 1.0 if order.side == 'buy' else -1.0  # adverse move
            fill.status = OrderStatus.FILLED
            fill.fill_price = order.quoted_price * (1.0 + direction * slip_bps / 1e4)
            fill.filled_quantity = order.quantity

        fill.fill_time = order.submit_time + latency_s
        return fill

    # ----------------------------------------------------------------- live
    def _execute_via_adapter(self, order: OrderMsg) -> None:
        result = self.adapter.submit_market_order(
            order.symbol, order.side, order.quantity)
        status = result['status']

        if is_terminal(status):
            fill = self._fill_from_result(order, result, status)
            self.pub.publish(fill)
            self.get_logger().info(
                f'FILL[{self.venue}] {fill.status} {order.order_id[:8]} '
                f'@ {fill.fill_price:.2f}')
            if fill.error_reason:
                self.get_logger().warning(
                    f'{self.venue} order {order.order_id[:8]}: '
                    f'{fill.error_reason}')
            return

        # Non-terminal acknowledgement (e.g. open/0-filled market order):
        # publish any initial partial execution, then poll asynchronously
        # until the venue reports a terminal state. The order is NEVER
        # resubmitted.
        if result['filled_quantity'] > 0:
            initial = self._fill_from_result(order, result,
                                             OrderStatus.PARTIAL)
            self.pub.publish(initial)
        self.get_logger().info(
            f'{self.venue}: order {order.order_id[:8]} acknowledged as '
            f"'{status}' (filled {result['filled_quantity']}/"
            f'{order.quantity}); polling for final state')
        self.poller.track(order, result)

    def _fill_from_result(self, order: OrderMsg, result: dict,
                          status: str) -> FillMsg:
        fill = FillMsg()
        fill.order_id = order.order_id
        fill.venue = self.venue
        fill.symbol = order.symbol
        fill.status = status
        fill.fill_price = float(result['average_price'])
        fill.filled_quantity = float(result['filled_quantity'])
        fill.fill_time = time.time()
        fill.error_reason = result.get('error_reason', '')
        fill.exchange_order_id = result.get('exchange_order_id', '')
        fill.exchange_status = result.get('exchange_status', '')
        return fill

    def _publish_local_failure(self, order: OrderMsg, status: str,
                               reason: str) -> None:
        fill = FillMsg()
        fill.order_id = order.order_id
        fill.venue = self.venue
        fill.symbol = order.symbol
        fill.status = status
        fill.fill_price = 0.0
        fill.filled_quantity = 0.0
        fill.fill_time = time.time()
        fill.error_reason = reason
        self.pub.publish(fill)

    # ------------------------------------------------------------- shutdown
    def destroy_node(self):
        if self.poller is not None:
            self.poller.shutdown()
        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = ExecutionListener()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
