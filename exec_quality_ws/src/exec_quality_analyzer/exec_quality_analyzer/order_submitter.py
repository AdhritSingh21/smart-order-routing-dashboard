"""OrderSubmitter node.

Round-robins small test orders across configured venues on a timer.

Topology validation (startup, before any order can be submitted)
----------------------------------------------------------------
The complete execution topology — the submitter's mode plus every target
venue's execution and market-data mode — is validated by
exec_quality_analyzer.topology.validate_topology() in __init__. Incompatible
configurations (e.g. a sandbox venue whose reference prices would be
simulated) fail startup with TopologyConfigError; nothing continues
silently. The all-sim default validates with no credentials or network.

Reference price semantics
-------------------------
- sim venues: random-walk reference price (reference_source="sim",
  quote_mode="sim").
- sandbox/production venues: a fresh quote is fetched from THE TARGET
  VENUE's configured market-data source immediately before submission —
  best ask for buys, best bid for sells (fallback: ticker last, flagged
  "last"). A quote from one exchange is never used as the reference for an
  order sent to another exchange. If no valid fresh quote can be obtained
  the order is NOT submitted; a FillMsg with status="quote_failed" is
  published instead so the metrics pipeline records the failure honestly.

Every order records: target venue, quote venue, quote mode, execution mode,
reference bid/ask, quote timestamp + timestamp source, and ranking
comparability.

Per-venue configuration is given as comma-separated lists aligned with
`venues` (empty string = inherit defaults):
  venue_modes:             "sim,sandbox,sim"
  venue_market_data_modes: "venue,public,venue"
  venue_exchange_ids:      "alpaca,binance,coinbase"
"""
import time
import uuid
import random

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy

from exec_quality_interfaces.msg import OrderMsg, FillMsg

from exec_quality_analyzer.adapters import (
    VenueAdapter, VenueConfig, QuoteUnavailableError,
)
from exec_quality_analyzer.order_status import OrderStatus
from exec_quality_analyzer.topology import validate_topology


def _csv_list(raw: str, n: int, default: str) -> list[str]:
    """Parse a comma-separated per-venue list; '' -> n defaults."""
    if not raw or not str(raw).strip():
        return [default] * n
    items = [s.strip() for s in str(raw).split(',')]
    return items


class OrderSubmitter(Node):
    def __init__(self, adapters: dict | None = None):
        super().__init__('order_submitter')

        self.declare_parameter('venues', ['alpaca', 'binance_testnet', 'coinbase_sandbox'])
        self.declare_parameter('symbol', 'BTC/USDT')
        self.declare_parameter('order_quantity', 0.001)
        self.declare_parameter('submit_period_sec', 2.0)
        self.declare_parameter('mode', 'sim')               # default venue mode
        self.declare_parameter('reference_price', 65000.0)  # sim-mode anchor
        self.declare_parameter('venue_modes', '')
        self.declare_parameter('venue_market_data_modes', '')
        self.declare_parameter('venue_exchange_ids', '')
        self.declare_parameter('allow_production_trading', False)
        self.declare_parameter('allow_sim_reference_for_live_testing', False)

        self.venues = list(self.get_parameter('venues').value)
        self.symbol = self.get_parameter('symbol').value
        self.qty = float(self.get_parameter('order_quantity').value)
        self.mode = self.get_parameter('mode').value
        period = float(self.get_parameter('submit_period_sec').value)
        self.ref_price = float(self.get_parameter('reference_price').value)
        allow_prod = bool(self.get_parameter('allow_production_trading').value)
        allow_sim_ref = bool(self.get_parameter(
            'allow_sim_reference_for_live_testing').value)

        if not self.venues:
            raise ValueError('order_submitter: venues list must not be empty')
        if self.qty <= 0:
            raise ValueError('order_submitter: order_quantity must be positive')
        if period <= 0:
            raise ValueError('order_submitter: submit_period_sec must be positive')

        n = len(self.venues)
        modes = _csv_list(self.get_parameter('venue_modes').value, n, self.mode)
        md_modes = _csv_list(
            self.get_parameter('venue_market_data_modes').value, n, 'venue')
        ex_ids = _csv_list(self.get_parameter('venue_exchange_ids').value, n, '')
        if not (len(modes) == len(md_modes) == len(ex_ids) == n):
            raise ValueError(
                f'order_submitter: per-venue lists must align with venues '
                f'({n} venues, {len(modes)} modes, {len(md_modes)} '
                f'market-data modes, {len(ex_ids)} exchange ids)')

        # Validate the COMPLETE topology before any timer/publisher exists —
        # incompatible modes must fail startup, not mislabel metrics later.
        specs = []
        for i, v in enumerate(self.venues):
            specs.append({
                'venue': v,
                'mode': modes[i],
                'market_data_mode': md_modes[i],
                'ccxt_exchange_id': ex_ids[i] or (
                    v.split('_')[0] if modes[i] != 'sim' else ''),
            })
        self.topology = validate_topology(
            self.mode, specs,
            allow_production_trading=allow_prod,
            allow_sim_reference_for_live_testing=allow_sim_ref)

        # Quote-only adapters for venues that need real reference quotes.
        # quote_only=True means these adapters physically cannot submit.
        # The caller can inject pre-built adapters (tests).
        self.adapters: dict = adapters or {}
        for v, topo in self.topology.items():
            if topo.quote_mode == 'sim' or v in self.adapters:
                continue
            self.adapters[v] = VenueAdapter(
                VenueConfig(venue=v, mode=topo.execution_mode,
                            ccxt_exchange_id=topo.ccxt_exchange_id,
                            market_data_mode=topo.market_data_mode,
                            quote_only=True,
                            allow_production_trading=allow_prod),
                logger=self.get_logger())

        qos = QoSProfile(reliability=ReliabilityPolicy.RELIABLE,
                         history=HistoryPolicy.KEEP_LAST, depth=100)
        self.pub = self.create_publisher(OrderMsg, '/orders/submit', qos)
        self.fail_pub = self.create_publisher(FillMsg, '/orders/fills', qos)
        self.timer = self.create_timer(period, self.submit_next)
        self._rr_index = 0

        self.get_logger().info(
            f'OrderSubmitter up | mode={self.mode} venues={self.venues} '
            f'symbol={self.symbol} period={period}s topology=' + ', '.join(
                f'{t.venue}[exec={t.execution_mode} quote={t.quote_mode}'
                f' comparable={t.comparable}]'
                for t in self.topology.values()))

    # ------------------------------------------------------------------
    def submit_next(self):
        venue = self.venues[self._rr_index % len(self.venues)]
        self._rr_index += 1
        topo = self.topology[venue]
        side = random.choice(['buy', 'sell'])
        order_id = str(uuid.uuid4())

        if topo.quote_mode == 'sim':
            self.ref_price *= 1.0 + random.gauss(0.0, 0.0004)
            quoted = round(self.ref_price, 2)
            quote = dict(price=quoted, source='sim', quote_time=time.time(),
                         bid=quoted, ask=quoted, timestamp_source='sim')
            symbol = self.symbol
        else:
            adapter = self.adapters[venue]
            symbol = adapter.normalized(self.symbol)
            try:
                q = adapter.get_quote(self.symbol, side)
                quote = dict(price=q.price, source=q.source,
                             quote_time=q.quote_time, bid=q.bid, ask=q.ask,
                             timestamp_source=q.timestamp_source)
            except QuoteUnavailableError as exc:
                self.get_logger().error(
                    f'{venue}: quote failed, order NOT submitted '
                    f'({order_id[:8]}): {exc}')
                # Publish the order context first so metrics can match it,
                # then the failure event.
                self._publish_order(
                    order_id, venue, symbol, side, topo,
                    dict(price=0.0, source='none', quote_time=time.time(),
                         bid=0.0, ask=0.0, timestamp_source='none'))
                fail = FillMsg()
                fail.order_id = order_id
                fail.venue = venue
                fail.symbol = symbol
                fail.status = OrderStatus.QUOTE_FAILED
                fail.fill_price = 0.0
                fail.filled_quantity = 0.0
                fail.fill_time = time.time()
                fail.error_reason = str(exc)
                self.fail_pub.publish(fail)
                return

        self._publish_order(order_id, venue, symbol, side, topo, quote)
        self.get_logger().info(
            f"SUBMIT {side} {self.qty} {symbol} @ {quote['price']} "
            f"({quote['source']}/{quote['timestamp_source']}) "
            f'-> {venue} ({order_id[:8]})')

    def _publish_order(self, order_id, venue, symbol, side, topo, quote):
        msg = OrderMsg()
        msg.order_id = order_id
        msg.venue = venue
        msg.symbol = symbol
        msg.side = side
        msg.quantity = self.qty
        msg.quoted_price = float(quote['price'])
        msg.reference_source = quote['source']
        msg.quote_time = float(quote['quote_time'])
        msg.submit_time = time.time()
        msg.quote_venue = venue
        msg.quote_mode = topo.quote_mode
        msg.execution_mode = topo.execution_mode
        msg.timestamp_source = quote['timestamp_source']
        msg.bid = float(quote['bid'])
        msg.ask = float(quote['ask'])
        msg.comparable = topo.comparable
        self.pub.publish(msg)


def main(args=None):
    rclpy.init(args=args)
    node = OrderSubmitter()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
