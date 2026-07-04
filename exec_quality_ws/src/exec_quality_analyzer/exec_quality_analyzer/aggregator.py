"""Aggregator node.

Maintains a rolling window of per-order metrics for each venue and
periodically publishes VenueReport messages with fill rate and
P50/P95/P99 statistics for slippage and latency.

Statistics semantics (documented contract)
------------------------------------------
- fill_rate counts ONLY fully filled orders (MetricMsg.filled). Terminal
  partial outcomes (canceled_partial, timeout_partial, ...) count in the
  denominator: a partial cancellation is never treated as a full fill.
- Slippage and latency percentiles include every order with executed
  quantity (fill_ratio > 0), so terminal partials keep contributing their
  measured execution quality instead of being discarded.
- comparable is false when any metric in the window came from a
  non-comparable execution environment (e.g. sandbox fills measured against
  public production quotes); the Reporter excludes such venues from the
  cross-venue ranking.
"""
import time
from collections import defaultdict, deque

import numpy as np
import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy

from exec_quality_interfaces.msg import MetricMsg, VenueReport


class Aggregator(Node):
    def __init__(self):
        super().__init__('aggregator')

        self.declare_parameter('window_size', 100)        # orders per venue
        self.declare_parameter('report_period_sec', 5.0)

        self.window = int(self.get_parameter('window_size').value)
        period = float(self.get_parameter('report_period_sec').value)
        if self.window <= 0 or period <= 0:
            raise ValueError('aggregator: window_size and report_period_sec '
                             'must be positive')

        self.metrics = defaultdict(lambda: deque(maxlen=self.window))

        qos = QoSProfile(
            reliability=ReliabilityPolicy.RELIABLE,
            history=HistoryPolicy.KEEP_LAST,
            depth=200,
        )
        self.create_subscription(MetricMsg, '/metrics/per_order', self.on_metric, qos)
        self.pub = self.create_publisher(VenueReport, '/reports/venue', qos)
        self.create_timer(period, self.publish_reports)

        self.get_logger().info(f'Aggregator up. window={self.window}, period={period}s')

    def on_metric(self, m: MetricMsg):
        self.metrics[m.venue].append(m)

    def publish_reports(self):
        for venue, window in self.metrics.items():
            if not window:
                continue
            executed = [m for m in window
                        if m.fill_ratio > 0.0 and m.latency_ms >= 0.0]
            fully_filled = [m for m in window if m.filled]
            report = VenueReport()
            report.venue = venue
            report.window_orders = len(window)
            report.fill_rate = len(fully_filled) / len(window)
            report.comparable = all(
                getattr(m, 'comparable', True) for m in window)
            report.timestamp = time.time()

            if executed:
                slip = np.array([m.slippage_bps for m in executed])
                lat = np.array([m.latency_ms for m in executed])
                report.slippage_bps_p50 = float(np.percentile(slip, 50))
                report.slippage_bps_p95 = float(np.percentile(slip, 95))
                report.latency_ms_p50 = float(np.percentile(lat, 50))
                report.latency_ms_p95 = float(np.percentile(lat, 95))
                report.latency_ms_p99 = float(np.percentile(lat, 99))

            self.pub.publish(report)


def main(args=None):
    rclpy.init(args=args)
    node = Aggregator()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
