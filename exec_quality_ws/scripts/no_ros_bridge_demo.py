"""Run the full analyzer pipeline + dashboard bridge WITHOUT ROS 2.

Reuses the test suite's mock rclpy layer (test/conftest.py) so the REAL node
code — OrderSubmitter, ExecutionListeners, MetricsEngine, Aggregator, and
DashboardBridge — runs over an in-process bus, while the bridge performs real
HTTP POSTs to the FastAPI backend. Useful on machines without ROS 2
(e.g. Windows dev boxes) to exercise the complete path:

    sim orders -> /metrics/per_order -> /reports/venue
               -> dashboard_bridge -> POST /ingest -> /ws -> dashboard

On Ubuntu with ROS 2 Humble installed, use the real launch instead:
    ros2 launch exec_quality_analyzer analyzer.launch.py

Usage:
    python scripts/no_ros_bridge_demo.py [--ingest-url URL]
        [--submit-period SEC] [--report-period SEC] [--run-seconds SEC]
"""
import argparse
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PKG = os.path.join(ROOT, 'src', 'exec_quality_analyzer')
sys.path.insert(0, os.path.join(PKG, 'test'))
sys.path.insert(0, PKG)

import conftest  # noqa: E402,F401  (installs mock rclpy + interface messages)
from conftest import MockNode  # noqa: E402

from exec_quality_analyzer.order_submitter import OrderSubmitter  # noqa: E402
from exec_quality_analyzer.execution_listener import ExecutionListener  # noqa: E402
from exec_quality_analyzer.metrics_engine import MetricsEngine  # noqa: E402
from exec_quality_analyzer.aggregator import Aggregator  # noqa: E402
from exec_quality_analyzer.dashboard_bridge import DashboardBridge  # noqa: E402

VENUES = [
    ('alpaca', dict(sim_latency_ms_mean=120.0, sim_latency_ms_std=40.0,
                    sim_slippage_bps_mean=1.0, sim_slippage_bps_std=1.5,
                    sim_reject_prob=0.02)),
    ('binance_testnet', dict(sim_latency_ms_mean=60.0, sim_latency_ms_std=20.0,
                             sim_slippage_bps_mean=2.5, sim_slippage_bps_std=3.0,
                             sim_reject_prob=0.05)),
    ('coinbase_sandbox', dict(sim_latency_ms_mean=95.0, sim_latency_ms_std=30.0,
                              sim_slippage_bps_mean=1.8, sim_slippage_bps_std=2.0,
                              sim_reject_prob=0.03)),
]


def build(cls, name, params):
    node = cls.__new__(cls)
    MockNode.__init__(node, name)
    for key, value in params.items():
        node.set_param(key, value)
    cls.__init__(node)
    return node


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--ingest-url', default='http://127.0.0.1:8000/ingest')
    parser.add_argument('--submit-period', type=float, default=0.5)
    parser.add_argument('--report-period', type=float, default=2.0)
    parser.add_argument('--run-seconds', type=float, default=None)
    args = parser.parse_args()

    build(MetricsEngine, 'metrics_engine', {})
    for venue, sim_params in VENUES:
        build(ExecutionListener, f'{venue}_listener',
              dict(venue=venue, mode='sim', **sim_params))
    aggregator = build(Aggregator, 'aggregator', {})
    bridge = build(DashboardBridge, 'dashboard_bridge',
                   dict(backend_ingest_url=args.ingest_url))
    submitter = build(OrderSubmitter, 'order_submitter',
                      dict(venues=[v for v, _ in VENUES], mode='sim',
                           submit_period_sec=args.submit_period))

    print(f'no-ROS demo up: 3 sim venues -> bridge -> {args.ingest_url} '
          '(Ctrl+C to stop)', flush=True)
    started = time.monotonic()
    next_report = started + args.report_period
    try:
        while True:
            submitter.submit_next()
            now = time.monotonic()
            if now >= next_report:
                aggregator.publish_reports()
                next_report = now + args.report_period
                for level, message in bridge.get_logger().records:
                    print(f'[bridge/{level}] {message}', flush=True)
                bridge.get_logger().records.clear()
            if args.run_seconds and now - started >= args.run_seconds:
                break
            time.sleep(args.submit_period)
    except KeyboardInterrupt:
        pass
    finally:
        bridge.destroy_node()
        print('demo stopped.')


if __name__ == '__main__':
    main()
