"""Dashboard bridge: serialization, queueing, HTTP delivery, and shutdown."""
import json
import queue

import pytest
import requests
from conftest import BUS, MockNode, MetricMsg, VenueReport

from exec_quality_analyzer.dashboard_bridge import (
    DashboardBridge, message_to_json_safe)

VENUE_REPORT_FIELDS = {
    'venue', 'window_orders', 'fill_rate', 'slippage_bps_p50',
    'slippage_bps_p95', 'latency_ms_p50', 'latency_ms_p95',
    'latency_ms_p99', 'comparable', 'timestamp'}
METRIC_FIELDS = {
    'order_id', 'venue', 'symbol', 'status', 'slippage_bps', 'latency_ms',
    'fill_ratio', 'filled', 'requested_quantity', 'filled_quantity',
    'avg_fill_price', 'reference_price', 'exchange_status',
    'terminal_reason', 'quote_mode', 'execution_mode', 'comparable',
    'timestamp'}


class FakeResponse:
    def __init__(self, status_code=200):
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f'HTTP {self.status_code}')


class FakeSession:
    def __init__(self, fail_times=0, status_code=200,
                 exc_factory=lambda: requests.ConnectionError('refused')):
        self.posts = []
        self.fail_times = fail_times
        self.status_code = status_code
        self.exc_factory = exc_factory
        self.closed = False

    def post(self, url, json=None, timeout=None):
        self.posts.append({'url': url, 'json': json, 'timeout': timeout})
        if self.fail_times > 0:
            self.fail_times -= 1
            raise self.exc_factory()
        return FakeResponse(self.status_code)

    def close(self):
        self.closed = True


def build_bridge(params=None):
    """Build a bridge with the worker thread halted for deterministic tests."""
    n = DashboardBridge.__new__(DashboardBridge)
    MockNode.__init__(n, 'dashboard_bridge')
    defaults = dict(retry_backoff_sec=0.01, log_period_sec=60.0)
    defaults.update(params or {})
    for k, v in defaults.items():
        n.set_param(k, v)
    DashboardBridge.__init__(n)
    n._stop.set()
    n._worker.join(timeout=3.0)
    assert not n._worker.is_alive()
    n._stop.clear()              # _deliver() respects the stop flag
    n._session = FakeSession()
    return n


def drain(bridge):
    delivered = []
    while True:
        try:
            envelope = bridge.outbox.get_nowait()
        except queue.Empty:
            return delivered
        if bridge._deliver(envelope):
            delivered.append(envelope)


def make_venue_report():
    r = VenueReport()
    r.venue = 'binance_testnet'
    r.window_orders = 42
    r.fill_rate = 0.93
    r.slippage_bps_p50 = 1.2
    r.slippage_bps_p95 = 4.5
    r.latency_ms_p50 = 55.0
    r.latency_ms_p95 = 130.0
    r.latency_ms_p99 = 210.0
    r.comparable = True
    r.timestamp = 1765432100.5
    return r


def make_metric():
    m = MetricMsg()
    m.order_id = 'ord-1'
    m.venue = 'alpaca'
    m.symbol = 'BTC/USDT'
    m.status = 'filled'
    m.slippage_bps = -0.8
    m.latency_ms = 102.0
    m.fill_ratio = 1.0
    m.filled = True
    m.requested_quantity = 0.001
    m.filled_quantity = 0.001
    m.avg_fill_price = 64998.0
    m.reference_price = 65000.0
    m.exchange_status = 'closed'
    m.terminal_reason = 'fully filled'
    m.quote_mode = 'sim'
    m.execution_mode = 'sim'
    m.comparable = True
    m.timestamp = 1765432100.0
    return m


def test_venue_report_envelope_has_every_msg_field():
    bridge = build_bridge()
    BUS.publish('/reports/venue', make_venue_report())
    delivered = drain(bridge)
    assert len(delivered) == 1
    envelope = delivered[0]
    assert envelope['type'] == 'venue_report'
    assert set(envelope['data']) == VENUE_REPORT_FIELDS
    assert envelope['data']['venue'] == 'binance_testnet'
    assert envelope['data']['window_orders'] == 42
    assert envelope['data']['comparable'] is True
    json.dumps(envelope)  # JSON-safe end to end


def test_metric_envelope_has_every_msg_field():
    bridge = build_bridge()
    BUS.publish('/metrics/per_order', make_metric())
    delivered = drain(bridge)
    assert len(delivered) == 1
    envelope = delivered[0]
    assert envelope['type'] == 'metric'
    assert set(envelope['data']) == METRIC_FIELDS
    assert envelope['data']['slippage_bps'] == -0.8
    assert envelope['data']['filled'] is True
    json.dumps(envelope)


def test_numpy_and_ros_time_values_become_json_safe():
    np = pytest.importorskip('numpy')

    class TimeLike:
        sec = 12
        nanosec = 500_000_000

    class WeirdMsg:
        def __init__(self):
            self.score = np.float64(3.5)
            self.count = np.int32(7)
            self.values = np.array([1.0, 2.0])
            self.raw = b'\x01\x02'
            self.stamp = TimeLike()

    data = message_to_json_safe(WeirdMsg())
    assert json.loads(json.dumps(data)) == {
        'score': 3.5, 'count': 7, 'values': [1.0, 2.0],
        'raw': [1, 2], 'stamp': 12.5}


def test_post_targets_configured_url_with_timeout():
    bridge = build_bridge(dict(
        backend_ingest_url='http://10.0.0.5:9999/ingest',
        http_timeout_sec=1.5))
    BUS.publish('/reports/venue', make_venue_report())
    drain(bridge)
    post = bridge._session.posts[0]
    assert post['url'] == 'http://10.0.0.5:9999/ingest'
    assert post['timeout'] == 1.5


def test_connection_failure_retries_then_drops_without_crash():
    bridge = build_bridge(dict(max_retries=2))
    bridge._session = FakeSession(fail_times=99)
    BUS.publish('/reports/venue', make_venue_report())
    delivered = drain(bridge)
    assert delivered == []
    assert len(bridge._session.posts) == 3          # initial + 2 retries
    assert any('delivery' in m and 'failed' in m
               for lvl, m in bridge.get_logger().records if lvl == 'warn')


def test_transient_failure_recovers_via_retry():
    bridge = build_bridge(dict(max_retries=2))
    bridge._session = FakeSession(fail_times=1)
    BUS.publish('/reports/venue', make_venue_report())
    delivered = drain(bridge)
    assert len(delivered) == 1
    assert len(bridge._session.posts) == 2


def test_http_error_status_is_handled_like_a_failure():
    bridge = build_bridge(dict(max_retries=0))
    bridge._session = FakeSession(status_code=422)
    BUS.publish('/metrics/per_order', make_metric())
    assert drain(bridge) == []


def test_full_queue_drops_oldest_and_keeps_newest():
    bridge = build_bridge(dict(queue_size=2))
    for i in range(5):
        report = make_venue_report()
        report.window_orders = i
        BUS.publish('/reports/venue', report)
    assert bridge.outbox.qsize() == 2
    kept = [bridge.outbox.get_nowait()['data']['window_orders']
            for _ in range(2)]
    assert kept == [3, 4]
    warns = [m for lvl, m in bridge.get_logger().records if lvl == 'warn']
    assert sum('outbox full' in m for m in warns) == 1  # throttled


def test_failure_warnings_are_throttled():
    bridge = build_bridge(dict(max_retries=0))
    bridge._session = FakeSession(fail_times=99)
    for _ in range(20):
        BUS.publish('/reports/venue', make_venue_report())
    drain(bridge)
    warns = [m for lvl, m in bridge.get_logger().records
             if lvl == 'warn' and 'failed' in m]
    assert len(warns) == 1


def test_success_logs_do_not_flood():
    bridge = build_bridge()
    for _ in range(50):
        BUS.publish('/metrics/per_order', make_metric())
    delivered = drain(bridge)
    assert len(delivered) == 50
    infos = [m for lvl, m in bridge.get_logger().records if lvl == 'info']
    # startup banner + first-delivery announcement + at most one summary
    assert len(infos) <= 3


def test_subscription_callbacks_do_no_network_io():
    bridge = build_bridge()
    BUS.publish('/reports/venue', make_venue_report())
    BUS.publish('/metrics/per_order', make_metric())
    assert bridge._session.posts == []              # only queued
    assert bridge.outbox.qsize() == 2


def test_clean_shutdown_stops_worker_and_closes_session():
    n = DashboardBridge.__new__(DashboardBridge)
    MockNode.__init__(n, 'dashboard_bridge')
    n.set_param('retry_backoff_sec', 0.01)
    DashboardBridge.__init__(n)
    session = FakeSession()
    n._session = session
    assert n._worker.is_alive()
    n.destroy_node()
    assert not n._worker.is_alive()
    assert session.closed


def test_invalid_parameters_rejected():
    with pytest.raises(ValueError):
        build_bridge(dict(queue_size=0))
    with pytest.raises(ValueError):
        build_bridge(dict(http_timeout_sec=0.0))
