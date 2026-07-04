"""Post-submission polling: async orders are watched to a terminal state,
cumulative fills are not double-counted, create_order is called exactly once,
and shutdown is clean. All exchanges are fakes — no network, no credentials."""
import time

import pytest
from conftest import BUS, MockNode, OrderMsg, _Logger

from exec_quality_analyzer.adapters import (
    VenueAdapter, VenueConfig, OrderStatusUnavailableError,
)
from exec_quality_analyzer.execution_listener import ExecutionListener
from exec_quality_analyzer.metrics_engine import MetricsEngine


class PollFakeExchange:
    """Fake ccxt exchange: scripted submit + fetch_order responses.

    The last fetch result repeats forever (steady state). Exception
    instances in the list are raised instead of returned.
    """
    def __init__(self, submit_result, fetch_results=()):
        self.urls = {'api': 'https://prod.example',
                     'test': 'https://test.example'}
        self.has = {'fetchOrder': True}
        self._submit = submit_result
        self._fetch = list(fetch_results)
        self.create_calls = 0
        self.fetch_calls = 0

    def set_sandbox_mode(self, flag):
        if flag:
            self.urls = dict(self.urls, api=self.urls['test'])

    def create_order(self, symbol, type, side, amount):
        self.create_calls += 1
        return self._submit

    def fetch_order(self, oid, symbol):
        self.fetch_calls += 1
        if not self._fetch:
            raise RuntimeError('fetch result exhausted')
        item = self._fetch.pop(0) if len(self._fetch) > 1 else self._fetch[0]
        if isinstance(item, Exception):
            raise item
        return item


def cfg(**kw):
    base = dict(venue='v1', mode='sandbox', ccxt_exchange_id='binance',
                dry_run=False, quote_retries=0, quote_retry_backoff_sec=0.0,
                poll_interval_sec=0.01, poll_max_duration_sec=0.5,
                poll_fetch_retries=2, poll_fetch_backoff_sec=0.01)
    base.update(kw)
    return VenueConfig(**base)


def make_listener(ex, **cfg_kw):
    a = VenueAdapter(cfg(**cfg_kw), client_factory=lambda c: ex,
                     logger=_Logger('a'))
    n = ExecutionListener.__new__(ExecutionListener)
    MockNode.__init__(n, 'listener')
    n.set_param('venue', 'v1')
    n.set_param('mode', 'sandbox')
    ExecutionListener.__init__(n, adapter=a)
    return n


def make_engine():
    e = MetricsEngine.__new__(MetricsEngine)
    MockNode.__init__(e, 'metrics_engine')
    MetricsEngine.__init__(e)
    captured = []
    BUS.subscribe('/metrics/per_order', captured.append)
    return e, captured


def order(oid='o1', qty=1.0, quoted=100.0, side='buy'):
    o = OrderMsg()
    o.order_id, o.venue, o.symbol, o.side = oid, 'v1', 'BTC/USDT', side
    o.quantity, o.quoted_price = qty, quoted
    o.reference_source = 'ask'
    o.quote_mode, o.execution_mode = 'venue', 'sandbox'
    o.submit_time = o.quote_time = time.time()
    return o


def wait_until(cond, timeout=3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.005)
    return cond()


# 1. Immediate full fill: terminal at submission, no polling at all.
def test_immediate_full_fill():
    ex = PollFakeExchange({'id': 'E1', 'status': 'closed', 'filled': 1.0,
                           'average': 100.2})
    listener = make_listener(ex)
    fills = []
    BUS.subscribe('/orders/fills', fills.append)
    listener.on_order(order())
    assert len(fills) == 1
    f = fills[0]
    assert f.status == 'filled' and f.filled_quantity == 1.0
    assert f.fill_price == pytest.approx(100.2)
    assert f.exchange_order_id == 'E1'
    assert ex.create_calls == 1 and ex.fetch_calls == 0
    listener.destroy_node()


# 2. Open then full fill resolved by polling.
def test_open_then_full_fill():
    ex = PollFakeExchange(
        {'id': 'E1', 'status': 'open', 'filled': 0},
        [{'id': 'E1', 'status': 'closed', 'filled': 1.0, 'average': 100.5}])
    listener = make_listener(ex)
    fills = []
    BUS.subscribe('/orders/fills', fills.append)
    listener.on_order(order())
    assert wait_until(lambda: any(f.status == 'filled' for f in fills))
    terminal = [f for f in fills if f.status == 'filled']
    assert len(terminal) == 1
    assert terminal[0].filled_quantity == pytest.approx(1.0)
    assert terminal[0].fill_price == pytest.approx(100.5)
    assert ex.create_calls == 1          # never resubmitted
    listener.destroy_node()


# 3 + 11. Open -> partial -> full: cumulative 'filled' converted to deltas,
# nothing double-counted end to end.
def test_partial_then_full_no_double_count():
    engine, metrics = make_engine()
    ex = PollFakeExchange(
        {'id': 'E1', 'status': 'open', 'filled': 0},
        [{'id': 'E1', 'status': 'open', 'filled': 0.4, 'average': 100.0},
         {'id': 'E1', 'status': 'closed', 'filled': 1.0, 'average': 100.6}])
    listener = make_listener(ex)
    o = order(qty=1.0)
    engine.on_order(o)
    listener.on_order(o)
    assert wait_until(lambda: len(metrics) == 1)
    m = metrics[0]
    assert m.status == 'filled' and m.filled
    assert m.filled_quantity == pytest.approx(1.0)     # NOT 1.4
    assert m.fill_ratio == pytest.approx(1.0)
    assert m.avg_fill_price == pytest.approx(100.6)    # venue cumulative avg
    assert m.slippage_bps == pytest.approx(60.0, abs=0.5)
    listener.destroy_node()


# 4. Open -> partial cancellation: terminal canceled_partial, fills kept.
def test_partial_cancellation():
    engine, metrics = make_engine()
    ex = PollFakeExchange(
        {'id': 'E1', 'status': 'open', 'filled': 0},
        [{'id': 'E1', 'status': 'open', 'filled': 0.4, 'average': 100.0},
         {'id': 'E1', 'status': 'canceled', 'filled': 0.4, 'average': 100.0}])
    listener = make_listener(ex)
    o = order(qty=1.0)
    engine.on_order(o)
    listener.on_order(o)
    assert wait_until(lambda: len(metrics) == 1)
    m = metrics[0]
    assert m.status == 'canceled_partial' and not m.filled
    assert m.filled_quantity == pytest.approx(0.4)
    assert m.fill_ratio == pytest.approx(0.4)
    assert m.slippage_bps == pytest.approx(0.0, abs=0.01)  # filled at ref
    listener.destroy_node()


# 5. Polling deadline with zero fill -> timeout.
def test_polling_timeout_zero_fill():
    ex = PollFakeExchange(
        {'id': 'E1', 'status': 'open', 'filled': 0},
        [{'id': 'E1', 'status': 'open', 'filled': 0}])
    listener = make_listener(ex, poll_max_duration_sec=0.08)
    fills = []
    BUS.subscribe('/orders/fills', fills.append)
    listener.on_order(order())
    assert wait_until(lambda: any(f.status == 'timeout' for f in fills))
    t = [f for f in fills if f.status == 'timeout'][0]
    assert t.filled_quantity == 0.0
    assert 'deadline' in t.error_reason
    listener.destroy_node()


# 6. Polling deadline with partial fill -> timeout_partial, slippage kept.
def test_polling_timeout_partial_fill():
    engine, metrics = make_engine()
    ex = PollFakeExchange(
        {'id': 'E1', 'status': 'open', 'filled': 0},
        [{'id': 'E1', 'status': 'open', 'filled': 0.5, 'average': 101.0}])
    listener = make_listener(ex, poll_max_duration_sec=0.12)
    o = order(qty=1.0)
    engine.on_order(o)
    listener.on_order(o)
    assert wait_until(lambda: len(metrics) == 1)
    m = metrics[0]
    assert m.status == 'timeout_partial' and not m.filled
    assert m.filled_quantity == pytest.approx(0.5)
    assert m.slippage_bps == pytest.approx(100.0, abs=0.5)  # preserved
    listener.destroy_node()


# 7. Temporary fetch failure, then success.
def test_temporary_fetch_failure_then_success():
    ex = PollFakeExchange(
        {'id': 'E1', 'status': 'open', 'filled': 0},
        [RuntimeError('transient network error'),
         {'id': 'E1', 'status': 'closed', 'filled': 1.0, 'average': 100.0}])
    listener = make_listener(ex)
    fills = []
    BUS.subscribe('/orders/fills', fills.append)
    listener.on_order(order())
    assert wait_until(lambda: any(f.status == 'filled' for f in fills))
    assert ex.create_calls == 1
    listener.destroy_node()


# 8. Permanent fetch failure -> terminal 'error' after limited retries.
def test_permanent_fetch_failure():
    ex = PollFakeExchange(
        {'id': 'E1', 'status': 'open', 'filled': 0},
        [RuntimeError('exchange is down')])
    listener = make_listener(ex, poll_fetch_retries=2)
    fills = []
    BUS.subscribe('/orders/fills', fills.append)
    listener.on_order(order())
    assert wait_until(lambda: any(f.status == 'error' for f in fills))
    err = [f for f in fills if f.status == 'error'][0]
    assert 'exchange is down' in err.error_reason
    assert ex.create_calls == 1          # status retries never resubmit
    assert ex.fetch_calls == 3           # initial + 2 retries
    listener.destroy_node()


# 9. Node shutdown during polling stops cleanly, publishes nothing fake.
def test_shutdown_during_polling():
    ex = PollFakeExchange(
        {'id': 'E1', 'status': 'open', 'filled': 0},
        [{'id': 'E1', 'status': 'open', 'filled': 0}])
    listener = make_listener(ex, poll_max_duration_sec=60.0)
    fills = []
    BUS.subscribe('/orders/fills', fills.append)
    listener.on_order(order())
    assert wait_until(lambda: ex.fetch_calls >= 1)
    listener.destroy_node()              # triggers poller.shutdown()
    assert wait_until(lambda: listener.poller.active_count() == 0)
    terminal = [f for f in fills if f.status not in ('partial',)]
    assert terminal == []                # no fabricated terminal result


# 10. Duplicate terminal exchange response: polling stops at the first.
def test_duplicate_terminal_response_single_event():
    ex = PollFakeExchange(
        {'id': 'E1', 'status': 'open', 'filled': 0},
        [{'id': 'E1', 'status': 'closed', 'filled': 1.0, 'average': 100.0},
         {'id': 'E1', 'status': 'closed', 'filled': 1.0, 'average': 100.0}])
    listener = make_listener(ex)
    fills = []
    BUS.subscribe('/orders/fills', fills.append)
    listener.on_order(order())
    assert wait_until(lambda: any(f.status == 'filled' for f in fills))
    time.sleep(0.05)                     # would catch a second poll cycle
    assert len([f for f in fills if f.status == 'filled']) == 1
    listener.destroy_node()


# 12. Initial partial at submission is the baseline: no double count.
def test_initial_partial_baseline():
    engine, metrics = make_engine()
    ex = PollFakeExchange(
        {'id': 'E1', 'status': 'open', 'filled': 0.3, 'average': 100.0},
        [{'id': 'E1', 'status': 'closed', 'filled': 1.0, 'average': 100.0}])
    listener = make_listener(ex)
    o = order(qty=1.0)
    engine.on_order(o)
    listener.on_order(o)
    assert wait_until(lambda: len(metrics) == 1)
    assert metrics[0].filled_quantity == pytest.approx(1.0)   # 0.3 + 0.7
    assert metrics[0].status == 'filled'
    listener.destroy_node()


# Missing exchange order id: finalized honestly instead of hopeless polling.
def test_missing_order_id_finalizes_error():
    ex = PollFakeExchange({'status': 'open', 'filled': 0})   # no 'id'
    listener = make_listener(ex)
    fills = []
    BUS.subscribe('/orders/fills', fills.append)
    listener.on_order(order())
    assert wait_until(lambda: any(f.status == 'error' for f in fills))
    assert 'no order id' in [f for f in fills if f.status == 'error'][0].error_reason
    assert ex.fetch_calls == 0
    listener.destroy_node()


# Adapter-level fallback when fetch_order is unsupported.
def test_fetch_order_status_fallback_methods():
    class NoFetchOrder:
        urls = {'api': 'p', 'test': 't'}
        has = {'fetchOrder': False}
        def set_sandbox_mode(self, flag):
            self.urls = dict(self.urls, api='t2')
        def fetch_order(self, oid, symbol):
            raise AssertionError('must not be called when unsupported')
        def fetch_open_order(self, oid, symbol):
            raise RuntimeError('OrderNotFound')
        def fetch_closed_order(self, oid, symbol):
            return {'id': oid, 'status': 'closed', 'filled': 1.0,
                    'average': 100.0}

    a = VenueAdapter(cfg(), client_factory=lambda c: NoFetchOrder(),
                     logger=_Logger('t'))
    r = a.fetch_order_status('E1', 'BTC/USDT')
    assert r['status'] == 'closed'


# Duplicate ROS delivery of the same OrderMsg: create_order exactly once.
def test_duplicate_order_msg_never_executes_twice():
    ex = PollFakeExchange({'id': 'E1', 'status': 'closed', 'filled': 1.0,
                           'average': 100.0})
    listener = make_listener(ex)
    o = order()
    listener.on_order(o)
    listener.on_order(o)        # duplicate delivery
    assert ex.create_calls == 1
    assert any('duplicate OrderMsg' in r[1]
               for r in listener.get_logger().records)
    listener.destroy_node()


def test_fetch_order_status_no_method_raises():
    class NoMethods:
        urls = {'api': 'p', 'test': 't'}
        has = {'fetchOrder': False}
        def set_sandbox_mode(self, flag):
            self.urls = dict(self.urls, api='t2')

    a = VenueAdapter(cfg(), client_factory=lambda c: NoMethods(),
                     logger=_Logger('t'))
    with pytest.raises(OrderStatusUnavailableError):
        a.fetch_order_status('E1', 'BTC/USDT')
