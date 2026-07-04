"""Terminal canceled/rejected partial fills: normalized status model,
centralized CCXT parsing, and immediate idempotent finalization."""
import time

import pytest
from conftest import BUS, MockNode, OrderMsg, FillMsg

from exec_quality_analyzer.order_status import (
    OrderStatus, is_terminal, normalize_ccxt_status, parse_ccxt_order,
    with_fill_qualifier, weighted_average_from_trades,
    TERMINAL_STATUSES, NONTERMINAL_STATUSES,
)
from exec_quality_analyzer.metrics_engine import MetricsEngine


# ---------------------------------------------------------------- unit level
def test_terminal_and_nonterminal_sets_disjoint():
    assert not (TERMINAL_STATUSES & NONTERMINAL_STATUSES)
    for s in ('open', 'pending', 'partial'):
        assert not is_terminal(s)
    for s in ('filled', 'canceled', 'canceled_partial', 'rejected',
              'rejected_partial', 'expired', 'expired_partial', 'timeout',
              'timeout_partial', 'dry_run', 'error', 'quote_failed',
              'submit_failed'):
        assert is_terminal(s)


def test_with_fill_qualifier():
    assert with_fill_qualifier('canceled', 0.3) == 'canceled_partial'
    assert with_fill_qualifier('canceled', 0.0) == 'canceled'
    assert with_fill_qualifier('rejected', 0.1) == 'rejected_partial'
    assert with_fill_qualifier('expired', 0.1) == 'expired_partial'
    assert with_fill_qualifier('timeout', 0.1) == 'timeout_partial'
    assert with_fill_qualifier('error', 0.1) == 'error'   # no variant


@pytest.mark.parametrize('raw,filled,expected', [
    ('canceled', 0.0, 'canceled'),
    ('cancelled', 0.0, 'canceled'),
    ('canceled', 0.3, 'canceled_partial'),
    ('canceled', 1.0, 'filled'),       # fully filled then "canceled" remainder
    ('closed', 1.0, 'filled'),
    ('closed', 0.4, 'canceled_partial'),   # e.g. IOC remainder canceled
    ('rejected', 0.0, 'rejected'),
    ('rejected', 0.2, 'rejected_partial'),
    ('expired', 0.0, 'expired'),
    ('expired', 0.7, 'expired_partial'),
    ('open', 0.0, 'open'),
    ('open', 0.5, 'partial'),
    ('open', 1.0, 'filled'),
    ('pending', 0.0, 'pending'),
    ('', 0.0, 'pending'),              # unknown stays NON-terminal
    ('weird_status', 0.0, 'pending'),
    ('weird_status', 0.5, 'partial'),
    (None, 1.0, 'filled'),
])
def test_normalize_ccxt_status(raw, filled, expected):
    assert normalize_ccxt_status(raw, filled, 1.0) == expected


def test_parser_missing_filled_never_means_full():
    r = parse_ccxt_order({'status': 'closed'}, 1.0)
    assert r['filled_quantity'] == 0.0
    assert r['status'] == 'canceled'   # terminal close with zero fill


def test_parser_zero_is_not_none():
    r = parse_ccxt_order({'filled': 0, 'status': 'open'}, 1.0)
    assert r['filled_quantity'] == 0.0 and r['status'] == 'open'


def test_parser_weighted_average_from_trades():
    r = parse_ccxt_order({'status': 'closed', 'filled': 1.0,
                          'trades': [
                              {'price': 100.0, 'amount': 0.5},
                              {'price': 102.0, 'amount': 0.5},
                              {'price': 'junk', 'amount': 1.0},  # skipped
                          ]}, 1.0)
    assert r['average_price'] == pytest.approx(101.0)
    assert r['status'] == 'filled'


def test_weighted_average_helper_malformed():
    assert weighted_average_from_trades(None) == (0.0, 0.0)
    assert weighted_average_from_trades([{'price': -1, 'amount': 2}]) == (0.0, 0.0)


def test_parser_retains_ids_and_converts_timestamp():
    r = parse_ccxt_order({'id': 'EX-1', 'clientOrderId': 'CL-1',
                          'status': 'open', 'filled': 0,
                          'timestamp': 1750000000000}, 1.0)
    assert r['exchange_order_id'] == 'EX-1'
    assert r['client_order_id'] == 'CL-1'
    assert r['order_timestamp'] == pytest.approx(1750000000.0)


def test_parser_malformed_response():
    r = parse_ccxt_order('not a dict', 1.0)
    assert r['status'] == 'error' and 'malformed' in r['error_reason']
    r2 = parse_ccxt_order({'filled': 'garbage', 'status': 'open'}, 1.0)
    assert r2['filled_quantity'] == 0.0 and "non-numeric 'filled'" in r2['error_reason']
    r3 = parse_ccxt_order({'filled': -2.0, 'status': 'open'}, 1.0)
    assert r3['filled_quantity'] == 0.0


# ------------------------------------------------------------- engine level
def make_engine():
    e = MetricsEngine.__new__(MetricsEngine)
    MockNode.__init__(e, 'metrics_engine')
    MetricsEngine.__init__(e)
    captured = []
    BUS.subscribe('/metrics/per_order', captured.append)
    return e, captured


def order(oid='o1', side='buy', qty=1.0, quoted=100.0, t=None):
    o = OrderMsg()
    o.order_id, o.venue, o.symbol, o.side = oid, 'v1', 'BTC/USD', side
    o.quantity, o.quoted_price = qty, quoted
    o.reference_source = 'ask' if side == 'buy' else 'bid'
    o.submit_time = t if t is not None else time.time()
    o.quote_time = o.submit_time
    o.quote_mode, o.execution_mode = 'venue', 'sandbox'
    return o


def fill(oid='o1', status='filled', price=100.0, qty=1.0, t=None,
         reason='', exchange_status=''):
    f = FillMsg()
    f.order_id, f.venue, f.symbol, f.status = oid, 'v1', 'BTC/USD', status
    f.fill_price, f.filled_quantity = price, qty
    f.fill_time = t if t is not None else time.time()
    f.error_reason = reason
    f.exchange_status = exchange_status
    return f


# 1. Zero-filled canceled order: terminal unfilled cancellation.
def test_zero_filled_cancel_terminal():
    e, out = make_engine()
    e.on_order(order())
    e.on_fill(fill(status='canceled', price=0.0, qty=0.0,
                   exchange_status='canceled'))
    assert len(out) == 1
    m = out[0]
    assert m.status == 'canceled' and not m.filled
    assert m.fill_ratio == 0.0 and m.filled_quantity == 0.0
    assert m.slippage_bps == 0.0
    assert m.exchange_status == 'canceled'


# 2. Partially filled canceled order: terminal, metrics preserved.
def test_partial_cancel_terminal_preserves_everything():
    e, out = make_engine()
    e.on_order(order(side='buy', qty=2.0, quoted=100.0))
    e.on_fill(fill(status='partial', qty=0.5, price=100.5, t=time.time()))
    assert out == []   # non-terminal partial: keep waiting
    e.on_fill(fill(status='canceled_partial', qty=0.5, price=101.5,
                   exchange_status='canceled',
                   reason='venue canceled remainder'))
    assert len(out) == 1
    m = out[0]
    assert m.status == 'canceled_partial' and not m.filled
    assert m.requested_quantity == pytest.approx(2.0)
    assert m.filled_quantity == pytest.approx(1.0)
    assert m.fill_ratio == pytest.approx(0.5)
    assert m.avg_fill_price == pytest.approx(101.0)
    assert m.reference_price == pytest.approx(100.0)
    # weighted avg 101.0 vs ref 100.0 -> +100 bps, NOT overwritten with 0.0
    assert m.slippage_bps == pytest.approx(100.0, abs=0.01)
    assert m.latency_ms >= 0.0
    assert m.exchange_status == 'canceled'
    assert m.terminal_reason == 'venue canceled remainder'


# 2b. A terminal 'canceled' event arriving with the fills already
# accumulated maps to canceled_partial via the engine.
def test_cancel_after_partials_becomes_canceled_partial():
    e, out = make_engine()
    e.on_order(order(qty=2.0))
    e.on_fill(fill(status='partial', qty=0.8, price=100.0, t=1.0))
    e.on_fill(fill(status='canceled', qty=0.0, price=0.0))
    assert len(out) == 1 and out[0].status == 'canceled_partial'
    assert out[0].filled_quantity == pytest.approx(0.8)


# 3. Completely filled order reported with closed/canceled-like response.
def test_complete_fill_with_canceled_like_response_is_filled():
    e, out = make_engine()
    e.on_order(order(qty=1.0))
    e.on_fill(fill(status='canceled_partial', qty=1.0, price=100.0,
                   exchange_status='canceled'))
    # cumulative reached requested -> it IS a complete fill
    assert len(out) == 1
    assert out[0].status == 'filled' and out[0].filled
    assert out[0].fill_ratio == pytest.approx(1.0)


# 4. Rejected order with zero fill.
def test_rejected_zero_fill():
    e, out = make_engine()
    e.on_order(order())
    e.on_fill(fill(status='rejected', price=0.0, qty=0.0,
                   reason='insufficient funds'))
    assert len(out) == 1
    m = out[0]
    assert m.status == 'rejected' and not m.filled and m.fill_ratio == 0.0
    assert m.terminal_reason == 'insufficient funds'


def test_rejected_partial_preserved():
    e, out = make_engine()
    e.on_order(order(qty=1.0))
    e.on_fill(fill(status='rejected_partial', qty=0.4, price=99.5))
    assert len(out) == 1
    assert out[0].status == 'rejected_partial'
    assert out[0].slippage_bps == pytest.approx(-50.0, abs=0.01)  # improvement


# 5+6. Terminal partial publishes IMMEDIATELY with slippage preserved.
def test_terminal_partial_immediate():
    e, out = make_engine()
    e.on_order(order(qty=2.0))
    e.on_fill(fill(status='canceled_partial', qty=1.0, price=102.0))
    assert len(out) == 1               # immediately, no timeout wait
    assert out[0].slippage_bps == pytest.approx(200.0, abs=0.01)
    assert e.pending == {}             # nothing left to time out


# 7. Terminal partial result is NOT later replaced by a timeout.
def test_terminal_partial_not_replaced_by_timeout(monkeypatch):
    e, out = make_engine()
    e.on_order(order(qty=2.0, t=time.time() - 999))   # would be "timed out"
    e.on_fill(fill(status='canceled_partial', qty=1.0, price=101.0))
    assert len(out) == 1 and out[0].status == 'canceled_partial'
    e.sweep()                                          # sweep finds nothing
    assert len(out) == 1


# Duplicate terminal updates produce exactly one metric.
def test_duplicate_terminal_one_metric():
    e, out = make_engine()
    e.on_order(order())
    f1 = fill(status='canceled', qty=0.0, price=0.0, t=123.0)
    e.on_fill(f1)
    e.on_fill(fill(status='canceled', qty=0.0, price=0.0, t=123.0))
    e.on_fill(fill(status='canceled', qty=0.0, price=0.0, t=124.0))
    assert len(out) == 1
    # and the late duplicates were not buffered as "unmatched"
    assert e.unmatched_fills == {}


# Duplicate OrderMsg does not reset accumulated fills.
def test_duplicate_order_msg_ignored():
    e, out = make_engine()
    e.on_order(order(qty=2.0))
    e.on_fill(fill(status='partial', qty=1.0, price=100.0, t=1.0))
    e.on_order(order(qty=2.0))         # duplicate
    assert e.pending['o1'].filled_qty == pytest.approx(1.0)


# Timeout with partial fills -> timeout_partial, slippage preserved.
def test_timeout_partial_preserves_slippage():
    e, out = make_engine()
    e.on_order(order(qty=2.0, t=time.time() - 999))
    e.on_fill(fill(status='partial', qty=1.0, price=101.0,
                   t=time.time() - 998))
    e.sweep()
    assert len(out) == 1
    m = out[0]
    assert m.status == 'timeout_partial' and not m.filled
    assert m.fill_ratio == pytest.approx(0.5)
    assert m.slippage_bps == pytest.approx(100.0, abs=0.01)
    assert m.filled_quantity == pytest.approx(1.0)
    assert 'no terminal venue status' in m.terminal_reason


# dry_run is a terminal, unfilled, clearly-labeled outcome.
def test_dry_run_terminal():
    e, out = make_engine()
    e.on_order(order())
    e.on_fill(fill(status='dry_run', qty=0.0, price=0.0,
                   reason='dry_run: order constructed but not submitted'))
    assert len(out) == 1
    assert out[0].status == 'dry_run' and not out[0].filled
