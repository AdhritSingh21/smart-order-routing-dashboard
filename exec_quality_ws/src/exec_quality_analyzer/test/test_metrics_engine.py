import time
import pytest
from conftest import BUS, MockNode, OrderMsg, FillMsg

from exec_quality_analyzer.metrics_engine import MetricsEngine


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
    return o


def fill(oid='o1', status='filled', price=100.0, qty=1.0, t=None):
    f = FillMsg()
    f.order_id, f.venue, f.symbol, f.status = oid, 'v1', 'BTC/USD', status
    f.fill_price, f.filled_quantity = price, qty
    f.fill_time = t if t is not None else time.time()
    return f


# 1. order followed by fill (normal path)
def test_order_then_fill():
    e, out = make_engine()
    e.on_order(order())
    e.on_fill(fill(price=100.1))
    assert len(out) == 1
    m = out[0]
    assert m.filled and m.status == 'filled'
    assert m.slippage_bps == pytest.approx(10.0, abs=0.01)  # buy worse by 10bps


# 2. fill arrives BEFORE order (race) -> buffered then processed
def test_fill_then_order_race():
    e, out = make_engine()
    e.on_fill(fill())
    assert out == []                       # not discarded, not yet emitted
    e.on_order(order())
    assert len(out) == 1 and out[0].filled


# 3. duplicate fill ignored
def test_duplicate_fill():
    e, out = make_engine()
    e.on_order(order(qty=2.0))
    f = fill(status='partial', qty=1.0, price=100.0, t=12345.0)
    e.on_fill(f)
    e.on_fill(f)  # exact duplicate
    assert out == []  # still only 1.0/2.0 filled, no metric yet
    assert e.pending['o1'].filled_qty == pytest.approx(1.0)


# 4. multiple partial fills -> quantity-weighted average price
def test_partial_fills_weighted_average():
    e, out = make_engine()
    e.on_order(order(side='buy', qty=2.0, quoted=100.0))
    e.on_fill(fill(status='partial', qty=1.0, price=100.0, t=1.0))
    e.on_fill(fill(status='partial', qty=1.0, price=102.0, t=2.0))
    assert len(out) == 1
    # weighted avg = 101.0 -> buy slippage = +100 bps
    assert out[0].slippage_bps == pytest.approx(100.0, abs=0.01)
    assert out[0].fill_ratio == pytest.approx(1.0)
    assert out[0].status == 'filled' and out[0].filled
    assert out[0].avg_fill_price == pytest.approx(101.0)


# 5. unmatched fill expiration
def test_unmatched_fill_expiry(monkeypatch):
    e, out = make_engine()
    e.on_fill(fill(oid='ghost'))
    assert 'ghost' in e.unmatched_fills
    monkeypatch.setattr(time, 'monotonic', lambda: time.time() + 999)
    e.sweep()
    assert 'ghost' not in e.unmatched_fills
    assert any('expired' in r[1] for r in e.get_logger().records)


# sell slippage sign: fill below bid reference = worse = positive
def test_sell_slippage_sign():
    e, out = make_engine()
    e.on_order(order(side='sell', quoted=100.0))
    e.on_fill(fill(price=99.9))
    assert out[0].slippage_bps == pytest.approx(10.0, abs=0.01)

def test_price_improvement_negative():
    e, out = make_engine()
    e.on_order(order(side='buy', quoted=100.0))
    e.on_fill(fill(price=99.9))
    assert out[0].slippage_bps == pytest.approx(-10.0, abs=0.01)


# rejected / quote_failed orders are terminal, unfilled
def test_rejection_terminal_unfilled():
    e, out = make_engine()
    e.on_order(order())
    e.on_fill(fill(status='rejected', price=0.0, qty=0.0))
    assert len(out) == 1 and not out[0].filled and out[0].status == 'rejected'

def test_quote_failed_terminal():
    e, out = make_engine()
    e.on_order(order(quoted=0.0))
    e.on_fill(fill(status='quote_failed', price=0.0, qty=0.0))
    assert len(out) == 1 and out[0].status == 'quote_failed'
    assert out[0].slippage_bps == 0.0  # no division by zero on quoted=0


# timeout sweep emits unfilled metric
def test_order_timeout():
    e, out = make_engine()
    e.on_order(order(t=time.time() - 999))
    e.sweep()
    assert len(out) == 1 and out[0].status == 'timeout' and not out[0].filled


# negative latency clamped to zero with warning
def test_negative_latency_clamped():
    e, out = make_engine()
    e.on_order(order(t=time.time() + 500))  # submit "in the future"
    e.on_fill(fill())
    assert out[0].latency_ms == 0.0
    assert any('negative latency' in r[1] for r in e.get_logger().records)


# malformed messages dropped without crashing
def test_malformed_messages_dropped():
    e, out = make_engine()
    e.on_order(OrderMsg())       # empty order_id
    e.on_fill(FillMsg())         # empty order_id
    assert out == [] and e.pending == {}
