"""Integration: real node code end-to-end in sim mode over the mock bus."""
import time
import csv
import pytest
from conftest import BUS, MockNode

from exec_quality_analyzer.order_submitter import OrderSubmitter
from exec_quality_analyzer.execution_listener import ExecutionListener
from exec_quality_analyzer.metrics_engine import MetricsEngine
from exec_quality_analyzer.aggregator import Aggregator
from exec_quality_analyzer.reporter import Reporter


def build(cls, name, params):
    n = cls.__new__(cls)
    MockNode.__init__(n, name)
    for k, v in params.items():
        n.set_param(k, v)
    cls.__init__(n)
    return n


def test_full_sim_pipeline(tmp_path):
    csv_path = str(tmp_path / 'metrics.csv')

    metrics = build(MetricsEngine, 'metrics_engine', {})
    listeners = [
        build(ExecutionListener, f'{v}_listener',
              dict(venue=v, mode='sim', sim_latency_ms_mean=5.0,
                   sim_latency_ms_std=1.0, sim_reject_prob=rej))
        for v, rej in [('alpaca', 0.0), ('binance_testnet', 1.0)]
    ]  # binance rejects EVERYTHING -> fill rate must reflect that
    agg = build(Aggregator, 'aggregator', {})
    rep = build(Reporter, 'reporter', dict(csv_path=csv_path))
    sub = build(OrderSubmitter, 'order_submitter',
                dict(venues=['alpaca', 'binance_testnet'],
                     submit_period_sec=0.01, mode='sim'))

    for _ in range(40):
        sub.submit_next()
    agg.publish_reports()
    rep.print_table()
    rep.destroy_node()

    rows = list(csv.DictReader(open(csv_path)))
    assert len(rows) == 40
    alp = [r for r in rows if r['venue'] == 'alpaca']
    bnc = [r for r in rows if r['venue'] == 'binance_testnet']
    assert all(r['filled'] == '1' for r in alp)          # 0% reject
    assert all(r['filled'] == '0' for r in bnc)          # 100% reject
    assert all(r['status'] == 'rejected' for r in bnc)   # never counted filled
    assert all(float(r['latency_ms']) >= 0 for r in rows)
    # CSV has the new correctness columns
    assert {'status', 'fill_ratio'} <= set(rows[0].keys())


def test_invalid_submitter_config_rejected():
    with pytest.raises(ValueError):
        build(OrderSubmitter, 'order_submitter',
              dict(venues=[], mode='sim'))
    with pytest.raises(ValueError):
        build(OrderSubmitter, 'order_submitter',
              dict(order_quantity=-1.0, mode='sim'))


def test_quote_failure_publishes_failed_result():
    """Submitter in sandbox mode with a quote-less adapter must NOT submit,
    and must publish a quote_failed result that metrics records as unfilled."""
    from exec_quality_analyzer.adapters import (
        VenueAdapter, VenueConfig, QuoteUnavailableError)

    class NoQuoteAdapter:
        def normalized(self, s): return s
        def get_quote(self, s, side):
            raise QuoteUnavailableError('no market data')

    metrics = build(MetricsEngine, 'metrics_engine', {})
    captured = []
    BUS.subscribe('/metrics/per_order', captured.append)

    sub = OrderSubmitter.__new__(OrderSubmitter)
    MockNode.__init__(sub, 'order_submitter')
    sub.set_param('venues', ['alpaca'])
    sub.set_param('mode', 'sandbox')
    OrderSubmitter.__init__(sub, adapters={'alpaca': NoQuoteAdapter()})

    sub.submit_next()
    assert len(captured) == 1
    m = captured[0]
    assert m.status == 'quote_failed' and not m.filled
