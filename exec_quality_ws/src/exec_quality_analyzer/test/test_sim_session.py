"""Controlled simulator session over the mock bus: the full pipeline runs
for a short period and every runtime guarantee is checked — orders
generated, fills AND rejections processed, venue reports published, CSV
created, clean shutdown, and zero exchange/network activity in sim mode."""
import csv
import sys

from conftest import BUS, MockNode

from exec_quality_analyzer.order_submitter import OrderSubmitter
from exec_quality_analyzer.execution_listener import ExecutionListener
from exec_quality_analyzer.metrics_engine import MetricsEngine
from exec_quality_analyzer.aggregator import Aggregator
from exec_quality_analyzer.reporter import Reporter


def build(cls, name, params, **kw):
    n = cls.__new__(cls)
    MockNode.__init__(n, name)
    for k, v in params.items():
        n.set_param(k, v)
    cls.__init__(n, **kw)
    return n


def test_controlled_sim_session(tmp_path):
    csv_path = str(tmp_path / 'metrics.csv')

    metrics = build(MetricsEngine, 'metrics_engine', {})
    listeners = [
        build(ExecutionListener, f'{v}_listener',
              dict(venue=v, mode='sim', sim_latency_ms_mean=5.0,
                   sim_latency_ms_std=1.0, sim_reject_prob=rej))
        for v, rej in [('alpaca', 0.0), ('binance_testnet', 0.5),
                       ('coinbase_sandbox', 0.1)]
    ]
    agg = build(Aggregator, 'aggregator', {})
    rep = build(Reporter, 'reporter', dict(csv_path=csv_path))
    sub = build(OrderSubmitter, 'order_submitter',
                dict(venues=['alpaca', 'binance_testnet', 'coinbase_sandbox'],
                     submit_period_sec=0.01, mode='sim'))

    reports = []
    BUS.subscribe('/reports/venue', reports.append)

    # ~60 submission timer ticks, with periodic sweep/report/table timers.
    for i in range(60):
        sub.submit_next()
        if i % 10 == 9:
            metrics.sweep()
            agg.publish_reports()
            rep.print_table()

    # Orders were generated and fills AND rejections were processed.
    rows = list(csv.DictReader(open(csv_path)))
    assert len(rows) == 60
    statuses = {r['status'] for r in rows}
    assert 'filled' in statuses and 'rejected' in statuses
    by_venue = {v: [r for r in rows if r['venue'] == v]
                for v in ('alpaca', 'binance_testnet', 'coinbase_sandbox')}
    assert all(len(v) == 20 for v in by_venue.values())
    assert all(r['status'] == 'filled' for r in by_venue['alpaca'])
    # Every row labels its quote/execution sources and is comparable in sim.
    assert all(r['quote_mode'] == 'sim' and r['execution_mode'] == 'sim'
               and r['comparable'] == '1' for r in rows)
    assert all(float(r['latency_ms']) >= 0.0 for r in rows)

    # Venue reports were published for every venue.
    assert {r.venue for r in reports} == set(by_venue)
    binance = [r for r in reports if r.venue == 'binance_testnet'][-1]
    assert binance.fill_rate < 1.0          # rejections lower the fill rate

    # Nothing is left pending and no fills were lost.
    assert metrics.pending == {} and metrics.unmatched_fills == {}

    # Clean shutdown: pollers stopped (none exist in sim), CSV closed.
    for node in (*listeners, sub, metrics, agg):
        getattr(node, 'destroy_node', lambda: None)()
    rep.destroy_node()
    assert rep.csv_file.closed

    # No exchange network activity in sim mode: no adapters were built and
    # ccxt was never even imported.
    assert sub.adapters == {}
    assert all(l.adapter is None for l in listeners)
    assert 'ccxt' not in sys.modules


def test_csv_schema_mismatch_rotates_old_file(tmp_path):
    csv_path = tmp_path / 'metrics.csv'
    csv_path.write_text('old,header,columns\n1,2,3\n')
    rep = build(Reporter, 'reporter', dict(csv_path=str(csv_path)))
    rep.destroy_node()
    backups = list(tmp_path.glob('metrics.csv.*.bak'))
    assert len(backups) == 1
    assert 'old,header,columns' in backups[0].read_text()
    header = csv_path.read_text().splitlines()[0]
    assert header.startswith('timestamp,order_id,venue')
    assert any('rotated' in r[1] for r in rep.get_logger().records)
