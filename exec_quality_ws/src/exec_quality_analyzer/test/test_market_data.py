"""Coinbase-style market-data/execution separation: public quote client vs
sandbox execution client, no silent production fallback, ranking exclusion."""
import time

import pytest
from conftest import BUS, MockNode, OrderMsg, FillMsg, VenueReport, _Logger

from exec_quality_analyzer.adapters import (
    VenueAdapter, VenueConfig, AdapterConfigError, SandboxActivationError,
    QuoteUnavailableError, PublicMarketDataClient,
)


class ExecFake:
    """Sandbox execution client: orders only, NO market data (like the
    Coinbase Advanced Trade sandbox)."""
    def __init__(self, has_test_url=True):
        self.urls = {'api': 'https://prod.example'}
        if has_test_url:
            self.urls['test'] = 'https://test.example'
        self.create_calls = 0
        self.book_calls = 0

    def set_sandbox_mode(self, flag):
        if flag and 'test' in self.urls:
            self.urls = dict(self.urls, api=self.urls['test'])

    def fetch_order_book(self, symbol, limit=1):
        self.book_calls += 1
        raise RuntimeError('sandbox serves no market data')

    def fetch_ticker(self, symbol):
        return {}

    def create_order(self, symbol, type, side, amount):
        self.create_calls += 1
        return {'id': 'S1', 'status': 'closed', 'filled': amount,
                'average': 100.0}


class PublicFake:
    """Public market-data client: fresh book, no credentials."""
    def __init__(self):
        self.book_calls = 0
        self.urls = {'api': 'https://public.example'}

    def fetch_order_book(self, symbol, limit=1):
        self.book_calls += 1
        return {'bids': [[99.0, 1]], 'asks': [[101.0, 1]],
                'timestamp': time.time() * 1000}

    def fetch_ticker(self, symbol):
        return {}

    def create_order(self, *a, **k):
        raise AssertionError('public client must never be asked to trade')


def cfg(**kw):
    base = dict(venue='coinbase_sandbox', mode='sandbox',
                ccxt_exchange_id='coinbase', dry_run=False,
                quote_retries=0, quote_retry_backoff_sec=0.0)
    base.update(kw)
    return VenueConfig(**base)


# 1. Quotes come from the public client, orders go to the sandbox client.
def test_separate_quote_and_execution_clients():
    exec_fake, public_fake = ExecFake(), PublicFake()
    a = VenueAdapter(cfg(market_data_mode='public'),
                     client_factory=lambda c: exec_fake,
                     market_data_client_factory=lambda c: public_fake,
                     logger=_Logger('t'))
    q = a.get_quote('BTC/USDT', 'buy')
    assert q.price == 101.0
    assert public_fake.book_calls == 1 and exec_fake.book_calls == 0

    r = a.submit_market_order('BTC/USDT', 'buy', 1.0)
    assert r['status'] == 'filled'
    assert exec_fake.create_calls == 1
    # sandbox was positively activated on the EXECUTION client
    assert exec_fake.urls['api'] == 'https://test.example'


# 2. Public quote access cannot submit an order (or read credentials).
def test_public_client_cannot_submit():
    guard = PublicMarketDataClient(PublicFake())
    with pytest.raises(PermissionError):
        guard.create_order('BTC/USD', 'market', 'buy', 1.0)
    with pytest.raises(PermissionError):
        guard.cancel_order('x')
    with pytest.raises(PermissionError):
        _ = guard.apiKey
    with pytest.raises(PermissionError):
        guard.apiKey = 'nope'
    # market data still works
    assert guard.fetch_order_book('BTC/USD')['asks'][0][0] == 101.0


def test_adapter_public_client_is_guarded():
    a = VenueAdapter(cfg(market_data_mode='public'),
                     client_factory=lambda c: ExecFake(),
                     market_data_client_factory=lambda c: PublicFake(),
                     logger=_Logger('t'))
    with pytest.raises(PermissionError):
        a.market_data_client.create_order('BTC/USD', 'market', 'buy', 1.0)


# 3. The sandbox execution client can never silently fall back to production.
def test_sandbox_execution_cannot_silently_use_production():
    with pytest.raises(SandboxActivationError):
        VenueAdapter(cfg(market_data_mode='public'),
                     client_factory=lambda c: ExecFake(has_test_url=False),
                     market_data_client_factory=lambda c: PublicFake(),
                     logger=_Logger('t'))


# 4. Coinbase sandbox without market data fails clearly, pointing at the fix.
def test_sandbox_without_market_data_fails_with_guidance():
    a = VenueAdapter(cfg(market_data_mode='venue'),
                     client_factory=lambda c: ExecFake(),
                     logger=_Logger('t'))
    with pytest.raises(QuoteUnavailableError,
                       match="market_data_mode: 'public'"):
        a.get_quote('BTC/USDT', 'buy')


def test_sandbox_with_public_market_data_quotes_fine():
    a = VenueAdapter(cfg(market_data_mode='public'),
                     client_factory=lambda c: ExecFake(),
                     market_data_client_factory=lambda c: PublicFake(),
                     logger=_Logger('t'))
    q = a.get_quote('BTC/USDT', 'sell')
    assert q.price == 99.0 and q.source == 'bid'


# 5. Synthetic sandbox execution is excluded from ranking by default.
def test_synthetic_sandbox_excluded_from_ranking_by_default():
    assert cfg(market_data_mode='public').ranking_comparable is False
    assert cfg(market_data_mode='venue').ranking_comparable is True
    assert cfg(synthetic_execution=True).ranking_comparable is False
    # explicit override wins (documented, deliberate)
    assert cfg(market_data_mode='public',
               include_in_ranking=True).ranking_comparable is True
    assert VenueConfig(venue='s', mode='sim').ranking_comparable is True


def test_reporter_excludes_non_comparable_venue():
    from exec_quality_analyzer.reporter import Reporter
    rep = Reporter.__new__(Reporter)
    MockNode.__init__(rep, 'reporter')
    import os, tempfile
    rep.set_param('csv_path', os.path.join(tempfile.mkdtemp(), 'm.csv'))
    Reporter.__init__(rep)

    good, synth = VenueReport(), VenueReport()
    good.venue, good.fill_rate, good.comparable = 'binance_testnet', 0.99, True
    synth.venue, synth.fill_rate, synth.comparable = 'coinbase_sandbox', 1.0, False
    rep.on_report(good)
    rep.on_report(synth)
    rep.print_table()
    table = '\n'.join(m for lvl, m in rep.get_logger().records if 'RANK' in m)
    assert 'EXCLUDED from ranking' in table
    # the synthetic venue appears only as excluded, never with a rank number
    ranked_lines = [ln for ln in table.splitlines()
                    if ln.strip().startswith('1')]
    assert ranked_lines and 'binance_testnet' in ranked_lines[0]
    assert all('coinbase_sandbox' not in ln for ln in ranked_lines)
    rep.destroy_node()


# 6. Metrics record quote source and execution source.
def test_metrics_record_quote_and_execution_modes():
    from exec_quality_analyzer.metrics_engine import MetricsEngine
    e = MetricsEngine.__new__(MetricsEngine)
    MockNode.__init__(e, 'metrics_engine')
    MetricsEngine.__init__(e)
    captured = []
    BUS.subscribe('/metrics/per_order', captured.append)

    o = OrderMsg()
    o.order_id, o.venue, o.symbol, o.side = 'o1', 'coinbase_sandbox', 'BTC/USD', 'buy'
    o.quantity, o.quoted_price = 1.0, 100.0
    o.reference_source = 'ask'
    o.submit_time = o.quote_time = time.time()
    o.quote_mode, o.execution_mode, o.comparable = 'public', 'sandbox', False
    e.on_order(o)

    f = FillMsg()
    f.order_id, f.venue, f.symbol = 'o1', 'coinbase_sandbox', 'BTC/USD'
    f.status, f.fill_price, f.filled_quantity = 'filled', 100.0, 1.0
    f.fill_time = time.time()
    e.on_fill(f)

    assert len(captured) == 1
    m = captured[0]
    assert m.quote_mode == 'public' and m.execution_mode == 'sandbox'
    assert m.comparable is False


# A quote-only adapter physically cannot submit orders.
def test_quote_only_adapter_cannot_submit():
    a = VenueAdapter(cfg(quote_only=True, dry_run=True),
                     client_factory=lambda c: ExecFake(),
                     logger=_Logger('t'))
    assert a.execution_client is None
    with pytest.raises(AdapterConfigError, match='quote-only'):
        a.submit_market_order('BTC/USDT', 'buy', 1.0)
