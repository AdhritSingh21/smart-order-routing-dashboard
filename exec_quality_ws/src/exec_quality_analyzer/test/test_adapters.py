import time
import pytest
from conftest import _Logger

from exec_quality_analyzer.adapters import (
    VenueAdapter, VenueConfig, AdapterConfigError, SandboxActivationError,
    QuoteUnavailableError, normalize_symbol,
)


class FakeExchange:
    """Mock ccxt exchange — no network ever."""
    def __init__(self, has_test_url=True, sandbox_switches=True,
                 order_book=None, ticker=None, order_result=None,
                 raise_on_order=None):
        self.urls = {'api': 'https://prod.example'}
        if has_test_url:
            self.urls['test'] = 'https://test.example'
        self._switches = sandbox_switches
        self._ob = order_book
        self._ticker = ticker or {}
        self._order_result = order_result or {}
        self._raise_on_order = raise_on_order
        self.orders_submitted = []

    def set_sandbox_mode(self, flag):
        if self._switches and flag:
            self.urls = dict(self.urls, api=self.urls['test'])

    def fetch_order_book(self, symbol, limit=1):
        if self._ob is None:
            raise RuntimeError('order book unavailable')
        return self._ob

    def fetch_ticker(self, symbol):
        return self._ticker

    def create_order(self, symbol, type, side, amount):
        if self._raise_on_order:
            raise self._raise_on_order
        self.orders_submitted.append((symbol, type, side, amount))
        return self._order_result


def cfg(**kw):
    base = dict(venue='testvenue', mode='sandbox', ccxt_exchange_id='binance',
                dry_run=True, quote_retries=0, quote_retry_backoff_sec=0.0)
    base.update(kw)
    return VenueConfig(**base)


# ---------------- configuration validation ----------------
def test_invalid_mode_rejected():
    with pytest.raises(AdapterConfigError):
        VenueConfig(venue='v', mode='yolo').validate()

def test_production_requires_explicit_flag():
    with pytest.raises(AdapterConfigError):
        VenueConfig(venue='v', mode='production',
                    ccxt_exchange_id='binance').validate()

def test_production_allowed_with_flag():
    VenueConfig(venue='v', mode='production', ccxt_exchange_id='binance',
                allow_production_trading=True).validate()

def test_missing_venue_rejected():
    with pytest.raises(AdapterConfigError):
        VenueConfig(venue='', mode='sim').validate()

def test_negative_timeout_rejected():
    with pytest.raises(AdapterConfigError):
        VenueConfig(venue='v', mode='sim', quote_timeout_sec=-1).validate()


# ---------------- sandbox fail-safe ----------------
def test_sandbox_activation_confirmed():
    ex = FakeExchange()
    a = VenueAdapter(cfg(), client_factory=lambda c: ex, logger=_Logger('t'))
    assert a.exchange.urls['api'] == 'https://test.example'

def test_sandbox_no_test_endpoint_refuses_start():
    ex = FakeExchange(has_test_url=False)
    with pytest.raises(SandboxActivationError):
        VenueAdapter(cfg(), client_factory=lambda c: ex, logger=_Logger('t'))

def test_sandbox_url_does_not_switch_refuses_start():
    ex = FakeExchange(sandbox_switches=False)
    with pytest.raises(SandboxActivationError):
        VenueAdapter(cfg(), client_factory=lambda c: ex, logger=_Logger('t'))

def test_no_set_sandbox_mode_refuses_start():
    class NoSandbox:
        urls = {'api': 'p', 'test': 't'}
    with pytest.raises(SandboxActivationError):
        VenueAdapter(cfg(), client_factory=lambda c: NoSandbox(),
                     logger=_Logger('t'))


# ---------------- quotes ----------------
def quoted_adapter(**fk):
    ex = FakeExchange(**fk)
    return VenueAdapter(cfg(), client_factory=lambda c: ex, logger=_Logger('t')), ex

def test_buy_uses_best_ask():
    a, _ = quoted_adapter(order_book={'bids': [[99.0, 1]], 'asks': [[101.0, 1]]})
    q = a.get_quote('BTC/USDT', 'buy')
    assert q.price == 101.0 and q.source == 'ask'

def test_sell_uses_best_bid():
    a, _ = quoted_adapter(order_book={'bids': [[99.0, 1]], 'asks': [[101.0, 1]]})
    q = a.get_quote('BTC/USDT', 'sell')
    assert q.price == 99.0 and q.source == 'bid'

def test_fallback_to_last_with_warning():
    a, _ = quoted_adapter(order_book={'bids': [], 'asks': []},
                          ticker={'last': 100.5})
    q = a.get_quote('BTC/USDT', 'buy')
    assert q.price == 100.5 and q.source == 'last'

def test_no_quote_raises():
    a, _ = quoted_adapter(order_book=None, ticker={})
    with pytest.raises(QuoteUnavailableError):
        a.get_quote('BTC/USDT', 'buy')

def test_quote_has_timestamp_and_venue():
    a, _ = quoted_adapter(order_book={'bids': [[99, 1]], 'asks': [[101, 1]]})
    q = a.get_quote('BTC/USDT', 'buy')
    assert abs(q.quote_time - time.time()) < 2.0
    assert q.venue == 'testvenue'


# ---------------- order result parsing (Fix 3) ----------------
def test_filled_none_treated_as_zero_not_full():
    r = VenueAdapter.parse_order_result({'status': 'open'}, requested_qty=1.0)
    assert r['filled_quantity'] == 0.0 and r['status'] == 'open'

def test_filled_zero_is_real_zero():
    r = VenueAdapter.parse_order_result(
        {'filled': 0, 'status': 'open'}, requested_qty=1.0)
    assert r['filled_quantity'] == 0.0 and r['status'] == 'open'

def test_partial_fill():
    r = VenueAdapter.parse_order_result(
        {'filled': 0.4, 'average': 100.0, 'status': 'open'}, requested_qty=1.0)
    assert r['status'] == 'partial' and r['filled_quantity'] == 0.4

def test_complete_fill():
    r = VenueAdapter.parse_order_result(
        {'filled': 1.0, 'average': 100.0, 'status': 'closed'}, requested_qty=1.0)
    assert r['status'] == 'filled' and r['fill_price'] == 100.0

def test_canceled_with_partial_is_terminal_canceled_partial():
    # A canceled order with fills is TERMINAL — it must never degrade to a
    # non-terminal generic 'partial' that waits for fills that cannot come.
    r = VenueAdapter.parse_order_result(
        {'filled': 0.3, 'average': 100.0, 'status': 'canceled'}, requested_qty=1.0)
    assert r['status'] == 'canceled_partial'
    assert r['filled_quantity'] == 0.3 and r['fill_price'] == 100.0

def test_canceled_zero_fill():
    r = VenueAdapter.parse_order_result(
        {'filled': 0, 'status': 'canceled'}, requested_qty=1.0)
    assert r['status'] == 'canceled'


# ---------------- dry run + submission safety ----------------
def test_dry_run_never_submits():
    a, ex = quoted_adapter(order_book={'bids': [[99, 1]], 'asks': [[101, 1]]})
    r = a.submit_market_order('BTC/USDT', 'buy', 1.0)
    assert r['status'] == 'dry_run' and ex.orders_submitted == []

def test_order_submission_not_retried_on_error():
    ex = FakeExchange(raise_on_order=RuntimeError('boom'))
    a = VenueAdapter(cfg(dry_run=False), client_factory=lambda c: ex,
                     logger=_Logger('t'))
    r = a.submit_market_order('BTC/USDT', 'buy', 1.0)
    assert r['status'] == 'submit_failed' and 'boom' in r['error_reason']
    assert ex.orders_submitted == []  # exactly zero — no retry duplicates


# ---------------- symbol normalization ----------------
def test_alpaca_usdt_to_usd():
    assert normalize_symbol('alpaca', 'alpaca', 'BTC/USDT') == 'BTC/USD'

def test_coinbase_usdt_to_usd():
    assert normalize_symbol('coinbase_sandbox', 'coinbase', 'btc/usdt') == 'BTC/USD'

def test_binance_keeps_usdt():
    assert normalize_symbol('binance_testnet', 'binance', 'BTC/USDT') == 'BTC/USDT'

def test_override_wins():
    assert normalize_symbol('x', 'y', 'BTC/USDT', override='XBT-USD') == 'XBT-USD'

def test_bad_symbol_rejected():
    with pytest.raises(AdapterConfigError):
        normalize_symbol('v', 'binance', 'BTCUSDT')
