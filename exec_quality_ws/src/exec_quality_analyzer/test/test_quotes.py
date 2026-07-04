"""Stale-quote detection: exchange-provided timestamps, ms conversion,
local fallback, future/invalid rejection, and no-submit-without-quote."""
import time

import pytest
from conftest import BUS, MockNode, _Logger

from exec_quality_analyzer.adapters import (
    VenueAdapter, VenueConfig, QuoteUnavailableError,
    parse_exchange_timestamp,
    TS_SOURCE_ORDER_BOOK, TS_SOURCE_TICKER, TS_SOURCE_LOCAL,
)


class FakeQuoteExchange:
    """Mock ccxt exchange for quote tests — no network ever."""
    def __init__(self, order_book=None, ticker=None):
        self.urls = {'api': 'https://prod.example',
                     'test': 'https://test.example'}
        self._ob = order_book
        self._ticker = ticker or {}
        self.orders_submitted = []

    def set_sandbox_mode(self, flag):
        if flag:
            self.urls = dict(self.urls, api=self.urls['test'])

    def fetch_order_book(self, symbol, limit=1):
        if self._ob is None:
            raise RuntimeError('order book unavailable')
        return self._ob

    def fetch_ticker(self, symbol):
        return self._ticker

    def create_order(self, symbol, type, side, amount):
        self.orders_submitted.append((symbol, type, side, amount))
        return {'id': 'x', 'status': 'closed', 'filled': amount,
                'average': 100.0}


def cfg(**kw):
    base = dict(venue='testvenue', mode='sandbox', ccxt_exchange_id='binance',
                dry_run=True, quote_retries=0, quote_retry_backoff_sec=0.0,
                quote_max_age_sec=2.0)
    base.update(kw)
    return VenueConfig(**base)


def adapter(**fk):
    ex = FakeQuoteExchange(**fk)
    return VenueAdapter(cfg(), client_factory=lambda c: ex,
                        logger=_Logger('t')), ex


def ms(epoch_sec):
    return epoch_sec * 1000.0


# 1. Fresh order-book timestamp accepted, full quote record populated.
def test_fresh_order_book_timestamp():
    now = time.time()
    a, _ = adapter(order_book={'bids': [[99.0, 1]], 'asks': [[101.0, 1]],
                               'timestamp': ms(now - 0.5)})
    q = a.get_quote('BTC/USDT', 'buy')
    assert q.price == 101.0 and q.source == 'ask'
    assert q.bid == 99.0 and q.ask == 101.0
    assert q.timestamp_source == TS_SOURCE_ORDER_BOOK
    assert q.quote_time == pytest.approx(now - 0.5, abs=0.2)
    assert 0.0 <= q.age_sec < 2.0
    assert q.venue == 'testvenue' and q.symbol == 'BTC/USDT'


# 2. Stale order-book timestamp rejected with a logged reason.
def test_stale_order_book_timestamp_rejected():
    log = _Logger('t')
    ex = FakeQuoteExchange(order_book={'bids': [[99.0, 1]], 'asks': [[101.0, 1]],
                                       'timestamp': ms(time.time() - 300)})
    a = VenueAdapter(cfg(), client_factory=lambda c: ex, logger=log)
    with pytest.raises(QuoteUnavailableError, match='stale'):
        a.get_quote('BTC/USDT', 'buy')
    assert any('stale' in r[1].lower() for r in log.records)


# 3. Millisecond CCXT timestamps converted to seconds.
def test_millisecond_timestamp_converted_to_seconds():
    now = time.time()
    a, _ = adapter(order_book={'bids': [[99.0, 1]], 'asks': [[101.0, 1]],
                               'timestamp': ms(now)})
    q = a.get_quote('BTC/USDT', 'buy')
    # If ms were treated as seconds, quote_time would be ~year 56000.
    assert abs(q.quote_time - now) < 2.0


def test_parse_exchange_timestamp_units():
    now = 1750000000.0
    # milliseconds in -> seconds out
    assert parse_exchange_timestamp(ms(now - 1), now, 5.0) == \
        pytest.approx(now - 1)
    # plain seconds also accepted (some exchange metadata)
    assert parse_exchange_timestamp(now - 1, now, 5.0) == pytest.approx(now - 1)
    # absent -> None (caller falls back to local receipt time)
    assert parse_exchange_timestamp(None, now, 5.0) is None


# 4. Missing exchange timestamp -> local receipt time, recorded as such.
def test_missing_timestamp_local_fallback():
    a, _ = adapter(order_book={'bids': [[99.0, 1]], 'asks': [[101.0, 1]]})
    q = a.get_quote('BTC/USDT', 'buy')
    assert q.timestamp_source == TS_SOURCE_LOCAL
    assert abs(q.quote_time - time.time()) < 2.0
    assert q.age_sec == pytest.approx(0.0, abs=0.5)


# 5. Future timestamp rejected.
def test_future_timestamp_rejected():
    a, _ = adapter(order_book={'bids': [[99.0, 1]], 'asks': [[101.0, 1]],
                               'timestamp': ms(time.time() + 3600)})
    with pytest.raises(QuoteUnavailableError, match='future'):
        a.get_quote('BTC/USDT', 'buy')


# 6. Invalid timestamps rejected: non-numeric, zero, negative, bool, NaN.
@pytest.mark.parametrize('bad', ['garbage', 0, -5, True, float('nan'),
                                 float('inf'), 12345.0])
def test_invalid_timestamp_rejected(bad):
    # 12345.0 is implausible (1970) — neither valid seconds nor milliseconds.
    a, _ = adapter(order_book={'bids': [[99.0, 1]], 'asks': [[101.0, 1]],
                               'timestamp': bad})
    with pytest.raises(QuoteUnavailableError):
        a.get_quote('BTC/USDT', 'buy')


# 7. Stale ticker fallback rejected.
def test_stale_ticker_fallback_rejected():
    a, _ = adapter(order_book={'bids': [], 'asks': []},
                   ticker={'last': 100.5, 'timestamp': ms(time.time() - 600)})
    with pytest.raises(QuoteUnavailableError, match='stale'):
        a.get_quote('BTC/USDT', 'buy')


# 8. Valid (fresh) ticker fallback accepted and labeled.
def test_valid_ticker_fallback():
    log = _Logger('t')
    ex = FakeQuoteExchange(order_book={'bids': [], 'asks': []},
                           ticker={'last': 100.5,
                                   'timestamp': ms(time.time() - 0.2)})
    a = VenueAdapter(cfg(), client_factory=lambda c: ex, logger=log)
    q = a.get_quote('BTC/USDT', 'buy')
    assert q.price == 100.5 and q.source == 'last'
    assert q.timestamp_source == TS_SOURCE_TICKER
    assert any('falling back to last price' in r[1] for r in log.records)


def test_ticker_fallback_without_timestamp_uses_local():
    a, _ = adapter(order_book={'bids': [], 'asks': []},
                   ticker={'last': 100.5})
    q = a.get_quote('BTC/USDT', 'sell')
    assert q.source == 'last' and q.timestamp_source == TS_SOURCE_LOCAL


# 9. A rejected quote means NO order is submitted.
def test_quote_rejected_blocks_order_submission():
    from exec_quality_analyzer.order_submitter import OrderSubmitter

    ex = FakeQuoteExchange(order_book={'bids': [[99.0, 1]], 'asks': [[101.0, 1]],
                                       'timestamp': ms(time.time() - 300)})
    stale_adapter = VenueAdapter(cfg(venue='binance_testnet'),
                                 client_factory=lambda c: ex,
                                 logger=_Logger('t'))
    orders, fills = [], []
    BUS.subscribe('/orders/submit', orders.append)
    BUS.subscribe('/orders/fills', fills.append)

    sub = OrderSubmitter.__new__(OrderSubmitter)
    MockNode.__init__(sub, 'order_submitter')
    sub.set_param('venues', ['binance_testnet'])
    sub.set_param('mode', 'sandbox')
    OrderSubmitter.__init__(sub, adapters={'binance_testnet': stale_adapter})

    sub.submit_next()
    assert ex.orders_submitted == []                # nothing reached the venue
    assert len(orders) == 1 and orders[0].reference_source == 'none'
    assert len(fills) == 1 and fills[0].status == 'quote_failed'
    assert 'stale' in fills[0].error_reason
