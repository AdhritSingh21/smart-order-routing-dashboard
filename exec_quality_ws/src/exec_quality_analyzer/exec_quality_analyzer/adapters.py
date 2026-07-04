"""Venue adapters: all exchange-specific logic lives here, not in ROS nodes.

Safety model
------------
- mode is one of: "sim", "sandbox", "production". There is no implicit default
  to production anywhere. Default mode is "sim".
- "sandbox" covers paper/testnet/sandbox endpoints. Activation must be
  POSITIVELY confirmed (the exchange must expose a test endpoint and the API
  URL must actually switch). If confirmation fails, initialization raises —
  we never fall back to a production endpoint.
- "production" is refused unless allow_production_trading=True is explicitly
  set in configuration.
- dry_run=True performs quote retrieval and order construction but never
  submits. It is the default for non-sim modes.
- API secrets are read from environment variables and never logged. We only
  log whether they are present.

Market-data / execution separation
----------------------------------
Some sandboxes (notably Coinbase Advanced Trade) cannot serve a usable
real-time order book. A venue may therefore use TWO clients:

  - execution client : authenticated, sandbox-confirmed, submits orders.
  - market-data client: where reference quotes come from. Either the venue
    client itself (market_data_mode: "venue", the default) or a separate
    PUBLIC, UNAUTHENTICATED client (market_data_mode: "public").

The public client is wrapped in a read-only guard that physically cannot
submit orders, and it is created WITHOUT credentials. Conversely a
quote_only adapter has no execution client at all and refuses to submit.
Quotes and executions are labeled (quote_mode / execution_mode) so metrics
never silently mix production market data with sandbox executions. When
execution is sandbox but quotes are public production data, the venue's
results are flagged non-comparable and excluded from ranking by default
(see VenueConfig.ranking_comparable).

Quote freshness
---------------
Quote timestamps come from the exchange whenever provided (order book
'timestamp', else ticker 'timestamp'); CCXT reports milliseconds and is
converted to epoch seconds. Local receipt time is used ONLY when the
exchange provides no timestamp, and the source is recorded
(timestamp_source: "exchange_order_book" | "exchange_ticker" | "local").
Invalid timestamps (non-numeric, zero/negative, implausibly old, or in the
future beyond a small skew) reject the quote with a logged reason. Quotes
older than quote_max_age_sec are rejected; an order is never submitted
without a valid fresh reference quote.

Sign/semantics
--------------
- get_quote(side="buy")  -> best ask (what a buyer pays)
- get_quote(side="sell") -> best bid (what a seller receives)
- Fallback to ticker last price is allowed only when the needed side is
  unavailable; it is logged and tagged reference_source="last".
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

from exec_quality_analyzer.order_status import (
    OrderStatus, parse_ccxt_order,
)


class AdapterConfigError(ValueError):
    """Raised for invalid or unsafe venue configuration."""


class SandboxActivationError(RuntimeError):
    """Raised when sandbox mode is requested but cannot be confirmed."""


class QuoteUnavailableError(RuntimeError):
    """Raised when no valid, fresh reference quote can be obtained."""


class OrderStatusUnavailableError(RuntimeError):
    """Raised when an order's status cannot be fetched from the venue."""


# Timestamps below this (1990-01-01) are implausible for a live quote.
_MIN_PLAUSIBLE_EPOCH_SEC = 631152000.0
# Raw values at or above this are CCXT milliseconds, not seconds.
_MS_THRESHOLD = 1e11

TS_SOURCE_ORDER_BOOK = 'exchange_order_book'
TS_SOURCE_TICKER = 'exchange_ticker'
TS_SOURCE_LOCAL = 'local'


@dataclass
class Quote:
    price: float          # selected reference price
    source: str           # "ask" | "bid" | "last"
    bid: float
    ask: float
    quote_time: float     # wall-clock epoch seconds
    venue: str
    symbol: str
    timestamp_source: str = TS_SOURCE_LOCAL
    age_sec: float = 0.0  # receipt_time - quote_time at validation


def parse_exchange_timestamp(raw: Any, now: float,
                             max_future_skew_sec: float) -> Optional[float]:
    """Validate and convert an exchange-provided timestamp to epoch seconds.

    Returns None when the exchange simply did not provide one (caller falls
    back to local receipt time). Raises QuoteUnavailableError when a value
    WAS provided but is malformed: non-numeric, boolean, zero, negative,
    implausibly old, or further in the future than max_future_skew_sec.
    CCXT timestamps are milliseconds since epoch and are converted.
    """
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise QuoteUnavailableError(f'invalid exchange timestamp {raw!r}')
    try:
        ts = float(raw)
    except (TypeError, ValueError):
        raise QuoteUnavailableError(
            f'non-numeric exchange timestamp {raw!r}') from None
    if ts != ts or ts in (float('inf'), float('-inf')):
        raise QuoteUnavailableError(f'invalid exchange timestamp {raw!r}')
    if ts <= 0:
        raise QuoteUnavailableError(
            f'zero/negative exchange timestamp {raw!r}')
    if ts >= _MS_THRESHOLD:           # ccxt milliseconds -> seconds
        ts = ts / 1000.0
    if ts < _MIN_PLAUSIBLE_EPOCH_SEC:
        raise QuoteUnavailableError(
            f'implausible exchange timestamp {raw!r} '
            f'(neither valid seconds nor milliseconds)')
    if ts > now + max_future_skew_sec:
        raise QuoteUnavailableError(
            f'exchange timestamp {raw!r} is {ts - now:.2f}s in the future '
            f'(max allowed skew {max_future_skew_sec}s)')
    return ts


@dataclass
class VenueConfig:
    venue: str
    mode: str = 'sim'                      # sim | sandbox | production
    ccxt_exchange_id: str = ''
    symbol_override: str = ''              # venue-specific symbol if it differs
    dry_run: bool = True                   # default safe
    allow_production_trading: bool = False
    quote_timeout_sec: float = 5.0
    quote_max_age_sec: float = 2.0
    quote_retries: int = 2                 # retries for QUOTES only
    quote_retry_backoff_sec: float = 0.5
    max_future_timestamp_skew_sec: float = 5.0
    # --- market-data / execution separation ---
    market_data_mode: str = 'venue'        # venue | public
    market_data_exchange_id: str = ''      # defaults to ccxt_exchange_id
    quote_only: bool = False               # adapter serves quotes, never trades
    synthetic_execution: bool = False      # mark fills as not representative
    include_in_ranking: Optional[bool] = None  # None -> conservative default
    # --- post-submission polling ---
    poll_interval_sec: float = 1.0
    poll_max_duration_sec: float = 30.0
    poll_request_timeout_sec: float = 5.0
    poll_fetch_retries: int = 5            # consecutive status-fetch failures
    poll_fetch_backoff_sec: float = 0.5

    VALID_MODES = ('sim', 'sandbox', 'production')
    VALID_MARKET_DATA_MODES = ('venue', 'public')

    def validate(self) -> None:
        if not self.venue or not isinstance(self.venue, str):
            raise AdapterConfigError('venue name is required')
        if self.mode not in self.VALID_MODES:
            raise AdapterConfigError(
                f"venue '{self.venue}': mode must be one of {self.VALID_MODES}, "
                f"got '{self.mode}'")
        if self.mode == 'production' and not self.allow_production_trading:
            raise AdapterConfigError(
                f"venue '{self.venue}': production mode requires explicit "
                f"allow_production_trading: true in configuration")
        if self.mode != 'sim' and not self.ccxt_exchange_id:
            raise AdapterConfigError(
                f"venue '{self.venue}': ccxt_exchange_id is required for "
                f"mode '{self.mode}'")
        if self.market_data_mode not in self.VALID_MARKET_DATA_MODES:
            raise AdapterConfigError(
                f"venue '{self.venue}': market_data_mode must be one of "
                f"{self.VALID_MARKET_DATA_MODES}, got '{self.market_data_mode}'")
        if self.mode == 'sim' and self.market_data_mode != 'venue':
            raise AdapterConfigError(
                f"venue '{self.venue}': market_data_mode "
                f"'{self.market_data_mode}' is meaningless in sim mode")
        if self.mode == 'sim' and self.quote_only:
            raise AdapterConfigError(
                f"venue '{self.venue}': quote_only requires a non-sim mode")
        if self.quote_timeout_sec <= 0 or self.quote_max_age_sec <= 0:
            raise AdapterConfigError(
                f"venue '{self.venue}': quote timeouts must be positive")
        if self.quote_retries < 0:
            raise AdapterConfigError(
                f"venue '{self.venue}': quote_retries must be >= 0")
        if self.max_future_timestamp_skew_sec <= 0:
            raise AdapterConfigError(
                f"venue '{self.venue}': max_future_timestamp_skew_sec must "
                f"be positive")
        if (self.poll_interval_sec <= 0 or self.poll_max_duration_sec <= 0
                or self.poll_request_timeout_sec <= 0):
            raise AdapterConfigError(
                f"venue '{self.venue}': polling intervals/durations must be "
                f"positive")
        if self.poll_fetch_retries < 0 or self.poll_fetch_backoff_sec < 0:
            raise AdapterConfigError(
                f"venue '{self.venue}': polling retry settings must be >= 0")

    @property
    def ranking_comparable(self) -> bool:
        """Whether this venue's execution quality is comparable for ranking.

        Conservative defaults: explicitly marked synthetic execution, or a
        sandbox that needs public (production) market data for quotes, is
        NOT comparable — its fills are measured against prices from a
        different market. An explicit include_in_ranking always wins.
        """
        if self.include_in_ranking is not None:
            return bool(self.include_in_ranking)
        if self.synthetic_execution:
            return False
        if self.mode == 'sandbox' and self.market_data_mode == 'public':
            return False
        return True


# --------------------------------------------------------------------------
# Symbol normalization
# --------------------------------------------------------------------------
def normalize_symbol(venue: str, ccxt_exchange_id: str, symbol: str,
                     override: str = '') -> str:
    """Map a canonical 'BASE/QUOTE' symbol to the venue's convention.

    An explicit per-venue override always wins. Otherwise:
    - alpaca crypto uses USD quotes (BTC/USDT -> BTC/USD)
    - coinbase advanced trade primarily lists USD pairs (BTC/USDT -> BTC/USD)
    - binance keeps USDT pairs
    """
    if override:
        return override
    if '/' not in symbol:
        raise AdapterConfigError(f"symbol '{symbol}' must be 'BASE/QUOTE'")
    base, quote = symbol.upper().split('/', 1)
    ex = (ccxt_exchange_id or venue).lower()
    if 'alpaca' in ex or 'coinbase' in ex:
        if quote == 'USDT':
            quote = 'USD'
    return f'{base}/{quote}'


# --------------------------------------------------------------------------
# Public market-data guard
# --------------------------------------------------------------------------
class PublicMarketDataClient:
    """Read-only wrapper around an exchange client.

    Exposes ONLY public market-data methods. Any trading-capable attribute
    (create_order, cancel_order, withdraw, ...) raises PermissionError, so a
    public quote client can never submit an order — even by accident.
    """
    _ALLOWED = frozenset({
        'fetch_order_book', 'fetch_ticker', 'fetch_time', 'fetch_trades',
        'load_markets', 'urls', 'id', 'has', 'symbols', 'markets',
    })

    def __init__(self, client: Any):
        object.__setattr__(self, '_client', client)

    def __getattr__(self, name: str):
        if name in PublicMarketDataClient._ALLOWED:
            return getattr(object.__getattribute__(self, '_client'), name)
        raise PermissionError(
            f"public market-data client: '{name}' is not permitted "
            f"(quote access only, order submission is impossible here)")

    def __setattr__(self, name: str, value: Any):
        raise PermissionError('public market-data client is read-only')


# --------------------------------------------------------------------------
# Adapter
# --------------------------------------------------------------------------
class VenueAdapter:
    """One adapter per venue. Owns the exchange clients and all venue I/O.

    `client_factory` builds the (authenticated) venue client and
    `market_data_client_factory` builds the public quote client — both are
    injectable so tests can supply fakes.
    `logger` accepts any object with .info/.warning/.error (e.g. a ROS logger).
    """

    def __init__(self, config: VenueConfig,
                 client_factory: Optional[Callable[[VenueConfig], Any]] = None,
                 market_data_client_factory:
                     Optional[Callable[[VenueConfig], Any]] = None,
                 logger: Any = None):
        config.validate()
        self.config = config
        self.log = logger or _PrintLogger(config.venue)
        self.execution_client: Any = None
        self.market_data_client: Any = None
        self._market_symbol_cache: dict[str, str] = {}

        if config.mode == 'sim':
            return

        needs_venue_client = (not config.quote_only
                              or config.market_data_mode == 'venue')
        if needs_venue_client:
            factory = client_factory or self._default_client_factory
            venue_client = factory(config)
            if config.mode == 'sandbox':
                self._activate_sandbox_or_die(venue_client)
            if config.quote_only:
                # Quotes only: wrap so this client can never submit.
                self.market_data_client = PublicMarketDataClient(venue_client)
            else:
                self.execution_client = venue_client
                if config.market_data_mode == 'venue':
                    self.market_data_client = venue_client

        if config.market_data_mode == 'public':
            md_factory = (market_data_client_factory
                          or self._default_market_data_client_factory)
            self.market_data_client = PublicMarketDataClient(
                md_factory(config))

        self._log_startup()

    # Backwards-compatible alias: `exchange` is the execution client.
    @property
    def exchange(self):
        return self.execution_client

    # ------------------------------------------------------------- startup
    @staticmethod
    def _env_prefix(venue: str) -> str:
        return venue.upper().replace('-', '_')

    def _default_client_factory(self, cfg: VenueConfig):
        import ccxt
        prefix = self._env_prefix(cfg.venue)
        key_var, sec_var = f'{prefix}_API_KEY', f'{prefix}_API_SECRET'
        api_key = os.environ.get(key_var)
        secret = os.environ.get(sec_var)
        # Validate presence WITHOUT printing values
        missing = [v for v, val in ((key_var, api_key), (sec_var, secret)) if not val]
        if missing and not cfg.dry_run and not cfg.quote_only:
            raise AdapterConfigError(
                f"venue '{cfg.venue}': missing environment variables "
                f"{missing} (values are never logged)")
        if missing:
            self.log.warning(
                f"{cfg.venue}: env vars {missing} not set — OK for "
                f"dry_run/quote-only, order submission would fail")
        klass = getattr(ccxt, cfg.ccxt_exchange_id, None)
        if klass is None:
            raise AdapterConfigError(
                f"venue '{cfg.venue}': unknown ccxt exchange id "
                f"'{cfg.ccxt_exchange_id}'")
        return klass({
            'apiKey': api_key or '',
            'secret': secret or '',
            'enableRateLimit': True,
            'timeout': int(max(self.config.quote_timeout_sec,
                               self.config.poll_request_timeout_sec) * 1000),
        })

    def _default_market_data_client_factory(self, cfg: VenueConfig):
        """Public market-data client: NO credentials, public endpoints only."""
        import ccxt
        ex_id = cfg.market_data_exchange_id or cfg.ccxt_exchange_id
        klass = getattr(ccxt, ex_id, None)
        if klass is None:
            raise AdapterConfigError(
                f"venue '{cfg.venue}': unknown market-data ccxt exchange id "
                f"'{ex_id}'")
        # Deliberately no apiKey/secret: public data must never carry
        # credentials and can never enable trading.
        return klass({
            'enableRateLimit': True,
            'timeout': int(self.config.quote_timeout_sec * 1000),
        })

    def _activate_sandbox_or_die(self, ex: Any) -> None:
        """Positively confirm sandbox activation; never continue on production."""
        urls = getattr(ex, 'urls', {}) or {}
        prod_api = urls.get('api')
        if not hasattr(ex, 'set_sandbox_mode'):
            raise SandboxActivationError(
                f"venue '{self.config.venue}': exchange client has no "
                f"set_sandbox_mode(); cannot confirm sandbox — refusing to start")
        if 'test' not in urls or not urls.get('test'):
            raise SandboxActivationError(
                f"venue '{self.config.venue}': ccxt exchange "
                f"'{self.config.ccxt_exchange_id}' exposes no test endpoint; "
                f"cannot confirm sandbox — refusing to start")
        try:
            ex.set_sandbox_mode(True)
        except Exception as exc:
            raise SandboxActivationError(
                f"venue '{self.config.venue}': set_sandbox_mode failed: {exc}"
            ) from exc
        new_api = (getattr(ex, 'urls', {}) or {}).get('api')
        if new_api == prod_api:
            raise SandboxActivationError(
                f"venue '{self.config.venue}': API endpoint did not switch "
                f"after sandbox activation — refusing to run on production URL")

    def _log_startup(self) -> None:
        c = self.config
        endpoint_type = {'sandbox': 'TEST/SANDBOX', 'production': 'PRODUCTION'}[c.mode]
        submission = ('IMPOSSIBLE (quote-only)' if c.quote_only
                      else ('DISABLED (dry_run)' if c.dry_run else 'ENABLED'))
        self.log.info(
            f"adapter up | venue={c.venue} exchange={c.ccxt_exchange_id} "
            f"mode={c.mode} endpoint={endpoint_type} "
            f"market_data={c.market_data_mode} order_submission={submission}")
        if c.mode == 'sandbox' and c.market_data_mode == 'public':
            self.log.warning(
                f"{c.venue}: quotes come from PUBLIC production market data "
                f"while orders go to the SANDBOX — execution results may be "
                f"synthetic and are "
                f"{'INCLUDED' if c.ranking_comparable else 'EXCLUDED'} "
                f"in venue ranking (include_in_ranking="
                f"{c.include_in_ranking})")
        if c.synthetic_execution:
            self.log.warning(
                f"{c.venue}: synthetic_execution=true — fills are not "
                f"representative of real execution quality")

    # -------------------------------------------------------------- quotes
    def get_quote(self, symbol: str, side: str) -> Quote:
        """Best ask for buys, best bid for sells, from this venue's
        configured market-data source.

        Retries (with exponential backoff) apply to quotes only — never to
        order submission. Raises QuoteUnavailableError if no fresh quote.
        """
        if self.config.mode == 'sim':
            raise RuntimeError('get_quote is not used in sim mode')
        if side not in ('buy', 'sell'):
            raise ValueError(f"side must be 'buy' or 'sell', got '{side}'")
        if self.market_data_client is None:
            raise QuoteUnavailableError(
                f'{self.config.venue}: no market-data client configured')
        norm = self.normalized(symbol)
        last_err: Optional[Exception] = None
        for attempt in range(self.config.quote_retries + 1):
            try:
                quote = self._fetch_quote_once(norm, side)
                if quote.age_sec > self.config.quote_max_age_sec:
                    self.log.warning(
                        f'{self.config.venue}: REJECTED stale quote for '
                        f'{norm}: age {quote.age_sec:.2f}s > max '
                        f'{self.config.quote_max_age_sec}s '
                        f'(timestamp_source={quote.timestamp_source})')
                    raise QuoteUnavailableError(
                        f'stale quote ({quote.age_sec:.2f}s > '
                        f'{self.config.quote_max_age_sec}s, '
                        f'timestamp_source={quote.timestamp_source})')
                return quote
            except Exception as exc:  # retried with backoff below
                last_err = exc
                if attempt < self.config.quote_retries:
                    delay = self.config.quote_retry_backoff_sec * (2 ** attempt)
                    self.log.warning(
                        f'{self.config.venue}: quote attempt {attempt+1} failed '
                        f'({exc}); retrying in {delay:.1f}s')
                    time.sleep(delay)
        reason = (f'{self.config.venue}: no valid quote for {norm} after '
                  f'{self.config.quote_retries + 1} attempts: {last_err}')
        if (self.config.mode == 'sandbox'
                and self.config.market_data_mode == 'venue'):
            reason += (' — this sandbox may not serve market data '
                       '(e.g. Coinbase Advanced Trade sandbox); configure '
                       "market_data_mode: 'public' to quote from public "
                       'production market data instead')
        self.log.error(f'quote REJECTED: {reason}')
        raise QuoteUnavailableError(reason)

    def _fetch_quote_once(self, symbol: str, side: str) -> Quote:
        md = self.market_data_client
        receipt_time = time.time()
        bid = ask = 0.0
        ob_ts_raw = None
        ob_ok = False
        try:
            ob = md.fetch_order_book(symbol, limit=1)
            if isinstance(ob, dict):
                bids, asks = ob.get('bids') or [], ob.get('asks') or []
                bid = float(bids[0][0]) if bids else 0.0
                ask = float(asks[0][0]) if asks else 0.0
                ob_ts_raw = ob.get('timestamp')
                ob_ok = True
        except PermissionError:
            raise
        except Exception as exc:
            self.log.warning(f'{self.config.venue}: order book fetch failed: {exc}')

        if (side == 'buy' and ask > 0) or (side == 'sell' and bid > 0):
            # Exchange timestamp preferred; malformed values reject the quote.
            ts = parse_exchange_timestamp(
                ob_ts_raw, receipt_time,
                self.config.max_future_timestamp_skew_sec)
            if ts is None:
                ts, ts_source = receipt_time, TS_SOURCE_LOCAL
                self.log.info(
                    f'{self.config.venue}: order book for {symbol} has no '
                    f'exchange timestamp; using local receipt time')
            else:
                ts_source = TS_SOURCE_ORDER_BOOK
            price, source = (ask, 'ask') if side == 'buy' else (bid, 'bid')
            return Quote(price=price, source=source, bid=bid, ask=ask,
                         quote_time=ts, venue=self.config.venue,
                         symbol=symbol, timestamp_source=ts_source,
                         age_sec=max(0.0, receipt_time - ts))

        # Documented fallback: ticker last price, flagged with a warning.
        ticker = md.fetch_ticker(symbol)
        if not isinstance(ticker, dict):
            raise QuoteUnavailableError(
                f'malformed ticker response: {type(ticker).__name__}')
        last_raw = ticker.get('last')
        try:
            last = float(last_raw) if last_raw is not None else 0.0
        except (TypeError, ValueError):
            raise QuoteUnavailableError(
                f'non-numeric ticker last price {last_raw!r}') from None
        if last <= 0:
            raise QuoteUnavailableError(
                'no bid/ask and no valid last price '
                f'(order_book_fetched={ob_ok})')
        ts = parse_exchange_timestamp(
            ticker.get('timestamp'), receipt_time,
            self.config.max_future_timestamp_skew_sec)
        if ts is None:
            ts, ts_source = receipt_time, TS_SOURCE_LOCAL
        else:
            ts_source = TS_SOURCE_TICKER
        self.log.warning(
            f'{self.config.venue}: missing {"ask" if side == "buy" else "bid"} '
            f'for {symbol}; falling back to last price {last} '
            f'(reference_source=last, timestamp_source={ts_source})')
        return Quote(price=last, source='last', bid=bid, ask=ask,
                     quote_time=ts, venue=self.config.venue, symbol=symbol,
                     timestamp_source=ts_source,
                     age_sec=max(0.0, receipt_time - ts))

    # -------------------------------------------------------------- orders
    def submit_market_order(self, symbol: str, side: str,
                            quantity: float) -> dict:
        """Submit (or dry-run) a market order. NEVER retried internally —
        retrying market orders risks duplicates.

        Returns the normalized dict from parse_ccxt_order (plus a
        'fill_price' alias for 'average_price'). 'filled_quantity' is the
        CUMULATIVE quantity reported at submission time; non-terminal
        statuses must be resolved by polling (see fetch_order_status).
        """
        norm = self.normalized(symbol)
        if self.config.quote_only:
            raise AdapterConfigError(
                f'{self.config.venue}: quote-only adapter cannot submit orders')
        if self.config.dry_run:
            self.log.info(
                f'{self.config.venue}: DRY RUN — would submit market {side} '
                f'{quantity} {norm}; not sending')
            return _normalized_result(
                OrderStatus.DRY_RUN,
                error_reason='dry_run: order constructed but not submitted')
        if self.execution_client is None:
            raise AdapterConfigError(
                f'{self.config.venue}: no execution client configured')
        try:
            result = self.execution_client.create_order(
                symbol=norm, type='market', side=side, amount=quantity)
        except Exception as exc:
            # Submission failure is terminal and never retried here: we
            # cannot know whether the venue saw the order, and a retry
            # could duplicate a market order.
            return _normalized_result(
                OrderStatus.SUBMIT_FAILED,
                error_reason=f'{type(exc).__name__}: {exc}')
        return self.parse_order_result(result, quantity)

    @staticmethod
    def parse_order_result(result: Any, requested_qty: float) -> dict:
        """Delegates to the centralized CCXT parser (order_status module)."""
        parsed = parse_ccxt_order(result, requested_qty)
        parsed['fill_price'] = parsed['average_price']
        return parsed

    def fetch_order_status(self, exchange_order_id: str, symbol: str) -> dict:
        """Fetch the current state of a previously submitted order.

        Read-only: never creates or retries an order. Prefers fetch_order;
        falls back to fetch_open_order / fetch_closed_order where the
        exchange supports them. Raises OrderStatusUnavailableError when no
        usable method exists; network errors propagate so the poller can
        apply its limited retry/backoff.
        """
        ex = self.execution_client
        if ex is None:
            raise OrderStatusUnavailableError(
                f'{self.config.venue}: no execution client (quote-only or '
                f'sim adapter cannot poll orders)')
        if not exchange_order_id:
            raise OrderStatusUnavailableError(
                f'{self.config.venue}: order has no exchange order id')
        norm = self.normalized(symbol)
        has = getattr(ex, 'has', None) or {}

        fetch_order = getattr(ex, 'fetch_order', None)
        if callable(fetch_order) and has.get('fetchOrder', True):
            return fetch_order(exchange_order_id, norm)

        fallback_errors = []
        for name in ('fetch_open_order', 'fetch_closed_order'):
            method = getattr(ex, name, None)
            if not callable(method):
                continue
            try:
                result = method(exchange_order_id, norm)
                if result:
                    return result
            except Exception as exc:
                fallback_errors.append(f'{name}: {exc}')
        raise OrderStatusUnavailableError(
            f'{self.config.venue}: cannot fetch order {exchange_order_id}: '
            f'no supported fetch method succeeded ({fallback_errors})')

    # --------------------------------------------------------------- misc
    def normalized(self, symbol: str) -> str:
        if symbol not in self._market_symbol_cache:
            self._market_symbol_cache[symbol] = normalize_symbol(
                self.config.venue, self.config.ccxt_exchange_id,
                symbol, self.config.symbol_override)
        return self._market_symbol_cache[symbol]


def _normalized_result(status: str, error_reason: str = '') -> dict:
    """A parse_ccxt_order-shaped dict for locally generated outcomes."""
    return {
        'status': status, 'exchange_status': '',
        'filled_quantity': 0.0, 'average_price': 0.0, 'fill_price': 0.0,
        'exchange_order_id': '', 'client_order_id': '',
        'order_timestamp': 0.0, 'error_reason': error_reason, 'raw': None,
    }


class _PrintLogger:
    def __init__(self, name): self.name = name
    def info(self, m): print(f'[INFO] [{self.name}]: {m}')
    def warning(self, m): print(f'[WARN] [{self.name}]: {m}')
    def error(self, m): print(f'[ERROR] [{self.name}]: {m}')


def venue_config_from_params(get) -> VenueConfig:
    """Build a VenueConfig from a callable get(name, default) over ROS params."""
    include = get('include_in_ranking', '')
    if isinstance(include, str):
        include = {'true': True, 'false': False}.get(include.strip().lower())
    return VenueConfig(
        venue=get('venue', ''),
        mode=get('mode', 'sim'),
        ccxt_exchange_id=get('ccxt_exchange_id', ''),
        symbol_override=get('symbol_override', ''),
        dry_run=bool(get('dry_run', True)),
        allow_production_trading=bool(get('allow_production_trading', False)),
        quote_timeout_sec=float(get('quote_timeout_sec', 5.0)),
        quote_max_age_sec=float(get('quote_max_age_sec', 2.0)),
        quote_retries=int(get('quote_retries', 2)),
        quote_retry_backoff_sec=float(get('quote_retry_backoff_sec', 0.5)),
        max_future_timestamp_skew_sec=float(
            get('max_future_timestamp_skew_sec', 5.0)),
        market_data_mode=get('market_data_mode', 'venue'),
        market_data_exchange_id=get('market_data_exchange_id', ''),
        synthetic_execution=bool(get('synthetic_execution', False)),
        include_in_ranking=include,
        poll_interval_sec=float(get('poll_interval_sec', 1.0)),
        poll_max_duration_sec=float(get('poll_max_duration_sec', 30.0)),
        poll_request_timeout_sec=float(get('poll_request_timeout_sec', 5.0)),
        poll_fetch_retries=int(get('poll_fetch_retries', 5)),
        poll_fetch_backoff_sec=float(get('poll_fetch_backoff_sec', 0.5)),
    )
