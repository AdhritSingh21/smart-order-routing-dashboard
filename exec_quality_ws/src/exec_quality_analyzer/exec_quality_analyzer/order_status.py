"""Normalized internal order-status model and the single CCXT order parser.

Every component (adapters, poller, listener, metrics engine, aggregator)
imports its status semantics from this module so there is exactly ONE place
that decides:
  - which statuses are terminal,
  - how raw CCXT statuses map to internal statuses,
  - how cumulative fill quantities and weighted average prices are read out
    of a CCXT order dict.

Status model
------------
Non-terminal (the order may still change):
    pending   submitted/acknowledged, no fills yet, state unknown or queued
    open      resting on the venue, no fills yet
    partial   some quantity filled, order still working

Terminal (the order will never change again — finalize metrics now):
    filled            fully executed
    canceled          canceled with zero fill
    canceled_partial  canceled after a partial fill  (fills are REAL — keep them)
    rejected          venue rejected, zero fill
    rejected_partial  venue rejected after a partial fill
    expired           expired (e.g. IOC/FOK), zero fill
    expired_partial   expired after a partial fill
    timeout           we stopped waiting, zero fill observed
    timeout_partial   we stopped waiting after observing partial fills
    dry_run           order constructed but intentionally never submitted
    error             unrecoverable local/exchange error (fills preserved if any)
    quote_failed      no valid reference quote -> order never submitted
    submit_failed     submission attempt raised before reaching the venue

Fill-rate definition (documented contract, used by Aggregator)
--------------------------------------------------------------
fill_rate = (# orders with status 'filled') / (# terminal orders in window).
Terminal partial outcomes (canceled_partial, timeout_partial, ...) count in
the denominator only: they are NOT fully filled, but their executed quantity,
weighted average price, and slippage are preserved and included in the
slippage/latency percentile statistics (any order with fill_ratio > 0
contributes). A partial cancellation is therefore never counted as filled and
never loses its measured execution quality.
"""
from __future__ import annotations

from typing import Any, Optional

_EPS = 1e-12


class OrderStatus:
    """String constants for the normalized internal status model."""
    PENDING = 'pending'
    OPEN = 'open'
    PARTIAL = 'partial'
    FILLED = 'filled'
    CANCELED = 'canceled'
    CANCELED_PARTIAL = 'canceled_partial'
    REJECTED = 'rejected'
    REJECTED_PARTIAL = 'rejected_partial'
    EXPIRED = 'expired'
    EXPIRED_PARTIAL = 'expired_partial'
    TIMEOUT = 'timeout'
    TIMEOUT_PARTIAL = 'timeout_partial'
    DRY_RUN = 'dry_run'
    ERROR = 'error'
    QUOTE_FAILED = 'quote_failed'
    SUBMIT_FAILED = 'submit_failed'


NONTERMINAL_STATUSES = frozenset({
    OrderStatus.PENDING, OrderStatus.OPEN, OrderStatus.PARTIAL,
})

TERMINAL_STATUSES = frozenset({
    OrderStatus.FILLED,
    OrderStatus.CANCELED, OrderStatus.CANCELED_PARTIAL,
    OrderStatus.REJECTED, OrderStatus.REJECTED_PARTIAL,
    OrderStatus.EXPIRED, OrderStatus.EXPIRED_PARTIAL,
    OrderStatus.TIMEOUT, OrderStatus.TIMEOUT_PARTIAL,
    OrderStatus.DRY_RUN, OrderStatus.ERROR,
    OrderStatus.QUOTE_FAILED, OrderStatus.SUBMIT_FAILED,
})

# Terminal statuses that carry a real partial execution.
TERMINAL_PARTIAL_STATUSES = frozenset({
    OrderStatus.CANCELED_PARTIAL, OrderStatus.REJECTED_PARTIAL,
    OrderStatus.EXPIRED_PARTIAL, OrderStatus.TIMEOUT_PARTIAL,
})

_PARTIAL_VARIANT = {
    OrderStatus.CANCELED: OrderStatus.CANCELED_PARTIAL,
    OrderStatus.REJECTED: OrderStatus.REJECTED_PARTIAL,
    OrderStatus.EXPIRED: OrderStatus.EXPIRED_PARTIAL,
    OrderStatus.TIMEOUT: OrderStatus.TIMEOUT_PARTIAL,
}


def is_terminal(status: str) -> bool:
    """The ONE place that decides whether a status is terminal.

    Unknown statuses are treated as non-terminal so the poller keeps
    watching them until its own deadline; they are never silently final.
    """
    return status in TERMINAL_STATUSES


def is_terminal_partial(status: str) -> bool:
    return status in TERMINAL_PARTIAL_STATUSES


def with_fill_qualifier(base_status: str, filled_quantity: float) -> str:
    """Map a base terminal status to its *_partial variant when fills exist.

    e.g. with_fill_qualifier('canceled', 0.3) -> 'canceled_partial'
         with_fill_qualifier('canceled', 0.0) -> 'canceled'
    Statuses without a partial variant (error, dry_run, ...) pass through;
    their filled quantity is still preserved by the caller.
    """
    if filled_quantity > _EPS and base_status in _PARTIAL_VARIANT:
        return _PARTIAL_VARIANT[base_status]
    return base_status


# Raw CCXT/exchange status spellings -> handling category.
_CCXT_CANCELED = ('canceled', 'cancelled', 'canceling', 'cancelling')
_CCXT_CLOSED = ('closed', 'done', 'filled')
_CCXT_OPEN = ('open', 'new', 'accepted', 'live', 'partially_filled',
              'partiallyfilled')
_CCXT_PENDING = ('pending', 'submitted', 'received', 'untriggered')


def normalize_ccxt_status(raw_status: Optional[str], filled_quantity: float,
                          requested_quantity: float) -> str:
    """Map a raw CCXT order status + cumulative fill into the internal model.

    Rules:
    - A fully-filled order is 'filled' even when the venue reports a
      closed/canceled/expired-like terminal status (full fill then cancel of
      the remainder-of-nothing is a complete execution).
    - A terminal venue status with 0 < filled < requested becomes the
      *_partial variant — it must NEVER degrade to non-terminal 'partial'.
    - A terminal venue status with zero fill stays the unfilled terminal
      status.
    - Missing/unknown statuses are judged by fill progress alone and remain
      non-terminal ('pending'/'partial') so polling can resolve them. A
      missing field never means "fully executed".
    """
    raw = (raw_status or '').strip().lower()
    has_fill = filled_quantity > _EPS
    complete = (requested_quantity > _EPS
                and filled_quantity + _EPS >= requested_quantity)

    if raw in _CCXT_CANCELED:
        if complete:
            return OrderStatus.FILLED
        return OrderStatus.CANCELED_PARTIAL if has_fill else OrderStatus.CANCELED
    if raw in _CCXT_CLOSED:
        if complete:
            return OrderStatus.FILLED
        # 'closed' without complete fill: terminal (e.g. IOC remainder
        # canceled by the venue). Zero-fill close means nothing executed.
        return OrderStatus.CANCELED_PARTIAL if has_fill else OrderStatus.CANCELED
    if raw == 'expired':
        if complete:
            return OrderStatus.FILLED
        return OrderStatus.EXPIRED_PARTIAL if has_fill else OrderStatus.EXPIRED
    if raw == 'rejected':
        return OrderStatus.REJECTED_PARTIAL if has_fill else OrderStatus.REJECTED
    if raw in _CCXT_OPEN:
        if complete:
            return OrderStatus.FILLED
        return OrderStatus.PARTIAL if has_fill else OrderStatus.OPEN
    if raw in _CCXT_PENDING:
        return OrderStatus.PARTIAL if has_fill else OrderStatus.PENDING
    # Missing or unknown raw status: judge by fills, stay non-terminal.
    if complete:
        return OrderStatus.FILLED
    if has_fill:
        return OrderStatus.PARTIAL
    return OrderStatus.PENDING


def _coerce_float(value: Any) -> Optional[float]:
    """float() with bools and non-numerics rejected -> None."""
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out or out in (float('inf'), float('-inf')):  # NaN/inf
        return None
    return out


def weighted_average_from_trades(trades: Any) -> tuple[float, float]:
    """(weighted_avg_price, total_qty) from a CCXT trades/fills array.

    Malformed entries are skipped. Returns (0.0, 0.0) when nothing usable.
    """
    if not isinstance(trades, (list, tuple)):
        return 0.0, 0.0
    notional = qty = 0.0
    for t in trades:
        if not isinstance(t, dict):
            continue
        p = _coerce_float(t.get('price'))
        a = _coerce_float(t.get('amount'))
        if p is None or a is None or p <= 0 or a <= 0:
            continue
        notional += p * a
        qty += a
    if qty <= _EPS:
        return 0.0, 0.0
    return notional / qty, qty


def parse_ccxt_order(result: Any, requested_quantity: float) -> dict:
    """The ONE centralized CCXT order-response parser.

    Returns a normalized dict:
      status            internal status (see OrderStatus)
      exchange_status   raw venue status string ('' if absent)
      filled_quantity   CUMULATIVE filled quantity (0.0 when unknown — a
                        missing 'filled' field is never assumed executed)
      average_price     quantity-weighted average price for the cumulative
                        fill (0.0 when unknown); resolution order:
                        'average' -> derived from 'trades' -> 'price'
      exchange_order_id / client_order_id  ('' if absent)
      order_timestamp   venue order timestamp in epoch SECONDS (ccxt reports
                        milliseconds), 0.0 when absent/invalid
      error_reason      parser-level problems, '' when clean
      raw               the original response
    """
    if not isinstance(result, dict):
        return {
            'status': OrderStatus.ERROR, 'exchange_status': '',
            'filled_quantity': 0.0, 'average_price': 0.0,
            'exchange_order_id': '', 'client_order_id': '',
            'order_timestamp': 0.0,
            'error_reason': f'malformed order response: '
                            f'{type(result).__name__}',
            'raw': result,
        }

    reasons = []

    filled = _coerce_float(result.get('filled'))
    if result.get('filled') is not None and filled is None:
        reasons.append(f"non-numeric 'filled': {result.get('filled')!r}")
    filled = filled if filled is not None else 0.0
    if filled < 0:
        reasons.append(f"negative 'filled' {filled} clamped to 0")
        filled = 0.0

    avg = _coerce_float(result.get('average'))
    if avg is None or avg <= 0:
        trade_avg, trade_qty = weighted_average_from_trades(
            result.get('trades'))
        if trade_avg > 0:
            avg = trade_avg
            # Trades are the ground truth when 'filled' is missing.
            if filled <= _EPS and trade_qty > 0:
                filled = trade_qty
        else:
            avg = _coerce_float(result.get('price'))
    avg = avg if avg is not None and avg > 0 else 0.0
    if filled > _EPS and avg <= 0:
        reasons.append('fill reported without a usable price')

    raw_status = result.get('status')
    exchange_status = str(raw_status) if raw_status is not None else ''
    status = normalize_ccxt_status(exchange_status, filled,
                                   requested_quantity)

    ts = _coerce_float(result.get('timestamp'))
    if ts is not None and ts > 1e11:   # ccxt timestamps are milliseconds
        ts = ts / 1000.0
    order_timestamp = ts if ts is not None and ts > 0 else 0.0

    return {
        'status': status,
        'exchange_status': exchange_status,
        'filled_quantity': filled,
        'average_price': avg,
        'exchange_order_id': str(result.get('id') or ''),
        'client_order_id': str(result.get('clientOrderId') or ''),
        'order_timestamp': order_timestamp,
        'error_reason': '; '.join(reasons),
        'raw': result,
    }
