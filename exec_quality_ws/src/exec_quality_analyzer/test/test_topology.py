"""Startup topology validation: submitter mode vs per-venue quote/execution
modes must be consistent BEFORE any order can be submitted."""
import time

import pytest
from conftest import BUS, MockNode, OrderMsg, _Logger

from exec_quality_analyzer.adapters import VenueConfig
from exec_quality_analyzer.topology import (
    validate_topology, TopologyConfigError,
)
from exec_quality_analyzer.order_submitter import OrderSubmitter
from exec_quality_analyzer.execution_listener import ExecutionListener


# 1. All-simulation configuration succeeds (no credentials, no network).
def test_all_sim_succeeds():
    topo = validate_topology('sim', [
        {'venue': 'alpaca', 'mode': 'sim'},
        {'venue': 'binance_testnet', 'mode': 'sim'},
    ])
    assert all(t.quote_mode == 'sim' and t.execution_mode == 'sim'
               and t.comparable for t in topo.values())


def test_all_sim_submitter_records_quote_fields():
    sub = OrderSubmitter.__new__(OrderSubmitter)
    MockNode.__init__(sub, 'order_submitter')
    sub.set_param('venues', ['alpaca'])
    sub.set_param('mode', 'sim')
    OrderSubmitter.__init__(sub)
    orders = []
    BUS.subscribe('/orders/submit', orders.append)
    sub.submit_next()
    o = orders[0]
    assert o.quote_venue == 'alpaca'
    assert o.quote_mode == 'sim' and o.execution_mode == 'sim'
    assert o.timestamp_source == 'sim'
    assert o.bid == o.ask == o.quoted_price > 0
    assert o.quote_time > 0 and o.comparable


# 2. Sandbox listener + simulated submitter fails at startup.
def test_sandbox_venue_with_sim_submitter_fails():
    with pytest.raises(TopologyConfigError, match='SIMULATED reference'):
        validate_topology('sim', [
            {'venue': 'binance_testnet', 'mode': 'sandbox',
             'ccxt_exchange_id': 'binance'},
        ])


def test_sandbox_venue_with_sim_submitter_fails_in_node():
    sub = OrderSubmitter.__new__(OrderSubmitter)
    MockNode.__init__(sub, 'order_submitter')
    sub.set_param('venues', ['binance_testnet'])
    sub.set_param('mode', 'sim')
    sub.set_param('venue_modes', 'sandbox')
    with pytest.raises(TopologyConfigError):
        OrderSubmitter.__init__(sub)


def test_explicit_testing_override_permits_sim_reference():
    topo = validate_topology(
        'sim',
        [{'venue': 'b', 'mode': 'sandbox', 'ccxt_exchange_id': 'binance'}],
        allow_sim_reference_for_live_testing=True)
    assert topo['b'].quote_mode == 'sim'
    assert topo['b'].comparable is False   # still never ranked


# 3. Sandbox listener + matching venue quote source succeeds.
def test_sandbox_with_matching_quote_source():
    topo = validate_topology('sandbox', [
        {'venue': 'binance_testnet', 'mode': 'sandbox',
         'ccxt_exchange_id': 'binance'},
    ])
    t = topo['binance_testnet']
    assert t.quote_mode == 'venue' and t.execution_mode == 'sandbox'
    assert t.comparable is True


# 4. Mixed venue modes are validated independently.
def test_mixed_modes_validated_independently():
    topo = validate_topology('sandbox', [
        {'venue': 'simvenue', 'mode': 'sim'},
        {'venue': 'binance_testnet', 'mode': 'sandbox',
         'ccxt_exchange_id': 'binance'},
        {'venue': 'coinbase_sandbox', 'mode': 'sandbox',
         'ccxt_exchange_id': 'coinbase', 'market_data_mode': 'public'},
    ])
    assert topo['simvenue'].quote_mode == 'sim'
    assert topo['binance_testnet'].quote_mode == 'venue'
    assert topo['binance_testnet'].comparable is True
    assert topo['coinbase_sandbox'].quote_mode == 'public'
    assert topo['coinbase_sandbox'].comparable is False  # synthetic vs public

    # one invalid venue fails the WHOLE topology
    with pytest.raises(TopologyConfigError):
        validate_topology('sandbox', [
            {'venue': 'simvenue', 'mode': 'sim'},
            {'venue': 'bad', 'mode': 'warp-speed'},
        ])


# 5. Missing venue configuration fails.
def test_missing_venue_config_fails():
    with pytest.raises(TopologyConfigError, match='missing mode'):
        validate_topology('sim', [{'venue': 'x', 'mode': ''}])
    with pytest.raises(TopologyConfigError, match='without a name'):
        validate_topology('sim', [{'mode': 'sim'}])
    with pytest.raises(TopologyConfigError, match='at least one venue'):
        validate_topology('sim', [])
    with pytest.raises(TopologyConfigError, match='ccxt_exchange_id'):
        validate_topology('sandbox',
                          [{'venue': 'x', 'mode': 'sandbox'}])


def test_misaligned_per_venue_lists_fail_in_node():
    sub = OrderSubmitter.__new__(OrderSubmitter)
    MockNode.__init__(sub, 'order_submitter')
    sub.set_param('venues', ['a', 'b'])
    sub.set_param('mode', 'sim')
    sub.set_param('venue_modes', 'sim')      # 1 mode for 2 venues
    with pytest.raises(ValueError, match='align'):
        OrderSubmitter.__init__(sub)


# 6. Invalid mode string fails.
def test_invalid_mode_string_fails():
    with pytest.raises(TopologyConfigError, match='invalid mode'):
        validate_topology('sim', [{'venue': 'x', 'mode': 'yolo'}])
    with pytest.raises(TopologyConfigError, match='submitter mode'):
        validate_topology('yolo', [{'venue': 'x', 'mode': 'sim'}])


# 7. Production mode remains blocked without explicit permission.
def test_production_blocked_without_flag():
    with pytest.raises(TopologyConfigError, match='production'):
        validate_topology('sim', [
            {'venue': 'p', 'mode': 'production', 'ccxt_exchange_id': 'kraken'},
        ])
    with pytest.raises(TopologyConfigError, match='production'):
        validate_topology('production',
                          [{'venue': 'x', 'mode': 'sim'}])
    # explicit flag unlocks it (still not recommended)
    topo = validate_topology(
        'sandbox',
        [{'venue': 'p', 'mode': 'production', 'ccxt_exchange_id': 'kraken'}],
        allow_production_trading=True)
    assert topo['p'].execution_mode == 'production'


# Runtime defense in depth: a non-sim listener refuses simulated references.
def test_listener_refuses_sim_reference():
    class FakeAdapter:
        config = VenueConfig(venue='v1', mode='sandbox',
                             ccxt_exchange_id='binance')
        def submit_market_order(self, *a):
            raise AssertionError('must not submit')

    listener = ExecutionListener.__new__(ExecutionListener)
    MockNode.__init__(listener, 'listener')
    listener.set_param('venue', 'v1')
    listener.set_param('mode', 'sandbox')
    ExecutionListener.__init__(listener, adapter=FakeAdapter())

    fills = []
    BUS.subscribe('/orders/fills', fills.append)
    o = OrderMsg()
    o.order_id, o.venue, o.symbol, o.side = 'o1', 'v1', 'BTC/USDT', 'buy'
    o.quantity, o.quoted_price = 1.0, 100.0
    o.reference_source, o.quote_mode = 'sim', 'sim'
    o.submit_time = o.quote_time = time.time()
    listener.on_order(o)
    assert len(fills) == 1 and fills[0].status == 'rejected'
    assert 'simulated reference' in fills[0].error_reason
    assert any('REFUSED' in r[1] for r in listener.get_logger().records)
    listener.destroy_node()
