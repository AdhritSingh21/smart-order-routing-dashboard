"""Installs mock rclpy and message modules so node code imports without ROS."""
import sys
import types
import threading
from collections import defaultdict


class Bus:
    def __init__(self):
        self.subs = defaultdict(list)
    def publish(self, topic, msg):
        for cb in list(self.subs[topic]):
            cb(msg)
    def subscribe(self, topic, cb):
        self.subs[topic].append(cb)


BUS = Bus()


class _Logger:
    def __init__(self, name): self.name = name; self.records = []
    def _rec(self, lvl, m): self.records.append((lvl, m))
    def info(self, m): self._rec('info', m)
    def warning(self, m): self._rec('warn', m)
    def error(self, m): self._rec('error', m)


class _Pub:
    def __init__(self, topic): self.topic = topic
    def publish(self, msg): BUS.publish(self.topic, msg)


class MockNode:
    def __init__(self, name):
        self._name = name
        if not hasattr(self, '_params'):
            self._params = {}
        self._timer_cbs = []
        self._logger = _Logger(name)
    def declare_parameter(self, key, default):
        self._params.setdefault(key, default)
    def has_parameter(self, key): return key in self._params
    def get_parameter(self, key):
        return types.SimpleNamespace(value=self._params[key])
    def set_param(self, key, value): self._params[key] = value
    def create_publisher(self, mtype, topic, qos): return _Pub(topic)
    def create_subscription(self, mtype, topic, cb, qos):
        BUS.subscribe(topic, cb)
    def create_timer(self, period, cb):
        self._timer_cbs.append(cb)  # tests fire timers manually
    def get_logger(self): return self._logger
    def destroy_node(self): pass


rclpy = types.ModuleType('rclpy')
rclpy.init = lambda args=None: None
rclpy.shutdown = lambda: None
rclpy.spin = lambda n: None
node_mod = types.ModuleType('rclpy.node'); node_mod.Node = MockNode
qos_mod = types.ModuleType('rclpy.qos')
qos_mod.QoSProfile = lambda **kw: None
qos_mod.ReliabilityPolicy = types.SimpleNamespace(RELIABLE=1)
qos_mod.HistoryPolicy = types.SimpleNamespace(KEEP_LAST=1)
rclpy.node, rclpy.qos = node_mod, qos_mod
sys.modules.setdefault('rclpy', rclpy)
sys.modules.setdefault('rclpy.node', node_mod)
sys.modules.setdefault('rclpy.qos', qos_mod)


def _msg(name, fields):
    def __init__(self):
        for f, d in fields.items(): setattr(self, f, d)
    return type(name, (), {'__init__': __init__})


OrderMsg = _msg('OrderMsg', dict(order_id='', venue='', symbol='', side='',
    quantity=0.0, quoted_price=0.0, reference_source='', quote_time=0.0,
    submit_time=0.0, quote_venue='', quote_mode='', execution_mode='',
    timestamp_source='', bid=0.0, ask=0.0, comparable=True))
FillMsg = _msg('FillMsg', dict(order_id='', venue='', symbol='', status='',
    fill_price=0.0, filled_quantity=0.0, fill_time=0.0, error_reason='',
    exchange_order_id='', exchange_status=''))
MetricMsg = _msg('MetricMsg', dict(order_id='', venue='', symbol='', status='',
    slippage_bps=0.0, latency_ms=0.0, fill_ratio=0.0, filled=False,
    requested_quantity=0.0, filled_quantity=0.0, avg_fill_price=0.0,
    reference_price=0.0, exchange_status='', terminal_reason='',
    quote_mode='', execution_mode='', comparable=True, timestamp=0.0))
VenueReport = _msg('VenueReport', dict(venue='', window_orders=0,
    fill_rate=0.0, slippage_bps_p50=0.0, slippage_bps_p95=0.0,
    latency_ms_p50=0.0, latency_ms_p95=0.0, latency_ms_p99=0.0,
    comparable=True, timestamp=0.0))

iface = types.ModuleType('exec_quality_interfaces')
iface_msg = types.ModuleType('exec_quality_interfaces.msg')
for cls in (OrderMsg, FillMsg, MetricMsg, VenueReport):
    setattr(iface_msg, cls.__name__, cls)
iface.msg = iface_msg
sys.modules.setdefault('exec_quality_interfaces', iface)
sys.modules.setdefault('exec_quality_interfaces.msg', iface_msg)

import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest

@pytest.fixture(autouse=True)
def clean_bus():
    BUS.subs.clear()
    yield
