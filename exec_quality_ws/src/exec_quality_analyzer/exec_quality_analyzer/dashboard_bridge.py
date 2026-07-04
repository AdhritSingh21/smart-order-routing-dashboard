"""Dashboard bridge node.

Forwards analyzer output to the AI-viz FastAPI dashboard backend:

    /reports/venue     (VenueReport) -> {"type": "venue_report", "data": {...}}
    /metrics/per_order (MetricMsg)   -> {"type": "metric", "data": {...}}

both POSTed to the `backend_ingest_url` parameter (default
http://127.0.0.1:8000/ingest).

Subscription callbacks never perform network I/O: each message is
serialized to a JSON-safe envelope and pushed onto a bounded queue that a
single background worker thread drains with short-timeout HTTP POSTs.
When the queue is full the OLDEST envelope is dropped (freshest venue
stats win). Delivery failures are retried with backoff and then dropped —
the analyzer pipeline is never blocked or crashed by a missing backend —
and both failure and success logs are throttled so the console stays
readable while the backend is down.
"""
import array
import queue
import threading
import time

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy

import requests

from exec_quality_interfaces.msg import MetricMsg, VenueReport


def message_to_json_safe(msg) -> dict:
    """Convert a ROS message to a dict of JSON-serializable values.

    Uses the rosidl field map when available (real rclpy messages) and
    falls back to instance attributes (test doubles).
    """
    if hasattr(msg, 'get_fields_and_field_types'):
        fields = msg.get_fields_and_field_types().keys()
    else:
        fields = [f for f in vars(msg) if not f.startswith('_')]
    return {field: _json_safe(getattr(msg, field)) for field in fields}


def _json_safe(value):
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float, str)):
        return value
    if isinstance(value, (bytes, bytearray)):
        return list(value)
    if isinstance(value, (list, tuple, array.array)):
        return [_json_safe(v) for v in value]
    if hasattr(value, 'sec') and hasattr(value, 'nanosec'):
        # builtin_interfaces Time/Duration -> float seconds
        return float(value.sec) + float(value.nanosec) * 1e-9
    if hasattr(value, 'tolist'):                  # numpy array
        return value.tolist()
    if hasattr(value, 'item'):                    # numpy scalar
        return value.item()
    return str(value)


class DashboardBridge(Node):
    def __init__(self):
        super().__init__('dashboard_bridge')

        self.declare_parameter('backend_ingest_url', 'http://127.0.0.1:8000/ingest')
        self.declare_parameter('queue_size', 500)
        self.declare_parameter('http_timeout_sec', 2.0)
        self.declare_parameter('max_retries', 2)
        self.declare_parameter('retry_backoff_sec', 0.5)
        self.declare_parameter('log_period_sec', 5.0)

        self.url = str(self.get_parameter('backend_ingest_url').value)
        queue_size = int(self.get_parameter('queue_size').value)
        self.timeout = float(self.get_parameter('http_timeout_sec').value)
        self.max_retries = int(self.get_parameter('max_retries').value)
        self.backoff = float(self.get_parameter('retry_backoff_sec').value)
        self.log_period = float(self.get_parameter('log_period_sec').value)
        if queue_size <= 0 or self.timeout <= 0:
            raise ValueError('dashboard_bridge: queue_size and '
                             'http_timeout_sec must be positive')

        self.outbox: queue.Queue = queue.Queue(maxsize=queue_size)
        self._session = requests.Session()
        self._stop = threading.Event()

        # Throttled-log state: [last_emit_monotonic, suppressed_count]
        self._drop_log = [0.0, 0]
        self._fail_log = [0.0, 0]
        self._ok_log = [0.0, 0]
        self._delivered = 0
        self._announced_ok = False

        qos = QoSProfile(reliability=ReliabilityPolicy.RELIABLE,
                         history=HistoryPolicy.KEEP_LAST, depth=200)
        self.create_subscription(VenueReport, '/reports/venue',
                                 self.on_venue_report, qos)
        self.create_subscription(MetricMsg, '/metrics/per_order',
                                 self.on_metric, qos)

        self._worker = threading.Thread(target=self._run_worker,
                                        name='dashboard_bridge_http',
                                        daemon=True)
        self._worker.start()

        self.get_logger().info(
            f'Dashboard bridge up. POST -> {self.url} '
            f'(queue={queue_size}, timeout={self.timeout}s)')

    # -- subscription callbacks (no I/O, never block) ----------------------

    def on_venue_report(self, msg: VenueReport):
        self._enqueue({'type': 'venue_report', 'data': message_to_json_safe(msg)})

    def on_metric(self, msg: MetricMsg):
        self._enqueue({'type': 'metric', 'data': message_to_json_safe(msg)})

    def _enqueue(self, envelope: dict):
        try:
            self.outbox.put_nowait(envelope)
        except queue.Full:
            try:  # drop the oldest entry so the freshest stats survive
                self.outbox.get_nowait()
            except queue.Empty:
                pass
            try:
                self.outbox.put_nowait(envelope)
            except queue.Full:
                pass
            self._throttled(self._drop_log, self.get_logger().warning,
                            'outbox full; dropping oldest message')

    # -- background HTTP worker --------------------------------------------

    def _run_worker(self):
        while not self._stop.is_set():
            try:
                envelope = self.outbox.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                self._deliver(envelope)
            finally:
                self.outbox.task_done()

    def _deliver(self, envelope: dict) -> bool:
        """POST one envelope; limited retries with backoff, then drop."""
        for attempt in range(self.max_retries + 1):
            if self._stop.is_set():
                return False
            try:
                response = self._session.post(self.url, json=envelope,
                                              timeout=self.timeout)
                response.raise_for_status()
            except requests.RequestException as exc:
                if attempt < self.max_retries:
                    self._stop.wait(self.backoff * (2 ** attempt))
                    continue
                self._throttled(
                    self._fail_log, self.get_logger().warning,
                    f'delivery to {self.url} failed '
                    f'({type(exc).__name__}: {exc}); dropping message')
                return False
            else:
                self._delivered += 1
                if not self._announced_ok:
                    self._announced_ok = True
                    self.get_logger().info(
                        f'connected: first message delivered to {self.url}')
                else:
                    self._throttled(
                        self._ok_log, self.get_logger().info,
                        f'delivered {self._delivered} messages so far',
                        count_suppressed=False)
                return True
        return False

    def _throttled(self, state, log_fn, message, count_suppressed=True):
        """Emit at most one log line per log_period_sec."""
        now = time.monotonic()
        if now - state[0] >= self.log_period:
            suppressed = state[1]
            state[0], state[1] = now, 0
            if suppressed and count_suppressed:
                message += f' ({suppressed} similar suppressed)'
            log_fn(message)
        else:
            state[1] += 1

    # -- shutdown ------------------------------------------------------------

    def destroy_node(self):
        self._stop.set()
        if self._worker.is_alive():
            self._worker.join(timeout=3.0)
        try:
            self._session.close()
        except Exception as exc:
            self.get_logger().warning(f'HTTP session close failed: {exc}')
        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = DashboardBridge()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
