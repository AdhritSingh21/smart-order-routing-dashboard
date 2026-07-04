"""Reporter node.

1. Appends every per-order metric to CSV with a fixed, validated schema.
   If an existing file's header does not match (e.g. written by an older
   version), the old file is rotated aside instead of mixing schemas.
2. Prints a live venue-ranking table. Venues whose execution environment is
   not comparable (e.g. synthetic sandbox fills measured against public
   production quotes) are listed separately and EXCLUDED from the ranking.

Ranking score: fill_rate * 100 - slippage_p50 - latency_p50 / 50
(higher is better).
"""
import csv
import os
import time

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy

from exec_quality_interfaces.msg import MetricMsg, VenueReport

CSV_COLUMNS = [
    'timestamp', 'order_id', 'venue', 'symbol', 'filled', 'slippage_bps',
    'latency_ms', 'status', 'fill_ratio', 'requested_quantity',
    'filled_quantity', 'avg_fill_price', 'reference_price',
    'exchange_status', 'terminal_reason', 'quote_mode', 'execution_mode',
    'comparable',
]


class Reporter(Node):
    def __init__(self):
        super().__init__('reporter')

        self.declare_parameter('csv_path', os.path.expanduser('~/exec_quality_metrics.csv'))
        self.declare_parameter('table_period_sec', 10.0)

        self.csv_path = self.get_parameter('csv_path').value
        period = float(self.get_parameter('table_period_sec').value)

        self.latest_reports: dict[str, VenueReport] = {}

        self._open_csv()

        qos = QoSProfile(reliability=ReliabilityPolicy.RELIABLE,
                         history=HistoryPolicy.KEEP_LAST, depth=200)
        self.create_subscription(MetricMsg, '/metrics/per_order', self.on_metric, qos)
        self.create_subscription(VenueReport, '/reports/venue', self.on_report, qos)
        self.create_timer(period, self.print_table)

        self.get_logger().info(f'Reporter up. CSV -> {self.csv_path}')

    def _open_csv(self):
        """Open the CSV, rotating any existing file with a different schema."""
        write_header = True
        if os.path.exists(self.csv_path) and os.path.getsize(self.csv_path) > 0:
            try:
                with open(self.csv_path, newline='') as f:
                    existing = next(csv.reader(f), [])
            except OSError:
                existing = []
            if existing == CSV_COLUMNS:
                write_header = False
            else:
                rotated = f'{self.csv_path}.{int(time.time())}.bak'
                os.replace(self.csv_path, rotated)
                self.get_logger().warning(
                    f'CSV schema changed; rotated old file to {rotated}')
        self.csv_file = open(self.csv_path, 'a', newline='')
        self.writer = csv.writer(self.csv_file)
        self._csv_open = True
        if write_header:
            self.writer.writerow(CSV_COLUMNS)
            self.csv_file.flush()

    def on_metric(self, m: MetricMsg):
        if not self._csv_open:
            return
        try:
            self.writer.writerow(
                [f'{m.timestamp:.6f}', m.order_id, m.venue, m.symbol,
                 int(m.filled), f'{m.slippage_bps:.3f}', f'{m.latency_ms:.2f}',
                 m.status, f'{m.fill_ratio:.4f}',
                 f'{m.requested_quantity:.10g}', f'{m.filled_quantity:.10g}',
                 f'{m.avg_fill_price:.10g}', f'{m.reference_price:.10g}',
                 m.exchange_status, m.terminal_reason,
                 m.quote_mode, m.execution_mode, int(m.comparable)])
            self.csv_file.flush()
        except ValueError:
            pass  # file closed during shutdown race

    def on_report(self, r: VenueReport):
        self.latest_reports[r.venue] = r

    @staticmethod
    def score(r: VenueReport) -> float:
        return r.fill_rate * 100.0 - r.slippage_bps_p50 - r.latency_ms_p50 / 50.0

    def print_table(self):
        if not self.latest_reports:
            return
        comparable = [r for r in self.latest_reports.values()
                      if getattr(r, 'comparable', True)]
        excluded = [r for r in self.latest_reports.values()
                    if not getattr(r, 'comparable', True)]
        ranked = sorted(comparable, key=self.score, reverse=True)
        lines = ['', '=' * 78,
                 f'{"RANK":<5}{"VENUE":<20}{"FILL%":<8}{"SLIP p50/p95 (bps)":<22}'
                 f'{"LAT p50/p95/p99 (ms)":<23}', '-' * 78]
        for i, r in enumerate(ranked, 1):
            lines.append(
                f'{i:<5}{r.venue:<20}{r.fill_rate*100:<8.1f}'
                f'{r.slippage_bps_p50:>7.2f} /{r.slippage_bps_p95:>7.2f}      '
                f'{r.latency_ms_p50:>6.1f}/{r.latency_ms_p95:>6.1f}/{r.latency_ms_p99:>6.1f}')
        for r in excluded:
            lines.append(
                f'{"-":<5}{r.venue:<20}{r.fill_rate*100:<8.1f}'
                f'EXCLUDED from ranking (execution not comparable: '
                f'synthetic/sandbox fills vs public quotes)')
        lines.append('=' * 78)
        self.get_logger().info('\n'.join(lines))

    def destroy_node(self):
        self._csv_open = False
        try:
            self.csv_file.close()
        except Exception as exc:
            self.get_logger().warning(f'CSV close failed: {exc}')
        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = Reporter()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
