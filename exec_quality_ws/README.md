# Execution Quality Analyzer (ROS 2)

Measures **slippage**, **fill rate**, and **latency** per order across multiple
trading venues, then ranks the venues with rolling-window P50/P95/P99 statistics.

Built as a distributed ROS 2 system: each venue listener is an independent node,
so a venue failing or lagging never blocks the others — the same fault-isolation
argument that makes ROS 2 worth using in robotics.

## Architecture

```
                       /orders/submit
  OrderSubmitter ───────────┬──────────────┬──────────────┐
  (validates topology,      ▼              ▼              ▼
   quotes per venue) alpaca_listener  binance_listener  coinbase_listener
                            │   each: VenueAdapter + OrderPoller
                            └──────────────┴──────────────┘
                                    /orders/fills
                                          ▼
                                   MetricsEngine ──── /metrics/per_order ──┐
                                          │                                ▼
                                          │                            Reporter ── CSV + console table
                                          ▼                                ▲
                                     Aggregator ────── /reports/venue ─────┘
                                          │
                                          └── /reports/venue + /metrics/per_order
                                                          ▼
                                                  DashboardBridge ── POST /ingest ──▶ FastAPI dashboard (ai_viz_platform)
```

Custom messages (package `exec_quality_interfaces`):
`OrderMsg`, `FillMsg`, `MetricMsg`, `VenueReport`.

### Quote / execution architecture

All exchange-specific logic lives in `VenueAdapter` (`adapters.py`), which can
own **two separate clients**:

- **execution client** — authenticated, sandbox-confirmed, the only object
  that can call `create_order`.
- **market-data client** — where reference quotes come from. Either the venue
  client itself (`market_data_mode: venue`, default) or a separate **public,
  unauthenticated** client (`market_data_mode: public`) wrapped in a
  read-only guard (`PublicMarketDataClient`) that raises `PermissionError`
  for anything but quote methods. Public market data is created without
  credentials and can never enable trading. The OrderSubmitter's own
  adapters are `quote_only` and have **no execution client at all**.

Quote freshness: the quote timestamp comes from the exchange whenever
provided (order book `timestamp`, else ticker `timestamp`; CCXT milliseconds
are converted to seconds). Local receipt time is used **only** when the
exchange provides none, and the source is recorded in
`timestamp_source` (`exchange_order_book` / `exchange_ticker` / `local`).
Malformed timestamps (non-numeric, zero/negative, implausible epoch, or in
the future beyond `max_future_timestamp_skew_sec`) reject the quote with a
logged reason; quotes older than `quote_max_age_sec` are rejected; and an
order is **never submitted without a valid fresh reference quote** (a
`quote_failed` result is published instead). Buys reference best **ask**,
sells best **bid**; ticker-last fallback is used only when the needed side
is missing and is logged + tagged `reference_source="last"`.

### Normalized status model (CCXT → internal)

One parser (`order_status.parse_ccxt_order`) and one terminal-status function
(`order_status.is_terminal`) are used everywhere. `filled` is treated as
**cumulative**; a missing `filled` field means *unknown → 0.0*, never "fully
executed"; weighted average price resolves `average` → derived from `trades`
→ `price`.

| raw CCXT status        | fill = 0     | 0 < fill < req       | fill ≥ req |
|------------------------|--------------|----------------------|------------|
| `open/new/live/...`    | `open`       | `partial`            | `filled`   |
| `pending/submitted`    | `pending`    | `partial`            | `pending`* |
| `closed/done/filled`   | `canceled`   | `canceled_partial`   | `filled`   |
| `canceled/cancelled`   | `canceled`   | `canceled_partial`   | `filled`   |
| `expired`              | `expired`    | `expired_partial`    | `filled`   |
| `rejected`             | `rejected`   | `rejected_partial`   | `rejected_partial` |
| missing / unknown      | `pending`    | `partial`            | `filled`   |

Locally generated terminal statuses: `timeout`/`timeout_partial` (deadline),
`dry_run`, `error`, `quote_failed`, `submit_failed`.
Non-terminal: `pending`, `open`, `partial`. Everything else is terminal.

### Post-submission polling

If a submission is acknowledged non-terminal (e.g. `status=open, filled=0`),
the listener publishes any initial partial fill and hands the order to an
`OrderPoller` worker thread (the ROS executor is never blocked):

- polls `VenueAdapter.fetch_order_status` (prefers `fetch_order`, falls back
  to `fetch_open_order`/`fetch_closed_order`) every `poll_interval_sec`,
- converts cumulative `filled` into **incremental deltas** so quantities are
  never double-counted, deriving the delta price from the change in
  cumulative notional,
- stops immediately on a terminal state; finalization is idempotent,
- temporary fetch failures get limited exponential backoff
  (`poll_fetch_retries`); a permanent failure finalizes as `error`
  **preserving observed partial fills**,
- at `poll_max_duration_sec` it finalizes as `timeout`/`timeout_partial`
  with a clear reason, preserving fills, weighted price and slippage,
- `create_order` is **never retried or re-invoked** — only status fetches
  retry,
- node shutdown stops all polling promptly without fabricating results.

### Terminal partial fills

A canceled/rejected/expired order with `filled_quantity > 0` is **terminal**
(`canceled_partial`, …): the MetricsEngine finalizes it immediately with
requested + filled quantity, fill ratio, quantity-weighted average price,
reference price, slippage, latency, raw exchange status, and terminal
reason. It is never converted to a non-terminal `partial`, never waits for a
timeout, never has its slippage overwritten with 0.0, and is never counted
as fully filled. **Fill-rate definition:** `fill_rate` = fully filled orders
/ all terminal orders in the window; terminal partials count in the
denominator only, while their slippage/latency still feed the percentile
stats (any order with `fill_ratio > 0`).

### Coinbase sandbox market-data/execution separation

Coinbase Advanced Trade sandbox serves no usable real-time order book. Set
`market_data_mode: public` on that venue: quotes then come from a public,
unauthenticated Coinbase client while orders go to the sandbox client.
Because such fills are measured against prices from a different market, the
venue is flagged **non-comparable** (`comparable=false` on metrics/reports)
and **excluded from the cross-venue ranking by default** — sandbox fills
under public quotes are synthetic and unsuitable for true venue-performance
ranking. Override only deliberately with `include_in_ranking: 'true'`;
`synthetic_execution: true` marks a venue non-comparable explicitly. If the
sandbox cannot serve quotes and no public source is configured, quoting
fails with a clear error telling you to configure `market_data_mode: public`
— production execution and sandbox market data are never combined silently.

### Topology validation

The OrderSubmitter validates the complete topology at startup — before its
submission timer exists. A sandbox/production venue whose reference price
would be simulated fails startup (`TopologyConfigError`) unless the
explicitly named testing override `allow_sim_reference_for_live_testing` is
set; each listener additionally refuses such orders at runtime. Every order
records target venue, quote venue, quote mode, execution mode, bid/ask,
quote timestamp + source, and comparability.

### Dashboard bridge (`dashboard_bridge`)

Forwards analyzer output to the AI-viz dashboard backend over HTTP:

- subscribes to `/reports/venue` (`VenueReport`) and `/metrics/per_order`
  (`MetricMsg`) and converts every message field to JSON,
- POSTs `{"type": "venue_report", "data": {...}}` and
  `{"type": "metric", "data": {...}}` envelopes to `backend_ingest_url`
  (default `http://127.0.0.1:8000/ingest`),
- never blocks the ROS executor: callbacks only enqueue onto a bounded
  queue (oldest dropped when full) drained by one background HTTP worker
  with a short timeout and limited retry/backoff,
- a missing/unreachable backend never crashes the analyzer; warnings and
  delivery logs are throttled (`log_period_sec`) so the console stays
  readable.

Parameters: `backend_ingest_url`, `queue_size` (500), `http_timeout_sec`
(2.0), `max_retries` (2), `retry_backoff_sec` (0.5), `log_period_sec` (5.0).
Override the URL at launch time:

```bash
ros2 launch exec_quality_analyzer analyzer.launch.py \
    backend_ingest_url:=http://192.168.1.20:8000/ingest
```

On a machine without ROS 2, `scripts/no_ros_bridge_demo.py` runs the real
node code (submitter, listeners, metrics engine, aggregator, bridge) over
the test suite's mock rclpy bus while the bridge POSTs to a real backend.

## Requirements

- Ubuntu 22.04 (native, WSL2, or Docker)
- ROS 2 Humble Hawksbill
- Python 3.10
- `numpy` (sim mode), `requests` (dashboard bridge), `ccxt` (live mode only)

## Build (Ubuntu 22.04 + ROS 2 Humble)

```bash
# from the workspace root (exec_quality_ws/)
source /opt/ros/humble/setup.bash
sudo apt install python3-colcon-common-extensions python3-numpy
rosdep install --from-paths src --ignore-src -r -y
colcon build --symlink-install
source install/setup.bash
```

Interfaces build first automatically (colcon resolves the dependency), but if
you ever build packages individually, build `exec_quality_interfaces` before
`exec_quality_analyzer`.

## Run (sim mode — no API keys needed)

```bash
ros2 launch exec_quality_analyzer analyzer.launch.py
```

You should see SUBMIT/FILL logs immediately, and every 10 seconds the Reporter
prints a venue ranking table. All per-order metrics are appended to
`~/exec_quality_metrics.csv` (schema-checked; an old-format file is rotated
to `*.bak` instead of mixing columns). Ctrl+C shuts down cleanly (pollers
stop, CSV closes). Sim mode performs **no network access** and never imports
`ccxt`.

## Configuration recipes (`config/venues.yaml`)

**1. Simulation (default, safe):**
```yaml
# every listener:           mode: sim
# order_submitter:          mode: sim, venue_modes: ""
```

**2. Dry run (real quotes, orders constructed but never sent):**
```yaml
# listener:  mode: sandbox, dry_run: true, market_data_mode: venue
# submitter: mode: sandbox, venue_modes: "sandbox,...", venue_exchange_ids: "binance,..."
```
Orders finalize with status `dry_run`; nothing reaches any venue.

**3. Sandbox/testnet (paper trading):**
```yaml
# listener:  mode: sandbox, dry_run: false
#            market_data_mode: venue        # binance testnet
#            market_data_mode: public       # coinbase sandbox (no sandbox book)
# submitter: mode: sandbox, venue_modes/venue_market_data_modes to match
```
Sandbox activation must be positively confirmed (test endpoint exists AND
the API URL actually switches) or the node refuses to start. Keys come from
`<VENUE>_API_KEY` / `<VENUE>_API_SECRET` env vars and are never logged.

**4. Production-disabled startup (the default everywhere):**
```yaml
allow_production_trading: false   # any 'production' mode fails startup
```
`production` mode is refused by VenueConfig, by topology validation, and is
never the result of a silent fallback. Not recommended for this project.

## Tests

```bash
python3 -m compileall src
cd src/exec_quality_analyzer
python3 -m pytest test/ -v
```

149 tests run without ROS, credentials, or network (mock rclpy layer + fake
ccxt exchanges): quote freshness/timestamps, status normalization, terminal
partials, polling, market-data separation, topology validation, slippage
signs, races, duplicates, sandbox fail-safe, production guard, dry-run, CSV,
and the dashboard bridge (envelope serialization, JSON safety, bounded-queue
drops, retry/backoff, throttled logging, clean shutdown).

With ROS 2 Humble installed:

```bash
source /opt/ros/humble/setup.bash
colcon build --symlink-install
source install/setup.bash
colcon test
colcon test-result --verbose
```

## Run (live mode — paper trading)

1. Create paper/testnet accounts (Alpaca paper, Binance Spot Testnet,
   Coinbase Advanced Trade sandbox).
2. Export keys:
   ```bash
   export ALPACA_API_KEY=... ALPACA_API_SECRET=...
   export BINANCE_TESTNET_API_KEY=... BINANCE_TESTNET_API_SECRET=...
   export COINBASE_SANDBOX_API_KEY=... COINBASE_SANDBOX_API_SECRET=...
   ```
3. `pip install ccxt`
4. In `config/venues.yaml`: set per-listener `mode: sandbox`, the submitter
   `venue_modes` to match, `market_data_mode: public` for Coinbase, and
   `dry_run: false` once quotes flow correctly.
5. Launch as before. Alpaca trades equities, so set the submitter `symbol`
   appropriately (e.g. `AAPL`) or run crypto venues only.

## Metrics semantics

- Slippage is measured against the venue-specific reference captured at
  submission: best **ask** for buys, best **bid** for sells (fallback: ticker
  last, flagged in `reference_source`). Positive slippage = worse execution;
  negative = price improvement.
- Partial fills accumulate into a quantity-weighted average execution price.
- Rejected, canceled, timed-out, dry-run, and quote-failed orders are never
  counted as filled; terminal partials keep their executed quantity and
  execution quality but never count as full fills.

## Useful introspection commands

```bash
ros2 topic echo /metrics/per_order      # watch per-order metrics live
ros2 topic echo /reports/venue          # watch rolling venue stats
ros2 topic hz /orders/fills             # fill throughput
rqt_graph                               # visualize the node graph
ros2 bag record -a                      # record a session for replay
ros2 bag play <bag>                     # replay through the pipeline
```

## Tuning

Everything lives in `config/venues.yaml`:
- `submit_period_sec` — order rate
- `quote_max_age_sec`, `max_future_timestamp_skew_sec` — quote freshness
- `poll_interval_sec`, `poll_max_duration_sec`, `poll_fetch_retries` — polling
- `window_size` (Aggregator param) — rolling window length
- per-venue sim distributions and reject probabilities
