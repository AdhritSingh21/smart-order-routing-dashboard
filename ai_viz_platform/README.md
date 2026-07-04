# AI Viz Platform

FastAPI backend for the smart-order-routing dashboard. Two pipelines share
one process and one `/ws` WebSocket:

**1. Execution-quality routing (the headline).** Per-order metrics arrive at
`POST /ingest` from the ROS 2 analyzer (or its no-ROS fallback). For each
completed order the backend updates that venue's rolling feature state,
predicts the venue's *next-order* slippage (bps), fill probability, and p95
latency bound with gradient-boosting models (`exec_ml/`), re-ranks venues
into a routing recommendation, scores the previous prediction against the
realized outcome (rolling live validation), and broadcasts an
`execution_prediction` frame.

```text
POST /ingest (MetricMsg)
        -> exec_ml.features.VenueRollingState   (same code path as training)
        -> HistGradientBoosting slippage / fill / latency models
        -> routing score + recommended venue + live validation
        -> /ws broadcast {"type": "execution_prediction", ...}
```

**2. Market context (reference feed).** An asyncio market-data pipeline
(Alpaca WebSocket or simulator → rolling features → ONNX logistic regression
→ `/ws`) streams BTC/USD prices with a next-bar up/down signal. Next-bar
direction from price-only features is close to random; the dashboard keeps
this panel as market context, clearly labeled, and it plays no part in
routing decisions.

## Python

Python 3.10 or newer.

## Install

```bash
cd ai_viz_platform
python -m venv .venv
```

Activate it:

```bash
# macOS/Linux
source .venv/bin/activate

# Windows PowerShell
.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Train the execution-quality routing model

```bash
python train_exec_model.py
```

With no arguments this generates a **clearly labeled simulated replay**
(`exec_ml/simulate.py`, 30,000 orders with latent per-venue congestion and
shared market-stress regimes) and trains three sklearn
`HistGradientBoosting` models on it:

- `slippage_model.joblib` — regression, expected slippage (bps) of the next order
- `fill_model.joblib` — classification, probability the next order fully fills
- `latency_model.joblib` — 0.95-quantile regression, p95 latency bound (ms)

All artifacts land in `models/exec_quality/` together with
`exec_model_metadata.json` (targets, feature list, data source, `is_demo`
flag, train/test time ranges, metrics) and a human-readable
`EVAL_REPORT.md` / `eval_report.json`.

Evaluation uses a **time-ordered 80/20 split** (never random) and always
compares against the naive baseline "the next order looks like the venue's
recent average". Per-venue tables are included in the report.

### Training on real captured logs

Every `/ingest` metric can be captured for future training:

```bash
python main.py --mode sim --exec-log logs/exec_metrics.jsonl   # capture a session
python train_exec_model.py --from-logs logs/exec_metrics.jsonl
```

Artifacts stay `is_demo=true` unless you add `--real-data`, which you should
only do when the logs came from real venue executions.

### The demo-model serving guard

`exec_ml/serving.py` decides at startup whether the artifacts may serve:

- **Real-data model** → serves, frames say `model_status: "real"`.
- **Demo model + `--mode sim`** → serves with a loud startup warning; every
  frame and panel says `model_status: "demo"`.
- **Demo model + non-sim pipeline** → **refused** unless
  `ALLOW_DEMO_EXEC_MODEL=true` is set explicitly (and still warns loudly).

Demo predictions are never silently presented as production-quality.

## Train/export the BTC reference model (optional)

```bash
python train_model.py
```

Defaults: `BTC-USD`, 60 days, 5-minute bars, and a 30-period feature window. Outputs:

- `models/btc_direction.onnx`
- `models/model_metadata.json`

Yahoo occasionally rate-limits or blocks downloads. For an offline smoke test only, the training script can fall back to deterministic synthetic history:

```bash
python train_model.py --allow-synthetic-fallback
```

The metadata records whether the resulting artifact used `yfinance` or the fallback.

## Run with no API keys

```bash
python main.py --mode sim
```

Then use:

- Health: `http://127.0.0.1:8000/health`
- Routing snapshot: `http://127.0.0.1:8000/predict_execution`
- WebSocket: `ws://127.0.0.1:8000/ws`

Once metrics flow in (see the ROS 2 feed section below), each completed
order triggers an `execution_prediction` broadcast:

```json
{
  "type": "execution_prediction",
  "data": {
    "generated_at": 1751500000.12,
    "model_status": "demo",
    "recommended_venue": "alpaca",
    "venues": [
      {
        "venue": "alpaca",
        "status": "ok",
        "predicted_slippage_bps": 2.31,
        "predicted_fill_probability": 0.94,
        "predicted_latency_ms_p95": 210.5,
        "routing_score": 89.4,
        "recommended": true,
        "orders_observed": 42
      }
    ],
    "validation": {
      "window": 100,
      "orders_scored": 37,
      "slippage_mae": 2.4,
      "slippage_baseline_mae": 2.9,
      "fill_accuracy": 0.86,
      "fill_brier": 0.12,
      "latency_p95_coverage": 0.91,
      "recommendation_hit_rate": 0.6,
      "recommendation_windows": 5
    }
  }
}
```

`GET /predict_execution` returns the same data plus model metadata (data
source, demo flag, offline test metrics), or HTTP 503 with a reason when no
routing model is loaded.

Example WebSocket message:

```json
{
  "symbol": "BTC/USD",
  "event_time": "2026-06-11T20:00:00+00:00",
  "published_at": "2026-06-11T20:00:00.004000+00:00",
  "source": "sim",
  "market_event": "trade",
  "latest_price": 65012.42,
  "prediction": "up",
  "confidence": 0.612345,
  "pipeline_latency_ms": 1.834,
  "features": {
    "rolling_return": 0.000103,
    "volatility": 0.000291,
    "momentum": 0.001847
  }
}
```

## Run against Alpaca crypto data

Create a free Alpaca account and set credentials:

```bash
# macOS/Linux
export ALPACA_API_KEY="your-key"
export ALPACA_API_SECRET="your-secret"
python main.py --mode alpaca
```

```powershell
# Windows PowerShell
$env:ALPACA_API_KEY="your-key"
$env:ALPACA_API_SECRET="your-secret"
python main.py --mode alpaca
```

The ingestion worker authenticates to Alpaca's crypto WebSocket and subscribes to both `BTC/USD` trades and quotes. Trade events use the trade price; quote events use bid/ask midpoint.

## Test end to end

```bash
pytest -q
```

The test launches `main.py` in sim mode, polls `/health`, connects to `/ws`, validates a live prediction payload, and shuts the process down.

## Useful options

```bash
python main.py --help
python train_model.py --help
```

Notable runtime options include `--sim-interval`, `--feature-window`, `--queue-size`, `--host`, and `--port`.

## React dashboard

The Vite/React dashboard lives in `frontend/` and connects to the existing FastAPI WebSocket stream. Use Node.js 20.19+ or 22.12+.

It contains:

- **ML venue selection panel** (`RoutingPanel`) — the routing model's
  next-order forecast per venue (expected slippage, fill probability, p95
  latency bound, routing score), the recommended venue, a predicted-vs-
  observed slippage chart, and a rolling live-validation strip (slippage MAE
  vs naive baseline, fill accuracy/Brier, latency coverage, best-venue hit
  rate — last 100 orders). A prominent banner marks demo models.
- **Execution panel** — observed venue metrics from ROS 2 reports (venue
  ranking, fill-rate gauges, percentile cards) with connection state,
  last-update time, and staleness marking.
- **Market context panel** — the BTC/USD reference feed with its next-bar
  signal explicitly labeled *reference only / near-random on price-only
  features*, plus a pipeline-latency histogram.
- One shared WebSocket (`frontend/src/lib/sharedSocket.ts`) carries
  prediction, venue-report, metric, and execution-prediction frames; each
  panel filters by message shape.

Install frontend dependencies:

```bash
cd frontend
npm install
```

Run the backend from the repository root:

```bash
python main.py --mode sim
```

In a second terminal, run the dashboard:

```bash
cd frontend
npm run dev
```

Open `http://127.0.0.1:5173`. Vite proxies `/ws` and `/health` to `http://127.0.0.1:8000` during development.

For a separately hosted backend, copy `frontend/.env.example` to `frontend/.env` and set:

```bash
VITE_WS_URL=wss://your-api-host/ws
```

Frontend checks:

```bash
cd frontend
npm test
npm run build
```

With both backend and Vite running, verify the proxied live stream:

```bash
npm run test:live
```

## ROS 2 execution-quality feed

The backend also accepts execution analyzer messages at `POST /ingest`:

- `{"type":"venue_report","data":{...VenueReport fields...}}` is validated,
  cached by venue, and rebroadcast on `/ws` for the execution panel.
- `{"type":"metric","data":{...MetricMsg fields...}}` is validated, counted,
  rebroadcast on `/ws`, appended to the `--exec-log` JSONL capture when
  enabled, and **fed to the routing model**: the venue's rolling features
  refresh, a new next-order prediction is made, the previous prediction is
  scored against this realized outcome, and an `execution_prediction` frame
  is broadcast.

Each accepted message is broadcast exactly once (plus at most one
execution-prediction frame per metric); malformed or unsupported envelopes
are rejected with HTTP 422.

The dashboard data sources listen to the shared `/ws` and filter by frame
type. Newly connected clients immediately receive the latest cached venue
reports and the latest execution-prediction frame.

## Integrated demo — full stack (Ubuntu 22.04 + ROS 2 Humble)

Runs the complete path end to end:

```text
ROS 2 analyzer → dashboard_bridge → POST /ingest ─┬→ /ws → React execution panel
                                                  └→ routing model → /ws → React routing panel
market sim → ONNX inference → /ws → React market-context panel
```

### Prerequisites

- **Ubuntu 22.04** (native, WSL2, or Docker) — required for ROS 2.
- **ROS 2 Humble Hawksbill** — https://docs.ros.org/en/humble/Installation.html
- **Python 3.10+** (3.11 recommended) for the FastAPI backend.
- **Node.js 20.19+ or 22.12+** for the Vite/React dashboard.
- The bridge talks to the backend over HTTP, so `exec_quality_ws` and
  `ai_viz_platform` can share one machine (this guide) or run on separate hosts.

> **No ROS 2 on this machine** (e.g. a Windows/macOS dev box)? Skip the ROS build
> and use the no-ROS bridge demo in [Troubleshooting](#troubleshooting). It drives
> the **real** analyzer node code over an in-process mock bus and POSTs live venue
> reports to `/ingest`, so the dashboard behaves exactly as under real ROS 2.

### 1. Install dependencies

**ROS 2 workspace** (Ubuntu 22.04 + ROS 2 Humble):

```bash
cd exec_quality_ws
source /opt/ros/humble/setup.bash
sudo apt install python3-colcon-common-extensions python3-numpy python3-requests
rosdep install --from-paths src --ignore-src -r -y
```

**FastAPI backend + ONNX model:**

```bash
cd ai_viz_platform
python -m venv .venv
source .venv/bin/activate                  # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python train_exec_model.py                 # routing model → models/exec_quality/
python train_model.py                      # optional BTC reference model (ONNX)
```

**React dashboard:**

```bash
cd ai_viz_platform/frontend
npm install
```

### 2. Build the ROS 2 workspace

```bash
cd exec_quality_ws
source /opt/ros/humble/setup.bash
colcon build --symlink-install
source install/setup.bash
```

`exec_quality_interfaces` (the custom messages) builds before
`exec_quality_analyzer` automatically.

### 3. Launch the integrated stack (three terminals)

```bash
# Terminal 1 — FastAPI backend + prediction simulator
cd ai_viz_platform
source .venv/bin/activate                  # Windows PowerShell: .venv\Scripts\Activate.ps1
python main.py --mode sim
# serves http://127.0.0.1:8000  (REST /health, /ingest; WebSocket /ws)
```

```bash
# Terminal 2 — ROS 2 analyzer + dashboard_bridge
cd exec_quality_ws
source /opt/ros/humble/setup.bash
source install/setup.bash
ros2 launch exec_quality_analyzer analyzer.launch.py
```

```bash
# Terminal 3 — React dashboard (Vite dev server)
cd ai_viz_platform/frontend
npm run dev
# serves http://127.0.0.1:5173  (proxies /ws and /health to :8000)
```

`dashboard_bridge` launches inside `analyzer.launch.py` and POSTs to
`http://127.0.0.1:8000/ingest` by default. To target a backend on another
host/port, override the `backend_ingest_url` launch argument:

```bash
ros2 launch exec_quality_analyzer analyzer.launch.py \
    backend_ingest_url:=http://your-api-host:8000/ingest
```

### 4. Open the dashboard

Open **http://127.0.0.1:5173** in a browser.

### What each panel shows

**Execution panel (top left) — "ROS 2 analyzer"**

- Header status pill reads **Live ROS 2** once venue reports arrive.
- **Recommended venue** callout with a composite quality score computed from
  *observed* metrics (the heuristic the ML panel's predictions refine).
- **Venue ranking** bar chart across `coinbase_sandbox`, `alpaca`, and
  `binance_testnet` (non-comparable venues stay visible but out of the ranking).
- **Fill-rate gauges** per venue, plus **slippage** and **latency** percentile
  cards (p50 / p95 / p99) for the top-ranked venue.
- `updated HH:MM:SS` reflects the most recent venue report.

**ML venue selection panel (top right) — "Routing model"**

- A **model-status banner**: amber `DEMO MODEL — trained on simulated
  executions` for demo artifacts, green `Real-data model` otherwise.
- **Model-recommended venue** callout with its routing score.
- **Predicted execution quality by venue** table: expected slippage (bps),
  fill probability, p95 latency bound, routing score; the chosen row carries
  a `route` badge. Venues with fewer than 10 observed orders show
  `warming up`.
- **Slippage — predicted vs observed p50** chart, joining model forecasts
  with the analyzer's realized venue reports.
- **Live validation strip** over the last 100 completed orders: slippage MAE
  (model vs naive recent-average baseline), fill prediction accuracy and
  Brier score, best-venue recommendation hit rate, latency p95 coverage.

**Market context panel (bottom) — "Reference feed"**

- Live **BTC/USD** price with up/down signal markers, a confidence gauge,
  and a rolling pipeline-latency histogram.
- The next-bar signal is explicitly labeled *reference only — near-random on
  price-only features — not used for routing*.

### Connection & feed status indicators

Both panels share one WebSocket to FastAPI `/ws`. The prediction pill shows the
raw socket state; the execution pill additionally reflects whether ROS 2 venue
reports are actually flowing (15 s staleness threshold).

| Indicator | Where | Meaning |
|---|---|---|
| **Connected** | Prediction pill (`connected`) | Browser ↔ FastAPI `/ws` is open; prediction frames are streaming. |
| **Live ROS 2** | Execution pill | Socket connected **and** a venue report arrived within the last 15 s — venues are ranking live. |
| **Awaiting ROS 2** | Execution pill | Socket connected to FastAPI, but **no** venue report received yet. FastAPI is up; the ROS analyzer / `dashboard_bridge` hasn't delivered a `venue_report` (not started, still warming up, or wrong `backend_ingest_url`). |
| **Reconnecting…** | Both pills | The shared `/ws` socket dropped (FastAPI stopped or restarted) and the client is retrying with exponential backoff (800 ms → 8 s). |
| **Stale** (`Stale — ROS 2 silent`) | Execution pill | Venue reports were arriving but none in the last 15 s (`STALE_AFTER_MS`). The aggregator reports ~every 5 s, so 3 missed windows means the analyzer or bridge stopped publishing while FastAPI is still up. |

### Troubleshooting

- **Execution panel stuck on "Awaiting ROS 2".** FastAPI is up but no venue
  reports are arriving. Confirm Terminal 2 is running and the bridge logged
  `connected: first message delivered to http://127.0.0.1:8000/ingest`. Verify
  `backend_ingest_url` matches the backend host/port, and that the Reporter
  table prints every ~10 s in Terminal 2.
- **Both panels show "Reconnecting…".** The backend isn't reachable. Start it
  with `python main.py --mode sim` and confirm `http://127.0.0.1:8000/health`
  returns JSON. If the dashboard is hosted separately from the API, set
  `VITE_WS_URL` (below).
- **Execution panel flips to "Stale — ROS 2 silent".** The analyzer/bridge
  stopped publishing. Restart
  `ros2 launch exec_quality_analyzer analyzer.launch.py`; predictions keep
  streaming independently.
- **No ROS 2 on this machine (`ros2: command not found`, Windows/macOS dev
  box).** Run the analyzer pipeline + bridge over the mock rclpy bus — real node
  code, real HTTP POSTs, no ROS install required:
  ```bash
  # Terminal 1: python main.py --mode sim          (ai_viz_platform, venv active)
  # Terminal 2:
  cd exec_quality_ws
  python scripts/no_ros_bridge_demo.py --ingest-url http://127.0.0.1:8000/ingest
  # Terminal 3: npm run dev                          (ai_viz_platform/frontend)
  ```
  This streams live reports for `alpaca`, `binance_testnet`, and
  `coinbase_sandbox`, so the execution panel reads **Live ROS 2** just like the
  real launch.
- **Routing panel stuck on "Awaiting routing predictions".** Either no
  routing model is loaded (train one with `python train_exec_model.py` and
  restart the backend — check the startup log and
  `GET /predict_execution`), or per-order metrics aren't arriving yet (the
  model needs 10 observed orders per venue before it predicts). Confirm the
  analyzer/bridge terminal is running.
- **Backend log says "refusing to serve a demo execution model".** You ran a
  non-sim pipeline with demo artifacts. Retrain from real captured logs
  (`python train_exec_model.py --from-logs ... --real-data`) or explicitly
  opt in with `ALLOW_DEMO_EXEC_MODEL=true`.
- **Market panel never leaves "WAITING" / empty price chart.** The reference
  model artifact is missing — run `python train_model.py` (or
  `python train_model.py --allow-synthetic-fallback` for an offline build)
  before starting the backend.
- **Dashboard hosted on a different origin than the API.** Copy
  `frontend/.env.example` to `frontend/.env` and set
  `VITE_WS_URL=wss://your-api-host/ws`.
- **Port already in use.** Change the backend port with
  `python main.py --mode sim --port 8001` (and match the bridge's
  `backend_ingest_url`), or run Vite elsewhere with `npm run dev -- --port 5174`.
