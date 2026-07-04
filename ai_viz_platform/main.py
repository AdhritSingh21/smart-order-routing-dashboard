from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
from contextlib import suppress
from pathlib import Path

import uvicorn

from exec_ml.serving import load_exec_bundle, resolve_serving_decision
from pipeline.app import MetricJsonlLog, create_app
from pipeline.broadcast import ConnectionManager, prediction_broadcaster
from pipeline.exec_predictor import ExecutionQualityPredictor
from pipeline.features import feature_engineering_worker
from pipeline.inference import OnnxDirectionModel, inference_worker
from pipeline.ingestion import alpaca_ingestion, simulated_ingestion
from pipeline.state import RuntimeState

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_MODEL_PATH = PROJECT_ROOT / "models" / "btc_direction.onnx"
DEFAULT_METADATA_PATH = PROJECT_ROOT / "models" / "model_metadata.json"
DEFAULT_EXEC_MODEL_DIR = PROJECT_ROOT / "models" / "exec_quality"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the asyncio ML market-data pipeline.")
    parser.add_argument(
        "--mode",
        choices=("sim", "alpaca"),
        default=os.getenv("PIPELINE_MODE", "sim"),
    )
    parser.add_argument("--host", default=os.getenv("HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8000")))
    parser.add_argument("--symbol", default=os.getenv("SYMBOL", "BTC/USD"))
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL_PATH)
    parser.add_argument("--metadata", type=Path, default=DEFAULT_METADATA_PATH)
    parser.add_argument("--feature-window", type=int, default=None)
    parser.add_argument(
        "--queue-size", type=int, default=int(os.getenv("QUEUE_SIZE", "2048"))
    )
    parser.add_argument(
        "--sim-interval",
        type=float,
        default=float(os.getenv("SIM_INTERVAL", "0.1")),
    )
    parser.add_argument(
        "--sim-start-price",
        type=float,
        default=float(os.getenv("SIM_START_PRICE", "65000")),
    )
    parser.add_argument("--sim-seed", type=int, default=42)
    parser.add_argument(
        "--run-seconds",
        type=float,
        default=None,
        help="Optional auto-stop timer, useful for smoke tests.",
    )
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "info"))
    parser.add_argument(
        "--exec-model-dir",
        type=Path,
        default=DEFAULT_EXEC_MODEL_DIR,
        help="Directory with execution-quality model artifacts (train_exec_model.py).",
    )
    parser.add_argument(
        "--exec-log",
        type=Path,
        default=os.getenv("EXEC_LOG") or None,
        help="Append ingested per-order metrics to this JSONL file for future "
        "training (train_exec_model.py --from-logs).",
    )
    return parser.parse_args()


def build_exec_predictor(
    args: argparse.Namespace,
    state: RuntimeState,
    manager: ConnectionManager,
) -> ExecutionQualityPredictor | None:
    """Load the routing model behind the demo-model serving guard.

    A missing model disables predictions quietly; a demo model on a live
    pipeline is refused unless ALLOW_DEMO_EXEC_MODEL=true; a demo model is
    always announced loudly and labeled model_status="demo" downstream.
    """
    logger = logging.getLogger("exec_model")
    try:
        bundle = load_exec_bundle(args.exec_model_dir.resolve())
    except FileNotFoundError as exc:
        state.exec_model_disabled_reason = str(exc)
        logger.info("execution-quality predictions disabled: %s", exc)
        return None

    decision = resolve_serving_decision(bundle.is_demo, args.mode)
    for warning in decision.warnings:
        banner = "=" * 72
        logger.warning("\n%s\n%s\n%s", banner, warning, banner)
    if not decision.serve:
        state.exec_model_disabled_reason = decision.reason
        logger.error("execution-quality predictions disabled: %s", decision.reason)
        return None

    state.exec_model_status = decision.model_status
    logger.info(
        "execution-quality model loaded (%s, data_source=%s, trained_at=%s)",
        decision.model_status,
        bundle.data_source,
        bundle.metadata.get("trained_at"),
    )
    return ExecutionQualityPredictor(
        bundle, state, manager, model_status=decision.model_status or "demo"
    )


def load_feature_window(metadata_path: Path, override: int | None) -> int:
    metadata_window = 30
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata_window = int(metadata.get("window", metadata_window))
    if override is not None and override != metadata_window:
        logging.getLogger(__name__).warning(
            "Feature window override (%s) differs from model training window (%s)",
            override,
            metadata_window,
        )
    return override if override is not None else metadata_window


async def run_pipeline(args: argparse.Namespace) -> None:
    stop_event = asyncio.Event()
    state = RuntimeState(mode=args.mode, model_path=str(args.model.resolve()))
    tick_queue: asyncio.Queue = asyncio.Queue(maxsize=args.queue_size)
    feature_queue: asyncio.Queue = asyncio.Queue(maxsize=args.queue_size)
    prediction_queue: asyncio.Queue = asyncio.Queue(maxsize=args.queue_size)
    state.tick_queue = tick_queue
    state.feature_queue = feature_queue
    state.prediction_queue = prediction_queue

    manager = ConnectionManager(state)
    exec_predictor = build_exec_predictor(args, state, manager)
    metric_log = MetricJsonlLog(args.exec_log) if args.exec_log else None
    app = create_app(state, manager, exec_predictor=exec_predictor, metric_log=metric_log)
    model = OnnxDirectionModel(args.model.resolve(), args.metadata.resolve())
    feature_window = load_feature_window(args.metadata.resolve(), args.feature_window)

    if args.mode == "sim":
        ingestion_coroutine = simulated_ingestion(
            tick_queue=tick_queue,
            state=state,
            stop_event=stop_event,
            symbol=args.symbol,
            interval_seconds=args.sim_interval,
            start_price=args.sim_start_price,
            seed=args.sim_seed,
        )
    else:
        ingestion_coroutine = alpaca_ingestion(
            tick_queue=tick_queue,
            state=state,
            stop_event=stop_event,
            symbol=args.symbol,
            api_key=os.getenv("ALPACA_API_KEY", ""),
            api_secret=os.getenv("ALPACA_API_SECRET", ""),
        )

    workers = [
        asyncio.create_task(ingestion_coroutine, name="ingestion"),
        asyncio.create_task(
            feature_engineering_worker(
                tick_queue, feature_queue, stop_event, feature_window
            ),
            name="features",
        ),
        asyncio.create_task(
            inference_worker(feature_queue, prediction_queue, model, state, stop_event),
            name="inference",
        ),
        asyncio.create_task(
            prediction_broadcaster(prediction_queue, manager, state, stop_event),
            name="broadcaster",
        ),
    ]

    config = uvicorn.Config(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        access_log=False,
        loop="asyncio",
    )
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve(), name="api-server")

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, lambda: setattr(server, "should_exit", True))

    timer_task: asyncio.Task | None = None
    if args.run_seconds is not None:
        async def stop_after_delay() -> None:
            await asyncio.sleep(args.run_seconds)
            server.should_exit = True

        timer_task = asyncio.create_task(stop_after_delay(), name="auto-stop")

    try:
        await server_task
    finally:
        state.stopping = True
        stop_event.set()
        for task in workers:
            task.cancel()
        if timer_task:
            timer_task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        if timer_task:
            await asyncio.gather(timer_task, return_exceptions=True)
        if metric_log is not None:
            metric_log.close()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(run_pipeline(args))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
