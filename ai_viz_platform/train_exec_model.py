"""Train the execution-quality (smart-order-routing) models.

Two data paths:

1. Real logs (preferred once you have them):
       python main.py --mode sim --exec-log logs/exec_metrics.jsonl   # capture
       python train_exec_model.py --from-logs logs/exec_metrics.jsonl
   Artifacts stay marked is_demo=true unless you add --real-data, which you
   should only do when the logs came from real venue executions.

2. No logs yet (default): generates a clearly-labeled simulated replay log
   (exec_ml.simulate) and trains on it. The artifacts are stamped
   data_source="simulated-replay", is_demo=true, and the backend's serving
   guard treats them accordingly.

Outputs (under --output-dir, default models/exec_quality/):
    slippage_model.joblib, fill_model.joblib, latency_model.joblib,
    exec_model_metadata.json, eval_report.json, EVAL_REPORT.md
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from exec_ml.dataset import build_dataset, dataset_from_logs
from exec_ml.simulate import DATA_SOURCE_LABEL, generate_metrics
from exec_ml.train import save_artifacts, train_and_evaluate

PROJECT_ROOT = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--from-logs",
        nargs="+",
        type=Path,
        default=None,
        help="JSONL metric logs captured by the backend (--exec-log) or the bridge.",
    )
    parser.add_argument(
        "--real-data",
        action="store_true",
        help="Mark the artifacts as real-data trained. Only use when --from-logs "
        "points at logs of real venue executions.",
    )
    parser.add_argument("--orders", type=int, default=30_000, help="Simulated orders (demo path).")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--test-fraction", type=float, default=0.2)
    parser.add_argument(
        "--output-dir", type=Path, default=PROJECT_ROOT / "models" / "exec_quality"
    )
    parser.add_argument(
        "--dataset-csv",
        type=Path,
        default=None,
        help="Optionally also write the built dataset for inspection.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.real_data and not args.from_logs:
        print("--real-data requires --from-logs (simulated data is never 'real')", file=sys.stderr)
        return 2

    if args.from_logs:
        frame = dataset_from_logs(args.from_logs)
        data_source = "ingest-logs"
        source_detail = ", ".join(str(p) for p in args.from_logs)
        is_demo = not args.real_data
    else:
        print(
            f"no logs supplied — generating a SIMULATED demo replay of {args.orders} orders "
            "(artifacts will be stamped is_demo=true)",
            file=sys.stderr,
        )
        frame = build_dataset(generate_metrics(orders=args.orders, seed=args.seed))
        data_source = DATA_SOURCE_LABEL
        source_detail = f"exec_ml.simulate, orders={args.orders}, seed={args.seed}"
        is_demo = True

    if frame.empty:
        print("dataset is empty — nothing to train on", file=sys.stderr)
        return 1

    if args.dataset_csv:
        args.dataset_csv.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(args.dataset_csv, index=False)

    trained = train_and_evaluate(
        frame,
        data_source=data_source,
        is_demo=is_demo,
        test_fraction=args.test_fraction,
        source_detail=source_detail,
    )
    paths = save_artifacts(trained, args.output_dir)

    summary = {
        "artifacts": {name: str(path) for name, path in paths.items()},
        "data_source": data_source,
        "is_demo": is_demo,
        "rows": trained.metadata["rows"],
        "slippage_bps": trained.evaluation["slippage_bps"],
        "fill_probability": (
            None
            if trained.evaluation["fill_probability"] is None
            else trained.evaluation["fill_probability"]["model"]
        ),
        "latency_ms_p95": {
            "model_coverage": trained.evaluation["latency_ms_p95"]["model_coverage"]
        },
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
