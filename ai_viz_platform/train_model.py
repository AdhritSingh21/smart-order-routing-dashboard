from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

FEATURE_NAMES = ["rolling_return", "volatility", "momentum"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train and export a BTC next-period direction classifier."
    )
    parser.add_argument("--ticker", default="BTC-USD")
    parser.add_argument("--period", default="60d")
    parser.add_argument("--interval", default="5m")
    parser.add_argument("--window", type=int, default=30)
    parser.add_argument("--test-fraction", type=float, default=0.2)
    parser.add_argument("--output-dir", type=Path, default=Path("models"))
    parser.add_argument(
        "--allow-synthetic-fallback",
        action="store_true",
        help="Use a deterministic synthetic history only if Yahoo download fails.",
    )
    return parser.parse_args()


def download_close(ticker: str, period: str, interval: str) -> pd.Series:
    data = yf.download(
        ticker,
        period=period,
        interval=interval,
        auto_adjust=True,
        progress=False,
        threads=False,
    )
    if data.empty:
        raise RuntimeError("yfinance returned no rows")

    close = data["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    close = close.astype(float).dropna()
    if len(close) < 200:
        raise RuntimeError(f"not enough rows from yfinance: {len(close)}")
    close.name = "close"
    return close


def synthetic_close(rows: int = 10_000, seed: int = 42) -> pd.Series:
    rng = np.random.default_rng(seed)
    returns = rng.normal(0.00001, 0.0025, rows)
    close = 60_000.0 * np.exp(np.cumsum(returns))
    return pd.Series(close, name="close")


def make_dataset(close: pd.Series, window: int) -> tuple[pd.DataFrame, pd.Series]:
    if window < 3:
        raise ValueError("window must be at least 3")

    log_return = np.log(close).diff()
    future_close = close.shift(-1)
    frame = pd.DataFrame(
        {
            "rolling_return": log_return,
            "volatility": log_return.rolling(window).std(),
            "momentum": close / close.shift(window) - 1.0,
            "target": (future_close > close).astype(int),
            "future_available": future_close.notna(),
        }
    )
    frame = frame[frame["future_available"]].dropna()
    if len(frame) < 100:
        raise RuntimeError(f"not enough usable training rows: {len(frame)}")
    return frame[FEATURE_NAMES].astype(np.float32), frame["target"].astype(int)


def main() -> int:
    args = parse_args()
    data_source = "yfinance"
    try:
        close = download_close(args.ticker, args.period, args.interval)
    except Exception as exc:
        if not args.allow_synthetic_fallback:
            raise
        print(f"yfinance download failed ({exc}); using synthetic fallback", file=sys.stderr)
        close = synthetic_close()
        data_source = "synthetic-fallback"

    x, y = make_dataset(close, args.window)
    split_index = int(len(x) * (1.0 - args.test_fraction))
    if split_index <= 0 or split_index >= len(x):
        raise ValueError("test fraction leaves an empty train or test set")
    x_train, x_test = x.iloc[:split_index], x.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]

    classifier = LogisticRegression(
        max_iter=1000,
        class_weight="balanced",
        random_state=42,
    )
    model = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("classifier", classifier),
        ]
    )
    model.fit(x_train, y_train)

    predicted = model.predict(x_test)
    probabilities = model.predict_proba(x_test)[:, 1]
    metrics: dict[str, float | None] = {
        "accuracy": float(accuracy_score(y_test, predicted)),
        "log_loss": float(log_loss(y_test, model.predict_proba(x_test))),
        "roc_auc": (
            float(roc_auc_score(y_test, probabilities))
            if y_test.nunique() == 2
            else None
        ),
    }

    onnx_model = convert_sklearn(
        model,
        name="btc_next_period_direction",
        initial_types=[("features", FloatTensorType([None, len(FEATURE_NAMES)]))],
        target_opset=17,
        options={id(classifier): {"zipmap": False}},
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    model_path = args.output_dir / "btc_direction.onnx"
    metadata_path = args.output_dir / "model_metadata.json"
    model_path.write_bytes(onnx_model.SerializeToString())
    metadata = {
        "model_type": "LogisticRegression direction classifier",
        "ticker": args.ticker,
        "period": args.period,
        "interval": args.interval,
        "window": args.window,
        "feature_names": FEATURE_NAMES,
        "classes": [int(value) for value in model.classes_.tolist()],
        "rows": int(len(x)),
        "train_rows": int(len(x_train)),
        "test_rows": int(len(x_test)),
        "data_source": data_source,
        "metrics": metrics,
        "trained_at": datetime.now(timezone.utc).isoformat(),
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    print(json.dumps({"model": str(model_path), "metadata": metadata}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
