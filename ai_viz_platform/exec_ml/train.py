"""Train and evaluate the execution-quality models.

Three small, explainable sklearn models over the shared feature set:

- slippage:  HistGradientBoostingRegressor  -> expected slippage_bps of the
             next order at a venue
- fill:      HistGradientBoostingClassifier -> probability the next order
             fully fills
- latency:   HistGradientBoostingRegressor (quantile 0.95) -> p95 latency
             bound for the next order

Evaluation is strictly time-ordered (train on the first part of the stream,
test on the last part — never a random shuffle) and is always compared to a
naive baseline: "assume the next order matches the venue's recent average".
If the model cannot beat that baseline, the report says so.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    mean_absolute_error,
    mean_squared_error,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from .dataset import TARGET_FILLED, TARGET_LATENCY, TARGET_SLIPPAGE
from .features import (
    BASELINE_FILL_COLUMN,
    BASELINE_LATENCY_COLUMN,
    BASELINE_SLIPPAGE_COLUMN,
    FEATURE_COLUMNS,
    VENUE_COLUMN,
)

SLIPPAGE_MODEL_FILE = "slippage_model.joblib"
FILL_MODEL_FILE = "fill_model.joblib"
LATENCY_MODEL_FILE = "latency_model.joblib"
METADATA_FILE = "exec_model_metadata.json"
EVAL_JSON_FILE = "eval_report.json"
EVAL_MARKDOWN_FILE = "EVAL_REPORT.md"

LATENCY_QUANTILE = 0.95


def _encoder() -> ColumnTransformer:
    return ColumnTransformer(
        [
            (
                "venue",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                [VENUE_COLUMN],
            )
        ],
        remainder="passthrough",
    )


def make_slippage_model() -> Pipeline:
    return Pipeline(
        [
            ("encode", _encoder()),
            ("model", HistGradientBoostingRegressor(max_iter=200, learning_rate=0.08, random_state=42)),
        ]
    )


def make_fill_model() -> Pipeline:
    return Pipeline(
        [
            ("encode", _encoder()),
            ("model", HistGradientBoostingClassifier(max_iter=200, learning_rate=0.08, random_state=42)),
        ]
    )


def make_latency_model() -> Pipeline:
    return Pipeline(
        [
            ("encode", _encoder()),
            (
                "model",
                HistGradientBoostingRegressor(
                    loss="quantile",
                    quantile=LATENCY_QUANTILE,
                    max_iter=200,
                    learning_rate=0.08,
                    random_state=42,
                ),
            ),
        ]
    )


def time_split(frame: pd.DataFrame, test_fraction: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Chronological split: the test set is strictly after the train set."""
    if not 0.0 < test_fraction < 1.0:
        raise ValueError("test_fraction must be in (0, 1)")
    ordered = frame.sort_values("timestamp", kind="stable").reset_index(drop=True)
    boundary = int(len(ordered) * (1.0 - test_fraction))
    if boundary <= 0 or boundary >= len(ordered):
        raise ValueError(f"dataset too small to split: {len(ordered)} rows")
    return ordered.iloc[:boundary], ordered.iloc[boundary:]


@dataclass
class TrainedModels:
    slippage: Pipeline
    fill: Pipeline | None
    latency: Pipeline
    metadata: dict[str, Any]
    evaluation: dict[str, Any]


def _regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
    }


def _classification_metrics(y_true: np.ndarray, probabilities: np.ndarray) -> dict[str, float | None]:
    both_classes = len(np.unique(y_true)) == 2
    return {
        "roc_auc": float(roc_auc_score(y_true, probabilities)) if both_classes else None,
        "accuracy": float(accuracy_score(y_true, probabilities >= 0.5)),
        "brier": float(brier_score_loss(y_true, np.clip(probabilities, 0.0, 1.0))),
    }


def train_and_evaluate(
    frame: pd.DataFrame,
    data_source: str,
    is_demo: bool,
    test_fraction: float = 0.2,
    source_detail: str | None = None,
) -> TrainedModels:
    for column in (*FEATURE_COLUMNS, TARGET_SLIPPAGE, TARGET_FILLED, "timestamp"):
        if column not in frame.columns:
            raise ValueError(f"dataset is missing column: {column}")

    train_df, test_df = time_split(frame, test_fraction)

    # --- slippage regression (executed orders only) -------------------------
    slip_train = train_df.dropna(subset=[TARGET_SLIPPAGE])
    slip_test = test_df.dropna(subset=[TARGET_SLIPPAGE])
    if len(slip_train) < 50 or len(slip_test) < 20:
        raise ValueError("not enough executed orders to train the slippage model")

    slippage_model = make_slippage_model()
    slippage_model.fit(slip_train[FEATURE_COLUMNS], slip_train[TARGET_SLIPPAGE])
    slip_pred = slippage_model.predict(slip_test[FEATURE_COLUMNS])
    slip_true = slip_test[TARGET_SLIPPAGE].to_numpy()
    slip_baseline = slip_test[BASELINE_SLIPPAGE_COLUMN].to_numpy()

    slippage_eval = {
        "model": _regression_metrics(slip_true, slip_pred),
        "baseline_recent_venue_average": _regression_metrics(slip_true, slip_baseline),
        "test_rows": int(len(slip_test)),
    }
    slippage_eval["mae_improvement_vs_baseline_pct"] = float(
        100.0
        * (slippage_eval["baseline_recent_venue_average"]["mae"] - slippage_eval["model"]["mae"])
        / max(slippage_eval["baseline_recent_venue_average"]["mae"], 1e-12)
    )

    # --- fill classification (only if both outcomes appear in training) -----
    fill_model: Pipeline | None = None
    fill_eval: dict[str, Any] | None = None
    fill_train_y = train_df[TARGET_FILLED].astype(int)
    if fill_train_y.nunique() == 2:
        fill_model = make_fill_model()
        fill_model.fit(train_df[FEATURE_COLUMNS], fill_train_y)
        fill_prob = fill_model.predict_proba(test_df[FEATURE_COLUMNS])[:, 1]
        fill_true = test_df[TARGET_FILLED].astype(int).to_numpy()
        fill_baseline = np.clip(test_df[BASELINE_FILL_COLUMN].to_numpy(), 0.0, 1.0)
        fill_eval = {
            "model": _classification_metrics(fill_true, fill_prob),
            "baseline_recent_fill_rate": _classification_metrics(fill_true, fill_baseline),
            "positive_rate": float(fill_true.mean()),
            "test_rows": int(len(test_df)),
        }

    # --- latency p95 quantile regression ------------------------------------
    lat_train = train_df.dropna(subset=[TARGET_LATENCY])
    lat_test = test_df.dropna(subset=[TARGET_LATENCY])
    latency_model = make_latency_model()
    latency_model.fit(lat_train[FEATURE_COLUMNS], lat_train[TARGET_LATENCY])
    lat_pred = latency_model.predict(lat_test[FEATURE_COLUMNS])
    lat_true = lat_test[TARGET_LATENCY].to_numpy()
    lat_baseline = lat_test[BASELINE_LATENCY_COLUMN].to_numpy()
    latency_eval = {
        # For a p95 bound, "coverage" (share of orders at or under the bound)
        # should sit near 0.95; closer beats a tighter-but-wrong bound.
        "model_coverage": float(np.mean(lat_true <= lat_pred)),
        "baseline_rolling_p95_coverage": float(np.mean(lat_true <= lat_baseline)),
        "target_coverage": LATENCY_QUANTILE,
        "model_mean_bound_ms": float(np.mean(lat_pred)),
        "baseline_mean_bound_ms": float(np.mean(lat_baseline)),
        "test_rows": int(len(lat_test)),
    }

    # --- per-venue breakdown -------------------------------------------------
    per_venue: dict[str, dict[str, Any]] = {}
    for venue, venue_df in test_df.groupby(VENUE_COLUMN):
        venue_slip = venue_df.dropna(subset=[TARGET_SLIPPAGE])
        entry: dict[str, Any] = {"test_rows": int(len(venue_df))}
        if len(venue_slip) >= 5:
            pred = slippage_model.predict(venue_slip[FEATURE_COLUMNS])
            true = venue_slip[TARGET_SLIPPAGE].to_numpy()
            entry["slippage_mae"] = float(mean_absolute_error(true, pred))
            entry["slippage_baseline_mae"] = float(
                mean_absolute_error(true, venue_slip[BASELINE_SLIPPAGE_COLUMN])
            )
            entry["realized_mean_slippage_bps"] = float(true.mean())
        if fill_model is not None:
            prob = fill_model.predict_proba(venue_df[FEATURE_COLUMNS])[:, 1]
            true_fill = venue_df[TARGET_FILLED].astype(int).to_numpy()
            entry["fill_accuracy"] = float(accuracy_score(true_fill, prob >= 0.5))
            entry["realized_fill_rate"] = float(true_fill.mean())
        per_venue[str(venue)] = entry

    def _iso(ts: float) -> str:
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    evaluation = {
        "split": f"time-ordered {int((1 - test_fraction) * 100)}/{int(test_fraction * 100)} (no shuffling)",
        "slippage_bps": slippage_eval,
        "fill_probability": fill_eval,
        "latency_ms_p95": latency_eval,
        "per_venue": per_venue,
    }

    metadata = {
        "model_family": "sklearn HistGradientBoosting (regression + classification + p95 quantile)",
        "targets": {
            "slippage_bps": "expected slippage of the next order at the venue",
            "fill_probability": None if fill_model is None else "probability the next order fully fills",
            "latency_ms_p95": "p95 latency bound for the next order",
        },
        "feature_columns": FEATURE_COLUMNS,
        "baselines": {
            "slippage_bps": BASELINE_SLIPPAGE_COLUMN,
            "fill_probability": BASELINE_FILL_COLUMN,
            "latency_ms_p95": BASELINE_LATENCY_COLUMN,
        },
        "data_source": data_source,
        "source_detail": source_detail,
        "is_demo": bool(is_demo),
        "rows": int(len(frame)),
        "train_rows": int(len(train_df)),
        "test_rows": int(len(test_df)),
        "train_time_range": [_iso(train_df["timestamp"].min()), _iso(train_df["timestamp"].max())],
        "test_time_range": [_iso(test_df["timestamp"].min()), _iso(test_df["timestamp"].max())],
        "venues": sorted(frame[VENUE_COLUMN].unique().tolist()),
        "metrics": evaluation,
        "sklearn_version": sklearn.__version__,
        "trained_at": datetime.now(timezone.utc).isoformat(),
    }

    return TrainedModels(
        slippage=slippage_model,
        fill=fill_model,
        latency=latency_model,
        metadata=metadata,
        evaluation=evaluation,
    )


def save_artifacts(trained: TrainedModels, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "slippage": output_dir / SLIPPAGE_MODEL_FILE,
        "latency": output_dir / LATENCY_MODEL_FILE,
        "metadata": output_dir / METADATA_FILE,
        "eval_json": output_dir / EVAL_JSON_FILE,
        "eval_markdown": output_dir / EVAL_MARKDOWN_FILE,
    }
    joblib.dump(trained.slippage, paths["slippage"])
    joblib.dump(trained.latency, paths["latency"])
    if trained.fill is not None:
        paths["fill"] = output_dir / FILL_MODEL_FILE
        joblib.dump(trained.fill, paths["fill"])
    paths["metadata"].write_text(json.dumps(trained.metadata, indent=2), encoding="utf-8")
    paths["eval_json"].write_text(json.dumps(trained.evaluation, indent=2), encoding="utf-8")
    paths["eval_markdown"].write_text(render_markdown_report(trained.metadata), encoding="utf-8")
    return paths


def render_markdown_report(metadata: dict[str, Any]) -> str:
    evaluation = metadata["metrics"]
    slippage = evaluation["slippage_bps"]
    fill = evaluation["fill_probability"]
    latency = evaluation["latency_ms_p95"]

    lines = [
        "# Execution-Quality Model — Evaluation Report",
        "",
        f"- **Trained at:** {metadata['trained_at']}",
        f"- **Data source:** `{metadata['data_source']}`"
        + (f" ({metadata['source_detail']})" if metadata.get("source_detail") else ""),
        f"- **Demo model:** {'yes — trained on simulated executions, never presented as production-quality' if metadata['is_demo'] else 'no'}",
        f"- **Split:** {evaluation['split']}",
        f"- **Rows:** {metadata['rows']} total → {metadata['train_rows']} train / {metadata['test_rows']} test",
        f"- **Train window:** {metadata['train_time_range'][0]} → {metadata['train_time_range'][1]}",
        f"- **Test window:** {metadata['test_time_range'][0]} → {metadata['test_time_range'][1]}",
        "",
        "## Slippage (bps) — next-order regression",
        "",
        "| | MAE | RMSE |",
        "|---|---|---|",
        f"| Model | {slippage['model']['mae']:.3f} | {slippage['model']['rmse']:.3f} |",
        f"| Baseline (recent venue average) | {slippage['baseline_recent_venue_average']['mae']:.3f} | {slippage['baseline_recent_venue_average']['rmse']:.3f} |",
        "",
        f"MAE improvement vs baseline: **{slippage['mae_improvement_vs_baseline_pct']:.1f}%**"
        f" on {slippage['test_rows']} held-out orders.",
        "",
    ]

    if fill is not None:
        auc = fill["model"]["roc_auc"]
        base_auc = fill["baseline_recent_fill_rate"]["roc_auc"]
        lines += [
            "## Fill probability — next-order classification",
            "",
            "| | ROC-AUC | Accuracy | Brier |",
            "|---|---|---|---|",
            f"| Model | {auc:.3f} | {fill['model']['accuracy']:.3f} | {fill['model']['brier']:.4f} |"
            if auc is not None
            else f"| Model | n/a | {fill['model']['accuracy']:.3f} | {fill['model']['brier']:.4f} |",
            f"| Baseline (recent fill rate) | {base_auc:.3f} | {fill['baseline_recent_fill_rate']['accuracy']:.3f} | {fill['baseline_recent_fill_rate']['brier']:.4f} |"
            if base_auc is not None
            else f"| Baseline (recent fill rate) | n/a | {fill['baseline_recent_fill_rate']['accuracy']:.3f} | {fill['baseline_recent_fill_rate']['brier']:.4f} |",
            "",
            f"Test positive (filled) rate: {fill['positive_rate']:.3f}. With fills this common,",
            "accuracy is dominated by the majority class — Brier score and ROC-AUC are the",
            "honest comparison.",
            "",
        ]

    lines += [
        "## Latency p95 bound — quantile regression",
        "",
        f"Share of orders at or under the predicted bound (target {latency['target_coverage']:.2f}):",
        "",
        "| | Coverage | Mean bound (ms) |",
        "|---|---|---|",
        f"| Model | {latency['model_coverage']:.3f} | {latency['model_mean_bound_ms']:.1f} |",
        f"| Baseline (rolling p95) | {latency['baseline_rolling_p95_coverage']:.3f} | {latency['baseline_mean_bound_ms']:.1f} |",
        "",
        "## Per-venue (held-out test period)",
        "",
        "| Venue | Test rows | Slippage MAE (model) | Slippage MAE (baseline) | Fill accuracy | Realized fill rate |",
        "|---|---|---|---|---|---|",
    ]
    for venue, entry in sorted(evaluation["per_venue"].items()):
        slip_mae = f"{entry['slippage_mae']:.3f}" if "slippage_mae" in entry else "—"
        slip_base = f"{entry['slippage_baseline_mae']:.3f}" if "slippage_baseline_mae" in entry else "—"
        fill_acc = f"{entry['fill_accuracy']:.3f}" if "fill_accuracy" in entry else "—"
        fill_rate = f"{entry['realized_fill_rate']:.3f}" if "realized_fill_rate" in entry else "—"
        lines.append(
            f"| {venue} | {entry.get('test_rows', '—')} | {slip_mae} | {slip_base} | {fill_acc} | {fill_rate} |"
        )

    lines += [
        "",
        "---",
        "",
        "*This model predicts execution quality (venue-level slippage, fill",
        "probability, latency) — it does not predict market direction. Numbers",
        "above come from a strictly time-ordered held-out split and are only as",
        "meaningful as the underlying data source stated at the top.*",
        "",
    ]
    return "\n".join(lines)
