"""Load execution-quality model artifacts and decide whether they may serve.

The guard exists so a demo model (trained on simulated executions) is never
silently presented as production-quality:

- real-data model          -> serve, model_status "real"
- demo model, sim pipeline -> serve, model_status "demo" (the whole pipeline
                              is a simulation; the status is surfaced end to
                              end, including on the dashboard)
- demo model, live pipeline-> refused unless ALLOW_DEMO_EXEC_MODEL=true is
                              set explicitly; a loud warning is logged either
                              way
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import joblib
import pandas as pd

from .features import FEATURE_COLUMNS
from .train import FILL_MODEL_FILE, LATENCY_MODEL_FILE, METADATA_FILE, SLIPPAGE_MODEL_FILE

DEMO_ENV_FLAG = "ALLOW_DEMO_EXEC_MODEL"
_TRUTHY = {"1", "true", "yes", "on"}


@dataclass
class ExecModelBundle:
    slippage_model: Any
    latency_model: Any
    fill_model: Any | None
    metadata: dict[str, Any]
    model_dir: Path

    @property
    def is_demo(self) -> bool:
        return bool(self.metadata.get("is_demo", True))

    @property
    def data_source(self) -> str:
        return str(self.metadata.get("data_source", "unknown"))

    def predict_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, float | None]]:
        """Predict all targets for a batch of feature rows (one per venue)."""
        if not rows:
            return []
        frame = pd.DataFrame(rows, columns=FEATURE_COLUMNS)
        slippage = self.slippage_model.predict(frame)
        latency = self.latency_model.predict(frame)
        fill = (
            self.fill_model.predict_proba(frame)[:, 1]
            if self.fill_model is not None
            else [None] * len(rows)
        )
        return [
            {
                "predicted_slippage_bps": float(slippage[i]),
                "predicted_latency_ms_p95": float(latency[i]),
                "predicted_fill_probability": None if fill[i] is None else float(fill[i]),
            }
            for i in range(len(rows))
        ]


def load_exec_bundle(model_dir: Path) -> ExecModelBundle:
    """Load artifacts written by exec_ml.train.save_artifacts.

    Raises FileNotFoundError when the directory or required files are absent
    (callers treat that as "predictor not trained yet", not a crash).
    """
    metadata_path = model_dir / METADATA_FILE
    slippage_path = model_dir / SLIPPAGE_MODEL_FILE
    latency_path = model_dir / LATENCY_MODEL_FILE
    for required in (metadata_path, slippage_path, latency_path):
        if not required.exists():
            raise FileNotFoundError(
                f"execution-quality model artifact missing: {required}. "
                "Train one with: python train_exec_model.py"
            )

    fill_path = model_dir / FILL_MODEL_FILE
    return ExecModelBundle(
        slippage_model=joblib.load(slippage_path),
        latency_model=joblib.load(latency_path),
        fill_model=joblib.load(fill_path) if fill_path.exists() else None,
        metadata=json.loads(metadata_path.read_text(encoding="utf-8")),
        model_dir=model_dir,
    )


@dataclass
class ServingDecision:
    serve: bool
    model_status: str | None  # "real" | "demo" | None when refused
    reason: str
    warnings: list[str] = field(default_factory=list)


def resolve_serving_decision(
    is_demo: bool,
    pipeline_mode: str,
    env: Mapping[str, str] | None = None,
) -> ServingDecision:
    env = os.environ if env is None else env
    if not is_demo:
        return ServingDecision(True, "real", "real-data model")

    demo_warning = (
        "EXECUTION-QUALITY MODEL IS A DEMO ARTIFACT: it was trained on "
        "simulated execution logs (metadata is_demo=true). Its predictions "
        "are for demonstration only and are labeled model_status='demo' on "
        "every frame and panel."
    )
    if pipeline_mode == "sim":
        return ServingDecision(
            True,
            "demo",
            "demo model serving a simulated pipeline",
            [demo_warning],
        )

    allowed = env.get(DEMO_ENV_FLAG, "").strip().lower() in _TRUTHY
    if allowed:
        return ServingDecision(
            True,
            "demo",
            f"demo model explicitly allowed via {DEMO_ENV_FLAG}",
            [
                demo_warning,
                f"{DEMO_ENV_FLAG} is set: serving DEMO predictions alongside a "
                f"live '{pipeline_mode}' pipeline. Do not treat routing "
                "recommendations as production advice.",
            ],
        )
    return ServingDecision(
        False,
        None,
        (
            f"refusing to serve a demo execution model in '{pipeline_mode}' mode. "
            f"Train on real logs (python train_exec_model.py --from-logs ... --real-data) "
            f"or set {DEMO_ENV_FLAG}=true to explicitly allow demo predictions."
        ),
    )
