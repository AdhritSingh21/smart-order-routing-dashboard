"""Tests for model artifact loading and the demo-model serving guard."""
from __future__ import annotations

import pytest

from exec_ml.dataset import build_dataset
from exec_ml.serving import (
    DEMO_ENV_FLAG,
    load_exec_bundle,
    resolve_serving_decision,
)
from exec_ml.simulate import generate_metrics


def test_load_bundle_roundtrip(trained_bundle_dir):
    bundle = load_exec_bundle(trained_bundle_dir)
    assert bundle.is_demo is True
    assert bundle.data_source == "simulated-replay"
    assert bundle.metadata["metrics"]["slippage_bps"]["model"]["mae"] > 0

    rows = build_dataset(generate_metrics(orders=200, seed=3))
    prediction = bundle.predict_rows([rows.iloc[0].to_dict()])[0]
    assert isinstance(prediction["predicted_slippage_bps"], float)
    assert 0.0 <= prediction["predicted_fill_probability"] <= 1.0
    assert prediction["predicted_latency_ms_p95"] > 0


def test_load_bundle_missing_dir_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_exec_bundle(tmp_path / "nowhere")


def test_real_model_always_serves():
    decision = resolve_serving_decision(is_demo=False, pipeline_mode="alpaca", env={})
    assert decision.serve is True
    assert decision.model_status == "real"
    assert decision.warnings == []


def test_demo_model_serves_in_sim_mode_with_warning():
    decision = resolve_serving_decision(is_demo=True, pipeline_mode="sim", env={})
    assert decision.serve is True
    assert decision.model_status == "demo"
    assert decision.warnings, "demo serving must always be announced"


def test_demo_model_refused_on_live_pipeline_without_flag():
    decision = resolve_serving_decision(is_demo=True, pipeline_mode="alpaca", env={})
    assert decision.serve is False
    assert decision.model_status is None
    assert DEMO_ENV_FLAG in decision.reason


def test_demo_model_allowed_on_live_pipeline_with_explicit_flag():
    decision = resolve_serving_decision(
        is_demo=True, pipeline_mode="alpaca", env={DEMO_ENV_FLAG: "true"}
    )
    assert decision.serve is True
    assert decision.model_status == "demo"
    assert len(decision.warnings) >= 2  # demo banner + explicit-override notice
