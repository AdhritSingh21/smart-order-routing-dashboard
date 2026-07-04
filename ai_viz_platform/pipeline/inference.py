from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import onnxruntime as ort

from .state import RuntimeState
from .types import FeatureFrame


class OnnxDirectionModel:
    def __init__(self, model_path: Path, metadata_path: Path | None = None) -> None:
        if not model_path.exists():
            raise FileNotFoundError(
                f"ONNX model not found at {model_path}. Run: python train_model.py"
            )

        self.session = ort.InferenceSession(
            str(model_path), providers=["CPUExecutionProvider"]
        )
        self.input_name = self.session.get_inputs()[0].name
        self.output_names = [output.name for output in self.session.get_outputs()]
        self.classes = [0, 1]

        if metadata_path and metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            self.classes = [int(value) for value in metadata.get("classes", [0, 1])]

    def predict(self, features: FeatureFrame) -> tuple[int, float]:
        input_array = np.asarray([features.as_vector()], dtype=np.float32)
        outputs = self.session.run(None, {self.input_name: input_array})

        label: int | None = None
        probabilities: np.ndarray | None = None
        for name, output in zip(self.output_names, outputs, strict=True):
            array = np.asarray(output)
            lowered = name.lower()
            if "prob" in lowered and array.ndim == 2:
                probabilities = array.astype(np.float64, copy=False)
            elif "label" in lowered or array.dtype.kind in {"i", "u"}:
                label = int(array.reshape(-1)[0])

        if label is None:
            for output in outputs:
                array = np.asarray(output)
                if array.dtype.kind in {"i", "u"}:
                    label = int(array.reshape(-1)[0])
                    break

        if probabilities is None:
            for output in outputs:
                array = np.asarray(output)
                if array.ndim == 2 and array.shape[1] >= 2:
                    probabilities = array.astype(np.float64, copy=False)
                    break

        if label is None or probabilities is None:
            raise RuntimeError(
                f"Unexpected ONNX outputs: {[(n, np.asarray(o).shape) for n, o in zip(self.output_names, outputs, strict=True)]}"
            )

        try:
            class_index = self.classes.index(label)
        except ValueError:
            class_index = int(np.argmax(probabilities[0]))
            label = self.classes[class_index]
        confidence = float(probabilities[0, class_index])
        return label, confidence


async def inference_worker(
    feature_queue: asyncio.Queue[FeatureFrame],
    prediction_queue: asyncio.Queue[dict[str, Any]],
    model: OnnxDirectionModel,
    state: RuntimeState,
    stop_event: asyncio.Event,
) -> None:
    state.model_loaded = True

    while not stop_event.is_set():
        try:
            feature_frame = await asyncio.wait_for(feature_queue.get(), timeout=0.5)
        except TimeoutError:
            continue

        try:
            label, confidence = await asyncio.to_thread(model.predict, feature_frame)
            latency_ms = (time.perf_counter_ns() - feature_frame.tick.arrival_ns) / 1_000_000
            payload = {
                "symbol": feature_frame.tick.symbol,
                "event_time": feature_frame.tick.event_time,
                "published_at": datetime.now(timezone.utc).isoformat(),
                "source": feature_frame.tick.source,
                "market_event": feature_frame.tick.event_type,
                "latest_price": round(feature_frame.tick.price, 8),
                "prediction": "up" if label == 1 else "down",
                "confidence": round(confidence, 6),
                "pipeline_latency_ms": round(latency_ms, 3),
                "features": {
                    key: round(value, 10)
                    for key, value in feature_frame.as_dict().items()
                },
            }
            await prediction_queue.put(payload)
        except Exception as exc:
            state.last_error = f"inference: {type(exc).__name__}: {exc}"
            raise
        finally:
            feature_queue.task_done()
