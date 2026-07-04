import sys
from pathlib import Path

import pytest

# Make `pipeline` importable regardless of where pytest is invoked from.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def trained_bundle_dir(tmp_path_factory: pytest.TempPathFactory):
    """Train a tiny demo execution-quality bundle once for the whole session."""
    from exec_ml.dataset import build_dataset
    from exec_ml.simulate import generate_metrics
    from exec_ml.train import save_artifacts, train_and_evaluate

    frame = build_dataset(generate_metrics(orders=3_000, seed=11))
    trained = train_and_evaluate(
        frame, data_source="simulated-replay", is_demo=True, test_fraction=0.2
    )
    output_dir = tmp_path_factory.mktemp("exec_models")
    save_artifacts(trained, output_dir)
    return output_dir
