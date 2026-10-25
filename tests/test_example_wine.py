"""The bundled scikit-learn example: real stages, real cache hits."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest
from conftest import actions

from dagcache import Runner, Workspace

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "wine"

pytestmark = pytest.mark.slow


@pytest.fixture
def wine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "wine"
    shutil.copytree(
        EXAMPLE,
        root,
        ignore=shutil.ignore_patterns(
            ".dagcache", "dagcache.lock", "data", "models", "metrics", "reports"
        ),
    )
    # Stage commands call `python`; make it this interpreter (with sklearn).
    monkeypatch.setenv("PATH", f"{Path(sys.executable).parent}:{__import__('os').environ['PATH']}")
    return root


def load(root: Path) -> Workspace:
    return Workspace.load(root / "dagcache.yaml")


def test_wine_pipeline_cache_hits(wine: Path) -> None:
    ws = load(wine)
    first = Runner(ws, jobs=4).repro()
    ws.close()
    assert first.ok, [r.error for r in first.results]
    assert set(actions(first).values()) == {"ran"}
    assert first.max_parallel >= 2  # profile runs beside split; train beside baseline
    metrics = json.loads((wine / "metrics/metrics.json").read_text())
    baseline = json.loads((wine / "metrics/baseline.json").read_text())
    assert metrics["n_test"] == 54
    assert metrics["accuracy"] > baseline["accuracy"]

    ws = load(wine)
    assert set(actions(Runner(ws).repro()).values()) == {"up-to-date"}
    ws.close()

    params = wine / "params.yaml"
    original = params.read_text()
    params.write_text(original.replace("C: 1.0", "C: 0.05"))
    ws = load(wine)
    changed = actions(Runner(ws, jobs=4).repro())
    ws.close()
    assert changed == {
        "load": "up-to-date",
        "profile": "up-to-date",
        "split": "up-to-date",
        "featurize": "up-to-date",
        "train": "ran",
        "baseline": "up-to-date",
        "evaluate": "ran",
    }

    params.write_text(original)
    ws = load(wine)
    reverted = actions(Runner(ws, jobs=4).repro())
    ws.close()
    assert reverted["train"] == "restored" and reverted["evaluate"] == "restored"
    assert json.loads((wine / "metrics/metrics.json").read_text()) == metrics
