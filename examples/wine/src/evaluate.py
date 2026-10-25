"""Score the trained model on the held-out test split."""

from __future__ import annotations

import argparse
import json
import pickle
from pathlib import Path

import numpy as np
from sklearn.metrics import accuracy_score, f1_score


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    test = np.load(args.features / "test.npz")
    with args.model.open("rb") as fh:
        model = pickle.load(fh)
    pred = model.predict(test["X"])
    metrics = {
        "accuracy": round(float(accuracy_score(test["y"], pred)), 4),
        "macro_f1": round(float(f1_score(test["y"], pred, average="macro")), 4),
        "n_test": int(len(test["y"])),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(metrics, indent=2))
    print(f"held-out metrics: {metrics}")


if __name__ == "__main__":
    main()
