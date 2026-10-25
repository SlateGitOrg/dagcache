"""Majority-class baseline evaluated on the held-out split."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from sklearn.dummy import DummyClassifier
from sklearn.metrics import accuracy_score


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    train = np.load(args.features / "train.npz")
    test = np.load(args.features / "test.npz")
    model = DummyClassifier(strategy="most_frequent").fit(train["X"], train["y"])
    acc = float(accuracy_score(test["y"], model.predict(test["X"])))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps({"accuracy": round(acc, 4)}, indent=2))
    print(f"baseline accuracy {acc:.4f}")


if __name__ == "__main__":
    main()
