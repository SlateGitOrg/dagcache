"""Train a multinomial logistic-regression classifier."""

from __future__ import annotations

import argparse
import pickle
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--C", type=float, required=True)
    parser.add_argument("--max-iter", type=int, required=True)
    args = parser.parse_args()
    train = np.load(args.features / "train.npz")
    model = LogisticRegression(C=args.C, max_iter=args.max_iter)
    model.fit(train["X"], train["y"])
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("wb") as fh:
        pickle.dump(model, fh, protocol=5)
    print(f"trained LogisticRegression(C={args.C}) on {len(train['y'])} rows")


if __name__ == "__main__":
    main()
