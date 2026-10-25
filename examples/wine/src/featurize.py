"""Fit a scaler on the train split only and transform both splits."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from common import read_table
from sklearn.preprocessing import MinMaxScaler, StandardScaler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--train", type=Path, required=True)
    parser.add_argument("--test", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--scaler", choices=["standard", "minmax"], required=True)
    args = parser.parse_args()
    _, X_tr, y_tr = read_table(args.train)
    _, X_te, y_te = read_table(args.test)
    scaler = StandardScaler() if args.scaler == "standard" else MinMaxScaler()
    scaler.fit(X_tr)  # fit on train only: no information leaks from the test split
    args.out_dir.mkdir(parents=True, exist_ok=True)
    np.savez(args.out_dir / "train.npz", X=scaler.transform(X_tr), y=y_tr)
    np.savez(args.out_dir / "test.npz", X=scaler.transform(X_te), y=y_te)
    (args.out_dir / "scaler.json").write_text(json.dumps({"scaler": args.scaler}))
    print(f"{args.scaler} scaling of {X_tr.shape[1]} features")


if __name__ == "__main__":
    main()
