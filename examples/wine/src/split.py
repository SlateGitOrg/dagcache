"""Stratified train/test split."""

from __future__ import annotations

import argparse
from pathlib import Path

from common import read_table, write_table
from sklearn.model_selection import train_test_split


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--test-size", type=float, required=True)
    parser.add_argument("--seed", type=int, required=True)
    args = parser.parse_args()
    header, X, y = read_table(args.data)
    X_tr, X_te, y_tr, y_te = train_test_split(
        X, y, test_size=args.test_size, random_state=args.seed, stratify=y
    )
    write_table(args.out_dir / "train.csv", header, X_tr, y_tr)
    write_table(args.out_dir / "test.csv", header, X_te, y_te)
    print(f"train={len(y_tr)} test={len(y_te)} (seed {args.seed})")


if __name__ == "__main__":
    main()
