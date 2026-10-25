"""Export the UCI Wine dataset bundled with scikit-learn (no network access)."""

from __future__ import annotations

import argparse
from pathlib import Path

from common import write_table
from sklearn.datasets import load_wine


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    ds = load_wine()
    write_table(args.out, list(ds.feature_names), ds.data, ds.target)
    print(f"wrote {len(ds.target)} rows x {ds.data.shape[1]} features to {args.out}")


if __name__ == "__main__":
    main()
