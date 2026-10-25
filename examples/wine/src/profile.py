"""Summary statistics per column."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from common import read_table


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    header, X, y = read_table(args.data)
    stats = {
        name: {
            "mean": round(float(X[:, i].mean()), 4),
            "std": round(float(X[:, i].std()), 4),
            "min": float(X[:, i].min()),
            "max": float(X[:, i].max()),
        }
        for i, name in enumerate(header)
    }
    counts = {str(k): int(v) for k, v in zip(*np.unique(y, return_counts=True), strict=True)}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps({"rows": len(y), "classes": counts, "columns": stats}, indent=2))
    print(f"profiled {len(header)} columns, class counts {counts}")


if __name__ == "__main__":
    main()
