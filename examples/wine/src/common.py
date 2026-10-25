"""CSV helpers shared by the wine pipeline scripts (numpy only, no pandas)."""

from __future__ import annotations

import csv
from pathlib import Path

import numpy as np


def write_table(path: Path, header: list[str], X: np.ndarray, y: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow([*header, "target"])
        for row, label in zip(X, y, strict=True):
            writer.writerow([f"{v:.6g}" for v in row] + [int(label)])


def read_table(path: Path) -> tuple[list[str], np.ndarray, np.ndarray]:
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    header, body = rows[0], rows[1:]
    data = np.array([[float(v) for v in r] for r in body])
    return header[:-1], data[:, :-1], data[:, -1].astype(int)
