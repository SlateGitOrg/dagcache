"""Workspace-local helpers: editing these invalidates every stage that calls them."""

from __future__ import annotations

import statistics


def zscore(values: list[float]) -> list[float]:
    mu = statistics.fmean(values)
    sd = statistics.pstdev(values) or 1.0
    return [(v - mu) / sd for v in values]
