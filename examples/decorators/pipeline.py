"""A pipeline declared with Python decorators instead of YAML.

Each stage's cache key covers the function's normalised source plus, transitively,
the helpers, constants and library versions it references - so reformatting or
commenting a function changes nothing, while editing `zscore` in helpers.py
invalidates `normalize` and, through its output, `summarize`.
"""

from __future__ import annotations

import json
import random

from helpers import zscore

from dagcache import Pipeline, StageContext

pipeline = Pipeline("decorators")

ROUND_TO = 4  # a module constant: changing it invalidates `summarize` only


@pipeline.stage(outs=["data/samples.json"], params=["samples.n", "samples.seed"])
def generate(ctx: StageContext) -> None:
    """Draw reproducible pseudo-random samples."""
    rng = random.Random(ctx.params["samples.seed"])
    values = [rng.gauss(10.0, 2.5) for _ in range(ctx.params["samples.n"])]
    ctx.path("data/samples.json").write_text(json.dumps(values))


@pipeline.stage(deps=["data/samples.json"], outs=["data/normalized.json"])
def normalize(ctx: StageContext) -> None:
    """Standardise the samples with the local `zscore` helper."""
    values = json.loads(ctx.path("data/samples.json").read_text())
    ctx.path("data/normalized.json").write_text(json.dumps(zscore(values)))


@pipeline.stage(deps=["data/normalized.json"], outs=["reports/summary.json"])
def summarize(ctx: StageContext) -> None:
    """Summary statistics of the normalised data."""
    values = json.loads(ctx.path("data/normalized.json").read_text())
    summary = {
        "n": len(values),
        "min": round(min(values), ROUND_TO),
        "max": round(max(values), ROUND_TO),
    }
    ctx.path("reports/summary.json").write_text(json.dumps(summary, indent=2))
