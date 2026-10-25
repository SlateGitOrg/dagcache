"""dagcache - a content-addressed, cached pipeline runner for ML workflows.

Public API::

    from dagcache import Pipeline, Workspace, Runner

    pipe = Pipeline("demo")

    @pipe.stage(deps=["data/raw.csv"], outs=["data/clean.csv"], params=["clean.min"])
    def clean(ctx):
        ...

    ws = Workspace(root, pipe)
    report = Runner(ws, jobs=4).repro()
"""

from __future__ import annotations

__version__ = "0.1.0"

from .cache import Cache
from .errors import (
    CacheError,
    ConfigError,
    DagcacheError,
    LockTimeoutError,
    MissingDependencyError,
    ParamError,
    PipelineError,
    StageFailedError,
)
from .gc import GcReport, collect
from .lineage import LineageResolver
from .manifest import ActionManifest, KeyComponents, OutputRecord
from .pipeline import Pipeline
from .runner import Action, ReproReport, Runner, StageResult
from .stage import Stage, StageContext
from .status import StageStatus, State, plan
from .workspace import Workspace

__all__ = [
    "Action",
    "ActionManifest",
    "Cache",
    "CacheError",
    "ConfigError",
    "DagcacheError",
    "GcReport",
    "KeyComponents",
    "LineageResolver",
    "LockTimeoutError",
    "MissingDependencyError",
    "OutputRecord",
    "ParamError",
    "Pipeline",
    "PipelineError",
    "ReproReport",
    "Runner",
    "Stage",
    "StageContext",
    "StageFailedError",
    "StageResult",
    "StageStatus",
    "State",
    "Workspace",
    "__version__",
    "collect",
    "plan",
]
