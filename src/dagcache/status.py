"""Planning: what state is each stage in, and why.

``status`` walks the DAG in topological order and, for every stage, computes
the cache key it *would* have now. Dependencies produced by other stages are
resolved without running anything:

* producer up to date        -> hash the file in the workspace;
* producer restorable        -> take the digest from the producer's cached
                                manifest (it is what a restore would write);
* producer must (re-)run     -> unknown, so this stage is ``pending``: its
                                inputs will be known only after the producer runs.

That last rule is transitive invalidation: a change anywhere upstream makes
every downstream stage at least ``pending``. It is also *early cutoff* aware -
if the producer re-runs and writes byte-identical outputs, ``repro`` will find
the downstream keys unchanged and skip them.
"""

from __future__ import annotations

import enum
from collections.abc import Iterable
from dataclasses import dataclass, field

from .cache import Cache
from .errors import DagcacheError
from .keys import explain
from .manifest import ActionManifest, KeyComponents
from .stage import Stage
from .workspace import Workspace


class State(enum.Enum):
    UP_TO_DATE = "up-to-date"
    CACHED = "cached"  # stale in the workspace, but the result is in the cache
    STALE = "stale"  # must run
    PENDING = "pending"  # depends on a stage that must run first
    ALWAYS = "always-run"
    ERROR = "error"  # cannot be evaluated (missing dependency, bad param, ...)

    @property
    def needs_work(self) -> bool:
        return self is not State.UP_TO_DATE


@dataclass
class StageStatus:
    name: str
    state: State
    key: str | None = None
    reasons: list[str] = field(default_factory=list)
    components: KeyComponents | None = None
    manifest: ActionManifest | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "stage": self.name,
            "state": self.state.value,
            "key": self.key,
            "reasons": list(self.reasons),
        }


def check_outputs(ws: Workspace, entry: ActionManifest) -> list[str]:
    """Reasons the workspace outputs differ from ``entry`` (empty if they match)."""
    reasons: list[str] = []
    for rec in entry.outputs:
        try:
            current = ws.hasher.digest_path(rec.path)
        except FileNotFoundError:
            reasons.append(f"output {rec.path} missing")
            continue
        if current.digest != rec.digest or current.kind != rec.kind:
            reasons.append(f"output {rec.path} modified in workspace")
    return reasons


def _resolve_deps(
    ws: Workspace, stage: Stage, statuses: dict[str, StageStatus]
) -> tuple[dict[str, str], list[str], list[str]]:
    """Returns ``(dep digests, blocking upstream reasons, errors)``."""
    graph = ws.pipeline.graph
    digests: dict[str, str] = {}
    blocking: list[str] = []
    errors: list[str] = []
    for dep in stage.deps:
        producers = graph.producers.get((stage.name, dep), ())
        ups = [(statuses[p], out) for p, out in producers if p in statuses]
        unsettled = [u for u, _ in ups if u.state not in (State.UP_TO_DATE, State.CACHED)]
        if unsettled:
            for u in unsettled:
                msg = f"upstream stage '{u.name}' is {u.state.value}"
                if msg not in blocking:
                    blocking.append(msg)
            continue
        cached = [(u, out) for u, out in ups if u.state is State.CACHED]
        if cached:
            if len(ups) == 1 and ups[0][1] == dep and ups[0][0].manifest is not None:
                rec = ups[0][0].manifest.output(dep)
                if rec is not None:
                    digests[dep] = rec.digest
                    continue
            # Nested or partial overlap: digest only known after the restore.
            for u, _ in cached:
                msg = f"upstream stage '{u.name}' will be restored from cache"
                if msg not in blocking:
                    blocking.append(msg)
            continue
        try:
            digests[dep] = ws.hasher.digest_path(dep).digest
        except FileNotFoundError:
            errors.append(f"dependency {dep} does not exist")
    return digests, blocking, errors


def plan(
    ws: Workspace,
    cache: Cache,
    targets: Iterable[str] | None = None,
    *,
    force: bool = False,
) -> list[StageStatus]:
    """Status of every selected stage (``targets`` plus their upstream), in DAG order."""
    pipe = ws.pipeline
    graph = pipe.graph
    selected = pipe.upstream_closure(targets) if targets else set(graph.order)
    lock = ws.lockfile.read()
    statuses: dict[str, StageStatus] = {}
    for name in graph.order:
        if name not in selected:
            continue
        stage = pipe.get(name)
        st = StageStatus(name, State.STALE)
        statuses[name] = st
        digests, blocking, errors = _resolve_deps(ws, stage, statuses)
        if errors:
            st.state, st.reasons = State.ERROR, errors
            continue
        if blocking:
            st.state, st.reasons = State.PENDING, blocking
            continue
        try:
            comps = ws.keys.components(stage, digests)
        except DagcacheError as exc:
            st.state, st.reasons = State.ERROR, [str(exc)]
            continue
        st.components, st.key = comps, comps.key
        entry = lock.get(name)
        if stage.always_run:
            st.state, st.reasons = State.ALWAYS, ["stage is marked always_run"]
            continue
        if force:
            st.state, st.reasons = State.STALE, ["forced"]
            continue
        if entry is not None and entry.key == st.key:
            st.reasons = check_outputs(ws, entry)
            if not st.reasons:
                st.state = State.UP_TO_DATE
                st.manifest = entry
                continue
        elif entry is None:
            st.reasons = ["never run (no entry in dagcache.lock)"]
        else:
            st.reasons = explain(entry.components, comps)
        manifest = cache.get_action(st.key)
        if manifest is not None:
            st.state, st.manifest = State.CACHED, manifest
        else:
            st.state = State.STALE
    return [statuses[n] for n in graph.order if n in statuses]
