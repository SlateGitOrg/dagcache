"""``repro``: bring the workspace up to date, running only what must run.

For each stage, once every upstream stage has finished:

1. hash its dependencies (now materialised) and compute its cache key;
2. under the stage's exclusive lock, re-read ``dagcache.lock``:
   * same key and outputs intact          -> **up-to-date** (nothing to do);
   * key found in the cache (local/remote) -> **restored** (copy/link outputs);
   * otherwise                             -> **ran**: delete stale outputs,
     execute, hash and store the outputs, write the action manifest;
3. merge the stage's entry into ``dagcache.lock``.

Re-checking under the lock is what makes concurrent ``repro`` invocations safe:
the second process blocks on the stage lock and then finds the first one's
result instead of recomputing it.
"""

from __future__ import annotations

import datetime as _dt
import enum
import logging
import os
import subprocess
import time
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from pathlib import Path

from .cache import Cache, remove_path
from .errors import (
    CacheError,
    DagcacheError,
    MissingDependencyError,
    StageFailedError,
)
from .executor import DagExecutor, Outcome
from .keys import explain
from .locking import FileLock
from .manifest import ActionManifest, KeyComponents
from .stage import Stage, StageContext
from .status import check_outputs
from .workspace import Workspace

log = logging.getLogger(__name__)

_LOG_TAIL_LINES = 15


class Action(enum.Enum):
    UP_TO_DATE = "up-to-date"
    RESTORED = "restored"
    RAN = "ran"
    FAILED = "failed"
    NOT_RUN = "not-run"


@dataclass
class StageResult:
    name: str
    action: Action
    key: str | None = None
    duration_s: float = 0.0
    reasons: list[str] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "stage": self.name,
            "action": self.action.value,
            "key": self.key,
            "duration_s": round(self.duration_s, 4),
            "reasons": list(self.reasons),
            "error": self.error,
        }


@dataclass
class ReproReport:
    results: list[StageResult]
    elapsed_s: float
    max_parallel: int

    @property
    def ok(self) -> bool:
        return all(r.action not in (Action.FAILED, Action.NOT_RUN) for r in self.results)

    def count(self, action: Action) -> int:
        return sum(1 for r in self.results if r.action is action)

    def by_name(self) -> dict[str, StageResult]:
        return {r.name: r for r in self.results}


StageCallback = Callable[[StageResult], None]
OutputCallback = Callable[[str, str], None]


def _now() -> str:
    return _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")


class Runner:
    """Executes a workspace's pipeline with caching and parallelism."""

    def __init__(
        self,
        ws: Workspace,
        *,
        jobs: int = 1,
        force: bool = False,
        keep_going: bool = False,
        use_remote: bool = True,
        on_start: Callable[[str], None] | None = None,
        on_finish: StageCallback | None = None,
        on_output: OutputCallback | None = None,
    ) -> None:
        self.ws = ws
        self.cache: Cache = ws.cache(use_remote=use_remote)
        self.jobs = jobs
        self.force = force
        self.keep_going = keep_going
        self.use_remote = use_remote
        self.on_start = on_start
        self.on_finish = on_finish
        self.on_output = on_output
        self._keys: dict[str, str] = {}

    # -- public ------------------------------------------------------------
    def repro(self, targets: Iterable[str] | None = None) -> ReproReport:
        pipe = self.ws.pipeline
        graph = pipe.graph
        target_list = list(targets) if targets else []
        selected = pipe.upstream_closure(target_list) if target_list else set(graph.order)
        order = [n for n in graph.order if n in selected]
        started = time.monotonic()
        with FileLock(self.ws.repo_lock_path, shared=True, timeout=self.ws.lock_timeout):
            executor: DagExecutor[StageResult] = DagExecutor(
                order, graph.upstream, jobs=self.jobs, keep_going=self.keep_going
            )
            report = executor.run(self._process)
            if not target_list:
                stale = [n for n in self.ws.lockfile.read() if n not in pipe]
                if stale:
                    self.ws.lockfile.update({}, remove=stale)
        results: list[StageResult] = []
        for name, node in report.results.items():
            if node.outcome is Outcome.OK and node.value is not None:
                results.append(node.value)
            elif node.outcome is Outcome.FAILED:
                results.append(StageResult(name, Action.FAILED, error=str(node.error)))
            else:
                why = (
                    f"upstream stage '{node.blocked_by}' failed"
                    if node.blocked_by
                    else "run stopped after an earlier failure"
                )
                results.append(StageResult(name, Action.NOT_RUN, reasons=[why]))
        return ReproReport(results, time.monotonic() - started, report.max_parallel)

    # -- per stage -----------------------------------------------------------
    def _dep_digests(self, stage: Stage) -> dict[str, str]:
        digests: dict[str, str] = {}
        for dep in stage.deps:
            try:
                digests[dep] = self.ws.hasher.digest_path(dep).digest
            except FileNotFoundError:
                raise MissingDependencyError(stage.name, dep) from None
        return digests

    def _upstream_keys(self, stage: Stage) -> dict[str, str]:
        graph = self.ws.pipeline.graph
        upstream: dict[str, str] = {}
        for dep in stage.deps:
            producers = graph.producers.get((stage.name, dep), ())
            for producer, _ in producers:
                key = self._keys.get(producer)
                if key is None:
                    continue
                label = dep if len(producers) == 1 else f"{dep}|{producer}"
                upstream[label] = key
        return upstream

    def _process(self, name: str) -> StageResult:
        if self.on_start:
            self.on_start(name)
        try:
            return self._process_stage(name)
        except Exception as exc:
            if self.on_finish:
                self.on_finish(
                    StageResult(name, Action.FAILED, self._keys.get(name), error=str(exc))
                )
            raise

    def _process_stage(self, name: str) -> StageResult:
        stage = self.ws.pipeline.get(name)
        started = time.monotonic()
        comps = self.ws.keys.components(stage, self._dep_digests(stage))
        key = comps.key
        self._keys[name] = key
        upstream = self._upstream_keys(stage)
        with FileLock(self.ws.stage_lock_path(name), timeout=self.ws.lock_timeout):
            entry = self.ws.lockfile.read().get(name)
            if entry is not None and entry.key == key:
                reasons = check_outputs(self.ws, entry)
            elif entry is None:
                reasons = ["never run"]
            else:
                reasons = explain(entry.components, comps)
            if self.force:
                reasons = ["forced", *reasons]
            elif stage.always_run:
                reasons = ["always_run", *reasons]
            else:
                if entry is not None and entry.key == key and not reasons:
                    if entry.upstream != upstream:
                        # Early cutoff: an upstream re-ran but wrote identical bytes.
                        # Point lineage at the producers that are current now.
                        self.ws.lockfile.update({name: replace(entry, upstream=upstream)})
                    self.cache.touch(key)
                    return self._finish(StageResult(name, Action.UP_TO_DATE, key))
                restored = self._try_restore(name, key, upstream)
                if restored is not None:
                    restored.duration_s = time.monotonic() - started
                    restored.reasons = reasons
                    return self._finish(restored)
            self._execute(stage, comps, key, upstream)
            return self._finish(
                StageResult(name, Action.RAN, key, time.monotonic() - started, reasons)
            )

    def _finish(self, result: StageResult) -> StageResult:
        if self.on_finish:
            self.on_finish(result)
        return result

    def _try_restore(self, name: str, key: str, upstream: dict[str, str]) -> StageResult | None:
        manifest = self.cache.get_action(key, use_remote=self.use_remote)
        if manifest is None:
            return None
        try:
            self.cache.restore(self.ws.root, manifest.outputs)
        except CacheError as exc:
            log.warning("cache entry for %s is unusable (%s); re-running", name, exc)
            return None
        entry = replace(manifest, stage=name, upstream=upstream)
        self.ws.lockfile.update({name: entry})
        self.cache.touch(key)
        return StageResult(name, Action.RESTORED, key)

    def _execute(
        self, stage: Stage, comps: KeyComponents, key: str, upstream: dict[str, str]
    ) -> None:
        root = self.ws.root
        for out in stage.outs:
            remove_path(root / out)
            (root / out).parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        if stage.cmd is not None:
            self._run_command(stage, self.ws.keys.command(stage), key)
        else:
            self._run_function(stage)
        duration = time.monotonic() - started
        for out in stage.outs:
            if not (root / out).exists():
                raise StageFailedError(stage.name, f"did not produce declared output '{out}'")
        records = self.cache.store_outputs(root, stage.outs, self.ws.hasher)
        manifest = ActionManifest(
            key=key,
            stage=stage.name,
            components=comps,
            outputs=records,
            upstream=upstream,
            created_at=_now(),
            duration_s=duration,
        )
        self.cache.put_action(manifest)
        self.ws.lockfile.update({stage.name: manifest})

    def _run_command(self, stage: Stage, cmd: str, key: str) -> None:
        root = self.ws.root
        log_path = self.ws.logs_dir / f"{stage.name}.log"
        env = dict(os.environ)
        env.update(DAGCACHE_STAGE=stage.name, DAGCACHE_ROOT=str(root), DAGCACHE_KEY=key)
        tail: deque[str] = deque(maxlen=_LOG_TAIL_LINES)
        log.debug("running %s: %s", stage.name, cmd)
        with log_path.open("w", encoding="utf-8") as logf:
            proc = subprocess.Popen(
                cmd,
                shell=True,
                cwd=root / stage.wdir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                errors="replace",
                bufsize=1,
            )
            assert proc.stdout is not None
            for line in proc.stdout:
                logf.write(line)
                text = line.rstrip("\n")
                tail.append(text)
                if self.on_output:
                    self.on_output(stage.name, text)
            rc = proc.wait()
        if rc != 0:
            rel_log = Path(os.path.relpath(log_path, root)).as_posix()
            detail = "\n".join(f"    {t}" for t in tail)
            raise StageFailedError(
                stage.name,
                f"command exited with status {rc} (full log: {rel_log})"
                + (f"\n{detail}" if detail else ""),
            )

    def _run_function(self, stage: Stage) -> None:
        assert stage.func is not None
        ctx = StageContext(
            name=stage.name,
            root=self.ws.root,
            params=self.ws.keys.param_values(stage),
            deps=stage.deps,
            outs=stage.outs,
        )
        try:
            stage.func(ctx)
        except DagcacheError:
            raise
        except Exception as exc:
            log.debug("stage %s raised", stage.name, exc_info=True)
            raise StageFailedError(stage.name, f"{type(exc).__name__}: {exc}") from exc
