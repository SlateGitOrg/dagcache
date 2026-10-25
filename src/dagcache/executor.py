"""Generic parallel DAG executor.

Runs a callable for every node once all of its upstream nodes have completed
successfully, keeping at most ``jobs`` calls in flight. Scheduling is Kahn's
algorithm driven by completion events, so total work is O(V + E) plus the
calls themselves, and independent branches overlap freely.

Failure policy:

* the descendants of a failed node never run (``skipped``);
* with ``keep_going=False`` (default) no *new* nodes start after the first
  failure - in-flight ones are allowed to finish (``cancelled`` for the rest);
* with ``keep_going=True`` every node not downstream of a failure still runs.
"""

from __future__ import annotations

import enum
import heapq
import logging
import threading
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass

log = logging.getLogger(__name__)


class Outcome(enum.Enum):
    OK = "ok"
    FAILED = "failed"
    SKIPPED = "skipped"  # an upstream node failed
    CANCELLED = "cancelled"  # never started because the run stopped early


@dataclass
class NodeResult[T]:
    outcome: Outcome
    value: T | None = None
    error: BaseException | None = None
    blocked_by: str | None = None


@dataclass
class ExecutionReport[T]:
    results: dict[str, NodeResult[T]]
    max_parallel: int

    @property
    def ok(self) -> bool:
        return all(r.outcome is Outcome.OK for r in self.results.values())


class DagExecutor[T]:
    """Execute ``fn(node)`` over a DAG given as ``upstream`` adjacency."""

    def __init__(
        self,
        order: Sequence[str],
        upstream: Mapping[str, frozenset[str] | set[str]],
        *,
        jobs: int = 1,
        keep_going: bool = False,
    ) -> None:
        if jobs < 1:
            raise ValueError("jobs must be >= 1")
        self.order = list(order)
        nodes = set(self.order)
        # Only edges inside the selected sub-graph matter.
        self.upstream = {n: {u for u in upstream.get(n, ()) if u in nodes} for n in self.order}
        self.downstream: dict[str, set[str]] = {n: set() for n in self.order}
        for n, ups in self.upstream.items():
            for u in ups:
                self.downstream[u].add(n)
        self.jobs = jobs
        self.keep_going = keep_going

    def run(self, fn: Callable[[str], T]) -> ExecutionReport[T]:
        priority = {n: i for i, n in enumerate(self.order)}
        remaining = {n: len(u) for n, u in self.upstream.items()}
        ready: list[tuple[int, str]] = [(priority[n], n) for n, c in remaining.items() if c == 0]
        heapq.heapify(ready)
        results: dict[str, NodeResult[T]] = {}
        running: dict[Future[T], str] = {}
        stopping = False
        active = 0
        max_active = 0
        active_lock = threading.Lock()

        def wrapped(node: str) -> T:
            nonlocal active, max_active
            with active_lock:
                active += 1
                max_active = max(max_active, active)
            try:
                return fn(node)
            finally:
                with active_lock:
                    active -= 1

        def skip_descendants(node: str) -> None:
            stack = list(self.downstream[node])
            while stack:
                d = stack.pop()
                if d in results:
                    continue
                results[d] = NodeResult(Outcome.SKIPPED, blocked_by=node)
                stack.extend(self.downstream[d])

        with ThreadPoolExecutor(max_workers=self.jobs, thread_name_prefix="dagcache") as pool:
            try:
                while ready or running:
                    while ready and not stopping and len(running) < self.jobs:
                        _, node = heapq.heappop(ready)
                        if node in results:  # skipped meanwhile
                            continue
                        running[pool.submit(wrapped, node)] = node
                    if not running:
                        break
                    done, _ = wait(running, return_when=FIRST_COMPLETED)
                    for fut in done:
                        node = running.pop(fut)
                        exc = fut.exception()
                        if exc is not None and not isinstance(exc, Exception):
                            raise exc  # KeyboardInterrupt/SystemExit: abort the run
                        if exc is not None:
                            results[node] = NodeResult(Outcome.FAILED, error=exc)
                            skip_descendants(node)
                            if not self.keep_going:
                                stopping = True
                            continue
                        results[node] = NodeResult(Outcome.OK, value=fut.result())
                        for d in self.downstream[node]:
                            remaining[d] -= 1
                            if remaining[d] == 0 and d not in results:
                                heapq.heappush(ready, (priority[d], d))
            except BaseException:
                for fut in running:
                    fut.cancel()
                raise
        for n in self.order:
            results.setdefault(n, NodeResult(Outcome.CANCELLED))
        return ExecutionReport({n: results[n] for n in self.order}, max_active)
