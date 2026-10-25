"""The generic parallel DAG executor."""

from __future__ import annotations

import threading
import time

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from dagcache.executor import DagExecutor, Outcome


def test_independent_nodes_run_in_parallel_up_to_jobs() -> None:
    order = [f"n{i}" for i in range(8)]
    ex: DagExecutor[str] = DagExecutor(order, {}, jobs=4)
    t0 = time.monotonic()
    report = ex.run(lambda n: (time.sleep(0.2), n)[1])
    elapsed = time.monotonic() - t0
    assert report.ok
    assert report.max_parallel == 4
    assert elapsed < 0.2 * 8 * 0.75  # clearly faster than serial


def test_jobs_one_is_serial() -> None:
    ex: DagExecutor[int] = DagExecutor(["a", "b", "c"], {}, jobs=1)
    assert ex.run(lambda n: 1).max_parallel == 1


def test_respects_dependencies() -> None:
    upstream = {"b": {"a"}, "c": {"a"}, "d": {"b", "c"}}
    finished: list[str] = []
    lock = threading.Lock()

    def fn(n: str) -> str:
        with lock:
            for u in upstream.get(n, ()):
                assert u in finished
        time.sleep(0.01)
        with lock:
            finished.append(n)
        return n

    report = DagExecutor(["a", "b", "c", "d"], upstream, jobs=3).run(fn)
    assert report.ok
    assert finished[0] == "a" and finished[-1] == "d"


def test_failure_skips_descendants_and_stops_new_work() -> None:
    upstream = {"b": {"a"}, "c": set(), "d": {"b"}}

    def fn(n: str) -> str:
        if n == "a":
            raise RuntimeError("boom")
        return n

    report = DagExecutor(["a", "c", "b", "d"], upstream, jobs=1).run(fn)
    res = report.results
    assert res["a"].outcome is Outcome.FAILED and isinstance(res["a"].error, RuntimeError)
    assert res["b"].outcome is Outcome.SKIPPED and res["b"].blocked_by == "a"
    assert res["d"].outcome is Outcome.SKIPPED
    assert res["c"].outcome is Outcome.CANCELLED  # fail-fast: never started
    assert not report.ok


def test_keep_going_runs_independent_branches() -> None:
    upstream = {"b": {"a"}}

    def fn(n: str) -> str:
        if n == "a":
            raise ValueError("x")
        return n

    report = DagExecutor(["a", "b", "c"], upstream, jobs=1, keep_going=True).run(fn)
    assert report.results["c"].outcome is Outcome.OK
    assert report.results["b"].outcome is Outcome.SKIPPED


def test_edges_outside_selection_are_ignored() -> None:
    report = DagExecutor(["b"], {"b": {"a"}}, jobs=2).run(lambda n: n)
    assert report.results["b"].outcome is Outcome.OK


def test_invalid_jobs() -> None:
    with pytest.raises(ValueError, match="jobs"):
        DagExecutor(["a"], {}, jobs=0)


def test_interrupt_propagates() -> None:
    def fn(n: str) -> str:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        DagExecutor(["a"], {}, jobs=1).run(fn)


@st.composite
def dags(draw: st.DrawFn) -> tuple[list[str], dict[str, set[str]]]:
    n = draw(st.integers(1, 12))
    order = [f"s{i}" for i in range(n)]
    upstream: dict[str, set[str]] = {}
    for i, name in enumerate(order):
        ups = draw(st.sets(st.integers(0, max(i - 1, 0)), max_size=3)) if i else set()
        upstream[name] = {order[j] for j in ups if j < i}
    return order, upstream


@settings(max_examples=60, deadline=None)
@given(dags(), st.integers(1, 4))
def test_property_every_node_runs_once_after_its_upstream(
    dag: tuple[list[str], dict[str, set[str]]], jobs: int
) -> None:
    order, upstream = dag
    done: list[str] = []
    lock = threading.Lock()

    def fn(n: str) -> str:
        with lock:
            assert upstream[n] <= set(done)
            done.append(n)
        return n

    report = DagExecutor(order, upstream, jobs=jobs).run(fn)
    assert report.ok
    assert sorted(done) == sorted(order)
    assert report.max_parallel <= jobs
