"""Micro-benchmarks for dagcache. Usage: ``make bench``.

Measures, on synthetic workloads generated here (no external data):

1. hashing throughput of a 256 MiB file, cold vs. stat-cache hit;
2. scheduling/caching overhead on a 300-stage DAG: first run, no-op re-run, ``status``;
3. wall-clock speed-up from parallel execution of independent stages.

Prints a Markdown table. Numbers depend on the machine and its current load.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

from dagcache import Pipeline, Runner, Stage, Workspace, plan
from dagcache.hashing import Hasher, StatCache

ROWS: list[tuple[str, str, str]] = []


def record(name: str, value: str, note: str = "") -> None:
    ROWS.append((name, value, note))
    print(f"  {name}: {value} {note}", flush=True)


def bench_hashing(tmp: Path) -> None:
    size = 256 * 1024 * 1024
    path = tmp / "big.bin"
    chunk = os.urandom(1024 * 1024)
    with path.open("wb") as fh:
        for _ in range(size // len(chunk)):
            fh.write(chunk)
    old = time.time() - 60
    os.utime(path, (old, old))  # outside the racy window, so the memo is trusted
    cache = StatCache(tmp / "state.db")
    hasher = Hasher(tmp, cache)
    t0 = time.perf_counter()
    hasher.digest_path("big.bin")
    cold = time.perf_counter() - t0
    t0 = time.perf_counter()
    for _ in range(100):
        hasher.digest_path("big.bin")
    warm = (time.perf_counter() - t0) / 100
    cache.close()
    record(
        "SHA-256 hashing, 256 MiB file (cold)", f"{size / cold / 1e9:.2f} GB/s", f"({cold:.3f}s)"
    )
    record("same file, stat-cache hit", f"{warm * 1e6:.0f} µs", "per lookup")


def wide_dag(n: int) -> Pipeline:
    """n stages: 10 independent roots, each later stage depends on two earlier ones."""
    pipe = Pipeline("bench")

    def make(i: int) -> None:
        deps = () if i < 10 else (f"out/{i - 10}.txt", f"out/{(i * 7) % (i - 1)}.txt")

        def fn(ctx: object, i: int = i) -> None:
            Path(ctx.root, f"out/{i}.txt").write_text(f"{i}\n")  # type: ignore[attr-defined]

        pipe.add(Stage(name=f"s{i}", func=fn, deps=deps, outs=(f"out/{i}.txt",)))

    for i in range(n):
        make(i)
    return pipe


def bench_overhead(tmp: Path) -> None:
    n = 300
    root = tmp / "dag"
    root.mkdir()
    (root / "out").mkdir()
    pipe = wide_dag(n)
    ws = Workspace(root, pipe)
    t0 = time.perf_counter()
    report = Runner(ws, jobs=4).repro()
    first = time.perf_counter() - t0
    assert report.ok
    ws.close()
    ws = Workspace(root, pipe)
    t0 = time.perf_counter()
    report = Runner(ws, jobs=4).repro()
    noop = time.perf_counter() - t0
    assert {r.action.value for r in report.results} == {"up-to-date"}
    t0 = time.perf_counter()
    statuses = plan(ws, ws.cache())
    status = time.perf_counter() - t0
    assert all(not s.state.needs_work for s in statuses)
    ws.close()
    record(
        f"first run, {n}-stage DAG (trivial stages)",
        f"{first:.2f} s",
        f"({first / n * 1e3:.1f} ms/stage)",
    )
    record(f"no-op repro, {n} stages", f"{noop:.2f} s", f"({noop / n * 1e3:.2f} ms/stage)")
    record(f"status, {n} stages", f"{status:.2f} s", f"({status / n * 1e3:.2f} ms/stage)")


def bench_parallel(tmp: Path) -> None:
    for jobs in (1, 4, 8):
        root = tmp / f"par{jobs}"
        root.mkdir()
        pipe = Pipeline("par")
        for i in range(8):
            pipe.add(Stage(name=f"p{i}", cmd=f"sleep 0.5 && echo {i} > o{i}", outs=(f"o{i}",)))
        ws = Workspace(root, pipe)
        t0 = time.perf_counter()
        report = Runner(ws, jobs=jobs).repro()
        elapsed = time.perf_counter() - t0
        ws.close()
        assert report.ok
        record(
            f"8 independent 0.5 s stages, jobs={jobs}",
            f"{elapsed:.2f} s",
            f"(max parallel {report.max_parallel})",
        )


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="dagcache-bench-"))
    try:
        print("hashing", flush=True)
        bench_hashing(tmp)
        print("overhead", flush=True)
        bench_overhead(tmp)
        print("parallelism", flush=True)
        bench_parallel(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print(f"\nPython {sys.version.split()[0]}, {os.cpu_count()} CPUs\n")
    print("| benchmark | result | detail |\n|---|---|---|")
    for name, value, note in ROWS:
        print(f"| {name} | {value} | {note} |")


if __name__ == "__main__":
    main()
