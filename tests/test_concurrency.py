"""Concurrent runs: file locking keeps processes from duplicating or corrupting work."""

from __future__ import annotations

import subprocess
import sys
import threading
from pathlib import Path

import pytest
from conftest import WsFactory

from dagcache import LockTimeoutError, Runner, collect
from dagcache.locking import FileLock
from dagcache.manifest import ActionManifest, KeyComponents

SLOW_DIAMOND = """\
stages:
  root:
    cmd: echo run >> .count-root && sleep 0.6 && echo r > r.txt
    outs: [r.txt]
  left:
    cmd: echo run >> .count-left && sleep 0.4 && cat r.txt > l.txt
    deps: [r.txt]
    outs: [l.txt]
  right:
    cmd: echo run >> .count-right && sleep 0.4 && cat r.txt > rr.txt
    deps: [r.txt]
    outs: [rr.txt]
  join:
    cmd: echo run >> .count-join && cat l.txt rr.txt > j.txt
    deps: [l.txt, rr.txt]
    outs: [j.txt]
"""


def _spawn(root: Path, jobs: int) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "dagcache",
            "-f",
            str(root / "dagcache.yaml"),
            "repro",
            "-q",
            "-j",
            str(jobs),
            "--json",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_concurrent_processes_run_each_stage_exactly_once(make_ws: WsFactory) -> None:
    ws = make_ws(SLOW_DIAMOND)
    procs = [_spawn(ws.root, jobs) for jobs in (1, 2, 3)]
    outputs = [p.communicate(timeout=120) for p in procs]
    for p, (_out, err) in zip(procs, outputs, strict=True):
        assert p.returncode == 0, err
    for stage in ("root", "left", "right", "join"):
        count = (ws.root / f".count-{stage}").read_text().splitlines()
        assert len(count) == 1, f"{stage} ran {len(count)} times"
    lock = ws.lockfile.read()
    assert set(lock) == {"root", "left", "right", "join"}
    assert (ws.root / "j.txt").read_text() == "r\nr\n"


def test_threads_in_one_process_share_safely(make_ws: WsFactory) -> None:
    ws = make_ws(SLOW_DIAMOND)
    errors: list[BaseException] = []

    def go() -> None:
        try:
            other = make_ws(root=ws.root)
            assert Runner(other, jobs=2).repro().ok
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=go) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert len((ws.root / ".count-root").read_text().splitlines()) == 1


def test_gc_waits_for_running_repro(make_ws: WsFactory) -> None:
    ws = make_ws(SLOW_DIAMOND, lock_timeout=0.3)
    with FileLock(ws.repo_lock_path, shared=True), pytest.raises(LockTimeoutError):
        collect(ws)


def test_repro_waits_for_gc(make_ws: WsFactory) -> None:
    ws = make_ws(SLOW_DIAMOND, lock_timeout=0.3)
    with FileLock(ws.repo_lock_path, shared=False), pytest.raises(LockTimeoutError):
        Runner(ws).repro()


def test_shared_locks_coexist_exclusive_excludes(tmp_path: Path) -> None:
    path = tmp_path / "l.lock"
    with FileLock(path, shared=True), FileLock(path, shared=True, timeout=0.2):
        pass
    with FileLock(path) as held:
        assert held.locked
        with pytest.raises(LockTimeoutError):
            FileLock(path, timeout=0.15).acquire()
        with pytest.raises(LockTimeoutError):
            FileLock(path, shared=True, timeout=0.15).acquire()
        with pytest.raises(RuntimeError):
            held.acquire()
    assert path.read_text().strip().isdigit()  # holder pid recorded
    FileLock(path, timeout=0.1).acquire().release()


def test_lock_released_when_holder_waits(tmp_path: Path) -> None:
    path = tmp_path / "w.lock"
    first = FileLock(path).acquire()
    timer = threading.Timer(0.7, first.release)
    timer.start()
    second = FileLock(path, timeout=5, poll_interval=0.02).acquire()
    assert second.waited >= 0.5
    second.release()
    timer.join()


def _manifest(name: str) -> ActionManifest:
    comps = KeyComponents("cmd", f"echo {name}", {}, {}, {}, ())
    return ActionManifest(comps.key, name, comps, (), {}, "2026-01-01T00:00:00+00:00", 0.0)


def test_concurrent_lock_file_updates_do_not_lose_entries(make_ws: WsFactory) -> None:
    ws = make_ws("stages: {}\n")

    def update(i: int) -> None:
        ws.lockfile.update({f"s{i}": _manifest(f"s{i}")})

    threads = [threading.Thread(target=update, args=(i,)) for i in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert set(ws.lockfile.read()) == {f"s{i}" for i in range(16)}
