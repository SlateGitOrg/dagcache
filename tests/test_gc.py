"""Garbage collection keeps what the lock file references and frees the rest."""

from __future__ import annotations

import os
import shutil
import time
from pathlib import Path

from conftest import WsFactory, actions, repro, write

from dagcache import Workspace, collect

YAML = (
    "stages:\n"
    "  a:\n    cmd: mkdir -p d && echo ${v} > d/x && echo same > d/y\n    outs: [d]\n"
    "  b:\n    cmd: cat d/x > b.txt\n    deps: [d]\n    outs: [b.txt]\n"
)


def _two_generations(make_ws: WsFactory) -> Workspace:
    ws = make_ws(YAML, "v: 1\n")
    repro(ws)
    write(ws.root / "params.yaml", "v: 2\n")
    ws = make_ws(root=ws.root)
    repro(ws)
    return ws


def test_dry_run_reports_without_deleting(make_ws: WsFactory) -> None:
    ws = _two_generations(make_ws)
    before = sorted(ws.local.iter_objects())
    report = collect(ws, dry_run=True)
    assert report.actions_removed == 2 and report.actions_kept == 2
    # old generation: tree(v=1) and the blob "1\n" (d/x and b.txt share it);
    # "same\n" (d/y) is still referenced by the current tree.
    assert report.objects_removed == 2
    assert report.bytes_freed > 0
    assert sorted(ws.local.iter_objects()) == before


def test_gc_frees_old_generation_and_current_still_restores(make_ws: WsFactory) -> None:
    ws = _two_generations(make_ws)
    report = collect(ws)
    assert report.objects_removed == 2 and report.actions_removed == 2
    assert len(list(ws.local.iter_actions())) == 2
    shutil.rmtree(ws.root / "d")
    (ws.root / "b.txt").unlink()
    assert actions(repro(make_ws(root=ws.root))) == {"a": "restored", "b": "restored"}
    # The old generation is gone: reverting the param now has to recompute.
    write(ws.root / "params.yaml", "v: 1\n")
    assert actions(repro(make_ws(root=ws.root))) == {"a": "ran", "b": "ran"}


def test_keep_days_retains_recently_used(make_ws: WsFactory) -> None:
    ws = _two_generations(make_ws)
    report = collect(ws, keep_days=1)
    assert report.actions_removed == 0 and report.objects_removed == 0
    # Age every action to 10 days; the lock-file roots survive regardless.
    old = time.time() - 10 * 86400
    for key, _ in ws.local.iter_actions():
        os.utime(ws.local.actions_dir / key[:2] / f"{key[2:]}.json", (old, old))
    assert collect(ws, keep_days=1).actions_removed == 2


def test_extra_lock_file_roots(make_ws: WsFactory, tmp_path: Path) -> None:
    ws = make_ws(YAML, "v: 1\n")
    repro(ws)
    saved = tmp_path / "branch.lock"
    shutil.copy(ws.root / "dagcache.lock", saved)
    write(ws.root / "params.yaml", "v: 2\n")
    ws = make_ws(root=ws.root)
    repro(ws)
    report = collect(ws, extra_lock_files=[saved])
    assert report.actions_removed == 0 and report.objects_removed == 0


def test_gc_removes_orphans_and_stale_tmp(make_ws: WsFactory) -> None:
    ws = make_ws(YAML, "v: 1\n")
    repro(ws)
    orphan = b"orphan object"
    import hashlib

    ws.local.put_object_bytes(hashlib.sha256(orphan).hexdigest(), orphan)
    stale = ws.local.tmp_dir / "abandoned"
    stale.write_text("x")
    os.utime(stale, (time.time() - 7200, time.time() - 7200))
    report = collect(ws)
    assert report.objects_removed == 1 and report.tmp_removed == 1
    assert report.to_dict()["objects_removed"] == 1


def test_remote_gc(make_ws: WsFactory, tmp_path: Path) -> None:
    yaml = f"cache:\n  remote: {tmp_path / 'remote'}\n  autopush: true\n" + YAML
    ws = make_ws(yaml, "v: 1\n")
    repro(ws)
    write(ws.root / "params.yaml", "v: 2\n")
    ws = make_ws(root=ws.root)
    repro(ws)
    report = collect(ws, remote=True)
    assert report.location.endswith("remote")
    assert report.actions_removed == 2 and report.objects_removed == 2
