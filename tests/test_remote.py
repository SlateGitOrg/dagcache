"""Remote caches: a second clone restores everything without running a stage."""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path

import boto3
import pytest
from conftest import CHAIN_PARAMS, CHAIN_YAML, WsFactory, actions, repro, run_count, write
from moto import mock_aws

from dagcache import Workspace


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="team-cache")
        yield


def clone(src: Path, dest: Path) -> None:
    """Copy what would be committed to git: pipeline, params, lock file, sources."""
    dest.mkdir(parents=True)
    for name in ("dagcache.yaml", "params.yaml", "dagcache.lock", "raw.txt"):
        if (src / name).exists():
            shutil.copy(src / name, dest / name)


def _remote_yaml(remote: str, autopush: bool) -> str:
    cache = f"cache:\n  remote: {remote}\n  autopush: {'true' if autopush else 'false'}\n"
    return cache + CHAIN_YAML


@pytest.mark.parametrize("kind", ["dir", "s3"])
def test_push_then_fresh_clone_restores_everything(
    kind: str, tmp_path: Path, make_ws: WsFactory, request: pytest.FixtureRequest
) -> None:
    if kind == "s3":
        request.getfixturevalue("s3")
        remote = "s3://team-cache/proj"
    else:
        remote = str(tmp_path / "shared-remote")
    a = make_ws(_remote_yaml(remote, autopush=False), CHAIN_PARAMS, root=tmp_path / "a")
    write(a.root / "raw.txt", "hello world\n")
    assert repro(a).ok
    stats = a.cache().push(k.key for k in a.lockfile.read().values())
    assert stats.actions == 3 and stats.objects == 3 and stats.missing == 0
    again = a.cache().push(k.key for k in a.lockfile.read().values())
    assert again.objects == 0  # already there

    clone(a.root, tmp_path / "b")
    b = make_ws(root=tmp_path / "b")
    report = repro(b)
    assert actions(report) == {"prepare": "restored", "train": "restored", "evaluate": "restored"}
    assert run_count(b, "prepare") == 0
    assert (b.root / "metrics/score.txt").read_text() == (a.root / "metrics/score.txt").read_text()


def test_autopush_and_pull(tmp_path: Path, make_ws: WsFactory) -> None:
    remote = tmp_path / "remote"
    a = make_ws(_remote_yaml(str(remote), autopush=True), CHAIN_PARAMS, root=tmp_path / "a")
    write(a.root / "raw.txt", "hello world\n")
    repro(a)
    assert len(list(a.remote.iter_actions())) == 3  # type: ignore[union-attr]
    clone(a.root, tmp_path / "b")
    b = make_ws(root=tmp_path / "b")
    stats = b.cache().pull(e.key for e in b.lockfile.read().values())
    assert stats.actions == 3 and stats.objects == 3
    # Offline from now on: the local cache alone must be enough.
    report = repro(b, use_remote=False)
    assert set(actions(report).values()) == {"restored"}


def test_remote_tree_outputs(tmp_path: Path, make_ws: WsFactory) -> None:
    remote = tmp_path / "remote"
    yaml = (
        f"cache:\n  remote: {remote}\n  autopush: true\n"
        "stages:\n  s:\n    cmd: mkdir -p d/e && echo 1 > d/a && echo 2 > d/e/b\n    outs: [d]\n"
    )
    a = make_ws(yaml, root=tmp_path / "a")
    repro(a)
    clone(a.root, tmp_path / "b")
    b: Workspace = make_ws(root=tmp_path / "b")
    stats = b.cache().pull(e.key for e in b.lockfile.read().values())
    assert stats.objects == 3  # tree manifest + 2 files
    assert actions(repro(b)) == {"s": "restored"}
    assert (b.root / "d/e/b").read_text() == "2\n"


def test_pull_reports_missing(tmp_path: Path, make_ws: WsFactory) -> None:
    remote = tmp_path / "remote"
    a = make_ws(_remote_yaml(str(remote), autopush=False), CHAIN_PARAMS, root=tmp_path / "a")
    write(a.root / "raw.txt", "hello world\n")
    repro(a)
    stats = a.cache().pull(["0" * 64])
    assert stats.missing == 1 and stats.actions == 0
    assert a.cache().push(["f" * 64]).missing == 1
