"""End-to-end CLI behaviour and exit codes."""

from __future__ import annotations

import io
import json
import os
from pathlib import Path

import pytest
from conftest import CHAIN_PARAMS, CHAIN_YAML, write

from dagcache.cli import _human_bytes, main


@pytest.fixture
def proj(tmp_path: Path) -> Path:
    root = tmp_path / "proj"
    write(root / "dagcache.yaml", CHAIN_YAML)
    write(root / "params.yaml", CHAIN_PARAMS)
    write(root / "raw.txt", "hello world\n")
    return root


def run(proj: Path, *args: str) -> tuple[int, str]:
    out = io.StringIO()
    rc = main(["-f", str(proj / "dagcache.yaml"), *args], out=out)
    return rc, out.getvalue()


def test_repro_status_and_check(proj: Path) -> None:
    rc, out = run(proj, "status", "--check")
    assert rc == 1 and "never run" in out and "3 of 3 stage(s) need work" in out
    rc, out = run(proj, "repro", "-j", "2")
    assert rc == 0
    assert "repro: 3 stage(s), jobs=2" in out and "3 ran" in out
    assert "  ran        prepare" in out
    rc, out = run(proj, "status", "--check")
    assert rc == 0 and "0 of 3 stage(s) need work" in out
    rc, out = run(proj, "repro", "-q")
    assert rc == 0 and out.startswith("done in") and "3 up-to-date" in out


def test_status_json_and_dry_run(proj: Path) -> None:
    run(proj, "repro")
    write(proj / "params.yaml", CHAIN_PARAMS.replace("lr: 0.1", "lr: 0.3"))
    rc, out = run(proj, "status", "--json")
    data = {d["stage"]: d for d in json.loads(out)}
    assert rc == 0 and data["train"]["state"] == "stale"
    assert data["train"]["reasons"] == ["param train.lr changed: 0.1 → 0.3"]
    rc, out = run(proj, "repro", "--dry-run")
    assert rc == 0 and "train" in out and "stale" in out
    assert "lr=0.1" in (proj / "models/model.txt").read_text()  # nothing ran


def test_repro_json_and_targets(proj: Path) -> None:
    rc, out = run(proj, "repro", "--json", "-q", "train")
    payload = json.loads(out[: out.rindex("]") + 1])
    assert rc == 0 and [r["stage"] for r in payload] == ["prepare", "train"]


def test_failure_exit_code_and_not_run(proj: Path, capsys: pytest.CaptureFixture[str]) -> None:
    write(proj / "dagcache.yaml", CHAIN_YAML.replace("wc -c <", "exit 7; wc -c <"))
    rc, out = run(proj, "repro", "-v")
    assert rc == 1
    assert "FAILED     evaluate" in out and "status 7" in out and "start      prepare" in out
    write(proj / "dagcache.yaml", CHAIN_YAML.replace("cat data/clean.txt", "false && cat"))
    rc, out = run(proj, "repro")
    assert rc == 1 and "not run    evaluate" in out


def test_config_error_exit_code(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    write(tmp_path / "dagcache.yaml", "stages:\n  a: {}\n")
    rc = main(["-f", str(tmp_path / "dagcache.yaml"), "status"], out=io.StringIO())
    assert rc == 2 and "'cmd' is required" in capsys.readouterr().err
    write(
        tmp_path / "dagcache.yaml",
        "stages:\n  a:\n    cmd: cat x > y\n    deps: [x]\n    outs: [y]\n",
    )
    rc = main(["-f", str(tmp_path / "dagcache.yaml"), "status"], out=io.StringIO())
    assert rc == 2  # error state: missing dependency


def test_dag_lineage_gc_cache(proj: Path) -> None:
    run(proj, "repro")
    rc, out = run(proj, "dag", "--status")
    assert rc == 0 and "train  [up-to-date]  <- prepare" in out
    assert "flowchart LR" in run(proj, "dag", "--format", "mermaid")[1]
    assert "digraph pipeline" in run(proj, "dag", "--format", "dot")[1]
    rc, out = run(proj, "lineage", "metrics/score.txt")
    assert rc == 0 and "stage evaluate" in out and "raw.txt" in out
    assert json.loads(run(proj, "lineage", "metrics/score.txt", "--format", "json")[1])["path"]
    assert run(proj, "lineage", "metrics/score.txt", "--format", "mermaid")[1].startswith(
        "flowchart"
    )
    assert run(proj, "lineage", "metrics/score.txt", "--format", "dot")[1].startswith("digraph")
    write(proj / "params.yaml", CHAIN_PARAMS.replace("lr: 0.1", "lr: 0.3"))
    run(proj, "repro")
    rc, out = run(proj, "gc", "--dry-run")
    assert rc == 0 and "would remove 2 action(s)" in out
    rc, out = run(proj, "gc", "--json")
    assert json.loads(out)["actions_removed"] == 2
    rc, out = run(proj, "cache", "stats")
    assert rc == 0 and "3 action(s)" in out
    rc, out = run(proj, "cache", "verify")
    assert rc == 0 and "0 corrupt" in out
    obj = next((proj / ".dagcache/cache/objects").rglob("*"))
    while obj.is_dir():
        obj = next(obj.iterdir())
    os.chmod(obj, 0o644)
    obj.write_text("rot")
    rc, out = run(proj, "cache", "verify")
    assert rc == 1 and "corrupt object" in out


def test_push_pull(proj: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc, _ = run(proj, "push")
    assert rc == 2 and "no remote configured" in capsys.readouterr().err
    rc, _ = run(proj, "pull")
    assert rc == 2
    remote = tmp_path / "remote"
    write(proj / "dagcache.yaml", f"cache:\n  remote: {remote}\n" + CHAIN_YAML)
    run(proj, "repro")
    rc, out = run(proj, "push")
    assert rc == 0 and "pushed 3 action(s), 3 new object(s)" in out
    rc, out = run(proj, "pull", "train")
    assert rc == 0 and "pulled 2 action(s), 0 object(s)" in out


def test_init(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    out = io.StringIO()
    assert main(["init"], out=out) == 0 and "created" in out.getvalue()
    assert main(["init"], out=out) == 0 and "already exists" in out.getvalue()
    out = io.StringIO()
    assert main(["repro"], out=out) == 0  # discovered from cwd
    assert (tmp_path / "out/hello.txt").read_text() == "hello world\n"


def test_env_log_level_and_version(
    proj: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("DAGCACHE_LOG", "debug")
    assert run(proj, "status")[0] == 0  # DAGCACHE_LOG=debug path
    with pytest.raises(SystemExit):
        main(["--version"])
    assert "dagcache" in capsys.readouterr().out


def test_human_bytes() -> None:
    assert _human_bytes(10) == "10 B"
    assert _human_bytes(2048) == "2.0 KiB"
    assert _human_bytes(5 * 1024**3) == "5.0 GiB"
