"""Cache invalidation: code, params and data changes, including transitive effects."""

from __future__ import annotations

import os
import time
from pathlib import Path

from conftest import WsFactory, actions, repro, run_count, write

from dagcache import Pipeline, StageContext, Workspace


def reload(make_ws: WsFactory, ws: Workspace) -> Workspace:
    """Fresh Workspace object on the same root (new process semantics)."""
    return make_ws(root=ws.root)


def test_first_run_runs_everything_then_noop(chain_ws: Workspace, make_ws: WsFactory) -> None:
    first = repro(chain_ws)
    assert actions(first) == {"prepare": "ran", "train": "ran", "evaluate": "ran"}
    second = repro(reload(make_ws, chain_ws))
    assert actions(second) == {
        "prepare": "up-to-date",
        "train": "up-to-date",
        "evaluate": "up-to-date",
    }
    assert run_count(chain_ws, "prepare") == 1


def test_data_change_invalidates_transitively(chain_ws: Workspace, make_ws: WsFactory) -> None:
    repro(chain_ws)
    (chain_ws.root / "raw.txt").write_text("different data\n")
    report = repro(reload(make_ws, chain_ws))
    assert actions(report) == {"prepare": "ran", "train": "ran", "evaluate": "ran"}
    by = report.by_name()
    assert any("dependency raw.txt changed" in r for r in by["prepare"].reasons)
    assert any("dependency data/clean.txt changed" in r for r in by["train"].reasons)


def test_param_change_invalidates_only_consumer_and_downstream(
    chain_ws: Workspace, make_ws: WsFactory
) -> None:
    repro(chain_ws)
    write(
        chain_ws.root / "params.yaml",
        "prep:\n  mode: upper\ntrain:\n  lr: 0.5\nunrelated:\n  note: hello\n",
    )
    report = repro(reload(make_ws, chain_ws))
    assert actions(report) == {"prepare": "up-to-date", "train": "ran", "evaluate": "ran"}
    assert report.by_name()["train"].reasons == ["param train.lr changed: 0.1 → 0.5"]


def test_unrelated_param_change_invalidates_nothing(
    chain_ws: Workspace, make_ws: WsFactory
) -> None:
    repro(chain_ws)
    write(
        chain_ws.root / "params.yaml",
        "prep:\n  mode: upper\ntrain:\n  lr: 0.1\nunrelated:\n  note: bye\n",
    )
    report = repro(reload(make_ws, chain_ws))
    assert set(actions(report).values()) == {"up-to-date"}


def test_command_change_invalidates(chain_ws: Workspace, make_ws: WsFactory) -> None:
    repro(chain_ws)
    yaml_path = chain_ws.root / "dagcache.yaml"
    yaml_path.write_text(yaml_path.read_text().replace("wc -c <", "wc -l <"))
    report = repro(reload(make_ws, chain_ws))
    assert actions(report)["evaluate"] == "ran"
    assert report.by_name()["evaluate"].reasons[0].startswith("command changed")
    assert actions(report)["train"] == "up-to-date"


def test_revert_restores_from_cache_without_running(
    chain_ws: Workspace, make_ws: WsFactory
) -> None:
    repro(chain_ws)
    params = chain_ws.root / "params.yaml"
    original = params.read_text()
    params.write_text(original.replace("lr: 0.1", "lr: 0.9"))
    repro(reload(make_ws, chain_ws))
    params.write_text(original)
    report = repro(reload(make_ws, chain_ws))
    assert actions(report) == {"prepare": "up-to-date", "train": "restored", "evaluate": "restored"}
    assert run_count(chain_ws, "train") == 2  # not a third time
    assert "lr=0.1" in (chain_ws.root / "models/model.txt").read_text()


def test_deleted_output_is_restored(chain_ws: Workspace, make_ws: WsFactory) -> None:
    repro(chain_ws)
    (chain_ws.root / "models/model.txt").unlink()
    report = repro(reload(make_ws, chain_ws))
    assert actions(report)["train"] == "restored"
    assert report.by_name()["train"].reasons == ["output models/model.txt missing"]
    assert run_count(chain_ws, "train") == 1


def test_modified_output_is_restored(chain_ws: Workspace, make_ws: WsFactory) -> None:
    repro(chain_ws)
    (chain_ws.root / "metrics/score.txt").write_text("tampered")
    report = repro(reload(make_ws, chain_ws))
    assert actions(report)["evaluate"] == "restored"
    assert "modified" in report.by_name()["evaluate"].reasons[0]
    assert (chain_ws.root / "metrics/score.txt").read_text() != "tampered"


def test_early_cutoff_identical_output_stops_propagation(
    chain_ws: Workspace, make_ws: WsFactory
) -> None:
    repro(chain_ws)
    # Same content, new bytes on disk: prepare's input digest is unchanged -> nothing runs.
    (chain_ws.root / "raw.txt").write_text("hello world\n")
    assert set(actions(repro(reload(make_ws, chain_ws))).values()) == {"up-to-date"}
    # Change prepare's command in a way that yields identical output.
    yaml_path = chain_ws.root / "dagcache.yaml"
    yaml_path.write_text(yaml_path.read_text().replace(".upper())", ".upper().strip() + chr(10))"))
    report = repro(reload(make_ws, chain_ws))
    assert actions(report) == {"prepare": "ran", "train": "up-to-date", "evaluate": "up-to-date"}


def test_env_var_tracked_without_storing_value(make_ws: WsFactory, monkeypatch: object) -> None:
    import pytest

    mp: pytest.MonkeyPatch = monkeypatch  # type: ignore[assignment]
    ws = make_ws(
        "stages:\n  s:\n    cmd: echo $MODE > out.txt\n    outs: [out.txt]\n    env: [MODE]\n"
    )
    mp.setenv("MODE", "secret-a")
    repro(ws)
    lock_text = (ws.root / "dagcache.lock").read_text()
    assert "secret-a" not in lock_text
    mp.setenv("MODE", "secret-b")
    report = repro(make_ws(root=ws.root))
    assert actions(report) == {"s": "ran"}
    assert report.by_name()["s"].reasons == ["env MODE changed"]


def test_salt_and_outs_changes_invalidate(make_ws: WsFactory) -> None:
    ws = make_ws("stages:\n  s:\n    cmd: echo a > a.txt\n    outs: [a.txt]\n")
    repro(ws)
    ws = make_ws(
        "stages:\n  s:\n    cmd: echo a > a.txt\n    outs: [a.txt]\n    salt: v2\n", root=ws.root
    )
    report = repro(ws)
    assert report.by_name()["s"].reasons == ["salt changed: '' → 'v2'"]
    ws = make_ws(
        "stages:\n  s:\n    cmd: echo a > a.txt; echo b > b.txt\n    outs: [a.txt, b.txt]\n"
        "    salt: v2\n",
        root=ws.root,
    )
    reasons = repro(ws).by_name()["s"].reasons
    assert reasons[0].startswith("command changed")
    assert reasons[1] == "outputs changed: ['a.txt'] → ['a.txt', 'b.txt']"


def test_code_file_comment_change_is_ignored_but_logic_change_is_not(make_ws: WsFactory) -> None:
    yaml = "stages:\n  s:\n    cmd: python3 step.py\n    code: [step.py]\n    outs: [o.txt]\n"
    ws = make_ws(yaml)
    write(ws.root / "step.py", "open('o.txt', 'w').write('1')\n")
    repro(ws)
    write(ws.root / "step.py", "# a comment\nopen('o.txt',   'w').write('1')  # trailing\n")
    assert actions(repro(make_ws(root=ws.root))) == {"s": "up-to-date"}
    write(ws.root / "step.py", "open('o.txt', 'w').write('2')\n")
    report = repro(make_ws(root=ws.root))
    assert actions(report) == {"s": "ran"}
    assert report.by_name()["s"].reasons[0].startswith("code file step.py changed")


def test_non_python_code_file_is_hashed_raw(make_ws: WsFactory) -> None:
    ws = make_ws("stages:\n  s:\n    cmd: sh run.sh\n    code: [run.sh]\n    outs: [o.txt]\n")
    write(ws.root / "run.sh", "echo 1 > o.txt\n")
    repro(ws)
    write(ws.root / "run.sh", "echo 1 > o.txt  # comment\n")
    assert actions(repro(make_ws(root=ws.root))) == {"s": "ran"}


def _py_pipeline(root: Path, helper_body: str) -> Pipeline:
    """Build a decorator pipeline whose stage calls a helper from a local module."""
    import importlib.util
    import sys

    write(root / "localhelpers.py", helper_body)
    spec = importlib.util.spec_from_file_location("localhelpers", root / "localhelpers.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["localhelpers"] = mod
    spec.loader.exec_module(mod)
    pipe = Pipeline("py")

    @pipe.stage(outs=["a.txt"])
    def first(ctx: StageContext) -> None:
        ctx.path("a.txt").write_text(str(mod.helper(2)))

    @pipe.stage(deps=["a.txt"], outs=["b.txt"])
    def second(ctx: StageContext) -> None:
        ctx.path("b.txt").write_text(ctx.path("a.txt").read_text() * 2)

    return pipe


def test_python_helper_change_invalidates_transitively(tmp_path: Path) -> None:
    root = tmp_path / "pyws"
    root.mkdir()
    pipe = _py_pipeline(root, "def helper(x):\n    return x + 1\n")
    ws = Workspace(root, pipe)
    assert actions(repro(ws)) == {"first": "ran", "second": "ran"}
    ws.close()
    # Reformat + comment only: same AST, nothing runs.
    pipe = _py_pipeline(root, "# note\ndef helper(x):\n\n    return x+1  # same\n")
    ws = Workspace(root, pipe)
    assert actions(repro(ws)) == {"first": "up-to-date", "second": "up-to-date"}
    ws.close()
    pipe = _py_pipeline(root, "def helper(x):\n    return x + 2\n")
    ws = Workspace(root, pipe)
    report = repro(ws)
    ws.close()
    assert actions(report) == {"first": "ran", "second": "ran"}
    assert any("module localhelpers changed" in r for r in report.by_name()["first"].reasons)
    assert (root / "b.txt").read_text() == "44"


def test_stat_cache_does_not_hide_same_size_rewrite(
    chain_ws: Workspace, make_ws: WsFactory
) -> None:
    repro(chain_ws)
    raw = chain_ws.root / "raw.txt"
    st = raw.stat()
    raw.write_text("HELLO WORLD\n")  # same size
    os.utime(raw, ns=(st.st_atime_ns, st.st_mtime_ns))  # even restore the old mtime
    time.sleep(0.01)
    report = repro(reload(make_ws, chain_ws))
    assert actions(report)["prepare"] == "ran"  # ctime/inode metadata still differs
