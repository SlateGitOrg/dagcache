"""Runner behaviour: failures, targets, outputs, logs, link modes, corruption."""

from __future__ import annotations

import os
import stat
import time

from conftest import WsFactory, actions, repro, write

from dagcache import Action, Pipeline, Runner, StageContext, Workspace


def test_failing_command_reports_tail_and_log(make_ws: WsFactory) -> None:
    ws = make_ws("stages:\n  bad:\n    cmd: echo before-fail; exit 3\n    outs: [o]\n")
    report = repro(ws)
    res = report.by_name()["bad"]
    assert res.action is Action.FAILED
    assert res.error is not None
    assert "status 3" in res.error and "before-fail" in res.error
    assert ".dagcache/logs/bad.log" in res.error
    assert (ws.root / ".dagcache/logs/bad.log").read_text().strip() == "before-fail"
    assert not report.ok
    assert "bad" not in ws.lockfile.read()


def test_missing_declared_output_fails(make_ws: WsFactory) -> None:
    ws = make_ws("stages:\n  s:\n    cmd: 'true'\n    outs: [never.txt]\n")
    res = repro(ws).by_name()["s"]
    assert res.action is Action.FAILED
    assert "did not produce declared output 'never.txt'" in str(res.error)


def test_missing_dependency_fails_stage(make_ws: WsFactory) -> None:
    ws = make_ws("stages:\n  s:\n    cmd: cat nope > o\n    deps: [nope]\n    outs: [o]\n")
    res = repro(ws).by_name()["s"]
    assert res.action is Action.FAILED
    assert "dependency 'nope' does not exist" in str(res.error)


def test_failure_marks_downstream_not_run(make_ws: WsFactory) -> None:
    ws = make_ws(
        "stages:\n"
        "  a:\n    cmd: exit 1\n    outs: [a]\n"
        "  b:\n    cmd: cat a > b\n    deps: [a]\n    outs: [b]\n"
    )
    report = repro(ws)
    assert actions(report) == {"a": "failed", "b": "not-run"}
    assert report.by_name()["b"].reasons == ["upstream stage 'a' failed"]


def test_stale_outputs_are_removed_before_running(make_ws: WsFactory) -> None:
    ws = make_ws("stages:\n  s:\n    cmd: test ! -e o && echo new > o\n    outs: [o]\n")
    (ws.root / "o").write_text("stale leftovers")
    assert actions(repro(ws)) == {"s": "ran"}


def test_targets_run_only_upstream_closure(chain_ws: Workspace) -> None:
    report = repro(chain_ws, "train")
    assert actions(report) == {"prepare": "ran", "train": "ran"}
    assert not (chain_ws.root / "metrics").exists()


def test_removed_stage_is_pruned_from_lock_file(make_ws: WsFactory) -> None:
    two = (
        "stages:\n  a:\n    cmd: echo a > a\n    outs: [a]\n"
        "  b:\n    cmd: echo b > b\n    outs: [b]\n"
    )
    ws = make_ws(two)
    repro(ws)
    assert set(ws.lockfile.read()) == {"a", "b"}
    ws = make_ws("stages:\n  a:\n    cmd: echo a > a\n    outs: [a]\n", root=ws.root)
    repro(ws)
    assert set(ws.lockfile.read()) == {"a"}


def test_directory_outputs_round_trip_with_exec_bit(make_ws: WsFactory) -> None:
    ws = make_ws(
        "stages:\n  s:\n    cmd: mkdir -p out/sub && echo 1 > out/a && echo 2 > out/sub/b"
        " && printf '#!/bin/sh\\n' > out/run.sh && chmod +x out/run.sh && echo ${v} > out/v\n"
        "    outs: [out]\n",
        "v: 1\n",
    )
    repro(ws)
    write(ws.root / "params.yaml", "v: 2\n")
    repro(make_ws(root=ws.root))
    write(ws.root / "params.yaml", "v: 1\n")
    report = repro(make_ws(root=ws.root))
    assert actions(report) == {"s": "restored"}
    out = ws.root / "out"
    assert (out / "sub/b").read_text() == "2\n"
    assert (out / "v").read_text() == "1\n"
    assert os.stat(out / "run.sh").st_mode & stat.S_IXUSR
    assert not [p for p in ws.root.iterdir() if p.name.startswith(".out.")]


def test_hardlink_mode_shares_inode_and_is_read_only(make_ws: WsFactory) -> None:
    ws = make_ws(
        "cache:\n  link: hardlink\nstages:\n  s:\n    cmd: echo ${v} > o.txt\n    outs: [o.txt]\n",
        "v: 1\n",
    )
    repro(ws)
    (ws.root / "o.txt").unlink()
    assert actions(repro(make_ws(root=ws.root))) == {"s": "restored"}
    st = (ws.root / "o.txt").stat()
    obj = ws.local.object_path(ws.lockfile.read()["s"].outputs[0].digest)
    assert st.st_ino == obj.stat().st_ino
    assert not st.st_mode & stat.S_IWUSR


def test_corrupt_cache_object_falls_back_to_running(make_ws: WsFactory) -> None:
    ws = make_ws("stages:\n  s:\n    cmd: echo ${v} > o.txt\n    outs: [o.txt]\n", "v: 1\n")
    repro(ws)
    digest = ws.lockfile.read()["s"].outputs[0].digest
    obj = ws.local.object_path(digest)
    os.chmod(obj, 0o644)
    obj.write_text("bit rot")
    (ws.root / "o.txt").unlink()
    report = repro(make_ws(root=ws.root))
    assert actions(report) == {"s": "ran"}
    assert (ws.root / "o.txt").read_text() == "1\n"


def test_force_reruns_everything(chain_ws: Workspace) -> None:
    repro(chain_ws)
    report = repro(chain_ws, force=True)
    assert set(actions(report).values()) == {"ran"}
    assert report.by_name()["train"].reasons[0] == "forced"


def test_python_stage_exception_is_reported(py_ws: object) -> None:
    pipe = Pipeline("p")

    @pipe.stage(outs=["x"])
    def boom(ctx: StageContext) -> None:
        raise ValueError("bad input")

    ws = py_ws(pipe)  # type: ignore[operator]
    res = repro(ws).by_name()["boom"]
    assert res.action is Action.FAILED
    assert "ValueError: bad input" in str(res.error)


def test_python_stage_receives_params(py_ws: object) -> None:
    pipe = Pipeline("p")
    seen: dict[str, object] = {}

    @pipe.stage(outs=["x"], params=["a.b"])
    def s(ctx: StageContext) -> None:
        seen.update(ctx.params)
        ctx.path("x").write_text("ok")
        ctx.log.info("wrote x")

    ws = py_ws(pipe)  # type: ignore[operator]
    write(ws.root / "params.yaml", "a:\n  b: [1, 2]\n")
    assert actions(repro(ws)) == {"s": "ran"}
    assert seen == {"a.b": [1, 2]}


def test_parallel_independent_stages_overlap(make_ws: WsFactory) -> None:
    stages = "".join(
        f"  s{i}:\n    cmd: sleep 0.4 && echo {i} > o{i}\n    outs: [o{i}]\n" for i in range(4)
    )
    ws = make_ws("stages:\n" + stages)
    t0 = time.monotonic()
    report = Runner(ws, jobs=4).repro()
    assert report.ok and report.max_parallel == 4
    assert time.monotonic() - t0 < 1.4  # serial would be >= 1.6s


def test_callbacks_and_output_streaming(make_ws: WsFactory) -> None:
    ws = make_ws("stages:\n  s:\n    cmd: echo line1 && echo line2 > o\n    outs: [o]\n")
    started: list[str] = []
    finished: list[str] = []
    lines: list[tuple[str, str]] = []
    Runner(
        ws,
        on_start=started.append,
        on_finish=lambda r: finished.append(r.action.value),
        on_output=lambda n, t: lines.append((n, t)),
    ).repro()
    assert started == ["s"] and finished == ["ran"]
    assert lines == [("s", "line1")]
