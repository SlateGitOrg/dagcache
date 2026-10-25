"""``status``: states and human-readable reasons, without running anything."""

from __future__ import annotations

from conftest import WsFactory, repro, write

from dagcache import State, Workspace, plan


def states(ws: Workspace, **kw: object) -> dict[str, tuple[str, list[str]]]:
    return {
        s.name: (s.state.value, s.reasons)
        for s in plan(ws, ws.cache(), **kw)  # type: ignore[arg-type]
    }


def test_never_run_and_pending(chain_ws: Workspace) -> None:
    st = states(chain_ws)
    assert st["prepare"] == ("stale", ["never run (no entry in dagcache.lock)"])
    assert st["train"] == ("pending", ["upstream stage 'prepare' is stale"])
    assert st["evaluate"] == ("pending", ["upstream stage 'train' is pending"])


def test_all_up_to_date_after_run(chain_ws: Workspace) -> None:
    repro(chain_ws)
    assert {v[0] for v in states(chain_ws).values()} == {"up-to-date"}


def test_param_change_reason_and_pending_downstream(
    chain_ws: Workspace, make_ws: WsFactory
) -> None:
    repro(chain_ws)
    write(chain_ws.root / "params.yaml", "prep:\n  mode: lower\ntrain:\n  lr: 0.1\n")
    st = states(make_ws(root=chain_ws.root))
    assert st["prepare"] == ("stale", ["param prep.mode changed: upper → lower"])
    assert st["train"][0] == "pending"


def test_cached_state_uses_manifest_digest_for_downstream(
    chain_ws: Workspace, make_ws: WsFactory
) -> None:
    """After a revert, upstream is 'cached' and downstream keys resolve through it."""
    repro(chain_ws)
    params = chain_ws.root / "params.yaml"
    original = params.read_text()
    params.write_text(original.replace("lr: 0.1", "lr: 0.7"))
    repro(make_ws(root=chain_ws.root))
    params.write_text(original)
    st = states(make_ws(root=chain_ws.root))
    assert st["prepare"][0] == "up-to-date"
    assert st["train"] == ("cached", ["param train.lr changed: 0.7 → 0.1"])
    # evaluate's input digest is taken from train's cached manifest; that key is cached too.
    assert st["evaluate"][0] == "cached"
    assert st["evaluate"][1][0].startswith("dependency models/model.txt changed")


def test_missing_source_dependency_is_error(make_ws: WsFactory) -> None:
    ws = make_ws("stages:\n  s:\n    cmd: cat in.txt > o\n    deps: [in.txt]\n    outs: [o]\n")
    st = states(ws)
    assert st["s"] == ("error", ["dependency in.txt does not exist"])


def test_missing_param_is_error(make_ws: WsFactory) -> None:
    ws = make_ws("stages:\n  s:\n    cmd: echo ${a.b} > o\n    outs: [o]\n", "a: {}\n")
    st = states(ws)
    assert st["s"][0] == "error"
    assert "parameter 'a.b' not found" in st["s"][1][0]


def test_force_and_always_run(make_ws: WsFactory) -> None:
    ws = make_ws(
        "stages:\n"
        "  a:\n    cmd: echo a > a\n    outs: [a]\n"
        "  b:\n    cmd: date > b\n    outs: [b]\n    always_run: true\n"
    )
    repro(ws)
    st = states(ws)
    assert st["a"][0] == "up-to-date"
    assert st["b"] == ("always-run", ["stage is marked always_run"])
    assert states(ws, force=True)["a"] == ("stale", ["forced"])


def test_targets_limit_to_upstream_closure(chain_ws: Workspace) -> None:
    names = [s.name for s in plan(chain_ws, chain_ws.cache(), ["train"])]
    assert names == ["prepare", "train"]


def test_nested_dependency_inside_cached_tree_is_pending(make_ws: WsFactory) -> None:
    yaml = (
        "stages:\n"
        "  gen:\n    cmd: mkdir -p d && echo ${v} > d/x.txt\n    outs: [d]\n"
        "  use:\n    cmd: cat d/x.txt > u.txt\n    deps: [d/x.txt]\n    outs: [u.txt]\n"
    )
    ws = make_ws(yaml, "v: 1\n")
    repro(ws)
    write(ws.root / "params.yaml", "v: 2\n")
    repro(make_ws(root=ws.root))
    write(ws.root / "params.yaml", "v: 1\n")
    st = states(make_ws(root=ws.root))
    assert st["gen"][0] == "cached"
    assert st["use"] == ("pending", ["upstream stage 'gen' will be restored from cache"])
    assert State.PENDING.needs_work and not State.UP_TO_DATE.needs_work
