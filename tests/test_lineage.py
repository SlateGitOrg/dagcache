"""Lineage: tracing an artifact back to code, params and source data."""

from __future__ import annotations

import json

import pytest
from conftest import WsFactory, repro, write

from dagcache import ConfigError, LineageResolver, Workspace
from dagcache.lineage import render_dot, render_mermaid, render_pipeline, render_text, to_dict

DIAMOND = (
    "stages:\n"
    "  src:\n    cmd: mkdir -p d && echo ${seed} > d/a && echo k > d/b\n    outs: [d]\n"
    "  left:\n    cmd: cat d/a > l\n    deps: [d/a]\n    outs: [l]\n"
    "  right:\n    cmd: cat d/b > r\n    deps: [d]\n    outs: [r]\n"
    "  join:\n    cmd: cat l r raw.txt > j\n    deps: [l, r, raw.txt]\n    outs: [j]\n"
)


@pytest.fixture
def diamond(make_ws: WsFactory) -> Workspace:
    ws = make_ws(DIAMOND, "seed: 3\n")
    write(ws.root / "raw.txt", "source\n")
    assert repro(ws).ok
    return ws


def trace(ws: Workspace, path: str) -> object:
    return LineageResolver(ws, ws.cache(use_remote=False)).trace(path)


def test_trace_reaches_source_data(diamond: Workspace) -> None:
    node = trace(diamond, "j")
    d = to_dict(node)  # type: ignore[arg-type]
    assert d["produced_by"]["stage"] == "join"
    inputs = {i["path"]: i for i in d["produced_by"]["inputs"]}
    assert set(inputs) == {"l", "r", "raw.txt"}
    assert inputs["raw.txt"]["produced_by"] is None and inputs["raw.txt"]["note"] == "source data"
    left = inputs["l"]["produced_by"]
    assert left["stage"] == "left"
    src = left["inputs"][0]["produced_by"]
    assert src["stage"] == "src" and src["params"] == {"seed": 3}
    # The diamond's shared ancestor is shown once and referenced afterwards.
    assert inputs["r"]["produced_by"]["inputs"][0]["produced_by"]["inputs"] == "(see above)"


def test_text_and_graph_renderers(diamond: Workspace) -> None:
    node = trace(diamond, "j")
    text = render_text(node)  # type: ignore[arg-type]
    assert text.splitlines()[0].startswith("j  @")
    assert "stage join" in text and "params: seed=3" in text
    assert "(inputs shown above)" in text
    mermaid = render_mermaid(node)  # type: ignore[arg-type]
    assert mermaid.startswith("flowchart BT") and "-->" in mermaid
    dot = render_dot(node)  # type: ignore[arg-type]
    assert dot.startswith("digraph lineage") and dot.rstrip().endswith("}")
    assert render_mermaid(node) == mermaid  # deterministic ids  # type: ignore[arg-type]


def test_file_inside_tree_output(diamond: Workspace) -> None:
    d = to_dict(trace(diamond, "d/a"))  # type: ignore[arg-type]
    assert d["produced_by"]["stage"] == "src" and d["note"] == ""


def test_source_file_and_modified_file(diamond: Workspace) -> None:
    assert to_dict(trace(diamond, "raw.txt"))["note"].startswith("source data")  # type: ignore[arg-type]
    (diamond.root / "l").write_text("hand edited\n")
    d = to_dict(trace(diamond, "l"))  # type: ignore[arg-type]
    assert d["produced_by"] is None
    assert "differs from dagcache.lock" in d["note"]


def test_matched_by_content_after_lock_moves_on(diamond: Workspace, make_ws: WsFactory) -> None:
    old_j = (diamond.root / "j").read_bytes()
    write(diamond.root / "params.yaml", "seed: 4\n")
    ws = make_ws(root=diamond.root)
    repro(ws)
    (ws.root / "old_j").write_bytes(old_j)
    d = to_dict(trace(ws, "old_j"))  # type: ignore[arg-type]
    assert d["note"] == "matched by content"
    assert d["produced_by"]["stage"] == "join"


def test_missing_artifact_recorded_and_unknown(diamond: Workspace) -> None:
    (diamond.root / "j").unlink()
    d = to_dict(trace(diamond, "j"))  # type: ignore[arg-type]
    assert d["note"] == "not in workspace; as recorded"
    with pytest.raises(ConfigError):
        trace(diamond, "nowhere.txt")


def test_manifest_gone_note(diamond: Workspace) -> None:
    left_key = diamond.lockfile.read()["left"].key
    lock = diamond.lockfile.read()
    del lock["left"]
    diamond.lockfile._write(lock)
    diamond.local.delete_action(left_key)
    text = json.dumps(to_dict(trace(diamond, "j")))  # type: ignore[arg-type]
    assert "manifest gone" in text


def test_render_pipeline_formats(diamond: Workspace) -> None:
    pipe = diamond.pipeline
    text = render_pipeline(pipe, "text", {"src": "up-to-date"})
    assert "join  <- left, right" in text and "src  [up-to-date]" in text
    assert "n_src --> n_left" in render_pipeline(pipe, "mermaid")
    assert "n_src -> n_right;" in render_pipeline(pipe, "dot", {"src": "x"})
    with pytest.raises(ConfigError):
        render_pipeline(pipe, "svg")
