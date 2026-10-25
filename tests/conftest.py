"""Shared fixtures: throwaway workspaces built from YAML snippets."""

from __future__ import annotations

import textwrap
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from dagcache import Pipeline, Runner, Workspace
from dagcache.runner import ReproReport

WsFactory = Callable[..., Workspace]


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return path


@pytest.fixture
def make_ws(tmp_path: Path) -> Iterator[WsFactory]:
    """Create a workspace from YAML text (and optional params), reloadable."""
    opened: list[Workspace] = []

    def factory(
        yaml_text: str | None = None,
        params: str | None = None,
        *,
        root: Path | None = None,
        lock_timeout: float | None = 30.0,
    ) -> Workspace:
        base = root or tmp_path / "ws"
        base.mkdir(parents=True, exist_ok=True)
        if yaml_text is not None:
            write(base / "dagcache.yaml", yaml_text)
        if params is not None:
            write(base / "params.yaml", params)
        ws = Workspace.load(base / "dagcache.yaml", lock_timeout=lock_timeout)
        opened.append(ws)
        return ws

    yield factory
    for ws in opened:
        ws.close()


@pytest.fixture
def py_ws(tmp_path: Path) -> Iterator[Callable[[Pipeline], Workspace]]:
    """Wrap an in-memory :class:`Pipeline` in a workspace rooted at tmp_path/py."""
    opened: list[Workspace] = []

    def factory(pipe: Pipeline) -> Workspace:
        root = tmp_path / "py"
        root.mkdir(exist_ok=True)
        ws = Workspace(root, pipe, lock_timeout=30.0)
        opened.append(ws)
        return ws

    yield factory
    for ws in opened:
        ws.close()


def repro(ws: Workspace, *targets: str, **kwargs: object) -> ReproReport:
    return Runner(ws, **kwargs).repro(list(targets) or None)  # type: ignore[arg-type]


def actions(report: ReproReport) -> dict[str, str]:
    return {r.name: r.action.value for r in report.results}


CHAIN_YAML = """\
stages:
  prepare:
    cmd: mkdir -p data && python3 -c "import sys; open('data/clean.txt','w').write(open('raw.txt').read().upper())" && echo run >> .count-prepare
    deps: [raw.txt]
    outs: [data/clean.txt]
    params: [prep.mode]
  train:
    cmd: mkdir -p models && cat data/clean.txt > models/model.txt && echo "lr=${train.lr}" >> models/model.txt && echo run >> .count-train
    deps: [data/clean.txt]
    outs: [models/model.txt]
  evaluate:
    cmd: mkdir -p metrics && wc -c < models/model.txt > metrics/score.txt && echo run >> .count-evaluate
    deps: [models/model.txt]
    outs: [metrics/score.txt]
"""

CHAIN_PARAMS = """\
prep:
  mode: upper
train:
  lr: 0.1
unrelated:
  note: hello
"""


def run_count(ws: Workspace, stage: str) -> int:
    path = ws.root / f".count-{stage}"
    return len(path.read_text().splitlines()) if path.exists() else 0


@pytest.fixture
def chain_ws(make_ws: WsFactory) -> Workspace:
    ws = make_ws(CHAIN_YAML, CHAIN_PARAMS)
    (ws.root / "raw.txt").write_text("hello world\n")
    return ws
