"""Lineage: "what produced this artifact, from which code, params and inputs?"

Every action manifest records its key components (code digests, parameter
values, input digests) and, for each input another stage produced, that
producer's cache key. Following those keys backwards from an artifact yields a
provenance graph that ends at source data - files no stage produced.

The graph is resolved from cached manifests, not from the current pipeline
definition, so it stays correct for artifacts produced by older code.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from .cache import Cache
from .canonical import digest_bytes, short
from .errors import CacheError, ConfigError
from .hashing import Tree
from .manifest import ActionManifest
from .paths import is_within, normalize, relative_to
from .pipeline import Pipeline
from .workspace import Workspace


@dataclass
class StageNode:
    stage: str
    key: str
    created_at: str
    duration_s: float
    cmd: str | None
    params: dict[str, Any]
    code: dict[str, str]
    inputs: list[ArtifactNode] = field(default_factory=list)


@dataclass
class ArtifactNode:
    path: str
    digest: str | None
    producer: StageNode | None = None
    note: str = ""


class LineageResolver:
    """Resolves provenance graphs against a workspace's lock file and cache."""

    def __init__(self, ws: Workspace, cache: Cache) -> None:
        self.ws = ws
        self.cache = cache
        self.lock = ws.lockfile.read()
        self._nodes: dict[str, StageNode] = {}
        self._by_digest: dict[str, list[ActionManifest]] | None = None

    # -- lookup helpers --------------------------------------------------
    def _manifests_by_digest(self) -> dict[str, list[ActionManifest]]:
        if self._by_digest is None:
            index: dict[str, list[ActionManifest]] = {}
            for m in self.cache.iter_manifests():
                for rec in m.outputs:
                    index.setdefault(rec.digest, []).append(m)
            for lst in index.values():
                lst.sort(key=lambda m: m.created_at, reverse=True)
            self._by_digest = index
        return self._by_digest

    def _expected_digest(self, manifest: ActionManifest, path: str) -> str | None:
        """Digest ``manifest`` recorded for ``path`` (an output or a file inside one)."""
        for rec in manifest.outputs:
            if rec.path == path:
                return rec.digest
            if rec.kind == "tree" and is_within(path, rec.path):
                try:
                    tree = self.cache.load_tree(rec.digest)
                except CacheError:
                    return None
                sub = relative_to(path, rec.path)
                return _tree_lookup(tree, sub)
        return None

    def _manifest_for_key(
        self, key: str, fallback: ActionManifest | None = None
    ) -> ActionManifest | None:
        """The lock entry with ``key`` if any (it carries the *current* upstream
        links), else the cached manifest, else ``fallback``."""
        for entry in self.lock.values():
            if entry.key == key:
                return entry
        return self.cache.get_action(key, use_remote=False) or fallback

    # -- public ----------------------------------------------------------
    def trace(self, path: str) -> ArtifactNode:
        rel = normalize(path)
        try:
            current: str | None = self.ws.hasher.digest_path(rel).digest
        except FileNotFoundError:
            current = None
        for entry in self.lock.values():
            expected = self._expected_digest(entry, rel)
            if expected is None:
                continue
            manifest = self._manifest_for_key(entry.key, entry)
            assert manifest is not None
            if current is None:
                return ArtifactNode(
                    rel, expected, self._stage_node(manifest), "not in workspace; as recorded"
                )
            if current == expected:
                return ArtifactNode(rel, current, self._stage_node(manifest))
            note = "workspace copy differs from dagcache.lock"
            break
        else:
            note = ""
        if current is None:
            raise ConfigError(f"'{rel}' is not in the workspace and no stage records it")
        candidates = self._manifests_by_digest().get(current, [])
        if candidates:
            extra = "; " if note else ""
            return ArtifactNode(
                rel, current, self._stage_node(candidates[0]), f"{note}{extra}matched by content"
            )
        return ArtifactNode(rel, current, None, note or "source data (no stage produced it)")

    def _stage_node(self, manifest: ActionManifest) -> StageNode:
        if manifest.key in self._nodes:
            return self._nodes[manifest.key]
        comps = manifest.components
        node = StageNode(
            stage=manifest.stage,
            key=manifest.key,
            created_at=manifest.created_at,
            duration_s=manifest.duration_s,
            cmd=comps.cmd,
            params=dict(comps.params),
            code=dict(comps.code),
        )
        self._nodes[manifest.key] = node
        for dep, digest in sorted(comps.deps.items()):
            keys = [
                k
                for label, k in manifest.upstream.items()
                if label == dep or label.startswith(dep + "|")
            ]
            if not keys:
                node.inputs.append(ArtifactNode(dep, digest, None, "source data"))
                continue
            for key in keys:
                upstream = self._manifest_for_key(key)
                if upstream is None:
                    node.inputs.append(
                        ArtifactNode(dep, digest, None, f"produced by {short(key)} (manifest gone)")
                    )
                else:
                    node.inputs.append(ArtifactNode(dep, digest, self._stage_node(upstream)))
        return node


def _tree_lookup(tree: Tree, sub: str) -> str | None:
    for e in tree.entries:
        if e.path == sub:
            return e.digest
    return None


# -- rendering -----------------------------------------------------------------
def render_text(node: ArtifactNode) -> str:
    lines: list[str] = []
    shown: set[str] = set()

    def art_line(a: ArtifactNode) -> str:
        note = f"  [{a.note}]" if a.note else ""
        return f"{a.path}  @{short(a.digest)}{note}"

    def emit(a: ArtifactNode, prefix: str, is_last: bool, top: bool) -> None:
        branch = "" if top else ("└── " if is_last else "├── ")
        lines.append(f"{prefix}{branch}{art_line(a)}")
        child_prefix = prefix if top else prefix + ("    " if is_last else "│   ")
        st = a.producer
        if st is None:
            return
        lines.append(f"{child_prefix}  ⮑ stage {st.stage}  key {short(st.key)}  at {st.created_at}")
        if st.key in shown:
            lines.append(f"{child_prefix}     (inputs shown above)")
            return
        shown.add(st.key)
        if st.cmd:
            lines.append(f"{child_prefix}     cmd: {st.cmd}")
        if st.params:
            rendered = ", ".join(f"{k}={json.dumps(v)}" for k, v in sorted(st.params.items()))
            lines.append(f"{child_prefix}     params: {rendered}")
        if st.code:
            rendered = ", ".join(
                f"{k.split(':', 1)[1]}@{v if k.startswith('lib:') else short(v, 8)}"
                for k, v in sorted(st.code.items())
            )
            lines.append(f"{child_prefix}     code: {rendered}")
        for i, inp in enumerate(st.inputs):
            emit(inp, child_prefix + "  ", i == len(st.inputs) - 1, False)

    emit(node, "", True, True)
    return "\n".join(lines)


def to_dict(node: ArtifactNode) -> dict[str, Any]:
    def stage(st: StageNode, seen: set[str]) -> dict[str, Any]:
        out: dict[str, Any] = {
            "stage": st.stage,
            "key": st.key,
            "created_at": st.created_at,
            "duration_s": st.duration_s,
            "cmd": st.cmd,
            "params": st.params,
            "code": st.code,
        }
        if st.key in seen:
            out["inputs"] = "(see above)"
            return out
        seen.add(st.key)
        out["inputs"] = [art(i, seen) for i in st.inputs]
        return out

    def art(a: ArtifactNode, seen: set[str]) -> dict[str, Any]:
        out: dict[str, Any] = {"path": a.path, "digest": a.digest, "note": a.note}
        out["produced_by"] = stage(a.producer, seen) if a.producer else None
        return out

    return art(node, set())


def _graph_edges(node: ArtifactNode) -> tuple[dict[str, str], dict[str, str], set[tuple[str, str]]]:
    artifacts: dict[str, str] = {}
    stages: dict[str, str] = {}
    edges: set[tuple[str, str]] = set()

    def visit(a: ArtifactNode) -> str:
        ident = f"a_{short(a.digest, 10)}_{digest_bytes(a.path.encode())[:6]}"
        artifacts[ident] = f"{a.path}\\n@{short(a.digest, 8)}"
        st = a.producer
        if st is None:
            return ident
        sid = f"s_{short(st.key, 12)}"
        edges.add((sid, ident))
        if sid not in stages:
            stages[sid] = f"{st.stage}\\n{short(st.key, 8)}"
            for inp in st.inputs:
                edges.add((visit(inp), sid))
        return ident

    visit(node)
    return artifacts, stages, edges


def render_mermaid(node: ArtifactNode) -> str:
    artifacts, stages, edges = _graph_edges(node)
    lines = ["flowchart BT"]
    for ident, label in sorted(artifacts.items()):
        lines.append(f'  {ident}["{label.replace(chr(92) + "n", "<br/>")}"]')
    for ident, label in sorted(stages.items()):
        lines.append(f'  {ident}(["{label.replace(chr(92) + "n", "<br/>")}"])')
    for a, b in sorted(edges):
        lines.append(f"  {a} --> {b}")
    return "\n".join(lines)


def render_dot(node: ArtifactNode) -> str:
    artifacts, stages, edges = _graph_edges(node)
    lines = ["digraph lineage {", "  rankdir=BT;"]
    for ident, label in sorted(artifacts.items()):
        lines.append(f'  {ident} [shape=note, label="{label}"];')
    for ident, label in sorted(stages.items()):
        lines.append(f'  {ident} [shape=box, style=rounded, label="{label}"];')
    for a, b in sorted(edges):
        lines.append(f"  {a} -> {b};")
    lines.append("}")
    return "\n".join(lines)


# -- pipeline DAG rendering -------------------------------------------------------
def render_pipeline(pipe: Pipeline, fmt: str = "text", states: dict[str, str] | None = None) -> str:
    """Render the stage graph as an indented text list, mermaid or DOT."""
    graph = pipe.graph
    states = states or {}
    if fmt == "mermaid":
        lines = ["flowchart LR"]
        for name in graph.order:
            label = f"{name}<br/>{states[name]}" if name in states else name
            lines.append(f'  {_ident(name)}["{label}"]')
        for name in graph.order:
            for up in sorted(graph.upstream[name]):
                lines.append(f"  {_ident(up)} --> {_ident(name)}")
        return "\n".join(lines)
    if fmt == "dot":
        lines = ["digraph pipeline {", "  rankdir=LR;"]
        for name in graph.order:
            label = f"{name}\\n{states[name]}" if name in states else name
            lines.append(f'  {_ident(name)} [shape=box, label="{label}"];')
        for name in graph.order:
            for up in sorted(graph.upstream[name]):
                lines.append(f"  {_ident(up)} -> {_ident(name)};")
        lines.append("}")
        return "\n".join(lines)
    if fmt != "text":
        raise ConfigError(f"unknown format '{fmt}' (text, mermaid, dot)")
    lines = []
    for name in graph.order:
        ups = sorted(graph.upstream[name])
        state = f"  [{states[name]}]" if name in states else ""
        after = f"  <- {', '.join(ups)}" if ups else ""
        lines.append(f"{name}{state}{after}")
    return "\n".join(lines)


def _ident(name: str) -> str:
    return "n_" + "".join(c if c.isalnum() else "_" for c in name)
