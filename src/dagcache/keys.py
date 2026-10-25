"""Computing cache-key components for a stage, and explaining key changes."""

from __future__ import annotations

import os
import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .canonical import canonical_json, digest_bytes, short
from .codehash import CodeFingerprinter, file_code_digest
from .errors import ConfigError
from .hashing import Hasher
from .manifest import KeyComponents
from .params import ParamStore, interpolate
from .stage import Stage


class KeyBuilder:
    """Builds :class:`KeyComponents` from a stage and its resolved dep digests.

    Dependency digests are supplied by the caller because *how* they are known
    differs: ``repro`` hashes materialised files, while ``status`` may take an
    upstream stage's digest from its cached manifest without materialising it.
    """

    def __init__(
        self,
        root: Path,
        hasher: Hasher,
        params: ParamStore,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        self.root = root
        self.hasher = hasher
        self.params = params
        self.environ = environ if environ is not None else os.environ
        self._fingerprinter = CodeFingerprinter(root)
        self._func_cache: dict[str, dict[str, str]] = {}
        self._lock = threading.Lock()

    def code_entries(self, stage: Stage) -> dict[str, str]:
        """Named digests of the stage's code: function closure plus ``code`` paths."""
        entries: dict[str, str] = {}
        if stage.func is not None:
            with self._lock:
                cached = self._func_cache.get(stage.name)
                if cached is None:
                    cached = self._fingerprinter.fingerprint(stage.func)
                    self._func_cache[stage.name] = cached
            entries.update(cached)
        for rel in stage.code:
            path = self.root / rel
            if path.is_dir():
                entries[f"file:{rel}"] = self.hasher.digest_path(rel).digest
            elif path.is_file():
                entries[f"file:{rel}"] = file_code_digest(path)
            else:
                raise ConfigError(f"stage '{stage.name}': code path '{rel}' does not exist")
        return entries

    def command(self, stage: Stage) -> str:
        """The shell command with ``${...}`` parameters substituted."""
        if stage.cmd is None:
            raise ConfigError(f"stage '{stage.name}' has no command")
        return interpolate(stage.cmd, self.params)

    def param_values(self, stage: Stage) -> dict[str, Any]:
        return self.params.values(stage.param_refs(self.params.default_file))

    def components(self, stage: Stage, dep_digests: Mapping[str, str]) -> KeyComponents:
        # The *template* goes into the key: every ``${param}`` it interpolates is
        # also recorded under ``params``, so template + values pin the command.
        cmd = stage.cmd
        if cmd is not None:
            interpolate(cmd, self.params)  # fail early on missing/non-scalar params
        env: dict[str, str | None] = {}
        for name in stage.env:
            value = self.environ.get(name)
            # Store a digest, not the value: lock files get committed and env
            # vars are a common home for secrets.
            env[name] = None if value is None else digest_bytes(value.encode())
        return KeyComponents(
            kind=stage.kind,
            cmd=cmd,
            code=self.code_entries(stage),
            deps={d: dep_digests[d] for d in stage.deps},
            params=self.param_values(stage),
            outs=tuple(sorted(stage.outs)),
            wdir=stage.wdir,
            env=env,
            salt=stage.salt,
        )


def _fmt(value: Any, limit: int = 40) -> str:
    text = repr(value) if not isinstance(value, str) else value
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _code_label(entry: str) -> str:
    kind, _, name = entry.partition(":")
    return {
        "file": f"code file {name}",
        "py": f"function {name}",
        "module": f"module {name}",
        "const": f"constant {name}",
        "lib": f"library {name}",
    }.get(kind, entry)


def explain(old: KeyComponents, new: KeyComponents) -> list[str]:
    """Human-readable differences between two component sets, most specific first."""
    reasons: list[str] = []
    if old.kind != new.kind:
        reasons.append(f"stage kind changed: {old.kind} → {new.kind}")
    if old.cmd != new.cmd:
        reasons.append(f"command changed: {_fmt(old.cmd)!s} → {_fmt(new.cmd)!s}")
    for entry in sorted(set(old.code) | set(new.code)):
        a, b = old.code.get(entry), new.code.get(entry)
        if a == b:
            continue
        label = _code_label(entry)
        if a is None:
            reasons.append(f"{label} added")
        elif b is None:
            reasons.append(f"{label} removed")
        elif entry.startswith("lib:"):
            reasons.append(f"{label} changed: {a} → {b}")
        else:
            reasons.append(f"{label} changed ({short(a, 8)} → {short(b, 8)})")
    for dep in sorted(set(old.deps) | set(new.deps)):
        a, b = old.deps.get(dep), new.deps.get(dep)
        if a == b:
            continue
        if a is None:
            reasons.append(f"dependency {dep} added")
        elif b is None:
            reasons.append(f"dependency {dep} removed")
        else:
            reasons.append(f"dependency {dep} changed ({short(a, 8)} → {short(b, 8)})")
    for name in sorted(set(old.params) | set(new.params)):
        if name not in new.params:
            reasons.append(f"param {name} no longer used")
        elif name not in old.params:
            reasons.append(f"param {name} added (= {_fmt(new.params[name])})")
        elif canonical_json(old.params[name]) != canonical_json(new.params[name]):
            reasons.append(
                f"param {name} changed: {_fmt(old.params[name])} → {_fmt(new.params[name])}"
            )
    if old.outs != new.outs:
        reasons.append(f"outputs changed: {list(old.outs)} → {list(new.outs)}")
    if old.wdir != new.wdir:
        reasons.append(f"wdir changed: {old.wdir} → {new.wdir}")
    for name in sorted(set(old.env) | set(new.env)):
        if old.env.get(name) != new.env.get(name):
            reasons.append(f"env {name} changed")
    if old.salt != new.salt:
        reasons.append(f"salt changed: {old.salt!r} → {new.salt!r}")
    if not reasons and old.key != new.key:
        reasons.append("cache key schema changed")
    return reasons
