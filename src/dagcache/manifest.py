"""Cache-key components and action manifests.

:class:`KeyComponents` is *everything* that determines a stage's result. The
stage's cache key is the SHA-256 of their canonical JSON. Keeping the components
(rather than only the key) in the lock file and in each action manifest is what
lets ``status`` explain *why* a key changed, and lets ``lineage`` answer "which
code, params and inputs produced this artifact".
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Literal, cast

from .canonical import canonical_json, digest_json
from .errors import CacheError

#: Bump when the meaning of key components changes; invalidates every key.
KEY_SCHEMA = 1
MANIFEST_SCHEMA = 1


@dataclass(frozen=True)
class KeyComponents:
    """Inputs that fully determine a stage's outputs."""

    kind: str
    cmd: str | None
    code: dict[str, str]
    deps: dict[str, str]
    params: dict[str, Any]
    outs: tuple[str, ...]
    wdir: str = "."
    env: dict[str, str | None] = field(default_factory=dict)
    salt: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "cmd": self.cmd,
            "code": dict(sorted(self.code.items())),
            "deps": dict(sorted(self.deps.items())),
            "params": dict(sorted(self.params.items())),
            "outs": list(self.outs),
            "wdir": self.wdir,
            "env": dict(sorted(self.env.items())),
            "salt": self.salt,
        }

    @property
    def key(self) -> str:
        return digest_json({"schema": KEY_SCHEMA, **self.to_dict()})

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> KeyComponents:
        try:
            return cls(
                kind=str(raw["kind"]),
                cmd=raw.get("cmd"),
                code={str(k): str(v) for k, v in (raw.get("code") or {}).items()},
                deps={str(k): str(v) for k, v in (raw.get("deps") or {}).items()},
                params=dict(raw.get("params") or {}),
                outs=tuple(str(o) for o in raw.get("outs") or ()),
                wdir=str(raw.get("wdir", ".")),
                env={
                    str(k): (None if v is None else str(v))
                    for k, v in (raw.get("env") or {}).items()
                },
                salt=str(raw.get("salt", "")),
            )
        except (KeyError, TypeError, AttributeError) as exc:
            raise CacheError(f"malformed key components: {exc}") from exc


@dataclass(frozen=True)
class OutputRecord:
    """One output path of a stage and the object that holds its content."""

    path: str
    kind: Literal["file", "tree"]
    digest: str
    size: int
    executable: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "kind": self.kind,
            "digest": self.digest,
            "size": self.size,
            "executable": self.executable,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> OutputRecord:
        kind = raw["kind"]
        if kind not in ("file", "tree"):
            raise CacheError(f"unknown output kind {kind!r}")
        return cls(
            path=str(raw["path"]),
            kind=cast(Literal["file", "tree"], kind),
            digest=str(raw["digest"]),
            size=int(raw["size"]),
            executable=bool(raw.get("executable", False)),
        )


@dataclass(frozen=True)
class ActionManifest:
    """Record of one successful stage execution, addressed by its cache key.

    ``upstream`` maps each dependency path that another stage produced to that
    producer's cache key - the edges of the lineage graph.
    """

    key: str
    stage: str
    components: KeyComponents
    outputs: tuple[OutputRecord, ...]
    upstream: dict[str, str]
    created_at: str
    duration_s: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": MANIFEST_SCHEMA,
            "key": self.key,
            "stage": self.stage,
            "created_at": self.created_at,
            "duration_s": round(self.duration_s, 4),
            "components": self.components.to_dict(),
            "outputs": [o.to_dict() for o in self.outputs],
            "upstream": dict(sorted(self.upstream.items())),
        }

    def to_bytes(self) -> bytes:
        return canonical_json(self.to_dict())

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ActionManifest:
        try:
            if raw.get("schema") != MANIFEST_SCHEMA:
                raise CacheError(f"unsupported manifest schema {raw.get('schema')!r}")
            return cls(
                key=str(raw["key"]),
                stage=str(raw["stage"]),
                components=KeyComponents.from_dict(raw["components"]),
                outputs=tuple(OutputRecord.from_dict(o) for o in raw["outputs"]),
                upstream={str(k): str(v) for k, v in (raw.get("upstream") or {}).items()},
                created_at=str(raw.get("created_at", "")),
                duration_s=float(raw.get("duration_s", 0.0)),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CacheError(f"malformed action manifest: {exc}") from exc

    @classmethod
    def from_bytes(cls, data: bytes) -> ActionManifest:
        try:
            raw = json.loads(data)
        except ValueError as exc:
            raise CacheError(f"action manifest is not JSON: {exc}") from exc
        if not isinstance(raw, dict):
            raise CacheError("action manifest must be a JSON object")
        return cls.from_dict(raw)

    def output(self, path: str) -> OutputRecord | None:
        for o in self.outputs:
            if o.path == path:
                return o
        return None
