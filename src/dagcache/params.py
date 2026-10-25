"""Parameter files, dotted-key lookup and ``${...}`` command interpolation.

A stage declares the parameters it reads, e.g. ``train.C`` (from the default
params file) or ``configs/model.yaml:layers.0.units``. Only the *values* of the
declared keys enter the stage's cache key, so editing an unrelated parameter
does not invalidate the stage. Values may be scalars or whole sub-trees.
"""

from __future__ import annotations

import datetime as _dt
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .canonical import canonical_json
from .errors import ParamError
from .paths import normalize

DEFAULT_PARAMS_FILE = "params.yaml"
_INTERP = re.compile(r"\$\{\s*([^}\s]+)\s*\}")
_MISSING = object()


@dataclass(frozen=True, slots=True, order=True)
class ParamRef:
    """A reference to one key in one params file."""

    file: str
    key: str

    def display(self, default_file: str) -> str:
        return self.key if self.file == default_file else f"{self.file}:{self.key}"

    @property
    def qualified(self) -> str:
        return f"{self.file}:{self.key}"


def parse_ref(spec: str, default_file: str = DEFAULT_PARAMS_FILE) -> ParamRef:
    """Parse ``"key.path"`` or ``"file.yaml:key.path"``."""
    spec = spec.strip()
    if not spec:
        raise ParamError("empty parameter reference")
    if ":" in spec:
        file, key = spec.rsplit(":", 1)
        file = normalize(file, what="params file")
    else:
        file, key = default_file, spec
    if not key or key.startswith(".") or key.endswith(".") or ".." in key:
        raise ParamError(f"malformed parameter key '{spec}'")
    return ParamRef(file, key)


def _jsonable(value: Any) -> Any:
    if isinstance(value, _dt.date | _dt.datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(v) for v in value]
    return value


def lookup(data: Any, key: str) -> Any:
    """Resolve dotted ``key`` in nested dicts/lists; returns ``_MISSING`` if absent."""
    node = data
    for part in key.split("."):
        if isinstance(node, dict) and part in node:
            node = node[part]
        elif isinstance(node, list) and part.isdigit() and int(part) < len(node):
            node = node[int(part)]
        else:
            return _MISSING
    return node


class ParamStore:
    """Lazily loads and caches params files relative to a workspace root."""

    def __init__(self, root: Path, default_file: str = DEFAULT_PARAMS_FILE) -> None:
        self.root = root
        self.default_file = default_file
        self._files: dict[str, Any] = {}
        self._lock = threading.Lock()

    def _load(self, file: str) -> Any:
        with self._lock:
            if file in self._files:
                return self._files[file]
            path = self.root / file
            try:
                with path.open("r", encoding="utf-8") as fh:
                    data = yaml.safe_load(fh)
            except FileNotFoundError as exc:
                raise ParamError(f"params file '{file}' does not exist") from exc
            except yaml.YAMLError as exc:
                raise ParamError(f"params file '{file}' is not valid YAML: {exc}") from exc
            data = _jsonable(data if data is not None else {})
            self._files[file] = data
            return data

    def get(self, ref: ParamRef) -> Any:
        value = lookup(self._load(ref.file), ref.key)
        if value is _MISSING:
            raise ParamError(
                f"parameter '{ref.display(self.default_file)}' not found in {ref.file}"
            )
        try:
            canonical_json(value)
        except (TypeError, ValueError) as exc:
            raise ParamError(f"parameter '{ref.key}' has no stable value: {exc}") from exc
        return value

    def values(self, refs: list[ParamRef]) -> dict[str, Any]:
        """``{display name: value}`` for every reference."""
        return {r.display(self.default_file): self.get(r) for r in sorted(refs)}

    def invalidate(self) -> None:
        with self._lock:
            self._files.clear()


def interpolation_refs(cmd: str, default_file: str = DEFAULT_PARAMS_FILE) -> list[ParamRef]:
    """Parameter references used as ``${...}`` in ``cmd``."""
    return [parse_ref(m.group(1), default_file) for m in _INTERP.finditer(cmd)]


def interpolate(cmd: str, store: ParamStore) -> str:
    """Substitute ``${key}`` placeholders with scalar parameter values."""

    def repl(match: re.Match[str]) -> str:
        ref = parse_ref(match.group(1), store.default_file)
        value = store.get(ref)
        if isinstance(value, dict | list):
            raise ParamError(f"cannot interpolate non-scalar parameter '{match.group(1)}'")
        if isinstance(value, bool):
            return "true" if value else "false"
        return "" if value is None else str(value)

    return _INTERP.sub(repl, cmd)
