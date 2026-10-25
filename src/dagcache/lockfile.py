"""``dagcache.lock``: the recorded state of the last successful run of each stage.

It is meant to be committed alongside the pipeline (like ``dvc.lock`` or a
package lock file): it pins exactly which code, params and inputs produced the
outputs in the workspace, and is the root set for garbage collection.
Updates are read-modify-write under an exclusive file lock and land with an
atomic rename, so concurrent runs that finish different stages never lose each
other's entries.
"""

from __future__ import annotations

import contextlib
import os
import tempfile
from collections.abc import Iterable, Mapping
from pathlib import Path

import yaml

from .errors import CacheError, ConfigError
from .locking import FileLock
from .manifest import ActionManifest

LOCK_SCHEMA = 1


class LockFile:
    """Reader/writer for the workspace lock file."""

    def __init__(self, path: Path, mutex_path: Path, *, timeout: float | None = 60.0) -> None:
        self.path = path
        self.mutex_path = mutex_path
        self.timeout = timeout

    def read(self) -> dict[str, ActionManifest]:
        """Entries by stage name; empty if the file does not exist."""
        return read_lock_file(self.path)

    def update(
        self,
        entries: Mapping[str, ActionManifest],
        *,
        remove: Iterable[str] = (),
    ) -> None:
        """Merge ``entries`` into the file and drop stages named in ``remove``."""
        with FileLock(self.mutex_path, timeout=self.timeout):
            current = self.read()
            current.update(entries)
            for name in remove:
                current.pop(name, None)
            self._write(current)

    def _write(self, entries: Mapping[str, ActionManifest]) -> None:
        doc = {
            "schema": LOCK_SCHEMA,
            "stages": {name: _entry_dict(entries[name]) for name in sorted(entries)},
        }
        text = yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=100)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=self.path.parent, prefix=".dagcache.lock.")
        tmp = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(text)
            os.chmod(tmp, 0o644)
            os.replace(tmp, self.path)
        finally:
            with contextlib.suppress(FileNotFoundError):
                tmp.unlink()


def _entry_dict(manifest: ActionManifest) -> dict[str, object]:
    raw = manifest.to_dict()
    raw.pop("schema")
    raw.pop("stage")
    return raw


def read_lock_file(path: Path) -> dict[str, ActionManifest]:
    """Parse a lock file at ``path`` (also used for extra GC roots)."""
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    try:
        doc = yaml.safe_load(text) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path.name} is not valid YAML: {exc}") from exc
    if not isinstance(doc, dict) or doc.get("schema") != LOCK_SCHEMA:
        raise ConfigError(f"{path.name}: unsupported or missing lock schema")
    out: dict[str, ActionManifest] = {}
    for name, raw in (doc.get("stages") or {}).items():
        if not isinstance(raw, dict):
            raise ConfigError(f"{path.name}: entry for '{name}' is not a mapping")
        try:
            out[str(name)] = ActionManifest.from_dict({**raw, "schema": 1, "stage": str(name)})
        except CacheError as exc:
            raise ConfigError(f"{path.name}: entry for '{name}': {exc}") from exc
    return out
