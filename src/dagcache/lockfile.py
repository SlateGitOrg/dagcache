"""``dagcache.lock``: the recorded state of the last successful run of each stage.

It is meant to be committed alongside the pipeline (like ``dvc.lock`` or a
package lock file): it pins exactly which code, params and inputs produced the
outputs in the workspace, and is the root set for garbage collection.

The format is pretty-printed JSON with stages in sorted order, so diffs are
reviewable. (It was YAML at first; parsing YAML once per stage made a
300-stage run spend most of its time in the YAML parser - see DECISIONS.md.)

Updates are read-modify-write under an exclusive file lock and land with an
atomic rename, so concurrent runs that finish different stages never lose each
other's entries. Reads are memoised on the file's ``(inode, size, mtime)``
signature; every write replaces the inode, so a change by another process is
always seen.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
import threading
from collections.abc import Iterable, Mapping
from pathlib import Path

from .errors import CacheError, ConfigError
from .locking import FileLock
from .manifest import ActionManifest

LOCK_SCHEMA = 1

_Signature = tuple[int, int, int]


def _signature(path: Path) -> _Signature | None:
    try:
        st = path.stat()
    except FileNotFoundError:
        return None
    return (st.st_ino, st.st_size, st.st_mtime_ns)


class LockFile:
    """Reader/writer for the workspace lock file."""

    def __init__(self, path: Path, mutex_path: Path, *, timeout: float | None = 60.0) -> None:
        self.path = path
        self.mutex_path = mutex_path
        self.timeout = timeout
        self._memo: tuple[_Signature, dict[str, ActionManifest]] | None = None
        self._memo_lock = threading.Lock()

    def read(self) -> dict[str, ActionManifest]:
        """Entries by stage name; empty if the file does not exist."""
        sig = _signature(self.path)
        if sig is None:
            return {}
        with self._memo_lock:
            if self._memo is not None and self._memo[0] == sig:
                return dict(self._memo[1])
        entries = read_lock_file(self.path)
        with self._memo_lock:
            self._memo = (sig, entries)
        return dict(entries)

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
        text = json.dumps(doc, indent=2, ensure_ascii=False) + "\n"
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
        sig = _signature(self.path)
        if sig is not None:
            with self._memo_lock:
                self._memo = (sig, dict(entries))


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
        doc = json.loads(text)
    except ValueError as exc:
        raise ConfigError(f"{path.name} is not valid JSON: {exc}") from exc
    if not isinstance(doc, dict) or doc.get("schema") != LOCK_SCHEMA:
        raise ConfigError(f"{path.name}: unsupported or missing lock schema")
    stages = doc.get("stages")
    if stages is None:
        stages = {}
    if not isinstance(stages, dict):
        raise ConfigError(f"{path.name}: 'stages' must be an object")
    out: dict[str, ActionManifest] = {}
    for name, raw in stages.items():
        if not isinstance(raw, dict):
            raise ConfigError(f"{path.name}: entry for '{name}' is not an object")
        try:
            out[str(name)] = ActionManifest.from_dict({**raw, "schema": 1, "stage": str(name)})
        except CacheError as exc:
            raise ConfigError(f"{path.name}: entry for '{name}': {exc}") from exc
    return out
