"""Garbage collection: mark-and-sweep over the content-addressed cache.

Roots are the action keys recorded in ``dagcache.lock`` (plus any extra lock
files given, e.g. exported from other branches) and, optionally, every action
used within the last ``keep_days`` days. Marking follows each root manifest to
its output objects, and into tree manifests to the files they list. Everything
unmarked is swept: unreachable action manifests first, then objects.

GC takes the repository lock *exclusively*, so it waits for running
``repro``/``push``/``pull`` invocations (which hold it shared) and blocks new
ones until it is done - a run can never lose an object it is about to restore.
Cost: O(A + O) for A action manifests and O objects, plus reading each tree.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

from .backends.base import Backend
from .backends.fs import FsBackend
from .cache import Cache
from .errors import CacheError
from .hashing import Tree
from .lockfile import read_lock_file
from .locking import FileLock
from .manifest import ActionManifest
from .workspace import Workspace

log = logging.getLogger(__name__)


@dataclass
class GcReport:
    location: str
    dry_run: bool
    roots: int = 0
    actions_kept: int = 0
    actions_removed: int = 0
    objects_kept: int = 0
    objects_removed: int = 0
    bytes_freed: int = 0
    tmp_removed: int = 0
    missing_objects: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "location": self.location,
            "dry_run": self.dry_run,
            "roots": self.roots,
            "actions_kept": self.actions_kept,
            "actions_removed": self.actions_removed,
            "objects_kept": self.objects_kept,
            "objects_removed": self.objects_removed,
            "bytes_freed": self.bytes_freed,
            "tmp_removed": self.tmp_removed,
            "missing_objects": len(self.missing_objects),
        }


def _mark(backend: Backend, manifests: list[ActionManifest], report: GcReport) -> set[str]:
    live: set[str] = set()
    for manifest in manifests:
        for rec in manifest.outputs:
            live.add(rec.digest)
            if rec.kind != "tree":
                continue
            try:
                tree = Tree.from_bytes(backend.get_object_bytes(rec.digest))
            except CacheError:
                report.missing_objects.append(rec.digest)
                continue
            live.update(e.digest for e in tree.entries)
    return live


def collect(
    ws: Workspace,
    *,
    keep_days: float | None = None,
    extra_lock_files: list[Path] | None = None,
    dry_run: bool = False,
    remote: bool = False,
) -> GcReport:
    """Run GC on the local cache (or the remote with ``remote=True``)."""
    cache: Cache = ws.cache(use_remote=remote)
    backend: Backend
    if remote:
        if cache.remote is None:
            raise CacheError("no remote configured")
        backend = cache.remote
    else:
        backend = cache.local
    report = GcReport(backend.location, dry_run)
    with FileLock(ws.repo_lock_path, shared=False, timeout=ws.lock_timeout):
        root_entries = list(ws.lockfile.read().values())
        for extra in extra_lock_files or []:
            root_entries.extend(read_lock_file(extra).values())
        root_keys = {e.key for e in root_entries}
        cutoff = time.time() - keep_days * 86400 if keep_days is not None else None
        actions = list(backend.iter_actions())
        if cutoff is not None:
            root_keys.update(k for k, used in actions if used >= cutoff)
        report.roots = len(root_keys)
        manifests: list[ActionManifest] = []
        by_key = {e.key: e for e in root_entries}
        for key in sorted(root_keys):
            data = backend.get_action(key)
            if data is not None:
                try:
                    manifests.append(ActionManifest.from_bytes(data))
                    continue
                except CacheError as exc:
                    log.warning("unreadable root manifest %s: %s", key[:12], exc)
            if key in by_key:  # lock entry still pins its outputs
                manifests.append(by_key[key])
        live = _mark(backend, manifests, report)
        for key, _ in actions:
            if key in root_keys:
                report.actions_kept += 1
                continue
            report.actions_removed += 1
            if not dry_run:
                backend.delete_action(key)
        for digest, size in list(backend.iter_objects()):
            if digest in live:
                report.objects_kept += 1
                continue
            report.objects_removed += 1
            report.bytes_freed += size
            if not dry_run:
                backend.delete_object(digest)
        if not dry_run and isinstance(backend, FsBackend):
            report.tmp_removed = backend.clean_tmp()
            ws.stat_cache.prune_missing()
    return report
