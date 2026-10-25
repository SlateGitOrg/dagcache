"""The layered artifact cache: a local filesystem store plus an optional remote.

Reads go local first, then remote (pulling what they find into the local store
so the next read is local). Writes always go local; with ``autopush`` they are
also copied to the remote right away, otherwise ``dagcache push`` does it.
"""

from __future__ import annotations

import contextlib
import logging
import os
import shutil
import tempfile
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

from .backends.base import Backend
from .backends.fs import FsBackend
from .canonical import short
from .errors import CacheError, CorruptObjectError, ObjectNotFoundError
from .hashing import Hasher, Tree
from .manifest import ActionManifest, OutputRecord
from .spec import LinkMode

log = logging.getLogger(__name__)


@dataclass
class TransferStats:
    """Counts from a push or pull."""

    actions: int = 0
    objects: int = 0
    bytes: int = 0
    missing: int = 0


def remove_path(path: Path) -> None:
    """Delete a file, symlink or directory tree if it exists."""
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)


class Cache:
    """Content-addressed artifact cache used by the runner, ``gc`` and ``lineage``."""

    def __init__(
        self,
        local: FsBackend,
        remote: Backend | None = None,
        *,
        link_mode: LinkMode = "copy",
        autopush: bool = False,
    ) -> None:
        self.local = local
        self.remote = remote
        self.link_mode = link_mode
        self.autopush = autopush

    # -- actions ---------------------------------------------------------
    def _parse_action(self, key: str, data: bytes, where: str) -> ActionManifest | None:
        try:
            manifest = ActionManifest.from_bytes(data)
        except CacheError as exc:
            log.warning("ignoring unreadable action %s in %s: %s", short(key), where, exc)
            return None
        if manifest.key != key or manifest.components.key != key:
            log.warning("ignoring action %s in %s: key does not match content", short(key), where)
            return None
        return manifest

    def get_action(self, key: str, *, use_remote: bool = True) -> ActionManifest | None:
        """Look up the manifest for ``key``; remote hits are copied locally."""
        data = self.local.get_action(key)
        if data is not None:
            manifest = self._parse_action(key, data, self.local.location)
            if manifest is not None:
                return manifest
        if use_remote and self.remote is not None:
            data = self.remote.get_action(key)
            if data is not None:
                manifest = self._parse_action(key, data, self.remote.location)
                if manifest is not None:
                    log.info("remote hit for %s (%s)", manifest.stage, short(key))
                    self.local.put_action(key, data)
                    return manifest
        return None

    def put_action(self, manifest: ActionManifest) -> None:
        self.local.put_action(manifest.key, manifest.to_bytes())
        if self.autopush and self.remote is not None:
            self.push_manifest(manifest, TransferStats())

    def touch(self, key: str) -> None:
        self.local.touch_action(key)

    def iter_manifests(self, backend: Backend | None = None) -> Iterator[ActionManifest]:
        """Every readable action manifest in ``backend`` (default: local)."""
        be = backend or self.local
        for key, _ in be.iter_actions():
            data = be.get_action(key)
            if data is None:
                continue
            manifest = self._parse_action(key, data, be.location)
            if manifest is not None:
                yield manifest

    # -- storing outputs -------------------------------------------------
    def store_outputs(
        self, root: Path, outs: Iterable[str], hasher: Hasher
    ) -> tuple[OutputRecord, ...]:
        """Hash each output path and copy its content into the local store."""
        records: list[OutputRecord] = []
        for rel in outs:
            pd = hasher.digest_path(rel)
            if pd.kind == "file":
                self.local.put_object_file(pd.digest, root / rel)
                records.append(OutputRecord(rel, "file", pd.digest, pd.size, pd.executable))
            else:
                assert pd.tree is not None
                for entry in pd.tree.entries:
                    self.local.put_object_file(entry.digest, root / rel / entry.path)
                self.local.put_object_bytes(pd.digest, pd.tree.to_bytes())
                records.append(OutputRecord(rel, "tree", pd.digest, pd.size))
        return tuple(records)

    # -- fetching objects ------------------------------------------------
    def _fetch_object(self, digest: str) -> None:
        """Make sure ``digest`` is in the local store, pulling it if needed."""
        if self.local.has_object(digest):
            return
        if self.remote is None:
            raise ObjectNotFoundError(digest, self.local.location)
        fd, tmp_name = tempfile.mkstemp(dir=self.local.tmp_dir)
        os.close(fd)
        tmp = Path(tmp_name)
        try:
            self.remote.get_object_file(digest, tmp)
            self.local.put_object_file(digest, tmp)
        finally:
            with contextlib.suppress(FileNotFoundError):
                tmp.unlink()

    def load_tree(self, digest: str) -> Tree:
        self._fetch_object(digest)
        return Tree.from_bytes(self.local.get_object_bytes(digest))

    def objects_of(self, record: OutputRecord, backend: Backend | None = None) -> set[str]:
        """Every object digest needed to restore ``record`` (tree + its files)."""
        if record.kind == "file":
            return {record.digest}
        be = backend or self.local
        try:
            tree = Tree.from_bytes(be.get_object_bytes(record.digest))
        except ObjectNotFoundError:
            return {record.digest}
        return {record.digest, *(e.digest for e in tree.entries)}

    def is_available(self, manifest: ActionManifest) -> bool:
        """True when every object of ``manifest`` is in the local store."""
        for rec in manifest.outputs:
            if not self.local.has_object(rec.digest):
                return False
            if rec.kind == "tree":
                try:
                    tree = Tree.from_bytes(self.local.get_object_bytes(rec.digest))
                except (ObjectNotFoundError, CorruptObjectError, CacheError):
                    return False
                if not all(self.local.has_object(e.digest) for e in tree.entries):
                    return False
        return True

    # -- restoring -------------------------------------------------------
    def _materialize_file(self, digest: str, dest: Path, executable: bool) -> None:
        self._fetch_object(digest)
        if self.link_mode == "hardlink" and not executable:
            self.local.link_object(digest, dest)
            return
        self.local.get_object_file(digest, dest)
        os.chmod(dest, 0o755 if executable else 0o644)

    def restore(self, root: Path, records: Iterable[OutputRecord]) -> None:
        """Materialise outputs into the workspace, replacing what is there.

        Directory outputs are assembled in a sibling staging directory and
        swapped in with renames, so a crash never leaves a half-restored tree
        under the real name.
        """
        for rec in records:
            dest = root / rec.path
            if rec.kind == "file":
                if dest.is_dir() and not dest.is_symlink():
                    shutil.rmtree(dest)
                self._materialize_file(rec.digest, dest, rec.executable)
                continue
            tree = self.load_tree(rec.digest)
            dest.parent.mkdir(parents=True, exist_ok=True)
            staging = Path(tempfile.mkdtemp(dir=dest.parent, prefix=f".{dest.name}.restore."))
            try:
                for entry in tree.entries:
                    self._materialize_file(entry.digest, staging / entry.path, entry.executable)
                trash: Path | None = None
                if dest.exists() or dest.is_symlink():
                    trash = dest.parent / f".{dest.name}.old.{os.getpid()}"
                    remove_path(trash)
                    os.replace(dest, trash)
                os.replace(staging, dest)
                if trash is not None:
                    remove_path(trash)
            finally:
                if staging.exists():
                    shutil.rmtree(staging, ignore_errors=True)

    # -- push / pull -----------------------------------------------------
    def push_manifest(self, manifest: ActionManifest, stats: TransferStats) -> None:
        """Copy a manifest and its objects to the remote (objects first)."""
        if self.remote is None:
            raise CacheError("no remote configured")
        for rec in manifest.outputs:
            for digest in sorted(self.objects_of(rec)):
                if not self.local.has_object(digest):
                    stats.missing += 1
                    continue
                if self.remote.put_object_file(digest, self.local.object_path(digest)):
                    stats.objects += 1
                    stats.bytes += self.local.object_path(digest).stat().st_size
        # The manifest goes last: a remote reader that sees it can fetch everything.
        self.remote.put_action(manifest.key, manifest.to_bytes())
        stats.actions += 1

    def push(self, keys: Iterable[str]) -> TransferStats:
        stats = TransferStats()
        for key in keys:
            manifest = self.get_action(key, use_remote=False)
            if manifest is None:
                stats.missing += 1
                continue
            self.push_manifest(manifest, stats)
        return stats

    def _pull_object(self, digest: str, stats: TransferStats) -> None:
        if self.local.has_object(digest):
            return
        try:
            self._fetch_object(digest)
        except ObjectNotFoundError:
            stats.missing += 1
            return
        stats.objects += 1
        stats.bytes += self.local.object_path(digest).stat().st_size

    def pull(self, keys: Iterable[str]) -> TransferStats:
        if self.remote is None:
            raise CacheError("no remote configured")
        stats = TransferStats()
        for key in keys:
            manifest = self.get_action(key, use_remote=True)
            if manifest is None:
                stats.missing += 1
                continue
            stats.actions += 1
            for rec in manifest.outputs:
                self._pull_object(rec.digest, stats)
                if rec.kind == "tree" and self.local.has_object(rec.digest):
                    tree = Tree.from_bytes(self.local.get_object_bytes(rec.digest))
                    for entry in tree.entries:
                        self._pull_object(entry.digest, stats)
        return stats
