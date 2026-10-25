"""Filesystem backend: the local cache, or a remote on a shared/NFS directory.

Layout (git-like fan-out keeps directories small)::

    <root>/objects/ab/cdef0123...      immutable, mode 0444
    <root>/actions/12/3456789a....json action manifests
    <root>/tmp/                        staging area for atomic writes
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import shutil
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

from ..canonical import digest_bytes, is_digest
from ..errors import CorruptObjectError, ObjectNotFoundError
from .base import Backend

_COPY_CHUNK = 1 << 20
#: Staging files older than this are considered abandoned by ``gc``.
STALE_TMP_SECONDS = 3600


class FsBackend(Backend):
    """Content-addressed store in a local directory."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.location = str(root)
        self.objects_dir = root / "objects"
        self.actions_dir = root / "actions"
        self.tmp_dir = root / "tmp"
        for d in (self.objects_dir, self.actions_dir, self.tmp_dir):
            d.mkdir(parents=True, exist_ok=True)

    # -- helpers ---------------------------------------------------------
    def object_path(self, digest: str) -> Path:
        return self.objects_dir / digest[:2] / digest[2:]

    def _action_path(self, key: str) -> Path:
        return self.actions_dir / key[:2] / f"{key[2:]}.json"

    def _tmp(self, suffix: str = "") -> tuple[int, Path]:
        fd, name = tempfile.mkstemp(dir=self.tmp_dir, suffix=suffix)
        return fd, Path(name)

    def _publish(self, tmp: Path, final: Path) -> None:
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp, final)

    # -- objects ---------------------------------------------------------
    def has_object(self, digest: str) -> bool:
        return self.object_path(digest).is_file()

    def put_object_file(self, digest: str, src: Path) -> bool:
        if self.has_object(digest):
            return False
        fd, tmp = self._tmp()
        try:
            h = hashlib.sha256()
            with os.fdopen(fd, "wb") as out, src.open("rb") as inp:
                while chunk := inp.read(_COPY_CHUNK):
                    h.update(chunk)
                    out.write(chunk)
            actual = h.hexdigest()
            if actual != digest:
                raise CorruptObjectError(digest, actual, f"source file {src}")
            os.chmod(tmp, 0o444)
            self._publish(tmp, self.object_path(digest))
        finally:
            with contextlib.suppress(FileNotFoundError):
                tmp.unlink()
        return True

    def put_object_bytes(self, digest: str, data: bytes) -> bool:
        actual = digest_bytes(data)
        if actual != digest:
            raise CorruptObjectError(digest, actual, "in-memory object")
        if self.has_object(digest):
            return False
        fd, tmp = self._tmp()
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(data)
            os.chmod(tmp, 0o444)
            self._publish(tmp, self.object_path(digest))
        finally:
            with contextlib.suppress(FileNotFoundError):
                tmp.unlink()
        return True

    def get_object_file(self, digest: str, dest: Path) -> None:
        src = self.object_path(digest)
        if not src.is_file():
            raise ObjectNotFoundError(digest, self.location)
        dest.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=dest.parent, prefix=f".{dest.name}.", suffix=".tmp")
        tmp = Path(tmp_name)
        try:
            h = hashlib.sha256()
            with os.fdopen(fd, "wb") as out, src.open("rb") as inp:
                while chunk := inp.read(_COPY_CHUNK):
                    h.update(chunk)
                    out.write(chunk)
            if h.hexdigest() != digest:
                raise CorruptObjectError(digest, h.hexdigest(), self.location)
            os.replace(tmp, dest)
        finally:
            with contextlib.suppress(FileNotFoundError):
                tmp.unlink()

    def link_object(self, digest: str, dest: Path) -> None:
        """Hard-link the (read-only) object to ``dest`` atomically."""
        src = self.object_path(digest)
        if not src.is_file():
            raise ObjectNotFoundError(digest, self.location)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.parent / f".{dest.name}.{os.getpid()}.{time.monotonic_ns()}.lnk"
        os.link(src, tmp)
        try:
            os.replace(tmp, dest)
        finally:
            with contextlib.suppress(FileNotFoundError):
                tmp.unlink()

    def get_object_bytes(self, digest: str) -> bytes:
        try:
            data = self.object_path(digest).read_bytes()
        except FileNotFoundError:
            raise ObjectNotFoundError(digest, self.location) from None
        actual = digest_bytes(data)
        if actual != digest:
            raise CorruptObjectError(digest, actual, self.location)
        return data

    def iter_objects(self) -> Iterator[tuple[str, int]]:
        if not self.objects_dir.is_dir():
            return
        for fan in sorted(self.objects_dir.iterdir()):
            if not fan.is_dir():
                continue
            for obj in sorted(fan.iterdir()):
                digest = fan.name + obj.name
                if is_digest(digest):
                    yield digest, obj.stat().st_size

    def delete_object(self, digest: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            self.object_path(digest).unlink()

    def verify_object(self, digest: str) -> bool:
        """Re-hash a stored object; False if its content no longer matches."""
        h = hashlib.sha256()
        with self.object_path(digest).open("rb") as fh:
            while chunk := fh.read(_COPY_CHUNK):
                h.update(chunk)
        return h.hexdigest() == digest

    # -- actions ---------------------------------------------------------
    def get_action(self, key: str) -> bytes | None:
        try:
            return self._action_path(key).read_bytes()
        except FileNotFoundError:
            return None

    def put_action(self, key: str, data: bytes) -> None:
        fd, tmp = self._tmp(".json")
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(data)
            self._publish(tmp, self._action_path(key))
        finally:
            with contextlib.suppress(FileNotFoundError):
                tmp.unlink()

    def iter_actions(self) -> Iterator[tuple[str, float]]:
        if not self.actions_dir.is_dir():
            return
        for fan in sorted(self.actions_dir.iterdir()):
            if not fan.is_dir():
                continue
            for entry in sorted(fan.iterdir()):
                if entry.suffix != ".json":
                    continue
                key = fan.name + entry.stem
                if is_digest(key):
                    yield key, entry.stat().st_mtime

    def delete_action(self, key: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            self._action_path(key).unlink()

    def touch_action(self, key: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            os.utime(self._action_path(key))

    # -- maintenance -----------------------------------------------------
    def clean_tmp(self, older_than: float = STALE_TMP_SECONDS) -> int:
        """Remove abandoned staging files; returns how many were removed."""
        removed = 0
        cutoff = time.time() - older_than
        for entry in self.tmp_dir.iterdir():
            try:
                if entry.stat().st_mtime < cutoff:
                    if entry.is_dir():
                        shutil.rmtree(entry)
                    else:
                        entry.unlink()
                    removed += 1
            except FileNotFoundError:
                continue
        return removed
