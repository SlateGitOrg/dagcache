"""Content hashing of files and directory trees.

* A **file** digest is the SHA-256 of its bytes.
* A **tree** (directory) digest is the SHA-256 of a canonical JSON manifest that
  lists every regular file underneath it as ``(relative path, file digest, size,
  executable bit)`` sorted by path. Empty directories, timestamps, owners and
  permission bits other than "executable" are deliberately ignored, so two
  checkouts of the same content hash identically.

Hashing large files repeatedly is the dominant cost of a no-op ``status``, so
:class:`StatCache` memoises ``(inode, size, mtime_ns, ctime_ns) -> digest`` in a
small SQLite database. Entries whose mtime is too close to the moment they were
hashed are treated as *racy* and re-hashed (the same trick git uses for its
index), because a write landing within the filesystem's timestamp granularity
would otherwise go unnoticed.
"""

from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
import stat
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .canonical import canonical_json, digest_bytes
from .errors import CacheError
from .paths import normalize

log = logging.getLogger(__name__)

CHUNK_SIZE = 1 << 20
#: Entries hashed less than this long after their mtime are re-verified.
RACY_WINDOW_NS = 2_000_000_000

Kind = Literal["file", "tree"]


@dataclass(frozen=True, slots=True)
class TreeEntry:
    """One regular file inside a directory tree."""

    path: str
    digest: str
    size: int
    executable: bool


@dataclass(frozen=True, slots=True)
class Tree:
    """Manifest of a directory: the files it contains, sorted by path."""

    entries: tuple[TreeEntry, ...]

    def to_bytes(self) -> bytes:
        return canonical_json(
            {
                "type": "tree",
                "v": 1,
                "entries": [[e.path, e.digest, e.size, e.executable] for e in self.entries],
            }
        )

    @property
    def digest(self) -> str:
        return digest_bytes(self.to_bytes())

    @property
    def size(self) -> int:
        return sum(e.size for e in self.entries)

    @classmethod
    def from_bytes(cls, data: bytes) -> Tree:
        import json

        try:
            raw = json.loads(data)
            if raw.get("type") != "tree":
                raise ValueError("not a tree manifest")
            entries = tuple(
                TreeEntry(str(p), str(d), int(s), bool(x)) for p, d, s, x in raw["entries"]
            )
        except (ValueError, KeyError, TypeError) as exc:
            raise CacheError(f"malformed tree manifest: {exc}") from exc
        return cls(entries)


@dataclass(frozen=True, slots=True)
class PathDigest:
    """Digest of a workspace path, which may be a file or a directory."""

    path: str
    kind: Kind
    digest: str
    size: int
    tree: Tree | None = None
    executable: bool = False


def hash_stream_file(path: Path) -> tuple[str, int]:
    """Stream ``path`` through SHA-256; returns ``(hex digest, size)``."""
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as fh:
        while chunk := fh.read(CHUNK_SIZE):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


class StatCache:
    """Persistent ``stat -> digest`` memo backed by SQLite.

    Safe to share between threads (a mutex guards the connection) and between
    processes (SQLite's own locking, WAL mode, generous busy timeout).
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), timeout=60.0, check_same_thread=False)
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS files ("
                " path TEXT PRIMARY KEY, ino INTEGER, size INTEGER, mtime_ns INTEGER,"
                " ctime_ns INTEGER, hashed_ns INTEGER, digest TEXT)"
            )
            self._conn.commit()

    def lookup(self, abs_path: str, st: os.stat_result) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT ino, size, mtime_ns, ctime_ns, hashed_ns, digest FROM files WHERE path=?",
                (abs_path,),
            ).fetchone()
        if row is None:
            self.misses += 1
            return None
        ino, size, mtime_ns, ctime_ns, hashed_ns, digest = row
        if (ino, size, mtime_ns, ctime_ns) != (
            st.st_ino,
            st.st_size,
            st.st_mtime_ns,
            st.st_ctime_ns,
        ):
            self.misses += 1
            return None
        if st.st_mtime_ns >= hashed_ns - RACY_WINDOW_NS:
            # Racy: the file may have been rewritten within timestamp granularity.
            self.misses += 1
            return None
        self.hits += 1
        return str(digest)

    def store(self, abs_path: str, st: os.stat_result, digest: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO files VALUES (?,?,?,?,?,?,?)",
                (
                    abs_path,
                    st.st_ino,
                    st.st_size,
                    st.st_mtime_ns,
                    st.st_ctime_ns,
                    time.time_ns(),
                    digest,
                ),
            )
            self._conn.commit()

    def prune_missing(self) -> int:
        """Drop entries for files that no longer exist; returns how many."""
        with self._lock:
            paths = [r[0] for r in self._conn.execute("SELECT path FROM files")]
            gone = [p for p in paths if not os.path.exists(p)]
            self._conn.executemany("DELETE FROM files WHERE path=?", [(p,) for p in gone])
            self._conn.commit()
        return len(gone)

    def close(self) -> None:
        with self._lock:
            self._conn.close()


class Hasher:
    """Hashes workspace paths, optionally through a :class:`StatCache`."""

    def __init__(self, root: Path, stat_cache: StatCache | None = None) -> None:
        self.root = root
        self.stat_cache = stat_cache

    def _file_digest(self, abs_path: Path, st: os.stat_result) -> str:
        key = str(abs_path)
        if self.stat_cache is not None:
            cached = self.stat_cache.lookup(key, st)
            if cached is not None:
                return cached
        digest, _ = hash_stream_file(abs_path)
        if self.stat_cache is not None:
            # Re-stat: if the file changed while we read it, do not memoise.
            st2 = abs_path.stat()
            if (st2.st_size, st2.st_mtime_ns) == (st.st_size, st.st_mtime_ns):
                self.stat_cache.store(key, st2, digest)
        return digest

    def file_digest(self, abs_path: Path) -> tuple[str, int, bool]:
        """Digest, size and executable bit of a regular file."""
        st = abs_path.stat()
        if not stat.S_ISREG(st.st_mode):
            raise CacheError(f"{abs_path} is not a regular file")
        return self._file_digest(abs_path, st), st.st_size, bool(st.st_mode & 0o111)

    def tree(self, abs_dir: Path) -> Tree:
        """Build the manifest for directory ``abs_dir``."""
        entries: list[TreeEntry] = []
        for dirpath, dirnames, filenames in os.walk(abs_dir, followlinks=False):
            for d in dirnames:
                if os.path.islink(os.path.join(dirpath, d)):
                    raise CacheError(
                        f"symlinked directory {os.path.join(dirpath, d)} is not supported"
                    )
            dirnames.sort()
            for name in sorted(filenames):
                full = Path(dirpath) / name
                st = full.stat()  # follows file symlinks: content is what matters
                if not stat.S_ISREG(st.st_mode):
                    continue
                rel = full.relative_to(abs_dir).as_posix()
                entries.append(
                    TreeEntry(
                        rel, self._file_digest(full, st), st.st_size, bool(st.st_mode & 0o111)
                    )
                )
        entries.sort(key=lambda e: e.path)
        return Tree(tuple(entries))

    def digest_path(self, rel: str) -> PathDigest:
        """Hash the workspace path ``rel``. Raises ``FileNotFoundError`` if absent."""
        rel = normalize(rel)
        abs_path = self.root / rel
        st = abs_path.stat()
        if stat.S_ISDIR(st.st_mode):
            tree = self.tree(abs_path)
            return PathDigest(rel, "tree", tree.digest, tree.size, tree)
        if stat.S_ISREG(st.st_mode):
            return PathDigest(
                rel,
                "file",
                self._file_digest(abs_path, st),
                st.st_size,
                executable=bool(st.st_mode & 0o111),
            )
        raise CacheError(f"{rel} is neither a regular file nor a directory")
