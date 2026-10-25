"""File/tree hashing and the stat cache."""

from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from dagcache import hashing
from dagcache.errors import CacheError
from dagcache.hashing import Hasher, StatCache, Tree, TreeEntry, hash_stream_file


def test_file_digest_is_sha256(tmp_path: Path) -> None:
    f = tmp_path / "f.bin"
    data = os.urandom(3 * hashing.CHUNK_SIZE + 17)
    f.write_bytes(data)
    assert hash_stream_file(f) == (hashlib.sha256(data).hexdigest(), len(data))
    pd = Hasher(tmp_path).digest_path("f.bin")
    assert pd.kind == "file" and pd.digest == hashlib.sha256(data).hexdigest()


def test_tree_digest_ignores_metadata_but_not_content(tmp_path: Path) -> None:
    for root in ("a", "b"):
        (tmp_path / root / "sub").mkdir(parents=True)
        (tmp_path / root / "sub" / "x").write_text("x")
        (tmp_path / root / "y").write_text("y")
    (tmp_path / "b" / "empty-dir").mkdir()
    os.utime(tmp_path / "b" / "y", (1, 1))
    h = Hasher(tmp_path)
    a, b = h.digest_path("a"), h.digest_path("b")
    assert a.kind == "tree" and a.digest == b.digest and a.size == 2
    assert a.tree is not None and [e.path for e in a.tree.entries] == ["sub/x", "y"]
    (tmp_path / "b" / "y").write_text("Y")
    assert h.digest_path("b").digest != a.digest


def test_exec_bit_is_part_of_tree_identity(tmp_path: Path) -> None:
    (tmp_path / "d").mkdir()
    script = tmp_path / "d" / "run.sh"
    script.write_text("echo")
    h = Hasher(tmp_path)
    before = h.digest_path("d").digest
    script.chmod(0o755)
    assert h.digest_path("d").digest != before
    assert h.digest_path("d/run.sh").executable


def test_tree_manifest_round_trip() -> None:
    t = Tree((TreeEntry("a", "0" * 64, 1, False), TreeEntry("b/c", "1" * 64, 2, True)))
    assert Tree.from_bytes(t.to_bytes()) == t
    for bad in [b"{}", b"nope", b'{"type": "tree"}', b'{"type":"blob","entries":[]}']:
        with pytest.raises(CacheError):
            Tree.from_bytes(bad)


@settings(max_examples=40, deadline=None)
@given(
    st.dictionaries(
        st.from_regex(r"[a-c]{1,2}(/[a-c]{1,2})?", fullmatch=True),
        st.binary(max_size=40),
        min_size=1,
        max_size=6,
    )
)
def test_tree_digest_independent_of_creation_order(files: dict[str, bytes]) -> None:
    import tempfile

    # Drop paths that are prefixes of others (a file cannot also be a directory).
    names = [n for n in files if not any(o.startswith(n + "/") for o in files)]
    digests = []
    for order in (names, list(reversed(names))):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for n in order:
                (root / "t" / n).parent.mkdir(parents=True, exist_ok=True)
                (root / "t" / n).write_bytes(files[n])
            digests.append(Hasher(root).digest_path("t").digest)
    assert digests[0] == digests[1]


def test_symlinked_directory_rejected_and_special_files(tmp_path: Path) -> None:
    (tmp_path / "real").mkdir()
    (tmp_path / "d").mkdir()
    (tmp_path / "d" / "link").symlink_to(tmp_path / "real")
    with pytest.raises(CacheError, match="symlinked directory"):
        Hasher(tmp_path).digest_path("d")
    with pytest.raises(FileNotFoundError):
        Hasher(tmp_path).digest_path("missing")
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    with pytest.raises(CacheError, match="neither"):
        Hasher(tmp_path).digest_path("fifo")
    with pytest.raises(CacheError, match="not a regular file"):
        Hasher(tmp_path).file_digest(fifo)
    (tmp_path / "e").mkdir()
    os.mkfifo(tmp_path / "e" / "pipe")
    (tmp_path / "e" / "f").write_text("f")
    assert [e.path for e in Hasher(tmp_path).tree(tmp_path / "e").entries] == ["f"]


def test_stat_cache_hits_only_when_safe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    f = tmp_path / "data.bin"
    f.write_bytes(b"x" * 1000)
    old = time.time() - 60
    os.utime(f, (old, old))
    sc = StatCache(tmp_path / "state.db")
    h = Hasher(tmp_path, sc)
    first = h.digest_path("data.bin").digest
    assert sc.misses == 1 and sc.hits == 0
    assert h.digest_path("data.bin").digest == first
    assert sc.hits == 1
    # Rewrite with same size and same mtime: ctime/inode still betray the change.
    f.write_bytes(b"y" * 1000)
    os.utime(f, (old, old))
    assert h.digest_path("data.bin").digest != first
    # A freshly written file is "racy" and never trusted from the cache.
    g = tmp_path / "fresh.bin"
    g.write_bytes(b"z")
    h.digest_path("fresh.bin")
    h.digest_path("fresh.bin")
    assert sc.hits == 1
    monkeypatch.setattr(hashing, "RACY_WINDOW_NS", -(10**12))
    assert h.digest_path("fresh.bin").digest == hashlib.sha256(b"z").hexdigest()
    assert sc.hits == 2
    g.unlink()
    assert sc.prune_missing() == 1
    sc.close()


def test_stat_cache_persists_across_instances(tmp_path: Path) -> None:
    f = tmp_path / "a"
    f.write_text("a")
    os.utime(f, (1_000_000, 1_000_000))
    sc = StatCache(tmp_path / "db")
    Hasher(tmp_path, sc).file_digest(f)
    sc.close()
    sc2 = StatCache(tmp_path / "db")
    Hasher(tmp_path, sc2).file_digest(f)
    assert sc2.hits == 1
    sc2.close()


def test_stat_cache_concurrent_first_open(tmp_path: Path) -> None:
    """Many processes creating the database at once must all succeed (WAL switch race)."""
    import subprocess
    import sys

    code = (
        "import os, sys, time; from pathlib import Path; from dagcache.hashing import StatCache\n"
        "while time.time() < float(sys.argv[3]): pass\n"  # start together
        "sc = StatCache(Path(sys.argv[1]))\n"
        "assert sc._conn is not None\n"
        "st = os.stat(sys.argv[1])\n"
        "sc.store(sys.argv[2], st, 'd' * 64); sc.close()\n"
    )
    import time

    db = tmp_path / "state.db"
    start = str(time.time() + 1.5)
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", code, str(db), f"/p{i}", start], stderr=subprocess.PIPE
        )
        for i in range(8)
    ]
    errors = [p.communicate(timeout=60)[1] for p in procs]
    assert all(p.returncode == 0 for p in procs), errors


def test_stat_cache_degrades_when_unusable(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    bad = tmp_path / "db-is-a-dir"
    bad.mkdir()
    sc = StatCache(bad)
    assert "stat cache disabled" in caplog.text
    f = tmp_path / "f"
    f.write_text("x")
    h = Hasher(tmp_path, sc)
    assert h.digest_path("f").digest == hashlib.sha256(b"x").hexdigest()
    assert sc.prune_missing() == 0
    sc.close()
    # A connection that breaks mid-run: queries fail, hashing still works.
    sc2 = StatCache(tmp_path / "ok.db")
    assert sc2._conn is not None
    sc2._conn.close()
    assert Hasher(tmp_path, sc2).digest_path("f").digest == hashlib.sha256(b"x").hexdigest()
