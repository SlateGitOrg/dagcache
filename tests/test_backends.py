"""Backend contract, run against the filesystem store and an in-process S3 (moto)."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from dagcache.backends import Backend, FsBackend, S3Backend, open_backend, parse_s3_url
from dagcache.errors import ConfigError, CorruptObjectError, ObjectNotFoundError


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def s3_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for k, v in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(k, v)
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="cache-bucket")
        yield


@pytest.fixture(params=["fs", "s3"])
def backend(request: pytest.FixtureRequest, tmp_path: Path) -> Backend:
    if request.param == "fs":
        return FsBackend(tmp_path / "store")
    request.getfixturevalue("s3_env")
    return S3Backend.from_url("s3://cache-bucket/team/proj", region="us-east-1")


def test_object_round_trip(backend: Backend, tmp_path: Path) -> None:
    src = tmp_path / "src.bin"
    data = b"payload" * 1000
    src.write_bytes(data)
    d = sha(data)
    assert not backend.has_object(d)
    assert backend.put_object_file(d, src) is True
    assert backend.put_object_file(d, src) is False  # idempotent
    assert backend.has_object(d)
    dest = tmp_path / "out" / "copy.bin"
    backend.get_object_file(d, dest)
    assert dest.read_bytes() == data
    assert backend.get_object_bytes(d) == data
    assert dict(backend.iter_objects()) == {d: len(data)}
    backend.delete_object(d)
    assert not backend.has_object(d)
    assert list(backend.iter_objects()) == []


def test_bytes_objects_and_digest_check(backend: Backend, tmp_path: Path) -> None:
    data = b'{"tree": 1}'
    assert backend.put_object_bytes(sha(data), data)
    assert not backend.put_object_bytes(sha(data), data)
    with pytest.raises(CorruptObjectError):
        backend.put_object_bytes(sha(b"other"), data)
    src = tmp_path / "f"
    src.write_bytes(b"abc")
    with pytest.raises(CorruptObjectError):
        backend.put_object_file(sha(b"not abc"), src)
    assert not backend.has_object(sha(b"not abc"))


def test_missing_objects_raise(backend: Backend, tmp_path: Path) -> None:
    missing = sha(b"nothing")
    with pytest.raises(ObjectNotFoundError):
        backend.get_object_file(missing, tmp_path / "x")
    with pytest.raises(ObjectNotFoundError):
        backend.get_object_bytes(missing)
    assert not (tmp_path / "x").exists()
    assert not [p for p in tmp_path.iterdir() if p.name.startswith(".x.")]


def test_actions_round_trip(backend: Backend) -> None:
    key = sha(b"key")
    assert backend.get_action(key) is None
    backend.put_action(key, b'{"a": 1}')
    backend.put_action(key, b'{"a": 2}')  # overwrite is allowed
    assert backend.get_action(key) == b'{"a": 2}'
    listed = dict(backend.iter_actions())
    assert list(listed) == [key] and listed[key] > 0
    backend.touch_action(key)
    backend.delete_action(key)
    assert backend.get_action(key) is None
    assert list(backend.iter_actions()) == []


def test_fs_detects_bit_rot_on_read(tmp_path: Path) -> None:
    be = FsBackend(tmp_path / "s")
    data = b"good"
    be.put_object_bytes(sha(data), data)
    path = be.object_path(sha(data))
    path.chmod(0o644)
    path.write_bytes(b"evil")
    assert not be.verify_object(sha(data))
    with pytest.raises(CorruptObjectError):
        be.get_object_bytes(sha(data))
    with pytest.raises(CorruptObjectError):
        be.get_object_file(sha(data), tmp_path / "dest")
    assert not (tmp_path / "dest").exists()


def test_fs_ignores_foreign_files_and_cleans_tmp(tmp_path: Path) -> None:
    be = FsBackend(tmp_path / "s")
    (be.objects_dir / "zz").mkdir()
    (be.objects_dir / "zz" / "not-a-digest").write_text("x")
    (be.objects_dir / "README").write_text("x")
    (be.actions_dir / "ab").mkdir()
    (be.actions_dir / "ab" / "notes.txt").write_text("x")
    assert list(be.iter_objects()) == [] and list(be.iter_actions()) == []
    (be.tmp_dir / "old").write_text("x")
    (be.tmp_dir / "olddir").mkdir()
    assert be.clean_tmp(older_than=-1) == 2


def test_fs_link_object(tmp_path: Path) -> None:
    be = FsBackend(tmp_path / "s")
    be.put_object_bytes(sha(b"x"), b"x")
    be.link_object(sha(b"x"), tmp_path / "w" / "x")
    assert (tmp_path / "w" / "x").stat().st_ino == be.object_path(sha(b"x")).stat().st_ino
    with pytest.raises(ObjectNotFoundError):
        be.link_object(sha(b"y"), tmp_path / "w" / "y")


def test_open_backend_urls(tmp_path: Path, s3_env: None) -> None:
    fs = open_backend("rel/cache", root=tmp_path)
    assert isinstance(fs, FsBackend) and fs.root == tmp_path / "rel/cache"
    absolute = open_backend(f"file://{tmp_path}/abs", root=tmp_path)
    assert isinstance(absolute, FsBackend) and absolute.root == tmp_path / "abs"
    s3 = open_backend("s3://cache-bucket", root=tmp_path, s3_region="us-east-1")
    assert isinstance(s3, S3Backend) and s3.location == "s3://cache-bucket"


def test_parse_s3_url() -> None:
    assert parse_s3_url("s3://b/p/q/") == ("b", "p/q")
    assert parse_s3_url("s3://b") == ("b", "")
    with pytest.raises(ConfigError):
        parse_s3_url("s3:///nobucket")
    with pytest.raises(ConfigError):
        parse_s3_url("gs://b")


def test_s3_reports_non_404_errors(s3_env: None) -> None:
    from dagcache.errors import CacheError

    be = S3Backend.from_url("s3://no-such-bucket/x", region="us-east-1")
    with pytest.raises(CacheError):
        be.has_object(sha(b"a"))
    with pytest.raises(CacheError):
        be.get_action(sha(b"a"))
