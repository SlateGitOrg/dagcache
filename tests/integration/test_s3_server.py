"""S3 backend against a real S3-compatible server in Docker (``make integration``)."""

from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from pathlib import Path

import pytest
from conftest import CHAIN_PARAMS, CHAIN_YAML, WsFactory, actions, repro, run_count, write

from dagcache.backends import S3Backend

ENDPOINT = os.environ.get("DAGCACHE_TEST_S3_ENDPOINT")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not ENDPOINT, reason="DAGCACHE_TEST_S3_ENDPOINT not set"),
]


@pytest.fixture
def bucket() -> str:
    import boto3

    name = f"dagcache-{uuid.uuid4().hex[:10]}"
    boto3.client("s3", endpoint_url=ENDPOINT, region_name="us-east-1").create_bucket(Bucket=name)
    return name


def test_backend_contract_against_real_s3(bucket: str, tmp_path: Path) -> None:
    be = S3Backend.from_url(f"s3://{bucket}/p", endpoint_url=ENDPOINT, region="us-east-1")
    data = os.urandom(9 * 1024 * 1024)  # large enough for a multipart upload
    src = tmp_path / "big"
    src.write_bytes(data)
    digest = hashlib.sha256(data).hexdigest()
    assert be.put_object_file(digest, src)
    assert be.has_object(digest) and not be.put_object_file(digest, src)
    be.get_object_file(digest, tmp_path / "back")
    assert (tmp_path / "back").read_bytes() == data
    assert dict(be.iter_objects()) == {digest: len(data)}
    be.put_action("a" * 64, b"{}")
    assert be.get_action("a" * 64) == b"{}"
    assert [k for k, _ in be.iter_actions()] == ["a" * 64]


def test_fresh_clone_restores_from_real_s3(bucket: str, tmp_path: Path, make_ws: WsFactory) -> None:
    yaml = (
        f"cache:\n  remote: s3://{bucket}/team\n  autopush: true\n"
        f"  s3:\n    endpoint_url: {ENDPOINT}\n    region: us-east-1\n" + CHAIN_YAML
    )
    a = make_ws(yaml, CHAIN_PARAMS, root=tmp_path / "a")
    write(a.root / "raw.txt", "hello world\n")
    first = repro(a)
    assert first.ok, [r.error for r in first.results]
    b_root = tmp_path / "b"
    b_root.mkdir()
    for name in ("dagcache.yaml", "params.yaml", "dagcache.lock", "raw.txt"):
        shutil.copy(a.root / name, b_root / name)
    b = make_ws(root=b_root)
    assert set(actions(repro(b)).values()) == {"restored"}
    assert run_count(b, "prepare") == 0
