"""S3-compatible backend (AWS S3, MinIO, Ceph RGW, R2, ...).

Uses the same key layout as :mod:`.fs` under an optional prefix::

    s3://bucket/prefix/objects/ab/cdef...
    s3://bucket/prefix/actions/12/3456....json

S3 ``PUT`` is atomic per object (readers see the old or the new body, never a
mix), so no staging is needed for writes. Downloads go to a temporary file next
to the destination and are verified against the digest before being renamed
into place. Credentials come from the standard AWS chain (environment,
``~/.aws``, instance role); dagcache never stores them.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..canonical import digest_bytes, is_digest
from ..errors import CacheError, ConfigError, CorruptObjectError, ObjectNotFoundError
from .base import Backend

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_NOT_FOUND = {"404", "NoSuchKey", "NotFound"}


def _is_not_found(exc: Exception) -> bool:
    response: Any = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    return str(response.get("Error", {}).get("Code")) in _NOT_FOUND


def parse_s3_url(url: str) -> tuple[str, str]:
    """``s3://bucket/some/prefix`` -> ``("bucket", "some/prefix")``."""
    if not url.startswith("s3://"):
        raise ConfigError(f"not an s3 url: {url}")
    rest = url[len("s3://") :]
    bucket, _, prefix = rest.partition("/")
    if not bucket:
        raise ConfigError(f"s3 url has no bucket: {url}")
    return bucket, prefix.strip("/")


class S3Backend(Backend):
    """Content-addressed store in an S3 bucket."""

    def __init__(self, bucket: str, prefix: str, client: S3Client) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = client
        self.location = f"s3://{bucket}/{self.prefix}" if self.prefix else f"s3://{bucket}"

    @classmethod
    def from_url(
        cls, url: str, *, endpoint_url: str | None = None, region: str | None = None
    ) -> S3Backend:
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - depends on install extras
            raise ConfigError(
                "S3 remotes need boto3: install with `pip install 'dagcache[s3]'`"
            ) from exc
        bucket, prefix = parse_s3_url(url)
        client = boto3.client("s3", endpoint_url=endpoint_url, region_name=region)
        return cls(bucket, prefix, client)

    # -- key helpers -----------------------------------------------------
    def _key(self, *parts: str) -> str:
        return "/".join(p for p in (self.prefix, *parts) if p)

    def _obj_key(self, digest: str) -> str:
        return self._key("objects", digest[:2], digest[2:])

    def _action_key(self, key: str) -> str:
        return self._key("actions", key[:2], f"{key[2:]}.json")

    @contextmanager
    def _guard(self, what: str) -> Iterator[None]:
        """Turn transport/service errors into :class:`CacheError`."""
        try:
            yield
        except CacheError:
            raise
        except Exception as exc:
            raise CacheError(f"{self.location}: {what} failed: {exc}") from exc

    def _exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise CacheError(f"{self.location}: head {key} failed: {exc}") from exc
        return True

    def _list(self, sub: str) -> Iterator[dict[str, Any]]:
        paginator = self.client.get_paginator("list_objects_v2")
        with self._guard(f"listing {sub}"):
            pages = list(paginator.paginate(Bucket=self.bucket, Prefix=self._key(sub) + "/"))
        for page in pages:
            for item in page.get("Contents", []):
                yield dict(item)

    # -- objects ---------------------------------------------------------
    def has_object(self, digest: str) -> bool:
        return self._exists(self._obj_key(digest))

    def put_object_file(self, digest: str, src: Path) -> bool:
        if self.has_object(digest):
            return False
        h = hashlib.sha256()
        with src.open("rb") as fh:
            while chunk := fh.read(1 << 20):
                h.update(chunk)
        if h.hexdigest() != digest:
            raise CorruptObjectError(digest, h.hexdigest(), f"source file {src}")
        with self._guard("upload"):
            self.client.upload_file(str(src), self.bucket, self._obj_key(digest))
        return True

    def put_object_bytes(self, digest: str, data: bytes) -> bool:
        actual = digest_bytes(data)
        if actual != digest:
            raise CorruptObjectError(digest, actual, "in-memory object")
        if self.has_object(digest):
            return False
        with self._guard("put"):
            self.client.put_object(Bucket=self.bucket, Key=self._obj_key(digest), Body=data)
        return True

    def get_object_file(self, digest: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=dest.parent, prefix=f".{dest.name}.", suffix=".tmp")
        os.close(fd)
        tmp = Path(tmp_name)
        try:
            try:
                self.client.download_file(self.bucket, self._obj_key(digest), str(tmp))
            except Exception as exc:
                if _is_not_found(exc):
                    raise ObjectNotFoundError(digest, self.location) from exc
                raise CacheError(f"{self.location}: download failed: {exc}") from exc
            h = hashlib.sha256()
            with tmp.open("rb") as fh:
                while chunk := fh.read(1 << 20):
                    h.update(chunk)
            if h.hexdigest() != digest:
                raise CorruptObjectError(digest, h.hexdigest(), self.location)
            os.replace(tmp, dest)
        finally:
            with contextlib.suppress(FileNotFoundError):
                tmp.unlink()

    def get_object_bytes(self, digest: str) -> bytes:
        try:
            resp = self.client.get_object(Bucket=self.bucket, Key=self._obj_key(digest))
        except Exception as exc:
            if _is_not_found(exc):
                raise ObjectNotFoundError(digest, self.location) from exc
            raise CacheError(f"{self.location}: get failed: {exc}") from exc
        data = resp["Body"].read()
        actual = digest_bytes(data)
        if actual != digest:
            raise CorruptObjectError(digest, actual, self.location)
        return data

    def iter_objects(self) -> Iterator[tuple[str, int]]:
        for item in self._list("objects"):
            parts = str(item["Key"]).split("/")
            digest = parts[-2] + parts[-1]
            if is_digest(digest):
                yield digest, int(item.get("Size", 0))

    def delete_object(self, digest: str) -> None:
        with self._guard("delete"):
            self.client.delete_object(Bucket=self.bucket, Key=self._obj_key(digest))

    # -- actions ---------------------------------------------------------
    def get_action(self, key: str) -> bytes | None:
        try:
            resp = self.client.get_object(Bucket=self.bucket, Key=self._action_key(key))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise CacheError(f"{self.location}: get action failed: {exc}") from exc
        body: bytes = resp["Body"].read()
        return body

    def put_action(self, key: str, data: bytes) -> None:
        with self._guard("put action"):
            self.client.put_object(
                Bucket=self.bucket,
                Key=self._action_key(key),
                Body=data,
                ContentType="application/json",
            )

    def iter_actions(self) -> Iterator[tuple[str, float]]:
        for item in self._list("actions"):
            parts = str(item["Key"]).split("/")
            name = parts[-1]
            if not name.endswith(".json"):
                continue
            key = parts[-2] + name[: -len(".json")]
            if is_digest(key):
                modified = item.get("LastModified")
                ts = modified.timestamp() if modified is not None else 0.0
                yield key, ts

    def delete_action(self, key: str) -> None:
        with self._guard("delete action"):
            self.client.delete_object(Bucket=self.bucket, Key=self._action_key(key))
