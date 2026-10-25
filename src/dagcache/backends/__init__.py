"""Cache storage backends and URL-based construction."""

from __future__ import annotations

from pathlib import Path

from .base import Backend
from .fs import FsBackend
from .s3 import S3Backend, parse_s3_url

__all__ = ["Backend", "FsBackend", "S3Backend", "open_backend", "parse_s3_url"]


def open_backend(
    url: str,
    *,
    root: Path,
    s3_endpoint_url: str | None = None,
    s3_region: str | None = None,
) -> Backend:
    """Open a backend from ``s3://bucket/prefix``, ``file:///abs/dir`` or a path.

    Relative paths are resolved against the workspace ``root``.
    """
    if url.startswith("s3://"):
        return S3Backend.from_url(url, endpoint_url=s3_endpoint_url, region=s3_region)
    if url.startswith("file://"):
        return FsBackend(Path(url[len("file://") :]))
    path = Path(url).expanduser()
    return FsBackend(path if path.is_absolute() else root / path)
