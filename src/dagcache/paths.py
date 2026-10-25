"""Workspace-relative path handling.

All paths stored in pipelines, cache keys, lock files and manifests are POSIX
strings relative to the workspace root. Keeping them relative is what makes a
cache key portable between two clones of the same repository.
"""

from __future__ import annotations

import posixpath
from pathlib import PurePosixPath

from .errors import ConfigError


def normalize(path: str, *, what: str = "path") -> str:
    """Return the canonical workspace-relative form of ``path``.

    Rejects absolute paths and paths that escape the workspace via ``..``.
    ``"."`` is returned for the workspace root itself.
    """
    if not path or not path.strip():
        raise ConfigError(f"empty {what}")
    text = path.replace("\\", "/")
    if text.startswith("/") or (len(text) > 1 and text[1] == ":"):
        raise ConfigError(f"{what} '{path}' must be relative to the workspace root")
    norm = posixpath.normpath(text)
    if norm == ".." or norm.startswith("../"):
        raise ConfigError(f"{what} '{path}' escapes the workspace root")
    return norm


def is_within(child: str, parent: str) -> bool:
    """True when ``child`` equals ``parent`` or lies underneath it."""
    if parent == ".":
        return True
    return child == parent or child.startswith(parent + "/")


def overlaps(a: str, b: str) -> bool:
    """True when one path equals or contains the other."""
    return is_within(a, b) or is_within(b, a)


def relative_to(child: str, parent: str) -> str:
    """``child`` expressed relative to ``parent`` (which must contain it)."""
    if not is_within(child, parent):
        raise ValueError(f"{child} is not within {parent}")
    if child == parent:
        return "."
    if parent == ".":
        return child
    return str(PurePosixPath(child).relative_to(parent))
