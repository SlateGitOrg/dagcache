"""Canonical serialisation and digest helpers.

Cache keys must be byte-for-byte stable across processes, machines and Python
versions, so everything that feeds a key goes through :func:`canonical_json`:
sorted keys, no insignificant whitespace, UTF-8, and a restricted value domain
(no NaN/Infinity, which JSON cannot represent portably).
"""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any

HASH_NAME = "sha256"
DIGEST_HEX_LEN = 64


def _check(value: Any, where: str) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"non-finite float at {where} cannot be canonicalised")
    if isinstance(value, dict):
        for k, v in value.items():
            if not isinstance(k, str):
                raise TypeError(f"non-string key {k!r} at {where}")
            _check(v, f"{where}.{k}")
    elif isinstance(value, list | tuple):
        for i, v in enumerate(value):
            _check(v, f"{where}[{i}]")
    elif not isinstance(value, str | int | float | bool | type(None)):
        raise TypeError(f"unsupported type {type(value).__name__} at {where}")


def canonical_json(value: Any) -> bytes:
    """Serialise ``value`` deterministically.

    Tuples are encoded as lists. Raises ``TypeError``/``ValueError`` for values
    that have no stable JSON form.
    """
    _check(value, "$")
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def digest_bytes(data: bytes) -> str:
    """Hex SHA-256 of ``data``."""
    return hashlib.sha256(data).hexdigest()


def digest_json(value: Any) -> str:
    """Hex SHA-256 of the canonical JSON encoding of ``value``."""
    return digest_bytes(canonical_json(value))


def is_digest(text: str) -> bool:
    """True when ``text`` looks like a hex SHA-256 digest."""
    if len(text) != DIGEST_HEX_LEN:
        return False
    try:
        int(text, 16)
    except ValueError:
        return False
    return text == text.lower()


def short(digest: str | None, n: int = 10) -> str:
    """Abbreviated digest for human output."""
    if digest is None:
        return "<none>"
    return digest[:n]
