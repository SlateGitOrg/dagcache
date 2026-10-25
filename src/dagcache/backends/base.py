"""Storage backend interface.

A backend stores two namespaces:

* **objects** - immutable blobs addressed by the SHA-256 of their content
  (file contents and directory-tree manifests);
* **actions** - small JSON manifests addressed by a stage *cache key*, recording
  which objects a stage produced for that key.

Implementations must make every write atomic (readers never observe a partial
object or manifest) and idempotent (writing an existing object is a no-op).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from pathlib import Path


class Backend(ABC):
    """Abstract content-addressed store."""

    #: Human-readable location used in log and error messages.
    location: str

    # -- objects ---------------------------------------------------------
    @abstractmethod
    def has_object(self, digest: str) -> bool: ...

    @abstractmethod
    def put_object_file(self, digest: str, src: Path) -> bool:
        """Store the file ``src`` under ``digest``; returns False if already present.

        Must raise :class:`~dagcache.errors.CorruptObjectError` if ``src`` does not
        hash to ``digest``.
        """

    @abstractmethod
    def put_object_bytes(self, digest: str, data: bytes) -> bool: ...

    @abstractmethod
    def get_object_file(self, digest: str, dest: Path) -> None:
        """Write the object to ``dest`` atomically, verifying its digest."""

    @abstractmethod
    def get_object_bytes(self, digest: str) -> bytes: ...

    @abstractmethod
    def iter_objects(self) -> Iterator[tuple[str, int]]:
        """Yield ``(digest, size)`` for every stored object."""

    @abstractmethod
    def delete_object(self, digest: str) -> None: ...

    # -- actions ---------------------------------------------------------
    @abstractmethod
    def get_action(self, key: str) -> bytes | None: ...

    @abstractmethod
    def put_action(self, key: str, data: bytes) -> None: ...

    @abstractmethod
    def iter_actions(self) -> Iterator[tuple[str, float]]:
        """Yield ``(key, last-used unix time)`` for every action manifest."""

    @abstractmethod
    def delete_action(self, key: str) -> None: ...

    def touch_action(self, key: str) -> None:  # noqa: B027 - optional hook
        """Record that ``key`` was just used (for age-based GC). Optional."""
