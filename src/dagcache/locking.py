"""Advisory file locks (``flock``) used to coordinate concurrent dagcache runs.

Lock hierarchy (always acquired in this order, never two of the same level):

1. ``repo.lock``      - *shared* for ``repro``/``push``/``pull``, *exclusive* for ``gc``.
2. ``stage-<name>.lock`` - exclusive while one process decides about and runs a stage.
3. ``lockfile.lock``  - exclusive for the read-modify-write of ``dagcache.lock``.

``flock`` locks belong to an open file description, so two threads of one
process that open the file separately exclude each other just like two
processes do. Locks are released automatically if the holder dies.
"""

from __future__ import annotations

import errno
import fcntl
import logging
import os
import time
from pathlib import Path
from types import TracebackType

from .errors import LockTimeoutError

log = logging.getLogger(__name__)

_WAIT_LOG_AFTER = 0.5


class FileLock:
    """A shared or exclusive ``flock`` on ``path`` with optional timeout."""

    def __init__(
        self,
        path: Path,
        *,
        shared: bool = False,
        timeout: float | None = None,
        poll_interval: float = 0.05,
    ) -> None:
        self.path = path
        self.shared = shared
        self.timeout = timeout
        self.poll_interval = poll_interval
        self._fd: int | None = None
        self.waited = 0.0

    @property
    def locked(self) -> bool:
        return self._fd is not None

    def acquire(self) -> FileLock:
        if self._fd is not None:
            raise RuntimeError(f"lock {self.path} already held by this object")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        mode = (fcntl.LOCK_SH if self.shared else fcntl.LOCK_EX) | fcntl.LOCK_NB
        start = time.monotonic()
        announced = False
        try:
            while True:
                try:
                    fcntl.flock(fd, mode)
                    break
                except OSError as exc:
                    if exc.errno not in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
                        raise
                elapsed = time.monotonic() - start
                if self.timeout is not None and elapsed >= self.timeout:
                    raise LockTimeoutError(str(self.path), self.timeout)
                if not announced and elapsed >= _WAIT_LOG_AFTER:
                    log.info(
                        "waiting for %s lock %s",
                        "shared" if self.shared else "exclusive",
                        self.path.name,
                    )
                    announced = True
                time.sleep(self.poll_interval)
        except BaseException:
            os.close(fd)
            raise
        self.waited = time.monotonic() - start
        if not self.shared:
            os.ftruncate(fd, 0)
            os.write(fd, f"{os.getpid()}\n".encode())
        self._fd = fd
        return self

    def release(self) -> None:
        if self._fd is None:
            return
        fd, self._fd = self._fd, None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def __enter__(self) -> FileLock:
        return self.acquire()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()
