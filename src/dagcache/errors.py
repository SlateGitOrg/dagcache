"""Exception hierarchy for dagcache.

Every error raised on purpose by the library derives from :class:`DagcacheError`,
so callers (and the CLI) can catch one type and print a clean message.
"""

from __future__ import annotations


class DagcacheError(Exception):
    """Base class for all dagcache errors."""


class ConfigError(DagcacheError):
    """The pipeline definition or configuration is invalid."""


class PipelineError(ConfigError):
    """The stage graph is malformed (cycle, overlapping outputs, unknown stage)."""


class ParamError(ConfigError):
    """A declared parameter is missing or a params file cannot be read."""


class MissingDependencyError(DagcacheError):
    """A stage dependency does not exist in the workspace and nothing produces it."""

    def __init__(self, stage: str, path: str) -> None:
        super().__init__(f"stage '{stage}': dependency '{path}' does not exist")
        self.stage = stage
        self.path = path


class StageFailedError(DagcacheError):
    """A stage command or function failed, or did not produce its outputs."""

    def __init__(self, stage: str, message: str) -> None:
        super().__init__(f"stage '{stage}' failed: {message}")
        self.stage = stage


class CacheError(DagcacheError):
    """Generic cache backend failure."""


class ObjectNotFoundError(CacheError):
    """A content-addressed object is not present in a backend."""

    def __init__(self, digest: str, where: str) -> None:
        super().__init__(f"object {digest[:12]} not found in {where}")
        self.digest = digest


class CorruptObjectError(CacheError):
    """An object's content does not hash to its address."""

    def __init__(self, digest: str, actual: str, where: str) -> None:
        super().__init__(
            f"corrupt object in {where}: expected {digest[:12]}, content hashes to {actual[:12]}"
        )
        self.digest = digest
        self.actual = actual


class LockTimeoutError(DagcacheError):
    """A file lock could not be acquired within the timeout."""

    def __init__(self, path: str, timeout: float) -> None:
        super().__init__(f"timed out after {timeout:.1f}s waiting for lock {path}")
        self.path = path
        self.timeout = timeout
