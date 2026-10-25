"""Workspace: the root directory, its pipeline, config, cache and state files.

Layout of the state directory (add ``.dagcache/`` to ``.gitignore``)::

    .dagcache/
      cache/          local content-addressed store (objects/, actions/, tmp/)
      locks/          flock files (repo, per-stage, lock file mutex)
      logs/           captured output of the last run of each command stage
      state.db        stat cache (inode/size/mtime -> digest)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from .backends import Backend, FsBackend, open_backend
from .cache import Cache
from .errors import ConfigError
from .hashing import Hasher, StatCache
from .keys import KeyBuilder
from .lockfile import LockFile
from .params import ParamStore
from .pipeline import Pipeline
from .spec import (
    Config,
    load_python_pipeline,
    load_yaml_pipeline,
    parse_config,
    read_yaml,
    split_python_ref,
)

log = logging.getLogger(__name__)

PIPELINE_FILE = "dagcache.yaml"
LOCK_FILE = "dagcache.lock"
STATE_DIR = ".dagcache"


@dataclass
class Workspace:
    """Everything needed to plan, run and inspect one pipeline."""

    root: Path
    pipeline: Pipeline
    config: Config = field(default_factory=Config)
    lock_timeout: float | None = None

    def __post_init__(self) -> None:
        self.root = self.root.resolve()
        self.state_dir = self.root / STATE_DIR
        self.locks_dir = self.state_dir / "locks"
        self.logs_dir = self.state_dir / "logs"
        for d in (self.state_dir, self.locks_dir, self.logs_dir):
            d.mkdir(parents=True, exist_ok=True)
        gitignore = self.state_dir / ".gitignore"
        if not gitignore.exists():
            gitignore.write_text("*\n", encoding="utf-8")
        self.stat_cache = StatCache(self.state_dir / "state.db")
        self.hasher = Hasher(self.root, self.stat_cache)
        self.params = ParamStore(self.root, self.pipeline.params_file)
        self.keys = KeyBuilder(self.root, self.hasher, self.params)
        self.lockfile = LockFile(
            self.root / LOCK_FILE, self.locks_dir / "lockfile.lock", timeout=self.lock_timeout
        )
        cache_dir = Path(self.config.cache.dir).expanduser()
        self.local = FsBackend(cache_dir if cache_dir.is_absolute() else self.root / cache_dir)
        self._remote: Backend | None = None
        self._remote_opened = False

    # -- construction ----------------------------------------------------
    @classmethod
    def load(
        cls,
        path: Path | None = None,
        *,
        start: Path | None = None,
        lock_timeout: float | None = None,
    ) -> Workspace:
        """Load from a ``dagcache.yaml``/``.py`` file, or discover upward from ``start``."""
        if path is None:
            path = find_pipeline_file(start or Path.cwd())
        ref_file, attr = split_python_ref(str(path))
        file = Path(ref_file).resolve()
        root = file.parent
        if file.suffix == ".py":
            config = Config()
            sidecar = root / PIPELINE_FILE
            if sidecar.is_file():
                raw = read_yaml(sidecar)
                raw.pop("stages", None)
                config = parse_config(raw)
            pipeline = load_python_pipeline(file, attr)
            config.params_file = pipeline.params_file
        else:
            raw = read_yaml(file)
            config = parse_config(raw)
            if config.python:
                py_file, py_attr = split_python_ref(config.python)
                if raw.get("stages"):
                    raise ConfigError("dagcache.yaml cannot define both 'stages' and 'python'")
                pipeline = load_python_pipeline(root / py_file, py_attr)
            else:
                pipeline = load_yaml_pipeline(raw, config)
        config.apply_env()
        return cls(root, pipeline, config, lock_timeout=lock_timeout)

    # -- cache -----------------------------------------------------------
    @property
    def remote(self) -> Backend | None:
        if not self._remote_opened:
            self._remote_opened = True
            url = self.config.cache.remote
            if url:
                self._remote = open_backend(
                    url,
                    root=self.root,
                    s3_endpoint_url=self.config.cache.s3_endpoint_url,
                    s3_region=self.config.cache.s3_region,
                )
        return self._remote

    def cache(self, *, use_remote: bool = True) -> Cache:
        return Cache(
            self.local,
            self.remote if use_remote else None,
            link_mode=self.config.cache.link,
            autopush=self.config.cache.autopush,
        )

    def stage_lock_path(self, name: str) -> Path:
        return self.locks_dir / f"stage-{name}.lock"

    @property
    def repo_lock_path(self) -> Path:
        return self.locks_dir / "repo.lock"

    def close(self) -> None:
        self.stat_cache.close()


def find_pipeline_file(start: Path) -> Path:
    """Walk up from ``start`` looking for ``dagcache.yaml``."""
    current = start.resolve()
    for folder in (current, *current.parents):
        candidate = folder / PIPELINE_FILE
        if candidate.is_file():
            return candidate
    raise ConfigError(f"no {PIPELINE_FILE} found in {start} or any parent directory")
