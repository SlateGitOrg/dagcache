"""Loading pipelines and configuration from ``dagcache.yaml`` or a Python file."""

from __future__ import annotations

import importlib.util
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

import yaml

from .errors import ConfigError
from .params import DEFAULT_PARAMS_FILE
from .paths import normalize
from .pipeline import Pipeline
from .stage import Stage

LinkMode = Literal["copy", "hardlink"]

_STAGE_KEYS = {
    "cmd",
    "deps",
    "outs",
    "params",
    "code",
    "env",
    "wdir",
    "always_run",
    "salt",
    "desc",
}
_TOP_KEYS = {"params", "cache", "jobs", "python", "stages", "name"}
_CACHE_KEYS = {"dir", "link", "remote", "autopush", "s3"}


@dataclass
class CacheConfig:
    """Where and how artifacts are cached."""

    dir: str = ".dagcache/cache"
    link: LinkMode = "copy"
    remote: str | None = None
    autopush: bool = False
    s3_endpoint_url: str | None = None
    s3_region: str | None = None


@dataclass
class Config:
    """Workspace-level settings (the non-stage part of ``dagcache.yaml``)."""

    params_file: str = DEFAULT_PARAMS_FILE
    cache: CacheConfig = field(default_factory=CacheConfig)
    jobs: int | None = None
    python: str | None = None

    def apply_env(self, environ: dict[str, str] | None = None) -> Config:
        """Override settings from ``DAGCACHE_*`` environment variables."""
        env = os.environ if environ is None else environ
        if env.get("DAGCACHE_REMOTE"):
            self.cache.remote = env["DAGCACHE_REMOTE"]
        if env.get("DAGCACHE_S3_ENDPOINT_URL"):
            self.cache.s3_endpoint_url = env["DAGCACHE_S3_ENDPOINT_URL"]
        if env.get("DAGCACHE_S3_REGION"):
            self.cache.s3_region = env["DAGCACHE_S3_REGION"]
        if env.get("DAGCACHE_JOBS"):
            self.jobs = _positive_int(env["DAGCACHE_JOBS"], "DAGCACHE_JOBS")
        return self


def _positive_int(value: Any, what: str) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        raise ConfigError(f"{what} must be a positive integer, got {value!r}") from None
    if n < 1:
        raise ConfigError(f"{what} must be a positive integer, got {value!r}")
    return n


def _str_list(value: Any, what: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list) and all(isinstance(v, str) for v in value):
        return tuple(value)
    raise ConfigError(f"{what} must be a string or a list of strings")


def parse_config(raw: dict[str, Any]) -> Config:
    unknown = set(raw) - _TOP_KEYS
    if unknown:
        raise ConfigError(f"unknown top-level keys in dagcache.yaml: {sorted(unknown)}")
    cfg = Config()
    if "params" in raw:
        if not isinstance(raw["params"], str):
            raise ConfigError("'params' must be the path of the default params file")
        cfg.params_file = normalize(raw["params"], what="params file")
    if raw.get("jobs") is not None:
        cfg.jobs = _positive_int(raw["jobs"], "jobs")
    if raw.get("python") is not None:
        if not isinstance(raw["python"], str):
            raise ConfigError("'python' must be 'file.py' or 'file.py:attribute'")
        cfg.python = raw["python"]
    cache = raw.get("cache") or {}
    if not isinstance(cache, dict):
        raise ConfigError("'cache' must be a mapping")
    unknown = set(cache) - _CACHE_KEYS
    if unknown:
        raise ConfigError(f"unknown keys in 'cache': {sorted(unknown)}")
    if "dir" in cache:
        cfg.cache.dir = str(cache["dir"])
    if "link" in cache:
        if cache["link"] not in ("copy", "hardlink"):
            raise ConfigError("cache.link must be 'copy' or 'hardlink'")
        cfg.cache.link = cast(LinkMode, cache["link"])
    if cache.get("remote") is not None:
        cfg.cache.remote = str(cache["remote"])
    if "autopush" in cache:
        if not isinstance(cache["autopush"], bool):
            raise ConfigError("cache.autopush must be true or false")
        cfg.cache.autopush = cache["autopush"]
    s3 = cache.get("s3") or {}
    if not isinstance(s3, dict):
        raise ConfigError("cache.s3 must be a mapping")
    cfg.cache.s3_endpoint_url = s3.get("endpoint_url")
    cfg.cache.s3_region = s3.get("region")
    return cfg


def parse_stage(name: str, raw: Any) -> Stage:
    if not isinstance(raw, dict):
        raise ConfigError(f"stage '{name}' must be a mapping")
    unknown = set(raw) - _STAGE_KEYS
    if unknown:
        raise ConfigError(f"stage '{name}': unknown keys {sorted(unknown)}")
    if "cmd" not in raw:
        raise ConfigError(f"stage '{name}': 'cmd' is required in YAML stages")
    cmd = raw["cmd"]
    if isinstance(cmd, list):
        if not all(isinstance(c, str) for c in cmd):
            raise ConfigError(f"stage '{name}': cmd list must contain strings")
        cmd = " && ".join(cmd)
    if not isinstance(cmd, str):
        raise ConfigError(f"stage '{name}': cmd must be a string or list of strings")
    always_run = raw.get("always_run", False)
    if not isinstance(always_run, bool):
        raise ConfigError(f"stage '{name}': always_run must be true or false")
    return Stage(
        name=name,
        cmd=cmd,
        deps=_str_list(raw.get("deps"), f"stage '{name}' deps"),
        outs=_str_list(raw.get("outs"), f"stage '{name}' outs"),
        params=_str_list(raw.get("params"), f"stage '{name}' params"),
        code=_str_list(raw.get("code"), f"stage '{name}' code"),
        env=_str_list(raw.get("env"), f"stage '{name}' env"),
        wdir=str(raw.get("wdir", ".")),
        always_run=always_run,
        salt=str(raw.get("salt", "")),
        description=str(raw.get("desc", "")),
    )


def read_yaml(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except FileNotFoundError as exc:
        raise ConfigError(f"{path.name} not found") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path.name} is not valid YAML: {exc}") from exc
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"{path.name} must contain a mapping at the top level")
    return raw


def load_yaml_pipeline(raw: dict[str, Any], config: Config) -> Pipeline:
    stages = raw.get("stages") or {}
    if not isinstance(stages, dict):
        raise ConfigError("'stages' must be a mapping of stage name to definition")
    pipe = Pipeline(str(raw.get("name", "pipeline")), params_file=config.params_file)
    for name, body in stages.items():
        pipe.add(parse_stage(str(name), body))
    pipe.validate()
    return pipe


def load_python_pipeline(path: Path, attr: str = "pipeline") -> Pipeline:
    """Import ``path`` and return its :class:`Pipeline` attribute ``attr``.

    The module is registered in ``sys.modules`` under its file stem so that code
    fingerprints (which include module names) are stable across invocations.
    The file's directory is put on ``sys.path`` so local helper modules import.
    """
    if not path.is_file():
        raise ConfigError(f"pipeline file {path} does not exist")
    modname = path.stem
    spec = importlib.util.spec_from_file_location(modname, path)
    if spec is None or spec.loader is None:
        raise ConfigError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    folder = str(path.parent.resolve())
    added = folder not in sys.path
    if added:
        sys.path.insert(0, folder)
    previous = sys.modules.get(modname)
    sys.modules[modname] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        if previous is not None:
            sys.modules[modname] = previous
        else:
            sys.modules.pop(modname, None)
        raise ConfigError(f"error importing {path.name}: {exc}") from exc
    pipe = getattr(module, attr, None)
    if not isinstance(pipe, Pipeline):
        raise ConfigError(f"{path.name} has no Pipeline named '{attr}'")
    pipe.validate()
    return pipe


def split_python_ref(ref: str) -> tuple[str, str]:
    """``"file.py:attr"`` -> ``("file.py", "attr")``; attr defaults to ``pipeline``."""
    if ":" in ref and not ref.endswith(".py"):
        file, attr = ref.rsplit(":", 1)
        return file, attr or "pipeline"
    return ref, "pipeline"
