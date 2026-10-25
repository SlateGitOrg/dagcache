"""Stage model: one node of the pipeline DAG."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from .errors import ConfigError
from .params import ParamRef, interpolation_refs, parse_ref
from .paths import normalize

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


@dataclass(frozen=True)
class StageContext:
    """What a Python stage function receives when it runs."""

    name: str
    root: Path
    params: dict[str, Any]
    deps: tuple[str, ...]
    outs: tuple[str, ...]

    def path(self, rel: str) -> Path:
        """Absolute path of a workspace-relative path."""
        return self.root / normalize(rel)

    @property
    def log(self) -> logging.Logger:
        return logging.getLogger(f"dagcache.stage.{self.name}")


StageFunc = Callable[[StageContext], None]


def _paths(values: Iterable[str], what: str) -> tuple[str, ...]:
    out: list[str] = []
    for v in values:
        if not isinstance(v, str):
            raise ConfigError(f"{what} entries must be strings, got {v!r}")
        p = normalize(v, what=what)
        if p not in out:
            out.append(p)
    return tuple(out)


@dataclass(frozen=True)
class Stage:
    """A unit of work with declared inputs and outputs.

    Exactly one of ``cmd`` (a shell command) or ``func`` (a Python callable
    taking a :class:`StageContext`) must be given.
    """

    name: str
    deps: tuple[str, ...] = ()
    outs: tuple[str, ...] = ()
    params: tuple[str, ...] = ()
    code: tuple[str, ...] = ()
    env: tuple[str, ...] = ()
    cmd: str | None = None
    func: StageFunc | None = field(default=None, compare=False)
    wdir: str = "."
    always_run: bool = False
    salt: str = ""
    description: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not _NAME.match(self.name):
            raise ConfigError(
                f"invalid stage name {self.name!r}: use letters, digits, '_', '-', '.'"
            )
        if (self.cmd is None) == (self.func is None):
            raise ConfigError(f"stage '{self.name}': give exactly one of cmd or func")
        if self.cmd is not None and (not isinstance(self.cmd, str) or not self.cmd.strip()):
            raise ConfigError(f"stage '{self.name}': cmd must be a non-empty string")
        if self.func is not None and not callable(self.func):
            raise ConfigError(f"stage '{self.name}': func must be callable")
        object.__setattr__(self, "deps", _paths(self.deps, f"stage '{self.name}' dep"))
        object.__setattr__(self, "outs", _paths(self.outs, f"stage '{self.name}' out"))
        object.__setattr__(self, "code", _paths(self.code, f"stage '{self.name}' code"))
        object.__setattr__(self, "wdir", normalize(self.wdir, what="wdir"))
        if "." in self.outs:
            raise ConfigError(f"stage '{self.name}': the workspace root cannot be an output")
        for env_name in self.env:
            if not isinstance(env_name, str) or not env_name:
                raise ConfigError(f"stage '{self.name}': env entries must be variable names")
        object.__setattr__(self, "env", tuple(dict.fromkeys(self.env)))
        object.__setattr__(self, "params", tuple(dict.fromkeys(self.params)))
        for spec in self.params:
            parse_ref(spec)  # validate syntax early

    @property
    def kind(self) -> Literal["cmd", "python"]:
        return "cmd" if self.cmd is not None else "python"

    def param_refs(self, default_file: str) -> list[ParamRef]:
        """Declared parameters plus any ``${...}`` used in ``cmd``, de-duplicated."""
        refs = {parse_ref(p, default_file) for p in self.params}
        if self.cmd is not None:
            refs.update(interpolation_refs(self.cmd, default_file))
        return sorted(refs)
