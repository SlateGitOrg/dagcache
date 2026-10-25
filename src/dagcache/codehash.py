"""Fingerprinting of code: Python functions, Python source files, other files.

The goal is "re-run when behaviour could have changed, not when formatting did".

* Python source is parsed and the AST is dumped without position information, so
  comments, blank lines, indentation style and line wrapping do not affect the
  digest. Docstrings *do* count: they are runtime-visible values.
* For a decorated Python stage, the digest covers the stage function plus,
  transitively, every global it references:

  - functions/classes defined in workspace-local modules are recursed into;
  - workspace-local modules referenced by name are hashed whole (normalised);
  - simple constants (numbers, strings, and lists/tuples/dicts of them) contribute
    their value;
  - default-argument values (``def f(x=SCALE)``, keyword-only defaults included)
    are classified like references: they are evaluated at definition time, so
    neither the source text nor ``co_names`` changes when ``SCALE`` does;
  - third-party modules/functions contribute ``<distribution>==<version>``;
  - standard-library references contribute the Python ``major.minor`` version.

Each contribution is a separate named entry so ``status`` can say *which* helper
or constant changed. Anything else (arbitrary objects) is ignored and logged at
debug level; the escape hatch is the stage ``salt``.
"""

from __future__ import annotations

import ast
import functools
import hashlib
import importlib.metadata
import inspect
import logging
import sys
import sysconfig
import textwrap
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .canonical import digest_bytes

log = logging.getLogger(__name__)

_SIMPLE = (int, float, complex, str, bytes, bool, type(None))
_MAX_CONST_REPR = 10_000


def normalize_python_source(source: str, *, strip_decorators: bool = False) -> str:
    """AST dump of ``source`` without line/column attributes.

    Raises ``SyntaxError`` when the source does not parse.
    """
    tree = ast.parse(textwrap.dedent(source))
    if strip_decorators:
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                node.decorator_list = []
                break
    return ast.dump(tree, annotate_fields=True, include_attributes=False)


def file_code_digest(path: Path) -> str:
    """Digest of a code file: normalised AST for ``.py`` files, raw bytes otherwise."""
    data = path.read_bytes()
    if path.suffix == ".py":
        try:
            text = data.decode("utf-8")
            return digest_bytes(("pyast:" + normalize_python_source(text)).encode())
        except (SyntaxError, UnicodeDecodeError, ValueError):
            log.debug("%s does not parse as Python; hashing raw bytes", path)
    return hashlib.sha256(data).hexdigest()


@functools.lru_cache(maxsize=1)
def _packages_distributions() -> dict[str, list[str]]:
    return dict(importlib.metadata.packages_distributions())


@functools.lru_cache(maxsize=256)
def library_version(top_level: str) -> str:
    """``dist==version`` for an importable top-level package, best effort."""
    if top_level in sys.stdlib_module_names or top_level == "builtins":
        return f"python=={sys.version_info.major}.{sys.version_info.minor}"
    dists = _packages_distributions().get(top_level, [])
    for dist in sorted(dists):
        try:
            return f"{dist}=={importlib.metadata.version(dist)}"
        except importlib.metadata.PackageNotFoundError:
            continue
    module = sys.modules.get(top_level)
    version = getattr(module, "__version__", None)
    return f"{top_level}=={version}" if isinstance(version, str) else f"{top_level}==unknown"


def _site_dirs() -> list[Path]:
    dirs = {sysconfig.get_paths()[k] for k in ("purelib", "platlib", "stdlib", "platstdlib")}
    return [Path(d).resolve() for d in dirs if d]


class CodeFingerprinter:
    """Computes named code-digest entries for Python callables.

    ``root`` is the workspace root; only modules whose source file lives under it
    (and not inside a virtualenv/site-packages) are considered *local* and
    recursed into.
    """

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self._site = _site_dirs()

    # -- classification -------------------------------------------------
    def _is_local_file(self, filename: str | None) -> bool:
        if not filename:
            return False
        try:
            path = Path(filename).resolve()
        except OSError:
            return False
        if not path.is_relative_to(self.root):
            return False
        if any(path.is_relative_to(s) for s in self._site):
            return False
        return "site-packages" not in path.parts and ".venv" not in path.parts

    def _module_is_local(self, module: types.ModuleType | None) -> bool:
        return module is not None and self._is_local_file(getattr(module, "__file__", None))

    # -- public ---------------------------------------------------------
    def fingerprint(self, fn: Callable[..., Any]) -> dict[str, str]:
        """Return ``{entry name: digest or version}`` for ``fn`` and its closure."""
        entries: dict[str, str] = {}
        self._visit(inspect.unwrap(fn), entries, set(), top=True)
        return entries

    # -- traversal ------------------------------------------------------
    def _visit(
        self, obj: Any, entries: dict[str, str], seen: set[int], *, top: bool = False
    ) -> None:
        if id(obj) in seen:
            return
        seen.add(id(obj))
        module = inspect.getmodule(obj)
        modname = module.__name__ if module else "?"
        label = f"{modname}.{getattr(obj, '__qualname__', getattr(obj, '__name__', '?'))}"
        try:
            source = inspect.getsource(obj)
            normalized = normalize_python_source(source, strip_decorators=top)
            entries[f"py:{label}"] = digest_bytes(normalized.encode())
        except (OSError, TypeError, SyntaxError):
            code = getattr(obj, "__code__", None)
            if code is None:
                log.debug("no source for %s; ignored", label)
                return
            log.warning("no source for %s; falling back to bytecode digest", label)
            payload = repr((code.co_code, code.co_consts, code.co_names)).encode()
            entries[f"py:{label}"] = digest_bytes(payload)
        if isinstance(obj, types.FunctionType):
            self._classify_function_refs(obj, modname, entries, seen)
        elif inspect.isclass(obj):
            for attr in vars(obj).values():
                func = attr.__func__ if isinstance(attr, classmethod | staticmethod) else attr
                if isinstance(func, types.FunctionType):
                    self._classify_function_refs(func, modname, entries, seen)

    def _classify_function_refs(
        self, fn: types.FunctionType, modname: str, entries: dict[str, str], seen: set[int]
    ) -> None:
        for name, value in self._referenced_values(fn):
            self._classify(name, value, modname, entries, seen)
        for param, value in _default_values(fn):
            self._classify(
                f"{fn.__qualname__}.{param}", value, modname, entries, seen, const_kind="default"
            )

    @staticmethod
    def _referenced_values(fn: types.FunctionType) -> list[tuple[str, Any]]:
        out: list[tuple[str, Any]] = []
        names = set(_global_names(fn.__code__))
        for name in sorted(names):
            if name in fn.__globals__:
                out.append((name, fn.__globals__[name]))
        if fn.__closure__:
            for name, cell in zip(fn.__code__.co_freevars, fn.__closure__, strict=True):
                try:
                    out.append((name, cell.cell_contents))
                except ValueError:  # empty cell
                    continue
        return out

    def _classify(
        self,
        name: str,
        value: Any,
        modname: str,
        entries: dict[str, str],
        seen: set[int],
        *,
        const_kind: str = "const",
    ) -> None:
        if isinstance(value, types.ModuleType):
            if self._module_is_local(value):
                key = f"module:{value.__name__}"
                if key not in entries and value.__file__:
                    entries[key] = file_code_digest(Path(value.__file__))
            else:
                top = value.__name__.split(".")[0]
                entries[f"lib:{top}"] = library_version(top)
            return
        if isinstance(value, types.FunctionType | type) or inspect.isbuiltin(value):
            module = inspect.getmodule(value)
            if module is not None and self._module_is_local(module):
                self._visit(value, entries, seen)
            elif module is not None:
                top = module.__name__.split(".")[0]
                entries[f"lib:{top}"] = library_version(top)
            return
        if _is_simple_constant(value):
            text = repr(value)
            if len(text) <= _MAX_CONST_REPR:
                entries[f"{const_kind}:{modname}.{name}"] = digest_bytes(text.encode())
                return
        log.debug("reference %s (%s) not fingerprinted", name, type(value).__name__)


def _default_values(fn: types.FunctionType) -> list[tuple[str, Any]]:
    """``(parameter name, default value)`` for positional and keyword-only defaults."""
    code = fn.__code__
    out: list[tuple[str, Any]] = []
    if fn.__defaults__:
        positional = code.co_varnames[: code.co_argcount]
        out.extend(zip(positional[-len(fn.__defaults__) :], fn.__defaults__, strict=True))
    if fn.__kwdefaults__:
        out.extend(sorted(fn.__kwdefaults__.items()))
    return out


def _global_names(code: types.CodeType) -> list[str]:
    """Global names loaded by ``code`` and nested code objects (comprehensions, inner defs)."""
    names = list(code.co_names)
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            names.extend(_global_names(const))
    return names


def _is_simple_constant(value: Any) -> bool:
    if isinstance(value, _SIMPLE):
        return True
    if isinstance(value, tuple | frozenset | list):
        return all(_is_simple_constant(v) for v in value)
    if isinstance(value, dict):
        return all(_is_simple_constant(k) and _is_simple_constant(v) for k, v in value.items())
    return False
