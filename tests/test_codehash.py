"""Code fingerprinting of Python functions and files."""

from __future__ import annotations

import importlib.util
import sys
import textwrap
import types
from pathlib import Path

import numpy as np
import pytest

from dagcache.codehash import (
    CodeFingerprinter,
    file_code_digest,
    library_version,
    normalize_python_source,
)

_counter = 0


def load(root: Path, source: str, name: str | None = None) -> types.ModuleType:
    """Import ``source`` as a fresh module file under ``root``."""
    global _counter
    _counter += 1
    modname = name or f"m{_counter}"
    path = root / f"{modname}.py"
    path.write_text(textwrap.dedent(source))
    spec = importlib.util.spec_from_file_location(modname, path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[modname] = mod
    spec.loader.exec_module(mod)
    return mod


def fp(root: Path, mod: types.ModuleType, fn: str = "stage") -> dict[str, str]:
    entries = CodeFingerprinter(root).fingerprint(getattr(mod, fn))
    # Module names differ between loads; compare on the qualified tail only.
    return {k.replace(mod.__name__ + ".", ""): v for k, v in entries.items()}


BASE = """
import json
import numpy as np
LIMIT = 3
OPTIONS = {"a": [1, 2]}

def helper(x):
    return x * LIMIT

class Model:
    def fit(self, x):
        return helper(x)

def stage(ctx):
    '''Docstring.'''
    return [helper(1), Model().fit(2), json.dumps(OPTIONS), np.zeros(1)]
"""


def test_formatting_and_comments_do_not_matter(tmp_path: Path) -> None:
    a = fp(tmp_path, load(tmp_path, BASE))
    reformatted = BASE.replace("return x * LIMIT", "return (x *   LIMIT)  # same").replace(
        "def stage(ctx):", "# comment\ndef stage(ctx):\n    # inner comment"
    )
    assert fp(tmp_path, load(tmp_path, reformatted)) == a


def test_entries_cover_closure(tmp_path: Path) -> None:
    entries = fp(tmp_path, load(tmp_path, BASE))
    assert {"py:stage", "py:helper", "py:Model", "const:LIMIT", "const:OPTIONS"} <= set(entries)
    assert entries["lib:numpy"] == f"numpy=={np.__version__}"
    assert entries["lib:json"].startswith("python==")


@pytest.mark.parametrize(
    ("old", "new", "changed"),
    [
        ("return x * LIMIT", "return x * LIMIT + 1", "py:helper"),
        ("LIMIT = 3", "LIMIT = 4", "const:LIMIT"),
        ('OPTIONS = {"a": [1, 2]}', 'OPTIONS = {"a": [1, 3]}', "const:OPTIONS"),
        ("return helper(x)", "return helper(x) * 2", "py:Model"),
        ("'''Docstring.'''", "'''Other docstring.'''", "py:stage"),
    ],
)
def test_semantic_changes_are_detected(tmp_path: Path, old: str, new: str, changed: str) -> None:
    a = fp(tmp_path, load(tmp_path, BASE))
    b = fp(tmp_path, load(tmp_path, BASE.replace(old, new)))
    diff = {k for k in set(a) | set(b) if a.get(k) != b.get(k)}
    assert diff == {changed}


def test_decorator_arguments_are_not_code(tmp_path: Path) -> None:
    src = """
    def deco(**kw):
        return lambda f: f

    @deco(outs=["{out}"])
    def stage(ctx):
        return 1
    """
    a = fp(tmp_path, load(tmp_path, src.replace("{out}", "a.txt")))
    b = fp(tmp_path, load(tmp_path, src.replace("{out}", "b.txt")))
    assert a["py:stage"] == b["py:stage"]


def test_closure_cells_and_local_module_reference(tmp_path: Path) -> None:
    helpers = load(tmp_path, "def f():\n    return 1\n", name="closurehelpers")
    mod = load(
        tmp_path,
        """
        import closurehelpers

        def make(scale):
            def stage(ctx):
                return closurehelpers.f() * scale
            return stage
        stage = make(5)
        """,
    )
    entries = CodeFingerprinter(tmp_path).fingerprint(mod.stage)
    assert "module:closurehelpers" in entries
    assert any(k.startswith("const:") and k.endswith(".scale") for k in entries)
    assert helpers.f() == 1


def test_non_local_modules_and_unhashable_refs(tmp_path: Path) -> None:
    mod = load(
        tmp_path,
        """
        import os
        from numpy import zeros
        STATE = object()

        def stage(ctx):
            return os.getcwd(), zeros(1), STATE, len([])
        """,
    )
    entries = fp(tmp_path, mod)
    assert entries["lib:os"].startswith("python==")
    assert entries["lib:numpy"].startswith("numpy==")
    assert not any("STATE" in k for k in entries)  # arbitrary objects are ignored


def test_function_without_source_falls_back_to_bytecode(tmp_path: Path) -> None:
    ns: dict[str, object] = {}
    exec("def stage(ctx):\n    return 42\n", ns)
    entries = CodeFingerprinter(tmp_path).fingerprint(ns["stage"])  # type: ignore[arg-type]
    assert len(entries) == 1 and next(iter(entries)).startswith("py:")


def test_library_version_fallbacks() -> None:
    assert library_version("sys").startswith("python==")
    assert library_version("numpy") == f"numpy=={np.__version__}"
    assert library_version("definitely_not_a_module").endswith("==unknown")


def test_file_code_digest(tmp_path: Path) -> None:
    py = tmp_path / "s.py"
    py.write_text("x = 1\n")
    d1 = file_code_digest(py)
    py.write_text("x  =  1  # c\n\n")
    assert file_code_digest(py) == d1
    py.write_text("x = (\n")  # does not parse -> raw bytes
    raw = file_code_digest(py)
    py.write_text("x = ( \n")
    assert file_code_digest(py) != raw
    txt = tmp_path / "s.sql"
    txt.write_text("select 1 -- c")
    assert len(file_code_digest(txt)) == 64


def test_normalize_python_source_strips_positions() -> None:
    assert normalize_python_source("a=1") == normalize_python_source("a = 1  # c")
    with pytest.raises(SyntaxError):
        normalize_python_source("def (")
