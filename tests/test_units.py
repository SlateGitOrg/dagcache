"""Unit tests for the small building blocks: canonical JSON, paths, params, manifests."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from dagcache.canonical import canonical_json, digest_json, is_digest, short
from dagcache.errors import CacheError, ConfigError, ParamError
from dagcache.keys import explain
from dagcache.lockfile import LockFile, read_lock_file
from dagcache.manifest import ActionManifest, KeyComponents, OutputRecord
from dagcache.params import ParamRef, ParamStore, interpolate, interpolation_refs, lookup, parse_ref
from dagcache.paths import is_within, normalize, overlaps, relative_to

json_values = st.recursive(
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(),
    lambda children: (
        st.lists(children, max_size=4) | st.dictionaries(st.text(max_size=5), children, max_size=4)
    ),
    max_leaves=12,
)


# -- canonical -----------------------------------------------------------------
@given(st.dictionaries(st.text(max_size=6), json_values, max_size=6))
def test_canonical_json_is_order_independent(d: dict[str, object]) -> None:
    reversed_d = dict(reversed(list(d.items())))
    assert canonical_json(d) == canonical_json(reversed_d)
    assert json.loads(canonical_json(d)) == json.loads(json.dumps(d))


def test_canonical_json_rejects_unstable_values() -> None:
    with pytest.raises(ValueError, match="non-finite"):
        canonical_json({"x": float("nan")})
    with pytest.raises(TypeError, match="non-string key"):
        canonical_json({1: "a"})
    with pytest.raises(TypeError, match="unsupported type"):
        canonical_json({"x": {1, 2}})
    assert canonical_json((1, "a")) == b'[1,"a"]'


def test_digest_helpers() -> None:
    d = digest_json({"a": 1})
    assert is_digest(d) and not is_digest(d.upper()) and not is_digest("xyz")
    assert not is_digest("g" * 64)
    assert short(d, 4) == d[:4] and short(None) == "<none>"


# -- paths --------------------------------------------------------------------
@pytest.mark.parametrize(
    ("raw", "norm"),
    [("a/b/../c", "a/c"), ("./x", "x"), ("a\\b", "a/b"), (".", "."), ("d/", "d")],
)
def test_normalize(raw: str, norm: str) -> None:
    assert normalize(raw) == norm


@pytest.mark.parametrize("bad", ["", "  ", "/abs", "../up", "a/../../b", "C:\\x"])
def test_normalize_rejects(bad: str) -> None:
    with pytest.raises(ConfigError):
        normalize(bad)


def test_path_relations() -> None:
    assert is_within("a/b", "a") and is_within("a", "a") and is_within("x", ".")
    assert not is_within("ab", "a")
    assert overlaps("a", "a/b/c") and not overlaps("a/b", "a/c")
    assert relative_to("a/b/c", "a") == "b/c"
    assert relative_to("a", "a") == "." and relative_to("x/y", ".") == "x/y"
    with pytest.raises(ValueError, match="not within"):
        relative_to("b", "a")


# -- params -------------------------------------------------------------------
def test_parse_ref_forms() -> None:
    assert parse_ref("train.lr") == ParamRef("params.yaml", "train.lr")
    assert parse_ref("conf/m.yaml:layers.0") == ParamRef("conf/m.yaml", "layers.0")
    assert parse_ref("a", "p2.yaml").display("p2.yaml") == "a"
    assert parse_ref("x.yaml:a").display("params.yaml") == "x.yaml:a"
    assert parse_ref("x.yaml:a").qualified == "x.yaml:a"
    for bad in ["", "a..b", ".a", "a."]:
        with pytest.raises(ParamError):
            parse_ref(bad)


def test_lookup_nested_and_lists() -> None:
    data = {"a": {"b": [10, {"c": 3}]}}
    assert lookup(data, "a.b.0") == 10
    assert lookup(data, "a.b.1.c") == 3
    assert lookup(data, "a.b.5") is not None  # sentinel, not a value
    assert lookup(data, "a.b.5") is lookup(data, "zzz")


def test_param_store(tmp_path: Path) -> None:
    (tmp_path / "params.yaml").write_text("a: 1\nday: 2026-01-02\nnested: {x: [1, 2]}\n")
    (tmp_path / "bad.yaml").write_text("a: [unclosed\n")
    (tmp_path / "empty.yaml").write_text("")
    store = ParamStore(tmp_path)
    assert store.get(parse_ref("a")) == 1
    assert store.get(parse_ref("day")) == "2026-01-02"
    assert store.values([parse_ref("nested"), parse_ref("a")]) == {"a": 1, "nested": {"x": [1, 2]}}
    with pytest.raises(ParamError, match="not found"):
        store.get(parse_ref("missing"))
    with pytest.raises(ParamError, match="does not exist"):
        store.get(parse_ref("nope.yaml:a"))
    with pytest.raises(ParamError, match="not valid YAML"):
        store.get(parse_ref("bad.yaml:a"))
    with pytest.raises(ParamError, match="not found"):
        store.get(parse_ref("empty.yaml:a"))
    (tmp_path / "params.yaml").write_text("a: 2\n")
    assert store.get(parse_ref("a")) == 1  # cached until invalidated
    store.invalidate()
    assert store.get(parse_ref("a")) == 2


def test_interpolation(tmp_path: Path) -> None:
    (tmp_path / "params.yaml").write_text("n: 3\nflag: true\nnone: null\nlst: [1]\ns: hi\n")
    store = ParamStore(tmp_path)
    cmd = "run --n ${n} --flag ${ flag } --none '${none}' --s ${s}"
    assert interpolate(cmd, store) == "run --n 3 --flag true --none '' --s hi"
    assert [r.key for r in interpolation_refs(cmd)] == ["n", "flag", "none", "s"]
    with pytest.raises(ParamError, match="non-scalar"):
        interpolate("x ${lst}", store)


def test_param_value_must_be_canonical(tmp_path: Path) -> None:
    (tmp_path / "params.yaml").write_text("x: .nan\n")
    with pytest.raises(ParamError, match="no stable value"):
        ParamStore(tmp_path).get(parse_ref("x"))
    assert dt.date(2026, 1, 1)  # date conversion covered above


# -- manifests & explain ---------------------------------------------------------
def comps(**over: object) -> KeyComponents:
    base: dict[str, object] = {
        "kind": "cmd",
        "cmd": "python train.py",
        "code": {"file:train.py": "a" * 64, "lib:numpy": "numpy==2.0"},
        "deps": {"data.csv": "b" * 64},
        "params": {"lr": 0.1},
        "outs": ("model.pkl",),
    }
    base.update(over)
    return KeyComponents(**base)  # type: ignore[arg-type]


def test_key_components_round_trip_and_key_stability() -> None:
    c = comps()
    again = KeyComponents.from_dict(json.loads(json.dumps(c.to_dict())))
    assert again == c and again.key == c.key
    assert comps(params={"lr": 0.2}).key != c.key
    with pytest.raises(CacheError):
        KeyComponents.from_dict({"cmd": "x"})


def test_explain_lists_every_difference() -> None:
    old = comps(env={"A": "1"}, salt="")
    new = comps(
        kind="python",
        cmd="python train.py --fast",
        code={"file:train.py": "c" * 64, "lib:numpy": "numpy==2.1", "py:m.f": "d" * 64},
        deps={"other.csv": "e" * 64},
        params={"lr": 0.2, "epochs": 3},
        outs=("model.pkl", "log.txt"),
        wdir="sub",
        env={"A": "2"},
        salt="v2",
    )
    reasons = explain(old, new)
    text = "\n".join(reasons)
    for expected in [
        "stage kind changed: cmd → python",
        "command changed",
        "code file train.py changed",
        "library numpy changed: numpy==2.0 → numpy==2.1",
        "function m.f added",
        "dependency data.csv removed",
        "dependency other.csv added",
        "param epochs added (= 3)",
        "param lr changed: 0.1 → 0.2",
        "outputs changed",
        "wdir changed: . → sub",
        "env A changed",
        "salt changed",
    ]:
        assert expected in text, expected
    assert explain(new, old)[2].startswith("code file train.py changed")
    assert "param epochs no longer used" in explain(new, old)
    assert "function m.f removed" in explain(new, old)
    assert explain(old, old) == []
    assert explain(comps(params={"x": True}), comps(params={"x": 1})) == [
        "param x changed: True → 1"
    ]


def test_action_manifest_serialisation() -> None:
    c = comps()
    m = ActionManifest(
        c.key, "train", c, (OutputRecord("model.pkl", "file", "f" * 64, 10),), {}, "t", 1.23456
    )
    back = ActionManifest.from_bytes(m.to_bytes())
    assert back == ActionManifest(c.key, "train", c, m.outputs, {}, "t", 1.2346)
    assert back.output("model.pkl") is not None and back.output("x") is None
    for bad in [b"not json", b"[]", b'{"schema": 99}', b'{"schema": 1}']:
        with pytest.raises(CacheError):
            ActionManifest.from_bytes(bad)
    with pytest.raises(CacheError, match="unknown output kind"):
        OutputRecord.from_dict({"path": "x", "kind": "link", "digest": "d", "size": 1})


def test_lock_file_round_trip_and_errors(tmp_path: Path) -> None:
    c = comps()
    m = ActionManifest(c.key, "train", c, (), {"data.csv": "k" * 64}, "t", 0.5)
    lf = LockFile(tmp_path / "dagcache.lock", tmp_path / "m.lock")
    assert lf.read() == {}
    lf.update({"train": m})
    assert lf.read()["train"] == m
    lf.update({}, remove=["train"])
    assert lf.read() == {}
    bad = tmp_path / "bad.lock"
    for text in [
        "schema: 7\n",
        "[: x",
        "schema: 1\nstages:\n  a: 3\n",
        "schema: 1\nstages:\n  a: {key: x}\n",
    ]:
        bad.write_text(text)
        with pytest.raises(ConfigError):
            read_lock_file(bad)
