"""Stage validation, DAG inference, YAML/Python loading and configuration."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import write
from hypothesis import given, settings
from hypothesis import strategies as st

from dagcache import ConfigError, Pipeline, PipelineError, Stage, Workspace
from dagcache.spec import Config, parse_config, parse_stage, split_python_ref


def cmd(name: str, deps: tuple[str, ...] = (), outs: tuple[str, ...] = ()) -> Stage:
    return Stage(name=name, cmd="true", deps=deps, outs=outs)


def test_stage_validation() -> None:
    for bad in ["", "-x", "a b", "x/y"]:
        with pytest.raises(ConfigError, match="invalid stage name"):
            cmd(bad)
    with pytest.raises(ConfigError, match="exactly one"):
        Stage(name="s")
    with pytest.raises(ConfigError, match="exactly one"):
        Stage(name="s", cmd="x", func=lambda ctx: None)
    with pytest.raises(ConfigError, match="non-empty"):
        Stage(name="s", cmd="  ")
    with pytest.raises(ConfigError, match="root cannot be an output"):
        cmd("s", outs=(".",))
    with pytest.raises(ConfigError, match="escapes"):
        cmd("s", deps=("../x",))
    with pytest.raises(ConfigError, match="must be strings"):
        Stage(name="s", cmd="x", deps=(1,))  # type: ignore[arg-type]
    with pytest.raises(ConfigError, match="env"):
        Stage(name="s", cmd="x", env=("",))
    s = Stage(name="s", cmd="echo ${b}", deps=("a", "./a"), params=("a", "a"))
    assert s.deps == ("a",) and s.params == ("a",) and s.kind == "cmd"
    assert [r.key for r in s.param_refs("params.yaml")] == ["a", "b"]


def test_edges_inferred_from_paths_including_nested() -> None:
    p = Pipeline()
    p.add(cmd("gen", outs=("data/raw", "logs/gen.txt")))
    p.add(cmd("one", deps=("data/raw/a.csv",), outs=("one.txt",)))
    p.add(cmd("all", deps=("data",), outs=("all.txt",)))
    p.add(cmd("final", deps=("one.txt", "all.txt"), outs=("final.txt",)))
    g = p.validate()
    assert g.upstream["one"] == {"gen"} and g.upstream["all"] == {"gen"}
    assert g.downstream["gen"] == {"one", "all"}
    assert g.order[0] == "gen" and g.order[-1] == "final"
    assert g.producers[("one", "data/raw/a.csv")] == (("gen", "data/raw"),)
    assert p.upstream_closure(["final"]) == {"gen", "one", "all", "final"}
    assert p.downstream_closure(["one"]) == {"one", "final"}
    assert "gen" in p and len(p) == 4 and [s.name for s in p] == p.names


def test_overlapping_outputs_rejected() -> None:
    p = Pipeline()
    p.add(cmd("a", outs=("data",)))
    p.add(cmd("b", outs=("data/x",)))
    with pytest.raises(PipelineError, match="overlaps"):
        p.validate()


def test_self_dependency_rejected() -> None:
    p = Pipeline()
    p.add(cmd("a", deps=("d",), outs=("d/x",)))
    with pytest.raises(PipelineError, match="its own output"):
        p.validate()


def test_cycle_reported_with_path() -> None:
    p = Pipeline()
    p.add(cmd("a", deps=("c.txt",), outs=("a.txt",)))
    p.add(cmd("b", deps=("a.txt",), outs=("b.txt",)))
    p.add(cmd("c", deps=("b.txt",), outs=("c.txt",)))
    p.add(cmd("d", deps=("c.txt",), outs=("d.txt",)))
    with pytest.raises(PipelineError, match="cycle: a -> b -> c -> a"):
        p.validate()


def test_duplicate_and_unknown_stage() -> None:
    p = Pipeline()
    p.add(cmd("a"))
    with pytest.raises(PipelineError, match="duplicate"):
        p.add(cmd("a"))
    with pytest.raises(PipelineError, match="unknown stage 'z'"):
        p.get("z")


@settings(max_examples=50, deadline=None)
@given(st.lists(st.sets(st.integers(0, 9), max_size=3), min_size=1, max_size=10))
def test_property_topological_order(dep_sets: list[set[int]]) -> None:
    p = Pipeline()
    for i, deps in enumerate(dep_sets):
        p.add(cmd(f"s{i}", deps=tuple(f"o{j}" for j in deps if j < i), outs=(f"o{i}",)))
    order = p.validate().order
    pos = {n: k for k, n in enumerate(order)}
    for name, ups in p.graph.upstream.items():
        for u in ups:
            assert pos[u] < pos[name]
    assert sorted(order) == sorted(p.names)


# -- YAML / config ---------------------------------------------------------------
def test_parse_stage_errors_and_forms() -> None:
    s = parse_stage("s", {"cmd": ["a", "b"], "deps": "x", "desc": "d"})
    assert s.cmd == "a && b" and s.deps == ("x",) and s.description == "d"
    for raw, msg in [
        ([], "must be a mapping"),
        ({"cmd": "x", "nope": 1}, "unknown keys"),
        ({}, "'cmd' is required"),
        ({"cmd": 3}, "cmd must be"),
        ({"cmd": ["a", 2]}, "cmd list"),
        ({"cmd": "x", "always_run": "yes"}, "always_run"),
        ({"cmd": "x", "deps": [1]}, "list of strings"),
    ]:
        with pytest.raises(ConfigError, match=msg):
            parse_stage("s", raw)


def test_parse_config() -> None:
    cfg = parse_config(
        {
            "params": "conf/p.yaml",
            "jobs": 3,
            "cache": {
                "dir": "/tmp/c",
                "link": "hardlink",
                "remote": "s3://b/p",
                "autopush": True,
                "s3": {"endpoint_url": "http://localhost:9000", "region": "eu-west-1"},
            },
        }
    )
    assert cfg.params_file == "conf/p.yaml" and cfg.jobs == 3
    assert cfg.cache.link == "hardlink" and cfg.cache.autopush
    assert cfg.cache.s3_endpoint_url == "http://localhost:9000"
    for raw, msg in [
        ({"bogus": 1}, "unknown top-level"),
        ({"params": 3}, "default params file"),
        ({"jobs": 0}, "positive integer"),
        ({"jobs": "x"}, "positive integer"),
        ({"python": 3}, "python"),
        ({"cache": []}, "mapping"),
        ({"cache": {"x": 1}}, "unknown keys"),
        ({"cache": {"link": "symlink"}}, "cache.link"),
        ({"cache": {"autopush": "yes"}}, "autopush"),
        ({"cache": {"s3": []}}, "cache.s3"),
    ]:
        with pytest.raises(ConfigError, match=msg):
            parse_config(raw)


def test_env_overrides() -> None:
    cfg = Config().apply_env(
        {
            "DAGCACHE_REMOTE": "/r",
            "DAGCACHE_S3_ENDPOINT_URL": "http://e",
            "DAGCACHE_S3_REGION": "r1",
            "DAGCACHE_JOBS": "5",
        }
    )
    assert (cfg.cache.remote, cfg.cache.s3_endpoint_url, cfg.cache.s3_region, cfg.jobs) == (
        "/r",
        "http://e",
        "r1",
        5,
    )


def test_split_python_ref() -> None:
    assert split_python_ref("p.py") == ("p.py", "pipeline")
    assert split_python_ref("p.py:pipe") == ("p.py", "pipe")
    assert split_python_ref("dir/p.py:") == ("dir/p.py", "pipeline")


def test_workspace_discovery_and_yaml_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(tmp_path / "dagcache.yaml", "stages:\n  a:\n    cmd: echo\n")
    (tmp_path / "sub" / "deeper").mkdir(parents=True)
    ws = Workspace.load(start=tmp_path / "sub" / "deeper")
    assert ws.root == tmp_path.resolve() and ws.pipeline.names == ["a"]
    assert (ws.state_dir / ".gitignore").read_text() == "*\n"
    ws.close()
    with pytest.raises(ConfigError, match=r"no dagcache\.yaml"):
        Workspace.load(start=Path("/"))
    for text, msg in [
        ("[1, 2]", "mapping at the top level"),
        ("stages: [x]", "'stages' must be a mapping"),
        ("a: [", "not valid YAML"),
        ("python: p.py\nstages: {a: {cmd: x}}\n", "both 'stages' and 'python'"),
    ]:
        write(tmp_path / "bad" / "dagcache.yaml", text)
        if "python" in text:
            write(
                tmp_path / "bad" / "p.py", "from dagcache import Pipeline\npipeline = Pipeline()\n"
            )
        with pytest.raises(ConfigError, match=msg):
            Workspace.load(tmp_path / "bad" / "dagcache.yaml")
    with pytest.raises(ConfigError, match="not found"):
        Workspace.load(tmp_path / "missing.yaml")
    empty = write(tmp_path / "empty" / "dagcache.yaml", "")
    ws = Workspace.load(empty)
    assert len(ws.pipeline) == 0
    ws.close()


def test_python_pipeline_loading(tmp_path: Path) -> None:
    write(
        tmp_path / "proj" / "flow.py",
        """
        from dagcache import Pipeline
        pipe = Pipeline("flow", params_file="conf.yaml")

        @pipe.stage(outs=["o.txt"])
        def make(ctx):
            '''Make the file.'''
            ctx.path("o.txt").write_text("x")
        """,
    )
    write(tmp_path / "proj" / "dagcache.yaml", "cache:\n  link: hardlink\nstages: {}\n")
    ws = Workspace.load(Path(f"{tmp_path}/proj/flow.py:pipe"))
    assert ws.pipeline.names == ["make"]
    assert ws.pipeline.get("make").description == "Make the file."
    assert ws.config.cache.link == "hardlink" and ws.params.default_file == "conf.yaml"
    ws.close()
    write(tmp_path / "proj" / "dagcache.yaml", "python: flow.py:pipe\n")
    ws = Workspace.load(tmp_path / "proj" / "dagcache.yaml")
    assert ws.pipeline.names == ["make"]
    ws.close()
    with pytest.raises(ConfigError, match="no Pipeline named 'nope'"):
        Workspace.load(Path(f"{tmp_path}/proj/flow.py:nope"))
    write(tmp_path / "proj" / "broken.py", "raise RuntimeError('import-time failure')\n")
    with pytest.raises(ConfigError, match="import-time failure"):
        Workspace.load(tmp_path / "proj" / "broken.py")
    with pytest.raises(ConfigError, match="does not exist"):
        Workspace.load(tmp_path / "proj" / "absent.py")
