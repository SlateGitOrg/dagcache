"""Offline end-to-end demo of dagcache on the bundled examples.

Runs entirely inside ``build/demo/`` (recreated each time) and prints every
command with its real output. Usage: ``make demo`` (or ``uv run python -u scripts/demo.py``).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
BUILD = REPO / "build" / "demo"
WINE = BUILD / "wine"
CLONE = BUILD / "wine-clone"
REMOTE = BUILD / "shared-remote"
DECO = BUILD / "decorators"
ARTIFACTS = ("data", "models", "metrics", "reports")

ENV = dict(os.environ)
# Stage commands call `python`: make it the interpreter running this demo.
ENV["PATH"] = f"{Path(sys.executable).parent}{os.pathsep}{ENV.get('PATH', '')}"
ENV.pop("DAGCACHE_REMOTE", None)


def banner(title: str) -> None:
    print(f"\n=== {title} " + "=" * max(0, 70 - len(title)), flush=True)


def dagcache(cwd: Path, *args: str, env: dict[str, str] | None = None) -> float:
    print(f"$ dagcache {' '.join(args)}", flush=True)
    t0 = time.monotonic()
    proc = subprocess.run(
        [sys.executable, "-m", "dagcache", *args],
        cwd=cwd,
        env=env or ENV,
        text=True,
        capture_output=True,
        check=False,
    )
    elapsed = time.monotonic() - t0
    sys.stdout.write(proc.stdout)
    if proc.stderr.strip():
        sys.stdout.write(proc.stderr)
    if proc.returncode not in (0, 1):
        raise SystemExit(f"command failed with exit code {proc.returncode}")
    sys.stdout.flush()
    return elapsed


def edit(path: Path, old: str, new: str) -> None:
    text = path.read_text()
    if old not in text:
        raise SystemExit(f"demo edit failed: {old!r} not in {path}")
    path.write_text(text.replace(old, new, 1))
    print(f"# edit {path.relative_to(BUILD)}: {old.strip()!r} -> {new.strip()!r}", flush=True)


def copy_example(src: Path, dest: Path) -> None:
    shutil.copytree(
        src,
        dest,
        ignore=shutil.ignore_patterns(".dagcache", "dagcache.lock", "__pycache__", *ARTIFACTS),
    )


def main() -> None:
    if BUILD.exists():
        shutil.rmtree(BUILD)
    BUILD.mkdir(parents=True)
    copy_example(REPO / "examples" / "wine", WINE)
    copy_example(REPO / "examples" / "decorators", DECO)
    params = WINE / "params.yaml"

    banner("1. The pipeline: 7 stages, DAG inferred from deps/outs")
    dagcache(WINE, "dag")

    banner("2. Status before the first run")
    dagcache(WINE, "status")

    banner("3. First run (4 parallel workers): everything runs")
    cold = dagcache(WINE, "repro", "-j", "4")

    banner("4. Second run: nothing to do")
    warm = dagcache(WINE, "repro")

    banner("5. Change a hyper-parameter: only train + evaluate are stale")
    edit(params, "C: 1.0", "C: 0.05")
    dagcache(WINE, "status")
    dagcache(WINE, "repro", "-j", "4", "--no-output")
    print((WINE / "metrics/metrics.json").read_text())

    banner("6. Revert it: results come back from the cache, nothing recomputes")
    edit(params, "C: 0.05", "C: 1.0")
    dagcache(WINE, "status")
    dagcache(WINE, "repro", "--no-output")

    banner("7. Comment-only code edit and an unused param: still up to date")
    edit(
        WINE / "src/split.py",
        '"""Stratified train/test split."""',
        '"""Stratified train/test split."""\n# reviewed',
    )
    edit(params, "title: Wine cultivar classifier", "title: Wine classifier v2")
    dagcache(WINE, "status")

    banner("8. Early cutoff: load re-runs but writes identical bytes")
    edit(WINE / "src/load.py", 'print(f"wrote', 'print(f"exported')
    dagcache(WINE, "repro", "--no-output")

    banner("9. Lineage of the metrics file")
    dagcache(WINE, "lineage", "metrics/metrics.json")

    banner("10. Delete every artifact: restore all of them from the cache")
    for name in ARTIFACTS:
        shutil.rmtree(WINE / name)
    restore = dagcache(WINE, "repro", "--no-output")

    banner("11. Garbage collection")
    dagcache(WINE, "cache", "stats")
    dagcache(WINE, "gc", "--dry-run")
    dagcache(WINE, "gc")
    dagcache(WINE, "cache", "stats")

    banner("12. Share through a remote: a fresh clone runs nothing")
    remote_env = dict(ENV, DAGCACHE_REMOTE=str(REMOTE))
    dagcache(WINE, "push", env=remote_env)
    CLONE.mkdir()
    for item in ("dagcache.yaml", "params.yaml", "dagcache.lock", "src"):
        src = WINE / item
        if src.is_dir():
            shutil.copytree(src, CLONE / item)
        else:
            shutil.copy(src, CLONE / item)
    print("# cloned pipeline + params + lock file + src (no data, no cache)")
    dagcache(CLONE, "repro", "--no-output", env=remote_env)

    banner("13. Two concurrent runs on a cold workspace share the work")
    race = BUILD / "wine-race"
    copy_example(REPO / "examples" / "wine", race)
    procs = [
        subprocess.Popen(
            [sys.executable, "-m", "dagcache", "repro", "-q", "-j", "2"],
            cwd=race,
            env=ENV,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        for _ in range(2)
    ]
    ran = 0
    for i, proc in enumerate(procs, 1):
        out, _ = proc.communicate()
        print(f"[process {i}] exit {proc.returncode}: {out.strip()}")
        if proc.returncode != 0:
            raise SystemExit("concurrent run failed")
        ran += int(out.split("s: ", 1)[1].split(" ran", 1)[0])
    print(f"stage executions across both processes: {ran} (pipeline has 7 stages)")
    if ran != 7:
        raise SystemExit("a stage ran more than once")

    banner("14. Python-decorator pipeline: function-level code hashing")
    dagcache(DECO, "-f", "pipeline.py", "repro")
    edit(
        DECO / "helpers.py",
        "sd = statistics.pstdev(values) or 1.0",
        "sd = statistics.pstdev(values) or 1e-9",
    )
    dagcache(DECO, "-f", "pipeline.py", "status")
    dagcache(DECO, "-f", "pipeline.py", "repro")

    banner("Summary")
    print(
        f"cold run {cold:.2f}s | no-op run {warm:.2f}s | full restore {restore:.2f}s "
        "(wall-clock incl. Python start-up, this machine)"
    )


if __name__ == "__main__":
    main()
