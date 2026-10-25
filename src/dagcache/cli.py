"""Command-line interface: ``dagcache <command>``.

Exit codes: 0 success; 1 a stage failed, ``status --check`` found work, or
``cache verify`` found corruption; 2 configuration/usage error.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
from collections.abc import Sequence
from pathlib import Path
from typing import TextIO

from . import __version__
from .canonical import short
from .errors import ConfigError, DagcacheError
from .gc import collect
from .lineage import (
    LineageResolver,
    render_dot,
    render_mermaid,
    render_pipeline,
    render_text,
    to_dict,
)
from .locking import FileLock
from .runner import Action, Runner, StageResult
from .status import State, plan
from .workspace import PIPELINE_FILE, Workspace

log = logging.getLogger("dagcache")

_TEMPLATE = """\
# dagcache pipeline definition. Paths are relative to this file.
params: params.yaml
cache:
  link: copy          # or: hardlink (read-only outputs, no copy)
  # remote: s3://my-bucket/dagcache      (or a directory path)
  # autopush: true
stages:
  hello:
    cmd: mkdir -p out && echo "hello ${greeting.name}" > out/hello.txt
    outs: [out/hello.txt]
"""


def _human_bytes(n: int) -> str:
    size = float(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024 or unit == "GiB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{n} B"  # pragma: no cover


class _Printer:
    """Thread-safe line printer for progress output."""

    def __init__(self, stream: TextIO, *, quiet: bool, show_output: bool) -> None:
        self.stream = stream
        self.quiet = quiet
        self.show_output = show_output
        self._lock = threading.Lock()

    def line(self, text: str) -> None:
        with self._lock:
            self.stream.write(text + "\n")
            self.stream.flush()

    def on_start(self, name: str) -> None:
        if not self.quiet:
            self.line(f"  start      {name}")

    def on_output(self, name: str, text: str) -> None:
        if self.show_output and not self.quiet:
            self.line(f"  | {name}: {text}")

    def on_finish(self, r: StageResult) -> None:
        if self.quiet and r.action is not Action.FAILED:
            return
        timing = f"{r.duration_s:6.2f}s" if r.action is not Action.UP_TO_DATE else "       "
        why = f"  ({'; '.join(r.reasons)})" if r.reasons else ""
        if r.action is Action.FAILED:
            self.line(f"  FAILED     {r.name}\n{r.error}")
            return
        self.line(f"  {r.action.value:<10} {r.name:<16}{timing}  {short(r.key)}{why}")


def _load(args: argparse.Namespace) -> Workspace:
    path = Path(args.file) if args.file else None
    return Workspace.load(path, start=Path.cwd(), lock_timeout=args.lock_timeout)


# -- commands -----------------------------------------------------------------
def cmd_init(args: argparse.Namespace, out: TextIO) -> int:
    target = Path(args.file or PIPELINE_FILE)
    if target.exists():
        out.write(f"{target} already exists\n")
        return 0
    target.write_text(_TEMPLATE, encoding="utf-8")
    params = target.parent / "params.yaml"
    if not params.exists():
        params.write_text("greeting:\n  name: world\n", encoding="utf-8")
    out.write(f"created {target} and {params}\n")
    return 0


def cmd_repro(args: argparse.Namespace, out: TextIO) -> int:
    ws = _load(args)
    try:
        if args.dry_run:
            return _print_status(ws, args.targets, out, as_json=False, force=args.force)
        jobs = args.jobs or ws.config.jobs or 1
        printer = _Printer(out, quiet=args.quiet, show_output=not args.no_output)
        runner = Runner(
            ws,
            jobs=jobs,
            force=args.force,
            keep_going=args.keep_going,
            use_remote=not args.no_remote,
            on_start=printer.on_start if args.verbose else None,
            on_finish=printer.on_finish,
            on_output=printer.on_output,
        )
        n = len(ws.pipeline.upstream_closure(args.targets)) if args.targets else len(ws.pipeline)
        if not args.quiet:
            printer.line(f"repro: {n} stage(s), jobs={jobs}")
        report = runner.repro(args.targets or None)
        if args.json:
            out.write(json.dumps([r.to_dict() for r in report.results], indent=2) + "\n")
        summary = (
            f"done in {report.elapsed_s:.2f}s: {report.count(Action.RAN)} ran, "
            f"{report.count(Action.RESTORED)} restored, {report.count(Action.UP_TO_DATE)} "
            f"up-to-date, {report.count(Action.FAILED)} failed, "
            f"{report.count(Action.NOT_RUN)} not run (max parallel {report.max_parallel})"
        )
        printer.line(summary)
        for r in report.results:
            if r.action is Action.NOT_RUN and not args.quiet:
                printer.line(f"  not run    {r.name}  ({'; '.join(r.reasons)})")
        return 0 if report.ok else 1
    finally:
        ws.close()


def _print_status(
    ws: Workspace, targets: Sequence[str] | None, out: TextIO, *, as_json: bool, force: bool = False
) -> int:
    statuses = plan(ws, ws.cache(), targets or None, force=force)
    if as_json:
        out.write(json.dumps([s.to_dict() for s in statuses], indent=2) + "\n")
    else:
        width = max((len(s.name) for s in statuses), default=5)
        for s in statuses:
            note = "   (restorable from cache)" if s.state is State.CACHED else ""
            first = s.reasons[0] if s.reasons else ""
            out.write(f"{s.name:<{width}}  {s.state.value:<10}  {first}{note}".rstrip() + "\n")
            for extra in s.reasons[1:]:
                out.write(f"{'':<{width}}  {'':<10}  {extra}\n")
        todo = sum(1 for s in statuses if s.state.needs_work)
        out.write(f"{todo} of {len(statuses)} stage(s) need work\n")
    if any(s.state is State.ERROR for s in statuses):
        return 2
    return 0


def cmd_status(args: argparse.Namespace, out: TextIO) -> int:
    ws = _load(args)
    try:
        rc = _print_status(ws, args.targets, out, as_json=args.json)
        if rc == 0 and args.check:
            statuses = plan(ws, ws.cache(), args.targets or None)
            return 1 if any(s.state.needs_work for s in statuses) else 0
        return rc
    finally:
        ws.close()


def cmd_dag(args: argparse.Namespace, out: TextIO) -> int:
    ws = _load(args)
    try:
        states = None
        if args.status:
            states = {s.name: s.state.value for s in plan(ws, ws.cache())}
        out.write(render_pipeline(ws.pipeline, args.format, states) + "\n")
        return 0
    finally:
        ws.close()


def cmd_lineage(args: argparse.Namespace, out: TextIO) -> int:
    ws = _load(args)
    try:
        node = LineageResolver(ws, ws.cache(use_remote=False)).trace(args.path)
        if args.format == "json":
            out.write(json.dumps(to_dict(node), indent=2) + "\n")
        elif args.format == "mermaid":
            out.write(render_mermaid(node) + "\n")
        elif args.format == "dot":
            out.write(render_dot(node) + "\n")
        else:
            out.write(render_text(node) + "\n")
        return 0
    finally:
        ws.close()


def cmd_gc(args: argparse.Namespace, out: TextIO) -> int:
    ws = _load(args)
    try:
        report = collect(
            ws,
            keep_days=args.keep_days,
            extra_lock_files=[Path(p) for p in args.keep_lock],
            dry_run=args.dry_run,
            remote=args.remote,
        )
        if args.json:
            out.write(json.dumps(report.to_dict(), indent=2) + "\n")
            return 0
        verb = "would remove" if report.dry_run else "removed"
        out.write(
            f"gc {report.location}: {report.roots} root action(s); kept {report.actions_kept} "
            f"action(s) / {report.objects_kept} object(s); {verb} {report.actions_removed} "
            f"action(s) / {report.objects_removed} object(s), {_human_bytes(report.bytes_freed)}\n"
        )
        if report.missing_objects:
            out.write(f"warning: {len(report.missing_objects)} referenced tree object(s) missing\n")
        return 0
    finally:
        ws.close()


def _keys_for(ws: Workspace, targets: Sequence[str]) -> list[str]:
    lock = ws.lockfile.read()
    names = ws.pipeline.upstream_closure(targets) if targets else set(lock)
    return [lock[n].key for n in sorted(names) if n in lock]


def cmd_push(args: argparse.Namespace, out: TextIO) -> int:
    ws = _load(args)
    try:
        cache = ws.cache()
        if cache.remote is None:
            raise ConfigError("no remote configured (cache.remote or DAGCACHE_REMOTE)")
        with _repo_shared(ws):
            stats = cache.push(_keys_for(ws, args.targets))
        out.write(
            f"pushed {stats.actions} action(s), {stats.objects} new object(s) "
            f"({_human_bytes(stats.bytes)}) to {cache.remote.location}\n"
        )
        if stats.missing:
            out.write(f"warning: {stats.missing} item(s) missing locally were not pushed\n")
        return 0
    finally:
        ws.close()


def cmd_pull(args: argparse.Namespace, out: TextIO) -> int:
    ws = _load(args)
    try:
        cache = ws.cache()
        if cache.remote is None:
            raise ConfigError("no remote configured (cache.remote or DAGCACHE_REMOTE)")
        with _repo_shared(ws):
            stats = cache.pull(_keys_for(ws, args.targets))
        out.write(
            f"pulled {stats.actions} action(s), {stats.objects} object(s) "
            f"({_human_bytes(stats.bytes)}) from {cache.remote.location}\n"
        )
        if stats.missing:
            out.write(f"warning: {stats.missing} item(s) not found on the remote\n")
        return 0
    finally:
        ws.close()


def _repo_shared(ws: Workspace) -> FileLock:
    return FileLock(ws.repo_lock_path, shared=True, timeout=ws.lock_timeout)


def cmd_cache(args: argparse.Namespace, out: TextIO) -> int:
    ws = _load(args)
    try:
        local = ws.local
        if args.cache_cmd == "verify":
            bad = [d for d, _ in local.iter_objects() if not local.verify_object(d)]
            for d in bad:
                out.write(f"corrupt object {d}\n")
            out.write(f"verified {local.location}: {len(bad)} corrupt object(s)\n")
            return 1 if bad else 0
        n_obj = n_bytes = 0
        for _, size in local.iter_objects():
            n_obj += 1
            n_bytes += size
        n_act = sum(1 for _ in local.iter_actions())
        out.write(
            f"{local.location}: {n_act} action(s), {n_obj} object(s), {_human_bytes(n_bytes)}\n"
        )
        return 0
    finally:
        ws.close()


# -- parser -------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dagcache", description="Content-addressed, cached pipeline runner for ML workflows."
    )
    p.add_argument("--version", action="version", version=f"dagcache {__version__}")
    p.add_argument("-f", "--file", help="pipeline file (dagcache.yaml or pipeline.py[:attr])")
    p.add_argument("-v", "--verbose", action="store_true", help="debug logging and stage starts")
    p.add_argument(
        "--lock-timeout",
        type=float,
        default=None,
        help="seconds to wait for locks (default: forever)",
    )
    sub = p.add_subparsers(dest="command", required=True)
    # Let -v also follow the subcommand without overriding a global -v.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("-v", "--verbose", action="store_true", default=argparse.SUPPRESS)

    s = sub.add_parser("init", help="create a starter dagcache.yaml", parents=[common])
    s.set_defaults(func=cmd_init)

    for name in ("repro", "run"):
        s = sub.add_parser(
            name, help="bring outputs up to date (run/restore stale stages)", parents=[common]
        )
        s.add_argument("targets", nargs="*", help="stages to reproduce (default: all)")
        s.add_argument("-j", "--jobs", type=int, default=None, help="parallel stages")
        s.add_argument("--force", action="store_true", help="run every selected stage")
        s.add_argument("--keep-going", action="store_true", help="continue independent branches")
        s.add_argument("--dry-run", action="store_true", help="show what would happen")
        s.add_argument("--no-remote", action="store_true", help="ignore the remote cache")
        s.add_argument("--no-output", action="store_true", help="hide stage command output")
        s.add_argument("-q", "--quiet", action="store_true", help="only print the summary")
        s.add_argument("--json", action="store_true", help="print results as JSON")
        s.set_defaults(func=cmd_repro)

    s = sub.add_parser("status", help="show stale stages and why", parents=[common])
    s.add_argument("targets", nargs="*")
    s.add_argument("--json", action="store_true")
    s.add_argument("--check", action="store_true", help="exit 1 if anything needs work")
    s.set_defaults(func=cmd_status)

    s = sub.add_parser("dag", help="print the stage graph", parents=[common])
    s.add_argument("--format", choices=["text", "mermaid", "dot"], default="text")
    s.add_argument("--status", action="store_true", help="annotate with stage states")
    s.set_defaults(func=cmd_dag)

    s = sub.add_parser("lineage", help="show what produced an artifact", parents=[common])
    s.add_argument("path")
    s.add_argument("--format", choices=["text", "json", "mermaid", "dot"], default="text")
    s.set_defaults(func=cmd_lineage)

    s = sub.add_parser(
        "gc", help="delete cache entries not reachable from the lock file", parents=[common]
    )
    s.add_argument("--dry-run", action="store_true")
    s.add_argument("--keep-days", type=float, default=None, help="also keep recently used")
    s.add_argument("--keep-lock", action="append", default=[], help="extra lock file roots")
    s.add_argument("--remote", action="store_true", help="collect the remote instead")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_gc)

    for name, func in (("push", cmd_push), ("pull", cmd_pull)):
        s = sub.add_parser(name, help=f"{name} cached results for locked stages", parents=[common])
        s.add_argument("targets", nargs="*")
        s.set_defaults(func=func)

    s = sub.add_parser("cache", help="inspect the local cache", parents=[common])
    s.add_argument("cache_cmd", choices=["stats", "verify"])
    s.set_defaults(func=cmd_cache)
    return p


def main(argv: Sequence[str] | None = None, out: TextIO | None = None) -> int:
    stream = out or sys.stdout
    parser = build_parser()
    args = parser.parse_args(argv)
    level = logging.DEBUG if args.verbose else logging.WARNING
    if os.environ.get("DAGCACHE_LOG"):
        level = getattr(logging, os.environ["DAGCACHE_LOG"].upper(), level)
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")
    try:
        rc: int = args.func(args, stream)
        return rc
    except ConfigError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except DagcacheError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    except KeyboardInterrupt:
        sys.stderr.write("interrupted\n")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
