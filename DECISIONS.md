# Decisions

Rulings made while building dagcache, in the form
`Ruling: <what> — <why> — <cost if wrong>`.

## Keys and hashing

- Ruling: a downstream key contains the **content digest** of each input, not the upstream stage's key — gives early cutoff (an upstream that re-runs but writes identical bytes does not invalidate anything after it), like Bazel/Shake — cost if wrong: `status` cannot know a downstream key while an upstream must run, so such stages show as `pending` rather than `stale`.
- Ruling: the command **template** (with `${param}` unexpanded) goes into the key, and every interpolated param is tracked automatically — a param change then reads as `param train.C changed: 1.0 → 0.05` instead of a noisy "command changed"; template + values determine the command exactly — cost if wrong: none found; the two are equivalent inputs.
- Ruling: the stage **name is not part of the key** — renaming a stage should not re-run it, and identical stage definitions may share cache entries — cost if wrong: two differently named stages with identical definitions share results, which is the intended content-addressed behaviour.
- Ruling: `.py` code dependencies are hashed by **AST dump without positions**; docstrings count, comments and formatting do not — formatter runs and comment edits are the most common non-semantic changes in ML repos; docstrings are runtime values (argparse help, etc.) — cost if wrong: a program that inspects its own comments would not be invalidated (contrived).
- Ruling: Python stage fingerprints strip the **decorator** — decorator arguments (deps/outs/params) are already key components; including them would double-report every declaration change as a code change — cost if wrong: none; the information is still in the key.
- Ruling: Python fingerprints recurse into **workspace-local** functions/classes/modules, record `dist==version` for third-party code and `python==X.Y` for the stdlib, and ignore arbitrary objects (with a debug log) — this covers how pipeline code actually changes without hashing all of site-packages; a library upgrade correctly invalidates — cost if wrong: behaviour that depends on a referenced *object's* state is not tracked; the documented escape hatch is `salt`.
- Ruling: declared env vars enter the key as **unsalted SHA-256 of the value**, never the raw value — the lock file is committed and env vars often carry secrets — cost if wrong: low-entropy secrets could be brute-forced from a committed lock file; documented in HASHING.md and README limitations.
- Ruling: `1` and `1.0` are **different** parameter values (canonical JSON, no numeric normalisation) — normalising would need type-aware rules that differ per consumer — cost if wrong: an occasional extra re-run after reformatting a params file.
- Ruling: the stat cache trusts a memo only if the file's mtime is ≥ 2 s older than when it was hashed (git's "racy" rule), keyed on inode+size+mtime_ns+ctime_ns — a same-size rewrite inside timestamp granularity would otherwise be missed — cost if wrong: files written in the last 2 s are re-hashed (cheap).

## Storage and state

- Ruling: the lock file is **JSON**, not YAML — the first YAML implementation spent ~33 s of a 100-stage run inside the pure-Python YAML parser (profiled with cProfile); JSON plus stat-memoised reads took the same run to ~0.9 s. JSON lock files are common (`package-lock.json`, `MODULE.bazel.lock`) and still diff well when pretty-printed and sorted — cost if wrong: slightly less pleasant to hand-read than YAML.
- Ruling: the lock file is rewritten after **every** stage (not batched at the end) — an interrupted run keeps the record of what finished — cost if wrong: O(V) bytes per stage, O(V²) per run; fine to a few thousand stages, noted in ARCHITECTURE.md.
- Ruling: declared outputs are **deleted before** a stage runs — otherwise a stage that silently fails to write an output could pass with stale content — cost if wrong: a stage that wanted to update an output incrementally cannot (declare it as a dependency of a separate stage instead).
- Ruling: restore **copies** by default; `cache.link: hardlink` is opt-in and cache objects are read-only (0444) — copies are safe for tools that write in place; hard links save disk for large artifacts — cost if wrong: double disk usage by default.
- Ruling: symlinked *directories* inside directory outputs are rejected; symlinked files are followed and restored as regular files — following directory links risks cycles and capturing data outside the workspace — cost if wrong: pipelines that emit symlink farms need a different layout.
- Ruling: object and manifest writes are **verify-then-rename** (hash while copying into `tmp/`, refuse on mismatch, then `os.replace`) — readers can never observe partial data; bit rot is detected on every read — cost if wrong: one extra hash pass on writes (cheap relative to I/O).

## Execution and concurrency

- Ruling: the scheduler is a **thread pool**; command stages are subprocesses (real parallelism), Python stages run in-thread — threads keep Python stages simple (closures, no pickling) — cost if wrong: pure-Python CPU-bound decorated stages do not speed up with `-j` (the GIL); NumPy/scikit-learn code mostly releases it. Documented.
- Ruling: concurrency control is **advisory `flock`** with three lock levels (repo shared/exclusive, per stage, lock-file mutex), re-checking state after acquiring the stage lock — simple, crash-safe (kernel releases locks), and enough to make concurrent `repro` runs share work — cost if wrong: POSIX only (no Windows), and `flock` over NFS is unreliable; both stated as limitations.
- Ruling: concurrent runs with **different** inputs on one workspace are not isolated from each other — isolating them needs per-run sandboxes (Bazel-style execroots), out of scope — cost if wrong: two people running different params in the same checkout at the same time can race on outputs; documented.
- Ruling: default is **fail-fast** (no new stages after a failure, in-flight ones finish); `--keep-going` runs independent branches — matches make/DVC expectations — cost if wrong: none.
- Ruling: the SQLite stat cache **degrades instead of failing** (retry the WAL switch with backoff; on persistent errors, re-hash) — a demo run of two cold processes crashed one of them with "database is locked"; the cache is an optimisation and must never fail a run — cost if wrong: slower status under pathological DB failures.

## Remote cache

- Ruling: remotes are either a directory (`file://` or a path) or **S3-compatible** via boto3 as an optional extra (`dagcache[s3]`) — covers NFS/shared disks and every S3 clone; boto3 is the de facto client — cost if wrong: ~80 MB extra install for S3 users only.
- Ruling: remote **failures degrade to local** behaviour (lookup errors are misses, autopush errors are warnings) — a flaky network must not fail a local run whose results are already cached locally — cost if wrong: a user may not notice that sharing failed; the warning tells them to run `dagcache push`.
- Ruling: unit tests use **moto** in-process for S3; `make integration` runs the same backend against a **real S3 server in Docker** (SeaweedFS 3.97) — `make check` must not need Docker or network. MinIO was the first choice, but `minio/minio` on Docker Hub returned "pull access denied" and quay.io returned 401 on 2026-10-25 — cost if wrong: SeaweedFS quirks differ from AWS (the first attempt needed more volume slots: `-volume.max=64`).
- Ruling: `gc --remote` uses the local lock file as roots and is **not coordinated across machines** — a distributed lease is out of scope — cost if wrong: GC on a shared remote while a teammate pushes could delete their fresh objects; documented, and remote GC is opt-in.

## Lineage, GC, CLI

- Ruling: lineage is resolved from **cached manifests**, preferring lock entries when the key matches — manifests survive pipeline edits, so old artifacts stay traceable; lock entries carry current upstream links after an early cutoff — cost if wrong: after `gc` removes old manifests, lineage of old artifacts ends at "manifest gone".
- Ruling: GC roots = current lock file + optional extra lock files (`--keep-lock`) + actions used in the last `--keep-days` — the lock file is what a checkout needs; extra lock files let users keep other branches' results without git integration — cost if wrong: results of other branches are collected unless their lock files are passed.
- Ruling: exit codes 0 / 1 (stage failure, `status --check` found work, corrupt cache) / 2 (configuration error, including `status` with unevaluable stages) — scripts and CI can tell "broken pipeline" from "work to do" — cost if wrong: none.
- Ruling: the example uses scikit-learn's **bundled UCI Wine** data — real, public, and offline (no network in `make check`/`make demo`); the test split is held out until `evaluate` and the scaler is fit on train only — cost if wrong: a tiny dataset (178 rows) means metrics are noisy; the demo is about caching, not modelling.
- Ruling: the CLI prints cache locations **relative to the current directory** — keeps absolute home paths out of pasted output and logs — cost if wrong: none.
