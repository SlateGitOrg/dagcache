# Hashing semantics

This document is the contract for **when dagcache considers a stage's result
reusable**. If two runs produce the same cache key, dagcache assumes the
stage would produce the same outputs and will reuse (or restore) them instead
of running it. Everything below follows from that one rule.

All digests are SHA-256, hex encoded. All paths are POSIX strings relative to
the workspace root (the directory holding `dagcache.yaml`), which is what makes
keys portable between clones and machines.

## 1. The cache key

A stage's key is `sha256(canonical_json({"schema": 1, ...components}))` where
the components are ([`manifest.py`](../src/dagcache/manifest.py),
[`keys.py`](../src/dagcache/keys.py)):

| component | what goes in | changes when |
|---|---|---|
| `kind` | `"cmd"` or `"python"` | the stage switches between a shell command and a decorated function |
| `cmd` | the command **template**, with `${...}` placeholders *unexpanded* | the command text changes |
| `code` | named digests of the stage's code (section 3) | code changes in a way that can affect behaviour |
| `deps` | `{path: content digest}` for every declared dependency | input data changes |
| `params` | `{name: value}` for declared parameters **and** every `${...}` in the command | a parameter the stage actually reads changes |
| `outs` | sorted list of declared output paths | the output declaration changes |
| `wdir` | working directory of the command | it changes |
| `env` | `{NAME: sha256(value) or null}` for declared env vars | a declared variable changes or is (un)set |
| `salt` | free-form string | you bump it to force a re-run |

Canonical JSON = sorted keys, no whitespace, UTF-8, no NaN/Infinity; tuples
become lists ([`canonical.py`](../src/dagcache/canonical.py)). `1` and `1.0`
serialise differently and therefore produce different keys; dagcache does not
attempt numeric normalisation.

What is **not** in the key, on purpose:

* **the stage name**: renaming a stage does not re-run it, and two stages with
  identical definitions share cache entries;
* **timestamps, owners, permissions** (except the executable bit of files
  inside directory outputs, see 2);
* **undeclared reads**: a script that reads a file not listed in `deps`/`code`
  is invisible to dagcache. This is the main correctness obligation on the user.

The template-plus-values rule means `cmd: python train.py --C ${train.C}` keeps
the same `cmd` component when `train.C` changes; the change shows up as
`param train.C changed: 1.0 → 0.05`, which is the useful explanation.

## 2. Data: files and directories

* **File**: SHA-256 of the bytes. Metadata is ignored.
* **Directory (tree)**: SHA-256 of a canonical JSON manifest
  `{"type": "tree", "v": 1, "entries": [[relpath, file digest, size, executable], ...]}`
  sorted by `relpath`. Empty directories are ignored. Symlinks to files are
  followed (their *content* counts); symlinked directories inside a tree are
  rejected rather than silently followed or skipped.
* The executable bit is part of a tree's identity because restoring a tree must
  reproduce runnable scripts; single-file outputs record it in the output record.

### Dependencies produced by another stage

A dependency may equal an upstream output, lie inside an upstream directory
output (`data/features/train.npz` inside `data/features`), or contain upstream
outputs. During `repro` the upstream has already finished, so the dependency is
hashed from the workspace. During `status` nothing runs, so the digest is
resolved without materialising anything ([`status.py`](../src/dagcache/status.py)):

| upstream state | digest used downstream |
|---|---|
| up to date | hash of the workspace file |
| restorable from cache (exact output path) | digest recorded in the upstream's cached manifest |
| restorable, but the dependency is nested/partial | unknown, so the stage is `pending` |
| must run | unknown, so the stage is `pending` |

### Transitive invalidation and early cutoff

Because a downstream key contains the *content digest* of its inputs (not the
upstream's key), invalidation is transitive exactly when it should be:

* change raw data → `prepare`'s output changes → `train`'s key changes → ... ;
* change `prepare`'s code in a way that writes **byte-identical** output →
  `prepare` re-runs, but `train`'s key is unchanged, so `train` and everything
  after it stay up to date. This is Bazel/Shake-style *early cutoff*, and it is
  why a comment or log-message change in an early stage does not retrain a model.
  (Section 8 of `make demo` shows it.)

## 3. Code

### Command stages: `code:` paths

* `.py` files are parsed and hashed by their **AST dump without positions**:
  comments, blank lines, indentation style and line wrapping do not matter.
  Docstrings *do* matter (they are runtime values, e.g. `argparse` descriptions).
* A `.py` file that does not parse is hashed as raw bytes.
* Any other file is hashed as raw bytes (a comment in a shell script *does* count).
* A directory is hashed as a tree of raw file digests.

### Python stages (`@pipeline.stage`)

The fingerprint is a set of named entries ([`codehash.py`](../src/dagcache/codehash.py)):

| entry | source |
|---|---|
| `py:<module>.<function>` | normalised AST of the stage function, **decorator stripped** (changing `deps=` in the decorator is already in the key; it should not also read as "code changed") |
| `py:<module>.<helper>` | every function/class it references that is defined in a **workspace-local** module, recursively |
| `module:<name>` | a workspace-local module referenced as a module (`import helpers; helpers.f()`), hashed whole |
| `const:<module>.<NAME>` | value of referenced module-level constants: numbers, strings, bytes, `None`, and lists/tuples/dicts/frozensets of those |
| `default:<module>.<function>.<param>` | value of a default argument (positional or keyword-only), for the stage, its local helpers and their methods: `def stage(ctx, scale=SCALE)` is invalidated when `SCALE` changes, even though the source text does not. Defaults that are functions, modules or library objects are classified like the `py:`/`module:`/`lib:` rows |
| `lib:<top-level package>` | `dist==version` for third-party code it references (e.g. `numpy==2.5.4`) |
| `lib:<stdlib module>` | `python==3.12` for standard-library references |

"Workspace-local" means the module's source file is under the workspace root
and not inside a virtualenv or `site-packages`. Closure variables of nested
functions are treated like globals, and so are default-argument values
(they are evaluated when the function is defined, so only their *values* can
reveal a change). References to arbitrary objects (an open
client, a model instance) are **not** fingerprinted; they are logged at debug
level. If behaviour depends on such state, declare it (as a param or file) or
bump `salt`.

Functions without retrievable source (defined via `exec`, in a REPL) fall
back to a digest of their bytecode and constants, with a warning; that
digest is only stable for one Python version.

Because each entry is named, `status` can say *what* changed:
`function helpers.zscore changed (749a30ba → c7930593)` or
`library numpy changed: numpy==2.5.3 → numpy==2.5.4`.

## 4. Parameters

Parameters live in YAML files (`params.yaml` by default). A stage lists dotted
keys (`train.C`, `layers.0.units`, `other.yaml:key`) and only those values enter
its key, so editing an unrelated parameter invalidates nothing. Values may be
whole sub-trees; YAML dates are converted to ISO strings; NaN/Infinity are
rejected (they have no canonical JSON form).

## 5. Environment variables

Only variables listed under `env:` are included, and only as `sha256(value)`.
The lock file is meant to be committed, and env vars are a common place for
secrets. The digest is **unsalted**: a low-entropy secret (a PIN, a short word)
could be recovered by brute force from a committed lock file. Do not track
such variables.

## 6. Racy files and the stat cache

Hashing every input on every `status` would be slow, so
[`StatCache`](../src/dagcache/hashing.py) memoises
`(path, inode, size, mtime_ns, ctime_ns) -> digest` in SQLite. A memo entry is
trusted only if the file's mtime is at least 2 s older than the moment it was
hashed. A file written within that window could be rewritten again inside the
filesystem's timestamp granularity without any of the stat fields changing.
This is the same "racy" rule git applies to its index. A same-size rewrite
that even restores the old mtime is still caught, because `ctime` and usually
the inode change (tested in `test_stat_cache_hits_only_when_safe`).

## 7. What "up to date" means

A stage is **up to date** iff its lock-file entry has the current key *and*
every recorded output exists in the workspace with the recorded digest. If
the key matches but an output was deleted or edited, the stage is restored
from the cache rather than re-run. Results are reused across branches and
clones: the cache is keyed only by content, so reverting a parameter brings
back the old outputs instantly.

## 8. Integrity

Every object write and read re-hashes the content and refuses a mismatch
(`CorruptObjectError`). Action manifests are checked on load: the stored key,
and the key recomputed from the stored components, must both equal the address
they were found under. A corrupt cache entry is treated as a miss and the
stage re-runs (`test_corrupt_cache_object_falls_back_to_running`).
`dagcache cache verify` re-hashes the whole local store.
