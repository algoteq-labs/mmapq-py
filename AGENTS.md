# AGENTS.md — mmapq

Instructions for AI coding agents working in this repository.

**This package wraps `mmapq`. Read `mmapq/AGENTS.md` first** (sibling
repo). Every upstream rule about frozen scope, on-disk layout, error codes,
testing discipline, and completion reporting applies here unchanged. Where
this file and upstream's differ, upstream wins.

The two upstream rules restated because violating them is unrecoverable:

1. **The on-disk layout and the public feature set are frozen.** No new
   features, no layout changes, no migration logic. If a task appears to
   need either, stop and ask.
2. **Under-claim.** Report "implemented and verified by executing X"
   separately from "implemented, not verified." A change reported as done
   that is not in the diff is the worst failure mode in this project's
   history. This repository specifically has had tests that "existed in the
   source file" but were never collected by the runner — presence in a file
   is not evidence; runner output is.

## Build and C source location

- The extension builds via cffi API mode (`setup.py`), compiling the C
  sources with `-DNDEBUG` — i.e. the Python binding runs the **release**
  configuration of the C library. Validation that exists only as
  `MMAP_ASSERT` upstream does not exist here at all; this is why input
  validation in the Python layer is mandatory, not defensive decoration.
- C sources are located relative to this repo (sibling `../mmapq`, or a
  vendored copy when packaging lands). Do not modify C sources from this
  repo — C changes are made upstream and flow here; see the workspace
  AGENTS.md for the sync protocol.
- Rebuild after any C change: `pip install -e . --force-reinstall` (or the
  project's documented equivalent). A stale extension silently tests old C.

## Binding discipline (hard)

- **Validate before every FFI call.** Empty bytes rejected before append
  (zero length is the end-of-stream sentinel — passing it through once
  caused reproducible on-disk corruption from two lines of Python), lengths
  bounded, closed handles guarded. The C release build will not save you.
- **Every C return code maps through `check_rc` in `_errors.py`.** A new
  `MMAPQ_*` code upstream means one `_RC_ERRORS` entry and (usually) one
  exception class, exported from `__init__.py`, in the same change. Exception
  classes are public API: importable from the package root, tested as such.
- **The cdef in `setup.py` must track the C headers.** Any change to a C
  struct or function signature used by the binding requires the matching
  cdef change in the same task. cffi will not warn you; it will corrupt.
- Callback trampolines: handler return values are validated/coerced; the
  cffi `error=` parameter is set so a raising handler cannot silently
  advance the queue. Handler objects must be kept alive for the duration of
  the call (`ffi.new_handle` held on the instance, not a local).
- Lifecycle: explicit `close()` is the primary API; `close()` is idempotent
  and never passes NULL/None to C; factory methods on closed objects raise
  rather than dereference. Interpreter-shutdown cleanup must be
  shutdown-safe (`weakref.finalize`, not bare `__del__`).

## Testing rules

- Framework: `unittest`, run via `python3 -m unittest tests.test_batch_queue -v`
  (or pytest over the same file). **A test not shown by `-v` output does not
  exist** — after adding tests, verify by name in runner output and report
  the new total. Test classes follow the existing pattern: `setUp` makes a
  unique temp dir, `tearDown` removes it.
- Every bug fix ships with a test executed and observed failing before the
  fix. Never delete or weaken an existing test to make a change pass; if a
  test seems wrong, stop and report.
- Error-path tests assert the **specific** exception type (`ValueError`,
  `QueueClosedError`, `QueueCorruptError`, `SchemaMismatchError`) — a bare
  `Exception` assertion is acceptable only for create-path failures, which
  currently surface as generic errors (documented limitation, do not "fix"
  by guessing).

## Scope notes (do not "fix" these)

- Python defaults may differ from Rust/C in mapping mode (sync vs async
  runtimes); this is documented behavior, not a bug — do not change
  defaults without an explicit task.
- API parity with the Rust crate is not a goal in itself; missing wrappers
  (reader, transfer, uncommitted append) are added by explicit task only.

## Style

- `from __future__ import annotations`, full type hints on public API,
  docstrings state contracts (what raises, what the bytes lifetime is),
  no restating signatures. Match surrounding style exactly; no drive-by
  reformatting.

## Completion report format

Files changed (matching the task manifest); tests added by name with the
runner's `-v` lines quoted; total test count before/after; commands executed
with actual output; deviations from the task, this file, or upstream's
AGENTS.md — explicitly. Claims are independently re-verified; under-claim.
