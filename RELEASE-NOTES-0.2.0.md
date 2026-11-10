# mmapq-py 0.2.0 release notes

**Breaking, format-level, and protocol-level changes are documented in the
C library's notes (`doc/RELEASE-NOTES-0.2.0.md` in the mmapq repository,
installed with the C library under `share/doc/mmapq/`) — which are
authoritative for the wire format (schema_version 2: 0.1.0
queues are refused at open with `SchemaMismatchError`; recreate or
export/import to migrate), the removed/replaced C functions, the behavior
changes under unchanged names, and the Java-interop position.** This file
carries only the binding-specific entries.

## Breaking (Python)

- `archivable()` now raises where 0.1.0 returned `False`. While the
  queue's metadata is not determinable yet it raises `FileNotFoundError`
  (`MMAPQ_ENOENT`), `QueueNotReadyError` (`MMAPQ_ENOTREADY`) or
  `MmapqError` (`MMAPQ_ETIMEDOUT`) — all retryable on the same archiver;
  every other failure is terminal. Existing `MmapqError` handlers keep
  catching, since the new class subclasses `MmapqError`.
- `IndexedReader.read` / `read_first` / `read_last` / `read_view` and
  `IndexedBatchReader.read` / `read_first` / `read_last` raise the new
  `ArchivedError(MmapqError, LookupError)` when the index is below the
  queue's start index, carrying it as `.index`; they returned `None`
  before. `None` now means only "not yet written". Code that treated
  `None` as "skip" should catch `ArchivedError` (or `LookupError`).
  `entry_status()` and the pollers are unchanged.
- `IndexedBatchReader.entry_status()` returns `BatchEntryStatus` instead
  of a plain `int`, matching `IndexedReader.entry_status()`. Values and
  equality with the old integers are unchanged (`IntEnum`).

## New (Python)

- New public exception `QueueNotReadyError(MmapqError)`, raised for
  `MMAPQ_ENOTREADY` on every Python surface that can return it (not only
  `archivable()`).
- Uncommitted-message iteration on both queue types: `uncommitted`,
  `uncommitted_batches`.
- Batch lifecycle: `join_completed_open_batch`, `complete_uncommitted`,
  `commit_batch`.
- `mmapq-ids` operator tool (in the C repository's `tools/`).
- `mmapq.__version__`, the package version string.
- `IndexedArchiver.reclaim_files` and `IndexedBatchArchiver.reclaim_files`: delete
  files from an `archivable_files()` listing safely, even against a concurrent undo.
- `IndexedQueue.create` takes keyword-only `region_size`, `ring_size`,
  `max_file_size`, `start_index` and `timeout_ns`, as `IndexedBatchQueue.create` does.

## Fixed (Python)

- **Silent truncation:** batch poll iteration uses the checked C step and
  raises on mid-batch timeout/corruption — at 0.1.0 the unchecked loop
  delivered a truncated message list with no error.
- Export errors are preserved during output cleanup; transfer mapper
  exceptions propagate to the caller instead of being swallowed.
- `archivable_files()` never freed the path strings the C library returned (a
  small leak on every call); it now frees them.
- Reader view borrowing enforced; context exceptions preserved.
- `IndexedReader.appender_id()` after a failed read raises instead of
  reporting the previous read's appender.
- `IndexedArchiver.archivable_files` and `IndexedBatchArchiver.archivable_files`
  returned at most 256 paths without saying that more files qualified; they
  now return the complete listing.
- Building from source failed with GCC 14 (an error by default there): one
  C struct field was declared without the header's `const`. The build now
  also fails on that class of mismatch with older compilers.

## Packaging (Python)

- The package is still published as a source distribution only:
  installing it builds the C extension and needs a C compiler. Wheels are
  planned.
- `cffi>=1.17.1` is now a declared minimum (it was unpinned). Building from
  source needs `setuptools>=77.0.3`, which pip installs automatically in
  its default isolated build.
- The license metadata is the SPDX expression `BSD-3-Clause`.

## Hardening (Python)

- Closed-handle and closed-runtime rejection, callback-result validation,
  no poller reentry or callback-time close, no duplicate appender
  cleanup, no overlapping batch writers.

## Known limitations (Python)

- `BatchWriter.append_remaining_from` needs a source batch iterator that
  the Python API does not expose yet (planned); see its docstring.
- The C library's known limitations apply (C notes, "Known limitations").
