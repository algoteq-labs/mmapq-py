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

- `archivable()` now raises where 0.1.0/0.2.0 returned `False`. While the
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

## Fixed (Python)

- **Silent truncation:** batch poll iteration uses the checked C step and
  raises on mid-batch timeout/corruption — at 0.1.0 the unchecked loop
  delivered a truncated message list with no error.
- Export errors are preserved during output cleanup; transfer mapper
  exceptions propagate to the caller instead of being swallowed.
- Reader view borrowing enforced; context exceptions preserved.

## Hardening (Python)

- Closed-handle and closed-runtime rejection, callback-result validation,
  no poller reentry or callback-time close, no duplicate appender
  cleanup, no overlapping batch writers.
