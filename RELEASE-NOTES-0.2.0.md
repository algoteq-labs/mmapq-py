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

- None beyond the C-level changes. The exception class set is unchanged;
  existing `MmapqError` handlers keep catching.

## New (Python)

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
