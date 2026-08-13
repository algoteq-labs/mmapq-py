# Implementation Spec: mmapq feature parity

**Audience:** implementing model/engineer with access to all three
repositories, WITHOUT prior design context. All design decisions are made
here. Where this spec and your judgment disagree, the spec wins — flag the
disagreement in your summary. All AGENTS.md rules apply, including the
completion-report format and under-claiming.

**Goal:** raise `mmapq` from the producer/consumer subset to near-parity
with `mmapq-rs`, per `mmapq/doc/feature-matrix.md`. This is wrapper parity
over existing C capability — the sanctioned category. **No C code changes
in this spec.** The C library is used as-is.

**Structure: four phases, landed and verified in order.** Each phase has
its own manifest, tests, and acceptance. Do not start phase N+1 until phase
N's acceptance passes. Report per phase.

---

## Global rules (apply to every phase)

- **Ruling 1 — read results are copies by default.** Every API returning
  message data returns `bytes` (a copy). C flyweights (`ReadingContext`)
  are internal; never expose a raw pointer lifetime to the user by default.
  Where a zero-copy escape hatch is specified, it returns a `memoryview`
  over the mapped memory and its docstring MUST state the validity window
  verbatim as given in that section. No other zero-copy paths.
- **Ruling 2 — no default behavior changes.** Existing factories keep sync
  mapping (NULL runtimes) as the default. Runtime is opt-in (Phase 3).
  Existing signatures gain only keyword-only, defaulted parameters.
- **cdef discipline:** every added cdef is copied from the C header
  signature character-for-character (types, order). cdef drift is silent
  memory corruption. Include the header file/line you copied from as a
  comment above each cdef block.
- **Every new class:** explicit idempotent `close()`; `close()` never
  passes NULL to C; factory/methods on a closed object raise
  `QueueClosedError`; cleanup registered via `weakref.finalize(self, lib.<close_fn>, ptr)` (NOT `__del__`); context-manager support
  (`__enter__`/`__exit__` → close).
- **Every error path** goes through `check_rc` / raises the `_errors.py`
  types. Pointer-returning C calls that yield NULL raise `RuntimeError`
  with the operation name (create-path limitation, documented).
- **Style:** match existing modules exactly (`from __future__ import
  annotations`, type hints, docstrings stating contracts). No
  reformatting of existing code beyond this spec's changes.
- **Tests:** `unittest`, in `tests/test_queue.py` following the
  existing class pattern (`setUp` temp dir, `tearDown` rmtree). A test not
  visible in `-v` runner output does not exist. Every phase's report
  quotes the new `-v` lines and the new total.
- **Docs:** each phase updates `mmapq-py/README.md` (the coverage
  section) and `mmapq/doc/feature-matrix.md` (flip the affected Python
  cells, adjusting the subset paragraph in BOTH places in the final phase).
  Matrix maintenance is mandatory per AGENTS.md.

---

# PHASE 1 — Reader (random access) + Transfer/Export

The highest-value phase: replay and analysis from Python.

## 1.1 cdef additions (`setup.py`)

Copy from `include/indexed_reader.h`:

```c
typedef enum { ... } EntryStatus;            /* copy the real enum body   */
typedef struct { ... } ReadingContext;       /* copy the real struct body */

IndexedReader *indexed_reader_create(
    const char *directory, const char *queue_name,
    int ring_size, uint64_t timeout_ns, Runtime *runtime);
ReadingContext *indexed_reader_read(IndexedReader *r, int64_t index, ReadingContext *ctx);
ReadingContext *indexed_reader_read_first(IndexedReader *r, ReadingContext *ctx);
ReadingContext *indexed_reader_read_last(IndexedReader *r, ReadingContext *ctx);
int64_t indexed_reader_last_index(IndexedReader *r);
int64_t indexed_reader_start_index(IndexedReader *r);
EntryStatus indexed_reader_entry_status(IndexedReader *r, int64_t index);
void indexed_reader_close(IndexedReader *r);
```

(The `...` bodies above are placeholders for YOU to copy from the header —
do not paste literal ellipses. Same rule for every cdef in this spec.)

From `include/queue_transfer.h`, the five functions
(`queue_transfer_export`, `_export_chunked`, `_import`,
`_import_batch_stream`, `_copy`) with their exact signatures, plus
`QueueFilterFn` typedef. If `_export_chunked`'s callback types are awkward
for cffi, bind only `export`, `import`, `import_batch_stream`, and `copy`,
and FLAG the omission — do not improvise a callback bridge.

Also add the batch reader from `include/indexed_batch_reader.h`
(create/read/first/last/close + `BatchReadingContext`), mirroring the
single reader.

## 1.2 New module: `reader.py`

```python
class EntryStatus(IntEnum):   # values copied from the C enum
    ...

class IndexedReader:
    @classmethod
    def create(cls, directory: str, queue_name: str, *,
               ring_size: int = <same default the other factories use>,
               timeout_ns: int = <same default>) -> "IndexedReader": ...
    def read(self, index: int) -> bytes | None: ...
    def read_first(self) -> bytes | None: ...
    def read_last(self) -> bytes | None: ...
    def read_view(self, index: int) -> memoryview | None: ...
    def entry_status(self, index: int) -> EntryStatus: ...
    def last_index(self) -> int: ...
    def start_index(self) -> int: ...
    def close(self) -> None: ...
```

Implementation notes:
- The reader owns ONE `ffi.new("ReadingContext *")` allocated at
  construction and reused across calls (the C contract: caller owns ctx).
- `read*()` returns `bytes(ffi.buffer(ctx.data, ctx.length))` when status
  is AVAILABLE; `None` when the status is the not-present/end value; raises
  `QueueCorruptError`/`RuntimeError` mapping for error statuses if the
  enum has any. Consult the enum you copied; map every value explicitly —
  no default-else silently returning None.
- `read_view()` is the SOLE zero-copy escape hatch. Docstring, verbatim:
  "Zero-copy view into the queue's mapped memory. Valid only until the
  next read()/read_view()/close() on this reader. Do not store it; copy
  with bytes(view) if you need the data beyond the next call."
- Runtime parameter intentionally absent in Phase 1 (sync mapping);
  Phase 3 adds it.
- `IndexedBatchReader` in `batch_reader.py`, same shape; its `read(index)`
  returns `list[bytes]` (all messages of the batch) and `read_view` is
  NOT provided for batch (copy-only) — flag if this feels asymmetric; it
  is intentional (batch iteration over a flyweight is where lifetime bugs
  live).

## 1.3 New module: `transfer.py`

Module-level functions (no class needed):

```python
def export_queue(directory: str, queue_name: str, dest_path: str, *,
                 from_index: int = 0, to_index: int | None = None) -> int: ...
def import_queue(src_path: str, directory: str, queue_name: str) -> int: ...
def copy_queue(src_dir: str, src_name: str, dst_dir: str, dst_name: str) -> int: ...
```

Map parameters onto the C signatures you copied; if the C functions take
options this sketch omits, expose them as keyword-only with C defaults —
do not invent semantics, mirror the header docs. Returns are the C int64
results passed through `check_rc` (negative → raise; non-negative =
count/bytes per the header doc, returned to the caller).

## 1.4 Exports and tests

`__init__.py` exports: `IndexedReader`, `IndexedBatchReader`,
`EntryStatus`, `export_queue`, `import_queue`, `copy_queue`.

Tests (new class `TestReader`, `TestTransfer`):
1. `test_reader_read_by_index` — append 5, read index 2, exact bytes.
2. `test_reader_first_last_and_indices` — first/last content;
   `last_index == 4`; `start_index == 0`.
3. `test_reader_missing_index_returns_none` — read index 99 → None (or
   the mapped status behavior you implemented — assert exactly it).
4. `test_reader_view_lifetime` — `read_view(0)` equals `read(0)` bytes;
   after `read(1)`, `bytes(old_view)` no longer equals message 0 OR
   equals it (undefined) — the assertion is only that the copy taken
   BEFORE the second read is stable. (Pins the documented contract
   without asserting undefined behavior.)
5. `test_reader_closed_raises` — close, then read → `QueueClosedError`.
6. `test_export_import_roundtrip` — write 10 messages, export to a temp
   file, import into a fresh queue dir, poll all 10, byte-equal.
7. `test_batch_reader_roundtrip` — batch of 3, read index 0 →
   list of 3 exact payloads.

## 1.5 Phase 1 acceptance

- Runner shows the 7 names; total = previous + 7.
- `python3 -c "from mmapq import IndexedReader, export_queue"` works.
- README coverage section + feature matrix Python cells for reader and
  transfer flipped (matrix: ✓ for read-at-index/first-last/status/
  last_index, batch reader; ✓ for export/import/copy; chunked per 1.1).
- Mirror untouched; C and Rust suites still green (nothing changed, run
  anyway).

---

# PHASE 2 — Producer completion: uncommitted append, commit, create_with_id

## 2.1 cdefs

From `include/indexed_appender.h`:
`indexed_appender_create_with_id` (exact signature),
`indexed_appender_append_uncommitted`, `indexed_appender_commit`.

## 2.2 API (`single_appender.py`, `single_queue.py`)

- `IndexedQueue.create_appender_with_id(self, appender_id: int)` →
  validates `0 <= appender_id < 256` (raise `ValueError` otherwise) before
  the FFI call; returns `IndexedAppender`.
- `IndexedAppender.append_uncommitted(self, data: bytes) -> int` — same
  input validation as `append` (non-empty, length bounds); returns the
  payload offset from C (via `check_rc`).
- `IndexedAppender.commit(self, payload_offset: int) -> int` — returns the
  committed index (via `check_rc`).
- Docstring on both, verbatim sentence: "An uncommitted payload is
  invisible to all readers and is discarded by crash recovery; only
  commit() publishes it."

## 2.3 Tests

1. `test_append_uncommitted_then_commit` — uncommitted write; poller sees
   END_OF_STREAM; commit; poller sees the message.
2. `test_uncommitted_discarded_on_restart` — append committed "a",
   append_uncommitted "b", close appender WITHOUT commit, reopen appender,
   append "c"; poll all → exactly [a, c].
3. `test_create_appender_with_id` — create with id 7; `appender.id() == 7`;
   append/poll round-trip.
4. `test_create_appender_with_bad_id_raises` — id 256 → `ValueError`
   before any FFI call.

## 2.4 Acceptance

Names in `-v`; total +4; matrix cells flipped (uncommitted+commit ✓,
with_id ✓); README updated.

---

# PHASE 3 — Runtime exposure + full queue configuration

## 3.1 cdefs

From `include/runtime.h`: `runtime_region_mapper` (already declared —
verify), `runtime_create` (exact signature — copy its config parameters
from the header), `runtime_destroy`. If `runtime_create`'s config is a
struct or many scalars, bind exactly what the header defines; do not
simplify.

From `include/indexed_queue.h` / `indexed_batch_queue.h`: the FULL
(non-`_default`) create functions if not already in the cdef (they were
added for `archivable`; verify and extend to all parameters).

## 3.2 API

New module `runtime.py`:

```python
class Runtime:
    @classmethod
    def region_mapper(cls) -> "Runtime":
        """Process-wide shared mapper runtime (async region mapping)."""
    # expose runtime_create only if its parameters bind cleanly; otherwise
    # region_mapper() alone is Phase 3 scope — FLAG the decision either way
    def close(self) -> None: ...   # runtime_destroy; idempotent; finalize-backed
```

`IndexedQueue.create` / `IndexedBatchQueue.create` gain keyword-only:
`region_size`, `ring_size`, `regions_to_map_ahead`, `max_file_size`,
`start_index`, `timeout_ns` (defaults = the C `_default` values — read
them from the C source and cite file:line in a comment), and
`runtime: Runtime | None = None`. `None` → `ffi.NULL` → sync mapping —
the existing default, unchanged (Ruling 2). When a Runtime is passed, the
queue object stores a reference to it (`self._runtime = runtime`) so
Python GC cannot collect the Runtime while the queue lives — this is the
Python twin of the Rust UAF fix; participants created from the queue also
store `self._runtime`. Document in each factory docstring: "The queue and
its participants hold a reference to the runtime; it will not be
finalized while they are alive."

`IndexedReader.create` gains the same `runtime` keyword.

README gets a short "Sync vs async mapping" subsection: default is sync
(mmap on the hot path at region rolls); pass
`runtime=Runtime.region_mapper()` for background mapping; matches the
C/Rust default behavior difference documented in the feature matrix.

## 3.3 Tests

1. `test_runtime_mapper_roundtrip` — create queue with
   `runtime=Runtime.region_mapper()`, append/poll 100 messages.
2. `test_runtime_kept_alive_by_queue` — create Runtime, create queue with
   it, `del` the local runtime reference, `gc.collect()`, append/poll
   still works (pins the reference-holding).
3. `test_custom_config_region_size` — create with a non-default
   region_size (a valid power of two), append/poll; then reopen with
   defaults and confirm reads still work (disk-wins reconciliation).

## 3.4 Acceptance

Names in `-v`; total +3; matrix cells (async mapping ◐→✓ noting opt-in,
custom config ◐→✓); README subsection present; Ruling 2 verified —
`grep` shows existing tests unmodified and still passing (defaults
unchanged).

---

# PHASE 4 — Archiver + callback/lifecycle hygiene

## 4.1 Archiver

cdefs from `include/indexed_archiver.h` (create, `archivable`,
`archive_up_to_index` — int return, `archivable_files`, `start_index`,
`start_offset`, close; copy signatures exactly; `print_summary` optional —
bind it, it writes to stdout, fine from Python). New module `archiver.py`:

```python
class IndexedArchiver:
    @classmethod
    def create(cls, directory, queue_name, *, ring_size=..., timeout_ns=..., runtime=None): ...
    def archivable(self) -> bool: ...
    def archive_up_to_index(self, index: int) -> None:   # check_rc; EINVAL → ValueError
    def archivable_files(self) -> list[str]: ...          # mirror the C return shape;
                                                          # if it's a callback/array API, bind faithfully and FLAG shape
    def start_index(self) -> int: ...
    def close(self) -> None: ...
```

Batch archiver: bind create/close only (parity with Rust — the matrix
documents this asymmetry; do not exceed it).

Tests: `test_archiver_on_archivable_queue` (create queue
`archivable=True`, archiver.archivable() is True, archive_up_to_index(0)
succeeds); `test_archiver_rejects_non_archivable` (default queue →
`ValueError` from EINVAL, pins the C conversion from Python).

## 4.2 Hygiene riders (same files, close the review P1s)

1. **Per-poller handler registration.** Replace the module-level
   `_lock + _handlers dict + per-poll register/unregister` in
   `single_poller.py` and `batch_poller.py`: each poller instance creates
   `self._handle = ffi.new_handle(self)` once; `poll(handler)` stores
   `self._current_handler = handler` and passes `self._handle` as
   user_data; the trampoline does `ffi.from_handle(user_data)._current_handler(...)`.
   No lock, no dict, no allocation per poll. Behavior identical; all
   existing poller tests must pass unmodified.
2. **`error=` on both trampolines.** `@ffi.callback(..., error=<STOP value>)`
   so a handler that raises causes the poll to STOP rather than silently
   ADVANCE; the trampoline additionally records the exception on the
   poller and `poll()` re-raises it after the C call returns (pattern:
   `self._pending_exc`; re-raise and clear). Test:
   `test_handler_exception_propagates_and_stops` — handler raises on
   message 2 of 5; poll raises that exception; a subsequent poll resumes
   at message 2 (was not advanced past).
3. **`weakref.finalize` sweep.** Every wrapper class (existing and new)
   uses finalize, not `__del__`; `close()` detaches the finalizer
   (`self._finalizer.detach()` or equivalent) to avoid double-close.
   Test: `test_close_idempotent_all_types` — create one of each public
   type, close twice each, no exception.

## 4.3 Acceptance

Names in `-v`; totals updated; matrix archiver cells flipped; the
**subset paragraph** in the Python README and the matrix preamble
REWRITTEN to describe the new near-parity state (remaining gaps: batch
archiver ops, chunked export if flagged in 1.1 — list them explicitly);
all pre-existing tests pass unmodified (the hygiene riders change no
public behavior).

---

## Final acceptance (after Phase 4)

1. Full suite: previous total + ~16 new tests, all named in `-v` output.
2. `python3 -c "import mmapq; print(sorted(mmapq.__all__))"`
   output included in the report.
3. C suite and Rust suite green (unchanged; run anyway).
4. Feature matrix and both README coverage sections consistent with each
   other — quote the updated Python column.
5. Report per phase, per the AGENTS.md format: files changed, tests by
   name with runner lines, counts before/after, every FLAG raised in the
   phases above answered explicitly, deviations called out. Under-claim.

## Non-goals (entire spec)

- No C changes; no Rust changes; no mirror changes.
- No new zero-copy paths beyond `IndexedReader.read_view`.
- No default-behavior changes to existing factories (Ruling 2).
- No batch mutable-batch low-level API (`append_reserved`,
  `append_remaining_from`) — out of scope, matrix keeps them —.
- No packaging changes (MANIFEST/vendoring is a separate task).
- No async/await wrappers, no threads, no "pythonic" renaming of C
  concepts. The API mirrors the C/Rust vocabulary.
