# mmapq

[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD_3--Clause-blue.svg)](LICENSE)

**Python bindings for [mmapq](https://github.com/algoteq-labs/mmapq)** — a persistent,
indexed, replayable inter-process queue backed by memory-mapped files. No
broker, no daemon; the queue is just files.

## Why Python for a sub-microsecond queue?

Not for the hot path — for the *read* side. The pattern mmapq is built for
fast producers (C or Rust: market data, sequenced commands, sensor and
telemetry streams) on one side of a persistent, indexed log, and lets any
number of consumers read that log independently. Python is the natural
language for the consumer: replay, analysis, notebooks, dashboards,
projections into pandas — over the exact same queue files the fast
producers write, with no serialization bridge, no message broker, and no
copy out of shared memory beyond the one `bytes()` you ask for.

Because the queue is indexed and replayable, the Python side gets the
capability the whole architecture exists for: **replay is the product.**
Copy a queue directory from production to a laptop and re-read it from index
zero — the same operation that drives recovery and replication is also how
you debug, backtest, and audit. "What did command K do" is a single indexed
read from a Python script. `doc/event-sourcing-guide.md` in the
[mmapq](https://github.com/algoteq-labs/mmapq) repository describes the
full pattern; `doc/feature-matrix.md` gives exact coverage across C, Rust,
and Python.

## Quick start

Use a directory on a persistent filesystem. The examples create `./data`;
`/dev/shm` is volatile memory and `/tmp` may be cleared on reboot. Both are
useful for disposable tests and benchmarks, not retained queue history.

Commit publishes messages to readers; it does not flush them to stable storage.
mmapq relies on OS writeback and does not guarantee durability through power
loss. Consumer positions are application state: save the last processed index
and seek explicitly when resuming. Export files also need an appropriate
storage and backup policy.

### Single-message queue

```python
from pathlib import Path
from mmapq import IndexedQueue, NextMove

Path("./data").mkdir(exist_ok=True)

def handler(index, appender_id, data):
    print(f"[{index}] {data!r}")
    return NextMove.ADVANCE

with IndexedQueue.create("./data", "my_queue") as queue:
    with queue.create_appender() as appender:
        index = appender.append(b"hello world")
    with queue.create_poller() as poller:
        poller.move_to_index(index)
        poller.poll(handler)
```

### Batch queue (atomic multi-message commit)

```python
from pathlib import Path
from mmapq import IndexedBatchQueue

Path("./data").mkdir(exist_ok=True)
with IndexedBatchQueue.create("./data", "events") as queue:
    with queue.create_appender() as appender:
        with appender.start_batch() as batch:
            batch.append(b"event 1")
            batch.append(b"event 2")
            batch.append(b"event 3")
            index = batch.commit()
    with queue.create_reader() as reader:
        assert reader.read(index) == [b"event 1", b"event 2", b"event 3"]
```

Exiting a batch block without `commit()` discards that batch, including when
an exception leaves the block before commit. An exception after a successful
commit does not undo published messages. Use one active batch per appender;
the writer cannot be reused after commit, rollback, or appender close.

### Reading and replay (random access)

Reopen the batch queue written above, using the matching batch reader and
export function:

```python
from mmapq import IndexedBatchReader, export_batch_queue

with IndexedBatchReader.create("./data", "events") as reader:
    first = reader.read_first()
    last = reader.read_last()
    assert last == [b"event 1", b"event 2", b"event 3"]
    print(reader.last_index())

export_batch_queue("./data", "events", "./data/events.mmapq")
```

For single-message queues use `IndexedReader` and `export_queue` instead.

## A note on validation and safety

The Python extension compiles the C library with `-DNDEBUG`, so it runs the
*release* build with internal asserts stripped. Input validation therefore
happens in the Python layer and is mandatory, not defensive decoration:
empty appends, out-of-range indices, and closed-handle use are checked
before the FFI call and raise `ValueError` / `QueueClosedError`. Corruption
detected on open or during a poll raises `QueueCorruptError` (distinct from
a transient timeout) rather than returning silently. Every wrapper object
supports explicit `close()`, is a context manager, and is finalized safely
at interpreter shutdown.

The raw cffi `FFI`/`lib` objects are deliberately **not** exported as
`mmapq.ffi` / `mmapq.lib`. Every C symbol reachable through them has no
argument checking — one bad call segfaults the interpreter. If you need
low-level access (e.g. iterator access), reach the raw objects only through
the documented escape hatches, and only when you know what you are doing:

```python
from mmapq import unsafe_ffi, unsafe_lib
ffi = unsafe_ffi()   # cffi FFI object
lib = unsafe_lib()   # loaded C library
```

## Error messages

Exception messages are the single source of truth in the C library
(`src/mmapq_strerror.c`): the Python layer maps return codes to exception
*types* only (e.g. `QueueCorruptError`), and the message text comes from
`mmapq_strerror` via cffi. This guarantees the text in Python matches C
by construction and never drifts independently.

| Return code | Exception type |
|---|---:|
| `MMAPQ_ETIMEDOUT` (-1) | `MmapqError` |
| `MMAPQ_EINVAL` (-2) | `ValueError` |
| `MMAPQ_ECLOSED` (-3) | `QueueClosedError` |
| `MMAPQ_ECORRUPT` (-4) | `QueueCorruptError` |
| `MMAPQ_ESCHEMA` (-5) | `SchemaMismatchError` |
| `MMAPQ_EEXIST` (-6) | `QueueExistsError` |
| `MMAPQ_ENOENT` (-7) | `FileNotFoundError` |
| `MMAPQ_EIDCONFLICT` (-8) | `AppenderIdConflictError` |
| `MMAPQ_ENOMEM` (-9) | `MmapOutOfMemoryError` |
| `MMAPQ_EIO` (-10) | `MmapqError` |
| `MMAPQ_ENOTREADY` (-11) | `MmapqError` |

## Installation

```bash
pip install mmapq
```

### From source

```bash
pip install -e .        # builds the cffi extension against the C sources
```

## API

### Exports

```python
from mmapq import (
    # Shared types
    NextMove,               # Handler return: ADVANCE, STOP, RETREAT
    PollResult,             # Poll result: END_OF_STREAM, ADVANCED, etc.
    BatchPollResult,        # Alias for PollResult
    BatchEntryStatus,       # Batch availability
    EntryStatus,            # Entry availability (reader)

    # Single-message queue
    IndexedQueue,           # Queue factory
    IndexedAppender,        # Producer
    IndexedPoller,          # Sequential consumer

    # Batch queue
    IndexedBatchQueue,      # Queue factory
    IndexedBatchPoller,     # Sequential batch consumer
    IndexedBatchAppender,   # Batch producer
    BatchWriter,            # In-progress batch (context manager)

    # Random-access readers
    IndexedReader,          # Single-message reader
    IndexedBatchReader,     # Batch reader

    # Exceptions
    MmapqError,             # Base exception
    QueueClosedError,       # Closed handle
    QueueCorruptError,      # On-disk corruption
    SchemaMismatchError,    # Schema/type mismatch
    QueueExistsError,       # Destination already exists
    AppenderIdConflictError,# Appender ID conflict
    MmapOutOfMemoryError,   # Allocation failure

    # Runtime
    Runtime,                # Async region mapper

    # Archiver
    IndexedArchiver,        # Start-index management (single-message)

    # Transfer (export/import/copy)
    export_queue,           # Export queue to flat stream
    export_batch_queue,     # Export batch queue to flat stream
    import_queue,           # Import flat stream to queue
    import_batch_stream,    # Import batch stream to flat queue
    import_batch_queue,     # Import batch stream to batch queue
    import_flat_to_batch,   # Import flat stream to batch queue
    copy_queue,             # Copy queue to new directory
    copy_batch_queue,       # Copy batch queue to new directory
    copy_normal_to_batch,   # Copy flat queue as batch queue
    copy_batch_to_normal,   # Copy batch queue as flat queue
)
```

### Single-message handler callback

The handler passed to `poller.poll()` receives `(index: int, appender_id: int, data: bytes)`:

```python
def handler(index, appender_id, data):
    print(f"[{index}] {data}")
    return NextMove.ADVANCE
```

### Batch handler callback

The handler passed to `poller.poll()` receives `(batch_index: int, messages: list[bytes])`:

```python
def handler(batch_index, messages):
    # messages is a list of payload bytes, one per message in the batch
    for msg in messages:
        print(f"[{batch_index}] {msg}")
    return NextMove.ADVANCE
```

### Additional appender methods

`IndexedAppender` (single-message) also provides:

- **`append_uncommitted(data)`** — writes payload without committing. Invisible to readers,
  discarded by crash recovery. Returns a payload offset for later use with `commit()`.
- **`commit(payload_offset)`** — publishes a previously uncommitted payload. Returns the
  committed index.

`IndexedQueue` (single-message) also provides:

- **`create_appender_with_id(appender_id)`** — creates an appender with a specific lane ID
  (0–255). Raises `ValueError` if the ID is out of range.

`IndexedBatchQueue` (batch) also provides:

- **`create_appender_with_id(appender_id)`** — creates a batch appender with a specific lane ID
  (0–255). Raises `ValueError` if the ID is out of range.
- **`create(directory, queue_name, *, region_size=..., ring_size=..., max_file_size=..., start_index=..., timeout_ns=..., archivable=..., runtime=...)`**
  — full-configuration create. All params match the C library's `INDEXED_QUEUE_DEFAULT_*` constants.
  Existing queues ignore sizing params (on-disk config is authoritative).

`IndexedBatchAppender` (batch) also provides:

- **`id()`** — returns this appender's lane ID (0–255).

### Sync vs async mapping

By default mapping happens synchronously on the hot path. For async background
region mapping, pass a `Runtime`:

```python
from mmapq import Runtime, IndexedQueue

rt = Runtime.region_mapper()
q = IndexedQueue.create("./data", "rt_queue", runtime=rt)

# Runtime is kept alive by the queue — safe to drop the local reference
del rt
```

### Archiving

An archivable queue (`archivable=True`) can advance its start index so old
entries become inaccessible and their backing files can be safely deleted:

```python
from mmapq import IndexedArchiver, IndexedQueue

q = IndexedQueue.create("./data", "arch_q", archivable=True)
# ... produce messages ...

arch = IndexedArchiver.create("./data", "arch_q")
arch.archive_up_to_index(1000)          # archive entries < 1000
files = arch.archivable_files()         # paths safe to delete
for path in files:
    # delete, compress, or move them
    pass
```

### Transfer (export/import/copy)

The library provides 10 transfer functions across single-message and batch
queues:

```python
from mmapq import (
    export_queue, export_batch_queue,
    import_queue, import_batch_stream, import_batch_queue,
    import_flat_to_batch,
    copy_queue, copy_batch_queue,
    copy_normal_to_batch, copy_batch_to_normal,
)

# Export a queue to a portable flat file (single-message)
n = export_queue("./data", "src", "./data/export.bin")

# Import flat file into a new queue (allow_append=True appends to existing)
n = import_queue("./data/export.bin", "./data", "restored")
n = import_queue("./data/export.bin", "./data", "restored", allow_append=True)

# Copy queue files directly (avoids intermediate export file)
n = copy_queue("./data", "src", "./data", "copy")

# Preserve index space (start_index, appender IDs)
n = export_queue("./data", "src", "./data/preserve.bin", preserve=True)
n = import_queue("./data/preserve.bin", "./data", "preserved", preserve=True)

# Batch queue transfers
n = export_batch_queue("./data", "batch_src", "./data/batch_export.bin")
n = import_batch_queue("./data/batch_export.bin", "./data", "batch_dst")
n = import_batch_stream("./data/batch_export.bin", "./data", "flat_stream")

# Cross-type conversions (flat to batch, batch to flat)
n = import_flat_to_batch("./data/export.bin", "./data", "batch_queue")
n = copy_normal_to_batch("./data", "flat_src", "./data", "batch_dst")
n = copy_batch_to_normal("./data", "batch_src", "./data", "flat_dst")

# All copy/import functions accept:
#   allow_append=True    — append to existing queue instead of refusing
#   preserve=True        — keep original index space from source
#   mapper=callable      — map source appender IDs to destination IDs
#   on_conflict=<enum>   — ConflictBehavior.AUTO or .ABORT
```

## Examples

### Batch echo server

```bash
rm -rf /tmp/request /tmp/response
python3 examples/batch_echo_server.py /tmp request response &
python3 examples/batch_latency_bench.py 100000 10000 64 --external
```

### Internal benchmark (single process)

```bash
rm -rf /tmp/request /tmp/response
python3 examples/batch_latency_bench.py 100000 10000 64
```

## Performance

The Python layer adds roughly **1–2 µs** per `poll()` call through the cffi
callback trampoline (C → Python → C dispatch); the C library itself delivers
~300 ns median round-trip, so the interpreter dominates the hot path.
Expected throughput on isolated cores is **100,000–200,000 ops/sec** against
~2.8M for C. This is by design and not the point: Python is the consumer and
analysis language here, not the low-latency producer.

Reach for the Python bindings when you want:

- A Python consumer, projection, or analysis process reading a log that C or
  Rust producers write — the replay/notebook/dashboard side of an
  event-sourced pipeline.
- Cross-process compatibility with existing C, Rust, or Algoteq Java
  deployments sharing a queue.
- Replay, export, and offline forensics over recorded queues.
- Prototyping and testing with the real wire format.

For low-latency *production* on the hot path, produce from the C or Rust
bindings; let Python read.

## Compatibility

The wire format is identical to the C library and the Rust bindings, and
compatible with Algoteq's Java implementation (tools4j-derived; **not**
compatible with upstream tools4j — the formats have diverged). Batch queues
are a C-ecosystem feature and are not read by Java.

## Stability

The wire format and feature set are **frozen**: queue files written today
remain readable by every future version, and the API grows only to complete
parity with the Algoteq Java reference. The Python bindings expose the
producer/consumer and replay surface; some low-level batch construction
primitives remain C/Rust only (see `doc/feature-matrix.md`).

## Testing

```bash
python3 -m unittest discover tests
```

## Acknowledgements

`mmapq-py` wraps Algoteq's mmapq, originally derived from the
[tools4j/mmap](https://github.com/tools4j/mmap) Java library. The
event-sourcing architecture follows
[tools4j/event-sourcing](https://github.com/tools4j/event-sourcing) and
[elara](https://github.com/tools4j/elara).

## License

BSD 3-Clause — see [LICENSE](LICENSE).

## Platform

Linux only (x86-64; ARM64 untested). Requires naturally aligned lock-free
atomics over `MAP_SHARED` mmap.
## Upcoming hardening compatibility changes

Transfer mapper exceptions now stop the transfer and re-raise the original
exception after C cleanup, rather than silently assigning another appender ID.
Transfers are not atomic: an already committed prefix may remain after failure.
Package versions are unchanged pending release.

`IndexedReader.read_view()` returns a read-only, zero-copy `memoryview` that
keeps the reader alive. Reads, index queries, and `close()` raise `BufferError`
while any exported view or alias remains. Release every view with `release()`
or a memoryview context manager before advancing or closing the reader:

```python
with reader.read_view(0) as view:  # when entry 0 exists
    payload = bytes(view)        # optional owned copy
reader.close()                  # succeeds after all views/aliases release
```

Releasing the original view does not release surviving slices, casts, or buffer
consumers. A failed `close()` leaves the reader open, including on reader context
manager exit. If the block is already propagating an exception, the reader's
context manager suppresses only the cleanup `BufferError`, preserving that
original exception. The reader remains open with its finalizer armed; release
all views and close explicitly, or allow garbage collection once all references
(including retained tracebacks) are gone. Normal context exit still raises
`BufferError` for an outstanding borrow.

For nested reader/view contexts, release buffer consumers before leaving the
view context. For example, a retained `numpy.frombuffer(view)` array can prevent
`view.release()` from succeeding. Python's memoryview context exit may then raise
`BufferError` and mask a processing exception; the reader cannot suppress that
inner cleanup failure. Surviving view aliases also keep the reader borrowed.

Ordinary `read()` continues to return owned bytes. Raw FFI access
and external modification/truncation of committed files remain caller obligations.

Commit publishes data, but does not guarantee power-loss durability. See the
[C persistence contract](https://github.com/algoteq-labs/mmapq/blob/main/doc/persistence-contract.md) for restart,
checkpoint, filesystem, and archival limitations.
