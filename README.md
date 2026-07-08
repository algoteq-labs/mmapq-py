# mmapq

[![License: BSD-2-Clause](https://img.shields.io/badge/License-BSD_2--Clause-blue.svg)](LICENSE)

**Python bindings for [mmapq](https://github.com/anton-anufriev/mmapq)** — a persistent,
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
[mmapq](https://github.com/anton-anufriev/mmapq) repository describes the
full pattern; `doc/feature-matrix.md` gives exact coverage across C, Rust,
and Python.

## Quick start

### Single-message queue

```python
from mmapq import IndexedQueue, NextMove

q = IndexedQueue.create("/dev/shm", "my_queue")

appender = q.create_appender()
appender.append(b"hello world")        # committed atomically

def handler(index, appender_id, data):
    print(f"[{index}] {data!r}")
    return NextMove.ADVANCE

poller = q.create_poller()
poller.poll(handler)
```

### Batch queue (atomic multi-message commit)

```python
from mmapq import IndexedBatchQueue, NextMove

q = IndexedBatchQueue.create("/dev/shm", "events")
appender = q.create_appender()

with appender.start_batch() as batch:      # context manager = transaction
    batch.append(b"event 1")
    batch.append(b"event 2")
    batch.append(b"event 3")
# leaving the block without an exception commits the batch atomically;
# an exception inside the block discards it — the events never become
# visible. This is the command/event rollback semantics in one construct.
```

### Reading and replay (random access)

```python
from mmapq import IndexedReader, export_queue

reader = IndexedReader.create("/dev/shm", "events")
first = reader.read_first()              # bytes | None
last  = reader.read_last()
msg   = reader.read(42)                  # read any committed index
print(reader.last_index())

# Export the durable stream for long-term storage or offline analysis:
export_queue("/dev/shm", "events", "/data/events-2026-07.mmapq")
```

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

    # Transfer (export/import/copy)
    export_queue,           # Export queue to flat stream
    import_queue,           # Import flat stream to queue
    copy_queue,             # Copy queue to new directory
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

The handler passed to `poller.poll()` receives `(batch_index: int, ctx: CData)`:

```python
def handler(batch_index, ctx):
    # ctx is a cffi pointer to BatchReadingContext
    # ctx.message_count — number of messages in the batch
    # ctx.status — BatchEntryStatus

    # Access messages via the iterator:
    from mmapq import ffi, lib
    it = ffi.addressof(ctx.batch_iterator)

    if not lib.batch_iterator_advance(it):
        return NextMove.ADVANCE

    length = lib.batch_iterator_current_length_ffi(it)
    data_ptr = lib.batch_iterator_current_data_ffi(it)
    data = bytes(ffi.buffer(data_ptr, length))

    # data is the first message payload
    return NextMove.ADVANCE
```

### Additional appender methods

`IndexedAppender` also provides:

- **`append_uncommitted(data)`** — writes payload without committing. Invisible to readers,
  discarded by crash recovery. Returns a payload offset for later use with `commit()`.
- **`commit(payload_offset)`** — publishes a previously uncommitted payload. Returns the
  committed index.

`IndexedQueue` also provides:

- **`create_appender_with_id(appender_id)`** — creates an appender with a specific lane ID
  (0–255). Raises `ValueError` if the ID is out of range.

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

BSD 2-Clause — see [LICENSE](LICENSE).

## Platform

Linux only (x86-64, ARM64). Requires naturally aligned lock-free atomics
over `MAP_SHARED` mmap.
