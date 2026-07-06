# mmap-c-py

Python bindings for mmap-c — a low-latency,
memory-mapped, shared-memory indexed queue for inter-process messaging on Linux.

Built with [cffi](https://cffi.readthedocs.io/) in API mode, the same architecture as
the Rust bindings (`mmap-c-rs`): a thin zero-overhead FFI layer over the C library, with
Pythonic RAII via context managers.

Python exposes the **producer/consumer subset plus random-access reading
and queue transfer** — append and poll for both single-message and batch
queues, random-access readers for replay and analysis, and export/import/copy
for durable long-term storage. Archiving and low-level batch construction
remain C/Rust territory. See `doc/feature-matrix.md` in the mmap-c repository
for the full matrix.

## Features

- **Zero-copy** — messages are read directly from mapped memory through the C library
- **Batch queue** — group multiple messages under a single atomic commit with per-batch callback delivery
- **RAII via context managers** — `BatchWriter` auto-rolls back on error when used with `with`
- **Cross-process** — share queues between Python, C, Rust, and Algoteq Java processes on the same machine
- **C-ABI compatible** — identical wire format and protocol as Algoteq's Java implementation (tools4j-derived; NOT compatible with upstream tools4j)
- **Panic-safe callbacks** — Python exceptions in handlers are caught and logged without crashing the process

## Quick Start

### Single-message queue

```python
from mmap_c_py import IndexedQueue, NextMove

queue = IndexedQueue.create("/dev/shm", "my_queue")
appender = queue.create_appender()
poller = queue.create_poller()

# Append a message (committed atomically)
appender.append(b"hello world")

# Poll for the message
def handler(index, appender_id, data):
    print(f"[{index}] {data}")
    return NextMove.ADVANCE

poller.poll(handler)

appender.close()
poller.close()
queue.close()
```

### Batch queue

```python
from mmap_c_py import IndexedBatchQueue, NextMove

queue = IndexedBatchQueue.create("/dev/shm", "my_batch_queue")
appender = queue.create_appender()

# Append a batch with three messages (single atomic commit)
with appender.start_batch() as writer:
    writer.append(b"hello")
    writer.append(b"world")
    writer.append(b"!")
    writer.commit()

# Poll for the batch
poller = queue.create_poller()

def handler(batch_index, ctx):
    print(f"Batch {batch_index}: {ctx.message_count} messages")
    return NextMove.ADVANCE

poller.poll(handler)

poller.close()
appender.close()
queue.close()
```

## Installation

```bash
pip install mmap-c-py
```

Requires **Python 3.10+**, **Linux x86-64**, and **GCC** (for compiling the C extension at install time).

### From source

```bash
git clone --recursive <repo-url>
cd mmap-c-py
pip install -e .
```

## Architecture

The underlying C library stores messages in two file types:

- **Header file** (one per queue): 8-byte atomically-committed entries spaced 512 bytes
  apart (vertical walk layout). Each header encodes an appender ID and a payload offset
  in a single 64-bit CAS word.
- **Payload files** (one per appender): `[int32 length][payload bytes]` framing, with a
  message count prefix for batches.

The vertical walk layout (index N at offset N × 512) ensures consecutive writes by
multiple appenders land on separate cache lines, avoiding false sharing.

Python interacts with these files exclusively through the C library — all header CAS
operations, payload reads, and region mapping happen in C code compiled by cffi.

## API

### Exports

```python
from mmap_c_py import (
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
    from mmap_c_py import ffi, lib
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
# Terminal 1: start the echo server
python examples/batch_echo_server.py /tmp batch_req batch_res

# Terminal 2: run the latency benchmark
python examples/batch_latency_bench.py 100000 10000 64 --external
```

### Internal benchmark (single process)

```bash
python examples/batch_latency_bench.py 100000 10000 64
```

## Performance

The Python layer adds approximately **1–2 µs** per `poll()` call through the cffi callback
trampoline (C → Python → C dispatch). The underlying C library delivers ~300 ns median
round-trip at the C level, but Python's interpreter overhead dominates the hot path.

Expected throughput on isolated cores: **100,000–200,000 ops/sec** (vs 2.8M for C).

This is the right tool when you need:
- Cross-process queue compatibility with existing C/Rust/Algoteq Java deployments
- Python process integration into an existing mmap-c pipeline
- Prototyping or testing with batch queue semantics

For production throughput at scale, use the C or Rust bindings directly.

## Testing

```bash
python -m unittest discover tests
```

## Acknowledgements

mmap-c-py is a Python port of Algoteq's mmap-c,
which was derived from the [tools4j/mmap](https://github.com/tools4j/mmap) Java library.

## License

MIT

## Platform

Linux only. Requires naturally aligned lock-free atomics over `MAP_SHARED`
mmap — guaranteed on x86-64 and ARM64.
