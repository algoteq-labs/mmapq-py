"""IndexedReader and IndexedBatchReader — random access readers."""

from __future__ import annotations

import enum
import weakref
from typing import Optional

from . import _cffi as _cffi_mod
from ._errors import QueueClosedError, QueueCorruptError, check_rc

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib


def _destroy_reader(inner):
    if inner is not None:
        lib.indexed_reader_close(inner)


def _destroy_batch_reader(inner):
    if inner is not None:
        lib.indexed_batch_reader_close(inner)


class EntryStatus(enum.IntEnum):
    """Availability of a queue entry at a given index."""

    AVAILABLE = 0
    """The entry exists and is readable."""
    END_OF_STREAM = 1
    """No entry has been committed at this index yet."""
    ARCHIVED = 2
    """The entry was archived and is no longer accessible."""
    ERROR = 3
    """A mapping timeout or I/O error occurred."""


class IndexedReader:
    """Random-access reader for a single-message indexed queue.

    Reads any committed entry by index without advancing a cursor.
    Thread-confined. One per consumer thread.
    """

    def __init__(self, inner, *, runtime=None):
        self._inner = inner
        self._runtime = runtime
        self._ctx = ffi.new("ReadingContext *")
        self._finalizer = weakref.finalize(self, _destroy_reader, inner)

    @classmethod
    def create(cls, directory: str, queue_name: str, *,
               ring_size: int = 4,
               timeout_ns: int = 10000000000,
               runtime=None) -> IndexedReader:
        """Create or open a random-access reader.

        directory:   path to the queue data directory
        queue_name:  logical queue name
        ring_size:   number of simultaneously mapped regions
        timeout_ns:  region-mapping timeout in nanoseconds

        The queue does not need to exist at creation time — reads on a
        non-existent queue return None (END_OF_STREAM).
        """
        rt = runtime._inner if runtime is not None else ffi.NULL
        ptr = lib.indexed_reader_create(
            directory.encode(), queue_name.encode(),
            ring_size, timeout_ns, rt,
        )
        if not ptr:
            raise RuntimeError("indexed_reader_create returned NULL")
        return cls(ptr, runtime=runtime)

    def read(self, index: int) -> Optional[bytes]:
        """Read the entry at the given index.

        Returns the payload bytes if the entry exists, or None if the
        index is beyond the last committed entry or has been archived.
        Raises QueueCorruptError on corruption, RuntimeError on I/O error.
        """
        if index < 0:
            raise ValueError(f"index must be >= 0, got {index}")
        self._check_open()
        lib.indexed_reader_read(self._inner, index, self._ctx)
        return self._extract()

    def read_first(self) -> Optional[bytes]:
        """Read the first unarchived entry (at start_index)."""
        self._check_open()
        lib.indexed_reader_read_first(self._inner, self._ctx)
        return self._extract()

    def read_last(self) -> Optional[bytes]:
        """Read the last committed entry."""
        self._check_open()
        lib.indexed_reader_read_last(self._inner, self._ctx)
        return self._extract()

    def read_view(self, index: int) -> Optional[memoryview]:
        """Zero-copy view into the queue's mapped memory.

        Valid only until the next read()/read_view()/close() on this
        reader. Do not store it; copy with bytes(view) if you need the
        data beyond the next call.
        """
        if index < 0:
            raise ValueError(f"index must be >= 0, got {index}")
        self._check_open()
        lib.indexed_reader_read(self._inner, index, self._ctx)
        self._check_status()
        if self._ctx.status == EntryStatus.AVAILABLE:
            return memoryview(ffi.buffer(self._ctx.data, self._ctx.length))
        return None

    def entry_status(self, index: int) -> EntryStatus:
        """Check availability of the entry at the given index.

        Lightweight check that does not populate a ReadingContext.
        """
        if index < 0:
            raise ValueError(f"index must be >= 0, got {index}")
        self._check_open()
        return EntryStatus(lib.indexed_reader_entry_status(self._inner, index))

    def last_index(self) -> int:
        """Highest committed index, or -1 if the queue is empty."""
        self._check_open()
        return lib.indexed_reader_last_index(self._inner)

    def start_index(self) -> int:
        """Lowest readable index (0 if queue is not archivable)."""
        self._check_open()
        return lib.indexed_reader_start_index(self._inner)

    def close(self) -> None:
        """Close the reader and release mapped regions. Idempotent."""
        if self._inner is not None:
            self._finalizer.detach()
            _destroy_reader(self._inner)
            self._inner = None
            self._ctx = None

    def _check_open(self):
        if self._inner is None:
            raise QueueClosedError("IndexedReader is closed")

    def _check_status(self):
        status = self._ctx.status
        if status == EntryStatus.END_OF_STREAM:
            return
        if status == EntryStatus.ARCHIVED:
            return
        if status == EntryStatus.AVAILABLE:
            return
        if status == EntryStatus.ERROR:
            raise RuntimeError(
                "indexed_reader_read: ENTRY_ERROR (mapping timeout or I/O failure)")
        raise QueueCorruptError(
            f"indexed_reader_read: unknown status {status}")

    def _extract(self) -> Optional[bytes]:
        self._check_status()
        if self._ctx.status == EntryStatus.AVAILABLE:
            return bytes(ffi.buffer(self._ctx.data, self._ctx.length))
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class IndexedBatchReader:
    """Random-access reader for an indexed batch queue.

    Reads any committed batch by index. Each read() returns a list of
    message payload bytes, or None if no batch exists at the given index.
    Thread-confined. One per consumer thread.
    """

    def __init__(self, inner, *, runtime=None):
        self._inner = inner
        self._runtime = runtime
        self._ctx = ffi.new("BatchReadingContext *")
        self._finalizer = weakref.finalize(self, _destroy_batch_reader, inner)

    @classmethod
    def create(cls, directory: str, queue_name: str, *,
               ring_size: int = 4,
               timeout_ns: int = 10000000000,
               runtime=None) -> IndexedBatchReader:
        """Create or open a batch reader.

        directory:   path to the queue data directory
        queue_name:  logical queue name
        ring_size:   number of simultaneously mapped regions
        timeout_ns:  region-mapping timeout in nanoseconds
        runtime:     optional Runtime for async region mapping (keyword-only)
        """
        rt = runtime._inner if runtime is not None else ffi.NULL
        ptr = lib.indexed_batch_reader_create(
            directory.encode(), queue_name.encode(),
            ring_size, timeout_ns, rt,
        )
        if not ptr:
            raise RuntimeError("indexed_batch_reader_create returned NULL")
        return cls(ptr)

    def read(self, index: int) -> Optional[list[bytes]]:
        """Read the batch at the given index.

        Returns a list of payload bytes (one per message) if the batch
        exists, or None if the index is beyond the last committed batch
        or has been archived.
        """
        self._check_open()
        lib.indexed_batch_reader_read(self._inner, index, self._ctx)
        return self._extract_batch()

    def read_first(self) -> Optional[list[bytes]]:
        """Read the first unarchived batch (at start_index)."""
        self._check_open()
        lib.indexed_batch_reader_read_first(self._inner, self._ctx)
        return self._extract_batch()

    def read_last(self) -> Optional[list[bytes]]:
        """Read the last committed batch."""
        self._check_open()
        lib.indexed_batch_reader_read_last(self._inner, self._ctx)
        return self._extract_batch()

    def entry_status(self, index: int) -> int:
        """Check availability of the batch at the given index.

        Returns a BatchEntryStatus value.
        """
        if index < 0:
            raise ValueError(f"index must be >= 0, got {index}")
        self._check_open()
        return lib.indexed_batch_reader_entry_status(self._inner, index)

    def last_index(self) -> int:
        """Highest committed batch index, or -1 if the queue is empty."""
        self._check_open()
        return lib.indexed_batch_reader_last_index(self._inner)

    def start_index(self) -> int:
        """First unarchived batch index (lowest readable)."""
        self._check_open()
        return lib.indexed_batch_reader_start_index(self._inner)

    def close(self) -> None:
        """Close the batch reader. Idempotent."""
        if self._inner is not None:
            self._finalizer.detach()
            _destroy_batch_reader(self._inner)
            self._inner = None
            self._ctx = None

    def _check_open(self):
        if self._inner is None:
            raise QueueClosedError("IndexedBatchReader is closed")

    def _extract_batch(self) -> Optional[list[bytes]]:
        from .types import BatchEntryStatus
        status = self._ctx.status
        if status == BatchEntryStatus.AVAILABLE:
            messages = []
            it_ptr = ffi.addressof(self._ctx.batch_iterator)
            while lib.batch_iterator_advance(it_ptr):
                length = lib.batch_iterator_current_length_ffi(it_ptr)
                data_ptr = lib.batch_iterator_current_data_ffi(it_ptr)
                messages.append(bytes(ffi.buffer(data_ptr, length)))
            return messages
        elif status in (BatchEntryStatus.END_OF_STREAM,
                        BatchEntryStatus.ARCHIVED):
            return None
        elif status == BatchEntryStatus.ERROR:
            raise RuntimeError(
                "indexed_batch_reader_read: ENTRY_ERROR "
                "(mapping timeout or I/O failure)")
        else:
            raise QueueCorruptError(
                f"indexed_batch_reader_read: unknown status {status}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
