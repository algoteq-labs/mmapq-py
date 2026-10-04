"""IndexedReader and IndexedBatchReader — random access readers."""

from __future__ import annotations

import weakref
from functools import wraps
from threading import Lock
from typing import Optional

from . import _cffi as _cffi_mod
from ._borrowed_buffer import make_view
from ._cdata import require_cdata, require_open
from ._errors import MmapqError, QueueClosedError, QueueCorruptError, check_rc
from .types import EntryStatus  # noqa: F401  (re-exported for compatibility)

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib


def _destroy_reader(inner):
    if inner is not None:
        lib.mmapq_indexed_reader_close(inner)


def _destroy_batch_reader(inner):
    if inner is not None:
        lib.mmapq_indexed_batch_reader_close(inner)


def _reader_operation(method):
    @wraps(method)
    def guarded(self, *args, **kwargs):
        # CFFI releases the GIL. Keep the borrow check and mapping operation
        # indivisible relative to another read/close, including reentrant calls.
        if not self._operation_lock.acquire(blocking=False):
            raise BufferError("reader operation is already active")
        try:
            lease = self._view_lease() if self._view_lease is not None else None
            if lease is not None and lease.active:
                raise BufferError("release all reader views before reading or closing")
            return method(self, *args, **kwargs)
        finally:
            self._operation_lock.release()
    return guarded


class IndexedReader:
    """Random-access reader for a single-message indexed queue.

    Reads any committed entry by index without advancing a cursor.
    Thread-confined. One per consumer thread.
    """

    def __init__(self, inner, *, runtime=None):
        require_cdata(inner, "struct MmapqIndexedReader *", "IndexedReader")
        self._inner = inner
        self._runtime = runtime
        self._ctx = ffi.new("MmapqReadingContext *")
        self._last_read_valid = False
        self._operation_lock = Lock()
        self._view_lease = None
        self._finalizer = weakref.finalize(self, _destroy_reader, inner)
        # finalize's default exit hook ignores strong references. A view held
        # by a later atexit callback must still keep its mapping alive.
        self._finalizer.atexit = False

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
        rt = require_open(runtime._inner) if runtime is not None else ffi.NULL
        ptr = ffi.new("MmapqIndexedReader **")
        rc = lib.mmapq_indexed_reader_create(
            directory.encode(), queue_name.encode(),
            ring_size, timeout_ns, rt, ptr,
        )
        check_rc(rc, "mmapq_indexed_reader_create")
        return cls(ptr[0], runtime=runtime)

    @_reader_operation
    def read(self, index: int) -> Optional[bytes]:
        """Read the entry at the given index.

        Returns the payload bytes if the entry exists, or None if the
        index is beyond the last committed entry or has been archived.
        Raises QueueCorruptError on corruption, RuntimeError on I/O error.
        """
        if index < 0:
            raise ValueError(f"index must be >= 0, got {index}")
        self._check_open()
        lib.mmapq_indexed_reader_read(self._inner, index, self._ctx)
        return self._extract()

    @_reader_operation
    def read_first(self) -> Optional[bytes]:
        """Read the first unarchived entry (at start_index)."""
        self._check_open()
        lib.mmapq_indexed_reader_read_first(self._inner, self._ctx)
        return self._extract()

    @_reader_operation
    def read_last(self) -> Optional[bytes]:
        """Read the last committed entry."""
        self._check_open()
        lib.mmapq_indexed_reader_read_last(self._inner, self._ctx)
        return self._extract()

    @_reader_operation
    def read_view(self, index: int) -> Optional[memoryview]:
        """Zero-copy view into the queue's mapped memory.

        The read-only view keeps this reader alive. Reads, index queries,
        and close raise BufferError until this view and all aliases are
        released. Use view.release() or a memoryview context manager.
        Copy with bytes(view) when independent ownership is needed.
        """
        if index < 0:
            raise ValueError(f"index must be >= 0, got {index}")
        self._check_open()
        lib.mmapq_indexed_reader_read(self._inner, index, self._ctx)
        self._check_status()
        if self._ctx.status == EntryStatus.AVAILABLE:
            self._last_read_valid = True
            view = make_view(ffi.buffer(self._ctx.data, self._ctx.length), self)
            self._view_lease = weakref.ref(view.obj)
            return view
        self._last_read_valid = False
        return None

    @_reader_operation
    def entry_status(self, index: int) -> EntryStatus:
        """Check availability of the entry at the given index.

        Lightweight check that does not populate a MmapqReadingContext.
        """
        if index < 0:
            raise ValueError(f"index must be >= 0, got {index}")
        self._check_open()
        return EntryStatus(lib.mmapq_indexed_reader_entry_status(self._inner, index))

    @_reader_operation
    def last_index(self) -> int:
        """Highest committed index, or -1 if the queue is empty."""
        self._check_open()
        return lib.mmapq_indexed_reader_last_index(self._inner)

    @_reader_operation
    def start_index(self) -> int:
        """Lowest readable index (0 if queue is not archivable)."""
        self._check_open()
        return lib.mmapq_indexed_reader_start_index(self._inner)

    @_reader_operation
    def close(self) -> None:
        """Close mapped regions; raise BufferError while views exist. Idempotent."""
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
            rc = lib.mmapq_indexed_reader_last_error(self._inner)
            if rc != 0:
                check_rc(rc, "mmapq_indexed_reader_read")
            raise MmapqError(
                "mmapq_indexed_reader_read: MMAPQ_ENTRY_ERROR (mapping timeout or I/O failure)")
        raise QueueCorruptError(
            f"mmapq_indexed_reader_read: unknown status {status}")

    def appender_id(self) -> int:
        """Return the appender ID of the last entry read.

        Valid only after a ``read()``, ``read_first()``, or ``read_last()``
        call whose result was not None.

        Raises:
            QueueClosedError: the reader has been closed.
            RuntimeError: no valid read has been performed yet.
        """
        self._check_open()
        if not self._last_read_valid:
            raise MmapqError(
                "no valid read performed — call read() first and check result")
        return self._ctx.appender_id

    def _extract(self) -> Optional[bytes]:
        self._check_status()
        if self._ctx.status == EntryStatus.AVAILABLE:
            self._last_read_valid = True
            return bytes(ffi.buffer(self._ctx.data, self._ctx.length))
        self._last_read_valid = False
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.close()
        except BufferError:
            if exc_type is None:
                raise
            # Preserve the processing exception. Outstanding views still own
            # the mapping; leave the reader open and its finalizer armed.
        return False


class IndexedBatchReader:
    """Random-access reader for an indexed batch queue.

    Reads any committed batch by index. Each read() returns a list of
    message payload bytes, or None if no batch exists at the given index.
    Thread-confined. One per consumer thread.
    """

    def __init__(self, inner, *, runtime=None):
        require_cdata(inner, "struct MmapqIndexedBatchReader *", "IndexedBatchReader")
        self._inner = inner
        self._runtime = runtime
        self._ctx = ffi.new("MmapqBatchReadingContext *")
        self._last_read_valid = False
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
        rt = require_open(runtime._inner) if runtime is not None else ffi.NULL
        ptr = ffi.new("MmapqIndexedBatchReader **")
        rc = lib.mmapq_indexed_batch_reader_create(
            directory.encode(), queue_name.encode(),
            ring_size, timeout_ns, rt, ptr,
        )
        check_rc(rc, "mmapq_indexed_batch_reader_create")
        return cls(ptr[0])

    def read(self, index: int) -> Optional[list[bytes]]:
        """Read the batch at the given index.

        Returns a list of payload bytes (one per message) if the batch
        exists, or None if the index is beyond the last committed batch
        or has been archived.
        """
        self._check_open()
        lib.mmapq_indexed_batch_reader_read(self._inner, index, self._ctx)
        return self._extract_batch()

    def read_first(self) -> Optional[list[bytes]]:
        """Read the first unarchived batch (at start_index)."""
        self._check_open()
        lib.mmapq_indexed_batch_reader_read_first(self._inner, self._ctx)
        return self._extract_batch()

    def read_last(self) -> Optional[list[bytes]]:
        """Read the last committed batch."""
        self._check_open()
        lib.mmapq_indexed_batch_reader_read_last(self._inner, self._ctx)
        return self._extract_batch()

    def entry_status(self, index: int) -> int:
        """Check availability of the batch at the given index.

        Returns a BatchEntryStatus value.
        """
        if index < 0:
            raise ValueError(f"index must be >= 0, got {index}")
        self._check_open()
        return lib.mmapq_indexed_batch_reader_entry_status(self._inner, index)

    def batch_appender_id(self) -> int:
        """Return the appender ID of the last batch read.

        Valid only after a ``read()``, ``read_first()``, or ``read_last()``
        call whose result was not None. Returns the per-lane ID (0–255)
        of the appender that wrote the batch.

        Raises:
            QueueClosedError: the reader has been closed.
            RuntimeError: no valid batch read has been performed yet.
        """
        self._check_open()
        if not self._last_read_valid:
            raise MmapqError(
                "no valid batch read performed — call read() first and check result")
        return self._ctx.appender_id

    def last_index(self) -> int:
        """Highest committed batch index, or -1 if the queue is empty."""
        self._check_open()
        return lib.mmapq_indexed_batch_reader_last_index(self._inner)

    def start_index(self) -> int:
        """First unarchived batch index (lowest readable)."""
        self._check_open()
        return lib.mmapq_indexed_batch_reader_start_index(self._inner)

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
        from .batch_poller import iter_batch_messages
        status = self._ctx.status
        if status == BatchEntryStatus.AVAILABLE:
            self._last_read_valid = True
            messages = iter_batch_messages(self._ctx)
            error = lib.mmapq_indexed_batch_reader_last_error(self._inner)
            if error < 0:
                self._last_read_valid = False
                check_rc(error, "read_batch_messages")
            return messages
        else:
            self._last_read_valid = False
        if status in (BatchEntryStatus.END_OF_STREAM,
                      BatchEntryStatus.ARCHIVED):
            return None
        if status == BatchEntryStatus.ERROR:
            rc = lib.mmapq_indexed_batch_reader_last_error(self._inner)
            if rc != 0:
                check_rc(rc, "mmapq_indexed_batch_reader_read")
            raise MmapqError(
                "mmapq_indexed_batch_reader_read: MMAPQ_ENTRY_ERROR "
                "(mapping timeout or I/O failure)")
        else:
            raise QueueCorruptError(
                f"mmapq_indexed_batch_reader_read: unknown status {status}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
