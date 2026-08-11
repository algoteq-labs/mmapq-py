"""IndexedBatchQueue — factory and lifecycle owner for an indexed batch queue."""

from __future__ import annotations

import weakref

from . import _cffi as _cffi_mod
from ._errors import check_rc

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib


def _destroy_batch_queue(inner):
    if inner is not None:
        lib.indexed_batch_queue_close(inner)


class IndexedBatchQueue:
    """Factory for batch queue participants.

    Creates pollers and appenders on the same underlying queue.
    Thread-safe for factory calls; participants are thread-confined.
    """

    def __init__(self, inner, *, runtime=None):
        self._inner = inner
        self._runtime = runtime
        self._finalizer = weakref.finalize(self, _destroy_batch_queue, inner)

    @classmethod
    def create(cls, directory, queue_name, write_runtime=None, read_runtime=None,
               *, region_size=4096*1024, ring_size=4, max_file_size=None,
               start_index=0, timeout_ns=10_000_000_000, archivable=False,
               runtime=None):
        """Create or open a batch queue.

        All sizing parameters apply only when creating a *new* queue —
        if the queue already exists on disk, the on-disk configuration is
        authoritative and these arguments are silently ignored.

        directory:      path to the queue data directory
        queue_name:     logical queue name
        write_runtime:  runtime for appender-side async region mapping
        read_runtime:   runtime for poller/reader-side async region mapping
        region_size:    payload region size in bytes (power of two, default 4 MB)
        ring_size:      number of simultaneously mapped regions (default 4)
        max_file_size:  max size of a single payload file; if None, computed
                        as region_size * 256 (default 1 GB with default region_size)
        start_index:    initial batch index (default 0)
        timeout_ns:     region-mapping timeout in nanoseconds (default 10 s)
        archivable:     if True, create archivable (default False)
        runtime:        shared runtime for both sides (keyword-only)

        All defaults match the C library's INDEXED_QUEUE_DEFAULT_* constants.
        """
        if max_file_size is None:
            max_file_size = region_size * 256
        elif max_file_size <= 0:
            raise ValueError(
                f"max_file_size must be positive, got {max_file_size}")
        regions_to_map_ahead = 1  # match INDEXED_QUEUE_DEFAULT_REGIONS_TO_MAP_AHEAD
        wr = write_runtime._inner if write_runtime is not None and write_runtime._inner is not None else ffi.NULL
        rr = read_runtime._inner if read_runtime is not None and read_runtime._inner is not None else ffi.NULL
        arch_int = 1 if archivable else 0
        if runtime is not None:
            wr = runtime._inner
            rr = runtime._inner
        ptr = ffi.new("IndexedBatchQueue **")
        rc = lib.indexed_batch_queue_create(
            directory.encode(), queue_name.encode(),
            region_size, ring_size, regions_to_map_ahead, max_file_size,
            start_index, timeout_ns, arch_int, wr, rr, ptr,
        )
        check_rc(rc, "indexed_batch_queue_create")
        q = cls(ptr[0], runtime=runtime)
        q._write_runtime = write_runtime
        q._read_runtime = read_runtime
        return q

    def create_poller(self):
        """Create a sequential batch poller."""
        from .batch_poller import IndexedBatchPoller
        ptr = ffi.new("IndexedBatchPoller **")
        rc = lib.indexed_batch_queue_create_poller(self._inner, ptr)
        check_rc(rc, "create_poller")
        p = IndexedBatchPoller(ptr[0])
        p._runtime = self._runtime
        return p

    def create_appender(self):
        """Create a batch appender."""
        from .batch_appender import IndexedBatchAppender
        ptr = ffi.new("IndexedBatchAppender **")
        rc = lib.indexed_batch_queue_create_appender(self._inner, ptr)
        check_rc(rc, "create_appender")
        a = IndexedBatchAppender(ptr[0])
        a._runtime = self._runtime
        return a

    def create_appender_with_id(self, appender_id: int):
        """Create a batch appender with a specific lane ID (0–255).

        Raises ValueError if the ID is out of range.
        Raises AppenderIdConflictError if the ID is already in use.
        """
        if not 0 <= appender_id < 256:
            raise ValueError(
                f"appender_id must be in [0, 255], got {appender_id}")
        from .batch_appender import IndexedBatchAppender
        ptr = ffi.new("IndexedBatchAppender **")
        rc = lib.indexed_batch_queue_create_appender_with_id(
            self._inner, appender_id, ptr)
        check_rc(rc, "create_appender_with_id")
        a = IndexedBatchAppender(ptr[0])
        a._runtime = self._runtime
        return a

    def close(self):
        self._finalizer.detach()
        _destroy_batch_queue(self._inner)
        self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
