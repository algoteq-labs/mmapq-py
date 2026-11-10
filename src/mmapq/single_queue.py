"""IndexedQueue — factory and lifecycle owner for a single-message indexed queue."""

from __future__ import annotations

import weakref

from . import _cffi as _cffi_mod
from ._cdata import require_cdata, require_open
from ._errors import check_rc

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib


def _destroy_queue(inner):
    if inner is not None:
        lib.mmapq_indexed_queue_close(inner)


class IndexedQueue:
    """Factory for single-message queue participants.

    Creates appenders and pollers on the same underlying queue.
    Thread-safe for factory calls; participants are thread-confined.
    """

    def __init__(self, inner, *, runtime=None):
        require_cdata(inner, "struct MmapqIndexedQueue *", "IndexedQueue")
        self._inner = inner
        self._runtime = runtime
        self._finalizer = weakref.finalize(self, _destroy_queue, inner)

    @classmethod
    def create(cls, directory, queue_name, write_runtime=None, read_runtime=None,
               archivable=False, *, region_size=4096*1024, ring_size=4,
               max_file_size=None, start_index=0, timeout_ns=10_000_000_000,
               runtime=None):
        """Create or open a queue.

        archivable, region_size, max_file_size and start_index apply only
        when this handle creates the queue's first appender: the queue's
        files are written then, with this handle's settings, and every
        later open uses the stored values and ignores its own. ring_size
        and timeout_ns apply to this handle. Create the queue and its first
        appender in one place with the intended settings.

        directory:      path to the queue data directory
        queue_name:     logical queue name
        write_runtime:  runtime for appender-side async region mapping
        read_runtime:   runtime for poller/reader-side async region mapping
        archivable:     if True, create archivable (positional, default False)
        region_size:    payload region size in bytes (power of two and a
                        multiple of 32 KB, and a multiple of the OS page size
                        — the geometry rule; default 4 MB)
        ring_size:      number of simultaneously mapped regions (default 4)
        max_file_size:  max size of a single payload file (a multiple of
                        region_size, therefore of the 32 KB header block); if
                        None, computed as region_size * 256 (default 1 GB with
                        default region_size)
        start_index:    initial message index (default 0)
        timeout_ns:     region-mapping timeout in nanoseconds (default 10 s)
        runtime:        shared runtime for both sides (keyword-only)

        All defaults match the C library's INDEXED_QUEUE_DEFAULT_* constants.
        """
        if max_file_size is None:
            max_file_size = region_size * 256
        elif max_file_size <= 0:
            raise ValueError(
                f"max_file_size must be positive, got {max_file_size}")
        regions_to_map_ahead = 1  # match INDEXED_QUEUE_DEFAULT_REGIONS_TO_MAP_AHEAD
        wr = require_open(write_runtime._inner) if write_runtime is not None else ffi.NULL
        rr = require_open(read_runtime._inner) if read_runtime is not None else ffi.NULL
        if runtime is not None:
            wr = require_open(runtime._inner)
            rr = require_open(runtime._inner)
        arch_int = 1 if archivable else 0
        ptr = ffi.new("MmapqIndexedQueue **")
        rc = lib.mmapq_indexed_queue_create(
            directory.encode(), queue_name.encode(),
            region_size, ring_size, regions_to_map_ahead, max_file_size,
            start_index, timeout_ns, arch_int, wr, rr, ptr,
        )
        check_rc(rc, "mmapq_indexed_queue_create")
        q = cls(ptr[0], runtime=runtime)
        q._write_runtime = write_runtime
        q._read_runtime = read_runtime
        return q

    def create_appender(self):
        """Create a single-message appender.

        A transient mapping timeout during the appender's join walk
        surfaces as the base ``MmapqError`` (rc -1), not
        ``QueueCorruptError`` — callers catching ``QueueCorruptError``
        around appender open do not catch that case (D8; a distinct
        timeout exception class remains deferred, X-01).
        """
        from .single_appender import IndexedAppender
        ptr = ffi.new("MmapqIndexedAppender **")
        rc = lib.mmapq_indexed_queue_create_appender(require_open(self._inner), ptr)
        check_rc(rc, "create_appender")
        a = IndexedAppender(ptr[0])
        a._runtime = self._runtime
        return a

    def create_appender_with_id(self, appender_id: int):
        """Create an appender with a specific lane ID (0–255).

        Validates that the ID is in range before the FFI call.
        Raises ValueError if the ID is out of range, AppenderIdConflictError
        if it is taken. A writer killed without closing leaves its ID taken
        until an operator releases it (the mmapq-ids tool in the C
        repository); one that closed released it.
        A transient mapping timeout during the appender's join walk
        surfaces as the base ``MmapqError`` (rc -1), not
        ``QueueCorruptError`` (D8; a distinct timeout exception class
        remains deferred, X-01).
        """
        if not 0 <= appender_id < 256:
            raise ValueError(
                f"appender_id must be in [0, 255], got {appender_id}")
        from .single_appender import IndexedAppender
        ptr = ffi.new("MmapqIndexedAppender **")
        rc = lib.mmapq_indexed_queue_create_appender_with_id(
            require_open(self._inner), appender_id, ptr)
        check_rc(rc, "create_appender_with_id")
        a = IndexedAppender(ptr[0])
        a._runtime = self._runtime
        return a

    def create_poller(self):
        """Create a single-message poller."""
        from .single_poller import IndexedPoller
        ptr = ffi.new("MmapqIndexedPoller **")
        rc = lib.mmapq_indexed_queue_create_poller(require_open(self._inner), ptr)
        check_rc(rc, "create_poller")
        p = IndexedPoller(ptr[0])
        p._runtime = self._runtime
        return p

    def create_reader(self):
        """Create a random-access reader inheriting the queue's runtime."""
        from .reader import IndexedReader
        ptr = ffi.new("MmapqIndexedReader **")
        rc = lib.mmapq_indexed_queue_create_reader(require_open(self._inner), ptr)
        check_rc(rc, "create_reader")
        r = IndexedReader(ptr[0], runtime=self._read_runtime or self._runtime)
        return r

    def create_archiver(self):
        """Create an archiver inheriting the queue's runtime."""
        from .archiver import IndexedArchiver
        ptr = ffi.new("MmapqIndexedArchiver **")
        rc = lib.mmapq_indexed_queue_create_archiver(require_open(self._inner), ptr)
        check_rc(rc, "create_archiver")
        return IndexedArchiver(ptr[0])

    def close(self):
        self._finalizer.detach()
        _destroy_queue(self._inner)
        self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
