"""IndexedQueue — factory and lifecycle owner for a single-message indexed queue."""

from __future__ import annotations

from . import _cffi as _cffi_mod

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib


class IndexedQueue:
    """Factory for single-message queue participants.

    Creates appenders and pollers on the same underlying queue.
    Thread-safe for factory calls; participants are thread-confined.
    """

    def __init__(self, inner):
        self._inner = inner

    @classmethod
    def create(cls, directory, queue_name, write_runtime=None, read_runtime=None,
               archivable=False):
        """Create or open a queue with default configuration.

        Args:
            directory: Path to the queue data directory.
            queue_name: Logical queue name.
            write_runtime: Optional Runtime for write-side background mapping.
            read_runtime: Optional Runtime for read-side background mapping.
            archivable: If True, create the queue archivable (archiver may
                advance start_index). Default False (performant mode).
                Applies at creation only — existing queues keep on-disk value.
        """
        wr = write_runtime if write_runtime is not None else ffi.NULL
        rr = read_runtime if read_runtime is not None else ffi.NULL
        if archivable:
            ptr = lib.indexed_queue_create(
                directory.encode(), queue_name.encode(),
                4096 * 1024, 4, 1, (4096 * 1024) * 256,
                0, 10000000000, 1, wr, rr,
            )
        else:
            ptr = lib.indexed_queue_create_default(
                directory.encode(), queue_name.encode(), wr, rr,
            )
        if not ptr:
            raise RuntimeError("indexed_queue_create returned NULL")
        return cls(ptr)

    def create_appender(self):
        """Create a single-message appender."""
        from .single_appender import IndexedAppender
        ptr = lib.indexed_queue_create_appender(self._inner)
        if not ptr:
            raise RuntimeError("create_appender returned NULL")
        return IndexedAppender(ptr)

    def create_poller(self):
        """Create a single-message poller."""
        from .single_poller import IndexedPoller
        ptr = lib.indexed_queue_create_poller(self._inner)
        if not ptr:
            raise RuntimeError("create_poller returned NULL")
        return IndexedPoller(ptr)

    def close(self):
        lib.indexed_queue_close(self._inner)
        self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
