"""IndexedBatchQueue — factory and lifecycle owner for an indexed batch queue."""

from __future__ import annotations

from . import _cffi as _cffi_mod

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib


class IndexedBatchQueue:
    """Factory for batch queue participants.

    Creates pollers and appenders on the same underlying queue.
    Thread-safe for factory calls; participants are thread-confined.
    """

    def __init__(self, inner):
        self._inner = inner

    @classmethod
    def create(cls, directory, queue_name, write_runtime=None, read_runtime=None):
        """Create or open a batch queue with default configuration.

        Args:
            directory: Path to the queue data directory.
            queue_name: Logical queue name.
            write_runtime: Optional Runtime for write-side background mapping.
            read_runtime: Optional Runtime for read-side background mapping.
        """
        wr = write_runtime if write_runtime is not None else ffi.NULL
        rr = read_runtime if read_runtime is not None else ffi.NULL
        ptr = lib.indexed_batch_queue_create_default(
            directory.encode(), queue_name.encode(),
            wr, rr,
        )
        if not ptr:
            raise RuntimeError("indexed_batch_queue_create_default returned NULL")
        return cls(ptr)

    def create_poller(self):
        """Create a sequential batch poller."""
        from .batch_poller import IndexedBatchPoller
        ptr = lib.indexed_batch_queue_create_poller(self._inner)
        if not ptr:
            raise RuntimeError("create_poller returned NULL")
        return IndexedBatchPoller(ptr)

    def create_appender(self):
        """Create a batch appender."""
        from .batch_appender import IndexedBatchAppender
        ptr = lib.indexed_batch_queue_create_appender(self._inner)
        if not ptr:
            raise RuntimeError("create_appender returned NULL")
        return IndexedBatchAppender(ptr)

    def close(self):
        lib.indexed_batch_queue_close(self._inner)
        self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
