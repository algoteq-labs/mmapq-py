"""IndexedBatchQueue — factory and lifecycle owner for an indexed batch queue."""

from __future__ import annotations

import weakref

from . import _cffi as _cffi_mod

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
               *, runtime=None):
        """Create or open a batch queue with default configuration.

        directory:   path to the queue data directory
        queue_name:  logical queue name
        runtime:     optional Runtime for async region mapping (keyword-only)
        """
        wr = write_runtime if write_runtime is not None else ffi.NULL
        rr = read_runtime if read_runtime is not None else ffi.NULL
        if runtime is not None:
            wr = runtime._inner
            rr = runtime._inner
        ptr = ffi.new("IndexedBatchQueue **")
        rc = lib.indexed_batch_queue_create_default(
            directory.encode(), queue_name.encode(),
            wr, rr, ptr,
        )
        if rc != 0:
            raise RuntimeError(f"indexed_batch_queue_create_default failed with code {rc}")
        return cls(ptr[0], runtime=runtime)

    def create_poller(self):
        """Create a sequential batch poller."""
        from .batch_poller import IndexedBatchPoller
        ptr = ffi.new("IndexedBatchPoller **")
        rc = lib.indexed_batch_queue_create_poller(self._inner, ptr)
        if rc != 0:
            raise RuntimeError(f"create_poller failed with code {rc}")
        p = IndexedBatchPoller(ptr[0])
        p._runtime = self._runtime
        return p

    def create_appender(self):
        """Create a batch appender."""
        from .batch_appender import IndexedBatchAppender
        ptr = ffi.new("IndexedBatchAppender **")
        rc = lib.indexed_batch_queue_create_appender(self._inner, ptr)
        if rc != 0:
            raise RuntimeError(f"create_appender failed with code {rc}")
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
