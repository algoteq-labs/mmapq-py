"""IndexedQueue — factory and lifecycle owner for a single-message indexed queue."""

from __future__ import annotations

import weakref

from . import _cffi as _cffi_mod
from ._errors import check_rc

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib


def _destroy_queue(inner):
    if inner is not None:
        lib.indexed_queue_close(inner)


class IndexedQueue:
    """Factory for single-message queue participants.

    Creates appenders and pollers on the same underlying queue.
    Thread-safe for factory calls; participants are thread-confined.
    """

    def __init__(self, inner, *, runtime=None):
        self._inner = inner
        self._runtime = runtime
        self._finalizer = weakref.finalize(self, _destroy_queue, inner)

    @classmethod
    def create(cls, directory, queue_name, write_runtime=None, read_runtime=None,
               archivable=False, *, runtime=None):
        """Create or open a queue with default configuration.

        directory:   path to the queue data directory
        queue_name:  logical queue name
        runtime:     optional Runtime for async region mapping (keyword-only)
        archivable:  if True, create archivable (default False)
        """
        wr = write_runtime if write_runtime is not None else ffi.NULL
        rr = read_runtime if read_runtime is not None else ffi.NULL
        if runtime is not None:
            wr = runtime._inner
            rr = runtime._inner
        arch_int = 1 if archivable else 0
        ptr = lib.indexed_queue_create(
            directory.encode(), queue_name.encode(),
            4096 * 1024, 4, 1, (4096 * 1024) * 256,
            0, 10000000000, arch_int, wr, rr,
        )
        if not ptr:
            raise RuntimeError("indexed_queue_create returned NULL")
        return cls(ptr, runtime=runtime)

    def create_appender(self):
        """Create a single-message appender."""
        from .single_appender import IndexedAppender
        ptr = lib.indexed_queue_create_appender(self._inner)
        if not ptr:
            raise RuntimeError("create_appender returned NULL")
        a = IndexedAppender(ptr)
        a._runtime = self._runtime
        return a

    def create_appender_with_id(self, appender_id: int):
        """Create an appender with a specific lane ID (0–255).

        Validates that the ID is in range before the FFI call.
        Raises ValueError if the ID is out of range or already in use.
        """
        if not 0 <= appender_id < 256:
            raise ValueError(
                f"appender_id must be in [0, 255], got {appender_id}")
        from .single_appender import IndexedAppender
        ptr = lib.indexed_queue_create_appender_with_id(
            self._inner, appender_id)
        if not ptr:
            raise RuntimeError("create_appender_with_id returned NULL")
        a = IndexedAppender(ptr)
        a._runtime = self._runtime
        return a

    def create_poller(self):
        """Create a single-message poller."""
        from .single_poller import IndexedPoller
        ptr = lib.indexed_queue_create_poller(self._inner)
        if not ptr:
            raise RuntimeError("create_poller returned NULL")
        p = IndexedPoller(ptr)
        p._runtime = self._runtime
        return p

    def close(self):
        self._finalizer.detach()
        _destroy_queue(self._inner)
        self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
