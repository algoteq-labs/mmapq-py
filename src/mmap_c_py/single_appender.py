"""IndexedAppender — single-message producer."""

from __future__ import annotations

from . import _cffi as _cffi_mod

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib


class IndexedAppender:
    """Single-message appender.

    Appends individual messages to the queue. Each append commits atomically.
    Thread-confined. One per producer lane.
    """

    def __init__(self, inner):
        self._inner = inner

    def append(self, data: bytes) -> int:
        """Append a single message to the queue.

        The message is committed atomically — visible to pollers immediately.
        Returns the committed index.
        """
        buf = ffi.from_buffer(data)
        idx = lib.indexed_appender_append(self._inner, buf, len(data))
        if idx < 0:
            raise RuntimeError("indexed_appender_append failed")
        return idx

    def current_index(self) -> int:
        """Return the index that will be assigned to the next append."""
        return lib.indexed_appender_current_index(self._inner)

    def id(self) -> int:
        """Return the appender's per-lane ID (0–255)."""
        return lib.indexed_appender_id(self._inner)

    def close(self):
        lib.indexed_appender_close(self._inner)
        self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __del__(self):
        if hasattr(self, '_inner') and self._inner is not None:
            lib.indexed_appender_close(self._inner)
