"""MmapqIndexedAppender — single-message producer."""

from __future__ import annotations

import weakref

from . import _cffi as _cffi_mod
from ._cdata import require_cdata
from ._errors import check_rc

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib


def _destroy_appender(inner):
    if inner is not None:
        lib.mmapq_indexed_appender_close(inner)


class MmapqIndexedAppender:
    """Single-message appender.

    Appends individual messages to the queue. Each append commits atomically.
    Thread-confined. One per producer lane.
    """

    def __init__(self, inner):
        require_cdata(inner, "struct MmapqIndexedAppender *", "MmapqIndexedAppender")
        self._inner = inner
        self._runtime = None
        self._finalizer = weakref.finalize(self, _destroy_appender, inner)

    def append(self, data: bytes) -> int:
        """Append a single message to the queue.

        The message is committed atomically — visible to pollers immediately.
        Returns the committed index.
        """
        buf = ffi.from_buffer(data)
        rc = lib.mmapq_indexed_appender_append(self._inner, buf, len(data))
        return check_rc(rc, "append")

    def append_uncommitted(self, data: bytes) -> int:
        """Write a payload without committing it.

        An uncommitted payload is invisible to all readers and is discarded
        by crash recovery; only commit() publishes it.
        Returns the payload offset (for later use with commit()).
        """
        buf = ffi.from_buffer(data)
        rc = lib.mmapq_indexed_appender_append_uncommitted(self._inner, buf, len(data))
        return check_rc(rc, "append_uncommitted")

    def commit(self, payload_offset: int) -> int:
        """Commit a previously uncommitted payload.

        An uncommitted payload is invisible to all readers and is discarded
        by crash recovery; only commit() publishes it.
        Returns the committed index.
        """
        rc = lib.mmapq_indexed_appender_commit(self._inner, payload_offset)
        return check_rc(rc, "commit")

    def current_index(self) -> int:
        """Return the index that will be assigned to the next append."""
        return lib.mmapq_indexed_appender_current_index(self._inner)

    def id(self) -> int:
        """Return the appender's per-lane ID (0–255)."""
        return lib.mmapq_indexed_appender_id(self._inner)

    def close(self):
        self._finalizer.detach()
        _destroy_appender(self._inner)
        self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
