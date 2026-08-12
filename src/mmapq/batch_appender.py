"""IndexedBatchAppender — producer-side batch writer."""

from __future__ import annotations

import weakref

from . import _cffi as _cffi_mod
from ._cdata import require_cdata
from ._errors import MmapqError, check_rc

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib


def _destroy_batch_appender(inner):
    if inner is not None:
        lib.indexed_batch_appender_close(inner)


class BatchWriter:
    """An in-progress batch write session — discards on exit unless commit() was called.

    Usable as a context manager::

        with appender.start_batch() as writer:
            writer.append(b"msg1")
            writer.commit()          # required — without this the batch is discarded
    """

    def __init__(self, appender, inner):
        self._appender = appender
        self._inner = inner
        self._committed = False

    def append(self, data: bytes) -> int:
        """Append a message to the in-progress batch.

        Returns the zero-based message index within the batch.
        Raises ValueError if data is empty or exceeds region_size.
        """
        buf = ffi.from_buffer(data)
        rc = lib.mutable_batch_append(self._inner, buf, len(data))
        return check_rc(rc, "mutable_batch_append")

    def append_remaining_from(self, it) -> int:
        """Append all remaining messages from a BatchPayloadIterator.

        The iterator must be positioned before or at the first un-copied
        message. Returns the number of messages appended.
        Raises ValueError if a message exceeds the destination queue's
        region_size — the batch is partially populated and must be
        discarded, not committed.
        """
        rc = lib.mutable_batch_append_remaining_from(self._inner, it)
        return check_rc(rc, "append_remaining_from")

    def commit(self) -> int:
        """Complete and commit the batch.

        Returns the committed batch index. The writer is consumed — do not
        use after calling commit().
        """
        if self._committed:
            raise MmapqError("batch already committed")
        offset = lib.mutable_batch_complete(self._inner)
        if offset < 0:
            raise MmapqError("mutable_batch_complete failed")
        idx = lib.indexed_batch_appender_commit_batch(self._appender._inner, offset)
        if idx < 0:
            raise MmapqError("commit_batch failed")
        self._committed = True
        return idx

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._committed:
            # Auto-rollback on error or missing commit()
            offset = lib.mutable_batch_complete(self._inner)
            if offset >= 0:
                lib.indexed_batch_appender_discard_batch(self._appender._inner, offset)
        return False


class IndexedBatchAppender:
    """Batch appender.

    Thread-confined. One per producer lane.
    """

    def __init__(self, inner):
        require_cdata(inner, "struct IndexedBatchAppender *", "IndexedBatchAppender")
        self._inner = inner
        self._runtime = None
        self._finalizer = weakref.finalize(self, _destroy_batch_appender, inner)

    def start_batch(self) -> BatchWriter:
        """Start a new batch write session."""
        ptr = lib.indexed_batch_appender_start_batch(self._inner)
        if not ptr:
            raise MmapqError("start_batch returned NULL")
        return BatchWriter(self, ptr)

    def id(self) -> int:
        """Return this appender's lane ID (0–255)."""
        return lib.indexed_batch_appender_id(self._inner)

    def close(self):
        self._finalizer.detach()
        _destroy_batch_appender(self._inner)
        self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __del__(self):
        if self._inner is not None:
            lib.indexed_batch_appender_close(self._inner)
