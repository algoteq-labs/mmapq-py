"""IndexedBatchAppender — producer-side batch writer."""

from __future__ import annotations

import weakref

from . import _cffi as _cffi_mod
from ._cdata import require_cdata, require_open
from ._errors import MmapqError, check_rc

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib


def _destroy_batch_appender(inner):
    if inner is not None:
        lib.mmapq_indexed_batch_appender_close(inner)


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
        self._finished = False

    def _check_open(self):
        require_open(self._appender._inner)
        if self._finished:
            raise MmapqError("batch writer is no longer active")

    def append(self, data: bytes) -> int:
        """Append a message to the in-progress batch.

        Returns the zero-based message index within the batch.
        Raises ValueError if data is empty or exceeds region_size.
        """
        self._check_open()
        if not 0 < len(data) <= 0x7fffffff:
            raise ValueError("message length must be between 1 and 2147483647")
        buf = ffi.from_buffer(data)
        rc = lib.mmapq_mutable_batch_append(self._inner, buf, len(data))
        return check_rc(rc, "mmapq_mutable_batch_append")

    def append_remaining_from(self, it) -> int:
        """Append all remaining messages from a MmapqBatchPayloadIterator.

        The iterator must be positioned before or at the first un-copied
        message. Returns the number of messages appended.
        Raises ValueError if a message exceeds the destination queue's
        region_size — the batch is partially populated and must be
        discarded, not committed.
        """
        self._check_open()
        require_cdata(it, "struct MmapqBatchPayloadIterator *", "append_remaining_from")
        rc = lib.mmapq_mutable_batch_append_remaining_from(self._inner, it)
        return check_rc(rc, "append_remaining_from")

    def commit(self) -> int:
        """Complete and commit the batch.

        Returns the committed batch index. The writer is consumed — do not
        use after calling commit().
        """
        self._check_open()
        offset = check_rc(lib.mmapq_mutable_batch_complete(self._inner), "complete_batch")
        idx = check_rc(lib.mmapq_indexed_batch_appender_commit_batch(
            self._appender._inner, offset), "commit_batch")
        self._committed = True
        self._finished = True
        self._appender._active_batch = False
        return idx

    def __enter__(self):
        self._check_open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._finished:
            try:
                if self._appender._inner is not None:
                    offset = check_rc(lib.mmapq_mutable_batch_complete(self._inner), "complete_batch")
                    lib.mmapq_indexed_batch_appender_discard_batch(self._appender._inner, offset)
            finally:
                self._finished = True
                self._appender._active_batch = False
        return False


class IndexedBatchAppender:
    """Batch appender.

    Thread-confined. One per producer lane.
    """

    def __init__(self, inner):
        require_cdata(inner, "struct MmapqIndexedBatchAppender *", "IndexedBatchAppender")
        self._inner = inner
        self._runtime = None
        self._active_batch = False
        self._finalizer = weakref.finalize(self, _destroy_batch_appender, inner)

    def start_batch(self) -> BatchWriter:
        """Start a new batch write session."""
        require_open(self._inner)
        if self._active_batch:
            raise MmapqError("a batch is already active on this appender")
        ptr = lib.mmapq_indexed_batch_appender_start_batch(self._inner)
        if not ptr:
            raise MmapqError("start_batch returned NULL")
        self._active_batch = True
        return BatchWriter(self, ptr)

    def id(self) -> int:
        """Return this appender's lane ID (0–255)."""
        return lib.mmapq_indexed_batch_appender_id(require_open(self._inner))

    def close(self):
        self._finalizer.detach()
        _destroy_batch_appender(self._inner)
        self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
