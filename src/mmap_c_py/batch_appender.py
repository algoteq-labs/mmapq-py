"""IndexedBatchAppender — producer-side batch writer."""

from __future__ import annotations

from . import _cffi as _cffi_mod

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib


class BatchWriter:
    """An in-progress batch write session.

    Usable as a context manager — automatic rollback on error::

        with appender.start_batch() as writer:
            writer.append(b"msg1")
            writer.append(b"msg2")
            writer.commit()
    """

    def __init__(self, appender, inner):
        self._appender = appender
        self._inner = inner
        self._committed = False

    def append(self, data: bytes) -> int:
        """Append a message to the in-progress batch.

        Returns the zero-based message index within the batch.
        """
        buf = ffi.from_buffer(data)
        result = lib.mutable_batch_append(self._inner, buf, len(data))
        if result < 0:
            raise RuntimeError("mutable_batch_append failed")
        return result

    def append_remaining_from(self, it) -> int:
        """Append all remaining messages from a BatchPayloadIterator.

        The iterator must be positioned before or at the first un-copied
        message. Returns the number of messages appended.
        """
        return lib.mutable_batch_append_remaining_from(self._inner, it)

    def commit(self) -> int:
        """Complete and commit the batch.

        Returns the committed batch index. The writer is consumed — do not
        use after calling commit().
        """
        offset = lib.mutable_batch_complete(self._inner)
        if offset < 0:
            raise RuntimeError("mutable_batch_complete failed")
        idx = lib.indexed_batch_appender_commit_batch(self._appender._inner, offset)
        if idx < 0:
            raise RuntimeError("commit_batch failed")
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
        self._inner = inner

    def start_batch(self) -> BatchWriter:
        """Start a new batch write session."""
        ptr = lib.indexed_batch_appender_start_batch(self._inner)
        if not ptr:
            raise RuntimeError("start_batch returned NULL")
        return BatchWriter(self, ptr)

    def close(self):
        lib.indexed_batch_appender_close(self._inner)
        self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __del__(self):
        if self._inner is not None:
            lib.indexed_batch_appender_close(self._inner)
