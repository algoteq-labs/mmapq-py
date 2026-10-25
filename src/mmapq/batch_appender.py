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
        """Publish the batch (one call: complete is now the publish).

        Returns the committed batch index. The writer is consumed — do not
        use after calling commit().
        """
        self._check_open()
        idx = check_rc(lib.mmapq_mutable_batch_complete(self._inner), "complete_batch")
        self._committed = True
        self._finished = True
        self._appender._active_batch = False
        return idx

    def complete_uncommitted(self) -> int:
        """Finalise the batch WITHOUT publishing it (stage).

        Stores the message count and returns the batch offset for a later
        ``IndexedBatchAppender.commit_batch()`` — same session or after
        rejoin. The writer is consumed — do not use after calling
        complete_uncommitted().
        """
        self._check_open()
        offset = check_rc(
            lib.mmapq_mutable_batch_complete_uncommitted(self._inner),
            "complete_uncommitted",
        )
        self._finished = True
        self._appender._active_batch = False
        return offset

    def __enter__(self):
        self._check_open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._finished:
            try:
                if self._appender._inner is not None:
                    # Discard-only, NEVER complete (complete publishes now).
                    # The rc is ignored per the owner ruling: exit is
                    # best-effort rollback.
                    lib.mmapq_mutable_batch_discard(self._inner)
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

    def commit_batch(self, offset: int) -> int:
        """Commit a staged batch, making it visible to pollers and readers.

        ``offset`` is the value returned by
        ``BatchWriter.complete_uncommitted()``, or an offset yielded by
        ``uncommitted_batches()`` — any subset, any order, same session or
        after rejoin. Returns the committed batch index. Raises ValueError
        if the offset is not outstanding (never staged by this incarnation,
        already committed, a discarded batch, above the last started batch)
        or has been overtaken by archiving.
        """
        rc = lib.mmapq_indexed_batch_appender_commit_batch(
            require_open(self._inner), offset)
        return check_rc(rc, "commit_batch")

    def uncommitted_batches(self):
        """Iterate this appender's uncommitted (staged or discarded) batches.

        Snapshot at call time — appends/commits after the call are
        invisible. Yields ``(offset: int, messages: list[bytes])`` pairs
        from the archive line onward. Do not append while iterating (the
        appender is exclusively lent to the walk, by contract). Commit found
        offsets with ``commit_batch()`` — any subset, any order, same
        session or after rejoin; not committing = skip. Discarded batches
        reappear in recovery after a restart — the walker reports them and
        refill makes them committable; recovery code decides. Archiving may
        remove batches the walk has not yielded, even after an undo. Raises
        QueueCorruptError on a zero archive line — a corrupt appender meta
        (pre-0.2.0 queues are refused at open with SchemaMismatchError).
        """
        require_open(self._inner)
        out = ffi.new("MmapqUncommittedBatchIterator **")
        check_rc(
            lib.mmapq_indexed_batch_appender_uncommitted_batches_checked(
                self._inner, out),
            "uncommitted_batches",
        )
        return self._uncommitted_batches_gen(out[0])

    def _uncommitted_batches_gen(self, it):
        offset = ffi.new("int64_t *")
        count = ffi.new("int32_t *")
        msgs = ffi.new("MmapqBatchPayloadIterator **")
        while True:
            require_open(self._inner)
            rc = lib.mmapq_uncommitted_batch_next(it, offset, count, msgs)
            if rc == 0:
                return
            if rc < 0:
                check_rc(rc, "uncommitted_batches")
            yield int(offset[0]), self._read_step_messages(msgs[0])

    @staticmethod
    def _read_step_messages(cursor) -> list[bytes]:
        """Read all messages of one walked batch via the checked step.

        A negative rc raises (a mid-batch timeout/corruption must not
        surface as a silently truncated message list — recovery decides on
        what it sees). Do not call advance() first: the checked call itself
        advances.
        """
        data = ffi.new("const void **")
        length = ffi.new("int32_t *")
        messages = []
        while True:
            rc = lib.mmapq_batch_iterator_next(cursor, data, length)
            if rc == 0:
                return messages
            if rc < 0:
                check_rc(rc, "uncommitted_batches")
            messages.append(bytes(ffi.buffer(data[0], length[0])))

    def join_completed_open_batch(self) -> int | None:
        """Report whether the join completed a trailing open batch to staged.

        Returns the completed batch's offset once per join-completion event
        (read-and-clear), None otherwise. The batch was completed by the
        queue, not the app: it may lack messages the app intended; the last
        counted message may have partial content.
        """
        out = ffi.new("int64_t *")
        rc = lib.mmapq_indexed_batch_appender_join_completed_open_batch(
            require_open(self._inner), out)
        if rc == 1:
            return int(out[0])
        if rc == 0:
            return None
        return check_rc(rc, "join_completed_open_batch")

    def close(self):
        self._finalizer.detach()
        _destroy_batch_appender(self._inner)
        self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
