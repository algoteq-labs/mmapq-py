"""IndexedBatchPoller — sequential batch consumer.

Each poll() delivers one batch. The handler receives the batch index and
a list of message payload bytes. The handler iterates over the messages
directly and returns a NextMove directive.

Note: messages are fully materialized into Python bytes before the handler
runs. For batches with thousands of large payloads, this doubles memory
(mapped C memory + Python copies). The old CData-based handler could
stream messages lazily — if you need that, fall back to raw FFI calls.
"""

from __future__ import annotations

import weakref
from typing import Callable

from . import _cffi as _cffi_mod
from ._cdata import require_cdata, require_open

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib
from .types import NextMove, PollResult
from ._errors import MmapqError, QueueCorruptError, check_rc


def iter_batch_messages(ctx: "CData") -> list[bytes]:
    """Iterate over messages in a MmapqBatchReadingContext.

    Returns a list of payload bytes for each message in the batch.
    The inner batch_iterator starts before message 0 — this function
    advances through all messages and returns their payloads.

    ctx: a cffi MmapqBatchReadingContext CData pointer
    Returns: list[bytes]
    """
    messages = []
    it_ptr = ctx.iterator
    data = ffi.new("const void **")
    length = ffi.new("int32_t *")
    while True:
        rc = lib.mmapq_batch_iterator_next(it_ptr, data, length)
        if rc == 0:
            return messages
        if rc < 0:
            # A mid-batch timeout/corruption must never surface as a
            # silently truncated message list (CAND-51a).
            check_rc(rc, "iter_batch_messages")
        messages.append(bytes(ffi.buffer(data[0], length[0])))


def _destroy_batch_poller(inner):
    if inner is not None:
        lib.mmapq_indexed_batch_poller_close(inner)


@ffi.callback("MmapqNextMove(int64_t, MmapqBatchReadingContext*, void*)",
              error=NextMove.STOP)
def _c_trampoline(batch_index, ctx_ptr, user_data):
    """Called from C — dispatches to the poller's current handler."""
    poller = ffi.from_handle(user_data)
    handler = poller._current_handler
    if handler is None:
        return NextMove.STOP
    try:
        messages = iter_batch_messages(ctx_ptr)
        if len(messages) != ctx_ptr.message_count:
            # Unreachable once the loop above is checked — raise, never
            # stall silently (CAND-51a).
            raise QueueCorruptError(
                f"batch {batch_index}: iterated {len(messages)} of "
                f"{ctx_ptr.message_count} messages without an error")
        return NextMove(handler(batch_index, messages))
    except BaseException as e:
        poller._pending_exc = e
        return NextMove.STOP


class IndexedBatchPoller:
    """Sequential batch consumer.

    Thread-confined. One per consumer thread.
    """

    def __init__(self, inner):
        require_cdata(inner, "struct MmapqIndexedBatchPoller *", "IndexedBatchPoller")
        self._inner = inner
        self._runtime = None
        self._handle = ffi.new_handle(self)
        self._current_handler = None
        self._pending_exc = None
        self._finalizer = weakref.finalize(self, _destroy_batch_poller, inner)

    def poll(self, handler):
        """Poll the next batch, delivering it to the handler.

        The handler receives (batch_index, messages) where messages is
        a list[bytes] of the batch's payloads. The handler iterates
        over the messages and returns a NextMove directive.

        Returns a PollResult describing what happened.
        Raises the handler's exception if one was raised during delivery.
        """
        require_open(self._inner)
        if self._current_handler is not None:
            raise MmapqError("poll is already active")
        if not callable(handler):
            raise TypeError("handler must be callable")
        self._current_handler = handler
        self._pending_exc = None
        try:
            result = lib.mmapq_indexed_batch_poller_poll(
                require_open(self._inner),
                _c_trampoline,
                self._handle,
            )
            if self._pending_exc is not None:
                exc = self._pending_exc
                self._pending_exc = None
                raise exc
            if result == PollResult.ERROR:
                check_rc(lib.mmapq_indexed_batch_poller_last_error(require_open(self._inner)), "mmapq_indexed_batch_poller_poll")
            if result == PollResult.CORRUPT:
                raise QueueCorruptError("batch poll: committed batch payload is corrupt")
            return PollResult(result)
        finally:
            self._current_handler = None

    def move_to_index(self, index: int) -> bool:
        return bool(lib.mmapq_indexed_batch_poller_move_to_index(require_open(self._inner), index))

    def move_to_end(self) -> bool:
        return bool(lib.mmapq_indexed_batch_poller_move_to_end(require_open(self._inner)))

    def move_to_last_existing_batch(self) -> bool:
        return bool(lib.mmapq_indexed_batch_poller_move_to_last_existing_batch(require_open(self._inner)))

    def move_to_start(self) -> bool:
        return bool(lib.mmapq_indexed_batch_poller_move_to_start(require_open(self._inner)))

    def current_batch_index(self) -> int:
        return lib.mmapq_indexed_batch_poller_current_batch_index(require_open(self._inner))

    def start_index(self) -> int:
        return lib.mmapq_indexed_batch_poller_start_index(require_open(self._inner))

    def exists(self, batch_index: int) -> bool:
        return bool(lib.mmapq_indexed_batch_poller_exists(require_open(self._inner), batch_index))

    def close(self):
        if self._current_handler is not None:
            raise MmapqError("cannot close a poller during its handler")
        if self._inner is not None:
            self._finalizer.detach()
            lib.mmapq_indexed_batch_poller_close(self._inner)
            self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
