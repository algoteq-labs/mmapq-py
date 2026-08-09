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

from typing import Callable

from . import _cffi as _cffi_mod

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib
from .types import NextMove, PollResult
from ._errors import QueueCorruptError


def iter_batch_messages(ctx):
    """Iterate over messages in a BatchReadingContext.

    Returns a list of payload bytes for each message in the batch.
    The inner batch_iterator starts before message 0 — this function
    advances through all messages and returns their payloads.

    ctx: a cffi BatchReadingContext CData pointer
    Returns: list[bytes]
    """
    messages = []
    it_ptr = ffi.addressof(ctx.batch_iterator)
    while lib.batch_iterator_advance(it_ptr):
        length = lib.batch_iterator_current_length_ffi(it_ptr)
        data_ptr = lib.batch_iterator_current_data_ffi(it_ptr)
        messages.append(bytes(ffi.buffer(data_ptr, length)))
    return messages


def _destroy_batch_poller(inner):
    if inner is not None:
        lib.indexed_batch_poller_close(inner)


@ffi.callback("NextMove(int64_t, BatchReadingContext*, void*)",
              error=NextMove.STOP)
def _c_trampoline(batch_index, ctx_ptr, user_data):
    """Called from C — dispatches to the poller's current handler."""
    poller = ffi.from_handle(user_data)
    handler = poller._current_handler
    if handler is None:
        return NextMove.STOP
    try:
        messages = iter_batch_messages(ctx_ptr)
        return handler(batch_index, messages)
    except Exception as e:
        poller._pending_exc = e
        return NextMove.STOP


class IndexedBatchPoller:
    """Sequential batch consumer.

    Thread-confined. One per consumer thread.
    """

    def __init__(self, inner):
        self._inner = inner
        self._runtime = None
        self._handle = ffi.new_handle(self)
        self._current_handler = None
        self._pending_exc = None

    def poll(self, handler):
        """Poll the next batch, delivering it to the handler.

        The handler receives (batch_index, messages) where messages is
        a list[bytes] of the batch's payloads. The handler iterates
        over the messages and returns a NextMove directive.

        Returns a PollResult describing what happened.
        Raises the handler's exception if one was raised during delivery.
        """
        self._current_handler = handler
        self._pending_exc = None
        try:
            result = lib.indexed_batch_poller_poll(
                self._inner,
                _c_trampoline,
                self._handle,
            )
            if self._pending_exc is not None:
                exc = self._pending_exc
                self._pending_exc = None
                raise exc
            if result == PollResult.CORRUPT:
                raise QueueCorruptError("batch poll: committed batch payload is corrupt")
            return PollResult(result)
        finally:
            self._current_handler = None

    def move_to_index(self, index: int) -> bool:
        return bool(lib.indexed_batch_poller_move_to_index(self._inner, index))

    def move_to_end(self) -> bool:
        return bool(lib.indexed_batch_poller_move_to_end(self._inner))

    def move_to_last_existing_batch(self) -> bool:
        return bool(lib.indexed_batch_poller_move_to_last_existing_batch(self._inner))

    def move_to_start(self) -> bool:
        return bool(lib.indexed_batch_poller_move_to_start(self._inner))

    def current_batch_index(self) -> int:
        return lib.indexed_batch_poller_current_batch_index(self._inner)

    def start_index(self) -> int:
        return lib.indexed_batch_poller_start_index(self._inner)

    def exists(self, batch_index: int) -> bool:
        return bool(lib.indexed_batch_poller_exists(self._inner, batch_index))

    def close(self):
        if self._inner is not None:
            lib.indexed_batch_poller_close(self._inner)
            self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __del__(self):
        if self._inner is not None:
            lib.indexed_batch_poller_close(self._inner)
