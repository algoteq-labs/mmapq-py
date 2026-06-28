"""IndexedBatchPoller — sequential batch consumer.

Each poll() delivers one batch. The handler receives the batch index and
a BatchReadingContext with an embedded BatchPayloadIterator positioned
before the first message. The handler must call batch_iterator_advance()
to iterate messages, then return a NextMove directive.
"""

from __future__ import annotations

import threading
import traceback
from typing import Callable

from . import _cffi as _cffi_mod

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib
from .types import NextMove, PollResult

# ── Global handler registry ────────────────────────────────────────────────
#
# cffi callbacks are C function pointers — they cannot capture Python closures.
# Instead, we register Python handlers in a global dict and pass a unique
# integer token through the void* ctx parameter.

_lock = threading.Lock()
_next_token = 0
_handlers: dict[int, Callable] = {}


def _register(handler: Callable) -> int:
    global _next_token
    with _lock:
        _next_token += 1
        token = _next_token
        _handlers[token] = handler
        return token


def _unregister(token: int) -> None:
    with _lock:
        _handlers.pop(token, None)


# ── C-callable trampoline ──────────────────────────────────────────────────

@ffi.callback("NextMove(int64_t, BatchReadingContext*, void*)")
def _c_trampoline(batch_index, ctx_ptr, user_data):
    """Called from C — dispatches to the registered Python handler."""
    # user_data is a void* encoding the integer token
    token = ffi.cast("intptr_t", user_data)
    handler = _handlers.get(token)
    if handler is None:
        return NextMove.STOP
    try:
        return handler(batch_index, ctx_ptr)
    except Exception:
        traceback.print_exc()
        return NextMove.STOP


# ── IndexedBatchPoller ─────────────────────────────────────────────────────

class IndexedBatchPoller:
    """Sequential batch consumer.

    Thread-confined. One per consumer thread.
    """

    def __init__(self, inner):
        self._inner = inner
        self._handler_token = None

    def poll(self, handler):
        """Poll the next batch, delivering it to the handler.

        The handler receives (batch_index, ctx) where ctx is a cffi CData
        pointer to a BatchReadingContext. Access fields via ctx.index,
        ctx.status, ctx.message_count, etc.

        The embedded batch_iterator starts before message 0 — call
        lib.batch_iterator_advance(ffi.addressof(ctx.batch_iterator))
        before reading data.

        Returns a PollResult describing what happened.
        """
        token = _register(handler)
        self._handler_token = token
        try:
            result = lib.indexed_batch_poller_poll(
                self._inner,
                _c_trampoline,
                ffi.cast("void*", token),
            )
        finally:
            _unregister(token)
            self._handler_token = None
        return PollResult(result)

    def move_to_index(self, index: int) -> bool:
        """Move the cursor to a specific batch index.

        Returns True if the index is valid (>= start_index).
        """
        return bool(lib.indexed_batch_poller_move_to_index(self._inner, index))

    def move_to_end(self) -> bool:
        """Move the cursor to one past the last committed batch.

        After this, poll() returns END_OF_STREAM until a new batch arrives.
        """
        return bool(lib.indexed_batch_poller_move_to_end(self._inner))

    def move_to_last_existing_batch(self) -> bool:
        """Move the cursor to the last committed batch.

        Unlike move_to_end(), this positions AT the last valid batch.
        Returns True if a committed batch exists.
        """
        return bool(lib.indexed_batch_poller_move_to_last_existing_batch(self._inner))

    def move_to_start(self) -> bool:
        """Move the cursor to the first unarchived batch index."""
        return bool(lib.indexed_batch_poller_move_to_start(self._inner))

    def current_batch_index(self) -> int:
        """Return the current batch index (cursor position)."""
        return lib.indexed_batch_poller_current_batch_index(self._inner)

    def start_index(self) -> int:
        """Return the first unarchived batch index (lowest readable)."""
        return lib.indexed_batch_poller_start_index(self._inner)

    def exists(self, batch_index: int) -> bool:
        """Return True if a committed batch exists at the given index.

        Does not alter the cursor.
        """
        return bool(lib.indexed_batch_poller_exists(self._inner, batch_index))

    def close(self):
        """Close the poller and release associated resources."""
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
