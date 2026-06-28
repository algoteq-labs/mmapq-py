"""IndexedPoller — sequential single-message consumer.

Each poll() delivers one message. The handler receives the index, appender_id,
and message data as bytes, then returns a NextMove directive.
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


@ffi.callback("NextMove(int64_t, uint8_t, const void*, int32_t, void*)")
def _c_trampoline(index, appender_id, data_ptr, length, user_data):
    """Called from C — dispatches to the registered Python handler."""
    token = ffi.cast("intptr_t", user_data)
    handler = _handlers.get(token)
    if handler is None:
        return NextMove.STOP
    try:
        data = bytes(ffi.buffer(data_ptr, length))
        return handler(index, appender_id, data)
    except Exception:
        traceback.print_exc()
        return NextMove.STOP


# ── IndexedPoller ─────────────────────────────────────────────────────────

class IndexedPoller:
    """Sequential single-message consumer.

    Thread-confined. One per consumer thread.
    """

    def __init__(self, inner):
        self._inner = inner

    def poll(self, handler):
        """Poll for the next message, delivering it to the handler.

        The handler receives (index: int, appender_id: int, data: bytes)
        and must return a NextMove directive.

        Returns a PollResult describing what happened.
        """
        token = _register(handler)
        try:
            result = ffi.cast("int", lib.indexed_poller_poll(
                self._inner,
                _c_trampoline,
                ffi.cast("void*", token),
            ))
        finally:
            _unregister(token)
        return PollResult(result)

    def move_to_index(self, index: int) -> bool:
        """Move the cursor to a specific index.

        Returns True if the index is valid (>= start_index).
        """
        return bool(lib.indexed_poller_move_to_index(self._inner, index))

    def move_to_end(self) -> bool:
        """Move the cursor to one past the last committed entry.

        After this, poll() returns END_OF_STREAM until a new message arrives.
        """
        return bool(lib.indexed_poller_move_to_end(self._inner))

    def move_to_last(self) -> bool:
        """Move the cursor to the last committed entry.

        Returns True if a committed entry exists.
        """
        return bool(lib.indexed_poller_move_to_last(self._inner))

    def move_to_start(self) -> bool:
        """Move the cursor to the first unarchived entry."""
        return bool(lib.indexed_poller_move_to_start(self._inner))

    def current_index(self) -> int:
        """Return the current cursor position."""
        return lib.indexed_poller_current_index(self._inner)

    def start_index(self) -> int:
        """Return the first unarchived index (lowest readable)."""
        return lib.indexed_poller_start_index(self._inner)

    def exists(self, index: int) -> bool:
        """Return True if a committed entry exists at the given index.

        Does not alter the cursor.
        """
        return bool(lib.indexed_poller_exists(self._inner, index))

    def close(self):
        lib.indexed_poller_close(self._inner)
        self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __del__(self):
        if hasattr(self, '_inner') and self._inner is not None:
            lib.indexed_poller_close(self._inner)
