"""IndexedPoller — sequential single-message consumer.

Each poll() delivers one message. The handler receives the index, appender_id,
and message data as bytes, then returns a NextMove directive.
"""

from __future__ import annotations

import weakref

from . import _cffi as _cffi_mod

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib
from .types import NextMove, PollResult
from ._errors import QueueCorruptError, check_rc


def _destroy_poller(inner):
    if inner is not None:
        lib.indexed_poller_close(inner)


@ffi.callback("NextMove(int64_t, uint8_t, const void*, int32_t, void*)",
              error=NextMove.STOP)
def _c_trampoline(index, appender_id, data_ptr, length, user_data):
    """Called from C — dispatches to the poller's current handler."""
    poller = ffi.from_handle(user_data)
    handler = poller._current_handler
    if handler is None:
        return NextMove.STOP
    try:
        data = bytes(ffi.buffer(data_ptr, length))
        return handler(index, appender_id, data)
    except Exception as e:
        poller._pending_exc = e
        return NextMove.STOP


class IndexedPoller:
    """Sequential single-message consumer.

    Thread-confined. One per consumer thread.
    """

    def __init__(self, inner):
        self._inner = inner
        self._runtime = None
        self._handle = ffi.new_handle(self)
        self._current_handler = None
        self._pending_exc = None
        self._finalizer = weakref.finalize(self, _destroy_poller, inner)

    def poll(self, handler):
        """Poll for the next message, delivering it to the handler.

        The handler receives (index: int, appender_id: int, data: bytes)
        and must return a NextMove directive.

        Returns a PollResult describing what happened.
        Raises the handler's exception if one was raised during delivery.
        """
        self._current_handler = handler
        self._pending_exc = None
        try:
            result = ffi.cast("int", lib.indexed_poller_poll(
                self._inner,
                _c_trampoline,
                self._handle,
            ))
            if self._pending_exc is not None:
                exc = self._pending_exc
                self._pending_exc = None
                raise exc
            if result == PollResult.ERROR:
                check_rc(lib.indexed_poller_last_error(self._inner), "indexed_poller_poll")
            if result == PollResult.CORRUPT:
                raise QueueCorruptError("poll: committed payload data is corrupt")
            return PollResult(result)
        finally:
            self._current_handler = None

    def move_to_index(self, index: int) -> bool:
        return bool(lib.indexed_poller_move_to_index(self._inner, index))

    def move_to_end(self) -> bool:
        return bool(lib.indexed_poller_move_to_end(self._inner))

    def move_to_last(self) -> bool:
        return bool(lib.indexed_poller_move_to_last(self._inner))

    def move_to_start(self) -> bool:
        return bool(lib.indexed_poller_move_to_start(self._inner))

    def current_index(self) -> int:
        return lib.indexed_poller_current_index(self._inner)

    def start_index(self) -> int:
        return lib.indexed_poller_start_index(self._inner)

    def exists(self, index: int) -> bool:
        return bool(lib.indexed_poller_exists(self._inner, index))

    def close(self):
        if self._inner is not None:
            self._finalizer.detach()
            lib.indexed_poller_close(self._inner)
            self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
