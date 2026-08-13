"""MmapqIndexedPoller — sequential single-message consumer.

Each poll() delivers one message. The handler receives the index, appender_id,
and message data as bytes, then returns a MmapqNextMove directive.
"""

from __future__ import annotations

import weakref

from . import _cffi as _cffi_mod
from ._cdata import require_cdata

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib
from .types import MmapqNextMove, MmapqPollResult
from ._errors import QueueCorruptError, check_rc


def _destroy_poller(inner):
    if inner is not None:
        lib.mmapq_indexed_poller_close(inner)


@ffi.callback("MmapqNextMove(int64_t, uint8_t, const void*, int32_t, void*)",
              error=MmapqNextMove.STOP)
def _c_trampoline(index, appender_id, data_ptr, length, user_data):
    """Called from C — dispatches to the poller's current handler."""
    poller = ffi.from_handle(user_data)
    handler = poller._current_handler
    if handler is None:
        return MmapqNextMove.STOP
    try:
        data = bytes(ffi.buffer(data_ptr, length))
        return handler(index, appender_id, data)
    except Exception as e:
        poller._pending_exc = e
        return MmapqNextMove.STOP


class MmapqIndexedPoller:
    """Sequential single-message consumer.

    Thread-confined. One per consumer thread.
    """

    def __init__(self, inner):
        require_cdata(inner, "struct MmapqIndexedPoller *", "MmapqIndexedPoller")
        self._inner = inner
        self._runtime = None
        self._handle = ffi.new_handle(self)
        self._current_handler = None
        self._pending_exc = None
        self._finalizer = weakref.finalize(self, _destroy_poller, inner)

    def poll(self, handler):
        """Poll for the next message, delivering it to the handler.

        The handler receives (index: int, appender_id: int, data: bytes)
        and must return a MmapqNextMove directive.

        Returns a MmapqPollResult describing what happened.
        Raises the handler's exception if one was raised during delivery.
        """
        self._current_handler = handler
        self._pending_exc = None
        try:
            result = ffi.cast("int", lib.mmapq_indexed_poller_poll(
                self._inner,
                _c_trampoline,
                self._handle,
            ))
            if self._pending_exc is not None:
                exc = self._pending_exc
                self._pending_exc = None
                raise exc
            if result == MmapqPollResult.ERROR:
                check_rc(lib.mmapq_indexed_poller_last_error(self._inner), "mmapq_indexed_poller_poll")
            if result == MmapqPollResult.CORRUPT:
                raise QueueCorruptError("poll: committed payload data is corrupt")
            return MmapqPollResult(result)
        finally:
            self._current_handler = None

    def move_to_index(self, index: int) -> bool:
        return bool(lib.mmapq_indexed_poller_move_to_index(self._inner, index))

    def move_to_end(self) -> bool:
        return bool(lib.mmapq_indexed_poller_move_to_end(self._inner))

    def move_to_last(self) -> bool:
        return bool(lib.mmapq_indexed_poller_move_to_last(self._inner))

    def move_to_start(self) -> bool:
        return bool(lib.mmapq_indexed_poller_move_to_start(self._inner))

    def current_index(self) -> int:
        return lib.mmapq_indexed_poller_current_index(self._inner)

    def start_index(self) -> int:
        return lib.mmapq_indexed_poller_start_index(self._inner)

    def exists(self, index: int) -> bool:
        return bool(lib.mmapq_indexed_poller_exists(self._inner, index))

    def close(self):
        if self._inner is not None:
            self._finalizer.detach()
            lib.mmapq_indexed_poller_close(self._inner)
            self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
