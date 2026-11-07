"""Error types and return-code mapping for mmapq operations.

Message text is the single source of truth in C (src/mmapq_strerror.c).
_RC_ERRORS maps return codes to exception types only; the message string
comes from mmapq_strerror via cffi, so it can never diverge from C by
construction.
"""

from __future__ import annotations

from . import _cffi as _cffi_mod

_ffi = _cffi_mod.ffi
_lib = _cffi_mod.lib


class MmapqError(RuntimeError):
    """Base exception for all mmapq errors."""


class QueueClosedError(MmapqError):
    """The queue or participant has been closed."""


class QueueCorruptError(MmapqError):
    """On-disk corruption detected (e.g., committed zero-length message)."""


class SchemaMismatchError(MmapqError):
    """Schema version or queue type mismatch on open, or on-disk geometry
    not supported by this build/host (pre-CAND-38-rule geometry, or a
    page-size mismatch: a queue is portable only to hosts whose page size
    divides its region_size)."""


class QueueNotReadyError(MmapqError):
    """The queue metadata is not yet ready (absent, incomplete, or a
    mapping timeout). Transient — retry the same handle later."""


class QueueExistsError(MmapqError):
    """The destination queue already exists and allow_append was not set."""

class AppenderIdConflictError(MmapqError):
    """The mapped appender ID is already in use by the destination queue."""


class MmapOutOfMemoryError(MmapqError):
    """Internal allocation failure (calloc, strdup, etc.). Back off, do not retry hot-loop."""


class ArchivedError(MmapqError, LookupError):
    """A reader's read resolved to an index below the queue's start index.

    ``index`` is the index the read resolved to. An archived index stays
    unreadable unless an archiver undo lowers the start index again; an
    index that is merely not yet written reads as None instead. ARCHIVED is
    an entry status in C, not a return code, so this class has no
    _RC_ERRORS entry.
    """

    def __init__(self, index: int, message: str | None = None):
        super().__init__(message if message is not None
                         else f"index {index} is archived (below the queue's start index)")
        self.index = index



_RC_ERRORS = {
    -12: MmapqError,
    -11: QueueNotReadyError,
    -10: MmapqError,
    -9: MmapOutOfMemoryError,
    -8: AppenderIdConflictError,
    -7: FileNotFoundError,
    -6: QueueExistsError,
    -5: SchemaMismatchError,
    -4: QueueCorruptError,
    -3: QueueClosedError,
    -2: ValueError,
    -1: MmapqError,
}


def _c_message(rc: int) -> str:
    """Return the human-readable message for an MMAPQ_* error code from C."""
    ptr = _lib.mmapq_strerror(rc)
    if ptr == _ffi.NULL:
        return "unknown error"
    # ffi.string() returns bytes; decode to str for exception messages.
    return _ffi.string(ptr).decode("utf-8", errors="replace")


def check_rc(rc: int, label: str = "operation") -> int:
    """Check a C return code and raise the appropriate exception if negative.

    Returns ``rc`` unchanged on success (``rc >= 0``). The exception message
    is the C library's own text (mmapq_strerror.c).
    """
    if rc >= 0:
        return rc
    exc = _RC_ERRORS.get(rc, MmapqError)
    raise exc(f"{label}: {_c_message(rc)} (rc={rc})")
