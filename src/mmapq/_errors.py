"""Error types and return-code mapping for mmapq operations.

Message text is the single source of truth in C (src/mmapq_strerror.c).
_RC_ERRORS maps return codes to exception types only; the message string
comes from mmapq_strerror via a dedicated cffi.FFI() (cffi API mode has a
known issue with char* return values — the wrapper frees static memory).
Using a separate dlopen avoids the bug entirely.
"""

from __future__ import annotations

import cffi as _cffi

from . import _cffi as _cffi_mod

_lib = _cffi_mod.lib

# cffi API mode cannot safely wrap functions returning char* — it assumes
# the returned pointer is heap-allocated and frees it after the first call,
# which corrupts the static string table in mmapq_strerror.c. Use a separate
# dlopen FFI that reads the pointer without freeing. The .so path comes from
# the loaded module's __file__, which is always correct per-build.
_so_ffi = _cffi.FFI()
_so_ffi.cdef('const char* mmapq_strerror(int rc);')
_so_lib = _so_ffi.dlopen(_cffi_mod.__file__)


class MmapqError(RuntimeError):
    """Base exception for all mmapq errors."""


class QueueClosedError(MmapqError):
    """The queue or participant has been closed."""


class QueueCorruptError(MmapqError):
    """On-disk corruption detected (e.g., committed zero-length message)."""


class SchemaMismatchError(MmapqError):
    """Schema version or queue type mismatch on open."""


class QueueExistsError(MmapqError):
    """The destination queue already exists and allow_append was not set."""

class AppenderIdConflictError(MmapqError):
    """The mapped appender ID is already in use by the destination queue."""


class MmapOutOfMemoryError(MmapqError):
    """Internal allocation failure (calloc, strdup, etc.). Back off, do not retry hot-loop."""



_RC_ERRORS = {
    -11: MmapqError,
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
    ptr = _so_lib.mmapq_strerror(rc)
    if ptr == _so_ffi.NULL:
        return "unknown error"
    # ffi.string() returns bytes; decode to str for exception messages.
    return _so_ffi.string(ptr).decode("utf-8", errors="replace")


def check_rc(rc: int, label: str = "operation") -> int:
    """Check a C return code and raise the appropriate exception if negative.

    Returns ``rc`` unchanged on success (``rc >= 0``). The exception message
    is the C library's own text (mmapq_strerror.c).
    """
    if rc >= 0:
        return rc
    exc = _RC_ERRORS.get(rc, MmapqError)
    raise exc(f"{label}: {_c_message(rc)} (rc={rc})")
