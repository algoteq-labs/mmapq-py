"""Error types and return-code mapping for mmapq operations."""

from __future__ import annotations

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
    -9: (MmapOutOfMemoryError, "internal allocation failure"),
    -8: (AppenderIdConflictError, "appender ID already in use"),
    -7: (FileNotFoundError, "file or queue does not exist"),
    -6: (QueueExistsError, "destination queue already exists"),
    -5: (SchemaMismatchError, "schema version or queue type mismatch"),
    -4: (QueueCorruptError, "on-disk corruption detected"),
    -3: (QueueClosedError, "queue/appender is closed"),
    -2: (ValueError, "invalid argument"),
    -1: (MmapqError, "mapping timed out or I/O error"),
}


def check_rc(rc: int, label: str = "operation") -> int:
    """Check a C return code and raise the appropriate exception if negative.

    Returns ``rc`` unchanged on success (``rc >= 0``).
    """
    if rc >= 0:
        return rc
    exc, msg = _RC_ERRORS.get(rc, (RuntimeError, f"unexpected return code {rc}"))
    raise exc(f"{label}: {msg} (rc={rc})")
