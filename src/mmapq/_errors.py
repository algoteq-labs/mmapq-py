"""Error types and return-code mapping for mmapq operations."""

from __future__ import annotations


class QueueClosedError(RuntimeError):
    """The queue or participant has been closed."""


class QueueCorruptError(RuntimeError):
    """On-disk corruption detected (e.g., committed zero-length message)."""


class SchemaMismatchError(RuntimeError):
    """Schema version or queue type mismatch on open."""


_RC_ERRORS = {
    -1: (RuntimeError, "mapping timed out or I/O error"),
    -2: (ValueError, "invalid argument"),
    -3: (QueueClosedError, "queue/appender is closed"),
    -4: (QueueCorruptError, "on-disk corruption detected"),
    -5: (SchemaMismatchError, "schema version or queue type mismatch"),
}


def check_rc(rc: int, label: str = "operation") -> int:
    """Check a C return code and raise the appropriate exception if negative.

    Returns ``rc`` unchanged on success (``rc >= 0``).
    """
    if rc >= 0:
        return rc
    exc, msg = _RC_ERRORS.get(rc, (RuntimeError, f"unexpected return code {rc}"))
    raise exc(f"{label}: {msg} (rc={rc})")
