"""Pure-Python enum types matching the C library's ABI."""

from __future__ import annotations

import enum


class NextMove(enum.IntEnum):
    """Direction returned by the batch handler to control cursor movement."""

    ADVANCE = 0
    """Move cursor forward to the next batch."""
    STOP = 1
    """Hold cursor at the current batch."""
    RETREAT = 2
    """Move cursor backward to the previous batch."""


class PollResult(enum.IntEnum):
    """Result of a single poll() call."""

    END_OF_STREAM = 0
    """No batch at the current index; retry on next poll()."""
    ADVANCED = 1
    """Batch delivered and cursor moved forward."""
    STOPPED = 2
    """Batch delivered and cursor held (handler returned STOP)."""
    RETREATED = 3
    """Batch delivered and cursor moved backward."""
    ARCHIVED = 4
    """Current index is below the queue's start index (archived)."""
    ERROR = 5
    """Mapping timeout or I/O error."""
    CORRUPT = 6
    """Committed payload data is corrupt."""


# Alias matching C's `typedef PollResult BatchPollResult`.
BatchPollResult = PollResult


class BatchEntryStatus(enum.IntEnum):
    """Availability of a batch entry at a given index."""

    AVAILABLE = 0
    """The batch exists and is readable."""
    END_OF_STREAM = 1
    """No batch has been committed at this index yet."""
    ARCHIVED = 2
    """The batch was archived and is no longer accessible."""
    ERROR = 3
    """A mapping timeout or I/O error occurred."""


class ConflictBehavior(enum.IntEnum):
    """What to do when a mapped appender ID is already in use."""

    AUTO = 0
    """Fall back to auto-assign (default, always succeeds)."""
    ABORT = 1
    """Return AppenderIdConflictError on conflict."""
