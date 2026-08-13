"""Pure-Python enum types matching the C library's ABI."""

from __future__ import annotations

import enum


class NextMove(enum.IntEnum):
    """Direction returned by a poll handler to control cursor movement.

    Works for both single-message and batch pollers.  The handler returns
    one of these to tell the poller what to do with the cursor after the
    current entry (or batch).
    """

    ADVANCE = 0
    """Move cursor forward to the next entry (or batch)."""
    STOP = 1
    """Hold cursor at the current entry (or batch)."""
    RETREAT = 2
    """Move cursor backward to the previous entry (or batch)."""


class PollResult(enum.IntEnum):
    """Result of a single poll() call."""

    END_OF_STREAM = 0
    """No entry (or batch) at the current index; retry on next poll()."""
    ADVANCED = 1
    """Entry (or batch) delivered and cursor moved forward."""
    STOPPED = 2
    """Entry (or batch) delivered and cursor held (handler returned STOP)."""
    RETREATED = 3
    """Entry (or batch) delivered and cursor moved backward."""
    ARCHIVED = 4
    """Current index is below the queue's start index (archived)."""
    ERROR = 5
    """Mapping timeout or I/O error."""
    CORRUPT = 6
    """Committed payload data is corrupt."""


# Alias matching C's `typedef PollResult BatchPollResult`.
BatchPollResult = PollResult


class EntryStatus(enum.IntEnum):
    """Availability of a queue entry at a given index."""

    AVAILABLE = 0
    """The entry exists and is readable."""
    END_OF_STREAM = 1
    """No entry has been committed at this index yet."""
    ARCHIVED = 2
    """The entry was archived and is no longer accessible."""
    ERROR = 3
    """A mapping timeout or I/O error occurred."""


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
