"""mmapq — Python bindings for the mmapq batch queue."""

from . import _cffi as _cffi_mod

# Expose ffi and lib for advanced use (iterator access, etc.)
ffi = _cffi_mod.ffi
lib = _cffi_mod.lib

from ._errors import (
    QueueClosedError,
    QueueCorruptError,
    SchemaMismatchError,
    check_rc,
)
from .types import (
    NextMove,
    PollResult,
    BatchPollResult,
    BatchEntryStatus,
)
from .single_queue import IndexedQueue
from .single_appender import IndexedAppender
from .single_poller import IndexedPoller
from .batch_queue import IndexedBatchQueue
from .batch_poller import IndexedBatchPoller
from .batch_appender import IndexedBatchAppender, BatchWriter
from .reader import IndexedReader, IndexedBatchReader, EntryStatus
from .transfer import export_queue, import_queue, copy_queue
from .runtime import Runtime
from .archiver import IndexedArchiver

__all__ = [
    "NextMove",
    "PollResult",
    "BatchPollResult",
    "BatchEntryStatus",
    "IndexedQueue",
    "IndexedAppender",
    "IndexedPoller",
    "IndexedBatchQueue",
    "IndexedBatchPoller",
    "IndexedBatchAppender",
    "BatchWriter",
    "IndexedReader",
    "IndexedBatchReader",
    "EntryStatus",
    "export_queue",
    "import_queue",
    "copy_queue",
    "Runtime",
    "IndexedArchiver",
    "QueueClosedError",
    "QueueCorruptError",
    "SchemaMismatchError",
]
