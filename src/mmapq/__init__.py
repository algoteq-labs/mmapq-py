"""mmapq — Python bindings for the mmapq batch queue."""

from . import _cffi as _cffi_mod

# Expose ffi and lib for advanced use (iterator access, etc.)
ffi = _cffi_mod.ffi
lib = _cffi_mod.lib

from ._errors import (
    QueueClosedError,
    QueueCorruptError,
    SchemaMismatchError,
    QueueExistsError,
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
from .transfer import (
    export_queue, import_queue, import_batch_stream, import_flat_to_batch,
    copy_queue, copy_batch_queue, copy_normal_to_batch, copy_batch_to_normal,
)
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
    "import_batch_stream",
    "import_flat_to_batch",
    "copy_queue",
    "copy_batch_queue",
    "copy_normal_to_batch",
    "copy_batch_to_normal",
    "Runtime",
    "IndexedArchiver",
    "QueueClosedError",
    "QueueCorruptError",
    "SchemaMismatchError",
    "QueueExistsError",
]
