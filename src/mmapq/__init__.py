"""mmapq — Python bindings for the mmapq batch queue."""

from . import _cffi as _cffi_mod

# Expose ffi and lib for advanced use (iterator access, etc.)
ffi = _cffi_mod.ffi
lib = _cffi_mod.lib

from ._errors import (
    AppenderIdConflictError,
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
    ConflictBehavior,
)
from .single_queue import IndexedQueue
from .single_appender import IndexedAppender
from .single_poller import IndexedPoller
from .batch_queue import IndexedBatchQueue
from .batch_poller import IndexedBatchPoller
from .batch_appender import IndexedBatchAppender, BatchWriter
from .reader import IndexedReader, IndexedBatchReader, EntryStatus
from .transfer import (
    APPENDER_ID_AUTO,
    export_queue, export_batch_queue,
    import_queue, import_batch_stream, import_batch_queue,
    import_flat_to_batch,
    copy_queue, copy_batch_queue, copy_normal_to_batch, copy_batch_to_normal,
)
from .runtime import Runtime
from .archiver import IndexedArchiver

__all__ = [
    "NextMove",
    "PollResult",
    "BatchPollResult",
    "BatchEntryStatus",
    "ConflictBehavior",
    "APPENDER_ID_AUTO",
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
    "export_batch_queue",
    "import_queue",
    "import_batch_stream",
    "import_batch_queue",
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
    "AppenderIdConflictError",
    "check_rc",
]
