"""mmapq — Python bindings for the mmapq batch queue."""

from . import _cffi as _cffi_mod

# Expose ffi and lib for advanced use (iterator access, etc.)
ffi = _cffi_mod.ffi
lib = _cffi_mod.lib

from ._errors import (
    MmapqError,
    AppenderIdConflictError,
    MmapOutOfMemoryError,
    QueueClosedError,
    QueueCorruptError,
    SchemaMismatchError,
    QueueExistsError,
    check_rc,
)
from .types import (
    MmapqNextMove,
    MmapqPollResult,
    BatchPollResult,
    MmapqBatchEntryStatus,
    MmapqConflictBehavior,
)
from .single_queue import MmapqIndexedQueue
from .single_appender import MmapqIndexedAppender
from .single_poller import MmapqIndexedPoller
from .batch_queue import MmapqIndexedBatchQueue
from .batch_poller import MmapqIndexedBatchPoller
from .batch_appender import MmapqIndexedBatchAppender, BatchWriter
from .reader import MmapqIndexedReader, MmapqIndexedBatchReader, MmapqEntryStatus
from .transfer import (
    APPENDER_ID_AUTO,
    export_queue, export_batch_queue,
    import_queue, import_batch_stream, import_batch_queue,
    import_flat_to_batch,
    copy_queue, copy_batch_queue, copy_normal_to_batch, copy_batch_to_normal,
)
from .runtime import MmapqRuntime
from .archiver import MmapqIndexedArchiver, MmapqIndexedBatchArchiver

__all__ = [
    "MmapqNextMove",
    "MmapqPollResult",
    "BatchPollResult",
    "MmapqBatchEntryStatus",
    "MmapqConflictBehavior",
    "APPENDER_ID_AUTO",
    "MmapqIndexedQueue",
    "MmapqIndexedAppender",
    "MmapqIndexedPoller",
    "MmapqIndexedBatchQueue",
    "MmapqIndexedBatchPoller",
    "MmapqIndexedBatchAppender",
    "BatchWriter",
    "MmapqIndexedReader",
    "MmapqIndexedBatchReader",
    "MmapqEntryStatus",
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
    "MmapqRuntime",
    "MmapqIndexedArchiver",
    "MmapqIndexedBatchArchiver",
    "MmapqError",
    "QueueClosedError",
    "QueueCorruptError",
    "SchemaMismatchError",
    "QueueExistsError",
    "AppenderIdConflictError",
    "MmapOutOfMemoryError",
    "check_rc",
]
