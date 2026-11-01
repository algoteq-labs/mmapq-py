"""mmapq — Python bindings for the mmapq batch queue."""

from . import _cffi as _cffi_mod

# The raw cffi FFI and lib objects are private. Every C symbol reachable
# through them has no argument checking — one bad call segfaults the
# interpreter. Use the high-level API instead; reach the raw objects only
# through unsafe_ffi()/unsafe_lib() when you explicitly need them.
_ffi = _cffi_mod.ffi
_lib = _cffi_mod.lib


def unsafe_ffi():
    """Return the raw cffi ``FFI`` object.

    Unsafe: calling C symbols directly bypasses mmapq's validation, and a
    bad call can crash the interpreter. Prefer the high-level API. This is
    the escape hatch for advanced/low-level use (e.g. iterator access).
    """
    return _ffi


def unsafe_lib():
    """Return the raw cffi ``lib`` object (the loaded C library).

    Unsafe: same caveats as ``unsafe_ffi()``. Prefer the high-level API.
    """
    return _lib


from ._errors import (
    MmapqError,
    AppenderIdConflictError,
    MmapOutOfMemoryError,
    QueueClosedError,
    QueueCorruptError,
    QueueNotReadyError,
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
from .archiver import IndexedArchiver, IndexedBatchArchiver

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
    "IndexedBatchArchiver",
    "MmapqError",
    "QueueClosedError",
    "QueueCorruptError",
    "QueueNotReadyError",
    "SchemaMismatchError",
    "QueueExistsError",
    "AppenderIdConflictError",
    "MmapOutOfMemoryError",
    "check_rc",
    "unsafe_ffi",
    "unsafe_lib",
]
