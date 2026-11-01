"""IndexedArchiver — manage archiving of queue entries."""

from __future__ import annotations

import weakref

from . import _cffi as _cffi_mod
from ._cdata import require_cdata, require_open
from ._errors import check_rc

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib


def _destroy_archiver(inner):
    if inner is not None:
        lib.mmapq_indexed_archiver_close(inner)


class IndexedArchiver:
    """Archiver for a single-message indexed queue.

    Advances the queue's start index so that old entries become
    inaccessible and their backing files can be safely deleted.
    Thread-confined. One per consumer/archiver thread.
    """

    def __init__(self, inner):
        require_cdata(inner, "struct MmapqIndexedArchiver *", "IndexedArchiver")
        self._inner = inner
        self._finalizer = weakref.finalize(self, _destroy_archiver, inner)

    @classmethod
    def create(cls, directory: str, queue_name: str, *,
               ring_size: int = 4,
               runtime=None) -> IndexedArchiver:
        """Create an archiver for the named queue.

        directory:   path to the queue data directory
        queue_name:  logical queue name
        ring_size:   number of simultaneously mapped regions
        runtime:     optional Runtime for async region mapping (keyword-only)

        The queue must have been created before archiving is possible.
        """
        rt = require_open(runtime._inner) if runtime is not None else ffi.NULL
        ptr = ffi.new("MmapqIndexedArchiver **")
        rc = lib.mmapq_indexed_archiver_create(
            directory.encode(), queue_name.encode(),
            ring_size, rt, ptr,
        )
        check_rc(rc, "mmapq_indexed_archiver_create")
        return cls(ptr[0])

    def archivable(self) -> bool:
        """Return True if the queue was created with archivable=True.

        Raises while the queue's metadata is not determinable yet; the
        call is retryable on this same archiver — do not recreate it. The
        retryable exceptions are FileNotFoundError (MMAPQ_ENOENT),
        QueueNotReadyError (MMAPQ_ENOTREADY) and MmapqError (MMAPQ_ETIMEDOUT);
        every other failure is terminal and raises the matching exception
        via check_rc.
        """
        out = ffi.new("int *")
        rc = lib.mmapq_indexed_archiver_archivable_checked(
            require_open(self._inner), out)
        check_rc(rc, "archivable")
        return bool(out[0])

    def archive_up_to_index(self, index: int) -> None:
        """Archive all entries up to (but not including) the given index.

        Advances start_index so that entries below it are inaccessible.
        Raises ValueError if the queue is not archivable.
        """
        check_rc(lib.mmapq_indexed_archiver_archive_up_to_index(require_open(self._inner), index),
                 "archive_up_to_index")

    def archivable_files(self) -> list[str]:
        """Return paths to files fully covered by the archive boundary.

        Files no live entry can need at the time of the call. Delete them
        with reclaim_files() (always safe). If you delete, move or compress
        them yourself, they are safe provided no undo (archive_up_to_index
        below the current start_index) runs between this call and your last
        unlink. An undo that ran before this call is harmless, because the
        listing already reflects it (see doc/design/archiving-design.md).
        """
        max_paths = 256
        arr = ffi.new("char *[256]")
        n = lib.mmapq_indexed_archiver_archivable_files(
            require_open(self._inner), arr, max_paths)
        check_rc(n, "archivable_files")
        result = []
        for i in range(n):
            result.append(ffi.string(arr[i]).decode())
        return result

    def start_index(self) -> int:
        """Return the current start index (first unarchived entry)."""
        return lib.mmapq_indexed_archiver_start_index(require_open(self._inner))

    def close(self) -> None:
        """Close the archiver. Idempotent."""
        self._finalizer.detach()
        _destroy_archiver(self._inner)
        self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def _destroy_batch_archiver(inner):
    if inner is not None:
        lib.mmapq_indexed_batch_archiver_close(inner)


class IndexedBatchArchiver:
    """Archiver for an indexed batch queue.

    Advances the queue's start index so that old batches become
    inaccessible and their backing files can be safely deleted.
    Thread-confined. One per consumer/archiver thread.
    """

    def __init__(self, inner):
        require_cdata(inner, "struct MmapqIndexedBatchArchiver *", "IndexedBatchArchiver")
        self._inner = inner
        self._finalizer = weakref.finalize(self, _destroy_batch_archiver, inner)

    def archivable(self) -> bool:
        """Return True if the queue was created with archivable=True.

        Raises while the queue's metadata is not determinable yet; the
        call is retryable on this same archiver — do not recreate it. The
        retryable exceptions are FileNotFoundError (MMAPQ_ENOENT),
        QueueNotReadyError (MMAPQ_ENOTREADY) and MmapqError (MMAPQ_ETIMEDOUT);
        every other failure is terminal and raises the matching exception
        via check_rc.
        """
        out = ffi.new("int *")
        rc = lib.mmapq_indexed_batch_archiver_archivable_checked(
            require_open(self._inner), out)
        check_rc(rc, "archivable")
        return bool(out[0])

    def archive_up_to_index(self, index: int) -> None:
        """Archive all batches up to (but not including) the given index."""
        check_rc(lib.mmapq_indexed_batch_archiver_archive_up_to_index(require_open(self._inner), index),
                 "archive_up_to_index")

    def archivable_files(self) -> list[str]:
        """Return paths to files fully covered by the archive boundary."""
        max_paths = 256
        arr = ffi.new("char *[256]")
        n = lib.mmapq_indexed_batch_archiver_archivable_files(
            require_open(self._inner), arr, max_paths)
        check_rc(n, "archivable_files")
        result = []
        for i in range(n):
            result.append(ffi.string(arr[i]).decode())
        return result

    def start_index(self) -> int:
        """Return the current start index (first unarchived batch)."""
        return lib.mmapq_indexed_batch_archiver_start_index(require_open(self._inner))

    def close(self) -> None:
        """Close the archiver. Idempotent."""
        self._finalizer.detach()
        _destroy_batch_archiver(self._inner)
        self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
