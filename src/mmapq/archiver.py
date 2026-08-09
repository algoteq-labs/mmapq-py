"""IndexedArchiver — manage archiving of queue entries."""

from __future__ import annotations

import weakref

from . import _cffi as _cffi_mod
from ._errors import check_rc

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib


def _destroy_archiver(inner):
    if inner is not None:
        lib.indexed_archiver_close(inner)


class IndexedArchiver:
    """Archiver for a single-message indexed queue.

    Advances the queue's start index so that old entries become
    inaccessible and their backing files can be safely deleted.
    Thread-confined. One per consumer/archiver thread.
    """

    def __init__(self, inner):
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
        rt = runtime._inner if runtime is not None else ffi.NULL
        ptr = ffi.new("IndexedArchiver **")
        rc = lib.indexed_archiver_create(
            directory.encode(), queue_name.encode(),
            ring_size, rt, ptr,
        )
        if rc != 0:
            raise RuntimeError(f"indexed_archiver_create failed with code {rc}")
        return cls(ptr[0])

    def archivable(self) -> bool:
        """Return True if the queue was created with archivable=True."""
        return bool(lib.indexed_archiver_archivable(self._inner))

    def archive_up_to_index(self, index: int) -> None:
        """Archive all entries up to (but not including) the given index.

        Advances start_index so that entries below it are inaccessible.
        Raises ValueError if the queue is not archivable.
        """
        check_rc(lib.indexed_archiver_archive_up_to_index(self._inner, index),
                 "archive_up_to_index")

    def archivable_files(self) -> list[str]:
        """Return paths to files fully covered by the archive boundary.

        These files are safe to delete, move, or compress.
        """
        max_paths = 256
        arr = ffi.new("char *[256]")
        n = lib.indexed_archiver_archivable_files(
            self._inner, arr, max_paths)
        check_rc(n, "archivable_files")
        result = []
        for i in range(n):
            result.append(ffi.string(arr[i]).decode())
        return result

    def start_index(self) -> int:
        """Return the current start index (first unarchived entry)."""
        return lib.indexed_archiver_start_index(self._inner)

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
