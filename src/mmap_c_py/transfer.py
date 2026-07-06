"""Queue transfer: export, import, and copy queues.

Module-level functions that operate on single-message queues.
Filtering is not supported in v1 — all entries are transferred.
"""

from __future__ import annotations

from typing import Optional

from . import _cffi as _cffi_mod
from .reader import IndexedReader
from ._errors import check_rc

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib

_REBASE = 0


def export_queue(directory: str, queue_name: str, dest_path: str, *,
                 from_index: int = 0,
                 to_index: Optional[int] = None) -> int:
    """Export entries from a queue to a portable flat stream file.

    Creates the queue reader, opens the destination file, and writes
    all entries in [from_index, to_index] range. The stream format
    includes a 64-byte ExportMeta header followed by framed entries.

    Filtering is not supported in v1 — all entries in range are exported.

    Args:
        directory:  base queue directory
        queue_name: queue subdirectory name
        dest_path:  output file path
        from_index: first index to export (inclusive, default 0)
        to_index:   last index to export (inclusive, default = last_index)

    Returns:
        Number of entries written.
    """
    reader = IndexedReader.create(directory, queue_name)
    try:
        if to_index is None:
            to_index = reader.last_index()
            if to_index < 0:
                return 0
        dest = lib.fopen(dest_path.encode(), "wb".encode())
        if not dest:
            raise OSError(f"cannot open {dest_path!r} for writing")
        try:
            n = lib.queue_transfer_export(
                reader._inner, dest,
                from_index, to_index, _REBASE,
                ffi.NULL, ffi.NULL, ffi.NULL,
            )
            return check_rc(n, "export_queue")
        finally:
            lib.fclose(dest)
    finally:
        reader.close()


def import_queue(src_path: str, directory: str,
                 queue_name: str) -> int:
    """Import a flat stream file into a queue.

    Creates the destination queue with parameters matching the stream
    header and appends all entries. Filtering is not supported in v1.

    Args:
        src_path:   input stream file path
        directory:  base queue directory
        queue_name: queue subdirectory name

    Returns:
        Number of entries imported.
    """
    src = lib.fopen(src_path.encode(), "rb".encode())
    if not src:
        raise OSError(f"cannot open {src_path!r} for reading")
    try:
        n = lib.queue_transfer_import(
            src, directory.encode(), queue_name.encode(),
            0, ffi.NULL, ffi.NULL,
        )
        return check_rc(n, "import_queue")
    finally:
        lib.fclose(src)


def copy_queue(src_dir: str, src_name: str,
               dst_dir: str, dst_name: str) -> int:
    """Copy entries from one queue to another.

    Creates the destination queue with the same configuration as the
    source and copies all entries. Filtering is not supported in v1.

    Args:
        src_dir:  source queue base directory
        src_name: source queue name
        dst_dir:  destination queue base directory
        dst_name: destination queue name

    Returns:
        Number of entries copied.
    """
    reader = IndexedReader.create(src_dir, src_name)
    try:
        last = reader.last_index()
        if last < 0:
            return 0
        n = lib.queue_transfer_copy(
            reader._inner,
            dst_dir.encode(), dst_name.encode(),
            0, last, _REBASE,
            ffi.NULL, ffi.NULL, ffi.NULL,
            ffi.NULL, ffi.NULL,
        )
        return check_rc(n, "copy_queue")
    finally:
        reader.close()
