"""Queue transfer: export, import, and copy queues.

Module-level functions that operate on single-message and batch queues.
Filtering is not supported in v1 — all entries are transferred.
"""

from __future__ import annotations

from typing import Optional

from . import _cffi as _cffi_mod
from .reader import IndexedReader, IndexedBatchReader
from ._errors import check_rc

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib

_REBASE = 0
_PRESERVE = 1


def _mode(preserve: bool) -> int:
    return _PRESERVE if preserve else _REBASE


def _check_no_preserve_append(preserve: bool, allow_append: bool) -> None:
    if preserve and allow_append:
        raise ValueError(
            "cannot preserve start index on an existing queue. "
            "Use preserve=False when allow_append=True."
        )


def export_queue(directory: str, queue_name: str, dest_path: str, *,
                 from_index: int = 0,
                 to_index: Optional[int] = None,
                 preserve: bool = False) -> int:
    """Export entries from a queue to a portable flat stream file.

    Raises:
        OSError: cannot open destination file for writing.
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
                from_index, to_index, _mode(preserve),
                ffi.NULL, ffi.NULL, ffi.NULL,
            )
            return check_rc(n, "export_queue")
        finally:
            lib.fclose(dest)
    finally:
        reader.close()


def import_queue(src_path: str, directory: str,
                 queue_name: str, *,
                 start_index: int = 0,
                 allow_append: bool = False,
                 preserve: bool = False) -> int:
    """Import entries from a flat stream file into a queue.

    Raises:
        OSError: cannot open source file for reading.
        QueueExistsError: destination exists and allow_append=False.
        ValueError: preserve=True with allow_append=True.
    """
    _check_no_preserve_append(preserve, allow_append)
    src = lib.fopen(src_path.encode(), b"rb")
    if not src:
        raise OSError(f"cannot open {src_path!r} for reading")
    try:
        n = lib.queue_transfer_import(
            src, directory.encode(), queue_name.encode(),
            start_index, _mode(preserve), ffi.NULL, ffi.NULL,
            int(allow_append),
        )
        return check_rc(n, "import_queue")
    finally:
        lib.fclose(src)


def import_batch_stream(src_path: str, directory: str,
                         queue_name: str, *,
                         start_index: int = 0,
                         allow_append: bool = False,
                         preserve: bool = False) -> int:
    """Import a batch-format stream into a normal queue (flatten).

    Each batch's messages become individual entries in the destination.

    Raises:
        OSError: cannot open source file for reading.
        QueueExistsError: destination exists and allow_append=False.
        ValueError: preserve=True with allow_append=True.
    """
    _check_no_preserve_append(preserve, allow_append)
    src = lib.fopen(src_path.encode(), b"rb")
    if not src:
        raise OSError(f"cannot open {src_path!r} for reading")
    try:
        n = lib.queue_transfer_import_batch_stream(
            src, directory.encode(), queue_name.encode(),
            start_index, _mode(preserve), ffi.NULL, ffi.NULL,
            int(allow_append),
        )
        return check_rc(n, "import_batch_stream")
    finally:
        lib.fclose(src)


def import_flat_to_batch(src_path: str, directory: str,
                          queue_name: str, *,
                          start_index: int = 0,
                          allow_append: bool = False,
                          preserve: bool = False) -> int:
    """Import a flat stream into a batch queue (1-msg batches).

    Each entry becomes its own 1-element batch.

    Raises:
        OSError: cannot open source file for reading.
        QueueExistsError: destination exists and allow_append=False.
        ValueError: preserve=True with allow_append=True.
    """
    _check_no_preserve_append(preserve, allow_append)
    src = lib.fopen(src_path.encode(), b"rb")
    if not src:
        raise OSError(f"cannot open {src_path!r} for reading")
    try:
        n = lib.batch_queue_transfer_import_flat_stream(
            src, directory.encode(), queue_name.encode(),
            start_index, _mode(preserve), ffi.NULL, ffi.NULL,
            int(allow_append),
        )
        return check_rc(n, "import_flat_to_batch")
    finally:
        lib.fclose(src)


def copy_queue(src_dir: str, src_name: str,
               dst_dir: str, dst_name: str, *,
               allow_append: bool = False,
               preserve: bool = False) -> int:
    """Copy entries from one queue to another.

    Raises:
        QueueExistsError: destination exists and allow_append=False.
        ValueError: preserve=True with allow_append=True.
    """
    _check_no_preserve_append(preserve, allow_append)
    reader = IndexedReader.create(src_dir, src_name)
    try:
        last = reader.last_index()
        if last < 0:
            return 0
        n = lib.queue_transfer_copy(
            reader._inner,
            dst_dir.encode(), dst_name.encode(),
            0, last, _mode(preserve),
            ffi.NULL, ffi.NULL, ffi.NULL,
            ffi.NULL, ffi.NULL,
            int(allow_append),
        )
        return check_rc(n, "copy_queue")
    finally:
        reader.close()


def copy_batch_queue(src_dir: str, src_name: str,
                     dst_dir: str, dst_name: str, *,
                     allow_append: bool = False,
                     preserve: bool = False) -> int:
    """Copy batches from one batch queue to another.

    Raises:
        QueueExistsError: destination exists and allow_append=False.
        ValueError: preserve=True with allow_append=True.
    """
    _check_no_preserve_append(preserve, allow_append)
    reader = IndexedBatchReader.create(src_dir, src_name)
    try:
        last = reader.last_index()
        if last < 0:
            return 0
        n = lib.batch_queue_transfer_copy(
            reader._inner,
            dst_dir.encode(), dst_name.encode(),
            0, last, _mode(preserve),
            ffi.NULL, ffi.NULL, ffi.NULL,
            ffi.NULL, ffi.NULL,
            int(allow_append),
        )
        return check_rc(n, "copy_batch_queue")
    finally:
        reader.close()


def copy_normal_to_batch(src_dir: str, src_name: str,
                         dst_dir: str, dst_name: str, *,
                         allow_append: bool = False,
                         preserve: bool = False) -> int:
    """Copy entries from a normal queue into a batch queue (1-msg batches).

    Raises:
        QueueExistsError: destination exists and allow_append=False.
        ValueError: preserve=True with allow_append=True.
    """
    _check_no_preserve_append(preserve, allow_append)
    reader = IndexedReader.create(src_dir, src_name)
    try:
        last = reader.last_index()
        if last < 0:
            return 0
        n = lib.batch_queue_transfer_copy_normal_to_batch(
            reader._inner,
            dst_dir.encode(), dst_name.encode(),
            0, last, _mode(preserve),
            ffi.NULL, ffi.NULL, ffi.NULL,
            ffi.NULL, ffi.NULL,
            int(allow_append),
        )
        return check_rc(n, "copy_normal_to_batch")
    finally:
        reader.close()


def copy_batch_to_normal(src_dir: str, src_name: str,
                         dst_dir: str, dst_name: str, *,
                         allow_append: bool = False,
                         preserve: bool = False) -> int:
    """Copy batches from a batch queue into a normal queue (flatten).

    Raises:
        QueueExistsError: destination exists and allow_append=False.
        ValueError: preserve=True with allow_append=True.
    """
    _check_no_preserve_append(preserve, allow_append)
    reader = IndexedBatchReader.create(src_dir, src_name)
    try:
        last = reader.last_index()
        if last < 0:
            return 0
        n = lib.queue_transfer_copy_batch_to_normal(
            reader._inner,
            dst_dir.encode(), dst_name.encode(),
            0, last, _mode(preserve),
            ffi.NULL, ffi.NULL, ffi.NULL,
            ffi.NULL, ffi.NULL,
            int(allow_append),
        )
        return check_rc(n, "copy_batch_to_normal")
    finally:
        reader.close()
