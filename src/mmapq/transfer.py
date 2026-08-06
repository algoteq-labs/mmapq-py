"""Queue transfer: export, import, and copy queues.

Module-level functions that operate on single-message and batch queues.
Filtering is not supported in v1 — all entries are transferred.
"""

from __future__ import annotations

from typing import Callable, Optional

from . import _cffi as _cffi_mod
from .reader import IndexedReader, IndexedBatchReader
from ._errors import check_rc
from .types import ConflictBehavior

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib

_REBASE = 0
_PRESERVE = 1

APPENDER_ID_AUTO = -1


@ffi.callback("int32_t(uint16_t, void*)", error=APPENDER_ID_AUTO)
def _mapper_trampoline(source_aid: int, ctx) -> int:
    fn = ffi.from_handle(ctx)
    result = fn(source_aid)
    if result is None:
        return APPENDER_ID_AUTO
    # Valid appender IDs: 0-255 (256 slots, 8-bit header field).
    if isinstance(result, int) and (result == APPENDER_ID_AUTO or 0 <= result <= 255):
        return result
    return APPENDER_ID_AUTO


def _validate_conflict(on_conflict):
    if on_conflict not in (ConflictBehavior.AUTO, ConflictBehavior.ABORT):
        raise ValueError(f"invalid ConflictBehavior: {on_conflict}")


def _setup_mapper(mapper):
    if mapper is None:
        return ffi.NULL, ffi.NULL
    if not callable(mapper):
        raise TypeError(f"mapper must be callable, got {type(mapper).__name__}")
    return _mapper_trampoline, ffi.new_handle(mapper)


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
                 allow_append: bool = False,
                 preserve: bool = False,
                 mapper: Optional[Callable[[int], int]] = None,
                 on_conflict: ConflictBehavior = ConflictBehavior.AUTO) -> int:
    """Import entries from a flat stream file into a queue.

    Start index is determined automatically:
      REBASE  → 0
      PRESERVE → the stream header's start_index

    Args:
        mapper: Maps source appender ID to destination ID.
                Return -1 (APPENDER_ID_AUTO) for auto-assign.
                Pass None (the parameter value) for identity mapping.
                **Note: if this function raises, the transfer
                continues with auto-assigned IDs.**
        on_conflict: ``ConflictBehavior.AUTO`` (default) or
                     ``ConflictBehavior.ABORT``.

    Raises:
        OSError: cannot open source file for reading.
        QueueExistsError: destination exists and allow_append=False.
        ValueError: preserve=True with allow_append=True.
        AppenderIdConflictError: on_conflict=ABORT and ID conflict.
    """
    _check_no_preserve_append(preserve, allow_append)
    _validate_conflict(on_conflict)
    src = lib.fopen(src_path.encode(), b"rb")
    if not src:
        raise OSError(f"cannot open {src_path!r} for reading")
    try:
        cb, ctx = _setup_mapper(mapper)
        n = lib.queue_transfer_import(
            src, directory.encode(), queue_name.encode(),
            _mode(preserve), ffi.NULL, ffi.NULL,
            int(allow_append),
            cb, ctx, on_conflict,
        )
        return check_rc(n, "import_queue")
    finally:
        lib.fclose(src)


def import_batch_stream(src_path: str, directory: str,
                         queue_name: str, *,
                         allow_append: bool = False,
                         preserve: bool = False,
                         mapper: Optional[Callable[[int], int]] = None,
                         on_conflict: ConflictBehavior = ConflictBehavior.AUTO) -> int:
    """
    Note: if the mapper raises, the transfer continues with
    auto-assigned appender IDs.

    Import a batch-format stream into a normal queue (flatten).

    Each batch's messages become individual entries in the destination.
    Each batch carries its own appender ID which is passed through
    the mapper.

    Raises:
        OSError: cannot open source file for reading.
        QueueExistsError: destination exists and allow_append=False.
        ValueError: preserve=True with allow_append=True.
        AppenderIdConflictError: on_conflict=ABORT and ID conflict.
    """
    _check_no_preserve_append(preserve, allow_append)
    _validate_conflict(on_conflict)
    src = lib.fopen(src_path.encode(), b"rb")
    if not src:
        raise OSError(f"cannot open {src_path!r} for reading")
    try:
        cb, ctx = _setup_mapper(mapper)
        n = lib.queue_transfer_import_batch_stream(
            src, directory.encode(), queue_name.encode(),
            _mode(preserve), ffi.NULL, ffi.NULL,
            int(allow_append),
            cb, ctx, on_conflict,
        )
        return check_rc(n, "import_batch_stream")
    finally:
        lib.fclose(src)


def export_batch_queue(directory: str, queue_name: str, dest_path: str, *,
                        from_index: int = 0,
                        to_index: Optional[int] = None,
                        preserve: bool = False) -> int:
    """Export batches from a batch queue to a portable batch stream file.

    Raises:
        OSError: cannot open destination file for writing.
    """
    reader = IndexedBatchReader.create(directory, queue_name)
    try:
        if to_index is None:
            to_index = reader.last_index()
            if to_index < 0:
                return 0
        dest = lib.fopen(dest_path.encode(), "wb".encode())
        if not dest:
            raise OSError(f"cannot open {dest_path!r} for writing")
        try:
            n = lib.batch_queue_transfer_export(
                reader._inner, dest,
                from_index, to_index, _mode(preserve),
                ffi.NULL, ffi.NULL, ffi.NULL,
            )
            return check_rc(n, "export_batch_queue")
        finally:
            lib.fclose(dest)
    finally:
        reader.close()


def import_batch_queue(src_path: str, directory: str,
                        queue_name: str, *,
                        allow_append: bool = False,
                        preserve: bool = False,
                        mapper: Optional[Callable[[int], int]] = None,
                        on_conflict: ConflictBehavior = ConflictBehavior.AUTO) -> int:
    """
    Note: if the mapper raises, the transfer continues with
    auto-assigned appender IDs.

    Import a batch-format stream into a batch queue.

    Reads a batched stream and creates batches in the destination queue.
    Each batch in the stream carries its own appender ID.

    Raises:
        OSError: cannot open source file for reading.
        QueueExistsError: destination exists and allow_append=False.
        ValueError: preserve=True with allow_append=True.
        AppenderIdConflictError: on_conflict=ABORT and ID conflict.
    """
    _check_no_preserve_append(preserve, allow_append)
    _validate_conflict(on_conflict)
    src = lib.fopen(src_path.encode(), b"rb")
    if not src:
        raise OSError(f"cannot open {src_path!r} for reading")
    try:
        cb, ctx = _setup_mapper(mapper)
        n = lib.batch_queue_transfer_import(
            src, directory.encode(), queue_name.encode(),
            _mode(preserve), ffi.NULL, ffi.NULL,
            int(allow_append),
            cb, ctx, on_conflict,
        )
        return check_rc(n, "import_batch_queue")
    finally:
        lib.fclose(src)


def import_flat_to_batch(src_path: str, directory: str,
                           queue_name: str, *,
                           allow_append: bool = False,
                           preserve: bool = False,
                           mapper: Optional[Callable[[int], int]] = None,
                           on_conflict: ConflictBehavior = ConflictBehavior.AUTO) -> int:
    """
    Note: if the mapper raises, the transfer continues with
    auto-assigned appender IDs.

    Import a flat stream into a batch queue (1-msg batches).

    Each entry becomes its own 1-element batch.
    Each entry's appender ID is passed through the mapper.

    Raises:
        OSError: cannot open source file for reading.
        QueueExistsError: destination exists and allow_append=False.
        ValueError: preserve=True with allow_append=True.
        AppenderIdConflictError: on_conflict=ABORT and ID conflict.
    """
    _check_no_preserve_append(preserve, allow_append)
    _validate_conflict(on_conflict)
    src = lib.fopen(src_path.encode(), b"rb")
    if not src:
        raise OSError(f"cannot open {src_path!r} for reading")
    try:
        cb, ctx = _setup_mapper(mapper)
        n = lib.batch_queue_transfer_import_flat_stream(
            src, directory.encode(), queue_name.encode(),
            _mode(preserve), ffi.NULL, ffi.NULL,
            int(allow_append),
            cb, ctx, on_conflict,
        )
        return check_rc(n, "import_flat_to_batch")
    finally:
        lib.fclose(src)


def copy_queue(src_dir: str, src_name: str,
               dst_dir: str, dst_name: str, *,
               allow_append: bool = False,
               preserve: bool = False,
               mapper: Optional[Callable[[int], int]] = None,
               on_conflict: ConflictBehavior = ConflictBehavior.AUTO) -> int:
    """
    Note: if the mapper raises, the transfer continues with
    auto-assigned appender IDs.

    Copy entries from one queue to another.

    Each entry's appender ID is passed through the mapper.

    Raises:
        QueueExistsError: destination exists and allow_append=False.
        ValueError: preserve=True with allow_append=True.
        AppenderIdConflictError: on_conflict=ABORT and ID conflict.
    """
    _check_no_preserve_append(preserve, allow_append)
    _validate_conflict(on_conflict)
    reader = IndexedReader.create(src_dir, src_name)
    try:
        last = reader.last_index()
        if last < 0:
            return 0
        cb, ctx = _setup_mapper(mapper)
        n = lib.queue_transfer_copy(
            reader._inner,
            dst_dir.encode(), dst_name.encode(),
            0, last, _mode(preserve),
            ffi.NULL, ffi.NULL, ffi.NULL,
            ffi.NULL, ffi.NULL,
            int(allow_append),
            cb, ctx, on_conflict,
        )
        return check_rc(n, "copy_queue")
    finally:
        reader.close()


def copy_batch_queue(src_dir: str, src_name: str,
                     dst_dir: str, dst_name: str, *,
                     allow_append: bool = False,
                     preserve: bool = False,
                     mapper: Optional[Callable[[int], int]] = None,
                     on_conflict: ConflictBehavior = ConflictBehavior.AUTO) -> int:
    """
    Note: if the mapper raises, the transfer continues with
    auto-assigned appender IDs.

    Copy batches from one batch queue to another.

    Each batch's appender ID is passed through the mapper.

    Raises:
        QueueExistsError: destination exists and allow_append=False.
        ValueError: preserve=True with allow_append=True.
        AppenderIdConflictError: on_conflict=ABORT and ID conflict.
    """
    _check_no_preserve_append(preserve, allow_append)
    _validate_conflict(on_conflict)
    reader = IndexedBatchReader.create(src_dir, src_name)
    try:
        last = reader.last_index()
        if last < 0:
            return 0
        cb, ctx = _setup_mapper(mapper)
        n = lib.batch_queue_transfer_copy(
            reader._inner,
            dst_dir.encode(), dst_name.encode(),
            0, last, _mode(preserve),
            ffi.NULL, ffi.NULL, ffi.NULL,
            ffi.NULL, ffi.NULL,
            int(allow_append),
            cb, ctx, on_conflict,
        )
        return check_rc(n, "copy_batch_queue")
    finally:
        reader.close()


def copy_normal_to_batch(src_dir: str, src_name: str,
                         dst_dir: str, dst_name: str, *,
                         allow_append: bool = False,
                         preserve: bool = False,
                         mapper: Optional[Callable[[int], int]] = None,
                         on_conflict: ConflictBehavior = ConflictBehavior.AUTO) -> int:
    """
    Note: if the mapper raises, the transfer continues with
    auto-assigned appender IDs.

    Copy entries from a normal queue into a batch queue (1-msg batches).

    Each entry's appender ID is passed through the mapper.

    Raises:
        QueueExistsError: destination exists and allow_append=False.
        ValueError: preserve=True with allow_append=True.
        AppenderIdConflictError: on_conflict=ABORT and ID conflict.
    """
    _check_no_preserve_append(preserve, allow_append)
    _validate_conflict(on_conflict)
    reader = IndexedReader.create(src_dir, src_name)
    try:
        last = reader.last_index()
        if last < 0:
            return 0
        cb, ctx = _setup_mapper(mapper)
        n = lib.batch_queue_transfer_copy_normal_to_batch(
            reader._inner,
            dst_dir.encode(), dst_name.encode(),
            0, last, _mode(preserve),
            ffi.NULL, ffi.NULL, ffi.NULL,
            ffi.NULL, ffi.NULL,
            int(allow_append),
            cb, ctx, on_conflict,
        )
        return check_rc(n, "copy_normal_to_batch")
    finally:
        reader.close()


def copy_batch_to_normal(src_dir: str, src_name: str,
                         dst_dir: str, dst_name: str, *,
                         allow_append: bool = False,
                         preserve: bool = False,
                         mapper: Optional[Callable[[int], int]] = None,
                         on_conflict: ConflictBehavior = ConflictBehavior.AUTO) -> int:
    """
    Note: if the mapper raises, the transfer continues with
    auto-assigned appender IDs.

    Copy batches from a batch queue into a normal queue (flatten).

    Each batch's appender ID is passed through the mapper.
    All messages within a batch share the same appender ID.

    Raises:
        QueueExistsError: destination exists and allow_append=False.
        ValueError: preserve=True with allow_append=True.
        AppenderIdConflictError: on_conflict=ABORT and ID conflict.
    """
    _check_no_preserve_append(preserve, allow_append)
    _validate_conflict(on_conflict)
    reader = IndexedBatchReader.create(src_dir, src_name)
    try:
        last = reader.last_index()
        if last < 0:
            return 0
        cb, ctx = _setup_mapper(mapper)
        n = lib.queue_transfer_copy_batch_to_normal(
            reader._inner,
            dst_dir.encode(), dst_name.encode(),
            0, last, _mode(preserve),
            ffi.NULL, ffi.NULL, ffi.NULL,
            ffi.NULL, ffi.NULL,
            int(allow_append),
            cb, ctx, on_conflict,
        )
        return check_rc(n, "copy_batch_to_normal")
    finally:
        reader.close()
