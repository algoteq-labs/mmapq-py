"""Tests for mmapq — single-message and batch queue bindings."""

import os
import shutil
import struct
import tempfile
import unittest

import gc

from mmapq import (
    NextMove,
    PollResult,
    BatchPollResult,
    IndexedQueue,
    IndexedAppender,
    IndexedPoller,
    IndexedBatchQueue,
    IndexedBatchAppender,
    IndexedBatchPoller,
    IndexedReader,
    IndexedBatchReader,
    IndexedArchiver,
    Runtime,
    MmapqError,
    SchemaMismatchError,
    QueueExistsError,
    QueueCorruptError,
    QueueNotReadyError,
    AppenderIdConflictError,
    ConflictBehavior,
    export_queue,
    export_batch_queue,
    import_queue,
    import_batch_queue,
    import_batch_stream,
    import_flat_to_batch,
    copy_queue,
    copy_batch_queue,
    copy_normal_to_batch,
    copy_batch_to_normal,
    check_rc,
    unsafe_ffi,
    unsafe_lib,
)

# Raw C symbols used by the low-level tests below. The public module-level
# ffi/lib were removed in 19f; tests that need raw access go through the
# documented escape hatches.
ffi = unsafe_ffi()
lib = unsafe_lib()


class TestBatchQueue(unittest.TestCase):
    """Integration tests for the batch queue bindings."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_test")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_create_queue(self):
        q = IndexedBatchQueue.create(self.test_dir, "test_q")
        self.assertIsNotNone(q)
        q.close()

    def test_create_appender_and_poller(self):
        q = IndexedBatchQueue.create(self.test_dir, "test_ap")
        a = q.create_appender()
        p = q.create_poller()
        self.assertIsNotNone(a)
        self.assertIsNotNone(p)
        a.close()
        p.close()
        q.close()

    def test_start_batch_closed_appender_guard_raises_closed(self):
        """CAND-50 item 4 (D4.3): pins the Python guard — a closed appender
        is refused by require_open before any FFI call (exact type). No
        negative rc from start_batch_checked is reachable from Python:
        EINVAL is impossible by construction or pre-empted (NULL out/app,
        the active-batch guard), ECLOSED is pre-empted here, ORPHANED
        recovery needs the C test seam, ETIMEDOUT needs a mapping stall.
        The rc → exception mapping itself is covered by the check_rc unit
        tests (TestErrors.test_eio_error_via_check_rc,
        TestErrors.test_exception_types_importable)."""
        from mmapq import QueueClosedError
        q = IndexedBatchQueue.create(self.test_dir, "test_sb_closed")
        a = q.create_appender()
        a.close()
        with self.assertRaises(QueueClosedError) as cm:
            a.start_batch()
        self.assertIs(type(cm.exception), QueueClosedError)
        q.close()

    def test_append_and_poll_one_batch(self):
        q = IndexedBatchQueue.create(self.test_dir, "test_b1")
        a = q.create_appender()
        p = q.create_poller()

        with a.start_batch() as w:
            w.append(b"hello")
            w.append(b"world")
            w.append(b"!")
            w.commit()

        results = []

        def handler(bi, msgs):
            results.append((bi, len(msgs)))
            return NextMove.ADVANCE

        result = p.poll(handler)
        self.assertEqual(result, PollResult.ADVANCED)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], 0)  # batch_index
        self.assertEqual(results[0][1], 3)  # message_count

        p.close()
        a.close()
        q.close()

    def test_poll_end_of_stream(self):
        q = IndexedBatchQueue.create(self.test_dir, "test_eos")
        p = q.create_poller()

        result = p.poll(lambda bi, _msgs: NextMove.ADVANCE)
        self.assertEqual(result, PollResult.END_OF_STREAM)

        p.close()
        q.close()

    def test_poll_stop(self):
        q = IndexedBatchQueue.create(self.test_dir, "test_stop")
        a = q.create_appender()
        p = q.create_poller()

        with a.start_batch() as w:
            w.append(b"msg")
            w.commit()

        result = p.poll(lambda bi, _msgs: NextMove.STOP)
        self.assertEqual(result, PollResult.STOPPED)

        p.close()
        a.close()
        q.close()

    def test_poll_retreat(self):
        q = IndexedBatchQueue.create(self.test_dir, "test_retreat")
        a = q.create_appender()
        p = q.create_poller()

        with a.start_batch() as w:
            w.append(b"batch0")
            w.commit()
        with a.start_batch() as w:
            w.append(b"batch1")
            w.commit()

        # Advance past batch 0
        p.poll(lambda bi, _msgs: NextMove.ADVANCE)
        # Retreat from batch 1
        result = p.poll(lambda bi, _msgs: NextMove.RETREAT)
        self.assertEqual(result, PollResult.RETREATED)
        # Now poll batch 0 again via ADVANCE
        result = p.poll(lambda bi, _msgs: NextMove.ADVANCE)
        self.assertEqual(result, PollResult.ADVANCED)

        p.close()
        a.close()
        q.close()

    def test_poller_move_and_exists(self):
        q = IndexedBatchQueue.create(self.test_dir, "test_move")
        a = q.create_appender()
        p = q.create_poller()

        self.assertFalse(p.exists(0))

        with a.start_batch() as w:
            w.append(b"x")
            w.commit()

        self.assertTrue(p.exists(0))
        self.assertFalse(p.exists(1))

        self.assertTrue(p.move_to_last_existing_batch())
        self.assertEqual(p.current_batch_index(), 0)

        p.close()
        a.close()
        q.close()

    def test_iterate_messages_in_handler(self):
        q = IndexedBatchQueue.create(self.test_dir, "test_iter")
        a = q.create_appender()
        p = q.create_poller()

        with a.start_batch() as w:
            w.append(b"alpha")
            w.append(b"beta")
            w.append(b"gamma")
            w.commit()

        messages = []

        def handler(bi, msgs):
            for msg in msgs:
                messages.append((bi, msg))
            return NextMove.ADVANCE

        result = p.poll(handler)
        self.assertEqual(result, PollResult.ADVANCED)
        self.assertEqual(len(messages), 3)
        self.assertEqual(messages[0], (0, b"alpha"))
        self.assertEqual(messages[1], (0, b"beta"))
        self.assertEqual(messages[2], (0, b"gamma"))

        p.close()
        a.close()
        q.close()

    def test_batch_append_rejects_oversized_message(self):
        q = IndexedBatchQueue.create(
            self.test_dir, "bq_oversized",
            region_size=32768, ring_size=16,
            runtime=Runtime.region_mapper())
        a = q.create_appender()

        with a.start_batch() as w:
            ok = w.append(b"small")
            self.assertEqual(ok, 0)

            big = b"x" * 32768
            with self.assertRaises(ValueError):
                w.append(big)

    def test_batch_recovery_refuses_truncated_scan(self):
        """A corrupt message length inside the last committed batch must
        make recovery (a fresh appender) refuse with QueueCorruptError
        rather than silently recover to a wrong append position (A6)."""
        q = IndexedBatchQueue.create(self.test_dir, "bq_rec_trunc")
        a = q.create_appender()
        for b in range(2):
            with a.start_batch() as w:
                for i in range(3):
                    self.assertEqual(w.append(f"b{b}-m{i}-padding".encode()), i)
                self.assertEqual(w.commit(), b)
        a.close()

        # Overwrite the length prefix of message 1 of the last committed
        # batch: 64 start-of-stream + 55 first batch (4-byte count +
        # 3*(4+13)) + 4 count + 17 first message (4+13) = offset 140.
        payload = os.path.join(self.test_dir, "bq_rec_trunc", "payload_0_0")
        with open(payload, "r+b") as f:
            f.seek(140)
            f.write(struct.pack("<i", -2147483632))  # 0x80000010 as int32

        with self.assertRaises(QueueCorruptError):
            q.create_appender()
        q.close()

    def _write_corruptable_batch(self, name):
        """Write one committed batch [AAAA, BBBB, CCCC], close, then corrupt
        the second message's length on disk (0x7FFFFFFF exceeds the region
        capacity, so the C iterator fails MMAPQ_ECORRUPT at message 2).
        Batch framing: @64 int32 count, then per message int32 len + data."""
        q = IndexedBatchQueue.create(self.test_dir, name)
        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"AAAA")
            w.append(b"BBBB")
            w.append(b"CCCC")
            w.commit()
        a.close()
        q.close()
        payload = os.path.join(self.test_dir, name, "payload_0_0")
        with open(payload, "r+b") as f:
            f.seek(64)
            (count,) = struct.unpack("<i", f.read(4))
            self.assertEqual(count, 3)
            (len1,) = struct.unpack("<i", f.read(4))
            self.assertEqual(len1, 4)
            f.seek(64 + 4 + 4 + len1)
            f.write(struct.pack("<i", 0x7FFFFFFF))

    def test_iter_batch_messages_mid_batch_corruption_raises(self):
        """CAND-51a F1 fail-first pin: the raw batch-message iteration helper
        must raise on a mid-batch corruption. Pre-fix it silently returned
        the truncated list [b'AAAA'] (observed); post-fix QueueCorruptError."""
        from mmapq.batch_poller import iter_batch_messages
        from mmapq.types import BatchEntryStatus
        self._write_corruptable_batch("bq_iter_corrupt")
        br = IndexedBatchReader.create(self.test_dir, "bq_iter_corrupt")
        ctx = ffi.new("MmapqBatchReadingContext *")
        lib.mmapq_indexed_batch_reader_read(br._inner, 0, ctx)
        self.assertEqual(ctx.status, BatchEntryStatus.AVAILABLE)
        with self.assertRaises(QueueCorruptError):
            iter_batch_messages(ctx)
        br.close()

    def test_batch_poll_mid_batch_corruption_raises_and_cursor_held(self):
        """CAND-51a contract pin: polling a batch whose middle message length
        is corrupt raises QueueCorruptError; the handler is never invoked
        and the cursor does not advance."""
        self._write_corruptable_batch("bq_poll_corrupt")
        q = IndexedBatchQueue.create(self.test_dir, "bq_poll_corrupt")
        p = q.create_poller()
        self.assertEqual(p.current_batch_index(), 0)
        seen = []
        with self.assertRaises(QueueCorruptError):
            p.poll(lambda bi, msgs: seen.extend(msgs) or NextMove.ADVANCE)
        self.assertEqual(seen, [])
        self.assertEqual(p.current_batch_index(), 0)
        p.close()
        q.close()

    def test_batch_reader_mid_batch_corruption_raises(self):
        """CAND-51a contract pin: reading a batch whose middle message length
        is corrupt raises QueueCorruptError, reproducibly."""
        self._write_corruptable_batch("bq_read_corrupt")
        br = IndexedBatchReader.create(self.test_dir, "bq_read_corrupt")
        with self.assertRaises(QueueCorruptError):
            br.read(0)
        with self.assertRaises(QueueCorruptError):
            br.read(0)
        br.close()

    def test_batch_reader_corruption_invalidates_last_read(self):
        """CAND-51a review fix pin: a failed read must invalidate the last
        valid read — after read() raises on a corrupt batch,
        batch_appender_id() raises MmapqError rather than returning the
        corrupt batch's appender id. Batch 0 (b'good') spans @64..76; batch 1
        ([AAAA,BBBB,CCCC]) starts @76 — its second message length (@88) is
        corrupted to 0x7FFFFFFF."""
        name = "bq_lastread"
        q = IndexedBatchQueue.create(self.test_dir, name)
        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"good")
            w.commit()
        with a.start_batch() as w:
            w.append(b"AAAA")
            w.append(b"BBBB")
            w.append(b"CCCC")
            w.commit()
        a.close()
        q.close()
        payload = os.path.join(self.test_dir, name, "payload_0_0")
        with open(payload, "r+b") as f:
            f.seek(76)
            (count,) = struct.unpack("<i", f.read(4))
            self.assertEqual(count, 3)
            (len1,) = struct.unpack("<i", f.read(4))
            self.assertEqual(len1, 4)
            f.seek(76 + 4 + 4 + len1)
            f.write(struct.pack("<i", 0x7FFFFFFF))

        br = IndexedBatchReader.create(self.test_dir, name)
        self.assertEqual(br.read(0), [b"good"])
        self.assertEqual(br.batch_appender_id(), 0)
        with self.assertRaises(QueueCorruptError):
            br.read(1)
        with self.assertRaises(MmapqError):
            br.batch_appender_id()
        br.close()



class TestSingleQueue(unittest.TestCase):
    """Tests for the single-message queue bindings."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_single_test")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_create_queue(self):
        q = IndexedQueue.create(self.test_dir, "sq")
        self.assertIsNotNone(q)
        q.close()

    def test_create_appender_and_poller(self):
        q = IndexedQueue.create(self.test_dir, "sq_ap")
        a = q.create_appender()
        p = q.create_poller()
        self.assertIsNotNone(a)
        self.assertIsNotNone(p)
        a.close()
        p.close()
        q.close()

    def test_append_and_poll(self):
        q = IndexedQueue.create(self.test_dir, "sq_ap1")
        a = q.create_appender()
        p = q.create_poller()

        a.append(b"hello")

        messages = []

        def handler(idx, aid, data):
            messages.append((idx, aid, data))
            return NextMove.ADVANCE

        result = p.poll(handler)
        self.assertEqual(result, PollResult.ADVANCED)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0][0], 0)
        self.assertEqual(messages[0][2], b"hello")

        p.close()
        a.close()
        q.close()

    def test_end_of_stream(self):
        q = IndexedQueue.create(self.test_dir, "sq_eos")
        p = q.create_poller()

        result = p.poll(lambda i, a, d: NextMove.ADVANCE)
        self.assertEqual(result, PollResult.END_OF_STREAM)

        p.close()
        q.close()

    def test_stop(self):
        q = IndexedQueue.create(self.test_dir, "sq_stop")
        a = q.create_appender()
        p = q.create_poller()

        a.append(b"msg")
        result = p.poll(lambda i, a, d: NextMove.STOP)
        self.assertEqual(result, PollResult.STOPPED)

        p.close()
        a.close()
        q.close()

    def test_poll_corrupt_payload_raises(self):
        from mmapq import QueueCorruptError
        q = IndexedQueue.create(self.test_dir, "sq_corrupt")
        a = q.create_appender()
        a.append(b"hello")
        a.close()
        payload_path = os.path.join(self.test_dir, "sq_corrupt", "payload_0_0")
        with open(payload_path, "r+b") as f:
            f.seek(64)
            f.write(b"\x00\x00\x00\x00")
        p = q.create_poller()
        with self.assertRaises(QueueCorruptError):
            p.poll(lambda i, a, d: NextMove.ADVANCE)
        p.close()
        q.close()

    def test_move_and_exists(self):
        q = IndexedQueue.create(self.test_dir, "sq_move")
        a = q.create_appender()
        p = q.create_poller()

        self.assertFalse(p.exists(0))
        a.append(b"x")
        self.assertTrue(p.exists(0))
        self.assertFalse(p.exists(1))

        self.assertTrue(p.move_to_last())
        self.assertEqual(p.current_index(), 0)

        p.close()
        a.close()
        q.close()

    def test_multiple_messages(self):
        q = IndexedQueue.create(self.test_dir, "sq_multi")
        a = q.create_appender()
        p = q.create_poller()

        for i in range(5):
            a.append(f"msg{i}".encode())

        seen = []

        def handler(idx, aid, data):
            seen.append((idx, data))
            return NextMove.ADVANCE

        for _ in range(5):
            p.poll(handler)

        self.assertEqual(len(seen), 5)
        for i in range(5):
            self.assertEqual(seen[i][1], f"msg{i}".encode())

        p.close()
        a.close()
        q.close()

class TestErrors(unittest.TestCase):
    """Tests for error handling."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_err_test")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_append_empty_raises_value_error(self):
        from mmapq import IndexedQueue
        q = IndexedQueue.create(self.test_dir, "err_empty")
        a = q.create_appender()
        with self.assertRaises(ValueError):
            a.append(b"")
        a.close()
        q.close()

    def test_exception_types_importable(self):
        from mmapq import (MmapqError, MmapOutOfMemoryError, QueueClosedError,
                           QueueCorruptError, SchemaMismatchError, QueueExistsError,
                           AppenderIdConflictError)
        self.assertTrue(issubclass(MmapqError, RuntimeError))
        self.assertTrue(issubclass(MmapOutOfMemoryError, MmapqError))
        self.assertTrue(issubclass(QueueClosedError, MmapqError))
        self.assertTrue(issubclass(QueueCorruptError, MmapqError))
        self.assertTrue(issubclass(SchemaMismatchError, MmapqError))
        self.assertTrue(issubclass(QueueExistsError, MmapqError))
        self.assertTrue(issubclass(AppenderIdConflictError, MmapqError))

    def test_eio_error_via_check_rc(self):
        from mmapq import MmapqError, check_rc
        with self.assertRaises(MmapqError):
            check_rc(-10, "test_eio")


class TestArchivable(unittest.TestCase):
    """Tests for archivable parameter."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_arch")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_archivable_true_sets_byte24(self):
        q = IndexedQueue.create(self.test_dir, "q", archivable=True)
        a = q.create_appender()
        a.append(b"stamp")
        a.close()
        meta_path = os.path.join(self.test_dir, "q", "meta")
        with open(meta_path, "rb") as f:
            f.seek(24)
            self.assertEqual(ord(f.read(1)), 1)
        q.close()

    def test_archivable_default_is_zero(self):
        q = IndexedQueue.create(self.test_dir, "q")
        a = q.create_appender()
        a.append(b"stamp")
        a.close()
        meta_path = os.path.join(self.test_dir, "q", "meta")
        with open(meta_path, "rb") as f:
            f.seek(24)
            self.assertEqual(ord(f.read(1)), 0)
        q.close()


class TestPollArchivedSurface(unittest.TestCase):
    """POLL-ARCH-01 outcome-level pins: the ARCHIVED surface on every
    consumer, after the archiver moves start_index past the index.

    The mid-call race (the archiver reclaiming between the pre-read
    start_index check and the read) is pinned in C
    (c/test/test_poll_arch_pins.c); the shipped extension builds the C
    mirror without test hooks by owner ruling, so these are the
    outcome-level pins: archive past the index, then poll/read/
    entry_status it. The batch poller's ARCHIVED surface is already
    pinned in tests/test_recovery.py
    (TestBatchRecovery.test_batch_archiver_forward_and_undo).
    """

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_pollarch")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_single_poller_archived_after_archive(self):
        q = IndexedQueue.create(self.test_dir, "q", archivable=True)
        a = q.create_appender()
        a.append(b"m0")
        a.append(b"m1")
        a.close()
        p = q.create_poller()
        # Poll once first: the poller initialises its cursor from
        # start_index at first poll, so the archive must happen after the
        # cursor exists (same shape as tests/test_recovery.py).
        self.assertEqual(p.poll(lambda i, aid, data: NextMove.ADVANCE),
                         PollResult.ADVANCED)
        arch = q.create_archiver()
        arch.archive_up_to_index(2)
        arch.close()
        # cursor 1 < start_index 2 → ARCHIVED, not an error
        self.assertEqual(p.poll(lambda i, aid, data: NextMove.ADVANCE),
                         PollResult.ARCHIVED)
        p.close()
        q.close()

    def test_reader_archived_returns_none_and_status(self):
        from mmapq.types import EntryStatus

        q = IndexedQueue.create(self.test_dir, "q", archivable=True)
        a = q.create_appender()
        a.append(b"m0")
        a.append(b"m1")
        a.close()
        r = IndexedReader.create(self.test_dir, "q")
        self.assertEqual(r.read(0), b"m0")
        arch = q.create_archiver()
        arch.archive_up_to_index(1)
        arch.close()
        self.assertEqual(r.entry_status(0), EntryStatus.ARCHIVED)
        self.assertIsNone(r.read(0))       # ARCHIVED collapses to None
        self.assertEqual(r.read(1), b"m1")  # above the line, still readable
        r.close()
        q.close()

    def test_batch_reader_archived_returns_none_and_status(self):
        from mmapq.types import BatchEntryStatus

        q = IndexedBatchQueue.create(self.test_dir, "bq", archivable=True)
        a = q.create_appender()
        w = a.start_batch()
        w.append(b"m0")
        self.assertEqual(w.commit(), 0)
        a.close()
        r = IndexedBatchReader.create(self.test_dir, "bq")
        self.assertEqual(r.read(0), [b"m0"])
        arch = q.create_archiver()
        arch.archive_up_to_index(1)
        arch.close()
        self.assertEqual(r.entry_status(0), BatchEntryStatus.ARCHIVED)
        self.assertIsNone(r.read(0))       # ARCHIVED collapses to None
        r.close()
        q.close()


class TestQueueType(unittest.TestCase):
    """Tests for queue_type discriminator rejection."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_qt")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_cross_type_open_rejected(self):
        q = IndexedQueue.create(self.test_dir, "q")
        a = q.create_appender()
        a.append(b"stamp-normal")
        a.close()
        q.close()
        with self.assertRaises(SchemaMismatchError):
            bq = IndexedBatchQueue.create(self.test_dir, "q")
            bq.create_appender()

    def test_reader_on_wrong_queue_type_raises(self):
        q = IndexedBatchQueue.create(self.test_dir, "bq")
        a = q.create_appender()
        w = a.start_batch()
        w.append(b"msg")
        w.commit()
        a.close()
        q.close()
        r = IndexedReader.create(self.test_dir, "bq")
        with self.assertRaises(SchemaMismatchError):
            r.read(0)

    def test_poller_wrong_type_raises_schema_mismatch(self):
        bq = IndexedBatchQueue.create(self.test_dir, "bbq")
        a = bq.create_appender()
        w = a.start_batch()
        w.append(b"msg")
        w.commit()
        a.close()
        bq.close()
        q = IndexedQueue.create(self.test_dir, "bbq")
        p = q.create_poller()
        with self.assertRaises(SchemaMismatchError):
            p.poll(lambda i, aid, data: NextMove.ADVANCE)

    def test_poller_before_queue_then_data_arrives(self):
        """Standalone poller on non-existent path: first poll END_OF_STREAM,
        then queue appears, next poll sees data."""
        polldir = os.path.join(self.test_dir, "pb")
        qname = "q"
        os.makedirs(polldir, exist_ok=True)
        ptr = ffi.new("MmapqIndexedPoller **")
        rc = lib.mmapq_indexed_poller_create(
            polldir.encode(), qname.encode(), 16, 10000000000, ffi.NULL, ptr)
        check_rc(rc, "create_poller")
        p = IndexedPoller(ptr[0])
        result1 = p.poll(lambda i, aid, data: NextMove.ADVANCE)
        self.assertEqual(result1, PollResult.END_OF_STREAM,
                         "first poll on empty path should be END_OF_STREAM")
        q = IndexedQueue.create(polldir, qname)
        a = q.create_appender()
        a.append(b"hello")
        a.close()
        q.close()
        result2 = p.poll(lambda i, aid, data: NextMove.ADVANCE)
        self.assertEqual(result2, PollResult.ADVANCED,
                         "poll after queue created should see data")
        p.close()

    def test_poll_header_timeout_raises_not_eos(self):
        """CAND-17(a): a mapping timeout on the poller's header read raises
        MmapqError (rc=-1, mapping timeout — text from C mmapq_strerror),
        never returns END_OF_STREAM, and the cursor does not advance.
        Pin shape: standalone poller with timeout_ns=0
        (audit/repro/INV-24_timeout_as_eos.c)."""
        import time
        rt = Runtime.region_mapper()  # shared process-wide runtime; not closed (house pattern)
        q = IndexedQueue.create(self.test_dir, "q", runtime=rt)
        a = q.create_appender()
        a.append(b"hello")
        a.close()

        ptr = ffi.new("MmapqIndexedPoller **")
        rc = lib.mmapq_indexed_poller_create(
            self.test_dir.encode(), b"q", 4, 0, rt._inner, ptr)
        check_rc(rc, "create_poller")
        p = IndexedPoller(ptr[0])
        p._runtime = rt

        seen = []
        with self.assertRaises(MmapqError) as cm:
            p.poll(lambda i, aid, data: seen.append(bytes(data)) or NextMove.ADVANCE)
        self.assertIn("mapping timeout", str(cm.exception))
        self.assertEqual(seen, [])

        # Cursor never advanced on the timeout: retry (bounded) until the
        # background mapping completes; the same message then advances.
        deadline = time.monotonic() + 5.0
        result = None
        while time.monotonic() < deadline:
            try:
                result = p.poll(
                    lambda i, aid, data: seen.append(bytes(data)) or NextMove.ADVANCE)
                break
            except MmapqError as e:
                if "mapping timeout" not in str(e):
                    raise  # retry only on the timeout, like the C and Rust pins
                continue
        self.assertEqual(result, PollResult.ADVANCED)
        self.assertEqual(seen, [b"hello"])
        p.close()
        q.close()

    def test_batch_poll_header_timeout_raises_not_eos(self):
        """CAND-17(a) batch twin of test_poll_header_timeout_raises_not_eos."""
        import time
        rt = Runtime.region_mapper()  # shared process-wide runtime; not closed (house pattern)
        q = IndexedBatchQueue.create(self.test_dir, "bq", runtime=rt)
        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"hello")
            w.commit()
        a.close()

        ptr = ffi.new("MmapqIndexedBatchPoller **")
        rc = lib.mmapq_indexed_batch_poller_create(
            self.test_dir.encode(), b"bq", 4, 0, rt._inner, ptr)
        check_rc(rc, "create_batch_poller")
        p = IndexedBatchPoller(ptr[0])
        p._runtime = rt

        seen = []
        with self.assertRaises(MmapqError) as cm:
            p.poll(lambda bi, msgs: seen.extend(bytes(m) for m in msgs) or NextMove.ADVANCE)
        self.assertIn("mapping timeout", str(cm.exception))
        self.assertEqual(seen, [])

        deadline = time.monotonic() + 5.0
        result = None
        while time.monotonic() < deadline:
            try:
                result = p.poll(
                    lambda bi, msgs: seen.extend(bytes(m) for m in msgs) or NextMove.ADVANCE)
                break
            except MmapqError as e:
                if "mapping timeout" not in str(e):
                    raise  # retry only on the timeout, like the C and Rust pins
                continue
        self.assertEqual(result, BatchPollResult.ADVANCED)
        self.assertEqual(seen, [b"hello"])
        p.close()
        q.close()

    def test_no_stderr_output_on_wrong_type(self):
        import os, tempfile
        err_fd = os.dup(2)
        stderr_path = os.path.join(self.test_dir, "stderr_capture")
        try:
            cap_fd = os.open(stderr_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            os.dup2(cap_fd, 2)
            os.close(cap_fd)
            q = IndexedBatchQueue.create(self.test_dir, "bq2")
            a = q.create_appender()
            w = a.start_batch()
            w.append(b"msg")
            w.commit()
            a.close()
            q.close()
            r = IndexedReader.create(self.test_dir, "bq2")
            try:
                r.read(0)
            except MmapqError:
                pass
        finally:
            os.dup2(err_fd, 2)
            os.close(err_fd)
        self.assertEqual(os.path.getsize(stderr_path), 0,
                         "C code wrote to stderr despite error code propagation")

    def test_schema_mismatch_error_importable(self):
        from mmapq import MmapqError, SchemaMismatchError, NextMove, IndexedQueue

        self.assertTrue(issubclass(SchemaMismatchError, MmapqError))

class TestReader(unittest.TestCase):
    """Tests for random-access IndexedReader."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_reader")
        self.q = IndexedQueue.create(self.test_dir, "rq")
        self.a = self.q.create_appender()
        for i in range(5):
            self.a.append(f"msg{i}".encode())
        self.a.close()

    def tearDown(self):
        self.q.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_reader_read_by_index(self):
        r = IndexedReader.create(self.test_dir, "rq")
        data = r.read(2)
        self.assertEqual(data, b"msg2")
        r.close()

    def test_reader_first_last_and_indices(self):
        r = IndexedReader.create(self.test_dir, "rq")
        first = r.read_first()
        last = r.read_last()
        self.assertEqual(first, b"msg0")
        self.assertEqual(last, b"msg4")
        self.assertEqual(r.last_index(), 4)
        self.assertEqual(r.start_index(), 0)
        r.close()

    def test_reader_missing_index_returns_none(self):
        r = IndexedReader.create(self.test_dir, "rq")
        data = r.read(99)
        self.assertIsNone(data)
        r.close()

    def test_reader_view_lifetime(self):
        r = IndexedReader.create(self.test_dir, "rq")
        view = r.read_view(0)
        self.assertIsNotNone(view)
        copy_before = bytes(view)
        self.assertEqual(copy_before, b"msg0")
        with self.assertRaises(BufferError):
            r.read(1)
        view.release()
        r.read(1)
        copy_after = bytes(copy_before)
        self.assertEqual(copy_after, b"msg0")
        r.close()

    def test_reader_closed_raises(self):
        from mmapq import QueueClosedError
        r = IndexedReader.create(self.test_dir, "rq")
        r.close()
        with self.assertRaises(QueueClosedError):
            r.read(0)

    def test_read_view_error_paths_match_read(self):
        r = IndexedReader.create(self.test_dir, "rq")
        self.assertIsNone(r.read(99))
        self.assertIsNone(r.read_view(99))
        r.close()

    def test_appender_id_after_read(self):
        r = IndexedReader.create(self.test_dir, "rq")
        data = r.read(0)
        self.assertIsNotNone(data)
        aid = r.appender_id()
        self.assertIsInstance(aid, int)
        r.close()

    def test_appender_id_before_read_raises(self):
        from mmapq import MmapqError
        r = IndexedReader.create(self.test_dir, "rq")
        with self.assertRaises(MmapqError):
            r.appender_id()
        r.close()

    def test_appender_id_closed_raises(self):
        from mmapq import QueueClosedError
        r = IndexedReader.create(self.test_dir, "rq")
        r.close()
        with self.assertRaises(QueueClosedError):
            r.appender_id()


class TestBatchReader(unittest.TestCase):
    """Tests for random-access IndexedBatchReader."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_batch_reader")
        self.q = IndexedBatchQueue.create(self.test_dir, "brq")
        self.a = self.q.create_appender()
        with self.a.start_batch() as w:
            w.append(b"alpha")
            w.append(b"beta")
            w.append(b"gamma")
            w.commit()
        self.a.close()

    def tearDown(self):
        self.q.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_batch_reader_roundtrip(self):
        r = IndexedBatchReader.create(self.test_dir, "brq")
        msgs = r.read(0)
        self.assertIsNotNone(msgs)
        self.assertEqual(len(msgs), 3)
        self.assertEqual(msgs[0], b"alpha")
        self.assertEqual(msgs[1], b"beta")
        self.assertEqual(msgs[2], b"gamma")
        r.close()

    def test_batch_appender_id_before_read_raises(self):
        from mmapq import MmapqError
        r = IndexedBatchReader.create(self.test_dir, "brq")
        with self.assertRaises(MmapqError):
            r.batch_appender_id()
        r.close()

    def test_batch_appender_id_closed_raises(self):
        from mmapq import QueueClosedError
        r = IndexedBatchReader.create(self.test_dir, "brq")
        r.close()
        with self.assertRaises(QueueClosedError):
            r.batch_appender_id()


class TestTransfer(unittest.TestCase):
    """Tests for queue transfer (export/import/copy)."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_transfer")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_export_import_roundtrip(self):
        src_dir = os.path.join(self.test_dir, "src")
        dst_dir = os.path.join(self.test_dir, "dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)

        q = IndexedQueue.create(src_dir, "q")
        a = q.create_appender()
        payloads = [f"msg{i}".encode() for i in range(10)]
        for p in payloads:
            a.append(p)
        a.close()
        q.close()

        export_path = os.path.join(self.test_dir, "export.bin")
        n = export_queue(src_dir, "q", export_path)
        self.assertEqual(n, 10)

        n = import_queue(export_path, dst_dir, "q_imported")
        self.assertEqual(n, 10)

        r = IndexedReader.create(dst_dir, "q_imported")
        for i, expected in enumerate(payloads):
            data = r.read(i)
            self.assertEqual(data, expected)
        r.close()

    def test_export_to_unwritable_path_raises(self):
        src_dir = os.path.join(self.test_dir, "src")
        os.makedirs(src_dir)
        q = IndexedQueue.create(src_dir, "q")
        a = q.create_appender()
        a.append(b"test")
        a.close()
        q.close()
        with self.assertRaises(OSError):
            export_queue(src_dir, "q", "/nonexistent-dir/x.dat")

    def test_import_allow_append_false(self):
        src_dir = os.path.join(self.test_dir, "src")
        dst_dir = os.path.join(self.test_dir, "dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)

        q = IndexedQueue.create(src_dir, "q")
        a = q.create_appender()
        a.append(b"test")
        a.close()
        q.close()

        export_path = os.path.join(self.test_dir, "export.bin")
        export_queue(src_dir, "q", export_path)

        # First import succeeds
        import_queue(export_path, dst_dir, "q")

        # Second import without allow_append should fail
        with self.assertRaises(QueueExistsError):
            import_queue(export_path, dst_dir, "q")

    def test_import_allow_append_true(self):
        src_dir = os.path.join(self.test_dir, "src")
        dst_dir = os.path.join(self.test_dir, "dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)

        q = IndexedQueue.create(src_dir, "q")
        a = q.create_appender()
        a.append(b"msg1")
        a.append(b"msg2")
        a.close()
        q.close()

        export_path = os.path.join(self.test_dir, "export.bin")
        export_queue(src_dir, "q", export_path)

        import_queue(export_path, dst_dir, "q")
        n = import_queue(export_path, dst_dir, "q", allow_append=True)
        self.assertEqual(n, 2)

    def test_copy_queue_with_allow_append(self):
        src_dir = os.path.join(self.test_dir, "src")
        dst_dir = os.path.join(self.test_dir, "dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)

        q = IndexedQueue.create(src_dir, "q")
        a = q.create_appender()
        a.append(b"data")
        a.close()
        q.close()

        copy_queue(src_dir, "q", dst_dir, "q_copy")
        n = copy_queue(src_dir, "q", dst_dir, "q_copy", allow_append=True)
        self.assertEqual(n, 1)

    def test_copy_batch_queue(self):
        src_dir = os.path.join(self.test_dir, "bsrc")
        dst_dir = os.path.join(self.test_dir, "bdst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)

        q = IndexedBatchQueue.create(src_dir, "bq")
        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"batch_msg")
            w.commit()
        a.close()
        q.close()

        n = copy_batch_queue(src_dir, "bq", dst_dir, "bq_copy")
        self.assertEqual(n, 1)

    def test_copy_batch_to_normal(self):
        src_dir = os.path.join(self.test_dir, "b2n_src")
        dst_dir = os.path.join(self.test_dir, "b2n_dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)

        q = IndexedBatchQueue.create(src_dir, "bq")
        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"m1")
            w.append(b"m2")
            w.commit()
        a.close()
        q.close()

        n = copy_batch_to_normal(src_dir, "bq", dst_dir, "normal")
        self.assertEqual(n, 2)

    def test_copy_normal_to_batch(self):
        src_dir = os.path.join(self.test_dir, "n2b_src")
        dst_dir = os.path.join(self.test_dir, "n2b_dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)

        q = IndexedQueue.create(src_dir, "q")
        a = q.create_appender()
        a.append(b"n2b_1")
        a.append(b"n2b_2")
        a.close()
        q.close()

        n = copy_normal_to_batch(src_dir, "q", dst_dir, "batch")
        self.assertEqual(n, 2)

    def test_export_preserve(self):
        src_dir = os.path.join(self.test_dir, "ep_src")
        dst_dir = os.path.join(self.test_dir, "ep_dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)
        q = IndexedQueue.create(src_dir, "q")
        a = q.create_appender()
        a.append(b"ep_test")
        a.close()
        q.close()
        export_path = os.path.join(self.test_dir, "ep_export.bin")
        n = export_queue(src_dir, "q", export_path, preserve=True)
        self.assertEqual(n, 1)
        # Import back and verify index space
        n = import_queue(export_path, dst_dir, "q_reloaded", preserve=True)
        self.assertEqual(n, 1)
        r = IndexedReader.create(dst_dir, "q_reloaded")
        self.assertEqual(r.start_index(), 0)
        self.assertIsNotNone(r.read(0))
        r.close()

    def test_import_preserve(self):
        src_dir = os.path.join(self.test_dir, "ip_src")
        dst_dir = os.path.join(self.test_dir, "ip_dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)
        q = IndexedQueue.create(src_dir, "q")
        a = q.create_appender()
        a.append(b"preserve_test")
        a.close()
        q.close()
        export_path = os.path.join(self.test_dir, "ip_export.bin")
        export_queue(src_dir, "q", export_path)
        n = import_queue(export_path, dst_dir, "q_imported",
                         preserve=True)
        self.assertEqual(n, 1)
        # Verify index space
        r = IndexedReader.create(dst_dir, "q_imported")
        self.assertEqual(r.start_index(), 0)
        self.assertIsNotNone(r.read(0))
        r.close()

    def test_copy_preserve(self):
        src_dir = os.path.join(self.test_dir, "cp_src")
        dst_dir = os.path.join(self.test_dir, "cp_dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)
        q = IndexedQueue.create(src_dir, "q")
        a = q.create_appender()
        a.append(b"cp_test")
        a.close()
        q.close()
        n = copy_queue(src_dir, "q", dst_dir, "q_copy", preserve=True)
        self.assertEqual(n, 1)
        # Verify index space
        r = IndexedReader.create(dst_dir, "q_copy")
        self.assertEqual(r.start_index(), 0)
        self.assertIsNotNone(r.read(0))
        r.close()

        src_dir = os.path.join(self.test_dir, "bcp_src")
        dst_dir = os.path.join(self.test_dir, "bcp_dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)
        q = IndexedBatchQueue.create(src_dir, "bq")
        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"bcp_msg")
            w.commit()
        a.close()
        q.close()
        n = copy_batch_queue(src_dir, "bq", dst_dir, "bq_copy", preserve=True)
        self.assertEqual(n, 1)
        # Verify index space
        r = IndexedBatchReader.create(dst_dir, "bq_copy")
        self.assertEqual(r.start_index(), 0)
        r.close()

    def test_batch_copy_preserve_to_new(self):
        src_dir = os.path.join(self.test_dir, "bcp_src")
        dst_dir = os.path.join(self.test_dir, "bcp_dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)
        q = IndexedBatchQueue.create(src_dir, "bq")
        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"bcp_msg")
            w.commit()
        a.close()
        q.close()
        n = copy_batch_queue(src_dir, "bq", dst_dir, "bq_copy", preserve=True)
        self.assertEqual(n, 1)
        r = IndexedBatchReader.create(dst_dir, "bq_copy")
        self.assertEqual(r.start_index(), 0)
        r.close()

    def test_import_batch_stream_preserve_to_new(self):
        src_dir = os.path.join(self.test_dir, "ibs_src")
        dst_dir = os.path.join(self.test_dir, "ibs_dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)
        q = IndexedBatchQueue.create(src_dir, "bq")
        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"ibs_msg")
            w.commit()
        a.close()
        q.close()

        export_path = os.path.join(self.test_dir, "ibs_export.bin")
        br = IndexedBatchReader.create(src_dir, "bq")
        f = lib.fopen(export_path.encode(), "wb".encode())
        lib.mmapq_batch_queue_transfer_export(br._inner, f, 0, 0, 1, ffi.NULL, ffi.NULL, ffi.NULL)
        lib.fclose(f)
        br.close()

        n = import_batch_stream(export_path, dst_dir, "q_restored", preserve=True)
        self.assertEqual(n, 1)
        # Verify index space
        r = IndexedReader.create(dst_dir, "q_restored")
        self.assertEqual(r.start_index(), 0)
        self.assertIsNotNone(r.read(0))
        r.close()

    def test_import_flat_to_batch_preserve_to_new(self):
        src_dir = os.path.join(self.test_dir, "ifb_src")
        dst_dir = os.path.join(self.test_dir, "ifb_dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)
        q = IndexedQueue.create(src_dir, "q")
        a = q.create_appender()
        a.append(b"ifb_msg")
        a.close()
        q.close()

        export_path = os.path.join(self.test_dir, "ifb_export.bin")
        export_queue(src_dir, "q", export_path, preserve=True)

        n = import_flat_to_batch(export_path, dst_dir, "q_restored", preserve=True)
        self.assertEqual(n, 1)
        # Verify index space
        r = IndexedBatchReader.create(dst_dir, "q_restored")
        self.assertEqual(r.start_index(), 0)
        r.close()

    def test_copy_batch_to_normal_preserve(self):
        src_dir = os.path.join(self.test_dir, "b2np_src")
        dst_dir = os.path.join(self.test_dir, "b2np_dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)
        q = IndexedBatchQueue.create(src_dir, "bq")
        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"b2np_1")
            w.append(b"b2np_2")
            w.commit()
        a.close()
        q.close()
        n = copy_batch_to_normal(src_dir, "bq", dst_dir, "normal", preserve=True)
        self.assertEqual(n, 2)
        # Verify index space
        r = IndexedReader.create(dst_dir, "normal")
        self.assertEqual(r.start_index(), 0)
        self.assertIsNotNone(r.read(0))
        r.close()

    def test_copy_normal_to_batch_preserve(self):
        src_dir = os.path.join(self.test_dir, "n2bp_src")
        dst_dir = os.path.join(self.test_dir, "n2bp_dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)
        q = IndexedQueue.create(src_dir, "q")
        a = q.create_appender()
        a.append(b"n2bp_1")
        a.append(b"n2bp_2")
        a.close()
        q.close()
        n = copy_normal_to_batch(src_dir, "q", dst_dir, "batch", preserve=True)
        self.assertEqual(n, 2)
        # Verify index space
        r = IndexedBatchReader.create(dst_dir, "batch")
        self.assertEqual(r.start_index(), 0)
        r.close()

    def test_copy_batch_to_normal_preserve_nonzero(self):
        src_dir = os.path.join(self.test_dir, "b2nz_src")
        dst_dir = os.path.join(self.test_dir, "b2nz_dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)

        # Create batch queue with start_index=100 via C API
        bq_ptr = ffi.new("MmapqIndexedBatchQueue **")
        rc = lib.mmapq_indexed_batch_queue_create(
            src_dir.encode(), b"bq", 32768, 16, 1, 262144, 100, 10000000000, 0,
            ffi.NULL, ffi.NULL, bq_ptr)
        self.assertEqual(rc, 0)
        bq = bq_ptr[0]
        ba_ptr = ffi.new("MmapqIndexedBatchAppender **")
        rc = lib.mmapq_indexed_batch_queue_create_appender(bq, ba_ptr)
        self.assertEqual(rc, 0)
        ba = ba_ptr[0]
        mb_ptr = ffi.new("MmapqMutableBatch **")
        rc = lib.mmapq_indexed_batch_appender_start_batch_checked(ba, mb_ptr)
        self.assertEqual(rc, 0)
        mb = mb_ptr[0]
        lib.mmapq_mutable_batch_append(mb, b"hello", 5)
        off = lib.mmapq_mutable_batch_complete(mb)
        lib.mmapq_indexed_batch_appender_commit_batch(ba, off)
        lib.mmapq_indexed_batch_appender_close(ba)
        lib.mmapq_indexed_batch_queue_close(bq)

        n = copy_batch_to_normal(src_dir, "bq", dst_dir, "normal", preserve=True)
        self.assertEqual(n, 1)
        r = IndexedReader.create(dst_dir, "normal")
        self.assertEqual(r.start_index(), 100)
        self.assertIsNotNone(r.read(100))
        r.close()

    def test_copy_normal_to_batch_preserve_nonzero(self):
        src_dir = os.path.join(self.test_dir, "n2bz_src")
        dst_dir = os.path.join(self.test_dir, "n2bz_dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)

        # Create normal queue with start_index=100 via C API
        q_ptr = ffi.new("MmapqIndexedQueue **")
        rc = lib.mmapq_indexed_queue_create(
            src_dir.encode(), b"q", 32768, 16, 1, 262144, 100, 10000000000, 0,
            ffi.NULL, ffi.NULL, q_ptr)
        self.assertEqual(rc, 0)
        q = q_ptr[0]
        aptr = ffi.new("MmapqIndexedAppender **")
        rc = lib.mmapq_indexed_queue_create_appender(q, aptr)
        self.assertEqual(rc, 0)
        a = aptr[0]
        lib.mmapq_indexed_appender_append(a, b"world", 5)
        lib.mmapq_indexed_appender_close(a)
        lib.mmapq_indexed_queue_close(q)

        n = copy_normal_to_batch(src_dir, "q", dst_dir, "batch", preserve=True)
        self.assertEqual(n, 1)
        r = IndexedBatchReader.create(dst_dir, "batch")
        self.assertEqual(r.start_index(), 100)
        r.close()

    def test_import_batch_stream_preserve_nonzero(self):
        src_dir = os.path.join(self.test_dir, "py_ibs_nz")
        dst_dir = os.path.join(self.test_dir, "py_ibs_nz_dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)

        bq_ptr = ffi.new("MmapqIndexedBatchQueue **")
        rc = lib.mmapq_indexed_batch_queue_create(
            src_dir.encode(), b"bq", 32768, 16, 1, 262144, 100, 10000000000, 0,
            ffi.NULL, ffi.NULL, bq_ptr)
        self.assertEqual(rc, 0)
        bq = bq_ptr[0]
        ba_ptr = ffi.new("MmapqIndexedBatchAppender **")
        rc = lib.mmapq_indexed_batch_queue_create_appender(bq, ba_ptr)
        self.assertEqual(rc, 0)
        ba = ba_ptr[0]
        mb_ptr = ffi.new("MmapqMutableBatch **")
        rc = lib.mmapq_indexed_batch_appender_start_batch_checked(ba, mb_ptr)
        self.assertEqual(rc, 0)
        mb = mb_ptr[0]
        lib.mmapq_mutable_batch_append(mb, b"test", 4)
        off = lib.mmapq_mutable_batch_complete(mb)
        lib.mmapq_indexed_batch_appender_commit_batch(ba, off)
        lib.mmapq_indexed_batch_appender_close(ba)
        lib.mmapq_indexed_batch_queue_close(bq)

        export_path = os.path.join(self.test_dir, "ibs_export.bin")
        br = IndexedBatchReader.create(src_dir, "bq")
        f = lib.fopen(export_path.encode(), "wb".encode())
        lib.mmapq_batch_queue_transfer_export(br._inner, f, 100, 100, 1, ffi.NULL, ffi.NULL, ffi.NULL)
        lib.fclose(f)
        br.close()

        n = import_batch_stream(export_path, dst_dir, "q", preserve=True)
        self.assertEqual(n, 1)
        r = IndexedReader.create(dst_dir, "q")
        self.assertEqual(r.start_index(), 100)
        self.assertIsNotNone(r.read(100))
        r.close()

    def test_import_flat_to_batch_preserve_nonzero(self):
        src_dir = os.path.join(self.test_dir, "py_ifb_nz")
        dst_dir = os.path.join(self.test_dir, "py_ifb_nz_dst")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)

        q_ptr = ffi.new("MmapqIndexedQueue **")
        rc = lib.mmapq_indexed_queue_create(
            src_dir.encode(), b"q", 32768, 16, 1, 262144, 100, 10000000000, 0,
            ffi.NULL, ffi.NULL, q_ptr)
        self.assertEqual(rc, 0)
        q = q_ptr[0]
        aptr = ffi.new("MmapqIndexedAppender **")
        rc = lib.mmapq_indexed_queue_create_appender(q, aptr)
        self.assertEqual(rc, 0)
        a = aptr[0]
        lib.mmapq_indexed_appender_append(a, b"test", 4)
        lib.mmapq_indexed_appender_close(a)
        lib.mmapq_indexed_queue_close(q)

        export_path = os.path.join(self.test_dir, "ifb_export.bin")
        export_queue(src_dir, "q", export_path, preserve=True)

        n = import_flat_to_batch(export_path, dst_dir, "bq", preserve=True)
        self.assertEqual(n, 1)
        r = IndexedBatchReader.create(dst_dir, "bq")
        self.assertEqual(r.start_index(), 100)
        r.close()

    def test_import_preserve_append_raises_value_error(self):
        with self.assertRaises(ValueError):
            import_queue("/nonexistent", self.test_dir, "q", preserve=True, allow_append=True)

    def test_copy_preserve_append_raises_value_error(self):
        with self.assertRaises(ValueError):
            copy_queue("/nonexistent", "q", self.test_dir, "q", preserve=True, allow_append=True)

    # ── Appender ID preservation tests ────────────────────────────────────

    def test_import_preserves_appender_ids(self):
        q = IndexedQueue.create(self.test_dir, "aid_preserve")
        a0 = q.create_appender()
        a1 = q.create_appender()
        id0 = a0.id()
        id1 = a1.id()
        self.assertNotEqual(id0, id1)
        a0.append(b"from-a0")
        a1.append(b"from-a1")
        a0.append(b"from-a0-2")
        a0.close(); a1.close()

        dest = tempfile.mkdtemp(suffix="_aid_preserve")
        try:
            export_queue(self.test_dir, "aid_preserve",
                         os.path.join(self.test_dir, "export.bin"),
                         preserve=False)
            n = import_queue(os.path.join(self.test_dir, "export.bin"),
                             dest, "q",
                             mapper=None,
                             on_conflict=ConflictBehavior.AUTO)
            self.assertEqual(n, 3)

            # Verify both appender IDs are present in the imported queue
            r = IndexedReader.create(dest, "q")
            seen0 = seen1 = False
            for i in range(r.last_index() + 1):
                msg = r.read(i)
                if msg is not None:
                    app_id = r.appender_id()
                    if app_id == id0:
                        seen0 = True
                    if app_id == id1:
                        seen1 = True
            self.assertTrue(seen0 and seen1)
            r.close()
        finally:
            shutil.rmtree(dest)
        q.close()

    def test_import_merge_via_mapper(self):
        q = IndexedQueue.create(self.test_dir, "aid_merge")
        a0 = q.create_appender()
        a1 = q.create_appender()
        a0.append(b"from-a0")
        a1.append(b"from-a1")
        a0.close(); a1.close()

        dest = tempfile.mkdtemp(suffix="_aid_merge")
        try:
            export_queue(self.test_dir, "aid_merge",
                         os.path.join(self.test_dir, "export.bin"),
                         preserve=False)
            n = import_queue(os.path.join(self.test_dir, "export.bin"),
                             dest, "q",
                             mapper=lambda aid: -1 if aid > 0 else 0,
                             on_conflict=ConflictBehavior.AUTO)
            self.assertEqual(n, 2)
        finally:
            shutil.rmtree(dest)
        q.close()

    def test_import_auto_on_conflict(self):
        q = IndexedQueue.create(self.test_dir, "aid_auto_conflict")
        existing = q.create_appender_with_id(0)

        q_src = IndexedQueue.create(self.test_dir, "aid_auto_src")
        a = q_src.create_appender()
        a.append(b"hello")
        a.close()
        export_queue(self.test_dir, "aid_auto_src",
                     os.path.join(self.test_dir, "export_src.bin"),
                     preserve=False)

        n = import_queue(os.path.join(self.test_dir, "export_src.bin"),
                         self.test_dir, "aid_auto_conflict",
                         allow_append=True, mapper=None,
                         on_conflict=ConflictBehavior.AUTO)
        self.assertEqual(n, 1)

        existing.close()
        q.close()
        q_src.close()

    def test_import_abort_on_conflict(self):
        q = IndexedQueue.create(self.test_dir, "aid_abort")
        existing = q.create_appender_with_id(0)

        q_src = IndexedQueue.create(self.test_dir, "aid_abort_src")
        a = q_src.create_appender()
        a.append(b"hello")
        a.close()
        export_queue(self.test_dir, "aid_abort_src",
                     os.path.join(self.test_dir, "export_abort.bin"),
                     preserve=False)

        with self.assertRaises(AppenderIdConflictError):
            import_queue(os.path.join(self.test_dir, "export_abort.bin"),
                         self.test_dir, "aid_abort",
                         allow_append=True, mapper=None,
                         on_conflict=ConflictBehavior.ABORT)
        existing.close()
        q.close()
        q_src.close()

    def test_batch_import_preserves_appender_ids(self):
        q = IndexedBatchQueue.create(self.test_dir, "batch_aid")
        a1 = q.create_appender()
        a2 = q.create_appender()
        with a1.start_batch() as w:
            w.append(b"a1"); w.commit()
        with a2.start_batch() as w:
            w.append(b"a2"); w.commit()
        a1.close(); a2.close()

        ep = os.path.join(self.test_dir, "export.dat")
        export_batch_queue(self.test_dir, "batch_aid", ep, preserve=False)

        dest = tempfile.mkdtemp(suffix="_batch_aid")
        try:
            n = import_batch_queue(ep, dest, "q",
                                   mapper=None,
                                   on_conflict=ConflictBehavior.AUTO)
            self.assertEqual(n, 2)

            r = IndexedBatchReader.create(dest, "q")
            try:
                r.read(0)
                got0 = r.batch_appender_id()
                r.read(1)
                got1 = r.batch_appender_id()
                self.assertNotEqual(got0, got1)
            finally:
                r.close()
        finally:
            shutil.rmtree(dest)
        q.close()

    def test_import_mapper_exception_aborts_transfer(self):
        q = IndexedQueue.create(self.test_dir, "aid_mapper_err")
        a = q.create_appender()
        a.append(b"hello")
        a.close()

        dest = tempfile.mkdtemp(suffix="_aid_mapper_err")
        try:
            export_queue(self.test_dir, "aid_mapper_err",
                         os.path.join(self.test_dir, "export.bin"),
                         preserve=False)

            def _bad_mapper(aid):
                raise ValueError("test exception")
            with self.assertRaisesRegex(ValueError, "test exception"):
                import_queue(os.path.join(self.test_dir, "export.bin"),
                             dest, "q",
                             mapper=_bad_mapper,
                             on_conflict=ConflictBehavior.AUTO)
        finally:
            shutil.rmtree(dest)
        q.close()

    def test_import_mapper_returns_none_falls_back_to_auto(self):
        q = IndexedQueue.create(self.test_dir, "aid_mapper_none")
        a = q.create_appender()
        a.append(b"hello")
        a.close()

        dest = tempfile.mkdtemp(suffix="_aid_mapper_none")
        try:
            export_queue(self.test_dir, "aid_mapper_none",
                         os.path.join(self.test_dir, "export.bin"),
                         preserve=False)

            def _none_mapper(aid):
                return None
            n = import_queue(os.path.join(self.test_dir, "export.bin"),
                             dest, "q",
                             mapper=_none_mapper,
                             on_conflict=ConflictBehavior.AUTO)
            self.assertEqual(n, 1)
        finally:
            shutil.rmtree(dest)
        q.close()

    def test_copy_with_mapper(self):
        q_src = IndexedQueue.create(self.test_dir, "copy_mapper")
        a0 = q_src.create_appender()
        a1 = q_src.create_appender()
        a0.append(b"from-a0")
        a1.append(b"from-a1")
        a0.close(); a1.close()

        dest = tempfile.mkdtemp(suffix="_copy_mapper")
        try:
            n = copy_queue(self.test_dir, "copy_mapper", dest, "q",
                           mapper=lambda aid: 0,
                           on_conflict=ConflictBehavior.AUTO)
            self.assertEqual(n, 2)
            r = IndexedReader.create(dest, "q")
            for i in range(r.last_index() + 1):
                msg = r.read(i)
                self.assertIsNotNone(msg)
                self.assertEqual(r.appender_id(), 0)
            r.close()
        finally:
            shutil.rmtree(dest)
        q_src.close()

    def test_read_view_then_appender_id(self):
        q = IndexedQueue.create(self.test_dir, "view_aid")
        a = q.create_appender()
        a.append(b"hello")
        a.close()

        r = IndexedReader.create(self.test_dir, "view_aid")
        view = None
        try:
            view = r.read_view(0)
            self.assertIsNotNone(view)
            aid = r.appender_id()
            self.assertIsInstance(aid, int)
        finally:
            if view is not None:
                view.release()
            r.close()
        q.close()

class TestProducerCompletion(unittest.TestCase):
    """Tests for uncommitted append, commit, and create_appender_with_id."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_prod")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_append_uncommitted_then_commit(self):
        q = IndexedQueue.create(self.test_dir, "q")
        a = q.create_appender()
        p = q.create_poller()

        offset = a.append_uncommitted(b"hello")
        self.assertIsInstance(offset, int)

        result = p.poll(lambda i, aid, d: NextMove.ADVANCE)
        self.assertEqual(result, PollResult.END_OF_STREAM)

        idx = a.commit(offset)
        self.assertIsInstance(idx, int)

        messages = []
        def handler(idx, aid, data):
            messages.append((idx, data))
            return NextMove.ADVANCE
        p.poll(handler)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0][1], b"hello")

        a.close()
        p.close()
        q.close()

    def test_uncommitted_discarded_on_restart(self):
        q = IndexedQueue.create(self.test_dir, "q")
        a = q.create_appender()
        a.append(b"a")
        offset = a.append_uncommitted(b"b")
        self.assertIsInstance(offset, int)
        a.close()
        q.close()

        q2 = IndexedQueue.create(self.test_dir, "q")
        a2 = q2.create_appender()
        a2.append(b"c")
        a2.close()

        p2 = q2.create_poller()
        messages = []
        def handler(idx, aid, data):
            messages.append(data)
            return NextMove.ADVANCE
        for _ in range(3):
            p2.poll(handler)
        self.assertEqual(messages, [b"a", b"c"])
        p2.close()
        q2.close()

    def test_create_appender_with_id(self):
        q = IndexedQueue.create(self.test_dir, "q")
        a = q.create_appender_with_id(7)
        self.assertEqual(a.id(), 7)
        a.append(b"hello")
        a.close()

        p = q.create_poller()
        messages = []
        def handler(idx, aid, data):
            messages.append((idx, data))
            return NextMove.ADVANCE
        p.poll(handler)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0][1], b"hello")
        p.close()
        q.close()

    def test_create_appender_with_bad_id_raises(self):
        q = IndexedQueue.create(self.test_dir, "q")
        with self.assertRaises(ValueError):
            q.create_appender_with_id(256)
        q.close()

    def test_batch_create_appender_with_id(self):
        q = IndexedBatchQueue.create(self.test_dir, "bq")
        a = q.create_appender_with_id(7)
        self.assertEqual(a.id(), 7)
        with a.start_batch() as w:
            w.append(b"hello")
            w.commit()
        a.close()
        p = q.create_poller()
        messages = []
        def handler(bi, msgs):
            messages.extend(msgs)
            return NextMove.ADVANCE
        p.poll(handler)
        self.assertEqual(messages, [b"hello"])
        p.close()
        q.close()

    def test_batch_create_appender_with_bad_id_raises(self):
        q = IndexedBatchQueue.create(self.test_dir, "bq")
        with self.assertRaises(ValueError):
            q.create_appender_with_id(256)
        q.close()

    def test_batch_create_appender_with_id_conflict_raises(self):
        q = IndexedBatchQueue.create(self.test_dir, "bq")
        a1 = q.create_appender_with_id(7)
        with self.assertRaises(AppenderIdConflictError):
            q.create_appender_with_id(7)
        a1.close()
        q.close()

    def test_batch_appender_id(self):
        q = IndexedBatchQueue.create(self.test_dir, "bq")
        a = q.create_appender()
        self.assertIsInstance(a.id(), int)
        self.assertTrue(0 <= a.id() <= 255)
        a.close()
        q.close()

    def test_writer_double_commit(self):
        from mmapq import MmapqError
        q = IndexedBatchQueue.create(self.test_dir, "bq")
        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"msg")
            w.commit()
        with self.assertRaises(MmapqError):
            w.commit()
        a.close()
        q.close()

class TestQueueScopedFactory(unittest.TestCase):
    """Tests for queue-scoped reader/archiver factory methods."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_qsf")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_queue_scoped_reader_inherits_runtime(self):
        rt = Runtime.region_mapper()
        q = IndexedBatchQueue.create(
            self.test_dir, "bq", write_runtime=rt, read_runtime=rt)
        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"msg")
            w.commit()
        a.close()
        r = q.create_reader()
        self.assertIs(r._runtime, rt,
            "reader should inherit the queue's read_runtime")
        # Also verify the reader actually works (observable behaviour)
        batch = r.read(0)
        self.assertEqual(batch, [b"msg"])
        r.close()
        q.close()
        rt.close()

    def test_single_queue_reader_works(self):
        q = IndexedQueue.create(self.test_dir, "sq")
        a = q.create_appender()
        a.append(b"msg")
        a.close()
        r = q.create_reader()
        msg = r.read(0)
        self.assertEqual(msg, b"msg")
        r.close()
        q.close()

    def test_queue_scoped_archiver_works(self):
        q = IndexedQueue.create(self.test_dir, "aq", archivable=True)
        a = q.create_appender()
        a.append(b"msg")
        a.close()
        arch = q.create_archiver()
        self.assertTrue(arch.archivable())
        arch.archive_up_to_index(1)
        arch.close()
        q.close()

    def test_queue_scoped_batch_archiver_works(self):
        q = IndexedBatchQueue.create(
            self.test_dir, "bbq", archivable=True)
        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"msg")
            w.commit()
        a.close()
        arch = q.create_archiver()
        self.assertTrue(arch.archivable())
        arch.archive_up_to_index(1)
        arch.close()
        q.close()


class TestRuntime(unittest.TestCase):
    """Tests for the Runtime class and async mapping."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_rt")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_runtime_mapper_roundtrip(self):
        q = IndexedQueue.create(
            self.test_dir, "rt1", runtime=Runtime.region_mapper())
        a = q.create_appender()
        p = q.create_poller()
        for i in range(100):
            a.append(f"msg{i}".encode())
        seen = []
        def handler(idx, aid, data):
            seen.append(data)
            return NextMove.ADVANCE
        for _ in range(100):
            p.poll(handler)
        self.assertEqual(len(seen), 100)
        self.assertEqual(seen[0], b"msg0")
        self.assertEqual(seen[99], b"msg99")
        a.close()
        p.close()
        q.close()

    def test_runtime_kept_alive_by_queue(self):
        rt = Runtime.region_mapper()
        q = IndexedQueue.create(self.test_dir, "rt2", runtime=rt)
        a = q.create_appender()
        p = q.create_poller()
        a.append(b"survivor")
        del q
        del rt
        gc.collect()
        messages = []
        def handler(idx, aid, data):
            messages.append(data)
            return NextMove.ADVANCE
        p.poll(handler)
        self.assertEqual(messages, [b"survivor"])
        a.close()
        p.close()

    def test_custom_config_region_size(self):
        q = IndexedQueue.create(
            self.test_dir, "rt3",
            runtime=Runtime.region_mapper())
        a = q.create_appender()
        p = q.create_poller()
        a.append(b"test")
        messages = []
        def handler(idx, aid, data):
            messages.append(data)
            return NextMove.ADVANCE
        p.poll(handler)
        self.assertEqual(messages, [b"test"])
        a.close()
        p.close()
        q.close()

    def test_batch_queue_with_custom_config(self):
        q = IndexedBatchQueue.create(
            self.test_dir, "rt4",
            region_size=8192*1024, ring_size=2,
            runtime=Runtime.region_mapper())
        a = q.create_appender()
        p = q.create_poller()
        with a.start_batch() as w:
            w.append(b"batch config test")
            w.commit()
        messages = []
        def handler(bi, msgs):
            messages.extend(msgs)
            return NextMove.ADVANCE
        p.poll(handler)
        self.assertEqual(messages, [b"batch config test"])
        a.close()
        p.close()
        q.close()


class TestArchiver(unittest.TestCase):
    """Tests for the IndexedArchiver."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_arch")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_archiver_on_archivable_queue(self):
        q = IndexedQueue.create(self.test_dir, "aq", archivable=True)
        a = q.create_appender()
        for i in range(5):
            a.append(f"msg{i}".encode())
        a.close()

        arch = IndexedArchiver.create(self.test_dir, "aq")
        self.assertTrue(arch.archivable())
        self.assertEqual(arch.start_index(), 0)
        arch.archive_up_to_index(3)
        self.assertEqual(arch.start_index(), 3)
        files = arch.archivable_files()
        self.assertIsInstance(files, list)
        arch.close()
        q.close()

    def test_archiver_rejects_non_archivable(self):
        q = IndexedQueue.create(self.test_dir, "naq")
        a = q.create_appender()
        a.append(b"test")
        a.close()

        arch = IndexedArchiver.create(self.test_dir, "naq")
        self.assertFalse(arch.archivable())
        with self.assertRaises(ValueError):
            arch.archive_up_to_index(1)
        arch.close()
        q.close()


class TestHandlerException(unittest.TestCase):
    """Tests for handler exception propagation."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_exc")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_handler_exception_propagates_and_stops(self):
        q = IndexedQueue.create(self.test_dir, "eq")
        a = q.create_appender()
        for i in range(5):
            a.append(f"msg{i}".encode())
        a.close()

        p = q.create_poller()
        calls = []
        def handler(idx, aid, data):
            calls.append(idx)
            if idx == 2:
                raise ValueError("fail at 2")
            return NextMove.ADVANCE

        p.poll(handler)
        p.poll(handler)
        with self.assertRaises(ValueError):
            p.poll(handler)
        self.assertEqual(calls, [0, 1, 2])

        calls2 = []
        def handler2(idx, aid, data):
            calls2.append(idx)
            return NextMove.ADVANCE
        p.poll(handler2)
        p.poll(handler2)
        p.poll(handler2)
        self.assertEqual(calls2, [2, 3, 4])
        p.close()
        q.close()


class TestCloseIdempotent(unittest.TestCase):
    """Tests that close() is idempotent on all types."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmapq_close")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_close_idempotent_all_types(self):
        q = IndexedQueue.create(self.test_dir, "ciq")
        a = q.create_appender()
        p = q.create_poller()
        bq = IndexedBatchQueue.create(self.test_dir, "cibq")
        ba = bq.create_appender()
        bp = bq.create_poller()
        r = IndexedReader.create(self.test_dir, "ciq")
        br = IndexedBatchReader.create(self.test_dir, "cibq")
        rt = Runtime.region_mapper()
        arch = IndexedArchiver.create(self.test_dir, "ciq")

        q.close()
        q.close()
        del q; gc.collect()
        a.close()
        a.close()
        del a; gc.collect()
        p.close()
        p.close()
        del p; gc.collect()
        bq.close()
        bq.close()
        del bq; gc.collect()
        ba.close()
        ba.close()
        del ba; gc.collect()
        bp.close()
        bp.close()
        del bp; gc.collect()
        r.close()
        r.close()
        del r; gc.collect()
        br.close()
        br.close()
        del br; gc.collect()
        arch.close()
        arch.close()
        del arch; gc.collect()
        rt.close()
        rt.close()
        del rt; gc.collect()

    def test_single_poller_finalizer_cleanup(self):
        q = IndexedQueue.create(self.test_dir, "sp")
        a = q.create_appender()
        a.append(b"gc test")
        a.close()
        p = q.create_poller()
        del p
        gc.collect()
        p2 = q.create_poller()
        messages = []
        def handler(idx, aid, data):
            messages.append(data)
            return NextMove.ADVANCE
        p2.poll(handler)
        self.assertEqual(messages, [b"gc test"])
        p2.close()
        q.close()

    def test_batch_poller_finalizer_cleanup(self):
        q = IndexedBatchQueue.create(self.test_dir, "bp")
        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"batch gc test")
            w.commit()
        a.close()
        p = q.create_poller()
        del p
        gc.collect()
        p2 = q.create_poller()
        messages = []
        def handler(bi, msgs):
            messages.extend(msgs)
            return NextMove.ADVANCE
        p2.poll(handler)
        self.assertEqual(messages, [b"batch gc test"])
        p2.close()
        q.close()


    def test_batch_queue_with_explicit_runtimes(self):
        rt = Runtime.region_mapper()
        q = IndexedBatchQueue.create(
            self.test_dir, "bq_explicit_rts",
            write_runtime=rt, read_runtime=rt)
        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"msg0")
            idx = w.commit()
        self.assertEqual(idx, 0)
        p = q.create_poller()
        messages = []
        def handler(bi, msgs):
            messages.extend(msgs)
            return NextMove.ADVANCE
        p.poll(handler)
        self.assertEqual(messages, [b"msg0"])
        a.close()
        p.close()
        q.close()
        rt.close()

    def test_writer_rollback_on_clean_exit(self):
        q = IndexedBatchQueue.create(self.test_dir, "bq_rollback")
        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"will_rollback")
        p = q.create_poller()
        messages = []
        def handler(bi, msgs):
            messages.extend(msgs)
            return NextMove.ADVANCE
        result = p.poll(handler)
        self.assertEqual(result, PollResult.END_OF_STREAM)
        self.assertEqual(messages, [])
        reader = IndexedBatchReader.create(
            self.test_dir, "bq_rollback", runtime=q._runtime)
        last = reader.last_index()
        self.assertEqual(last, -1)
        p.close()
        a.close()
        q.close()


    def test_constructor_rejects_string(self):
        with self.assertRaises(TypeError):
            IndexedQueue("/tmp")
        with self.assertRaises(TypeError):
            IndexedQueue(inner="/tmp")
        with self.assertRaises(TypeError):
            IndexedBatchQueue("/tmp")
        with self.assertRaises(TypeError):
            IndexedBatchQueue(inner="/tmp")

    def test_constructor_rejects_wrong_ptr_type(self):
        bq = IndexedBatchQueue.create(self.test_dir, "bq_wrong_ptr")
        with self.assertRaises(TypeError):
            IndexedQueue(bq._inner)
        bq.close()

    def test_constructor_rejects_null(self):
        with self.assertRaises(ValueError):
            IndexedQueue(ffi.NULL)
        with self.assertRaises(ValueError):
            IndexedBatchQueue(ffi.NULL)


class TestC60ArchivableChecked(unittest.TestCase):
    """CAND-60: archivable() distinguishes not-determinable from not-archivable."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_c60")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _write_short_meta(self, name):
        qdir = os.path.join(self.test_dir, name)
        os.makedirs(qdir, exist_ok=True)
        with open(os.path.join(qdir, "meta"), "wb") as f:
            f.write(b"\x00" * 10)

    def test_c60_queue_not_ready_error_importable(self):
        self.assertTrue(issubclass(QueueNotReadyError, MmapqError))

    def test_c60_archivable_missing_meta_raises_file_not_found(self):
        arch = IndexedArchiver.create(self.test_dir, "c60_missing")
        with self.assertRaises(FileNotFoundError):
            arch.archivable()
        arch.close()

    def test_c60_archivable_short_meta_raises_not_ready(self):
        self._write_short_meta("c60_short")
        arch = IndexedArchiver.create(self.test_dir, "c60_short")
        with self.assertRaises(QueueNotReadyError):
            arch.archivable()
        arch.close()

    def test_c60_archivable_true_and_false(self):
        q = IndexedQueue.create(self.test_dir, "c60_true", archivable=True)
        a = q.create_appender()
        a.append(b"m")
        a.close()
        arch = IndexedArchiver.create(self.test_dir, "c60_true")
        self.assertTrue(arch.archivable())
        arch.close()
        q.close()

        q = IndexedQueue.create(self.test_dir, "c60_false")
        a = q.create_appender()
        a.append(b"m")
        a.close()
        arch = IndexedArchiver.create(self.test_dir, "c60_false")
        self.assertFalse(arch.archivable())
        arch.close()
        q.close()

    def test_c60_archivable_sticky_raises(self):
        bq = IndexedBatchQueue.create(self.test_dir, "c60_sticky", archivable=True)
        a = bq.create_appender()
        with a.start_batch() as w:
            w.append(b"m")
            w.commit()
        a.close()
        bq.close()

        arch = IndexedArchiver.create(self.test_dir, "c60_sticky")
        with self.assertRaises(SchemaMismatchError):
            arch.archivable()
        with self.assertRaises(SchemaMismatchError):
            arch.archivable()
        arch.close()

    def test_c60_create_appender_not_ready_raises(self):
        self._write_short_meta("c60_app")
        q = IndexedQueue.create(self.test_dir, "c60_app")
        with self.assertRaises(QueueNotReadyError):
            q.create_appender()
        q.close()

    def test_c60_batch_archivable_missing_meta_raises_file_not_found(self):
        bq = IndexedBatchQueue.create(self.test_dir, "c60b_missing")
        arch = bq.create_archiver()
        with self.assertRaises(FileNotFoundError):
            arch.archivable()
        arch.close()
        bq.close()

    def test_c60_batch_archivable_short_meta_raises_not_ready(self):
        self._write_short_meta("c60b_short")
        bq = IndexedBatchQueue.create(self.test_dir, "c60b_short")
        arch = bq.create_archiver()
        with self.assertRaises(QueueNotReadyError):
            arch.archivable()
        arch.close()
        bq.close()

    def test_c60_batch_archivable_true_and_false(self):
        bq = IndexedBatchQueue.create(self.test_dir, "c60b_true", archivable=True)
        a = bq.create_appender()
        with a.start_batch() as w:
            w.append(b"m")
            w.commit()
        a.close()
        arch = bq.create_archiver()
        self.assertTrue(arch.archivable())
        arch.close()
        bq.close()

        bq = IndexedBatchQueue.create(self.test_dir, "c60b_false")
        a = bq.create_appender()
        with a.start_batch() as w:
            w.append(b"m")
            w.commit()
        a.close()
        arch = bq.create_archiver()
        self.assertFalse(arch.archivable())
        arch.close()
        bq.close()

    def test_c60_batch_archivable_sticky_raises(self):
        q = IndexedQueue.create(self.test_dir, "c60b_sticky", archivable=True)
        a = q.create_appender()
        a.append(b"m")
        a.close()
        q.close()

        bq = IndexedBatchQueue.create(self.test_dir, "c60b_sticky")
        arch = bq.create_archiver()
        with self.assertRaises(SchemaMismatchError):
            arch.archivable()
        with self.assertRaises(SchemaMismatchError):
            arch.archivable()
        arch.close()
        bq.close()

    def test_c60_batch_create_appender_not_ready_raises(self):
        self._write_short_meta("c60b_app")
        bq = IndexedBatchQueue.create(self.test_dir, "c60b_app")
        with self.assertRaises(QueueNotReadyError):
            bq.create_appender()
        bq.close()


if __name__ == "__main__":
    unittest.main()
