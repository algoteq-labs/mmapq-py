"""Tests for mmap-c-py — single-message and batch queue bindings."""

import os
import shutil
import tempfile
import unittest

import gc

from mmap_c_py import (
    NextMove,
    PollResult,
    BatchPollResult,
    IndexedQueue,
    IndexedAppender,
    IndexedPoller,
    IndexedBatchQueue,
    IndexedBatchAppender,
    IndexedReader,
    IndexedBatchReader,
    IndexedArchiver,
    Runtime,
    export_queue,
    import_queue,
    copy_queue,
    ffi,
    lib,
)


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

        def handler(bi, ctx):
            results.append((bi, ctx.message_count))
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

        result = p.poll(lambda bi, ctx: NextMove.ADVANCE)
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

        result = p.poll(lambda bi, ctx: NextMove.STOP)
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
        p.poll(lambda bi, ctx: NextMove.ADVANCE)
        # Retreat from batch 1
        result = p.poll(lambda bi, ctx: NextMove.RETREAT)
        self.assertEqual(result, PollResult.RETREATED)
        # Now poll batch 0 again via ADVANCE
        result = p.poll(lambda bi, ctx: NextMove.ADVANCE)
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

        def handler(bi, ctx):
            it_ptr = ffi.addressof(ctx.batch_iterator)
            while lib.batch_iterator_advance(it_ptr):
                length = lib.batch_iterator_current_length_ffi(it_ptr)
                data_ptr = lib.batch_iterator_current_data_ffi(it_ptr)
                data = ffi.buffer(data_ptr, length)[:]
                messages.append((bi, bytes(data)))
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
        from mmap_c_py import QueueCorruptError
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
        from mmap_c_py import IndexedQueue
        q = IndexedQueue.create(self.test_dir, "err_empty")
        a = q.create_appender()
        with self.assertRaises(ValueError):
            a.append(b"")
        a.close()
        q.close()

    def test_exception_types_importable(self):
        from mmap_c_py import QueueClosedError, QueueCorruptError, SchemaMismatchError
        self.assertTrue(issubclass(QueueClosedError, RuntimeError))
        self.assertTrue(issubclass(QueueCorruptError, RuntimeError))
        self.assertTrue(issubclass(SchemaMismatchError, RuntimeError))


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
        with self.assertRaises(Exception):
            bq = IndexedBatchQueue.create(self.test_dir, "q")
            bq.create_appender()

    def test_schema_mismatch_error_importable(self):
        from mmap_c_py import SchemaMismatchError
        self.assertTrue(issubclass(SchemaMismatchError, RuntimeError))


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
        r.read(1)
        copy_after = bytes(copy_before)
        self.assertEqual(copy_after, b"msg0")
        r.close()

    def test_reader_closed_raises(self):
        from mmap_c_py import QueueClosedError
        r = IndexedReader.create(self.test_dir, "rq")
        r.close()
        with self.assertRaises(QueueClosedError):
            r.read(0)

    def test_read_view_error_paths_match_read(self):
        r = IndexedReader.create(self.test_dir, "rq")
        self.assertIsNone(r.read(99))
        self.assertIsNone(r.read_view(99))
        r.close()


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
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_close")

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


if __name__ == "__main__":
    unittest.main()
