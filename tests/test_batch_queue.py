"""Tests for mmap-c-py — single-message and batch queue bindings."""

import os
import shutil
import tempfile
import unittest

from mmap_c_py import (
    NextMove,
    PollResult,
    BatchPollResult,
    IndexedQueue,
    IndexedAppender,
    IndexedPoller,
    IndexedBatchQueue,
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
        from mmap_c_py import QueueClosedError, QueueCorruptError
        self.assertTrue(issubclass(QueueClosedError, RuntimeError))
        self.assertTrue(issubclass(QueueCorruptError, RuntimeError))


if __name__ == "__main__":
    unittest.main()
