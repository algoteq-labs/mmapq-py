"""Tests for the mmap-c-py batch queue bindings."""

import os
import shutil
import tempfile
import unittest

from mmap_c_py import (
    NextMove,
    PollResult,
    BatchPollResult,
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


if __name__ == "__main__":
    unittest.main()
