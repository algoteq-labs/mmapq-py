"""Recovery-surface tests (CAND-49 §5.5): uncommitted iterators,
complete_uncommitted/commit_batch, join_completed_open_batch.

Shapes mirror the C pins in test_indexed_queue.c and
test_indexed_batch_queue.c (offsets are exact: a fresh lane starts at
payload offset 64) and the Rust suite (mmapq-rs/tests/recovery.rs).
"""

import tempfile
import types
import unittest
from unittest.mock import patch

from mmapq import IndexedBatchQueue, IndexedQueue, MmapqError, NextMove, PollResult
from mmapq import batch_appender


class TestSingleMessageRecovery(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def queue(self):
        queue = IndexedQueue.create(self.directory.name, "q")
        self.addCleanup(queue.close)
        return queue

    def test_uncommitted_finds_uncommitted(self):
        queue = self.queue()
        with queue.create_appender() as a:
            self.assertEqual(a.append(b"committed"), 0)
            off = a.append_uncommitted(b"uncommitted")
            # 64 (start of stream) + 4 (len prefix) + 9 ("committed")
            self.assertEqual(off, 77)

            self.assertEqual(list(a.uncommitted()), [(77, b"uncommitted")])

            self.assertEqual(a.commit(off), 1)
            self.assertEqual(list(a.uncommitted()), [])

    def test_uncommitted_is_a_call_time_snapshot(self):
        queue = self.queue()
        with queue.create_appender() as a:
            self.assertEqual(a.append_uncommitted(b"one"), 64)
            self.assertEqual(list(a.uncommitted()), [(64, b"one")])

            self.assertEqual(a.append_uncommitted(b"two"), 71)  # 64 + 4 + 3
            # A fresh iterator (a new snapshot) sees both.
            self.assertEqual(
                list(a.uncommitted()), [(64, b"one"), (71, b"two")])

    def test_uncommitted_rejoin_and_commit_validation(self):
        queue = self.queue()
        with queue.create_appender() as a:
            off_a = a.append_uncommitted(b"msg-a")
            self.assertEqual(off_a, 64)
            off_b = a.append_uncommitted(b"msg-bb")
            self.assertEqual(off_b, 73)  # 64 + 4 + 5
            # Committing an offset never written uncommitted is EINVAL.
            with self.assertRaises(ValueError):
                a.commit(8)
        # "Restart" — the appender closed with both messages uncommitted.

        with queue.create_appender() as a:
            self.assertEqual(
                list(a.uncommitted()), [(64, b"msg-a"), (73, b"msg-bb")])

            # Refilled by the walk: both offsets committable after rejoin.
            self.assertEqual(a.commit(off_b), 0)
            self.assertEqual(a.commit(off_a), 1)
            # Already committed: EINVAL.
            with self.assertRaises(ValueError):
                a.commit(off_a)


class TestBatchRecovery(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def queue(self):
        queue = IndexedBatchQueue.create(self.directory.name, "q")
        self.addCleanup(queue.close)
        return queue

    def test_complete_uncommitted_then_commit_batch(self):
        queue = self.queue()
        with queue.create_appender() as a:
            with a.start_batch() as w:
                self.assertEqual(w.append(b"a"), 0)
                self.assertEqual(w.append(b"bb"), 1)
                self.assertEqual(w.append(b"ccc"), 2)
                off = w.complete_uncommitted()
            self.assertEqual(off, 64)  # first batch of a fresh lane

            # Staged is not visible yet.
            with queue.create_poller() as p:
                result = p.poll(lambda bi, msgs: NextMove.ADVANCE)
                self.assertEqual(result, PollResult.END_OF_STREAM)

            self.assertEqual(a.commit_batch(off), 0)
            # Already committed: EINVAL. Never staged: EINVAL.
            with self.assertRaises(ValueError):
                a.commit_batch(off)
            with self.assertRaises(ValueError):
                a.commit_batch(8)

            with queue.create_poller() as p:
                captured = []

                def handler(bi, msgs):
                    captured.extend(msgs)
                    return NextMove.ADVANCE

                result = p.poll(handler)
                self.assertEqual(result, PollResult.ADVANCED)
                self.assertEqual(captured, [b"a", b"bb", b"ccc"])

    def test_discarded_batch_reappears_in_recovery(self):
        queue = self.queue()
        with queue.create_appender() as a:
            with a.start_batch() as w:
                w.append(b"d1")
                w.append(b"d2")
            # Exiting without commit/stage: discard-only (never publish).
            # Discard is final in-session.
            with self.assertRaises(ValueError):
                a.commit_batch(64)
        # "Restart".

        with queue.create_appender() as a:
            self.assertEqual(
                list(a.uncommitted_batches()), [(64, [b"d1", b"d2"])])

            # Refill made the discarded offset committable; recovery decides.
            self.assertEqual(a.commit_batch(64), 0)

    def test_uncommitted_batches_mid_batch_error_raises_not_truncates(self):
        """A negative from the checked batch step raises — a walked batch
        must never surface as a silently truncated message list (the
        unchecked advance() loop shape; commit-4 review).

        The C fault paths (mapping timeout / corruption mid-batch) are
        injected in the C suite via internal stub accessors that are not
        part of the public ABI, so this pins the wrapper's error
        propagation with a mocked lib — the same hook style as
        tests/test_safety.py.
        """
        queue = self.queue()
        with queue.create_appender() as a:
            ffi = batch_appender.ffi

            def fake_factory(_app, out):
                out[0] = ffi.cast("MmapqUncommittedBatchIterator *", 0)
                return 0

            def fake_batch_next(_it, offset, count, msgs):
                offset[0] = 64
                count[0] = 2
                msgs[0] = ffi.cast("MmapqBatchPayloadIterator *", 0)
                return 1

            def fake_step(_cursor, _data, _length):
                return -1  # MMAPQ_ETIMEDOUT, mid-batch

            fake = types.SimpleNamespace(
                mmapq_indexed_batch_appender_uncommitted_batches_checked=fake_factory,
                mmapq_uncommitted_batch_next=fake_batch_next,
                mmapq_batch_iterator_next=fake_step,
            )
            with patch.object(batch_appender, "lib", fake):
                with self.assertRaises(MmapqError):
                    list(a.uncommitted_batches())

    def test_rejoin_completes_open_batch(self):
        queue = self.queue()
        a = queue.create_appender()
        self.addCleanup(a.close)
        w = a.start_batch()
        for _ in range(3):
            w.append(b"A" * 200)
        # "Crash" with the batch open: abandon the writer (no __exit__) and
        # close the appender with the batch still open on disk.
        a.close()

        with queue.create_appender() as a2:
            self.assertEqual(a2.join_completed_open_batch(), 64)
            self.assertIsNone(a2.join_completed_open_batch())  # read-and-clear

            # The position moved past the completed batch: the next batch
            # publishes at index 0.
            with a2.start_batch() as w2:
                w2.append(b"y")
                self.assertEqual(w2.commit(), 0)

            # The walker yields the join-completed batch (count 3, exact).
            self.assertEqual(
                list(a2.uncommitted_batches()),
                [(64, [b"A" * 200, b"A" * 200, b"A" * 200])])


if __name__ == "__main__":
    unittest.main()
