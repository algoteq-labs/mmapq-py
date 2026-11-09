"""Cursor-move coverage on archivable queues (single and batch pollers).

Every queue below holds entries (or batches) 0..9 archived up to 5, so
start_index == 5. Pinned behaviors: move_to_start lands on start_index;
move_to_index refuses an archived index without moving the cursor; a poll
below start_index returns ARCHIVED, never calls the handler, and holds the
cursor; move_to_end parks one past the last entry until an append
publishes there.

Also covers the appender's current_index, the batch reader's read_last,
and the batch writer's append_remaining_from (the low-level echo pattern:
the reader-owned batch iterator is obtained through the unsafe_lib escape
hatch documented on mmapq).
"""

import shutil
import tempfile
import unittest

import mmapq
from mmapq import (
    IndexedBatchQueue,
    IndexedQueue,
    NextMove,
    PollResult,
)

lib = mmapq.unsafe_lib()


class CursorMovesTest(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="mmapq-py-cursor-")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _single_archived(self, name):
        """Archivable single queue with entries m0..m9, archived to 5."""
        q = IndexedQueue.create(self.test_dir, name, archivable=True)
        a = q.create_appender()
        for i in range(10):
            self.assertEqual(a.append(f"m{i}".encode()), i)
        a.close()
        arch = q.create_archiver()
        arch.archive_up_to_index(5)
        arch.close()
        return q

    def _batch_archived(self, name):
        """Archivable batch queue with batches 0..9 (one b<i> message each),
        archived to 5."""
        q = IndexedBatchQueue.create(self.test_dir, name, archivable=True)
        a = q.create_appender()
        for i in range(10):
            with a.start_batch() as w:
                w.append(f"b{i}".encode())
                self.assertEqual(w.commit(), i)
        a.close()
        arch = q.create_archiver()
        arch.archive_up_to_index(5)
        arch.close()
        return q

    def test_single_move_to_start_lands_on_start_index(self):
        q = self._single_archived("sq_start")
        p = q.create_poller()

        self.assertTrue(p.move_to_index(7))
        self.assertTrue(p.move_to_start())
        self.assertEqual(p.start_index(), 5)
        self.assertEqual(p.current_index(), 5)
        self.assertFalse(p.exists(4))
        self.assertTrue(p.exists(5))
        self.assertFalse(p.exists(10))

        seen = []
        result = p.poll(lambda idx, aid, data: (seen.append((idx, data)),
                                                NextMove.STOP)[1])
        self.assertEqual(result, PollResult.STOPPED)
        self.assertEqual(seen, [(5, b"m5")])
        self.assertEqual(p.current_index(), 5)
        p.close()
        q.close()

    def test_single_move_to_index_refuses_archived_index(self):
        q = IndexedQueue.create(self.test_dir, "sq_refused", archivable=True)
        a = q.create_appender()
        for i in range(10):
            self.assertEqual(a.append(f"m{i}".encode()), i)
        a.close()
        p = q.create_poller()
        self.assertTrue(p.move_to_index(2))

        arch = q.create_archiver()
        arch.archive_up_to_index(5)
        arch.close()

        self.assertFalse(p.move_to_index(3))
        self.assertEqual(p.current_index(), 2)

        seen = []
        result = p.poll(lambda idx, aid, data: (seen.append((idx, data)),
                                                NextMove.ADVANCE)[1])
        self.assertEqual(result, PollResult.ARCHIVED)
        self.assertEqual(seen, [])
        self.assertEqual(p.current_index(), 2)
        p.close()
        q.close()

    def test_single_move_to_index_then_end_delivers_appended_index(self):
        q = self._single_archived("sq_index_end")
        p = q.create_poller()

        self.assertTrue(p.move_to_index(7))
        seen = []
        result = p.poll(lambda idx, aid, data: (seen.append((idx, data)),
                                                NextMove.ADVANCE)[1])
        self.assertEqual(result, PollResult.ADVANCED)
        self.assertEqual(seen, [(7, b"m7")])

        self.assertTrue(p.move_to_end())
        self.assertEqual(p.current_index(), 10)
        result = p.poll(lambda idx, aid, data: (seen.append((idx, data)),
                                                NextMove.ADVANCE)[1])
        self.assertEqual(result, PollResult.END_OF_STREAM)
        self.assertEqual(len(seen), 1)

        a = q.create_appender()
        self.assertEqual(a.append(b"m10"), 10)
        a.close()
        result = p.poll(lambda idx, aid, data: (seen.append((idx, data)),
                                                NextMove.ADVANCE)[1])
        self.assertEqual(result, PollResult.ADVANCED)
        self.assertEqual(seen, [(7, b"m7"), (10, b"m10")])
        p.close()
        q.close()

    def test_batch_move_to_start_lands_on_start_index(self):
        q = self._batch_archived("bq_start")
        p = q.create_poller()

        self.assertTrue(p.move_to_index(7))
        self.assertTrue(p.move_to_start())
        self.assertEqual(p.start_index(), 5)
        self.assertEqual(p.current_batch_index(), 5)
        self.assertFalse(p.exists(4))
        self.assertTrue(p.exists(5))
        self.assertFalse(p.exists(10))

        seen = []
        result = p.poll(lambda bi, msgs: (seen.append((bi, msgs)),
                                          NextMove.STOP)[1])
        self.assertEqual(result, PollResult.STOPPED)
        self.assertEqual(seen, [(5, [b"b5"])])
        self.assertEqual(p.current_batch_index(), 5)
        p.close()
        q.close()

    def test_batch_move_to_index_refuses_archived_index(self):
        q = IndexedBatchQueue.create(self.test_dir, "bq_refused",
                                     archivable=True)
        a = q.create_appender()
        for i in range(10):
            with a.start_batch() as w:
                w.append(f"b{i}".encode())
                self.assertEqual(w.commit(), i)
        a.close()
        p = q.create_poller()
        self.assertTrue(p.move_to_index(2))

        arch = q.create_archiver()
        arch.archive_up_to_index(5)
        arch.close()

        self.assertFalse(p.move_to_index(3))
        self.assertEqual(p.current_batch_index(), 2)

        seen = []
        result = p.poll(lambda bi, msgs: (seen.append((bi, msgs)),
                                          NextMove.ADVANCE)[1])
        self.assertEqual(result, PollResult.ARCHIVED)
        self.assertEqual(seen, [])
        self.assertEqual(p.current_batch_index(), 2)
        p.close()
        q.close()

    def test_batch_move_to_index_then_end_delivers_appended_batch(self):
        q = self._batch_archived("bq_index_end")
        p = q.create_poller()

        self.assertTrue(p.move_to_index(7))
        seen = []
        result = p.poll(lambda bi, msgs: (seen.append((bi, msgs)),
                                          NextMove.ADVANCE)[1])
        self.assertEqual(result, PollResult.ADVANCED)
        self.assertEqual(seen, [(7, [b"b7"])])

        self.assertTrue(p.move_to_end())
        self.assertEqual(p.current_batch_index(), 10)
        result = p.poll(lambda bi, msgs: (seen.append((bi, msgs)),
                                          NextMove.ADVANCE)[1])
        self.assertEqual(result, PollResult.END_OF_STREAM)
        self.assertEqual(len(seen), 1)

        a = q.create_appender()
        with a.start_batch() as w:
            w.append(b"b10")
            self.assertEqual(w.commit(), 10)
        a.close()
        result = p.poll(lambda bi, msgs: (seen.append((bi, msgs)),
                                          NextMove.ADVANCE)[1])
        self.assertEqual(result, PollResult.ADVANCED)
        self.assertEqual(seen, [(7, [b"b7"]), (10, [b"b10"])])
        p.close()
        q.close()

    def test_appender_current_index_is_next_append_index(self):
        q = IndexedQueue.create(self.test_dir, "sq_appender_idx")
        a = q.create_appender()
        self.assertEqual(a.current_index(), 0)
        a.append(b"one")
        a.append(b"two")
        self.assertEqual(a.current_index(), 2)
        self.assertEqual(a.append(b"three"), 2)
        self.assertEqual(a.current_index(), 3)
        a.close()
        q.close()

    def test_batch_reader_read_last(self):
        q = IndexedBatchQueue.create(self.test_dir, "bq_read_last")
        a = q.create_appender()
        for i in range(3):
            with a.start_batch() as w:
                w.append(f"bm{i}".encode())
                self.assertEqual(w.commit(), i)
        a.close()
        r = q.create_reader()
        self.assertEqual(r.read_last(), [b"bm2"])
        r.close()
        q.close()

    def test_batch_writer_append_remaining_from(self):
        """The echo pattern: after the reader-owned iterator steps past the
        first message, append_remaining_from copies exactly the remaining
        two into a fresh destination batch."""
        src = IndexedBatchQueue.create(self.test_dir, "bq_remaining_src")
        a = src.create_appender()
        with a.start_batch() as w:
            w.append(b"r0")
            w.append(b"r1")
            w.append(b"r2")
            self.assertEqual(w.commit(), 0)
        a.close()

        dst = IndexedBatchQueue.create(self.test_dir, "bq_remaining_dst")
        r = src.create_reader()
        self.assertEqual(r.read(0), [b"r0", b"r1", b"r2"])

        # The wrapper's read() drains the iterator; re-arm it with a raw
        # read (unsafe_lib escape hatch) so the reader-owned iterator is
        # positioned before message 0 again.
        lib.mmapq_indexed_batch_reader_read(r._inner, 0, r._ctx)
        it = r._ctx.iterator

        a2 = dst.create_appender()
        with a2.start_batch() as w2:
            self.assertTrue(lib.mmapq_batch_iterator_advance(it))
            self.assertEqual(w2.append_remaining_from(it), 2)
            self.assertEqual(w2.commit(), 0)
        a2.close()
        r.close()

        r2 = dst.create_reader()
        self.assertEqual(r2.read(0), [b"r1", b"r2"])
        r2.close()
        src.close()
        dst.close()


if __name__ == "__main__":
    unittest.main()
