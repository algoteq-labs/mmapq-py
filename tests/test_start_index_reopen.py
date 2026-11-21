"""0.2.1: start_index applies only when the queue is created. Reopening an
existing queue with a larger value appends after its last entry (it used to
append at the given index, past empty slots no poller reaches), and an
appender whose rejoin meets a payload file it cannot read raises MmapqError
with MMAPQ_EIO, not QueueCorruptError. The unreadable file is a symlink loop
(ELOOP), so the tests behave the same as root and as an ordinary user."""
import os
import shutil
import tempfile
import unittest

from mmapq import IndexedQueue, MmapqError, NextMove, PollResult, QueueCorruptError

REGION = 32 * 1024
FILE = REGION * 8
TIMEOUT_NS = 2_000_000_000


class TestStartIndexReopen(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_si")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _open(self, start_index=0):
        return IndexedQueue.create(self.test_dir, "q", region_size=REGION, max_file_size=FILE,
                                   start_index=start_index, timeout_ns=TIMEOUT_NS)

    def test_reopen_with_higher_start_index_appends_at_tail(self):
        q = self._open()
        a = q.create_appender()
        try:
            for i in range(3):
                self.assertEqual(a.append(b"msg"), i)
        finally:
            a.close()
        q.close()

        q = self._open(start_index=10)
        a = q.create_appender()
        try:
            self.assertEqual(a.append(b"msg"), 3)
        finally:
            a.close()
        seen = []

        def handler(index, appender_id, data):
            seen.append(index)
            return NextMove.ADVANCE
        p = q.create_poller()
        r = q.create_reader()
        try:
            while True:
                res = p.poll(handler)
                if res != PollResult.ADVANCED:
                    break
            self.assertEqual(res, PollResult.END_OF_STREAM)
            self.assertEqual(seen, [0, 1, 2, 3])
            self.assertEqual(r.last_index(), 3)
        finally:
            p.close()
            r.close()
            q.close()

    def test_join_over_unreadable_payload_file_is_eio_not_corrupt(self):
        q = self._open()
        a = q.create_appender_with_id(7)
        try:
            self.assertEqual(a.append_uncommitted(b"msg"), 64)  # first offset of a new lane
        finally:
            a.close()
        path = os.path.join(self.test_dir, "q", "payload_7_0")
        os.unlink(path)
        os.symlink("payload_7_0", path)        # a link to itself: ELOOP
        try:
            with self.assertRaises(MmapqError) as cm:
                q.create_appender_with_id(7)
            self.assertNotIsInstance(cm.exception, QueueCorruptError)
            self.assertIn("(rc=-10)", str(cm.exception))
        finally:
            q.close()


if __name__ == "__main__":
    unittest.main()
