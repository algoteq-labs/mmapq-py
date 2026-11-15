"""0.2.1: a consumer that reaches a header or payload file it cannot read
raises MmapqError with MMAPQ_EIO at once — not a silent END_OF_STREAM (an
unreadable header file used to stop a poller as if caught up) and not
QueueCorruptError (an unreadable payload file) — and carries on once the
file is readable. The cause here is a symlink loop where the file was
(opening it fails with ELOOP), so the tests behave the same as root and as
an ordinary user."""
import os
import shutil
import tempfile
import time
import unittest

from mmapq import IndexedQueue, MmapqError, NextMove, PollResult, QueueCorruptError

REGION = 32 * 1024
FILE = REGION * 8          # 32,768 headers, or 256 1000-byte messages, per file
TIMEOUT_NS = 2_000_000_000
HDR_SI = 32768 - 10        # header file 0 has 10 slots
MSG = b"\x07" * 1000


class TestReadPermErrors(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_rp")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _queue(self, start_index, msg, count):
        q = IndexedQueue.create(self.test_dir, "q", region_size=REGION, max_file_size=FILE,
                                start_index=start_index, timeout_ns=TIMEOUT_NS)
        a = q.create_appender()
        try:
            for i in range(count):
                self.assertEqual(a.append(msg), start_index + i)
        finally:
            a.close()
        return q

    def _block(self, name):
        path = os.path.join(self.test_dir, "q", name)
        os.rename(path, path + ".aside")
        os.symlink(name, path)       # a link to itself: ELOOP
        return path

    def _unblock(self, path):
        os.unlink(path)
        os.rename(path + ".aside", path)

    def _drain(self, p, seen):
        def handler(index, appender_id, data):
            seen.append(index)
            return NextMove.ADVANCE
        while True:
            r = p.poll(handler)
            if r != PollResult.ADVANCED:
                return r

    def _assert_eio_at_once(self, call):
        t = time.monotonic()
        with self.assertRaises(MmapqError) as cm:
            call()
        self.assertLess(time.monotonic() - t, 1.0)
        self.assertNotIsInstance(cm.exception, QueueCorruptError)
        self.assertIn("(rc=-10)", str(cm.exception))

    def test_unreadable_header_file_is_eio_not_end_of_stream(self):
        q = self._queue(HDR_SI, b"12345678", 20)
        p = q.create_poller()
        r = q.create_reader()
        try:
            blocked = self._block("header_1")
            seen = []
            self._assert_eio_at_once(lambda: self._drain(p, seen))
            self.assertEqual(seen, list(range(HDR_SI, 32768)))
            self._assert_eio_at_once(lambda: r.read(32770))
            self.assertEqual(r.last_index(), -1)
            self._unblock(blocked)
            self.assertEqual(self._drain(p, seen), PollResult.END_OF_STREAM)
            self.assertEqual(seen, list(range(HDR_SI, HDR_SI + 20)))
            self.assertEqual(r.last_index(), HDR_SI + 19)
        finally:
            r.close()
            p.close()
            q.close()

    def test_unreadable_payload_file_is_eio_not_corrupt(self):
        q = self._queue(0, MSG, 300)
        p = q.create_poller()
        r = q.create_reader()
        try:
            blocked = self._block("payload_0_1")
            seen = []
            self._assert_eio_at_once(lambda: self._drain(p, seen))
            self.assertEqual(seen, list(range(256)))
            self._assert_eio_at_once(lambda: r.read(299))
            self.assertEqual(r.last_index(), 299)
            self._unblock(blocked)
            self.assertEqual(self._drain(p, seen), PollResult.END_OF_STREAM)
            self.assertEqual(seen, list(range(300)))
        finally:
            r.close()
            p.close()
            q.close()


if __name__ == "__main__":
    unittest.main()
