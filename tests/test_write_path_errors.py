"""0.2.1: an appender that cannot create, open or map the next queue file
raises MmapqError with MMAPQ_EIO at once (not the timeout code after the
mapping timeout), and the same call succeeds once the cause is gone. The
cause here is a directory sitting where the next payload file must go
(opening it fails with EISDIR), so the tests behave the same as root and as
an ordinary user."""
import os
import shutil
import tempfile
import time
import unittest

from mmapq import IndexedBatchQueue, IndexedQueue, MmapqError

REGION = 32 * 1024
FILE = REGION * 8          # 1000-byte messages: 256 per payload file
TIMEOUT_NS = 2_000_000_000
MSG = b"\x07" * 1000


class TestWritePathErrors(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_wp")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _create(self, cls):
        return cls.create(self.test_dir, "q", region_size=REGION,
                          max_file_size=FILE, timeout_ns=TIMEOUT_NS)

    def _assert_eio_at_once(self, call):
        t = time.monotonic()
        with self.assertRaises(MmapqError) as cm:
            call()
        self.assertLess(time.monotonic() - t, 1.0)
        self.assertNotIsInstance(cm.exception, FileNotFoundError)
        self.assertIn("(rc=-10)", str(cm.exception))

    def test_next_payload_file_unusable_is_eio(self):
        q = self._create(IndexedQueue)
        a = q.create_appender()
        try:
            blocker = os.path.join(self.test_dir, "q", "payload_0_1")
            os.mkdir(blocker)
            for i in range(256):
                self.assertEqual(a.append(MSG), i)
            self._assert_eio_at_once(lambda: a.append(MSG))
            os.rmdir(blocker)
            self.assertEqual(a.append(MSG), 256)
        finally:
            a.close()
            q.close()

    def test_next_payload_file_unusable_is_eio_batch(self):
        q = self._create(IndexedBatchQueue)
        a = q.create_appender()

        def one_batch():
            with a.start_batch() as b:
                b.append(MSG)
                return b.commit()

        try:
            blocker = os.path.join(self.test_dir, "q", "payload_0_1")
            os.mkdir(blocker)
            for i in range(256):
                self.assertEqual(one_batch(), i)
            self._assert_eio_at_once(one_batch)
            os.rmdir(blocker)
            self.assertEqual(one_batch(), 256)
        finally:
            a.close()
            q.close()


if __name__ == "__main__":
    unittest.main()
