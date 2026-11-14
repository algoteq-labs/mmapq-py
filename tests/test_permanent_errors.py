"""0.2.1 PERMANENT-NOT-TRANSIENT: a permanent failure is never MMAPQ_ETIMEDOUT."""
import os
import shutil
import tempfile
import unittest

import mmapq
from mmapq import IndexedBatchQueue, IndexedQueue, MmapqError, QueueFileNotFoundError


class TestPermanentErrors(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_mmap_pe")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_class_catchable_both_ways(self):
        self.assertIn("QueueFileNotFoundError", mmapq.__all__)
        self.assertTrue(issubclass(QueueFileNotFoundError, MmapqError))
        self.assertTrue(issubclass(QueueFileNotFoundError, FileNotFoundError))

    def test_missing_base_dir_is_enoent(self):
        base = os.path.join(self.test_dir, "absent")
        for cls in (IndexedQueue, IndexedBatchQueue):
            q = cls.create(base, "q")
            try:
                with self.assertRaises(QueueFileNotFoundError):
                    q.create_appender()
            finally:
                q.close()
            self.assertFalse(os.path.exists(base))

    def test_path_through_a_file_is_eio(self):
        base = os.path.join(self.test_dir, "file")
        with open(base, "wb"):
            pass
        q = IndexedQueue.create(base, "q")
        try:
            with self.assertRaises(MmapqError) as cm:
                q.create_appender()
            self.assertNotIsInstance(cm.exception, FileNotFoundError)
            self.assertIn("I/O error", str(cm.exception))
        finally:
            q.close()


if __name__ == "__main__":
    unittest.main()
