"""reclaim_files binding tests: the mark -> reclaim deletion protocol
through the wrapper on both queue types, at the 32 KiB region/file
geometry (one payload/header file per region — the smallest legal
geometry; a fresh lane starts at payload offset 64).

Offsets are exact: a 1000-byte message occupies 1004 framed bytes
(4-byte length prefix), a 1000-byte one-message batch occupies 1008
(4-byte count + 4 + 1000).
"""

import os
import tempfile
import unittest

from mmapq import IndexedBatchQueue, IndexedQueue, QueueClosedError, Runtime
from mmapq import _cffi
from mmapq._errors import check_rc


def payload(i):
    """1000 deterministic bytes: the message index in the first bytes,
    the rest a fixed pattern."""
    v = bytearray(b"\xa5" * 1000)
    head = f"msg-{i:03}".encode()
    v[: len(head)] = head
    return bytes(v)


def file_names(paths):
    return sorted(os.path.basename(p) for p in paths)


def create_single_small_geometry(directory):
    """Create a single-message queue with 32 KiB region/file through the
    C factory the binding already declares (the public IndexedQueue.create
    exposes no geometry parameters). Geometry applies only when a queue is
    created; on open every participant uses the geometry stored on disk,
    so the test then reopens it through the public API.

    Argument order copied from single_queue.py's own
    mmapq_indexed_queue_create call: directory, queue_name, region_size,
    ring_size, regions_to_map_ahead, max_file_size, start_index,
    timeout_ns, archivable, write_runtime, read_runtime, out.

    The factory itself persists nothing — the geometry reaches the meta
    file when the first appender opens, so one appender is opened (and
    closed) through this factory before it is closed.
    """
    ffi, lib = _cffi.ffi, _cffi.lib
    qptr = ffi.new("MmapqIndexedQueue **")
    rc = lib.mmapq_indexed_queue_create(
        directory.encode(), b"q",
        32768, 4, 1, 32768, 0, 10000000000, 1,
        ffi.NULL, ffi.NULL, qptr,
    )
    check_rc(rc, "mmapq_indexed_queue_create")
    aptr = ffi.new("MmapqIndexedAppender **")
    rc = lib.mmapq_indexed_queue_create_appender(qptr[0], aptr)
    check_rc(rc, "mmapq_indexed_queue_create_appender")
    lib.mmapq_indexed_appender_close(aptr[0])
    lib.mmapq_indexed_queue_close(qptr[0])


def open_single_small_geometry(directory):
    """Reopen the small-geometry queue through the public API."""
    q = IndexedQueue.create(directory, "q", archivable=True)
    return q


class TestSingleReclaim(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def test_single_positive(self):
        create_single_small_geometry(self.directory.name)
        q = open_single_small_geometry(self.directory.name)
        self.addCleanup(q.close)
        with q.create_appender() as a:
            for i in range(100):
                self.assertEqual(a.append(payload(i)), i)

        # Prove the small geometry really took: a payload file is exactly
        # one 32 KiB region.
        self.assertEqual(
            os.path.getsize(os.path.join(self.directory.name, "q", "payload_0_0")),
            32768,
        )

        arch = q.create_archiver()
        self.addCleanup(arch.close)
        arch.archive_up_to_index(80)
        files = arch.archivable_files()
        # Geometry at region = file = 32768, 100 messages of 1000 bytes
        # (1004 framed each), single appender (lane 0):
        #   file 0 [0, 32768):     msgs 0..=31 at 64 + 1004*i (ends 32192;
        #                          576 left < 1004 -> padding)
        #   file 1 [32768, 65536): msgs 32..=63 (ends 64896; 640 -> pad)
        #   file 2 [65536, 98304): msgs 64..=95; msg 79 ends at 81600
        #   file 3 [98304, ...):   msgs 96..=99
        # archive_up_to_index(80) -> boundary 81600: payload_0_0 and
        # payload_0_1 wholly below it; payload_0_2 straddles; header_0
        # (indices 0..=4095, 8-byte entries) is not below start_index 80.
        self.assertEqual(file_names(files), ["payload_0_0", "payload_0_1"])

        self.assertEqual(arch.reclaim_files(files), 2)
        for p in files:
            self.assertFalse(os.path.exists(p), f"{p} should be deleted")
        self.assertEqual(arch.archivable_files(), [])
        self.assertEqual(arch.reclaim_files(files), 0)

        r = q.create_reader()
        self.addCleanup(r.close)
        for i in range(80, 100):
            self.assertEqual(r.read(i), payload(i))

    def test_single_undo_protects(self):
        create_single_small_geometry(self.directory.name)
        q = open_single_small_geometry(self.directory.name)
        self.addCleanup(q.close)
        with q.create_appender() as a:
            for i in range(100):
                self.assertEqual(a.append(payload(i)), i)

        arch = q.create_archiver()
        self.addCleanup(arch.close)
        arch.archive_up_to_index(80)
        files = arch.archivable_files()
        self.assertNotEqual(files, [])

        arch.archive_up_to_index(0)  # undo — restores [0, 80)

        self.assertEqual(arch.reclaim_files(files), 0)
        for p in files:
            self.assertTrue(os.path.exists(p), f"{p} must survive the undo")

        r = q.create_reader()
        self.addCleanup(r.close)
        self.assertEqual(r.read(0), payload(0))

    def test_single_unlisted_path_untouched(self):
        create_single_small_geometry(self.directory.name)
        q = open_single_small_geometry(self.directory.name)
        self.addCleanup(q.close)
        unlisted = os.path.join(self.directory.name, "q", "not_a_queue_file")
        with open(unlisted, "wb") as f:
            f.write(b"not a queue file")

        arch = q.create_archiver()
        self.addCleanup(arch.close)
        self.assertEqual(arch.reclaim_files([unlisted]), 0)
        self.assertTrue(os.path.exists(unlisted), "an unmarked file is never deleted")
        self.assertEqual(arch.reclaim_files([]), 0)

    def test_single_type_error_for_non_str(self):
        create_single_small_geometry(self.directory.name)
        q = open_single_small_geometry(self.directory.name)
        self.addCleanup(q.close)
        arch = q.create_archiver()
        self.addCleanup(arch.close)
        with self.assertRaises(TypeError):
            arch.reclaim_files([b"bytes"])

    def test_single_value_error_for_nul(self):
        create_single_small_geometry(self.directory.name)
        q = open_single_small_geometry(self.directory.name)
        self.addCleanup(q.close)
        arch = q.create_archiver()
        self.addCleanup(arch.close)
        with self.assertRaises(ValueError):
            arch.reclaim_files(["a\x00b"])

    def test_single_closed_archiver_raises(self):
        create_single_small_geometry(self.directory.name)
        q = open_single_small_geometry(self.directory.name)
        self.addCleanup(q.close)
        arch = q.create_archiver()
        arch.close()
        with self.assertRaises(QueueClosedError):
            arch.reclaim_files([])


class TestBatchReclaim(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def _queue(self):
        q = IndexedBatchQueue.create(
            self.directory.name, "q",
            region_size=32768, max_file_size=32768,
            archivable=True, runtime=Runtime.region_mapper(),
        )
        self.addCleanup(q.close)
        return q

    def _append_100_batches(self, q):
        with q.create_appender() as a:
            for i in range(100):
                with a.start_batch() as w:
                    w.append(payload(i))
                    self.assertEqual(w.commit(), i)

    def test_batch_positive(self):
        q = self._queue()
        self._append_100_batches(q)

        arch = q.create_archiver()
        self.addCleanup(arch.close)
        arch.archive_up_to_index(80)
        files = arch.archivable_files()
        # Batch twin, one 1000-byte message per batch (1008 framed bytes:
        # 4-byte count + 4-byte length + 1000):
        #   file 0: batches 0..=31 at 64 + 1008*i (ends 32320; 448 -> pad)
        #   file 1: batches 32..=63 (ends 65024; 512 -> pad)
        #   file 2: batches 64..=95; batch 79 ends at 65536 + 1008*16 = 81664
        #   file 3: batches 96..=99
        # archive_up_to_index(80) -> boundary 81664: payload_0_0 and
        # payload_0_1 wholly below it; payload_0_2 straddles; header_0 not
        # below 80.
        self.assertEqual(file_names(files), ["payload_0_0", "payload_0_1"])

        self.assertEqual(arch.reclaim_files(files), 2)
        for p in files:
            self.assertFalse(os.path.exists(p), f"{p} should be deleted")
        self.assertEqual(arch.archivable_files(), [])
        self.assertEqual(arch.reclaim_files(files), 0)

        r = q.create_reader()
        self.addCleanup(r.close)
        for i in range(80, 100):
            self.assertEqual(r.read(i), [payload(i)])

    def test_batch_undo_protects(self):
        q = self._queue()
        self._append_100_batches(q)

        arch = q.create_archiver()
        self.addCleanup(arch.close)
        arch.archive_up_to_index(80)
        files = arch.archivable_files()
        self.assertNotEqual(files, [])

        arch.archive_up_to_index(0)  # undo

        self.assertEqual(arch.reclaim_files(files), 0)
        for p in files:
            self.assertTrue(os.path.exists(p), f"{p} must survive the undo")

        r = q.create_reader()
        self.addCleanup(r.close)
        self.assertEqual(r.read(0), [payload(0)])


# Geometry for the listing-size tests: 32 KiB regions and files, one
# 30,000-byte message per file (it does not fit beside another), so N
# messages fill payload files 0..N-1. Archiving up to N-1 leaves message N-1
# live; the lane's start_offset is the end of message N-2, inside file N-2,
# so exactly payload_0_0 .. payload_0_{N-3} (N-2 files) are listed.
def big_listing_names(n):
    return sorted(f"payload_0_{k}" for k in range(n - 2))


class TestListingPastTheFirstArray(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def test_single_listing_is_complete(self):
        msg = b"\x07" * 30000
        with IndexedQueue.create(self.directory.name, "q", archivable=True,
                                 region_size=32768,
                                 max_file_size=32768) as q:
            with q.create_appender() as a:
                for i in range(300):
                    self.assertEqual(a.append(msg), i)
            with q.create_archiver() as arch:
                arch.archive_up_to_index(299)
                listed = arch.archivable_files()
        self.assertEqual(len(listed), 298)
        self.assertEqual(file_names(listed), big_listing_names(300))

    def test_batch_listing_is_complete(self):
        msg = b"\x07" * 30000
        with IndexedBatchQueue.create(self.directory.name, "q",
                                      region_size=32768, max_file_size=32768,
                                      archivable=True) as q:
            with q.create_appender() as a:
                for i in range(300):
                    with a.start_batch() as w:
                        w.append(msg)
                        self.assertEqual(w.commit(), i)
            with q.create_archiver() as arch:
                arch.archive_up_to_index(299)
                listed = arch.archivable_files()
        self.assertEqual(len(listed), 298)
        self.assertEqual(file_names(listed), big_listing_names(300))


if __name__ == "__main__":
    unittest.main()
