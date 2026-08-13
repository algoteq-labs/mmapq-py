"""Lifecycle tests for pollers — the 19c poller-leak unknown.

single_poller.py / batch_poller.py store ``ffi.new_handle(self)`` on the
instance, a ``self -> _handle -> self`` reference cycle. Whether CPython's
GC can collect cffi handles is unmeasured (19c). These tests create many
pollers, drop them without ``close()``, force collection, and assert the
pollers were reclaimed.

If these tests fail, pollers leak when the user drops them without
``close()`` and the fix is to stop storing the handle on the instance.
If they pass, the unknown is resolved as "no leak" and the tests remain
as a guard.
"""

import gc
import os
import resource
import shutil
import tempfile
import unittest

from mmapq import IndexedBatchPoller, IndexedPoller, IndexedBatchQueue, IndexedQueue


def _count_type(tp):
    return sum(1 for o in gc.get_objects() if isinstance(o, tp))


class TestSinglePollerLifecycle(unittest.TestCase):
    """10,000 single-message pollers dropped without close() must be collectible."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_poller_lc")
        self.q = IndexedQueue.create(self.test_dir, "poll_lc")

    def tearDown(self):
        self.q.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_tenk_single_pollers_collectible_without_close(self):
        gc.collect()
        self.assertEqual(len(gc.garbage), 0, "baseline gc.garbage must be empty")
        baseline = _count_type(IndexedPoller)
        self.assertLessEqual(baseline, 0, "baseline should have no live pollers")

        pollers = [self.q.create_poller() for _ in range(10_000)]
        self.assertEqual(_count_type(IndexedPoller), 10_000)
        del pollers

        gc.collect()
        # Primary signals.
        self.assertEqual(len(gc.garbage), 0, "unreachable cycles must not accumulate")
        after = _count_type(IndexedPoller)
        # Generous threshold: a 10k leak is dramatic, not marginal. Allow a
        # tiny transient slack for interpreter bookkeeping.
        self.assertLess(
            after, 100, f"{after} of 10,000 pollers leaked (not GC-collectible)"
        )

        # Corroboration: RSS must not balloon to a 10k-struct leak.
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        self.assertLess(rss, 1_000_000, f"RSS {rss} kB unexpectedly large")


class TestBatchPollerLifecycle(unittest.TestCase):
    """10,000 batch pollers dropped without close() must be collectible."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(suffix="_poller_lc")
        self.q = IndexedBatchQueue.create(self.test_dir, "poll_lc")

    def tearDown(self):
        self.q.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_tenk_batch_pollers_collectible_without_close(self):
        gc.collect()
        self.assertEqual(len(gc.garbage), 0, "baseline gc.garbage must be empty")
        baseline = _count_type(IndexedBatchPoller)
        self.assertLessEqual(baseline, 0, "baseline should have no live pollers")

        pollers = [self.q.create_poller() for _ in range(10_000)]
        self.assertEqual(_count_type(IndexedBatchPoller), 10_000)
        del pollers

        gc.collect()
        self.assertEqual(len(gc.garbage), 0, "unreachable cycles must not accumulate")
        after = _count_type(IndexedBatchPoller)
        self.assertLess(
            after, 100, f"{after} of 10,000 batch pollers leaked (not GC-collectible)"
        )

        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        self.assertLess(rss, 1_000_000, f"RSS {rss} kB unexpectedly large")


if __name__ == "__main__":
    unittest.main()
