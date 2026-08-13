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

import contextlib
import gc
import os
import resource
import shutil
import tempfile
import unittest

from mmapq import IndexedBatchPoller, IndexedPoller, IndexedBatchQueue, IndexedQueue
from mmapq import batch_poller as _batch_poller_mod
from mmapq import single_poller as _single_poller_mod


def _count_type(tp):
    return sum(1 for o in gc.get_objects() if isinstance(o, tp))


class _DestroyCounter:
    """Count C-side teardowns without changing production code.

    ``__init__`` reads ``_destroy_poller`` as a module global when it
    registers the finalizer, so patching the module attribute *before*
    the pollers are constructed causes the counted wrapper to be the
    registered callback. The wrapper still performs the real teardown.

    Collecting the Python object is NOT the same event as releasing the C
    poller: a finalizer that is detached, replaced, or never registered
    leaves 10,000 C pollers alive while every Python-side signal (live
    count, ``gc.garbage``, RSS) stays clean. This counter is what makes
    the test fail in that case.
    """

    def __init__(self, module, attr):
        self._module = module
        self._attr = attr
        self._real = getattr(module, attr)
        self.count = 0

    def __call__(self, inner):
        self.count += 1
        return self._real(inner)

    @contextlib.contextmanager
    def patched(self):
        setattr(self._module, self._attr, self)
        try:
            yield self
        finally:
            setattr(self._module, self._attr, self._real)


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

        counter = _DestroyCounter(_single_poller_mod, "_destroy_poller")
        with counter.patched():
            pollers = [self.q.create_poller() for _ in range(10_000)]
            self.assertEqual(_count_type(IndexedPoller), 10_000)
            self.assertEqual(counter.count, 0, "no teardown before drop")
            del pollers

            gc.collect()

        # Primary signals.
        self.assertEqual(len(gc.garbage), 0, "unreachable cycles must not accumulate")
        # The C poller must actually be closed, not merely made unreachable.
        self.assertEqual(
            counter.count,
            10_000,
            f"only {counter.count} of 10,000 C pollers were closed on collection",
        )
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

        counter = _DestroyCounter(_batch_poller_mod, "_destroy_batch_poller")
        with counter.patched():
            pollers = [self.q.create_poller() for _ in range(10_000)]
            self.assertEqual(_count_type(IndexedBatchPoller), 10_000)
            self.assertEqual(counter.count, 0, "no teardown before drop")
            del pollers

            gc.collect()

        self.assertEqual(len(gc.garbage), 0, "unreachable cycles must not accumulate")
        self.assertEqual(
            counter.count,
            10_000,
            f"only {counter.count} of 10,000 C batch pollers were closed on collection",
        )
        after = _count_type(IndexedBatchPoller)
        self.assertLess(
            after, 100, f"{after} of 10,000 batch pollers leaked (not GC-collectible)"
        )

        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        self.assertLess(rss, 1_000_000, f"RSS {rss} kB unexpectedly large")


if __name__ == "__main__":
    unittest.main()
