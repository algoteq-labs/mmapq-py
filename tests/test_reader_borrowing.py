import gc
import subprocess
import sys
import tempfile
import threading
import unittest
import weakref
from unittest.mock import patch

from mmapq import IndexedQueue, IndexedReader, Runtime
from mmapq._cffi import ffi


class TestReaderBorrowing(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="mmapq-borrow-")
        self.queue = IndexedQueue.create(self.directory.name, "q")
        with self.queue.create_appender() as appender:
            appender.append(b"first")
            appender.append(b"second")
        self.reader = self.queue.create_reader()

    def tearDown(self):
        self.reader.close()
        self.queue.close()
        self.directory.cleanup()

    def test_close_rejects_view_and_surviving_alias(self):
        view = self.reader.read_view(0)
        alias = view[1:]
        try:
            # On the old implementation close succeeds; assertRaises stops
            # here before any access to the now-dangling view or alias.
            with self.assertRaises(BufferError):
                self.reader.close()
            view.release()
            with self.assertRaises(BufferError):
                self.reader.close()
            self.assertEqual(bytes(alias), b"irst")
        finally:
            alias.release()
            view.release()
        self.reader.close()

    def test_reads_and_queries_reject_outstanding_borrow(self):
        operations = [
            lambda: self.reader.read(1), self.reader.read_first,
            self.reader.read_last, lambda: self.reader.read_view(1),
            lambda: self.reader.entry_status(1), self.reader.last_index,
            self.reader.start_index,
        ]
        for operation in operations:
            with self.subTest(operation=operation):
                with self.reader.read_view(0) as view:
                    with self.assertRaises(BufferError):
                        operation()
                    self.assertEqual(bytes(view), b"first")
                    self.assertEqual(self.reader.appender_id(), 0)
                operation()
                # A successful read_view returned above owns a temporary view;
                # Python releases it when the unused result is discarded.
        self.assertEqual(self.reader.read(1), b"second")

    def test_exporter_cannot_reopen_released_snapshot(self):
        view = self.reader.read_view(0)
        exporter = view.obj
        second = memoryview(exporter)
        view.release()
        try:
            with self.assertRaises(BufferError):
                self.reader.close()
            self.assertEqual(bytes(second), b"first")
        finally:
            second.release()
        self.assertEqual(self.reader.read(1), b"second")
        with self.assertRaises(BufferError):
            memoryview(exporter)

    def test_views_and_reexports_are_readonly(self):
        with self.reader.read_view(0) as view:
            self.assertTrue(view.readonly)
            with self.assertRaises(TypeError):
                view[0] = 0
            with memoryview(view.obj) as other:
                self.assertTrue(other.readonly)
                with self.assertRaises(BufferError):
                    ffi.from_buffer(other, require_writable=True)

    def test_cffi_buffer_consumer_retains_borrow(self):
        view = self.reader.read_view(0)
        consumer = ffi.from_buffer("const char[]", view)
        try:
            self.assertEqual(ffi.cast("uintptr_t", consumer),
                             ffi.cast("uintptr_t", self.reader._ctx.data))
            with self.assertRaises(BufferError):
                view.release()
            with self.assertRaises(BufferError):
                self.reader.close()
            self.assertEqual(bytes(ffi.buffer(consumer)), b"first")
        finally:
            ffi.release(consumer)
            view.release()
        self.reader.close()

    def test_slice_cast_and_readonly_aliases_retain_borrow(self):
        view = self.reader.read_view(0)
        aliases = [view[::2], view.cast("B"), view.toreadonly(), memoryview(view)]
        view.release()
        try:
            for alias in aliases:
                with self.assertRaises(BufferError):
                    self.reader.close()
                alias.release()
        finally:
            for alias in aliases:
                alias.release()
        self.reader.close()

    def test_view_retains_reader_until_last_alias_release(self):
        reader = self.queue.create_reader()
        reference = weakref.ref(reader)
        finalized = reader._finalizer
        view = reader.read_view(0)
        alias = view[1:]
        del reader
        gc.collect()
        self.assertIsNotNone(reference())
        self.assertEqual(bytes(alias), b"irst")
        view.release()
        gc.collect()
        self.assertTrue(finalized.alive)
        alias.release()
        gc.collect()
        self.assertIsNone(reference())
        self.assertFalse(finalized.alive)

    def test_dropped_views_release_borrow(self):
        view = self.reader.read_view(0)
        alias = view[1:]
        del view
        gc.collect()
        with self.assertRaises(BufferError):
            self.reader.close()
        del alias
        gc.collect()
        self.assertEqual(self.reader.read(1), b"second")

    def test_unreachable_reader_view_cycle_is_collected(self):
        reader = self.queue.create_reader()
        reference = weakref.ref(reader)
        finalized = reader._finalizer
        reader.saved_view = reader.read_view(0)
        del reader
        gc.collect()
        self.assertIsNone(reference())
        self.assertFalse(finalized.alive)

    def test_shutdown_finalizers_preserve_live_view(self):
        result = subprocess.run([sys.executable, "-c", """
import sys, weakref
from mmapq import IndexedReader
reader = IndexedReader.create(sys.argv[1], 'q')
view = reader.read_view(0)
# Simulate weakref.finalize's exit hook in this isolated process. Check the
# finalizer state first, so a regression never reads an unmapped pointer.
weakref.finalize._exitfunc()
assert reader._finalizer.alive, 'shutdown invalidated a live view'
assert bytes(view) == b'first'
view.release()
reader.close()
""", self.directory.name], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_reader_context_exit_rejects_borrow(self):
        view = None
        try:
            with self.assertRaises(BufferError):
                with self.reader:
                    view = self.reader.read_view(0)
            self.assertEqual(bytes(view), b"first")
        finally:
            if view is not None:
                view.release()
        self.reader.close()

    def test_missing_entry_does_not_acquire_borrow(self):
        self.assertIsNone(self.reader.read_view(99))
        self.assertEqual(self.reader.read(0), b"first")
        self.reader.close()

    def test_reader_context_preserves_processing_exception_with_live_view(self):
        original = ValueError("processing failed")
        view = None
        try:
            with self.assertRaises(ValueError) as caught:
                with self.reader:
                    view = self.reader.read_view(0)
                    raise original
            self.assertIs(caught.exception, original)
            self.assertTrue(self.reader._finalizer.alive)
            self.assertEqual(bytes(view), b"first")
            with self.assertRaises(BufferError):
                self.reader.close()
        finally:
            if view is not None:
                view.release()
        self.reader.close()
        self.assertIsNone(self.reader._inner)
        self.assertFalse(self.reader._finalizer.alive)

    def test_nested_view_context_closes_reader_on_processing_exception(self):
        original = ValueError("processing failed")
        with self.assertRaises(ValueError) as caught:
            with self.reader:
                with self.reader.read_view(0) as view:
                    self.assertEqual(bytes(view), b"first")
                    raise original
        self.assertIs(caught.exception, original)
        self.assertIsNone(self.reader._inner)
        self.assertFalse(self.reader._finalizer.alive)

    def test_reader_context_does_not_suppress_other_cleanup_errors(self):
        original = ValueError("processing failed")
        cleanup = OSError("cleanup failed")
        with patch.object(self.reader, "close", side_effect=cleanup):
            with self.assertRaises(OSError) as caught:
                with self.reader:
                    raise original
        self.assertIs(caught.exception, cleanup)
        self.assertIs(cleanup.__context__, original)

    def test_async_view_keeps_mapping_valid(self):
        runtime = Runtime.region_mapper()
        reader = IndexedReader.create(self.directory.name, "q", runtime=runtime)
        try:
            with reader.read_view(0) as view:
                with self.assertRaises(BufferError):
                    reader.read(1)
                with self.assertRaises(BufferError):
                    reader.close()
                self.assertEqual(bytes(view), b"first")
            self.assertEqual(reader.read(1), b"second")
        finally:
            reader.close()
            runtime.close()

    def test_close_cannot_overlap_view_creation(self):
        # Pause immediately before export to force the otherwise tiny window
        # between the C read and publication of the borrow guard.
        from mmapq import reader as module
        entered = threading.Event()
        proceed = threading.Event()
        original = module.make_view
        views, errors = [], []

        def delayed(*args):
            entered.set()
            if not proceed.wait(5):
                raise AssertionError("test export gate timed out")
            return original(*args)

        def read():
            try:
                views.append(self.reader.read_view(0))
            except BaseException as error:
                errors.append(error)

        with patch.object(module, "make_view", delayed):
            worker = threading.Thread(target=read)
            worker.start()
            try:
                self.assertTrue(entered.wait(5))
                with self.assertRaises(BufferError):
                    self.reader.close()
            finally:
                proceed.set()
                worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        try:
            self.assertEqual(bytes(views[0]), b"first")
        finally:
            for view in views:
                view.release()

    def test_failed_export_does_not_leave_reader_borrowed(self):
        with patch("mmapq.reader.make_view", side_effect=MemoryError):
            with self.assertRaises(MemoryError):
                self.reader.read_view(0)
        self.assertEqual(self.reader.read(1), b"second")


if __name__ == "__main__":
    unittest.main()
