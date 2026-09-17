import gc
import tempfile
import unittest
import subprocess
import sys
from unittest.mock import patch

from mmapq import IndexedBatchQueue, IndexedQueue, MmapqError, NextMove, QueueClosedError
from mmapq import export_queue, export_batch_queue
from mmapq import copy_batch_queue, copy_batch_to_normal
from mmapq import QueueCorruptError
from mmapq import Runtime, IndexedReader, IndexedBatchReader, IndexedArchiver
from mmapq import batch_appender


class TestSafety(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def queue(self, queue_type):
        queue = queue_type.create(self.directory.name, queue_type.__name__)
        self.addCleanup(queue.close)
        return queue

    def test_batch_writer_rejects_append_after_commit(self):
        queue = self.queue(IndexedBatchQueue)
        with queue.create_appender() as appender:
            with appender.start_batch() as writer:
                self.assertEqual(writer.append(b"first"), 0)
                self.assertEqual(writer.commit(), 0)
                with self.assertRaises(MmapqError):
                    writer.append(b"second")

    def test_batch_writer_rejects_use_after_rollback(self):
        queue = self.queue(IndexedBatchQueue)
        with queue.create_appender() as appender:
            with appender.start_batch() as writer:
                writer.append(b"discarded")
            with self.assertRaises(MmapqError):
                writer.commit()

    def test_batch_appender_rejects_overlapping_writers(self):
        queue = self.queue(IndexedBatchQueue)
        with queue.create_appender() as appender:
            with appender.start_batch() as writer:
                writer.append(b"first")
                with self.assertRaises(MmapqError):
                    appender.start_batch()

    def test_batch_appender_finalizes_once(self):
        queue = self.queue(IndexedBatchQueue)
        real_lib = batch_appender.lib
        appender = queue.create_appender()
        inner = appender._inner
        try:
            with patch.object(batch_appender, "lib") as counted:
                del appender
                gc.collect()
                self.assertEqual(counted.mmapq_indexed_batch_appender_close.call_count, 1)
        finally:
            real_lib.mmapq_indexed_batch_appender_close(inner)

    def test_closed_queue_factories_raise_queue_closed(self):
        for queue_type in (IndexedQueue, IndexedBatchQueue):
            queue = self.queue(queue_type)
            queue.close()
            for method in ("create_appender", "create_poller", "create_reader", "create_archiver"):
                with self.subTest(queue=queue_type.__name__, method=method):
                    with self.assertRaises(QueueClosedError):
                        getattr(queue, method)()

    def test_closed_appender_raises_queue_closed(self):
        for queue_type in (IndexedQueue, IndexedBatchQueue):
            queue = self.queue(queue_type)
            appender = queue.create_appender()
            appender.close()
            with self.subTest(queue=queue_type.__name__):
                with self.assertRaises(QueueClosedError):
                    appender.id()

    def test_invalid_handler_result_raises_without_advancing(self):
        for queue_type in (IndexedQueue, IndexedBatchQueue):
            queue = self.queue(queue_type)
            with queue.create_appender() as appender:
                if queue_type is IndexedQueue:
                    appender.append(b"message")
                else:
                    with appender.start_batch() as writer:
                        writer.append(b"message")
                        writer.commit()
            with queue.create_poller() as poller:
                for invalid in (None, 99, "advance"):
                    with self.subTest(queue=queue_type.__name__, invalid=invalid):
                        with self.assertRaises((TypeError, ValueError)):
                            poller.poll(lambda *args: invalid)
                received = []
                poller.poll(lambda *args: received.append(args[0]) or NextMove.ADVANCE)
                self.assertEqual(received, [0])

    def test_closed_consumers_raise_queue_closed(self):
        for queue_type in (IndexedQueue, IndexedBatchQueue):
            queue = self.queue(queue_type)
            for factory, method in (("create_poller", "start_index"),
                                    ("create_archiver", "start_index")):
                consumer = getattr(queue, factory)()
                consumer.close()
                with self.subTest(queue=queue_type.__name__, factory=factory):
                    with self.assertRaises(QueueClosedError):
                        getattr(consumer, method)()

    def test_export_reports_buffered_write_failure(self):
        for queue_type, export in ((IndexedQueue, export_queue),
                                   (IndexedBatchQueue, export_batch_queue)):
            queue = self.queue(queue_type)
            with queue.create_appender() as appender:
                if queue_type is IndexedQueue:
                    appender.append(b"message")
                else:
                    with appender.start_batch() as writer:
                        writer.append(b"message")
                        writer.commit()
            with self.subTest(queue=queue_type.__name__):
                with self.assertRaises(OSError):
                    export(self.directory.name, queue_type.__name__, "/dev/full")

    def test_corrupt_batch_message_is_not_delivered_or_exported(self):
        from pathlib import Path
        import struct

        queue = self.queue(IndexedBatchQueue)
        with queue.create_appender() as appender:
            appender_id = appender.id()
            with appender.start_batch() as writer:
                writer.append(b"message")
                index = writer.commit()
        with queue.create_reader() as reader:
            self.assertEqual(reader.read(index), [b"message"])
            offset = reader._ctx.batch_offset
        path = Path(self.directory.name) / "IndexedBatchQueue" / f"payload_{appender_id}_0"
        with path.open("r+b") as payload:
            payload.seek(offset + 4)
            payload.write(struct.pack("<i", 0))
        with queue.create_reader() as reader:
            with self.subTest(operation="read"):
                with self.assertRaises(QueueCorruptError):
                    reader.read(index)
        with queue.create_poller() as poller:
            received = []
            with self.subTest(operation="poll"):
                with self.assertRaises(QueueCorruptError):
                    poller.poll(lambda *args: received.append(args) or NextMove.ADVANCE)
                self.assertEqual(received, [])
        with self.subTest(operation="export"):
            with self.assertRaises(QueueCorruptError):
                export_batch_queue(self.directory.name, "IndexedBatchQueue",
                                   str(Path(self.directory.name) / "broken.mmapq"))
        for copy in (copy_batch_queue, copy_batch_to_normal):
            with self.subTest(operation=copy.__name__):
                with self.assertRaises(QueueCorruptError):
                    copy(self.directory.name, "IndexedBatchQueue",
                         self.directory.name, copy.__name__)

    def test_factories_reject_closed_runtime(self):
        runtime = Runtime.region_mapper()
        runtime.close()
        for factory in (IndexedQueue, IndexedBatchQueue, IndexedReader,
                        IndexedBatchReader, IndexedArchiver):
            with self.subTest(factory=factory.__name__):
                with self.assertRaises(QueueClosedError):
                    instance = factory.create(self.directory.name, "closed_runtime", runtime=runtime)
                    instance.close()
        for factory in (IndexedQueue, IndexedBatchQueue):
            for keyword in ("write_runtime", "read_runtime"):
                with self.subTest(factory=factory.__name__, keyword=keyword):
                    with self.assertRaises(QueueClosedError):
                        instance = factory.create(self.directory.name, "closed_runtime",
                                                  **{keyword: runtime})
                        instance.close()

    def test_poller_rejects_close_and_reentry_from_handler(self):
        script = '''
import sys
from mmapq import IndexedQueue, IndexedBatchQueue, MmapqError, NextMove
queue_type = IndexedQueue if sys.argv[2] == "single" else IndexedBatchQueue
with queue_type.create(sys.argv[1], sys.argv[2] + sys.argv[3]) as queue:
    with queue.create_appender() as appender:
        if queue_type is IndexedQueue:
            appender.append(b"message")
        else:
            with appender.start_batch() as writer:
                writer.append(b"message")
                writer.commit()
    with queue.create_poller() as poller:
        def handler(*args):
            try:
                if sys.argv[3] == "close":
                    poller.close()
                else:
                    poller.poll(lambda *args: NextMove.STOP)
            except MmapqError:
                return NextMove.ADVANCE
            raise AssertionError("operation was allowed during callback")
        poller.poll(handler)
'''
        for queue_type in ("single", "batch"):
            for operation in ("close", "poll"):
                with self.subTest(queue=queue_type, operation=operation):
                    result = subprocess.run(
                        [sys.executable, "-c", script, self.directory.name, queue_type, operation],
                        capture_output=True, text=True, timeout=30,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
