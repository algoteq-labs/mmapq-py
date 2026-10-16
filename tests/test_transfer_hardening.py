import tempfile
import unittest
from pathlib import Path

from mmapq import IndexedQueue, IndexedBatchQueue, QueueCorruptError
from mmapq import transfer
from mmapq._errors import check_rc


class TestTransferHardening(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def sized_queue(self, queue_type, name, region_size):
        kind = "indexed" if queue_type is IndexedQueue else "indexed_batch"
        struct = "MmapqIndexedQueue" if queue_type is IndexedQueue else "MmapqIndexedBatchQueue"
        output = transfer.ffi.new(struct + " **")
        result = getattr(transfer.lib, "mmapq_" + kind + "_queue_create")(
            self.directory.name.encode(), name.encode(), region_size, 16, 1,
            region_size * 256, 0, 10_000_000_000, 0,
            transfer.ffi.NULL, transfer.ffi.NULL, output)
        check_rc(result)
        return queue_type(output[0])

    def test_copy_rejects_oversized_destination_message(self):
        for source_type, destination_type, copy in (
            (IndexedQueue, IndexedQueue, transfer.copy_queue),
            (IndexedBatchQueue, IndexedBatchQueue, transfer.copy_batch_queue),
            (IndexedBatchQueue, IndexedQueue, transfer.copy_batch_to_normal),
            (IndexedQueue, IndexedBatchQueue, transfer.copy_normal_to_batch),
        ):
            with self.subTest(operation=copy.__name__):
                source_name = copy.__name__ + "_source"
                destination_name = copy.__name__ + "_destination"
                with self.sized_queue(source_type, source_name, 65536) as source:
                    with source.create_appender() as appender:
                        # 40000 bytes: fits the 64 KB source region, exceeds
                        # the 32 KB destination region (32768 - 4).
                        for message in (b"prefix", b"x" * 40000, b"must-not-copy"):
                            if source_type is IndexedQueue:
                                appender.append(message)
                            else:
                                with appender.start_batch() as writer:
                                    writer.append(message)
                                    writer.commit()
                with self.sized_queue(destination_type, destination_name, 32768) as destination:
                    with destination.create_appender():
                        pass
                with self.assertRaises(ValueError):
                    copy(self.directory.name, source_name, self.directory.name,
                         destination_name, allow_append=True)
                with destination_type.create(self.directory.name, destination_name) as destination:
                    with destination.create_reader() as reader:
                        self.assertEqual(reader.last_index(), 0)
                        expected = b"prefix" if destination_type is IndexedQueue else [b"prefix"]
                        self.assertEqual(reader.read(0), expected)
                        self.assertIsNone(reader.read(1))

    def test_truncated_import_is_not_partial_success(self):
        for queue_type, export, import_stream in (
            (IndexedQueue, transfer.export_queue, transfer.import_queue),
            (IndexedQueue, transfer.export_queue, transfer.import_flat_to_batch),
            (IndexedBatchQueue, transfer.export_batch_queue, transfer.import_batch_queue),
            (IndexedBatchQueue, transfer.export_batch_queue, transfer.import_batch_stream),
        ):
            with self.subTest(operation=import_stream.__name__):
                name = import_stream.__name__
                with queue_type.create(self.directory.name, name) as queue:
                    with queue.create_appender() as appender:
                        if queue_type is IndexedQueue:
                            appender.append(b"payload")
                        else:
                            with appender.start_batch() as writer:
                                writer.append(b"payload")
                                writer.commit()
                path = Path(self.directory.name) / (name + ".export")
                export(self.directory.name, name, str(path))
                with path.open("r+b") as output:
                    output.truncate(path.stat().st_size - 1)
                with self.assertRaises(QueueCorruptError):
                    import_stream(str(path), self.directory.name, name + "_copy")

    def test_mapper_exception_aborts_copy_and_import(self):
        for source_type, destination_type, copy, export, import_stream in (
            (IndexedQueue, IndexedQueue, transfer.copy_queue, transfer.export_queue, transfer.import_queue),
            (IndexedQueue, IndexedBatchQueue, transfer.copy_normal_to_batch, transfer.export_queue, transfer.import_flat_to_batch),
            (IndexedBatchQueue, IndexedQueue, transfer.copy_batch_to_normal, transfer.export_batch_queue, transfer.import_batch_stream),
            (IndexedBatchQueue, IndexedBatchQueue, transfer.copy_batch_queue, transfer.export_batch_queue, transfer.import_batch_queue),
        ):
            name = copy.__name__
            with source_type.create(self.directory.name, name) as source:
                for appender_id in (0, 1):
                    with source.create_appender_with_id(appender_id) as appender:
                        if source_type is IndexedQueue:
                            appender.append(b"message")
                        else:
                            with appender.start_batch() as writer:
                                writer.append(b"message")
                                writer.commit()
            path = str(Path(self.directory.name) / (name + ".export"))
            export(self.directory.name, name, path)
            for importing in (False, True):
                failure = ValueError("mapper failed")
                calls = []

                def mapper(appender_id):
                    calls.append(appender_id)
                    if appender_id == 1:
                        raise failure
                    return appender_id

                destination_name = name + str(importing)
                with self.subTest(operation=name, importing=importing):
                    with self.assertRaises(ValueError) as caught:
                        if importing:
                            import_stream(path, self.directory.name, destination_name, mapper=mapper)
                        else:
                            copy(self.directory.name, name, self.directory.name, destination_name, mapper=mapper)
                    self.assertIs(caught.exception, failure)
                    self.assertEqual(calls, [0, 1])
                    with destination_type.create(self.directory.name, destination_name) as destination:
                        with destination.create_reader() as reader:
                            self.assertEqual(reader.last_index(), 0)
                            self.assertIsNone(reader.read(1))
