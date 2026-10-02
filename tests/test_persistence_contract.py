import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import unittest


WRITER = r'''
import sys
from mmapq import IndexedQueue, IndexedBatchQueue
directory, name, kind, phase = sys.argv[1:]
queue_type = IndexedQueue if kind == "single" else IndexedBatchQueue
with queue_type.create(directory, name) as queue:
    with queue.create_appender() as appender:
        if kind == "single":
            assert appender.append(b"prefix") == 0
            offset = appender.append_uncommitted(b"tail")
            if phase != "before":
                assert appender.commit(offset) == 1
            print("ready", flush=True)
            sys.stdin.read()
        else:
            with appender.start_batch() as writer:
                writer.append(b"prefix")
                assert writer.commit() == 0
            with appender.start_batch() as writer:
                writer.append(b"tail")
                if phase != "before":
                    assert writer.commit() == 1
                print("ready", flush=True)
                sys.stdin.read()
'''

READER = r'''
import sys
from pathlib import Path
from mmapq import IndexedQueue, IndexedBatchQueue
directory, name, kind, phase = sys.argv[1:]
queue_type = IndexedQueue if kind == "single" else IndexedBatchQueue
checkpoint = int((Path(directory) / "checkpoint").read_text())
expected_index = 1 if phase == "before" else 2
with queue_type.create(directory, name) as queue:
    with queue.create_reader() as reader:
        expected = None if phase == "before" else (b"tail" if kind == "single" else [b"tail"])
        assert reader.read(checkpoint + 1) == expected
        assert reader.last_index() == expected_index - 1
    with queue.create_appender() as appender:
        if kind == "single":
            assert appender.append(b"reopened") == expected_index
        else:
            with appender.start_batch() as writer:
                writer.append(b"reopened")
                assert writer.commit() == expected_index
    with queue.create_reader() as reader:
        assert reader.read(expected_index) == (b"reopened" if kind == "single" else [b"reopened"])
'''


class TestPersistenceContract(unittest.TestCase):
    def exercise(self, phase):
        base = os.environ.get("MMAPQ_FILE_TEST_DIR", str(Path(__file__).resolve().parents[1]))
        with tempfile.TemporaryDirectory(prefix="mmapq-restart-", dir=base) as directory:
            filesystem = subprocess.check_output(
                ["findmnt", "-n", "-o", "FSTYPE", "-T", directory], text=True).strip()
            self.assertNotIn(filesystem, ("tmpfs", "ramfs"), "choose file-backed MMAPQ_FILE_TEST_DIR")
            self.assertTrue(filesystem)
            for kind in ("single", "batch"):
                with self.subTest(kind=kind, phase=phase, filesystem=filesystem):
                    (Path(directory) / "checkpoint").write_text("0")
                    arguments = [directory, kind, kind, phase]
                    producer = subprocess.Popen([sys.executable, "-c", WRITER, *arguments],
                                                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE, text=True)
                    try:
                        readable, _, _ = select.select([producer.stdout], [], [], 15)
                        self.assertTrue(readable, "producer did not acknowledge publication state")
                        self.assertEqual(producer.stdout.readline().strip(), "ready")
                        if phase == "clean":
                            _, stderr = producer.communicate("exit", timeout=15)
                            self.assertEqual(producer.returncode, 0, stderr)
                        else:
                            producer.kill()
                            producer.communicate(timeout=15)
                            self.assertEqual(producer.returncode, -signal.SIGKILL)
                    finally:
                        if producer.poll() is None:
                            producer.kill()
                        producer.communicate(timeout=15)
                    consumer = subprocess.run([sys.executable, "-c", READER, *arguments],
                                              capture_output=True, text=True, timeout=20)
                    self.assertEqual(consumer.returncode, 0, consumer.stderr)

    def test_clean_reopen_and_application_checkpoint(self):
        self.exercise("clean")

    def test_process_killed_before_publication(self):
        self.exercise("before")

    def test_process_killed_after_publication(self):
        self.exercise("after")
