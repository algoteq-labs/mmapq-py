from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest


class TestReadme(unittest.TestCase):
    def test_quick_start_commit_and_reopen_in_separate_processes(self):
        readme = Path(__file__).parents[1] / "README.md"
        blocks = re.findall(r"```python\n(.*?)\n```", readme.read_text(), re.DOTALL)
        self.assertGreaterEqual(len(blocks), 3)
        with tempfile.TemporaryDirectory() as directory:
            for index, block in enumerate(blocks[:3]):
                with self.subTest(block=index):
                    result = subprocess.run(
                        [sys.executable, "-c", block], cwd=directory,
                        capture_output=True, text=True, timeout=30,
                    )
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertTrue((Path(directory) / "data/events.mmapq").is_file())
