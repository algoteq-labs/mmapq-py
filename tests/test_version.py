import importlib.metadata
import unittest

import mmapq


class VersionTest(unittest.TestCase):
    def test_version_matches_installed_metadata(self):
        self.assertEqual(mmapq.__version__, importlib.metadata.version("mmapq"))
        self.assertRegex(mmapq.__version__, r"^\d+\.\d+\.\d+(\.dev\d+)?$")
