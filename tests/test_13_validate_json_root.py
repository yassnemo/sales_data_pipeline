import json
import tempfile
import unittest
from pathlib import Path
from pipeline import read_json


class JsonShapeTests(unittest.TestCase):
    def test_object_root_has_clear_error(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sales.json"
            path.write_text(json.dumps({"date": "2024-01-01"}), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "array of sales objects"):
                read_json(path)
