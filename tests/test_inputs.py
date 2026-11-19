import json
from pathlib import Path
import unittest


class InputShapeTest(unittest.TestCase):
    def test_sources_declare_grain(self):
        rows = json.loads((Path(__file__).parents[1] / "data" / "sources.json").read_text(encoding="utf-8"))
        self.assertTrue(rows)
        self.assertTrue(all(row["source"] and row["grain"] for row in rows))


if __name__ == "__main__":
    unittest.main()
