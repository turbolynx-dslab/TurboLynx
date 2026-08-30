"""Contract checks for the semantic VENUE_PROFILE catalog."""
import sys
from pathlib import Path
import unittest

DEMO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DEMO))
from venue_profile_schema import SECTIONS, example_value  # noqa: E402


class VenueProfileSchemaTests(unittest.TestCase):
    def test_catalog_is_exactly_40_by_5_with_unique_properties(self):
        keys = [key for _, fields in SECTIONS for key in fields]
        self.assertEqual(len(SECTIONS), 40)
        self.assertTrue(all(len(fields) == 5 for _, fields in SECTIONS))
        self.assertEqual(len(keys), 200)
        self.assertEqual(len(set(keys)), 200)

    def test_examples_are_presentable_values(self):
        events = dict(zip(SECTIONS[10][1],
                          [example_value(key, section=10, offset=i)
                           for i, key in enumerate(SECTIONS[10][1])]))
        self.assertEqual(events["event_type"], "live set")
        self.assertEqual(events["average_duration"], "120 min")
        self.assertNotIn("p00_", " ".join(events))


if __name__ == "__main__":
    unittest.main()
