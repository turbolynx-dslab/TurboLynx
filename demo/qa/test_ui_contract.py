"""Static contracts for the semantic prelude and compact query presentation."""

from pathlib import Path
import unittest


UI = (Path(__file__).parents[1] / "ui.html").read_text()


class UIContractTests(unittest.TestCase):
    def test_semantic_prelude_explains_labels_and_relations(self):
        self.assertIn("What the data means", UI)
        self.assertIn('class="semantic-node-group"', UI)
        self.assertIn('<span class="semantic-group-label">:NODE</span>', UI)
        self.assertIn('<div class="semantic-visits"><span>VISITS</span></div>', UI)
        self.assertIn("PROFILE_OF", UI)
        self.assertIn(":VENUE_PROFILE", UI)
        self.assertIn('class="semantic-profile-group"', UI)
        self.assertIn('class="profile-records"', UI)
        self.assertIn("kind = 'person'", UI)
        self.assertIn("Music fans", UI)
        self.assertIn("Old Town", UI)
        self.assertIn("genre · follows", UI)
        self.assertIn("neighborhood · since", UI)
        self.assertIn("common {name, kind}", UI)
        self.assertIn("40 schemas · 5 fields each", UI)
        self.assertIn("function runGraphReveal()", UI)
        self.assertIn("rawRevealed?runCGC():runGraphReveal()", UI)
        self.assertIn("const profileNodes=Array.from({length:40}", UI)
        self.assertIn("const profileEdges=profileNodes.map", UI)
        self.assertIn("type:'PROFILE_OF'", UI)
        self.assertIn("graphProgress<.30?'dots':graphProgress<.60?'edges':'spread'", UI)
        self.assertIn("const move=smooth((graphProgress-.60)/.38)", UI)

    def test_query_and_cta_stay_compact(self):
        self.assertIn("vp.*", UI)
        self.assertNotIn("vp.venue_type, …,", UI)
        self.assertIn('id="navnext" aria-label="Run step"', UI)
        self.assertIn('<span class="ms">arrow_forward</span>', UI)
        self.assertNotIn('id="rail-source"', UI)
        self.assertNotIn("Recorded samples · milliseconds", UI)
        self.assertNotIn('class="nav-target"', UI)
        self.assertNotIn("Basics {venue_type", UI)
        self.assertNotIn("Hours {opens_at", UI)


if __name__ == "__main__":
    unittest.main()
