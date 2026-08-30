"""Engine-launch regressions; run with python3 -m unittest discover -s demo/qa."""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


class EngineLaunchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        (root / "profile_cols.txt").write_text(
            "vp.venue_type AS venue_type, vp.capacity AS capacity")
        engine = root / "engine"
        engine.write_text(
            f"#!{sys.executable}\n"
            "import os, sys\n"
            "print('result\\x00')\n"
            "print(os.environ.get('TLX_SSRF_OFF', 'unset'))\n"
            "print('diagnostic', file=sys.stderr)\n"
            "sys.exit(7 if '--fail' in sys.argv else 0)\n"
        )
        engine.chmod(0o755)
        spec = importlib.util.spec_from_file_location(
            "booth_server", Path(__file__).resolve().parents[1] / "server.py")
        self.server = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, {"SRC": str(root), "BIN": str(engine)}):
            spec.loader.exec_module(self.server)

    def test_query_uses_semantic_venue_profile(self):
        self.assertIn("(vp:`VENUE_PROFILE`)-[:PROFILE_OF]->(c)",
                      self.server.GROUPBY)
        self.assertIn("vp.id AS pid", self.server.RET_T10)
        self.assertIn("vp.venue_type AS venue_type", self.server.RET_FULL)

    def test_launch_without_external_timeout(self):
        with patch.dict(os.environ, {"PATH": "", "TLX_SSRF_OFF": "inherited"}):
            output = self.server.run_engine([])
            explicit = self.server.run_engine([], {"TLX_SSRF_OFF": "1"})
        self.assertEqual(output, "result\nunset\n\ndiagnostic\n")
        self.assertEqual(explicit, "result\n1\n\ndiagnostic\n")

    def test_nonzero_exit_preserves_diagnostic(self):
        with self.assertRaisesRegex(RuntimeError, "engine exited 7:.*") as error:
            self.server.run_engine(["--fail"])
        self.assertIn("diagnostic", str(error.exception))

    def test_timeout_reports_engine_failure(self):
        with patch.object(self.server.subprocess, "run", side_effect=
                          subprocess.TimeoutExpired("engine", 180)):
            with self.assertRaisesRegex(RuntimeError, "engine run timed out"):
                self.server.run_engine([])


if __name__ == "__main__":
    unittest.main()
