import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.disaster_recovery_gate import _client_command, _table_counts

ROOT = Path(__file__).resolve().parents[1]


class PhaseSixGateTests(unittest.TestCase):
    def test_disaster_recovery_client_command_can_run_inside_disposable_container(self):
        with patch("scripts.disaster_recovery_gate.shutil.which", return_value="docker"):
            self.assertEqual(
                _client_command("postgres-client", "pg_dump", "--version"),
                ["docker", "exec", "postgres-client", "pg_dump", "--version"],
            )

    def test_disaster_recovery_table_counts_parse_json(self):
        completed = subprocess.CompletedProcess(
            ["psql"], 0, '{"memory": 2, "audit_log": 1}\n', ""
        )
        with patch("scripts.disaster_recovery_gate._run", return_value=completed):
            counts, error = _table_counts("postgres-client", "postgresql://db")
        self.assertEqual(counts, {"memory": 2, "audit_log": 1})
        self.assertEqual(error, "")

    def test_disaster_recovery_table_counts_reject_invalid_json(self):
        completed = subprocess.CompletedProcess(["psql"], 0, "not-json", "")
        with patch("scripts.disaster_recovery_gate._run", return_value=completed):
            counts, error = _table_counts("postgres-client", "postgresql://db")
        self.assertIsNone(counts)
        self.assertIn("invalid JSON", error)

    def test_security_gate_passes_for_release_tree(self):
        result = subprocess.run(
            [sys.executable, "scripts/security_gate.py"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        report = json.loads(result.stdout)
        self.assertTrue(report["ok"])
        self.assertEqual(report["secret_findings"], [])

    def test_disaster_recovery_gate_is_explicitly_not_run_without_databases(self):
        env = os.environ.copy()
        for name in (
            "DISASTER_RECOVERY_SOURCE_DSN",
            "DISASTER_RECOVERY_TARGET_DSN",
            "DISASTER_RECOVERY_ALLOW_DESTRUCTIVE",
        ):
            env.pop(name, None)
        result = subprocess.run(
            [sys.executable, "scripts/disaster_recovery_gate.py"],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(json.loads(result.stdout)["status"], "not_run")

    def test_release_image_has_non_root_runtime_contract(self):
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("USER 10001:10001", dockerfile)
        self.assertIn("io.trpc.agent-service.source-fingerprint", dockerfile)


if __name__ == "__main__":
    unittest.main()
