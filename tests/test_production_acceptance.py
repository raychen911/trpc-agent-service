import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import production_acceptance
from scripts.evidence_lineage import make_evidence, release_binding


class ProductionAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.context = {
            "release_id": "online-im-test",
            "context_sha256": "a" * 64,
            "source_fingerprint": {"value": "b" * 64},
        }
        self.lock = {
            "lock_sha256": "c" * 64,
            "images": {
                "initial": {"digest": "sha256:" + "d" * 64},
                "upgrade": {"digest": "sha256:" + "e" * 64},
            },
        }
        self.environment = patch.dict(
            os.environ,
            {
                "TRPC_RELEASE_CONTEXT": "release-context.json",
                "TRPC_CANDIDATE_LOCK": "candidate-lock.json",
            },
            clear=False,
        )
        self.environment.start()

    def tearDown(self):
        self.environment.stop()

    def _evidence_path(self, payload):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "online-im.json"
            evidence = make_evidence(
                "online-im",
                "scripts.online_im_gate",
                payload,
                release_binding(self.context, self.lock),
            )
            path.write_text(json.dumps(evidence), encoding="utf-8")
            with patch.object(production_acceptance, "verify_context", return_value=self.context), patch.object(
                production_acceptance, "verify_lock", return_value=self.lock
            ):
                yield path

    def test_im_evidence_requires_verified_passing_payload_and_binding(self):
        for path in self._evidence_path({"status": "pass", "checks": []}):
            evidence = production_acceptance._validate_im_evidence(path)
        self.assertEqual(evidence["evidence_type"], "online-im")

    def test_im_evidence_rejects_non_passing_payload(self):
        for path in self._evidence_path({"status": "fail", "checks": []}):
            with self.assertRaisesRegex(ValueError, "payload status"):
                production_acceptance._validate_im_evidence(path)

    def test_performance_probe_is_not_run_without_live_endpoint(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PRODUCTION_ACCEPTANCE_PERFORMANCE_BASE_URL", None)
            result = production_acceptance._performance_probe()
        self.assertEqual(result["status"], "not_run")

    def test_performance_probe_passes_configured_workload_to_gate(self):
        with patch.dict(
            os.environ,
            {
                "PRODUCTION_ACCEPTANCE_PERFORMANCE_BASE_URL": "http://127.0.0.1:8000",
                "PRODUCTION_ACCEPTANCE_PERFORMANCE_REQUESTS": "25",
                "PRODUCTION_ACCEPTANCE_PERFORMANCE_CONCURRENCY": "5",
                "PRODUCTION_ACCEPTANCE_PERFORMANCE_BASELINE": "data/baseline.json",
            },
            clear=False,
        ), patch.object(
            production_acceptance,
            "_run",
            return_value={"status": "pass", "returncode": 0},
        ) as run:
            result = production_acceptance._performance_probe()
        command = run.call_args.args[0]
        self.assertIn("--requests", command)
        self.assertEqual(command[command.index("--requests") + 1], "25")
        self.assertIn("--baseline", command)
        self.assertEqual(result["status"], "pass")


if __name__ == "__main__":
    unittest.main()
