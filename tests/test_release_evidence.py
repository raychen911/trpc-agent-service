import base64
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from scripts.candidate_lock import create_lock, verify_lock
from scripts.evidence_lineage import (
    binding_matches,
    canonical_json,
    make_evidence,
    release_binding,
    sha256_bytes,
    verify_evidence,
    write_json,
)
from scripts.release_context import create_context, verify_context
from scripts.release_evidence import create_bundle, verify_bundle
from scripts.release_gate import _evidence_gate
from scripts.release_rehearsal import create_rehearsal
from scripts.supply_chain_gate import (
    _dependency_check,
    _sarif_check,
    _sbom_check,
    evaluate,
)


class ReleaseEvidenceTests(unittest.TestCase):
    def test_source_bound_context_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            context_path = Path(directory) / "release-context.json"
            context = create_context(release_id="test-release", allow_dirty=True)
            write_json(context_path, context)

            verified = verify_context(context_path)

        self.assertEqual(verified["release_id"], "test-release")
        self.assertEqual(verified["context_sha256"], context["context_sha256"])

    def test_context_rejects_source_or_hash_tampering(self):
        with tempfile.TemporaryDirectory() as directory:
            context_path = Path(directory) / "release-context.json"
            context = create_context(release_id="test-release", allow_dirty=True)
            context["source_fingerprint"]["value"] = "0" * 64
            write_json(context_path, context)

            with self.assertRaisesRegex(ValueError, "hash does not match"):
                verify_context(context_path)

    def test_signed_context_round_trip_requires_the_trusted_key(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private_key = Ed25519PrivateKey.generate()
            private_path = root / "release-signing-key.pem"
            private_path.write_bytes(
                private_key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                )
            )
            trust_path = root / "release-trust-key.json"
            write_json(
                trust_path,
                {
                    "schema_version": 1,
                    "algorithm": "ed25519",
                    "key_id": "ci-release",
                    "public_key": base64.b64encode(
                        private_key.public_key().public_bytes(
                            serialization.Encoding.Raw,
                            serialization.PublicFormat.Raw,
                        )
                    ).decode("ascii"),
                },
            )
            context_path = root / "release-context.json"
            context = create_context(
                release_id="signed-release",
                allow_dirty=True,
                signing_key_file=private_path,
                key_id="ci-release",
            )
            write_json(context_path, context)

            verified = verify_context(
                context_path,
                trust_key_file=trust_path,
                require_signature=True,
            )

        self.assertEqual(verified["signature_attestation"]["algorithm"], "ed25519")
        self.assertEqual(verified["signature_attestation"]["key_id"], "ci-release")

    def test_signed_context_rejects_missing_or_invalid_trust(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private_key = Ed25519PrivateKey.generate()
            private_path = root / "release-signing-key.pem"
            private_path.write_bytes(
                private_key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                )
            )
            context_path = root / "release-context.json"
            context = create_context(
                release_id="signed-release",
                allow_dirty=True,
                signing_key_file=private_path,
                key_id="ci-release",
            )
            write_json(context_path, context)

            with self.assertRaisesRegex(ValueError, "trust key is required"):
                verify_context(context_path, require_signature=True)

            context["signature_attestation"]["signature"] = base64.b64encode(b"0" * 64).decode("ascii")
            write_json(context_path, context)
            trust_path = root / "release-trust-key.json"
            write_json(
                trust_path,
                {
                    "schema_version": 1,
                    "algorithm": "ed25519",
                    "key_id": "ci-release",
                    "public_key": base64.b64encode(
                        private_key.public_key().public_bytes(
                            serialization.Encoding.Raw,
                            serialization.PublicFormat.Raw,
                        )
                    ).decode("ascii"),
                },
            )
            with self.assertRaisesRegex(ValueError, "signature verification failed"):
                verify_context(context_path, trust_key_file=trust_path, require_signature=True)

    def test_trust_key_document_rejects_wrong_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            public_path = root / "release-public-key.pem"
            public_path.write_bytes(
                Ed25519PrivateKey.generate()
                .public_key()
                .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
            )
            trust_path = root / "release-trust-key.json"
            from scripts.release_context import create_trust_key

            document = create_trust_key(public_path, "ci-release", trust_path)
            document["schema_version"] = 2
            write_json(trust_path, document)
            context = create_context(release_id="unsigned-release", allow_dirty=True)
            context_path = root / "release-context.json"
            write_json(context_path, context)

            with self.assertRaisesRegex(ValueError, "signed release context is required"):
                verify_context(context_path, trust_key_file=trust_path, require_signature=True)

    def test_candidate_lock_binds_two_distinct_immutable_images(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context_path = root / "release-context.json"
            lock_path = root / "candidate-lock.json"
            context = create_context(release_id="test-release", allow_dirty=True)
            write_json(context_path, context)

            with patch("scripts.candidate_lock.verify_context", return_value=context):
                lock = create_lock(
                    context_path,
                    "registry.example/trpc-agent-service@sha256:" + "a" * 64,
                    "registry.example/trpc-agent-service@sha256:" + "b" * 64,
                    lock_path,
                )

            with patch("scripts.candidate_lock.verify_context", return_value=context):
                verified = verify_lock(context_path, lock_path)

        self.assertEqual(verified["lock_sha256"], lock["lock_sha256"])
        self.assertEqual(verified["images"]["initial"]["digest"], "sha256:" + "a" * 64)

    def test_candidate_lock_rejects_tag_and_same_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context_path = root / "release-context.json"
            context = create_context(release_id="test-release", allow_dirty=True)
            write_json(context_path, context)

            with patch("scripts.candidate_lock.verify_context", return_value=context):
                with self.assertRaisesRegex(ValueError, "immutable"):
                    create_lock(
                        context_path,
                        "registry.example/trpc-agent-service:latest",
                        "registry.example/trpc-agent-service@sha256:" + "b" * 64,
                        root / "candidate-lock.json",
                    )
                with self.assertRaisesRegex(ValueError, "must differ"):
                    create_lock(
                        context_path,
                        "registry.example/trpc-agent-service@sha256:" + "a" * 64,
                        "registry.example/trpc-agent-service@sha256:" + "a" * 64,
                        root / "candidate-lock.json",
                    )

    def test_evidence_hash_chain_detects_payload_changes(self):
        binding = {
            "release_id": "test-release",
            "context_sha256": "c" * 64,
            "source_fingerprint": "f" * 64,
        }
        first = make_evidence("unit", "test", {"status": "pass"}, binding)
        second = make_evidence(
            "integration",
            "test",
            {"status": "pass", "case": "one"},
            binding,
            previous_evidence_sha256=first["evidence_sha256"],
        )

        self.assertEqual(verify_evidence(first), (True, "ok"))
        self.assertEqual(verify_evidence(second), (True, "ok"))
        second["payload"]["status"] = "fail"
        self.assertEqual(verify_evidence(second)[0], False)

    def test_binding_matching_is_explicit(self):
        actual = {"release_id": "r1", "source_fingerprint": "f1"}
        self.assertEqual(binding_matches(actual, actual), (True, "ok"))
        self.assertEqual(binding_matches(actual, {"release_id": "r2"})[0], False)

    def test_release_bundle_round_trip_detects_artifact_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context_path = root / "release-context.json"
            lock_path = root / "candidate-lock.json"
            bundle_path = root / "release-evidence.json"
            artifact = root / "result.json"
            context = create_context(release_id="test-release", allow_dirty=True)
            write_json(context_path, context)
            artifact.write_text('{"status":"pass"}\n', encoding="utf-8")

            with patch("scripts.release_context.verify_context", return_value=context):
                create_lock(
                    context_path,
                    "registry.example/trpc-agent-service@sha256:" + "a" * 64,
                    "registry.example/trpc-agent-service@sha256:" + "b" * 64,
                    lock_path,
                )
            lock = json.loads(lock_path.read_text(encoding="utf-8"))
            with patch("scripts.release_evidence.verify_context", return_value=context), patch(
                "scripts.release_evidence.verify_lock", return_value=lock
            ):
                create_bundle(context_path, lock_path, [artifact], bundle_path, root)
                self.assertEqual(verify_bundle(context_path, lock_path, bundle_path, root)["schema_version"], 1)

            artifact.write_text('{"status":"fail"}\n', encoding="utf-8")
            with patch("scripts.release_evidence.verify_context", return_value=context), patch(
                "scripts.release_evidence.verify_lock", return_value=lock
            ), self.assertRaisesRegex(ValueError, "artifact changed"):
                verify_bundle(context_path, lock_path, bundle_path, root)

    def test_supply_chain_artifact_formats_and_failures_are_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sbom = root / "sbom.json"
            sarif = root / "scan.sarif.json"
            audit = root / "audit.json"
            sbom.write_text(json.dumps({"packages": [{"name": "trpc-agent-service"}]}), encoding="utf-8")
            sarif.write_text(
                json.dumps(
                    {
                        "runs": [
                            {
                                "tool": {"driver": {"rules": []}},
                                "results": [],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            audit.write_text(json.dumps({"vulnerabilities": []}), encoding="utf-8")

            self.assertEqual(_sbom_check(sbom)["status"], "pass")
            self.assertEqual(_sarif_check(sarif)["status"], "pass")
            self.assertEqual(_dependency_check(audit, "pass")["status"], "pass")
            self.assertEqual(_dependency_check(audit, "fail")["status"], "fail")

            audit.write_text(json.dumps([{"name": "trpc-agent-service", "vulns": []}]), encoding="utf-8")
            self.assertEqual(_dependency_check(audit, "pass")["status"], "pass")

            sarif.write_text(
                json.dumps(
                    {
                        "runs": [
                            {
                                "tool": {"driver": {"rules": []}},
                                "results": [{"ruleId": "CVE-test", "level": "error"}],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            self.assertEqual(_sarif_check(sarif)["status"], "fail")

    def test_supply_chain_gate_requires_all_external_evidence_in_production(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context_path = root / "release-context.json"
            lock_path = root / "candidate-lock.json"
            context = create_context(release_id="test-release", allow_dirty=True)
            write_json(context_path, context)
            with patch("scripts.candidate_lock.verify_context", return_value=context):
                create_lock(
                    context_path,
                    "registry.example/trpc-agent-service@sha256:" + "a" * 64,
                    "registry.example/trpc-agent-service@sha256:" + "b" * 64,
                    lock_path,
                )
            with patch("scripts.supply_chain_gate.verify_context", return_value=context), patch(
                "scripts.supply_chain_gate.verify_lock",
                return_value=json.loads(lock_path.read_text(encoding="utf-8")),
            ), patch(
                "scripts.supply_chain_gate._docker_image_check",
                return_value={"status": "pass", "image": "candidate"},
            ):
                report = evaluate(
                    context_path=context_path,
                    lock_path=lock_path,
                    require_production=True,
                )

        self.assertEqual(report["status"], "fail")
        self.assertEqual(report["checks"]["sbom"]["status"], "not_run")
        self.assertEqual(report["checks"]["sarif"]["status"], "not_run")
        self.assertEqual(report["checks"]["dependency_audit"]["status"], "not_run")
        self.assertEqual(report["checks"]["provenance"]["status"], "not_run")

    def test_supply_chain_report_status_must_match_check_statuses(self):
        report = {
            "schema_version": 1,
            "gate": "supply-chain",
            "status": "pass",
            "release_binding": {},
            "checks": {"sbom": {"status": "not_run"}},
            "require_production": False,
        }
        report["report_sha256"] = sha256_bytes(
            canonical_json({key: value for key, value in report.items() if key != "report_sha256"})
        )

        from scripts.supply_chain_gate import verify_report

        self.assertEqual(verify_report(report), (False, "supply-chain report status does not match its checks"))

    def test_release_manifest_chains_reports_and_rejects_status_or_file_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context_path = root / "release-context.json"
            lock_path = root / "candidate-lock.json"
            report_path = root / "unit-report.json"
            manifest_path = root / "release-manifest.json"
            context = create_context(release_id="test-release", allow_dirty=True)
            write_json(context_path, context)
            with patch("scripts.candidate_lock.verify_context", return_value=context):
                lock = create_lock(
                    context_path,
                    "registry.example/trpc-agent-service@sha256:" + "a" * 64,
                    "registry.example/trpc-agent-service@sha256:" + "b" * 64,
                    lock_path,
                )
            report = make_evidence(
                "unit",
                "tests",
                {"status": "pass", "test_count": 12},
                {
                    "release_id": context["release_id"],
                    "context_sha256": context["context_sha256"],
                    "source_fingerprint": context["source_fingerprint"]["value"],
                    "candidate_lock_sha256": lock["lock_sha256"],
                    "image_digests": {
                        "initial": lock["images"]["initial"]["digest"],
                        "upgrade": lock["images"]["upgrade"]["digest"],
                    },
                },
            )
            write_json(report_path, report)

            from scripts.release_manifest import create_manifest, verify_manifest

            with patch("scripts.release_manifest.verify_context", return_value=context), patch(
                "scripts.release_manifest.verify_lock", return_value=lock
            ):
                manifest = create_manifest(
                    context_path,
                    lock_path,
                    [report_path],
                    manifest_path,
                    root,
                    require_pass=True,
                )
                verified = verify_manifest(
                    context_path,
                    lock_path,
                    manifest_path,
                    root,
                    require_pass=True,
                )

            self.assertEqual(verified["evidence_sha256"], manifest["evidence_sha256"])
            self.assertEqual(verified["payload"]["reports"][0]["status"], "pass")

            report["payload"]["status"] = "fail"
            write_json(report_path, report)
            with patch("scripts.release_manifest.verify_context", return_value=context), patch(
                "scripts.release_manifest.verify_lock", return_value=lock
            ), self.assertRaisesRegex(ValueError, "payload hash"):
                verify_manifest(context_path, lock_path, manifest_path, root, require_pass=True)

    def test_release_gate_rejects_supply_chain_report_from_another_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context_path = root / "release-context.json"
            lock_path = root / "candidate-lock.json"
            supply_path = root / "supply-chain.json"
            manifest_path = root / "release-manifest.json"
            context = create_context(release_id="test-release", allow_dirty=True)
            write_json(context_path, context)
            with patch("scripts.candidate_lock.verify_context", return_value=context):
                lock = create_lock(
                    context_path,
                    "registry.example/trpc-agent-service@sha256:" + "a" * 64,
                    "registry.example/trpc-agent-service@sha256:" + "b" * 64,
                    lock_path,
                )
            report = {
                "schema_version": 1,
                "gate": "supply-chain",
                "status": "not_run",
                "generated_at": "2026-09-03T00:00:00+00:00",
                "release_binding": release_binding(context, lock),
                "checks": {"image": {"status": "not_run"}},
                "require_production": False,
            }
            report["release_binding"]["candidate_lock_sha256"] = "d" * 64
            report["report_sha256"] = sha256_bytes(
                canonical_json({key: value for key, value in report.items() if key != "report_sha256"})
            )
            write_json(supply_path, report)
            write_json(manifest_path, {"placeholder": True})

            environment = {
                "TRPC_RELEASE_CONTEXT": str(context_path),
                "TRPC_CANDIDATE_LOCK": str(lock_path),
                "TRPC_SUPPLY_CHAIN_REPORT": str(supply_path),
                "TRPC_RELEASE_MANIFEST": str(manifest_path),
            }
            with patch.dict("os.environ", environment, clear=False), patch(
                "scripts.release_gate.verify_context", return_value=context
            ), patch("scripts.release_gate.verify_lock", return_value=lock):
                result = _evidence_gate(require_production=False)

        self.assertEqual(result["status"], "fail")
        self.assertIn("candidate_lock_sha256", result["output"])

    def test_release_gate_rejects_non_production_manifest_in_production_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context_path = root / "release-context.json"
            lock_path = root / "candidate-lock.json"
            supply_path = root / "supply-chain.json"
            manifest_path = root / "release-manifest.json"
            context = create_context(release_id="test-release", allow_dirty=True)
            write_json(context_path, context)
            with patch("scripts.candidate_lock.verify_context", return_value=context):
                lock = create_lock(
                    context_path,
                    "registry.example/trpc-agent-service@sha256:" + "a" * 64,
                    "registry.example/trpc-agent-service@sha256:" + "b" * 64,
                    lock_path,
                )
            binding = release_binding(context, lock)
            supply = {
                "schema_version": 1,
                "gate": "supply-chain",
                "status": "pass",
                "generated_at": "2026-09-03T00:00:00+00:00",
                "release_binding": binding,
                "checks": {"image": {"status": "pass"}},
                "require_production": False,
            }
            supply["report_sha256"] = sha256_bytes(
                canonical_json({key: value for key, value in supply.items() if key != "report_sha256"})
            )
            write_json(supply_path, supply)
            report_path = root / "quality-evidence.json"
            report = make_evidence("quality", "tests", {"status": "pass"}, binding)
            write_json(report_path, report)
            from scripts.release_manifest import create_manifest

            with patch("scripts.release_manifest.verify_context", return_value=context), patch(
                "scripts.release_manifest.verify_lock", return_value=lock
            ):
                create_manifest(
                    context_path,
                    lock_path,
                    [report_path, supply_path],
                    manifest_path,
                    root,
                    require_pass=False,
                )
            environment = {
                "TRPC_RELEASE_CONTEXT": str(context_path),
                "TRPC_CANDIDATE_LOCK": str(lock_path),
                "TRPC_SUPPLY_CHAIN_REPORT": str(supply_path),
                "TRPC_RELEASE_MANIFEST": str(manifest_path),
            }
            with patch.dict("os.environ", environment, clear=False), patch(
                "scripts.release_gate.verify_context", return_value=context
            ), patch("scripts.release_gate.verify_lock", return_value=lock), patch(
                "scripts.release_gate.verify_manifest",
                return_value={"payload": {"require_pass": False, "reports": []}},
            ):
                result = _evidence_gate(require_production=True)

        self.assertEqual(result["status"], "fail")
        self.assertIn("production release manifest", result["output"])

    def test_release_rehearsal_creates_a_signed_not_run_chain(self):
        from scripts.evidence_lineage import ROOT

        runs_root = ROOT / "runs"
        runs_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=runs_root) as directory:
            result = create_rehearsal(Path(directory), root=ROOT, allow_dirty=True)
            self.assertEqual(result["status"], "pass")
            self.assertEqual(result["supply_chain_status"], "not_run")
            context = json.loads(Path(result["context"]).read_text(encoding="utf-8"))
            self.assertEqual(context["signature_attestation"]["key_id"], "rehearsal")
            self.assertTrue(Path(result["manifest"]).is_file())

            environment = {
                "TRPC_RELEASE_CONTEXT": result["context"],
                "TRPC_CANDIDATE_LOCK": result["candidate_lock"],
                "TRPC_RELEASE_TRUST_KEY": result["trust_key"],
                "TRPC_SUPPLY_CHAIN_REPORT": result["supply_chain"],
                "TRPC_RELEASE_MANIFEST": result["manifest"],
            }
            with patch.dict("os.environ", environment, clear=False):
                gate = _evidence_gate(require_production=False)
            self.assertEqual(gate["status"], "not_run")
            self.assertEqual(gate["returncode"], 0)


if __name__ == "__main__":
    unittest.main()
