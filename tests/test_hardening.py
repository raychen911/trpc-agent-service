from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from scripts import fault_injection_gate
from scripts.kubernetes_runtime_gate import validate_manifest
from trpc_service.migration_state import MigrationPhase, new_migration
from trpc_service.policy.tenant_filter import PolicyDenied, TenantPolicy
from trpc_service.security.ssrf import SSRFProtectionError, validate_outbound_url
from trpc_service.storage.durable import InboxStatus, InMemoryInboxOutbox
from trpc_service.storage.factory import StorageBundle
from trpc_service.storage.in_memory import InMemoryStorage
from trpc_service.storage.mirror import MirroredStorageBundle
from trpc_service.storage.tool_governance import arguments_hash
from trpc_service.tenant.models import ToolPolicy, default_demo_config
from trpc_service.tenant.repository import InMemoryTenantRepository
from trpc_service.tenant.service import TenantService, TenantValidationError


class HardeningTests(unittest.TestCase):
    def test_production_kubernetes_manifest_passes_static_gate(self):
        report = validate_manifest()
        self.assertEqual(report["status"], "pass")
        self.assertTrue(all(item["ok"] for item in report["checks"]))

    def test_compose_can_render_an_immutable_candidate_image(self):
        import subprocess

        environment = os.environ.copy()
        environment.update(
            {
                "TRPC_IMAGE": "trpc-agent-service@sha256:deadbeef",
                "POSTGRES_PASSWORD": "local-only",
                "POSTGRES_DSN": "postgresql://trpc_agent:local-only@sql:5432/trpc_agent",
                "TENANT_DB_DSN": "postgresql://trpc_agent:local-only@sql:5432/trpc_agent",
            }
        )
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                "deployment/docker-compose.yml",
                "config",
                "--format",
                "json",
            ],
            capture_output=True,
            text=True,
            env=environment,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('"image": "trpc-agent-service@sha256:deadbeef"', result.stdout)

    def test_kubernetes_static_gate_rejects_insecure_container(self):
        manifest = """
apiVersion: v1
kind: Namespace
metadata:
  name: trpc-agent
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gateway
  namespace: trpc-agent
spec:
  template:
    spec:
      containers:
        - name: gateway
          image: example/app:v1
          readinessProbe: {httpGet: {path: /health, port: 8000}}
          livenessProbe: {httpGet: {path: /health, port: 8000}}
"""
        kustomization = """
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
images:
  - name: ghcr.io/xyxhhhhh/trpc-agent-service
    newName: registry.example/app
    newTag: release
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest_path = root / "platform.yaml"
            kustomization_path = root / "kustomization.yaml"
            manifest_path.write_text(manifest, encoding="utf-8")
            kustomization_path.write_text(kustomization, encoding="utf-8")
            report = validate_manifest(manifest_path, kustomization_path)
        self.assertEqual(report["status"], "fail")
        failed = {item["name"] for item in report["checks"] if not item["ok"]}
        self.assertIn("container security contexts", failed)
        self.assertIn("required application deployments", failed)

    def test_toxiproxy_cycle_requires_outage_and_recovery(self):
        with patch.dict(os.environ, {"TOXIPROXY_RECOVERY_TIMEOUT_SECONDS": "1"}, clear=False):
            with patch.object(fault_injection_gate, "_post", return_value={}) as create, \
                    patch.object(fault_injection_gate, "_delete", return_value={}) as remove, \
                    patch.object(
                        fault_injection_gate,
                        "_probe",
                        side_effect=[{"ok": False, "status": None}, {"ok": True, "status": 200}],
                    ):
                result = fault_injection_gate._cycle_proxy(
                    "http://toxiproxy:8474", "redis", "http://redis-probe/health"
                )
        self.assertTrue(result["ok"])
        create.assert_called_once()
        remove.assert_called_once()

    def test_toxiproxy_cycle_rejects_remove_failure(self):
        with (
            patch.object(fault_injection_gate, "_post", return_value={}),
            patch.object(fault_injection_gate, "_delete", side_effect=OSError("remove failed")),
            patch.object(fault_injection_gate, "_probe", return_value={"ok": False, "status": None}),
        ):
            result = fault_injection_gate._cycle_proxy(
                "http://toxiproxy:8474", "redis", "http://redis-probe/health"
            )
        self.assertFalse(result["ok"])
        self.assertIn("remove_error", result)

    def test_toxiproxy_cycle_uses_configured_toxic_attributes(self):
        with patch.dict(
            os.environ,
            {
                "TOXIPROXY_TOXIC_TYPE": "timeout",
                "TOXIPROXY_TOXIC_ATTRIBUTES": '{"timeout": 1}',
            },
            clear=False,
        ), patch.object(fault_injection_gate, "_post", return_value={}) as create, patch.object(
            fault_injection_gate, "_delete", return_value={}
        ), patch.object(
            fault_injection_gate,
            "_probe",
            side_effect=[{"ok": False, "status": None}, {"ok": True, "status": 200}],
        ):
            result = fault_injection_gate._cycle_proxy(
                "http://toxiproxy:8474", "redis", "http://redis-probe/health"
            )
        self.assertTrue(result["ok"])
        self.assertEqual(create.call_args.args[1]["attributes"], {"timeout": 1})

    def test_toxiproxy_gate_classifies_missing_probe_as_not_run(self):
        with patch.dict(
            os.environ,
            {
                "TOXIPROXY_REQUIRED_PROXIES": "redis",
                "TOXIPROXY_PROBE_URLS": "",
                "TOXIPROXY_CONTROL_ONLY": "0",
            },
            clear=False,
        ), patch.object(fault_injection_gate, "_get", return_value={"redis": {}}):
            with patch.object(sys, "argv", ["fault_injection_gate.py", "--api-url", "http://proxy:8474"]):
                output = io.StringIO()
                with redirect_stdout(output):
                    result = fault_injection_gate.main()
            self.assertEqual(result, 2)
            self.assertEqual(json.loads(output.getvalue())["status"], "not_run")

    def test_migration_mirror_replicates_durable_inbox_outbox_and_tool_ledger(self):
        primary = InMemoryStorage()
        secondary = InMemoryStorage()
        storage = MirroredStorageBundle(StorageBundle(primary), StorageBundle(secondary))
        try:
            accepted, created = storage.inbox_outbox.accept_inbox(
                "tenant", "message-1", "session", {"text": "hello"}, "worker"
            )
            self.assertTrue(created)
            self.assertEqual(
                secondary.inbox_outbox._inbox[("tenant", "message-1")].message_id,
                accepted.message_id,
            )
            storage.inbox_outbox.complete_inbox_and_enqueue_outbox(
                "tenant", "message-1", "worker", {"text": "ok"},
                "agent.response", "session", "event-1",
            )
            self.assertIn("event-1", secondary.inbox_outbox._outbox)

            args_hash = arguments_hash({"target": "ops"})
            execution = storage.tool_governance.begin_execution(
                "tenant", "execution-1", "request-1", "session", "send",
                "call-1", args_hash, True, 7,
            )
            self.assertEqual(execution.status, "running")
            mirrored = secondary.tool_governance.get_execution("tenant", "call-1")
            self.assertIsNotNone(mirrored)
            self.assertEqual(mirrored.fencing_token, 7)
            storage.tool_governance.complete_execution(
                "tenant", "call-1", {"sent": True}, fencing_token=7
            )
            self.assertEqual(
                secondary.tool_governance.get_execution("tenant", "call-1").status,
                "succeeded",
            )
        finally:
            storage.close()

    def test_atomic_inbox_completion_creates_one_outbox_record(self):
        store = InMemoryInboxOutbox()
        store.accept_inbox("tenant", "message", "session", {"text": "hi"}, "worker")
        result = store.complete_inbox_and_enqueue_outbox(
            "tenant",
            "message",
            "worker",
            {"text": "ok"},
            "agent.response",
            "session",
            "event-1",
        )
        self.assertEqual(result.event_id, "event-1")
        self.assertEqual(store._inbox[("tenant", "message")].status, InboxStatus.COMPLETED)
        self.assertEqual(len(store._outbox), 1)

    def test_tool_risk_and_budget_are_enforced(self):
        config = default_demo_config()
        config.apps[0].tool_policy = ToolPolicy(
            allowlist=["send"],
            risk_levels={"send": "critical"},
            max_calls_per_request=2,
            max_side_effect_calls_per_request=1,
        )
        policy = TenantPolicy(config)
        self.assertEqual(policy.tool_risk("send"), "critical")
        self.assertTrue(policy.requires_tool_approval("send"))
        policy.check_tool_budget(2, 1)
        with self.assertRaises(PolicyDenied):
            policy.check_tool_budget(3, 1)
        with self.assertRaises(PolicyDenied):
            policy.check_tool_budget(2, 2)

    def test_invalid_tool_governance_configuration_is_rejected(self):
        config = default_demo_config()
        config.apps[0].tool_policy = ToolPolicy(
            risk_levels={"search_knowledge": "unsafe"},
            max_calls_per_request=2,
            max_side_effect_calls_per_request=3,
        )
        with self.assertRaises(TenantValidationError):
            TenantService(InMemoryTenantRepository()).validate(config)

    def test_published_config_snapshot_is_frozen_and_json_stable(self):
        snapshot = default_demo_config().immutable_snapshot()
        self.assertEqual(snapshot.tenant_id, "tenant_demo")
        self.assertEqual(snapshot.payload()["tenant_id"], "tenant_demo")
        with self.assertRaises((TypeError, ValueError)):
            snapshot.tenant_id = "other"

    def test_ssrf_rejects_private_and_local_destinations(self):
        with self.assertRaises(SSRFProtectionError):
            validate_outbound_url("http://127.0.0.1:8080")
        with self.assertRaises(SSRFProtectionError):
            validate_outbound_url("http://localhost:8080")
        self.assertEqual(validate_outbound_url("https://api.example.test"), "https://api.example.test")
        self.assertEqual(
            validate_outbound_url("https://api.telegram.org/bot/test", trusted_hosts={"api.telegram.org"}),
            "https://api.telegram.org/bot/test",
        )

    def test_migration_state_machine_is_resumable_and_serializable(self):
        state = new_migration("m-1", "tenant", "redis", "postgres")
        for phase in (
            MigrationPhase.BACKFILL,
            MigrationPhase.SHADOW_READ,
            MigrationPhase.DUAL_WRITE,
            MigrationPhase.CUTOVER,
            MigrationPhase.VERIFY,
            MigrationPhase.CLEANUP,
        ):
            state.transition(phase, actor="test")
        restored = type(state).from_dict(state.to_dict())
        self.assertEqual(restored.phase, MigrationPhase.CLEANUP)
        self.assertEqual(restored.version, 7)
        with self.assertRaises(ValueError):
            restored.transition(MigrationPhase.PREPARE)


if __name__ == "__main__":
    unittest.main()
