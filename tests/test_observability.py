import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from trpc_service.storage.base import AuditRecord
from trpc_service.telemetry.tracing import TraceRecorder
from trpc_service.tenant.models import TenantContext


class ObservabilityTests(unittest.TestCase):
    def test_audit_record_redacts_nested_provider_secrets(self):
        record = AuditRecord(
            audit_id="audit-1",
            tenant_id="tenant-1",
            decision="error",
            trace_id="trace-1",
            error_type="provider token=top-secret",
            metadata={
                "safe": "value",
                "authorization": "Bearer top-secret",
                "nested": {"api_key": "top-secret", "count": 1},
            },
        )
        encoded = json.dumps(record.metadata, ensure_ascii=False)
        self.assertIn("value", encoded)
        self.assertNotIn("top-secret", encoded)
        self.assertNotIn("top-secret", record.error_type or "")

    def test_trace_hashes_user_and_session_and_retains_a_bounded_window(self):
        with patch.dict(os.environ, {"TRACE_MAX_RETAINED_SPANS": "2"}, clear=False):
            recorder = TraceRecorder()
            context = TenantContext("tenant", "app", 1, "trace", "private-session", "web", "private-user")
            for index in range(3):
                with recorder.span(f"operation-{index}", context):
                    pass
        self.assertEqual(len(recorder.spans), 2)
        attributes = recorder.spans[-1].attributes
        self.assertNotEqual(attributes["session.id"], "private-session")
        self.assertNotEqual(attributes["user.id"], "private-user")
        self.assertTrue(attributes["session.id"].startswith("sha256:"))
        self.assertTrue(attributes["user.id"].startswith("sha256:"))

    def test_http_probe_and_tenant_audit_contract(self):
        try:
            from fastapi.testclient import TestClient
        except (ImportError, RuntimeError):
            self.skipTest("FastAPI TestClient dependency is unavailable")
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "TENANT_DB_PATH": str(Path(directory) / "tenant.sqlite3"),
                "ADMIN_API_KEY": "observability-admin",
            },
            clear=False,
        ):
            from trpc_service.web.app import create_app

            application = create_app()
            tenant = application.state.gateway.tenants.get_tenant("tenant_demo")
            storage = application.state.gateway.storage_manager.get(tenant)
            storage.audit.append(
                AuditRecord(
                    audit_id="audit-observability",
                    tenant_id="tenant_demo",
                    decision="error",
                    trace_id="trace-observability",
                    metadata={"password": "do-not-return", "safe": "shown"},
                )
            )
            with TestClient(application) as client:
                self.assertEqual(client.get("/livez").json(), {"status": "ok"})
                ready = client.get("/readyz")
                self.assertEqual(ready.status_code, 200)
                self.assertEqual(
                    client.get("/admin/v1/tenants/tenant_demo/audit").status_code,
                    401,
                )
                response = client.get(
                    "/admin/v1/tenants/tenant_demo/audit?limit=1&decision=error",
                    headers={"X-Admin-API-Key": "observability-admin"},
                )
                self.assertEqual(response.status_code, 200)
                body = response.json()
                self.assertEqual(body["count"], 1)
                self.assertEqual(body["items"][0]["metadata"]["safe"], "shown")
                self.assertNotIn("do-not-return", json.dumps(body))
                self.assertEqual(
                    client.get(
                        "/admin/v1/tenants/tenant_demo/audit?limit=501",
                        headers={"X-Admin-API-Key": "observability-admin"},
                    ).status_code,
                    400,
                )


if __name__ == "__main__":
    unittest.main()
