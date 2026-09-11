"""Real HTTP/SQL/OTLP/Prometheus/Alertmanager acceptance in an isolated schema.

Run with python -m tests.integration.live_operations after starting the protected
database and docker-compose.telemetry-test.yml. This does not run IM acceptance.
"""

import asyncio
import json
import os
from pathlib import Path
import secrets
import socket
import time
import uuid

from fastapi import FastAPI
import httpx
from sqlalchemy import create_engine, select
from sqlalchemy.schema import CreateSchema, DropSchema
import uvicorn

from trpc_service.management import management_router
from trpc_service.persistence import Database
from trpc_service.persistence.models import AuditLogRow, OutboxMessageRow
from trpc_service.reliability import OutboxRepository
from trpc_service.telemetry.runtime import configure, operation, shutdown


async def run():
    url = os.environ["TRPC_DATABASE_URL"]
    admin = create_engine(url, hide_parameters=True)
    if admin.dialect.name != "postgresql":
        raise ValueError("real operations acceptance requires PostgreSQL")
    schema = "ops_acceptance_" + uuid.uuid4().hex
    with admin.begin() as connection:
        connection.execute(CreateSchema(schema))
    database = Database(url, connect_args={"options": "-csearch_path=" + schema})
    database.create_schema()
    token = secrets.token_urlsafe(40)
    previous_token = os.environ.get("TRPC_ADMIN_TOKEN")
    os.environ["TRPC_ADMIN_TOKEN"] = token
    report = {
        "scope": "real_operations_http_sql_otlp_prometheus_alertmanager",
        "passed": False,
        "full_im_acceptance_passed": False,
        "checks": []
    }
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    app = FastAPI()
    app.include_router(management_router(database))
    server = uvicorn.Server(uvicorn.Config(app, access_log=False, log_level="critical", lifespan="off"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    service = "ops-acceptance-" + uuid.uuid4().hex[:12]
    configure(service, database)
    try:
        async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
            deadline = time.monotonic() + 15
            while not server.started:
                if serving.done() or time.monotonic() > deadline:
                    raise RuntimeError("HTTP server startup failed")
                await asyncio.sleep(0.05)
            root = "http://127.0.0.1:" + str(listener.getsockname()[1]) + "/admin/tenants/tenant_ops"
            headers = {"Authorization": "Bearer " + token}
            outbox = OutboxRepository(database)
            with operation("acceptance.dead_letter", attributes={"tenant_id": "tenant_ops"}):
                identity = outbox.enqueue("tenant_ops", "acceptance_message", 0, {"text": "private acceptance body"})
                outbox.claim(limit=1, worker_id="ops-acceptance", lease_seconds=30)
                outbox.dead_letter(identity, "ops-acceptance", "acceptance_delivery_failure")
            response = await client.get(root + "/deliveries", headers=headers)
            response.raise_for_status()
            assert response.json()[0]["status"] == "dead_letter"
            report["checks"].append("real_http_reads_postgres_dead_letter")
            prometheus = os.environ.get("TRPC_TEST_PROMETHEUS_URL", "http://127.0.0.1:59090")
            alertmanager = os.environ.get("TRPC_TEST_ALERTMANAGER_URL", "http://127.0.0.1:59093")
            deadline = time.monotonic() + 160
            last_progress = 0
            metric_seen, alert_seen = False, False
            while time.monotonic() < deadline:
                response = await client.get(
                    prometheus + "/api/v1/query",
                    params={"query": 'trpc_queue{service_name="' + service + '",queue="outbox",measure="failed"}'})
                response.raise_for_status()
                metric_seen = any(float(row["value"][1]) == 1 for row in response.json()["data"]["result"])
                response = await client.get(alertmanager + "/api/v2/alerts")
                response.raise_for_status()
                alert_seen = any(row["labels"].get("alertname") == "TrpcTerminalWork"
                                 and row["labels"].get("service_name") == service for row in response.json())
                if metric_seen and alert_seen:
                    break
                if time.monotonic() - last_progress > 30:
                    print(json.dumps({"metric_seen": metric_seen, "alert_seen": alert_seen}), flush=True)
                    last_progress = time.monotonic()
                await asyncio.sleep(2)
            assert metric_seen and alert_seen, "real metric/alert chain did not complete"
            report["checks"].extend(["postgres_queue_to_otlp_to_prometheus", "prometheus_rule_to_alertmanager"])
            response = await client.post(root + "/deliveries/" + identity + "/retry",
                                         headers=headers,
                                         json={"expected_attempt": 1})
            response.raise_for_status()
            response = await client.get(root + "/audit/export",
                                        headers=headers,
                                        params={"decision": "delivery_requeued"})
            response.raise_for_status()
            evidence = [json.loads(line) for line in response.text.splitlines()]
            assert len(evidence) == 1 and evidence[0]["metadata"]["delivery_id"] == identity
            assert evidence[0]["trace_id"] and evidence[0]["request_id"]
            assert "private acceptance body" not in response.text
            with database.sessions() as session:
                assert session.get(OutboxMessageRow, identity).status == "retry"
                assert session.scalar(select(AuditLogRow.audit_id).where(AuditLogRow.decision == "audit_exported"))
            report["checks"].append("real_http_recovery_and_audit_export_receipt")
            report["passed"] = True
    except Exception as error:
        report["error_type"] = type(error).__name__
        raise
    finally:
        server.should_exit = True
        await serving
        listener.close()
        shutdown()
        database.close()
        with admin.begin() as connection:
            connection.execute(DropSchema(schema, cascade=True))
        admin.dispose()
        if previous_token is None:
            os.environ.pop("TRPC_ADMIN_TOKEN", None)
        else:
            os.environ["TRPC_ADMIN_TOKEN"] = previous_token
        target = Path("reports/real-operations-acceptance-2026-09-08.json")
        target.parent.mkdir(exist_ok=True)
        target.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report), flush=True)


if __name__ == "__main__":
    asyncio.run(run())
