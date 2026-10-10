#!/usr/bin/env python3
"""Exercise a running Compose control plane and clean up its random tenant."""

from __future__ import annotations

import argparse
import asyncio
import os
import time
from pathlib import Path
from uuid import uuid4

import asyncpg
import httpx

from scripts.report_io import atomic_write_json


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


async def _cleanup(dsn: str, tenant_id: str) -> None:
    connection = await asyncpg.connect(dsn)
    try:
        async with connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            for table in (
                "channel_bindings",
                "config_revisions",
                "agent_apps",
                "admin_idempotency",
                "audit_logs",
                "tenants",
            ):
                await connection.execute(
                    f"DELETE FROM {table} WHERE tenant_id=$1",  # noqa: S608 - fixed allow-list
                    tenant_id,
                )
    finally:
        await connection.close()


async def _run(admin_url: str, gateway_url: str) -> dict[str, object]:
    token = _required("TRPC_E2E_DEVELOPMENT_TOKEN")
    runtime_dsn = _required("TRPC_E2E_POSTGRES_RUNTIME_DSN")
    tenant_id = f"compose-e2e-{uuid4().hex}"
    binding_id = f"binding-{uuid4().hex}"
    started = time.perf_counter()
    checks: dict[str, bool] = {}
    error_type: str | None = None
    created = False
    try:
        headers = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient(timeout=10) as client:
            gateway_health = await client.get(f"{gateway_url.rstrip('/')}/health/ready")
            checks["gateway_ready"] = gateway_health.status_code == 200
            admin_health = await client.get(f"{admin_url.rstrip('/')}/health/ready")
            checks["admin_ready"] = admin_health.status_code == 200

            create = await client.post(
                f"{admin_url.rstrip('/')}/v1/tenants",
                headers={**headers, "Idempotency-Key": f"create-{uuid4().hex}"},
                json={"tenant_id": tenant_id, "display_name": "Compose E2E tenant"},
            )
            create.raise_for_status()
            created = True
            etag = create.headers["etag"]
            checks["tenant_created"] = create.json()["tenant_id"] == tenant_id

            config = await client.post(
                f"{admin_url.rstrip('/')}/v1/tenants/{tenant_id}/config-revisions",
                headers={
                    **headers,
                    "Idempotency-Key": f"config-{uuid4().hex}",
                    "If-Match": etag,
                },
                json={
                    "app_id": "support",
                    "config": {
                        "model": {"provider": "offline", "model": "deterministic"},
                        "storage": {"profile_id": "default"},
                    },
                },
            )
            config.raise_for_status()
            etag = config.headers["etag"]
            checks["config_revision_created"] = config.json()["version"] == 1

            account_id = f"cli_{uuid4().hex}"
            binding = await client.put(
                f"{admin_url.rstrip('/')}/v1/tenants/{tenant_id}/channel-bindings/{binding_id}",
                headers={
                    **headers,
                    "Idempotency-Key": f"binding-{uuid4().hex}",
                    "If-Match": etag,
                },
                json={
                    "app_id": "support",
                    "channel": "feishu",
                    "account_id": account_id,
                    "secret_refs": {
                        "app_secret": {"uri": "file:///run/secrets/feishu_app_secret"},
                        "verification_token": {
                            "uri": "file:///run/secrets/feishu_verification_token"
                        },
                        "encrypt_key": {"uri": "file:///run/secrets/feishu_encrypt_key"},
                    },
                },
            )
            binding.raise_for_status()
            checks["binding_created"] = binding.json()["binding_id"] == binding_id

            tenant = await client.get(
                f"{admin_url.rstrip('/')}/v1/tenants/{tenant_id}", headers=headers
            )
            tenant.raise_for_status()
            checks["etag_advanced"] = tenant.headers["etag"] == '"3"'
            audit = await client.get(
                f"{admin_url.rstrip('/')}/v1/tenants/{tenant_id}/audit", headers=headers
            )
            audit.raise_for_status()
            checks["audit_persisted"] = len(audit.json()["items"]) >= 3

        connection = await asyncpg.connect(runtime_dsn)
        try:
            route = await connection.fetchrow(
                "SELECT tenant_id,app_id FROM resolve_channel_binding($1)", binding_id
            )
            checks["security_definer_route"] = bool(
                route and route["tenant_id"] == tenant_id and route["app_id"] == "support"
            )
        finally:
            await connection.close()
    except BaseException as exc:
        error_type = type(exc).__name__
    finally:
        if created:
            try:
                await _cleanup(runtime_dsn, tenant_id)
                checks["tenant_cleanup"] = True
            except BaseException:
                checks["tenant_cleanup"] = False

    passed = bool(checks) and all(checks.values()) and error_type is None
    return {
        "baseline": {
            "scope": "control_plane",
            "gateway_ready": True,
            "admin_ready": True,
            "tenant_config_binding_audit": True,
            "runtime_binding_resolution": True,
            "cleanup": True,
        },
        "candidate": {
            "scope": "control_plane",
            "checks": checks,
            "duration_seconds": time.perf_counter() - started,
        },
        "case_deltas": {"failed_checks": [name for name, value in checks.items() if not value]},
        "gate": "pass" if passed else "fail",
        "rejection_reasons": [] if passed else [error_type or "Compose E2E check failed"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--admin-url", default="http://127.0.0.1:8081")
    parser.add_argument("--gateway-url", default="http://127.0.0.1:8080")
    parser.add_argument("--output", type=Path, default=Path("runs/multitenant/compose-e2e.json"))
    args = parser.parse_args()
    result = asyncio.run(_run(args.admin_url, args.gateway_url))
    rendered = atomic_write_json(args.output, result).rstrip("\n")
    print(rendered)
    return 0 if result["gate"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
