"""T043 RED: health endpoints with authorization boundary (FR-012/FR-016/FR-031).

``/health/live`` reflects only process progress, ``/health/ready`` reports
role readiness with stable reason codes (503 when unready), and
``/health/status`` is authorized-only (403 + minimal audit when
unauthorized) and returns the path-level platform summary. No response
ever contains DSNs, hosts, secrets or raw tenant values.
"""

from __future__ import annotations

import importlib

import httpx
import pytest

from tests.observability_support import SENSITIVE_SENTINELS, sentinel_values
from tests.support import FIXED_UTC


def _require_phase8_health() -> None:
    try:
        importlib.import_module("trpc_service.observability.health")
    except ModuleNotFoundError:
        pytest.fail("trpc_service.observability.health is not implemented")


def _app(env: dict[str, str]):
    from trpc_service.web.app import create_app

    return create_app(env, now=lambda: FIXED_UTC)


async def _get(env: dict[str, str], path: str, *, headers: dict[str, str] | None = None):
    app = _app(env)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(path, headers=headers or {})


async def test_liveness_is_200_while_process_progresses(runtime_secret_env) -> None:
    _require_phase8_health()
    response = await _get(runtime_secret_env, "/health/live")
    assert response.status_code == 200
    assert response.json()["status"] in {"alive", "ok"}


async def test_ready_returns_200_with_role_and_reason_codes(runtime_secret_env) -> None:
    _require_phase8_health()
    response = await _get(runtime_secret_env, "/health/ready")
    body = response.json()
    assert response.status_code in {200, 503}
    assert body["status"] in {"ready", "unready"}
    # Ready responses carry no reason codes; unready ones carry stable codes.
    if body["status"] == "ready":
        assert not body.get("reason_codes")
    else:
        assert response.status_code == 503
        assert body.get("reason_codes")
        for code in body["reason_codes"]:
            assert code.replace("_", "").isalnum(), code


async def test_status_requires_authorization_and_audits(runtime_secret_env) -> None:
    _require_phase8_health()
    env = dict(runtime_secret_env)
    env["TRPC_OPS_TOKEN"] = "ops-token-phase8"
    # Unauthorized: 403, minimal audit formed, no summary leak.
    response = await _get(env, "/health/status")
    assert response.status_code == 403
    assert response.json()["status"] == "forbidden"
    # Authorized: path-level summary returned.
    response = await _get(env, "/health/status", headers={"x-ops-token": "ops-token-phase8"})
    assert response.status_code == 200
    body = response.json()
    for key in ("state", "available_paths", "unavailable_paths"):
        assert key in body, key


async def test_health_responses_never_leak_sensitive_material(runtime_secret_env) -> None:
    _require_phase8_health()
    env = dict(runtime_secret_env)
    env["TRPC_OPS_TOKEN"] = SENSITIVE_SENTINELS["api_key"]
    paths = ("/health/live", "/health/ready")
    for path in paths:
        response = await _get(env, path)
        blob = response.text.lower()
        for value in sentinel_values():
            assert value.lower() not in blob, (path, value[:24])
        for forbidden in ("postgres://", "redis://", "dsn", "password", "@example"):
            assert forbidden not in blob, (path, forbidden)
    response = await _get(
        env, "/health/status", headers={"x-ops-token": SENSITIVE_SENTINELS["api_key"]}
    )
    blob = response.text.lower()
    for value in sentinel_values():
        assert value.lower() not in blob, value[:24]


async def test_status_audit_is_minimal_and_pseudonymous(runtime_secret_env) -> None:
    _require_phase8_health()
    from trpc_service.web.app import build_runtime

    env = dict(runtime_secret_env)
    env["TRPC_OPS_TOKEN"] = "ops-token-phase8"
    runtime = build_runtime(env, now=lambda: FIXED_UTC)
    try:
        assert await runtime.record_health_status_access() >= 1
        audit_blob = str(runtime.health_access_audit).lower()
        for tenant in ("tenant-alpha", "tenant-beta"):
            assert tenant not in audit_blob
        for value in sentinel_values():
            assert value.lower() not in audit_blob
    finally:
        await runtime.close()
