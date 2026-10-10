from __future__ import annotations

import hashlib

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from trpc_service.runtime import BindingMismatch, UnknownBinding
from trpc_service.tenant.auth import Principal, Role
from trpc_service.tenant.control import ControlVersionConflict, IdempotencyConflict
from trpc_service.web.admin import BindingRequest, create_admin_router
from trpc_service.web.app import create_base_app
from trpc_service.web.health import create_health_router


class Authorizer:
    def __init__(self, principal=None) -> None:
        self.principal = principal or Principal(
            subject="admin",
            roles=frozenset(Role),
            tenant_ids=frozenset({"tenant-1"}),
        )

    async def authenticate(self, token):
        return self.principal


class AdminRepository:
    def __init__(self) -> None:
        self.calls = []
        self.tenant = {"tenant_id": "tenant-1", "control_version": 1}
        self.error = None

    async def _result(self, operation, kwargs, result):
        self.calls.append((operation, kwargs))
        if self.error:
            raise self.error
        return result

    async def create_tenant(self, **kwargs):
        return await self._result("create", kwargs, self.tenant)

    async def get_tenant(self, tenant_id):
        return await self._result("get", {"tenant_id": tenant_id}, self.tenant)

    async def put_binding(self, **kwargs):
        return await self._result("binding", kwargs, {"tenant_control_version": 2})

    async def create_config_revision(self, **kwargs):
        return await self._result("config", kwargs, {"tenant_control_version": 3})

    async def activate_config(self, **kwargs):
        return await self._result("activate", kwargs, {"tenant_control_version": 4})

    async def audit_page(self, tenant_id, **kwargs):
        return await self._result("audit", kwargs, {"entries": [], "next_cursor": None})

    async def dead_letters(self, tenant_id, **kwargs):
        return await self._result("dead", kwargs, [])

    async def wecom_acceptance_snapshot(self, tenant_id, binding_id, **kwargs):
        return await self._result(
            "wecom_acceptance",
            {"tenant_id": tenant_id, "binding_id": binding_id, **kwargs},
            {
                "state": {
                    "owner_hash": "a" * 64,
                    "epoch": 1,
                    "phase": "authenticated",
                },
                "events": [],
            },
        )

    async def im_acceptance_outbound_evidence(self, tenant_id, binding_id, **kwargs):
        run_id = kwargs["run_id"]
        outbound_id = kwargs["outbound_id"]
        return await self._result(
            "im_acceptance_evidence",
            {"tenant_id": tenant_id, "binding_id": binding_id, **kwargs},
            {
                "schema_version": 1,
                "tenant_id": tenant_id,
                "binding_id": binding_id,
                "requested_run_id_sha256": hashlib.sha256(run_id.encode()).hexdigest(),
                "run_correlation": {"availability": "unavailable"},
                "outbound": {
                    "availability": "available",
                    "outbound_id_sha256": hashlib.sha256(str(outbound_id).encode()).hexdigest(),
                    "delivery_status": "delivered",
                    "provider_message_id_sha256": "a" * 64,
                    "attempt_count": 1,
                    "attempts_truncated": False,
                    "attempts": [
                        {
                            "attempt_number": 1,
                            "status": "delivered",
                            "provider_code": "0",
                        }
                    ],
                    "pending_count": 0,
                    "dlq_count": 0,
                },
                "artifact": {"availability": "unavailable"},
            },
        )

    async def im_acceptance_event_evidence(self, tenant_id, binding_id, **kwargs):
        return await self._result(
            "im_acceptance_event_evidence",
            {"tenant_id": tenant_id, "binding_id": binding_id, **kwargs},
            {
                "schema_version": 1,
                "tenant_id": tenant_id,
                "binding_id": binding_id,
                "channel": kwargs["channel"].value,
                "requested_run_id_sha256": "b" * 64,
                "run_binding_sha256": "c" * 64,
                "provider_event_hash": kwargs["provider_event_hash"],
                "correlation": {"availability": "available"},
                "outbounds": {"count": 1, "truncated": False, "items": []},
                "artifact": {"availability": "not_found", "count": 0, "items": []},
            },
        )

    async def register_im_acceptance_run(self, tenant_id, binding_id, **kwargs):
        return await self._result(
            "register_im_acceptance_run",
            {"tenant_id": tenant_id, "binding_id": binding_id, **kwargs},
            {
                "schema_version": 1,
                "tenant_id": tenant_id,
                "binding_id": binding_id,
                "channel": kwargs["channel"].value,
                "run_id_sha256": "d" * 64,
                "run_binding_sha256": "e" * 64,
                "created_at": "2026-01-01T00:00:00+00:00",
                "expires_at": "2026-01-01T00:05:00+00:00",
            },
        )

    async def replay_outbound(self, **kwargs):
        return await self._result("replay", kwargs, {"tenant_control_version": 5})


def admin_client(repo=None, authorizer=None) -> TestClient:
    app = create_base_app(title="admin")
    app.include_router(create_admin_router(repo or AdminRepository(), authorizer or Authorizer()))
    return TestClient(app, raise_server_exceptions=False)


def headers(etag='"1"'):
    return {
        "Authorization": "Bearer token",
        "Idempotency-Key": "idempotency-123",
        "If-Match": etag,
    }


def test_binding_request_normalisation_and_validation_branches() -> None:
    with pytest.raises(ValidationError):
        BindingRequest.model_validate(object())

    wecom = BindingRequest.model_validate(
        {"app_id": "app", "channel": "wecom_ai_bot", "account_id": "bot"}
    )
    assert wecom.account_id == "bot"
    with pytest.raises(ValidationError, match="account_id is required"):
        BindingRequest.model_validate({"app_id": "app", "channel": "wecom_ai_bot"})

    feishu = BindingRequest.model_validate(
        {
            "app_id": "app",
            "channel": "feishu",
            "account_id": "cli_app",
            "secret_refs": {
                "app_secret": "env://FEISHU_APP_SECRET",
                "verification_token": "env://FEISHU_VERIFICATION_TOKEN",
                "encrypt_key": "env://FEISHU_ENCRYPT_KEY",
            },
        }
    )
    assert feishu.account_id == "cli_app"
    with pytest.raises(ValidationError, match="Feishu secret references"):
        BindingRequest.model_validate(
            {"app_id": "app", "channel": "feishu", "account_id": "cli_app"}
        )


def test_admin_routes_etags_idempotency_and_rbac() -> None:
    repo = AdminRepository()
    client = admin_client(repo)
    response = client.post(
        "/v1/tenants",
        json={"tenant_id": "tenant-1", "display_name": "Tenant"},
        headers=headers(),
    )
    assert response.status_code == 201 and response.headers["etag"] == '"1"'
    assert client.get("/v1/tenants/tenant-1", headers=headers()).status_code == 200

    response = client.put(
        "/v1/tenants/tenant-1/channel-bindings/binding-1",
        json={
            "app_id": "app",
            "channel": "wecom_ai_bot",
            "account_id": "bot-account",
            "secret_refs": {"bot_secret": {"uri": "env://WECOM_BOT_SECRET"}},
        },
        headers=headers(),
    )
    assert response.status_code == 200 and response.headers["etag"] == '"2"'
    assert repo.calls[-1][1]["expected_version"] == 1

    assert (
        client.post(
            "/v1/tenants/tenant-1/config-revisions",
            json={"app_id": "app", "config": {"model": "fake"}},
            headers=headers('W/"2"'),
        ).status_code
        == 201
    )
    assert (
        client.post(
            "/v1/tenants/tenant-1/config-revisions/2:activate",
            json={"app_id": "app", "percentage": 25},
            headers=headers(),
        ).status_code
        == 200
    )
    assert (
        client.post(
            "/v1/tenants/tenant-1/config-revisions/1:rollback",
            json={"app_id": "app"},
            headers=headers(),
        ).status_code
        == 200
    )
    assert repo.calls[-1][1]["percentage"] == 100
    assert client.get("/v1/tenants/tenant-1/audit", headers=headers()).status_code == 200
    assert client.get("/v1/tenants/tenant-1/dead-letters", headers=headers()).status_code == 200
    assert (
        client.post(
            "/v1/tenants/tenant-1/outbound/outbound-1:replay",
            json={"confirm_ambiguous": True},
            headers=headers(),
        ).status_code
        == 202
    )

    assert client.get("/v1/tenants/tenant-1").status_code == 401
    forbidden = Authorizer(
        Principal(subject="other", roles=frozenset({Role.TENANT_ADMIN}), tenant_ids=frozenset())
    )
    assert (
        admin_client(authorizer=forbidden)
        .get("/v1/tenants/tenant-1", headers=headers())
        .status_code
        == 403
    )
    assert (
        client.put(
            "/v1/tenants/tenant-1/channel-bindings/b",
            json={
                "app_id": "app",
                "channel": "wecom_ai_bot",
                "account_id": "a",
            },
            headers=headers("invalid"),
        ).status_code
        == 400
    )
    assert (
        client.put(
            "/v1/tenants/tenant-1/channel-bindings/b",
            json={
                "app_id": "app",
                "channel": "wecom_ai_bot",
                "account_id": "a",
            },
            headers=headers("0"),
        ).status_code
        == 400
    )


def test_admin_not_found_and_safe_repository_errors() -> None:
    repo = AdminRepository()
    repo.tenant = None
    assert admin_client(repo).get("/v1/tenants/tenant-1", headers=headers()).status_code == 404
    repo.tenant = {"tenant_id": "tenant-1", "control_version": 1}
    repo.error = IdempotencyConflict()
    assert admin_client(repo).post(
        "/v1/tenants",
        json={"tenant_id": "tenant-1", "display_name": "Tenant"},
        headers=headers(),
    ).json() == {"error": "idempotency_conflict"}
    repo.error = ControlVersionConflict()
    assert admin_client(repo).get("/v1/tenants/tenant-1", headers=headers()).status_code == 412


def test_wecom_acceptance_snapshot_route_is_read_only_scoped_and_not_cached() -> None:
    repo = AdminRepository()
    auditor = Authorizer(
        Principal(
            subject="auditor",
            roles=frozenset({Role.AUDITOR}),
            tenant_ids=frozenset({"tenant-1"}),
        )
    )
    response = admin_client(repo, auditor).get(
        "/v1/tenants/tenant-1/bindings/binding-1/im-acceptance/wecom?limit=17",
        headers=headers(),
    )
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert repo.calls[-1] == (
        "wecom_acceptance",
        {"tenant_id": "tenant-1", "binding_id": "binding-1", "limit": 17},
    )
    assert "secret" not in response.text

    operator = Authorizer(
        Principal(
            subject="operator",
            roles=frozenset({Role.TENANT_ADMIN}),
            tenant_ids=frozenset({"tenant-1"}),
        )
    )
    assert (
        admin_client(AdminRepository(), operator)
        .get(
            "/v1/tenants/tenant-1/bindings/binding-1/im-acceptance/wecom",
            headers=headers(),
        )
        .status_code
        == 200
    )

    outside = Authorizer(
        Principal(
            subject="outside",
            roles=frozenset({Role.AUDITOR}),
            tenant_ids=frozenset({"tenant-2"}),
        )
    )
    assert (
        admin_client(AdminRepository(), outside)
        .get(
            "/v1/tenants/tenant-1/bindings/binding-1/im-acceptance/wecom",
            headers=headers(),
        )
        .status_code
        == 403
    )
    assert (
        admin_client()
        .get(
            "/v1/tenants/tenant-1/bindings/binding-1/im-acceptance/wecom?limit=201",
            headers=headers(),
        )
        .status_code
        == 422
    )


def test_wecom_acceptance_snapshot_unknown_binding_is_generic_not_found() -> None:
    repo = AdminRepository()

    async def missing(*_args, **_kwargs):
        return None

    repo.wecom_acceptance_snapshot = missing  # type: ignore[method-assign]
    response = admin_client(repo).get(
        "/v1/tenants/tenant-1/bindings/missing/im-acceptance/wecom",
        headers=headers(),
    )
    assert response.status_code == 404
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["detail"] == "resource not found"


def test_im_acceptance_evidence_route_is_scoped_hash_only_and_not_cached() -> None:
    repo = AdminRepository()
    auditor = Authorizer(
        Principal(
            subject="auditor",
            roles=frozenset({Role.AUDITOR}),
            tenant_ids=frozenset({"tenant-1"}),
        )
    )
    outbound_id = "11111111-1111-1111-1111-111111111111"
    response = admin_client(repo, auditor).get(
        "/v1/tenants/tenant-1/bindings/binding-1/im-acceptance/evidence",
        params={"run_id": "im-run-123", "outbound_id": outbound_id},
        headers=headers(),
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert repo.calls[-1] == (
        "im_acceptance_evidence",
        {
            "tenant_id": "tenant-1",
            "binding_id": "binding-1",
            "run_id": "im-run-123",
            "outbound_id": repo.calls[-1][1]["outbound_id"],
        },
    )
    assert str(repo.calls[-1][1]["outbound_id"]) == outbound_id
    rendered = response.text
    assert "im-run-123" not in rendered
    assert outbound_id not in rendered
    assert "raw-provider" not in rendered
    assert "secret" not in rendered
    assert response.json()["outbound"]["provider_message_id_sha256"] == "a" * 64

    outside = Authorizer(
        Principal(
            subject="outside",
            roles=frozenset({Role.AUDITOR}),
            tenant_ids=frozenset({"tenant-2"}),
        )
    )
    assert (
        admin_client(AdminRepository(), outside)
        .get(
            "/v1/tenants/tenant-1/bindings/binding-1/im-acceptance/evidence",
            params={"run_id": "im-run-123", "outbound_id": outbound_id},
            headers=headers(),
        )
        .status_code
        == 403
    )
    assert (
        admin_client()
        .get(
            "/v1/tenants/tenant-1/bindings/binding-1/im-acceptance/evidence",
            params={"run_id": "contains space", "outbound_id": outbound_id},
            headers=headers(),
        )
        .status_code
        == 422
    )


def test_im_acceptance_evidence_unknown_binding_is_generic_not_found() -> None:
    repo = AdminRepository()

    async def missing(*_args, **_kwargs):
        return None

    repo.im_acceptance_outbound_evidence = missing  # type: ignore[method-assign]
    response = admin_client(repo).get(
        "/v1/tenants/tenant-1/bindings/missing/im-acceptance/evidence",
        params={
            "run_id": "im-run-123",
            "outbound_id": "11111111-1111-1111-1111-111111111111",
        },
        headers=headers(),
    )
    assert response.status_code == 404
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["detail"] == "resource not found"


def test_im_acceptance_event_evidence_is_scoped_content_free_and_no_store() -> None:
    repo = AdminRepository()
    event_hash = "a" * 64
    response = admin_client(repo).post(
        "/v1/tenants/tenant-1/bindings/binding-1/im-acceptance/event-evidence",
        json={
            "channel": "feishu",
            "run_id": "im-run-456",
            "run_nonce": "acceptance-nonce-123456",
            "provider_event_hash": event_hash,
        },
        headers=headers(),
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert repo.calls[-1] == (
        "im_acceptance_event_evidence",
        {
            "tenant_id": "tenant-1",
            "binding_id": "binding-1",
            "channel": repo.calls[-1][1]["channel"],
            "run_id": "im-run-456",
            "run_nonce": "acceptance-nonce-123456",
            "provider_event_hash": event_hash,
        },
    )
    assert repo.calls[-1][1]["channel"].value == "feishu"
    assert "im-run-456" not in response.text
    assert "acceptance-nonce-123456" not in response.text
    assert response.json()["provider_event_hash"] == event_hash

    invalid = admin_client().post(
        "/v1/tenants/tenant-1/bindings/binding-1/im-acceptance/event-evidence",
        json={
            "channel": "feishu",
            "run_id": "contains space",
            "run_nonce": "short",
            "provider_event_hash": "A" * 64,
        },
        headers=headers(),
    )
    assert invalid.status_code == 422

    auditor = Authorizer(
        Principal(
            subject="auditor",
            roles=frozenset({Role.AUDITOR}),
            tenant_ids=frozenset({"tenant-1"}),
        )
    )
    assert (
        admin_client(AdminRepository(), auditor)
        .post(
            "/v1/tenants/tenant-1/bindings/binding-1/im-acceptance/event-evidence",
            json={
                "channel": "feishu",
                "run_id": "im-run-456",
                "run_nonce": "acceptance-nonce-123456",
                "provider_event_hash": event_hash,
            },
            headers=headers(),
        )
        .status_code
        == 403
    )


def test_register_im_acceptance_run_is_tenant_admin_scoped_and_content_free() -> None:
    repo = AdminRepository()
    response = admin_client(repo).post(
        "/v1/tenants/tenant-1/bindings/binding-1/im-acceptance/runs",
        json={
            "channel": "feishu",
            "run_id": "raw-run-id",
            "run_nonce": "acceptance-nonce-123456",
            "expires_in_seconds": 300,
        },
        headers=headers(),
    )

    assert response.status_code == 201
    assert response.headers["cache-control"] == "no-store"
    assert repo.calls[-1][0] == "register_im_acceptance_run"
    assert repo.calls[-1][1]["expires_in_seconds"] == 300
    assert "raw-run-id" not in response.text
    assert "acceptance-nonce-123456" not in response.text
    assert response.json()["run_id_sha256"] == "d" * 64
    assert response.json()["run_binding_sha256"] == "e" * 64

    auditor = Authorizer(
        Principal(
            subject="auditor",
            roles=frozenset({Role.AUDITOR}),
            tenant_ids=frozenset({"tenant-1"}),
        )
    )
    assert (
        admin_client(AdminRepository(), auditor)
        .post(
            "/v1/tenants/tenant-1/bindings/binding-1/im-acceptance/runs",
            json={
                "channel": "feishu",
                "run_id": "run",
                "run_nonce": "acceptance-nonce-123456",
                "expires_in_seconds": 300,
            },
            headers=headers(),
        )
        .status_code
        == 403
    )

    expired_or_duplicate = AdminRepository()

    async def missing(*_args, **_kwargs):
        return None

    expired_or_duplicate.register_im_acceptance_run = missing  # type: ignore[method-assign]
    assert (
        admin_client(expired_or_duplicate)
        .post(
            "/v1/tenants/tenant-1/bindings/binding-1/im-acceptance/runs",
            json={
                "channel": "feishu",
                "run_id": "used-run",
                "run_nonce": "acceptance-nonce-123456",
                "expires_in_seconds": 300,
            },
            headers=headers(),
        )
        .status_code
        == 409
    )


def test_im_acceptance_event_evidence_unknown_binding_is_generic_not_found() -> None:
    repo = AdminRepository()

    async def missing(*_args, **_kwargs):
        return None

    repo.im_acceptance_event_evidence = missing  # type: ignore[method-assign]
    response = admin_client(repo).post(
        "/v1/tenants/tenant-1/bindings/missing/im-acceptance/event-evidence",
        json={
            "channel": "wecom_ai_bot",
            "run_id": "im-run-456",
            "run_nonce": "acceptance-nonce-123456",
            "provider_event_hash": "c" * 64,
        },
        headers=headers(),
    )
    assert response.status_code == 404
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["detail"] == "resource not found"


def test_health_metrics_and_all_safe_error_shapes() -> None:
    app = create_base_app(title="base")

    async def unhealthy():
        return False

    app.include_router(create_health_router(unhealthy), prefix="/checked")

    @app.get("/unknown")
    async def unknown():
        raise UnknownBinding()

    @app.get("/mismatch")
    async def mismatch():
        raise BindingMismatch()

    client = TestClient(app, raise_server_exceptions=False)
    assert client.get("/health/live").json() == {"status": "ok"}
    assert client.get("/health/ready").status_code == 200
    assert client.get("/checked/health/ready").status_code == 503
    assert "text/plain" in client.get("/metrics").headers["content-type"]
    assert client.get("/unknown").json() == {"error": "not_found"}
    assert client.get("/mismatch").json() == {"error": "invalid_callback"}
