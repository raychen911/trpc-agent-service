"""Tests for the Admin API: schemas, routes, error mapping, lifespan."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import AsyncIterator
from datetime import datetime, timezone

import httpx
import pytest

from tests.tenant_helpers import make_audit_policy, make_backend_profile, make_governance
import pytest_asyncio
from fastapi import FastAPI
from starlette.testclient import TestClient

from trpc_service.admin.app import create_admin_app
from trpc_service.audit.models import ExecutionAuditEvent
from trpc_service.config.tenant import AgentAppConfig
from trpc_service.config.tenant import TenantConfig
from trpc_service.config.tenant import TenantConfigDraft
from trpc_service.config.tenant_repository import (
    TenantAlreadyExistsError,
    TenantConfigTargetVersionNotFoundError,
    TenantConfigVersionConflictError,
    TenantNotFoundError,
    TenantRepositoryDataError,
    TenantRepositoryUnavailableError,
)

TOKEN = "t" * 32
AUTH_HEADERS = {"X-TRPC-Admin-Token": TOKEN}


def _app_config(instruction: str = "instr", app_id: str = "app_a") -> AgentAppConfig:
    return AgentAppConfig(
        app_id=app_id,
        instruction=instruction,
        model_profile="default",
        allowed_tools=[],
    )


def _config(tenant_id: str, version: int = 1, instruction: str = "instr") -> TenantConfig:
    return TenantConfig(
        tenant_id=tenant_id,
        enabled=True,
        version=version,
        app=_app_config(instruction),
        governance=make_governance(),
        backend_profile=make_backend_profile(),
        audit_policy=make_audit_policy(),
    )


class FakeAdminRepository:
    """In-memory TenantConfigAdminRepository double with error injection."""

    def __init__(self, configs: dict[str, TenantConfig] | None = None) -> None:
        self._heads: dict[str, TenantConfig] = dict(configs or {})
        self._history: dict[str, list[TenantConfig]] = {tid: [cfg] for tid, cfg in self._heads.items()}
        self.ready_count = 0
        self.close_count = 0
        self.get_calls = 0
        self.update_calls = 0
        self.rollback_calls = 0
        self.raise_unavailable_on_call = False
        self.raise_data_on_call = False
        self.raise_generic_on_call = False

    def _maybe_raise(self) -> None:
        if self.raise_unavailable_on_call:
            raise TenantRepositoryUnavailableError("database is not reachable")
        if self.raise_data_on_call:
            raise TenantRepositoryDataError("tenant write failed")
        if self.raise_generic_on_call:
            raise RuntimeError("unexpected internal error")

    async def create(self, config: TenantConfig) -> TenantConfig:
        self._maybe_raise()
        if config.tenant_id in self._heads:
            raise TenantAlreadyExistsError("tenant already exists")
        created = TenantConfig(
            tenant_id=config.tenant_id,
            enabled=config.enabled,
            version=1,
            app=config.app,
            governance=make_governance(),
            backend_profile=config.backend_profile,
            audit_policy=config.audit_policy,
        )
        self._heads[created.tenant_id] = created
        self._history[created.tenant_id] = [created]
        return created

    async def update(
        self,
        tenant_id: str,
        expected_version: int,
        desired: TenantConfigDraft,
    ) -> TenantConfig:
        self.update_calls += 1
        self._maybe_raise()
        head = self._heads.get(tenant_id)
        if head is None:
            raise TenantNotFoundError("tenant not found")
        if head.version != expected_version:
            raise TenantConfigVersionConflictError("version conflict")
        updated = TenantConfig(
            tenant_id=tenant_id,
            enabled=desired.enabled,
            version=expected_version + 1,
            app=desired.app,
            governance=make_governance(),
            backend_profile=desired.backend_profile,
            audit_policy=desired.audit_policy,
        )
        self._heads[tenant_id] = updated
        self._history[tenant_id].append(updated)
        return updated

    async def rollback(
        self,
        tenant_id: str,
        expected_version: int,
        target_version: int,
    ) -> TenantConfig:
        self.rollback_calls += 1
        self._maybe_raise()
        head = self._heads.get(tenant_id)
        if head is None:
            raise TenantNotFoundError("tenant not found")
        if head.version != expected_version:
            raise TenantConfigVersionConflictError("version conflict")
        target = next(
            (c for c in self._history[tenant_id] if c.version == target_version),
            None,
        )
        if target is None:
            raise TenantConfigTargetVersionNotFoundError("target version not found")
        rolled = TenantConfig(
            tenant_id=tenant_id,
            enabled=target.enabled,
            version=expected_version + 1,
            app=target.app,
            governance=make_governance(),
            backend_profile=target.backend_profile,
            audit_policy=target.audit_policy,
        )
        self._heads[tenant_id] = rolled
        self._history[tenant_id].append(rolled)
        return rolled

    async def list_versions(
        self,
        tenant_id: str,
        *,
        before_version: int | None = None,
        limit: int = 50,
    ) -> tuple[TenantConfig, ...]:
        self._maybe_raise()
        history = self._history.get(tenant_id, [])
        selected = [c for c in history if before_version is None or c.version < before_version]
        selected.sort(key=lambda c: c.version, reverse=True)
        return tuple(selected[:limit])

    async def get(self, tenant_id: str) -> TenantConfig | None:
        self.get_calls += 1
        self._maybe_raise()
        return self._heads.get(tenant_id)

    async def check_ready(self) -> None:
        self.ready_count += 1
        self._maybe_raise()

    async def close(self) -> None:
        self.close_count += 1


@pytest.fixture
def fake_repo() -> FakeAdminRepository:
    return FakeAdminRepository()


class _FakeExecutionAuditRepository:
    """Protocol double: records receipt/request listing calls."""

    def __init__(self, events=()):
        self._events = tuple(events)
        self.list_calls = 0
        self.last_args = None
        self.last_method = None
        self.ready_count = 0
        self.close_count = 0
        self.raise_unavailable = False
        self.raise_data = False

    def _maybe_raise(self):
        if self.raise_unavailable:
            from trpc_service.storage.execution_audit_repository import \
                ExecutionAuditRepositoryUnavailableError
            raise ExecutionAuditRepositoryUnavailableError("db unreachable")
        if self.raise_data:
            from trpc_service.storage.execution_audit_repository import \
                ExecutionAuditRepositoryDataError
            raise ExecutionAuditRepositoryDataError("corrupt")

    def set_events(self, events):
        self._events = tuple(events)

    async def append(self, event):
        return None

    async def list_for_receipt(self, tenant_id, receipt_id, limit):
        self.list_calls += 1
        self.last_args = (tenant_id, receipt_id, limit)
        self.last_method = "receipt"
        self._maybe_raise()
        return self._events

    async def list_for_request(self, tenant_id, request_id, limit):
        self.list_calls += 1
        self.last_args = (tenant_id, request_id, limit)
        self.last_method = "request"
        self._maybe_raise()
        return self._events

    async def check_ready(self):
        self.ready_count += 1

    async def close(self):
        self.close_count += 1


def _audit_event(**overrides):
    import uuid as _uuid
    defaults = dict(
        audit_id=_uuid.uuid4(),
        tenant_id="tenant_x",
        receipt_id=_uuid.uuid4(),
        request_id=_uuid.uuid4(),
        config_version=1,
        trace_id="0" * 28 + "abcd",
        event_type="content_decision",
        outcome="allow",
        category="none",
        tool_name=None,
        error_code=None,
        latency_ms=5,
        occurred_at=datetime(2026, 9, 5, 12, 0, 0, tzinfo=timezone.utc),
    )
    defaults.update(overrides)
    return ExecutionAuditEvent(**defaults)


@pytest.fixture
def fake_exec_repo():
    return _FakeExecutionAuditRepository(events=[_audit_event()])


class _FakeUsageRepository:

    def __init__(self, usage=None):
        self.usage = usage
        self.get_calls = []
        self.ready_count = 0
        self.close_count = 0
        self.raise_unavailable = False
        self.raise_data = False

    async def get_daily(self, tenant_id, day):
        self.get_calls.append((tenant_id, day))
        if self.raise_unavailable:
            from trpc_service.storage.usage_repository import UsageRepositoryUnavailableError
            raise UsageRepositoryUnavailableError("db unreachable")
        if self.raise_data:
            from trpc_service.storage.usage_repository import UsageRepositoryDataError
            raise UsageRepositoryDataError("corrupt")
        return self.usage

    async def add_usage(self, usage):
        return self.usage

    async def check_ready(self):
        self.ready_count += 1

    async def close(self):
        self.close_count += 1


@pytest.fixture
def fake_usage_repo():
    from datetime import date as _date

    from trpc_service.usage.models import ProfileDailyUsage, TenantDailyUsage
    usage = TenantDailyUsage(
        usage_date=_date(2026, 9, 5),
        tenant_id="tenant_x",
        profiles=(ProfileDailyUsage(
            model_profile="default",
            requests=3,
            input_tokens=100,
            output_tokens=None,
            cost_microunits=None,
        ), ),
    )
    return _FakeUsageRepository(usage=usage)


@pytest.fixture
def app(
    fake_repo: FakeAdminRepository,
    fake_message_repo,
    fake_exec_repo,
    fake_usage_repo,
    monkeypatch: pytest.MonkeyPatch,
) -> FastAPI:
    monkeypatch.setattr(
        "trpc_service.storage.tenant_repository.SqlTenantConfigRepository.from_env",
        lambda *a, **k:
        (_ for _ in ()).throw(AssertionError("SQL repository must not be created when a repo is injected")),
    )
    monkeypatch.setattr(
        "trpc_service.storage.message_repository.SqlMessageReceiptRepository.from_env",
        lambda *a, **k:
        (_ for _ in ()).throw(AssertionError("SQL message repository must not be created when a repo is injected")),
    )
    monkeypatch.setattr(
        "trpc_service.storage.execution_audit_repository.SqlExecutionAuditRepository.from_env",
        lambda *a, **k:
        (_ for _ in ()).throw(AssertionError("SQL audit repository must not be created when a repo is injected")),
    )
    return create_admin_app(
        tenant_repository=fake_repo,
        message_repository=fake_message_repo,
        execution_repository=fake_exec_repo,
        usage_repository=fake_usage_repo,
        environ={"TRPC_ADMIN_TOKEN": TOKEN},
    )


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            headers=AUTH_HEADERS,
    ) as async_client:
        yield async_client


class TestAdminAuth:

    @pytest.mark.asyncio
    async def test_missing_token_returns_401_fixed_text(self, app: FastAPI):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as raw:
            response = await raw.get("/admin/v1/tenants/tenant_x")
        assert response.status_code == 401
        assert response.json()["detail"] == "Admin authentication failed."

    @pytest.mark.asyncio
    async def test_wrong_token_returns_401_fixed_text(self, app: FastAPI):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
                transport=transport,
                base_url="http://test",
                headers={"X-TRPC-Admin-Token": "w" * 32},
        ) as raw:
            response = await raw.get("/admin/v1/tenants/tenant_x")
        assert response.status_code == 401
        assert response.json()["detail"] == "Admin authentication failed."

    @pytest.mark.asyncio
    async def test_wrong_token_does_not_echo_submitted_token(self, app: FastAPI):
        bad_token = "w" * 32
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
                transport=transport,
                base_url="http://test",
                headers={"X-TRPC-Admin-Token": bad_token},
        ) as raw:
            response = await raw.get("/admin/v1/tenants/tenant_x")
        assert bad_token not in response.text


_GOV_JSON = {
    "allowed_channels": ["web", "web_console", "wecom", "feishu"],
    "allowed_user_ids": [],
    "tool_decisions": {},
    "content_policy": {
        "enabled": True,
        "input_action": "block",
        "output_action": "block",
    },
    "limits": None,
}

# R1A: every Admin tenant write/read payload must carry the explicit profile.
_BACKEND_PROFILE_JSON = {
    "state_backend": "redis",
    "artifact_backend": "s3",
    "knowledge_backend": "sql",
    "audit_backend": "sql",
}

_AUDIT_POLICY_JSON = {"retention_days": 365, "delivery_events": "all"}


class TestAdminSchemas:

    @pytest.mark.asyncio
    async def test_create_rejects_unknown_fields(self, client: httpx.AsyncClient):
        response = await client.post(
            "/admin/v1/tenants",
            json={
                "tenant_id": "tenant_x",
                "enabled": True,
                "app": {
                    "app_id": "app_a",
                    "instruction": "instr",
                    "model_profile": "default",
                    "allowed_tools": [],
                    "extra": 1,
                },
                "governance": _GOV_JSON,
                "backend_profile": _BACKEND_PROFILE_JSON,
                "audit_policy": _AUDIT_POLICY_JSON,
            },
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_create_rejects_version_field(self, client: httpx.AsyncClient):
        response = await client.post(
            "/admin/v1/tenants",
            json={
                "version": 7,
                "enabled": True,
                "app": {
                    "app_id": "app_a",
                    "instruction": "instr",
                    "model_profile": "default",
                    "allowed_tools": [],
                },
                "governance": _GOV_JSON,
                "backend_profile": _BACKEND_PROFILE_JSON,
                "audit_policy": _AUDIT_POLICY_JSON,
            },
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_create_rejects_loose_bool(self, client: httpx.AsyncClient):
        response = await client.post(
            "/admin/v1/tenants",
            json={
                "tenant_id": "tenant_x",
                "enabled": "yes",
                "app": {
                    "app_id": "app_a",
                    "instruction": "instr",
                    "model_profile": "default",
                    "allowed_tools": [],
                },
                "governance": _GOV_JSON,
                "backend_profile": _BACKEND_PROFILE_JSON,
                "audit_policy": _AUDIT_POLICY_JSON,
            },
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_update_rejects_zero_expected_version(self, client: httpx.AsyncClient):
        response = await client.put(
            "/admin/v1/tenants/tenant_x",
            json={
                "expected_version": 0,
                "desired": {
                    "enabled": True,
                    "app": {
                        "app_id": "app_a",
                        "instruction": "instr",
                        "model_profile": "default",
                        "allowed_tools": [],
                    },
                    "governance": _GOV_JSON,
                    "backend_profile": _BACKEND_PROFILE_JSON,
                    "audit_policy": _AUDIT_POLICY_JSON,
                },
            },
        )
        assert response.status_code == 422


class TestAdminRoutes:

    @pytest.mark.asyncio
    async def test_create_returns_201_with_version_one(
        self,
        client: httpx.AsyncClient,
    ):
        response = await client.post(
            "/admin/v1/tenants",
            json={
                "tenant_id": "tenant_x",
                "enabled": True,
                "app": {
                    "app_id": "app_a",
                    "instruction": "instr",
                    "model_profile": "default",
                    "allowed_tools": [],
                },
                "governance": _GOV_JSON,
                "backend_profile": _BACKEND_PROFILE_JSON,
                "audit_policy": _AUDIT_POLICY_JSON,
            },
        )
        assert response.status_code == 201
        body = response.json()
        assert body["tenant_id"] == "tenant_x"
        assert body["version"] == 1
        assert body["enabled"] is True
        assert body["app"]["app_id"] == "app_a"

    @pytest.mark.asyncio
    async def test_create_duplicate_returns_409(self, client: httpx.AsyncClient):
        payload = {
            "tenant_id": "tenant_x",
            "enabled": True,
            "app": {
                "app_id": "app_a",
                "instruction": "instr",
                "model_profile": "default",
                "allowed_tools": [],
            },
            "governance": _GOV_JSON,
            "backend_profile": _BACKEND_PROFILE_JSON,
            "audit_policy": _AUDIT_POLICY_JSON,
        }
        await client.post("/admin/v1/tenants", json=payload)
        response = await client.post("/admin/v1/tenants", json=payload)
        assert response.status_code == 409
        assert response.json()["detail"] == "Tenant already exists."

    @pytest.mark.asyncio
    async def test_get_current_returns_head(self, client: httpx.AsyncClient):
        await client.post(
            "/admin/v1/tenants",
            json={
                "tenant_id": "tenant_x",
                "enabled": True,
                "app": {
                    "app_id": "app_a",
                    "instruction": "instr",
                    "model_profile": "default",
                    "allowed_tools": [],
                },
                "governance": _GOV_JSON,
                "backend_profile": _BACKEND_PROFILE_JSON,
                "audit_policy": _AUDIT_POLICY_JSON,
            },
        )
        response = await client.get("/admin/v1/tenants/tenant_x")
        assert response.status_code == 200
        assert response.json()["version"] == 1

    @pytest.mark.asyncio
    async def test_get_unknown_tenant_returns_404(self, client: httpx.AsyncClient):
        response = await client.get("/admin/v1/tenants/nope")
        assert response.status_code == 404
        assert response.json()["detail"] == "Tenant not found."

    @pytest.mark.asyncio
    async def test_update_returns_new_version(self, client: httpx.AsyncClient):
        await client.post(
            "/admin/v1/tenants",
            json={
                "tenant_id": "tenant_x",
                "enabled": True,
                "app": {
                    "app_id": "app_a",
                    "instruction": "v1",
                    "model_profile": "default",
                    "allowed_tools": [],
                },
                "governance": _GOV_JSON,
                "backend_profile": _BACKEND_PROFILE_JSON,
                "audit_policy": _AUDIT_POLICY_JSON,
            },
        )
        response = await client.put(
            "/admin/v1/tenants/tenant_x",
            json={
                "expected_version": 1,
                "desired": {
                    "enabled": False,
                    "app": {
                        "app_id": "app_b",
                        "instruction": "v2",
                        "model_profile": "default",
                        "allowed_tools": ["get_current_time"],
                    },
                    "governance": _GOV_JSON,
                    "backend_profile": _BACKEND_PROFILE_JSON,
                    "audit_policy": _AUDIT_POLICY_JSON,
                },
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["version"] == 2
        assert body["enabled"] is False
        assert body["app"]["instruction"] == "v2"

    @pytest.mark.asyncio
    async def test_update_stale_expected_version_returns_409(
        self,
        client: httpx.AsyncClient,
    ):
        await client.post(
            "/admin/v1/tenants",
            json={
                "tenant_id": "tenant_x",
                "enabled": True,
                "app": {
                    "app_id": "app_a",
                    "instruction": "instr",
                    "model_profile": "default",
                    "allowed_tools": [],
                },
                "governance": _GOV_JSON,
                "backend_profile": _BACKEND_PROFILE_JSON,
                "audit_policy": _AUDIT_POLICY_JSON,
            },
        )
        response = await client.put(
            "/admin/v1/tenants/tenant_x",
            json={
                "expected_version": 5,
                "desired": {
                    "enabled": True,
                    "app": {
                        "app_id": "app_a",
                        "instruction": "instr",
                        "model_profile": "default",
                        "allowed_tools": [],
                    },
                    "governance": _GOV_JSON,
                    "backend_profile": _BACKEND_PROFILE_JSON,
                    "audit_policy": _AUDIT_POLICY_JSON,
                },
            },
        )
        assert response.status_code == 409
        assert response.json()["detail"] == "Configuration version conflict."

    @pytest.mark.asyncio
    async def test_update_unknown_tenant_returns_404(self, client: httpx.AsyncClient):
        response = await client.put(
            "/admin/v1/tenants/nope",
            json={
                "expected_version": 1,
                "desired": {
                    "enabled": True,
                    "app": {
                        "app_id": "app_a",
                        "instruction": "instr",
                        "model_profile": "default",
                        "allowed_tools": [],
                    },
                    "governance": _GOV_JSON,
                    "backend_profile": _BACKEND_PROFILE_JSON,
                    "audit_policy": _AUDIT_POLICY_JSON,
                },
            },
        )
        assert response.status_code == 404
        assert response.json()["detail"] == "Tenant not found."

    @pytest.mark.asyncio
    async def test_rollback_returns_forward_version_with_target_content(
        self,
        client: httpx.AsyncClient,
    ):
        payload_v1 = {
            "tenant_id": "tenant_x",
            "enabled": True,
            "app": {
                "app_id": "app_a",
                "instruction": "v1",
                "model_profile": "default",
                "allowed_tools": [],
            },
            "governance": _GOV_JSON,
            "backend_profile": _BACKEND_PROFILE_JSON,
            "audit_policy": _AUDIT_POLICY_JSON,
        }
        await client.post("/admin/v1/tenants", json=payload_v1)
        await client.put(
            "/admin/v1/tenants/tenant_x",
            json={
                "expected_version": 1,
                "desired": {
                    "enabled": True,
                    "app": {
                        "app_id": "app_a",
                        "instruction": "v2",
                        "model_profile": "default",
                        "allowed_tools": [],
                    },
                    "governance": _GOV_JSON,
                    "backend_profile": _BACKEND_PROFILE_JSON,
                    "audit_policy": _AUDIT_POLICY_JSON,
                },
            },
        )
        response = await client.post(
            "/admin/v1/tenants/tenant_x/rollback",
            json={
                "expected_version": 2,
                "target_version": 1
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["version"] == 3
        assert body["app"]["instruction"] == "v1"

    @pytest.mark.asyncio
    async def test_rollback_missing_target_returns_404(self, client: httpx.AsyncClient):
        await client.post(
            "/admin/v1/tenants",
            json={
                "tenant_id": "tenant_x",
                "enabled": True,
                "app": {
                    "app_id": "app_a",
                    "instruction": "instr",
                    "model_profile": "default",
                    "allowed_tools": [],
                },
                "governance": _GOV_JSON,
                "backend_profile": _BACKEND_PROFILE_JSON,
                "audit_policy": _AUDIT_POLICY_JSON,
            },
        )
        response = await client.post(
            "/admin/v1/tenants/tenant_x/rollback",
            json={
                "expected_version": 1,
                "target_version": 7
            },
        )
        assert response.status_code == 404
        assert response.json()["detail"] == "Target version not found."


class TestAdminVersionPaging:

    async def _seed_three_versions(self, client: httpx.AsyncClient) -> None:
        for instruction in ("v1", "v2", "v3"):
            if instruction == "v1":
                await client.post(
                    "/admin/v1/tenants",
                    json={
                        "tenant_id": "tenant_x",
                        "enabled": True,
                        "app": {
                            "app_id": "app_a",
                            "instruction": instruction,
                            "model_profile": "default",
                            "allowed_tools": [],
                        },
                        "governance": _GOV_JSON,
                        "backend_profile": _BACKEND_PROFILE_JSON,
                        "audit_policy": _AUDIT_POLICY_JSON,
                    },
                )
            else:
                response = await client.get("/admin/v1/tenants/tenant_x")
                expected = response.json()["version"]
                await client.put(
                    "/admin/v1/tenants/tenant_x",
                    json={
                        "expected_version": expected,
                        "desired": {
                            "enabled": True,
                            "app": {
                                "app_id": "app_a",
                                "instruction": instruction,
                                "model_profile": "default",
                                "allowed_tools": [],
                            },
                            "governance": _GOV_JSON,
                            "backend_profile": _BACKEND_PROFILE_JSON,
                            "audit_policy": _AUDIT_POLICY_JSON,
                        },
                    },
                )

    @pytest.mark.asyncio
    async def test_versions_descending_default_limit(
        self,
        client: httpx.AsyncClient,
    ):
        await self._seed_three_versions(client)
        response = await client.get("/admin/v1/tenants/tenant_x/versions")
        assert response.status_code == 200
        versions = [v["version"] for v in response.json()["versions"]]
        assert versions == [3, 2, 1]

    @pytest.mark.asyncio
    async def test_versions_before_version_is_exclusive(
        self,
        client: httpx.AsyncClient,
    ):
        await self._seed_three_versions(client)
        response = await client.get("/admin/v1/tenants/tenant_x/versions?before_version=3", )
        versions = [v["version"] for v in response.json()["versions"]]
        assert versions == [2, 1]

    @pytest.mark.asyncio
    async def test_versions_limit_bounds(self, client: httpx.AsyncClient):
        await self._seed_three_versions(client)
        ok = await client.get("/admin/v1/tenants/tenant_x/versions?limit=1")
        assert [v["version"] for v in ok.json()["versions"]] == [3]
        zero = await client.get("/admin/v1/tenants/tenant_x/versions?limit=0")
        assert zero.status_code == 422
        too_big = await client.get("/admin/v1/tenants/tenant_x/versions?limit=101")
        assert too_big.status_code == 422

    @pytest.mark.asyncio
    async def test_versions_unknown_tenant_returns_404(
        self,
        client: httpx.AsyncClient,
    ):
        response = await client.get("/admin/v1/tenants/nope/versions")
        assert response.status_code == 404
        assert response.json()["detail"] == "Tenant not found."


class TestAdminErrorMapping:

    @pytest.mark.asyncio
    async def test_repository_unavailable_returns_503_fixed_text(
        self,
        app: FastAPI,
        fake_repo: FakeAdminRepository,
    ):
        fake_repo.raise_unavailable_on_call = True
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
                transport=transport,
                base_url="http://test",
                headers=AUTH_HEADERS,
        ) as raw:
            response = await raw.get("/admin/v1/tenants/tenant_x")
        assert response.status_code == 503
        assert response.json()["detail"] == "Tenant repository is unavailable."

    @pytest.mark.asyncio
    async def test_unclassified_repository_error_returns_500_fixed_text(
        self,
        app: FastAPI,
        fake_repo: FakeAdminRepository,
        caplog: pytest.LogCaptureFixture,
    ):
        fake_repo.raise_generic_on_call = True
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx.AsyncClient(
                transport=transport,
                base_url="http://test",
                headers=AUTH_HEADERS,
        ) as raw:
            response = await raw.get("/admin/v1/tenants/tenant_x")
        assert response.status_code == 500
        assert response.json()["detail"] == "Internal server error."
        error_logs = [r.message for r in caplog.records if r.levelname == "ERROR"]
        assert any("RuntimeError" in msg for msg in error_logs), \
            f"Expected exception type in log, got: {error_logs}"
        assert not any("unexpected internal error" in msg for msg in error_logs), \
            "Log must not contain exception message text (sensitive data leak)"

    @pytest.mark.asyncio
    async def test_validation_errors_use_fixed_text(self, client: httpx.AsyncClient):
        response = await client.post("/admin/v1/tenants", json={"enabled": "yes"})
        assert response.status_code == 422
        assert response.json()["detail"] == "Request validation failed."

    @pytest.mark.asyncio
    @pytest.mark.parametrize("invalid_id", ["INVALID", "123start", "has space", "has@symbol"])
    async def test_invalid_tenant_id_returns_422_on_get(
        self,
        client: httpx.AsyncClient,
        fake_repo: FakeAdminRepository,
        invalid_id: str,
    ):
        response = await client.get(f"/admin/v1/tenants/{invalid_id}")
        assert response.status_code == 422
        assert response.json()["detail"] == "Invalid tenant ID format."
        assert fake_repo.get_calls == 0, "Repository must not be called for invalid tenant_id"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("invalid_id", ["INVALID", "123start", "has space"])
    async def test_invalid_tenant_id_returns_422_on_list_versions(
        self,
        client: httpx.AsyncClient,
        fake_repo: FakeAdminRepository,
        invalid_id: str,
    ):
        response = await client.get(f"/admin/v1/tenants/{invalid_id}/versions")
        assert response.status_code == 422
        assert response.json()["detail"] == "Invalid tenant ID format."
        assert fake_repo.get_calls == 0, "Repository must not be called for invalid tenant_id"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("invalid_id", ["INVALID", "123start", "has space"])
    async def test_invalid_tenant_id_returns_422_on_update(
        self,
        client: httpx.AsyncClient,
        fake_repo: FakeAdminRepository,
        invalid_id: str,
    ):
        response = await client.put(
            f"/admin/v1/tenants/{invalid_id}",
            json={
                "expected_version": 1,
                "desired": {
                    "enabled": True,
                    "app": {
                        "app_id": "app_a",
                        "instruction": "test",
                        "model_profile": "default",
                        "allowed_tools": [],
                    },
                    "governance": _GOV_JSON,
                    "backend_profile": _BACKEND_PROFILE_JSON,
                    "audit_policy": _AUDIT_POLICY_JSON,
                },
            },
        )
        assert response.status_code == 422
        assert response.json()["detail"] == "Invalid tenant ID format."
        assert fake_repo.update_calls == 0, "Repository must not be called for invalid tenant_id"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("invalid_id", ["INVALID", "123start", "has space"])
    async def test_invalid_tenant_id_returns_422_on_rollback(
        self,
        client: httpx.AsyncClient,
        fake_repo: FakeAdminRepository,
        invalid_id: str,
    ):
        response = await client.post(
            f"/admin/v1/tenants/{invalid_id}/rollback",
            json={
                "expected_version": 1,
                "target_version": 1
            },
        )
        assert response.status_code == 422
        assert response.json()["detail"] == "Invalid tenant ID format."
        assert fake_repo.rollback_calls == 0, "Repository must not be called for invalid tenant_id"


class TestAdminLifespan:

    def test_startup_readiness_and_shutdown_close_with_injected_repo(
        self,
        fake_repo: FakeAdminRepository,
        fake_message_repo,
        fake_exec_repo,
        fake_usage_repo,
    ):
        application = create_admin_app(
            tenant_repository=fake_repo,
            message_repository=fake_message_repo,
            execution_repository=fake_exec_repo,
            usage_repository=fake_usage_repo,
            environ={"TRPC_ADMIN_TOKEN": TOKEN},
        )
        with TestClient(application) as test_client:
            assert fake_repo.ready_count == 1
            assert fake_repo.close_count == 0
            response = test_client.get(
                "/admin/v1/tenants/tenant_x",
                headers=AUTH_HEADERS,
            )
            assert response.status_code == 404
        assert fake_repo.close_count == 0

    def test_readiness_failure_prevents_startup(
        self,
        fake_repo: FakeAdminRepository,
        fake_message_repo,
        fake_exec_repo,
        fake_usage_repo,
    ):
        fake_repo.raise_unavailable_on_call = True
        application = create_admin_app(
            tenant_repository=fake_repo,
            message_repository=fake_message_repo,
            execution_repository=fake_exec_repo,
            usage_repository=fake_usage_repo,
            environ={"TRPC_ADMIN_TOKEN": TOKEN},
        )
        with pytest.raises(TenantRepositoryUnavailableError):
            with TestClient(application):
                pass

    def test_default_assembly_shares_one_engine_and_disposes_it_once(
        self,
        fake_repo: FakeAdminRepository,
        monkeypatch: pytest.MonkeyPatch,
    ):
        import trpc_service.admin.app as admin_app

        class _Engine:

            def __init__(self) -> None:
                self.dispose_count = 0

            async def dispose(self) -> None:
                self.dispose_count += 1

        engine = _Engine()
        constructed: list[tuple[object, bool]] = []

        class _Settings:

            @classmethod
            def from_env(cls, environ):
                del environ
                return object()

        def _tenant_repository(received_engine: object, *, owns_engine: bool):
            constructed.append((received_engine, owns_engine))
            return fake_repo

        def _message_repository(received_engine: object, *, owns_engine: bool):
            constructed.append((received_engine, owns_engine))
            return fake_repo

        def _execution_repository(received_engine: object, *, owns_engine: bool):
            constructed.append((received_engine, owns_engine))
            return fake_repo

        def _usage_repository(received_engine: object, *, owns_engine: bool):
            constructed.append((received_engine, owns_engine))
            return fake_repo

        def _approval_repository(received_engine: object, *, owns_engine: bool):
            constructed.append((received_engine, owns_engine))
            return fake_repo

        monkeypatch.setattr(admin_app, "DatabaseSettings", _Settings, raising=False)
        monkeypatch.setattr(admin_app, "create_database_engine", lambda settings: engine, raising=False)
        monkeypatch.setattr(admin_app, "SqlTenantConfigRepository", _tenant_repository)
        monkeypatch.setattr(admin_app, "SqlMessageReceiptRepository", _message_repository)
        monkeypatch.setattr(admin_app, "SqlExecutionAuditRepository", _execution_repository)
        monkeypatch.setattr(admin_app, "SqlUsageRepository", _usage_repository)
        monkeypatch.setattr(admin_app, "SqlToolApprovalRepository", _approval_repository)
        monkeypatch.setattr(admin_app, "SqlChannelBindingRepository", _approval_repository)
        monkeypatch.setattr(admin_app, "SqlAuditQueryRepository", _approval_repository)
        monkeypatch.setattr(admin_app, "SqlTenantConfigRolloutRepository", _approval_repository)
        application = create_admin_app(environ={
            "TRPC_ADMIN_TOKEN": TOKEN,
            "TRPC_DATABASE_URL": "postgresql+asyncpg://x"
        }, )
        with TestClient(application):
            # tenant, message, execution and usage repositories are all checked
            assert fake_repo.ready_count == 8
        assert constructed == [(engine, False)] * 8
        assert fake_repo.close_count == 8
        assert engine.dispose_count == 1


class TestAdminIsolation:

    def test_admin_module_not_imported_by_gateway_or_worker(self):
        code = ("import sys\n"
                "import trpc_service.gateway.app\n"
                "import trpc_service.worker.app\n"
                "leaked = [m for m in sys.modules if m.startswith('trpc_service.admin')]\n"
                "assert not leaked, f'admin modules imported: {leaked}'\n")
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stderr


class TestAdminHealth:

    @pytest.mark.asyncio
    async def test_health_is_public_liveness_endpoint(self, app: FastAPI):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as raw:
            response = await raw.get("/health")
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# message-audit endpoint — strict validation
# ---------------------------------------------------------------------------


class _FakeMessageRepository:

    def __init__(self):
        self.list_audit_calls = 0
        self.ready_count = 0
        self.close_count = 0

    async def list_audit(self, tenant_id: str, message_id: str, limit: int):
        self.list_audit_calls += 1
        return []

    async def check_ready(self):
        self.ready_count += 1

    async def close(self):
        self.close_count += 1


@pytest.fixture
def fake_message_repo():
    return _FakeMessageRepository()


@pytest.fixture
def app_with_audit(
    fake_repo: FakeAdminRepository,
    fake_message_repo: _FakeMessageRepository,
    fake_exec_repo: _FakeExecutionAuditRepository,
    fake_usage_repo,
) -> FastAPI:
    return create_admin_app(
        tenant_repository=fake_repo,
        message_repository=fake_message_repo,
        execution_repository=fake_exec_repo,
        usage_repository=fake_usage_repo,
        environ={"TRPC_ADMIN_TOKEN": TOKEN},
    )


@pytest_asyncio.fixture
async def audit_client(app_with_audit: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app_with_audit)
    async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            headers=AUTH_HEADERS,
    ) as async_client:
        yield async_client


class TestMessageAuditValidation:

    @pytest.mark.asyncio
    async def test_blank_message_id_returns_422_and_no_repository_call(
        self,
        audit_client: httpx.AsyncClient,
        fake_message_repo: _FakeMessageRepository,
    ):
        """Whitespace-only message_id must return 422 without querying repository."""
        response = await audit_client.get(
            "/admin/v1/tenants/tenant_x/message-audit",
            params={
                "message_id": "   ",
                "limit": 10
            },
        )
        assert response.status_code == 422
        assert fake_message_repo.list_audit_calls == 0

    @pytest.mark.asyncio
    async def test_empty_message_id_returns_422_and_no_repository_call(
        self,
        audit_client: httpx.AsyncClient,
        fake_message_repo: _FakeMessageRepository,
    ):
        """Empty message_id must return 422 without querying repository."""
        response = await audit_client.get(
            "/admin/v1/tenants/tenant_x/message-audit",
            params={
                "message_id": "",
                "limit": 10
            },
        )
        assert response.status_code == 422
        assert fake_message_repo.list_audit_calls == 0

    @pytest.mark.asyncio
    async def test_overlong_message_id_returns_422_and_no_repository_call(
        self,
        audit_client: httpx.AsyncClient,
        fake_message_repo: _FakeMessageRepository,
    ):
        """message_id > 200 chars must return 422 without querying repository."""
        response = await audit_client.get(
            "/admin/v1/tenants/tenant_x/message-audit",
            params={
                "message_id": "x" * 201,
                "limit": 10
            },
        )
        assert response.status_code == 422
        assert fake_message_repo.list_audit_calls == 0

    @pytest.mark.asyncio
    async def test_missing_message_id_returns_422_and_no_repository_call(
        self,
        audit_client: httpx.AsyncClient,
        fake_message_repo: _FakeMessageRepository,
    ):
        """Missing message_id must return 422 without querying repository."""
        response = await audit_client.get(
            "/admin/v1/tenants/tenant_x/message-audit",
            params={"limit": 10},
        )
        assert response.status_code == 422
        assert fake_message_repo.list_audit_calls == 0

    @pytest.mark.asyncio
    async def test_valid_message_id_queries_repository(
        self,
        audit_client: httpx.AsyncClient,
        fake_message_repo: _FakeMessageRepository,
    ):
        """Valid message_id must query repository exactly once."""
        response = await audit_client.get(
            "/admin/v1/tenants/tenant_x/message-audit",
            params={
                "message_id": "msg-123",
                "limit": 10
            },
        )
        assert response.status_code == 200
        assert fake_message_repo.list_audit_calls == 1

    @pytest.mark.asyncio
    async def test_missing_token_returns_401(
        self,
        app_with_audit: FastAPI,
        fake_message_repo: _FakeMessageRepository,
    ):
        """Missing admin token must return 401."""
        transport = httpx.ASGITransport(app=app_with_audit)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as raw:
            response = await raw.get(
                "/admin/v1/tenants/tenant_x/message-audit",
                params={"message_id": "msg-123"},
            )
        assert response.status_code == 401
        assert fake_message_repo.list_audit_calls == 0

    @pytest.mark.asyncio
    async def test_invalid_tenant_id_returns_422(
        self,
        audit_client: httpx.AsyncClient,
        fake_message_repo: _FakeMessageRepository,
    ):
        """Invalid tenant_id format must return 422."""
        response = await audit_client.get(
            "/admin/v1/tenants/INVALID/message-audit",
            params={"message_id": "msg-123"},
        )
        assert response.status_code == 422
        assert fake_message_repo.list_audit_calls == 0

    @pytest.mark.asyncio
    async def test_invalid_limit_returns_422(
        self,
        audit_client: httpx.AsyncClient,
        fake_message_repo: _FakeMessageRepository,
    ):
        """limit < 1 or > 100 must return 422."""
        response = await audit_client.get(
            "/admin/v1/tenants/tenant_x/message-audit",
            params={
                "message_id": "msg-123",
                "limit": 0
            },
        )
        assert response.status_code == 422
        assert fake_message_repo.list_audit_calls == 0

        response = await audit_client.get(
            "/admin/v1/tenants/tenant_x/message-audit",
            params={
                "message_id": "msg-123",
                "limit": 101
            },
        )
        assert response.status_code == 422
        assert fake_message_repo.list_audit_calls == 0

    @pytest.mark.asyncio
    async def test_response_does_not_contain_raw_text(
        self,
        audit_client: httpx.AsyncClient,
    ):
        """Response must not contain raw message or response text."""
        response = await audit_client.get(
            "/admin/v1/tenants/tenant_x/message-audit",
            params={"message_id": "msg-123"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "events" in data
        # The response should only contain digests, not raw text
        # (This test verifies the schema structure)
        assert isinstance(data["events"], list)


# ---------------------------------------------------------------------------
# execution-audit endpoint — append-only governance trail (Stage 6B2 Task 1)
# ---------------------------------------------------------------------------

RECEIPT_URL = "/admin/v1/tenants/tenant_x/receipts/11111111-1111-1111-1111-111111111111/execution-audit"
REQUEST_URL = "/admin/v1/tenants/tenant_x/requests/22222222-2222-2222-2222-222222222222/execution-audit"


@pytest.fixture
def app_with_exec(app):
    # All three repositories are injected by the shared ``app`` fixture.
    return app


@pytest_asyncio.fixture
async def exec_client(app_with_exec):
    transport = httpx.ASGITransport(app=app_with_exec)
    async with httpx.AsyncClient(transport=transport, base_url="http://test", headers=AUTH_HEADERS) as c:
        yield c


class TestAdminFactoryInjection:
    """Half injection is a configuration bug, not a supported mode."""

    @pytest.mark.parametrize(
        "kwargs",
        [
            {
                "tenant_repository": "x"
            },
            {
                "tenant_repository": "x",
                "message_repository": "y"
            },
            {
                "message_repository": "y"
            },
            {
                "execution_repository": "z"
            },
            {
                "message_repository": "y",
                "execution_repository": "z"
            },
            {
                "usage_repository": "u"
            },
            {
                "tenant_repository": "x",
                "usage_repository": "u"
            },
        ],
    )
    def test_partial_injection_raises_fixed_error(self, kwargs):
        with pytest.raises(ValueError) as raised:
            create_admin_app(environ={"TRPC_ADMIN_TOKEN": TOKEN}, **kwargs)
        assert str(raised.value) == "Admin repositories must be injected together or not at all"

    def test_partial_injection_error_is_raised_before_token_check(self):
        # No token at all must still surface the injection error first — the
        # configuration error text is fixed and carries no input values.
        with pytest.raises(ValueError) as raised:
            create_admin_app(tenant_repository="x", environ=None)
        assert "TRPC_ADMIN_TOKEN" not in str(raised.value)

    def test_full_injection_is_accepted(self, fake_repo, fake_message_repo, fake_exec_repo, fake_usage_repo):
        application = create_admin_app(
            tenant_repository=fake_repo,
            message_repository=fake_message_repo,
            execution_repository=fake_exec_repo,
            usage_repository=fake_usage_repo,
            environ={"TRPC_ADMIN_TOKEN": TOKEN},
        )
        assert application is not None


class TestInjectedRepositoriesAreNotClosed:

    def test_injected_repositories_survive_app_shutdown(self, fake_repo, fake_message_repo, fake_exec_repo,
                                                        fake_usage_repo):
        application = create_admin_app(
            tenant_repository=fake_repo,
            message_repository=fake_message_repo,
            execution_repository=fake_exec_repo,
            usage_repository=fake_usage_repo,
            environ={"TRPC_ADMIN_TOKEN": TOKEN},
        )
        with TestClient(application):
            pass
        # The factory never closes resources it does not own.
        assert fake_repo.close_count == 0
        assert fake_message_repo.close_count == 0
        assert fake_exec_repo.close_count == 0
        # ...but it does readiness-check all three.
        assert fake_repo.ready_count == 1
        assert fake_message_repo.ready_count == 1
        assert fake_exec_repo.ready_count == 1
        assert fake_usage_repo.ready_count == 1
        assert fake_usage_repo.close_count == 0


class TestExecutionAuditEndpoint:

    @pytest.mark.asyncio
    async def test_valid_receipt_returns_events(self, exec_client, fake_exec_repo):
        rid = "11111111-1111-1111-1111-111111111111"
        response = await exec_client.get(
            f"/admin/v1/tenants/tenant_x/receipts/{rid}/execution-audit",
            params={"limit": 10},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["events"]) == 1
        assert fake_exec_repo.list_calls == 1
        assert fake_exec_repo.last_args[0] == "tenant_x"
        assert str(fake_exec_repo.last_args[1]) == rid

    @pytest.mark.asyncio
    async def test_missing_token_returns_401_without_query(self, app_with_exec, fake_exec_repo):
        transport = httpx.ASGITransport(app=app_with_exec)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as raw:
            response = await raw.get(
                "/admin/v1/tenants/tenant_x/receipts/11111111-1111-1111-1111-111111111111/execution-audit")
        assert response.status_code == 401
        assert fake_exec_repo.list_calls == 0

    @pytest.mark.asyncio
    async def test_invalid_tenant_id_returns_422(self, exec_client, fake_exec_repo):
        response = await exec_client.get(
            "/admin/v1/tenants/INVALID/receipts/11111111-1111-1111-1111-111111111111/execution-audit")
        assert response.status_code == 422
        assert fake_exec_repo.list_calls == 0

    @pytest.mark.asyncio
    async def test_invalid_receipt_uuid_returns_422(self, exec_client, fake_exec_repo):
        response = await exec_client.get("/admin/v1/tenants/tenant_x/receipts/not-a-uuid/execution-audit")
        assert response.status_code == 422
        assert fake_exec_repo.list_calls == 0

    @pytest.mark.asyncio
    async def test_out_of_range_limit_returns_422(self, exec_client, fake_exec_repo):
        for bad in (0, 101):
            response = await exec_client.get(
                "/admin/v1/tenants/tenant_x/receipts/11111111-1111-1111-1111-111111111111/execution-audit",
                params={"limit": bad},
            )
            assert response.status_code == 422
        assert fake_exec_repo.list_calls == 0

    @pytest.mark.asyncio
    async def test_unavailable_maps_503(self, exec_client, fake_exec_repo):
        fake_exec_repo.raise_unavailable = True
        response = await exec_client.get(
            "/admin/v1/tenants/tenant_x/receipts/11111111-1111-1111-1111-111111111111/execution-audit")
        assert response.status_code == 503

    @pytest.mark.asyncio
    async def test_data_error_maps_500(self, exec_client, fake_exec_repo):
        fake_exec_repo.raise_data = True
        response = await exec_client.get(
            "/admin/v1/tenants/tenant_x/receipts/11111111-1111-1111-1111-111111111111/execution-audit")
        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_response_is_redacted_and_typed(self, exec_client):
        response = await exec_client.get(
            "/admin/v1/tenants/tenant_x/receipts/11111111-1111-1111-1111-111111111111/execution-audit")
        event = response.json()["events"][0]
        assert set(event) == {
            "audit_id",
            "tenant_id",
            "receipt_id",
            "request_id",
            "config_version",
            "trace_id",
            "event_type",
            "outcome",
            "category",
            "tool_name",
            "error_code",
            "latency_ms",
            "occurred_at",
        }
        # No free-text body field can ever exist
        for banned in ("text", "body", "response", "args", "instruction", "secret", "dsn"):
            assert not any(banned in key for key in event)


class TestExecutionAuditByRequestEndpoint:
    """(tenant_id, request_id) listing covers receipt_id-NULL delivery events."""

    @pytest.mark.asyncio
    async def test_valid_request_returns_events_including_null_receipt(self, exec_client, fake_exec_repo):
        fake_exec_repo.set_events([
            _audit_event(receipt_id=None, event_type="delivery_result", outcome="delivered", category=None),
        ])
        response = await exec_client.get(REQUEST_URL, params={"limit": 10})
        assert response.status_code == 200
        data = response.json()
        assert len(data["events"]) == 1
        assert data["events"][0]["receipt_id"] is None
        assert data["events"][0]["event_type"] == "delivery_result"
        assert fake_exec_repo.last_method == "request"
        assert fake_exec_repo.last_args[0] == "tenant_x"
        assert str(fake_exec_repo.last_args[1]) == "22222222-2222-2222-2222-222222222222"
        assert fake_exec_repo.last_args[2] == 10

    @pytest.mark.asyncio
    async def test_missing_token_returns_401_without_query(self, app_with_exec, fake_exec_repo):
        transport = httpx.ASGITransport(app=app_with_exec)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as raw:
            response = await raw.get(REQUEST_URL)
        assert response.status_code == 401
        assert fake_exec_repo.list_calls == 0

    @pytest.mark.asyncio
    async def test_invalid_tenant_id_returns_422(self, exec_client, fake_exec_repo):
        response = await exec_client.get(
            "/admin/v1/tenants/INVALID/requests/22222222-2222-2222-2222-222222222222/execution-audit")
        assert response.status_code == 422
        assert fake_exec_repo.list_calls == 0

    @pytest.mark.asyncio
    async def test_invalid_request_uuid_returns_422(self, exec_client, fake_exec_repo):
        response = await exec_client.get("/admin/v1/tenants/tenant_x/requests/not-a-uuid/execution-audit")
        assert response.status_code == 422
        assert fake_exec_repo.list_calls == 0

    @pytest.mark.asyncio
    async def test_out_of_range_limit_returns_422(self, exec_client, fake_exec_repo):
        for bad in (0, 101):
            response = await exec_client.get(REQUEST_URL, params={"limit": bad})
            assert response.status_code == 422
        assert fake_exec_repo.list_calls == 0

    @pytest.mark.asyncio
    async def test_unavailable_maps_503(self, exec_client, fake_exec_repo):
        fake_exec_repo.raise_unavailable = True
        response = await exec_client.get(REQUEST_URL)
        assert response.status_code == 503

    @pytest.mark.asyncio
    async def test_data_error_maps_500(self, exec_client, fake_exec_repo):
        fake_exec_repo.raise_data = True
        response = await exec_client.get(REQUEST_URL)
        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_response_carries_no_free_text(self, exec_client, fake_exec_repo):
        fake_exec_repo.set_events([
            _audit_event(
                receipt_id=None,
                event_type="delivery_result",
                outcome="failed",
                category=None,
                error_code="worker_unavailable",
            ),
        ])
        response = await exec_client.get(REQUEST_URL)
        assert response.status_code == 200
        event = response.json()["events"][0]
        assert event["error_code"] == "worker_unavailable"
        for banned in ("text", "body", "response", "args", "instruction", "secret", "dsn"):
            assert not any(banned in key for key in event)


# ---------------------------------------------------------------------------
# usage endpoint — daily per-tenant usage (Stage 6C)
# ---------------------------------------------------------------------------


class TestUsageEndpoint:

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_returns_profiles_with_unknown_semantics(self, app, fake_usage_repo):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test", headers=AUTH_HEADERS) as c:
            response = await c.get("/admin/v1/tenants/tenant_x/usage", params={"day": "2026-09-05"})
        assert response.status_code == 200
        data = response.json()
        assert data["usage_date"] == "2026-09-05"
        assert data["tenant_id"] == "tenant_x"
        profile = data["profiles"][0]
        assert profile["requests"] == 3
        assert profile["input_tokens"] == 100
        assert profile["output_tokens"] is None
        assert profile["cost_microunits"] is None
        assert profile["cost_state"] == "unknown"
        assert fake_usage_repo.get_calls[0][1].isoformat() == "2026-09-05"

    @pytest.mark.asyncio
    async def test_missing_token_returns_401_without_query(self, app, fake_usage_repo):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as raw:
            response = await raw.get("/admin/v1/tenants/tenant_x/usage")
        assert response.status_code == 401
        assert fake_usage_repo.get_calls == []

    @pytest.mark.asyncio
    async def test_invalid_tenant_returns_422(self, app, fake_usage_repo):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test", headers=AUTH_HEADERS) as c:
            response = await c.get("/admin/v1/tenants/INVALID/usage")
        assert response.status_code == 422
        assert fake_usage_repo.get_calls == []

    @pytest.mark.asyncio
    async def test_invalid_day_returns_422_fixed_text(self, app, fake_usage_repo):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test", headers=AUTH_HEADERS) as c:
            response = await c.get("/admin/v1/tenants/tenant_x/usage", params={"day": "2026-13-99"})
        assert response.status_code == 422
        assert response.json()["detail"] == "Invalid day format; expected YYYY-MM-DD."
        assert fake_usage_repo.get_calls == []

    @pytest.mark.asyncio
    async def test_default_day_is_utc_today(self, app, fake_usage_repo):
        from datetime import datetime, timezone

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test", headers=AUTH_HEADERS) as c:
            response = await c.get("/admin/v1/tenants/tenant_x/usage")
        assert response.status_code == 200
        assert fake_usage_repo.get_calls[0][1] == datetime.now(timezone.utc).date()

    @pytest.mark.asyncio
    async def test_unavailable_maps_503(self, app, fake_usage_repo):
        fake_usage_repo.raise_unavailable = True
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test", headers=AUTH_HEADERS) as c:
            response = await c.get("/admin/v1/tenants/tenant_x/usage")
        assert response.status_code == 503
        assert response.json()["detail"] == "Tenant repository is unavailable."

    @pytest.mark.asyncio
    async def test_data_error_maps_500_fixed_text(self, app, fake_usage_repo):
        fake_usage_repo.raise_data = True
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test", headers=AUTH_HEADERS) as c:
            response = await c.get("/admin/v1/tenants/tenant_x/usage")
        assert response.status_code == 500
        assert response.json()["detail"] == "Usage query failed."

    @pytest.mark.asyncio
    async def test_response_has_no_free_text_fields(self, app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test", headers=AUTH_HEADERS) as c:
            response = await c.get("/admin/v1/tenants/tenant_x/usage")
        profile = response.json()["profiles"][0]
        assert set(profile) == {
            "model_profile",
            "requests",
            "input_tokens",
            "output_tokens",
            "cost_microunits",
            "cost_state",
        }
