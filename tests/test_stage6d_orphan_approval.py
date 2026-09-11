"""Stage 6D orphaned-approval disposition (unit).

Contracts pinned here — an ``executing`` approval whose controlled execution
died with its Worker must be dispositionable ONLY through the Admin API, and
ONLY to terminal ``failed``:

* the disposition is atomic, auditable, idempotent (second call replays the
  same terminal fact and rewrites nothing);
* it NEVER re-invokes the approved tool or resumes the turn (there is no
  execution surface at all — repository + Admin query/terminate only);
* active (younger than the operator-declared stale threshold), ``pending``,
  inconsistent (no processing decision receipt) or wrong-tenant records are
  rejected;
* Admin responses expose opaque metadata only: tool args, response text,
  user/session raw identities never appear.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timezone

import httpx
import pytest
import pytest_asyncio

from tests.tenant_helpers import make_audit_policy, make_backend_profile, make_governance
from trpc_service.admin.app import create_admin_app
from trpc_service.config.tenant import AgentAppConfig, TenantConfig
from trpc_service.governance.approval import (
    OrphanTermination,
    OrphanTerminationAction,
    OrphanedApproval,
)
from trpc_service.storage.approval_repository import (
    ToolApprovalRepository,
    ToolApprovalRepositoryUnavailableError,
)

TOKEN = "a" * 48
AUTH_HEADERS = {"X-TRPC-Admin-Token": TOKEN}

STALE_SECONDS = 3600


def _config(tenant_id: str = "tenant_x") -> TenantConfig:
    return TenantConfig(
        tenant_id=tenant_id,
        enabled=True,
        version=1,
        app=AgentAppConfig(
            app_id="app_a",
            instruction="i",
            model_profile="default",
            allowed_tools=[],
        ),
        governance=make_governance(),
        backend_profile=make_backend_profile(),
        audit_policy=make_audit_policy(),
    )


class _FakeAdminRepository:

    def __init__(self) -> None:
        self._head = _config()

    async def get(self, tenant_id: str):
        return self._head if tenant_id == self._head.tenant_id else None

    async def list_versions(self, tenant_id, *, before_version=None, limit=50):
        return [self._head]

    async def create(self, config):
        raise AssertionError("unused")

    async def update(self, tenant_id, expected_version, draft):
        raise AssertionError("unused")

    async def rollback(self, tenant_id, expected_version, target_version):
        raise AssertionError("unused")

    async def check_ready(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _NoopReceiptRepo:

    async def list_audit(self, tenant_id, message_id, limit):
        return ()

    async def check_ready(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _NoopExecRepo:

    async def list_for_receipt(self, tenant_id, receipt_id, limit):
        return ()

    async def list_for_request(self, tenant_id, request_id, limit):
        return ()

    async def append(self, event):
        raise AssertionError("unused")

    async def check_ready(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _NoopUsageRepo:

    async def get_daily(self, tenant_id, day):
        raise AssertionError("unused")

    async def check_ready(self) -> None:
        pass

    async def close(self) -> None:
        pass


def _orphan(approval_id: uuid.UUID | None = None) -> OrphanedApproval:
    return OrphanedApproval(
        approval_id=approval_id or uuid.uuid4(),
        tenant_id="tenant_x",
        session_id="ses_v1_" + "b" * 48,
        function_call_id="call-1",
        tool_name="get_current_time",
        args_digest="0" * 64,
        decision="approve",
        state="executing",
        decided_at=datetime(2026, 9, 5, 12, 0, tzinfo=timezone.utc),
        age_seconds=7200,
    )


class FakeOrphanRepository:
    """Scriptable double for the two new repository methods."""

    def __init__(self) -> None:
        self.orphans: tuple[OrphanedApproval, ...] = ()
        self.terminate_result: OrphanTermination | None = None
        self.list_calls: list[dict] = []
        self.terminate_calls: list[dict] = []
        self.raise_unavailable = False
        self.closed = 0

    async def list_stale_executing(self, *, tenant_id: str, stale_seconds: int, limit: int):
        self.list_calls.append({"tenant_id": tenant_id, "stale_seconds": stale_seconds, "limit": limit})
        if self.raise_unavailable:
            raise ToolApprovalRepositoryUnavailableError("database is not reachable")
        return self.orphans

    async def terminate_orphan(self, approval_id: uuid.UUID, *, tenant_id: str, stale_seconds: int):
        self.terminate_calls.append({
            "approval_id": approval_id,
            "tenant_id": tenant_id,
            "stale_seconds": stale_seconds,
        })
        if self.raise_unavailable:
            raise ToolApprovalRepositoryUnavailableError("database is not reachable")
        assert self.terminate_result is not None, "test must script a result"
        return self.terminate_result

    async def check_ready(self) -> None:
        pass

    async def close(self) -> None:
        self.closed += 1


def _app(approval_repo: FakeOrphanRepository | None = None):
    return create_admin_app(
        tenant_repository=_FakeAdminRepository(),
        message_repository=_NoopReceiptRepo(),
        execution_repository=_NoopExecRepo(),
        usage_repository=_NoopUsageRepo(),
        approval_repository=approval_repo if approval_repo is not None else FakeOrphanRepository(),
        environ={"TRPC_ADMIN_TOKEN": TOKEN},
    )


@pytest_asyncio.fixture
async def approval_repo() -> FakeOrphanRepository:
    return FakeOrphanRepository()


@pytest_asyncio.fixture
async def client(approval_repo: FakeOrphanRepository) -> AsyncIterator[httpx.AsyncClient]:
    app = _app(approval_repo)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test", headers=AUTH_HEADERS) as ac:
        yield ac


# ------------------------------------------------------------------ protocol


def test_repository_protocol_grows_disposition_surface():
    assert hasattr(ToolApprovalRepository, "list_stale_executing")
    assert hasattr(ToolApprovalRepository, "terminate_orphan")


def test_sql_repository_implements_surface():
    from trpc_service.storage.approval_repository import SqlToolApprovalRepository

    assert callable(SqlToolApprovalRepository.list_stale_executing)
    assert callable(SqlToolApprovalRepository.terminate_orphan)


# ---------------------------------------------------------------- query route


@pytest.mark.asyncio
async def test_list_orphaned_returns_opaque_candidates(client: httpx.AsyncClient, approval_repo: FakeOrphanRepository):
    orphan = _orphan()
    approval_repo.orphans = (orphan, )
    response = await client.get(
        "/admin/v1/tenants/tenant_x/approvals/orphaned",
        params={
            "stale_seconds": STALE_SECONDS,
            "limit": 25
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["approvals"][0]["approval_id"] == str(orphan.approval_id)
    assert body["approvals"][0]["state"] == "executing"
    assert body["approvals"][0]["age_seconds"] == 7200
    assert approval_repo.list_calls == [{
        "tenant_id": "tenant_x",
        "stale_seconds": STALE_SECONDS,
        "limit": 25,
    }]
    # Opaque only: no tool args, no response text, no user identifier.
    text = response.text
    assert "tool_args" not in text
    assert "response_text" not in text
    assert "user_id" not in text


@pytest.mark.asyncio
async def test_list_orphaned_rejects_unknown_tenant_id(client: httpx.AsyncClient, approval_repo: FakeOrphanRepository):
    response = await client.get("/admin/v1/tenants/BAD TENANT/approvals/orphaned")
    assert response.status_code == 422
    assert approval_repo.list_calls == []


@pytest.mark.asyncio
async def test_list_orphaned_requires_token(approval_repo: FakeOrphanRepository):
    app = _app(approval_repo)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as raw:
        response = await raw.get("/admin/v1/tenants/tenant_x/approvals/orphaned")
    assert response.status_code == 401
    assert response.json()["detail"] == "Admin authentication failed."


@pytest.mark.asyncio
async def test_list_orphaned_backend_down_maps_503(client: httpx.AsyncClient, approval_repo: FakeOrphanRepository):
    approval_repo.raise_unavailable = True
    response = await client.get("/admin/v1/tenants/tenant_x/approvals/orphaned")
    assert response.status_code == 503
    assert response.json()["detail"] == "Tenant repository is unavailable."


@pytest.mark.asyncio
async def test_list_orphaned_rejects_bad_stale_seconds(client: httpx.AsyncClient, approval_repo: FakeOrphanRepository):
    response = await client.get(
        "/admin/v1/tenants/tenant_x/approvals/orphaned",
        params={"stale_seconds": 0},
    )
    assert response.status_code == 422
    assert approval_repo.list_calls == []


# ------------------------------------------------------------- terminate route


@pytest.mark.asyncio
async def test_terminate_first_call_succeeds(client: httpx.AsyncClient, approval_repo: FakeOrphanRepository):
    approval_id = uuid.uuid4()
    approval_repo.terminate_result = OrphanTermination(
        action=OrphanTerminationAction.TERMINATED,
        approval_id=approval_id,
        state="failed",
    )
    response = await client.post(
        f"/admin/v1/tenants/tenant_x/approvals/{approval_id}/terminate",
        params={"stale_seconds": STALE_SECONDS},
    )
    assert response.status_code == 200
    assert response.json() == {
        "approval_id": str(approval_id),
        "disposition": "terminated",
        "state": "failed",
    }
    assert approval_repo.terminate_calls == [{
        "approval_id": approval_id,
        "tenant_id": "tenant_x",
        "stale_seconds": STALE_SECONDS,
    }]


@pytest.mark.asyncio
async def test_terminate_second_call_is_idempotent_replay(client: httpx.AsyncClient,
                                                          approval_repo: FakeOrphanRepository):
    approval_id = uuid.uuid4()
    approval_repo.terminate_result = OrphanTermination(
        action=OrphanTerminationAction.ALREADY_TERMINATED,
        approval_id=approval_id,
        state="failed",
    )
    response = await client.post(
        f"/admin/v1/tenants/tenant_x/approvals/{approval_id}/terminate",
        params={"stale_seconds": STALE_SECONDS},
    )
    assert response.status_code == 200
    assert response.json()["disposition"] == "already_terminated"
    assert response.json()["state"] == "failed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "action",
    [
        OrphanTerminationAction.ACTIVE,
        OrphanTerminationAction.PENDING,
        OrphanTerminationAction.INCONSISTENT,
    ],
)
async def test_terminate_rejects_active_pending_inconsistent(client: httpx.AsyncClient,
                                                             approval_repo: FakeOrphanRepository, action):
    approval_repo.terminate_result = OrphanTermination(action=action, approval_id=uuid.uuid4(), state="executing")
    response = await client.post(
        f"/admin/v1/tenants/tenant_x/approvals/{uuid.uuid4()}/terminate",
        params={"stale_seconds": STALE_SECONDS},
    )
    assert response.status_code == 409
    assert response.json()["detail"] == "Approval disposition rejected."


@pytest.mark.asyncio
async def test_terminate_not_available_maps_404(client: httpx.AsyncClient, approval_repo: FakeOrphanRepository):
    approval_repo.terminate_result = OrphanTermination(
        action=OrphanTerminationAction.NOT_AVAILABLE,
        approval_id=None,
        state=None,
    )
    response = await client.post(
        f"/admin/v1/tenants/tenant_x/approvals/{uuid.uuid4()}/terminate",
        params={"stale_seconds": STALE_SECONDS},
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "Approval not found."


@pytest.mark.asyncio
async def test_terminate_backend_down_maps_503(client: httpx.AsyncClient, approval_repo: FakeOrphanRepository):
    approval_repo.raise_unavailable = True
    response = await client.post(
        f"/admin/v1/tenants/tenant_x/approvals/{uuid.uuid4()}/terminate",
        params={"stale_seconds": STALE_SECONDS},
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "Tenant repository is unavailable."


@pytest.mark.asyncio
async def test_terminate_requires_token(approval_repo: FakeOrphanRepository):
    app = _app(approval_repo)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as raw:
        response = await raw.post(f"/admin/v1/tenants/tenant_x/approvals/{uuid.uuid4()}/terminate")
    assert response.status_code == 401
    assert approval_repo.terminate_calls == []


@pytest.mark.asyncio
async def test_terminate_defaults_stale_seconds(client: httpx.AsyncClient, approval_repo: FakeOrphanRepository):
    """No explicit threshold -> the documented default (900s) is enforced."""
    approval_repo.terminate_result = OrphanTermination(
        action=OrphanTerminationAction.TERMINATED,
        approval_id=uuid.uuid4(),
        state="failed",
    )
    response = await client.post(f"/admin/v1/tenants/tenant_x/approvals/{uuid.uuid4()}/terminate")
    assert response.status_code == 200
    assert approval_repo.terminate_calls[0]["stale_seconds"] == 900


# ------------------------------------------------------------- factory wiring


def test_factory_builds_approval_repository_from_env(monkeypatch: pytest.MonkeyPatch):
    import trpc_service.admin.app as admin_app

    created: dict[str, object] = {}

    def _fake_sql_repo(engine: object, *, owns_engine: bool):
        created["engine"] = engine
        created["owns_engine"] = owns_engine
        return FakeOrphanRepository()

    monkeypatch.setattr(admin_app, "SqlToolApprovalRepository", _fake_sql_repo)
    application = admin_app.create_admin_app(environ={
        "TRPC_ADMIN_TOKEN": TOKEN,
        "TRPC_DATABASE_URL": "postgresql+asyncpg://u:p@127.0.0.1:1/db",
    })
    assert created["owns_engine"] is False
    assert application.state.approval_repository is not None


def test_injection_contract_still_rejects_half_injection():
    """The four core repositories keep the all-or-none rule; approval is
    the additional optional reader/writer and half-core raises the fixed
    configuration error."""
    with pytest.raises(ValueError, match="injected together or not at all"):
        create_admin_app(
            tenant_repository=_FakeAdminRepository(),
            environ={"TRPC_ADMIN_TOKEN": TOKEN},
        )
