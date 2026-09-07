from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import update
from trpc_agent_sdk.context import new_agent_context
from trpc_agent_sdk.filter import FilterResult

from tenant_agent.governance.filters import TenantToolGovernanceFilter
from tenant_agent.governance.policies import (
    BudgetExceeded,
    ConfirmationManager,
    GovernanceService,
    PolicyDenied,
    confirmation_tokens,
    estimate_input_tokens,
)
from tenant_agent.models import (
    Attachment,
    BudgetPolicy,
    ChatType,
    ModelConfig,
    SecretRef,
    TenantStatus,
    UsageDelta,
    UserAccessPolicy,
)
from tenant_agent.security import Redactor
from tenant_agent.storage import schema
from tenant_agent.storage.base import TenantDataPlane
from tenant_agent.storage.memory import InMemoryPlane
from tenant_agent.storage.sql import SqlPlane
from tests.helpers import make_envelope, make_tenant


def data_plane(plane: InMemoryPlane) -> TenantDataPlane:
    return TenantDataPlane(
        sessions=plane,
        memories=plane,
        summaries=plane,
        artifacts=plane,
        knowledge=plane,
        audit=plane,
        receipts=plane,
        usage=plane,
        concurrency=plane,
        outbox=plane,
        leases=plane,
    )


async def authorize_input(
    service: GovernanceService,
    tenant: object,
    envelope: object,
    plane: TenantDataPlane,
) -> str:
    from tenant_agent.models import InboundEnvelope, TenantConfig

    assert isinstance(tenant, TenantConfig)
    assert isinstance(envelope, InboundEnvelope)
    return await service.authorize_input(
        tenant,
        envelope,
        plane,
        usage_period=datetime.now(UTC).strftime("%Y-%m"),
        reservation_id=f"test-{uuid.uuid4().hex}",
        reservation_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )


@pytest.mark.asyncio
async def test_user_acl_and_monthly_budget_are_shared_policy_decisions() -> None:
    plane = InMemoryPlane()
    tenant = make_tenant()
    denied_governance = tenant.governance.model_copy(
        update={"users": UserAccessPolicy(deny_users=frozenset({"blocked"}))}
    )
    denied_tenant = tenant.model_copy(update={"governance": denied_governance})
    service = GovernanceService(Redactor())
    with pytest.raises(PolicyDenied) as denied:
        await authorize_input(
            service,
            denied_tenant,
            make_envelope(denied_tenant, user_id="blocked"),
            data_plane(plane),
        )
    assert denied.value.decision == "user_denied"

    tiny_budget = tenant.governance.model_copy(
        update={
            "budget": BudgetPolicy(
                monthly_tokens=1,
                monthly_cost_usd=1,
                max_tokens_per_request=128,
                max_concurrent_sessions=1,
            )
        }
    )
    budget_tenant = tenant.model_copy(update={"governance": tiny_budget})
    with pytest.raises(BudgetExceeded):
        await authorize_input(
            service,
            budget_tenant,
            make_envelope(budget_tenant, text="this is larger than one token"),
            data_plane(plane),
        )

    attachment_input = await authorize_input(
        service,
        tenant,
        make_envelope(tenant, text="").model_copy(
            update={
                "attachments": (
                    Attachment(
                        kind="file",
                        external_id="platform-secret-file-id",
                        filename="report.pdf",
                        mime_type="application/pdf",
                        size_bytes=42,
                        download_url="https://private.example/file",
                    ),
                )
            }
        ),
        data_plane(plane),
    )
    assert "report.pdf" in attachment_input
    assert "application/pdf" in attachment_input
    assert "platform-secret-file-id" not in attachment_input
    assert "private.example" not in attachment_input


@pytest.mark.asyncio
async def test_budget_admission_atomically_reserves_tokens_and_worst_case_cost() -> None:
    plane = InMemoryPlane()
    service = GovernanceService(Redactor())
    base = make_tenant()
    token_limited = base.model_copy(
        update={
            "governance": base.governance.model_copy(
                update={"budget": base.governance.budget.model_copy(update={"monthly_tokens": 200_000})}
            )
        }
    )
    period = datetime.now(UTC).strftime("%Y-%m")

    async def reserve(index: int) -> bool:
        try:
            await service.authorize_input(
                token_limited,
                make_envelope(token_limited, text="x", message_id=f"budget-{index}"),
                data_plane(plane),
                usage_period=period,
                reservation_id=f"reservation-{index}",
                reservation_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            )
        except BudgetExceeded:
            return False
        return True

    assert sum(await asyncio.gather(reserve(1), reserve(2))) == 1
    assert len(plane._usage_reservations) == 1  # type: ignore[attr-defined]
    stored_reservation = next(iter(plane._usage_reservations.values()))  # type: ignore[attr-defined]
    profile = token_limited.models["offline"]
    assert stored_reservation.reserved_tokens == (profile.context_window_tokens + profile.max_output_tokens)

    priced_model = base.models["offline"].model_copy(
        update={
            "max_output_tokens": 100,
            "input_cost_per_million": 100.0,
            "output_cost_per_million": 100.0,
        }
    )
    cost_limited = base.model_copy(
        update={
            "models": {"offline": priced_model},
            "governance": base.governance.model_copy(
                update={"budget": base.governance.budget.model_copy(update={"monthly_cost_usd": 0.005})}
            ),
        }
    )
    with pytest.raises(BudgetExceeded, match="monthly_cost_budget"):
        await service.authorize_input(
            cost_limited,
            make_envelope(cost_limited, text="x", message_id="cost-budget"),
            data_plane(InMemoryPlane()),
            usage_period=period,
            reservation_id="cost-reservation",
            reservation_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )

    remote_plane = InMemoryPlane()
    remote_model = ModelConfig(
        provider="litellm",
        model_name="openai/test-model",
        api_key_ref=SecretRef(uri="env://TENANT_ALPHA_MODEL_KEY"),
        context_window_tokens=2_000,
        max_output_tokens=500,
        retry_count=2,
    )
    remote = base.model_copy(
        update={
            "models": {"offline": remote_model},
            "governance": base.governance.model_copy(
                update={"budget": base.governance.budget.model_copy(update={"monthly_tokens": 1_000_000})}
            ),
        }
    )
    await service.authorize_input(
        remote,
        make_envelope(remote, text="x", message_id="remote-bound"),
        data_plane(remote_plane),
        usage_period=period,
        reservation_id="remote-reservation",
        reservation_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    remote_reservation = next(iter(remote_plane._usage_reservations.values()))  # type: ignore[attr-defined]
    expected_attempts = remote.governance.budget.max_llm_calls_per_request * 3
    assert remote_reservation.reserved_tokens == expected_attempts * (2_000 + 500)


@pytest.mark.asyncio
async def test_dangerous_tool_filter_requires_exact_one_use_confirmation() -> None:
    plane = InMemoryPlane()
    tenant = make_tenant(tools=frozenset({"fetch_url"}), dangerous=frozenset({"fetch_url"}))
    governance = GovernanceService(Redactor())
    confirmations = ConfirmationManager(b"a-very-long-confirmation-key", plane)
    tool_filter = TenantToolGovernanceFilter(
        tenant=tenant,
        app_id="assistant",
        tool_name="fetch_url",
        governance=governance,
        confirmations=confirmations,
        audit=plane,
    )
    metadata = {
        "session_id": "session",
        "user_id": "user",
        "channel": "web",
        "confirmation_tokens": (),
    }
    context = new_agent_context(metadata=metadata)

    async def handle() -> FilterResult:
        return FilterResult(rsp={"ok": True})

    first = await tool_filter.run(context, {"url": "https://example.com"}, handle)
    assert first.rsp["error"] == "confirmation_required"
    token = first.rsp["confirmation_token"]

    approved_context = new_agent_context(metadata={**metadata, "confirmation_tokens": (token,)})
    approved = await tool_filter.run(approved_context, {"url": "https://example.com"}, handle)
    assert approved.rsp == {"ok": True}

    replay = await tool_filter.run(approved_context, {"url": "https://example.com"}, handle)
    assert replay.rsp["error"] == "confirmation_required"
    audit = await plane.query_audit("alpha")
    assert {row.decision for row in audit} >= {"confirm", "confirmed"}


@pytest.mark.asyncio
async def test_sql_confirmation_stays_one_use_after_receipt_lease_expiry(tmp_path: Path) -> None:
    plane = SqlPlane(f"sqlite+aiosqlite:///{(tmp_path / 'confirmation.db').as_posix()}")
    await plane.initialize()
    confirmations = ConfirmationManager(b"a-very-long-confirmation-key", plane)
    arguments = {"url": "https://example.com"}
    token = confirmations.issue(
        tenant_id="alpha",
        user_id="user",
        session_id="session",
        tool_name="fetch_url",
        args=arguments,
        ttl_seconds=300,
    )
    assert await confirmations.consume(
        token,
        tenant_id="alpha",
        user_id="user",
        session_id="session",
        tool_name="fetch_url",
        args=arguments,
    )
    async with plane.engine.begin() as connection:
        await connection.execute(
            update(schema.inbound_receipts)
            .where(schema.inbound_receipts.c.tenant_id == "alpha")
            .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    assert not await confirmations.consume(
        token,
        tenant_id="alpha",
        user_id="user",
        session_id="session",
        tool_name="fetch_url",
        args=arguments,
    )
    await plane.close()


@pytest.mark.asyncio
async def test_tool_error_classification_and_audit_failure_do_not_change_result() -> None:
    plane = InMemoryPlane()
    tenant = make_tenant(tools=frozenset({"calculator"}))
    governance = GovernanceService(Redactor())
    confirmations = ConfirmationManager(b"a-very-long-confirmation-key", plane)
    context = new_agent_context(
        metadata={
            "session_id": "session",
            "user_id": "user",
            "channel": "web",
            "confirmation_tokens": (),
        }
    )

    class FailingAudit:
        async def append_audit(self, record: object) -> None:
            del record
            raise RuntimeError("audit unavailable")

    audit_safe_filter = TenantToolGovernanceFilter(
        tenant=tenant,
        app_id="assistant",
        tool_name="calculator",
        governance=governance,
        confirmations=confirmations,
        audit=FailingAudit(),  # type: ignore[arg-type]
    )

    async def successful_handle() -> FilterResult:
        return FilterResult(rsp={"result": 4})

    successful = await audit_safe_filter.run(
        context,
        {"expression": "2+2"},
        successful_handle,
    )
    assert successful.rsp == {"result": 4}

    disabled_tenant = tenant.model_copy(update={"audit": tenant.audit.model_copy(update={"enabled": False})})
    disabled_filter = TenantToolGovernanceFilter(
        tenant=disabled_tenant,
        app_id="assistant",
        tool_name="calculator",
        governance=governance,
        confirmations=confirmations,
        audit=plane,
    )
    before = len(await plane.query_audit("alpha"))
    await disabled_filter.run(context, {"expression": "2+2"}, successful_handle)
    assert len(await plane.query_audit("alpha")) == before

    error_filter = TenantToolGovernanceFilter(
        tenant=tenant,
        app_id="assistant",
        tool_name="calculator",
        governance=governance,
        confirmations=confirmations,
        audit=plane,
    )

    async def failed_handle() -> FilterResult:
        return FilterResult(
            error=ValueError("tool failed"),
            is_continue=False,
        )

    failed = await error_filter.run(
        context,
        {"expression": "bad"},
        failed_handle,
    )
    assert isinstance(failed.error, ValueError)
    audit = await plane.query_audit("alpha")
    assert audit[0].decision == "tool_error"
    assert audit[0].error_type == "ValueError"


@pytest.mark.asyncio
async def test_all_input_policy_boundaries_and_pre_model_redaction() -> None:
    plane = InMemoryPlane()
    service = GovernanceService(Redactor())
    tenant = make_tenant()

    suspended = tenant.model_copy(update={"status": TenantStatus.SUSPENDED})
    with pytest.raises(PolicyDenied, match="tenant_suspended"):
        await authorize_input(service, suspended, make_envelope(suspended), data_plane(plane))

    disabled_apps = {"assistant": tenant.apps["assistant"].model_copy(update={"enabled": False})}
    disabled = tenant.model_copy(update={"apps": disabled_apps})
    with pytest.raises(PolicyDenied, match="app_disabled"):
        await authorize_input(service, disabled, make_envelope(disabled), data_plane(plane))

    allow_only = tenant.governance.model_copy(
        update={"users": UserAccessPolicy(allow_users=frozenset({"allowed"}))}
    )
    allow_tenant = tenant.model_copy(update={"governance": allow_only})
    with pytest.raises(PolicyDenied, match="user_not_allowed"):
        await authorize_input(
            service, allow_tenant, make_envelope(allow_tenant, user_id="other"), data_plane(plane)
        )

    group_only = tenant.governance.model_copy(
        update={"users": UserAccessPolicy(allow_groups=frozenset({"approved-group"}))}
    )
    group_tenant = tenant.model_copy(update={"governance": group_only})
    with pytest.raises(PolicyDenied, match="group_not_allowed"):
        await authorize_input(
            service,
            group_tenant,
            make_envelope(
                group_tenant,
                chat_type=ChatType.GROUP,
                chat_id="unapproved-group",
            ),
            data_plane(plane),
        )

    short_request = tenant.governance.model_copy(
        update={"budget": tenant.governance.budget.model_copy(update={"max_tokens_per_request": 128})}
    )
    request_tenant = tenant.model_copy(update={"governance": short_request})
    with pytest.raises(BudgetExceeded, match="request_token_limit"):
        await authorize_input(
            service,
            request_tenant,
            make_envelope(request_tenant, text="x" * 600),
            data_plane(plane),
        )

    period = datetime.now(UTC).strftime("%Y-%m")
    await plane.add_usage("alpha", period, UsageDelta(cost_usd=101))
    with pytest.raises(BudgetExceeded, match="monthly_cost_budget"):
        await authorize_input(service, tenant, make_envelope(tenant), data_plane(plane))

    redact_policy = tenant.governance.redaction.model_copy(update={"redact_before_model": True})
    redacted_tenant = tenant.model_copy(
        update={
            "governance": tenant.governance.model_copy(
                update={
                    "redaction": redact_policy,
                    "budget": tenant.governance.budget.model_copy(update={"monthly_cost_usd": 1_000}),
                }
            )
        }
    )
    effective = await authorize_input(
        service,
        redacted_tenant,
        make_envelope(redacted_tenant, text="alice@example.com"),
        data_plane(plane),
    )
    assert effective == "[REDACTED]"
    assert estimate_input_tokens("中" * 200) == 200
    cjk_budget = redacted_tenant.governance.model_copy(
        update={
            "budget": redacted_tenant.governance.budget.model_copy(update={"max_tokens_per_request": 128})
        }
    )
    cjk_tenant = redacted_tenant.model_copy(update={"governance": cjk_budget})
    with pytest.raises(BudgetExceeded, match="request_token_limit"):
        await authorize_input(
            service,
            cjk_tenant,
            make_envelope(cjk_tenant, text="中" * 200),
            data_plane(InMemoryPlane()),
        )


@pytest.mark.asyncio
async def test_tool_decisions_and_confirmation_invalid_tokens() -> None:
    tenant = make_tenant(
        tools=frozenset({"calculator", "fetch_url"}),
        dangerous=frozenset({"fetch_url"}),
    )
    assert GovernanceService.authorize_tool(tenant, "assistant", "calculator") == "allow"
    assert GovernanceService.authorize_tool(tenant, "assistant", "fetch_url") == "confirm"
    assert GovernanceService.authorize_tool(tenant, "assistant", "unknown") == "deny"
    denied_policy = tenant.governance.tools.model_copy(
        update={"allow": frozenset({"fetch_url"}), "deny": frozenset({"calculator"})}
    )
    denied = tenant.model_copy(
        update={"governance": tenant.governance.model_copy(update={"tools": denied_policy})}
    )
    assert GovernanceService.authorize_tool(denied, "assistant", "calculator") == "deny"

    plane = InMemoryPlane()
    manager = ConfirmationManager(b"a-very-long-confirmation-key", plane)
    assert manager.inspect("not-a-token") is None
    assert manager.inspect("bad.payload") is None
    expired = manager.issue(
        tenant_id="alpha",
        user_id="user",
        session_id="session",
        tool_name="fetch_url",
        args={"url": "https://example.com"},
        ttl_seconds=-1,
    )
    assert manager.inspect(expired) is None
    valid = manager.issue(
        tenant_id="alpha",
        user_id="user",
        session_id="session",
        tool_name="fetch_url",
        args={"url": "https://example.com"},
        ttl_seconds=60,
    )
    assert not await manager.consume(
        valid,
        tenant_id="alpha",
        user_id="user",
        session_id="session",
        tool_name="fetch_url",
        args={"url": "https://different.example"},
    )
    assert confirmation_tokens("hello") == ()
    assert confirmation_tokens("/confirm   ") == ()
    assert confirmation_tokens(f"/confirm {valid} extra") == (valid,)
