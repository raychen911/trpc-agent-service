"""PostgreSQL transaction primitives used by production repository adapters.

These operations deliberately make locking and conditional update predicates
visible.  They run only inside ``tenant_transaction`` so every model query has
both application-level ``TenantContext`` and database-enforced RLS scope.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.runtime.models import stable_id

from .context import TenantContext, require_tenant
from .models import BudgetAccount, Inbox, Outbox


class SqlOperationError(RuntimeError):
    """A PostgreSQL transaction primitive could not preserve its invariant."""


class SqlBudgetExceeded(SqlOperationError):
    """The conditional hard-budget update affected no account row."""


class PostgresOperations:
    @staticmethod
    async def resolve_binding(
        session: AsyncSession, *, webhook_key_hash: str, provider: str
    ) -> tuple[str, str] | None:
        """Call the protected locator before a tenant RLS context exists."""

        row = (
            await session.execute(
                text(
                    "SELECT resolved_tenant_id, resolved_binding_id "
                    "FROM app_security.resolve_binding(:key_hash, :provider)"
                ),
                {"key_hash": webhook_key_hash, "provider": provider},
            )
        ).first()
        return (str(row[0]), str(row[1])) if row else None

    @staticmethod
    async def claim_outbox(
        session: AsyncSession,
        context: TenantContext,
        *,
        owner: str,
        limit: int = 100,
        lease_seconds: int = 30,
    ) -> list[Outbox]:
        """Lease pending records using the PostgreSQL SKIP LOCKED work pattern."""

        context = require_tenant(context)
        now = datetime.now(UTC)
        statement = (
            select(Outbox)
            .where(
                Outbox.tenant_id == context.tenant_id,
                Outbox.available_at <= now,
                Outbox.status.in_(("pending", "processing")),
            )
            .where(
                (Outbox.status == "pending")
                | ((Outbox.status == "processing") & (Outbox.lease_expires_at <= now))
            )
            .order_by(Outbox.available_at, Outbox.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        rows = list((await session.scalars(statement)).all())
        expiry = now + timedelta(seconds=lease_seconds)
        for row in rows:
            row.status = "processing"
            row.lease_owner = owner
            row.lease_expires_at = expiry
            row.attempts += 1
        await session.flush()
        return rows

    @staticmethod
    async def reserve_hard_budget(
        session: AsyncSession,
        context: TenantContext,
        *,
        budget_name: str,
        period_start: datetime,
        estimate: int,
    ) -> None:
        """Atomically reserve units; never replace this with a cache decrement."""

        context = require_tenant(context)
        if estimate < 0:
            raise ValueError("budget estimate cannot be negative")
        result = await session.execute(
            update(BudgetAccount)
            .where(
                BudgetAccount.tenant_id == context.tenant_id,
                BudgetAccount.budget_name == budget_name,
                BudgetAccount.period_start == period_start,
                BudgetAccount.spent_units + BudgetAccount.reserved_units + estimate
                <= BudgetAccount.limit_units,
            )
            .values(
                reserved_units=BudgetAccount.reserved_units + estimate,
                version=BudgetAccount.version + 1,
                updated_at=datetime.now(UTC),
            )
        )
        if result.rowcount != 1:
            raise SqlBudgetExceeded(
                "hard-budget fact store rejected or could not reserve the request"
            )

    @staticmethod
    async def insert_inbox_and_dispatch_outbox(
        session: AsyncSession,
        context: TenantContext,
        *,
        channel_binding_id: str,
        agent_id: str,
        session_id: str,
        config_version: int,
        idempotency_key: str,
        external_message_id: str | None,
        subject_id: str | None,
        payload: dict[str, Any],
        request_id: str,
        trace_id: str,
    ) -> tuple[str, bool]:
        """Insert deterministic Inbox + inbound Outbox in the caller's one SQL transaction."""

        context = require_tenant(context)
        inbox_id = stable_id("inb", context.tenant_id, channel_binding_id, idempotency_key)
        existing = await session.scalar(
            select(Inbox.inbox_id).where(
                Inbox.tenant_id == context.tenant_id, Inbox.idempotency_key == idempotency_key
            )
        )
        if existing:
            return str(existing), True
        now = datetime.now(UTC)
        inbox = Inbox(
            tenant_id=context.tenant_id,
            inbox_id=inbox_id,
            channel_binding_id=channel_binding_id,
            agent_id=agent_id,
            session_id=session_id,
            config_version=config_version,
            subject_id=subject_id,
            idempotency_key=idempotency_key,
            external_message_id=external_message_id,
            payload=payload,
            payload_hash=hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
            ).hexdigest(),
            status="queued",
            request_id=request_id,
            trace_id=trace_id,
            received_at=now,
            updated_at=now,
        )
        session.add(inbox)
        outbox_id = stable_id("obx", context.tenant_id, "inbound.dispatch", inbox_id)
        session.add(
            Outbox(
                tenant_id=context.tenant_id,
                outbox_id=outbox_id,
                aggregate_type="inbox",
                aggregate_id=inbox_id,
                inbox_id=inbox_id,
                event_type="inbound.dispatch",
                payload={"inbox_id": inbox_id, "session_id": session_id, "request_id": request_id},
                idempotency_key=f"inbound:{inbox_id}",
                trace_id=trace_id,
                status="pending",
                attempts=0,
                available_at=now,
                created_at=now,
            )
        )
        await session.flush()
        return inbox_id, False
