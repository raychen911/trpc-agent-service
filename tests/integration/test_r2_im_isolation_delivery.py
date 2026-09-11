"""R2C PostgreSQL evidence for terminal IM delivery audit policy."""

from __future__ import annotations

import asyncio
import uuid
from unittest.mock import MagicMock

import pytest

from trpc_service.channels.delivery import ChannelExecutionStream
from trpc_service.gateway.channel_service import ChannelIngressService
from trpc_service.storage.database import DatabaseSettings, create_database_engine
from trpc_service.storage.execution_audit_repository import SqlExecutionAuditRepository
from trpc_service.transport.models import WorkerErrorCode, WorkerTask

from .pg_helpers import PostgreSQLContainer, requires_docker, run_alembic

pytestmark = requires_docker


@pytest.fixture(scope="module")
def r2c_pg():
    pg = PostgreSQLContainer(name_prefix="trpc-r2c-pg")
    pg.start()
    try:
        result = run_alembic(pg.url, "upgrade", "head")
        assert result.returncode == 0, result.stderr
        yield pg
    finally:
        pg.stop()


def _task(tenant_id: str) -> WorkerTask:
    return WorkerTask(
        protocol_version=1,
        request_id=uuid.uuid4(),
        tenant_id=tenant_id,
        app_id="app_demo",
        config_version=1,
        user_id="usr_v1_" + "a" * 48,
        channel="wecom",
        session_id="ses_v1_" + "a" * 48,
        message_id="message-1",
        message="hello",
    )


def test_terminal_delivery_is_unique_per_request_and_failures_policy_keeps_only_failure(r2c_pg):
    tenant_id = f"t{uuid.uuid4().hex[:12]}"

    async def empty():
        if False:
            yield None

    async def scenario() -> None:
        repository = SqlExecutionAuditRepository(create_database_engine(DatabaseSettings(url=r2c_pg.url)),
                                                 owns_engine=True)
        service = ChannelIngressService(MagicMock(), MagicMock(), execution_repository=repository)
        try:
            all_events = ChannelExecutionStream(empty())
            all_events.task = _task(tenant_id)
            await service.record_external_delivery(all_events, None)
            # A gateway-level duplicate call would be observable as two rows;
            # real SDK services make exactly one terminal call.
            rows = await repository.list_for_request(tenant_id, all_events.task.request_id, 10)
            assert len(rows) == 1
            assert rows[0].outcome == "delivered"

            failures_only = ChannelExecutionStream(empty())
            failures_only.task = _task(tenant_id)
            failures_only.delivery_events = "failures"
            await service.record_external_delivery(failures_only, None)
            assert await repository.list_for_request(tenant_id, failures_only.task.request_id, 10) == ()
            await service.record_external_delivery(failures_only, WorkerErrorCode.CHANNEL_DELIVERY_FAILED)
            rows = await repository.list_for_request(tenant_id, failures_only.task.request_id, 10)
            assert len(rows) == 1
            assert rows[0].outcome == "failed"
            assert rows[0].error_code == WorkerErrorCode.CHANNEL_DELIVERY_FAILED.value
        finally:
            await repository.close()

    asyncio.run(scenario())
