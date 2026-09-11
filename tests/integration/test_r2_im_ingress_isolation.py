"""R2B evidence: persisted IM bindings isolate identity and Redis ordering."""

from __future__ import annotations

import asyncio
import socket
import subprocess
import uuid

import pytest

from trpc_service.channels.binding import ChannelBinding
from trpc_service.channels.identity import project_identity
from trpc_service.channels.models import UnboundChannelMessage
from trpc_service.channels.order_gate import RedisChannelOrderGate
from trpc_service.channels.policy import bind_message
from trpc_service.storage.channel_binding_repository import SqlChannelBindingRepository
from trpc_service.storage.database import create_database_engine
from trpc_service.storage.database import DatabaseSettings

from .pg_helpers import PostgreSQLContainer, requires_docker, run_alembic

pytestmark = requires_docker

_GOVERNANCE = ('{"allowed_channels":["wecom","feishu"],"allowed_user_ids":[],"tool_decisions":{},'
               '"content_policy":{"enabled":false,"input_action":"block","output_action":"block"},'
               '"limits":null}')
_PROFILE = '{"state_backend":"redis","artifact_backend":"s3","knowledge_backend":"sql","audit_backend":"sql"}'
_POLICY = '{"retention_days":365,"delivery_events":"all"}'


def _port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def r2b_pg():
    pg = PostgreSQLContainer(name_prefix="trpc-r2b-pg")
    pg.start()
    try:
        result = run_alembic(pg.url, "upgrade", "head")
        assert result.returncode == 0, result.stderr
        yield pg
    finally:
        pg.stop()


@pytest.fixture(scope="module")
def r2b_redis_url():
    name = f"trpc-r2b-redis-{uuid.uuid4().hex[:8]}"
    port = _port()
    subprocess.run(["docker", "run", "-d", "--name", name, "-p", f"{port}:6379", "redis:7"],
                   capture_output=True,
                   check=True,
                   timeout=60)
    url = f"redis://127.0.0.1:{port}"
    import redis.asyncio as aioredis

    async def ready() -> bool:
        for _ in range(40):
            client = aioredis.from_url(url)
            try:
                await client.ping()
                return True
            except Exception:
                await asyncio.sleep(0.25)
            finally:
                await client.aclose()
        return False

    assert asyncio.run(ready()), "Redis container not ready"
    try:
        yield url
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=15)


def _insert_tenant(pg: PostgreSQLContainer, tenant_id: str, app_id: str) -> None:
    result = pg.run_sql("INSERT INTO tenant_configs "
                        "(tenant_id,enabled,version,app_id,instruction,model_profile,allowed_tools,governance,"
                        "backend_profile,audit_policy) "
                        f"VALUES ('{tenant_id}',true,1,'{app_id}','test','default','[]'::jsonb,"
                        f"'{_GOVERNANCE}'::jsonb,'{_PROFILE}'::jsonb,'{_POLICY}'::jsonb)")
    assert result.success, result.output


def test_two_real_bindings_keep_same_external_identity_and_ordering_separate(r2b_pg, r2b_redis_url):
    tenant_a = f"t{uuid.uuid4().hex[:10]}"
    tenant_b = f"t{uuid.uuid4().hex[:10]}"
    _insert_tenant(r2b_pg, tenant_a, "app_a")
    _insert_tenant(r2b_pg, tenant_b, "app_b")
    binding_a = ChannelBinding(
        binding_id=uuid.uuid4(),
        tenant_id=tenant_a,
        app_id="app_a",
        channel="wecom",
        external_account_id=f"account_{uuid.uuid4().hex[:8]}",
        secret_ref="env:TRPC_R2B_A",
        enabled=True,
        version=1,
    )
    binding_b = ChannelBinding(
        binding_id=uuid.uuid4(),
        tenant_id=tenant_b,
        app_id="app_b",
        channel="wecom",
        external_account_id=f"account_{uuid.uuid4().hex[:8]}",
        secret_ref="env:TRPC_R2B_B",
        enabled=True,
        version=1,
    )

    async def scenario() -> None:
        repository = SqlChannelBindingRepository(
            create_database_engine(DatabaseSettings(url=r2b_pg.url)),
            owns_engine=True,
        )
        order_gate = RedisChannelOrderGate.from_env({"TRPC_REDIS_URL": r2b_redis_url})
        try:
            await repository.create(binding_a)
            await repository.create(binding_b)
            resolved_a = await repository.resolve_enabled("wecom", binding_a.external_account_id)
            resolved_b = await repository.resolve_enabled("wecom", binding_b.external_account_id)
            assert resolved_a == binding_a
            assert resolved_b == binding_b

            def message(account: str, message_id: str) -> UnboundChannelMessage:
                return UnboundChannelMessage(
                    channel="wecom",
                    external_account_id=account,
                    conversation_kind="group",
                    external_user_id="same-external-user",
                    external_conversation_id="same-external-group",
                    external_message_id=message_id,
                    kind="text",
                    text="hello",
                    occurred_at_ms=100,
                )

            inbound_a = bind_message(message(binding_a.external_account_id, "message-a"), resolved_a)
            inbound_b = bind_message(message(binding_b.external_account_id, "message-b"), resolved_b)
            identity_a = project_identity(
                inbound_a.channel,
                inbound_a.external_user_id,
                inbound_a.external_conversation_id,
                binding_id=inbound_a.binding_id,
                conversation_kind=inbound_a.conversation_kind,
            )
            identity_b = project_identity(
                inbound_b.channel,
                inbound_b.external_user_id,
                inbound_b.external_conversation_id,
                binding_id=inbound_b.binding_id,
                conversation_kind=inbound_b.conversation_kind,
            )
            assert identity_a.user_id != identity_b.user_id
            assert identity_a.session_id != identity_b.session_id

            assert await order_gate.accept(tenant_a, binding_a.binding_id, "same-external-group", 100, "message-a")
            # A lower timestamp on a different tenant/binding is independent.
            assert await order_gate.accept(tenant_b, binding_b.binding_id, "same-external-group", 1, "message-b")
            assert not await order_gate.accept(tenant_a, binding_a.binding_id, "same-external-group", 99, "message-old")
        finally:
            await order_gate.close()
            await repository.close()

    asyncio.run(scenario())
