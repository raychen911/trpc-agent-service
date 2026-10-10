from __future__ import annotations

import hashlib
import os
from uuid import uuid4

import boto3
import pytest
import redis.asyncio as redis_async

from tests.conftest import envelope, repository
from trpc_service.queue.emergency import EmergencyQueue, EmergencyQueueDrainer
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.artifacts import S3ArtifactStore
from trpc_service.storage.redis_projection import RedisProjectionStore

pytestmark = pytest.mark.integration


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        pytest.skip(f"{name} is not set")
    return value


@pytest.mark.asyncio
async def test_real_redis_projection_uses_lua_cas_and_tenant_keys() -> None:
    client = redis_async.from_url(_required("TRPC_TEST_REDIS_URL"), decode_responses=False)
    suffix = uuid4().hex
    tenant_a = f"redis-a-{suffix}"
    tenant_b = f"redis-b-{suffix}"
    session_id = "same-session"
    store = RedisProjectionStore(client, ttl_seconds=60)
    keys = (store.key(tenant_a, session_id), store.key(tenant_b, session_id))
    try:
        await client.delete(*keys)
        await store.put_session(tenant_a, session_id, sequence=2, value={"tenant": tenant_a})
        await store.put_session(tenant_b, session_id, sequence=1, value={"tenant": tenant_b})
        assert await store.get_session(tenant_a, session_id, minimum_sequence=2) == {
            "tenant": tenant_a
        }
        assert await store.get_session(tenant_a, session_id, minimum_sequence=3) is None
        with pytest.raises(ValueError, match="backwards"):
            await store.put_session(tenant_a, session_id, sequence=1, value={"stale": True})
    finally:
        await client.delete(*keys)
        await client.aclose()


@pytest.mark.asyncio
async def test_real_redis_emergency_queue_reclaims_abandoned_delivery() -> None:
    client = redis_async.from_url(_required("TRPC_TEST_REDIS_URL"), decode_responses=False)
    suffix = uuid4().hex
    stream = f"trpc:test:emergency:{suffix}"
    group = f"trpc-test-drainers-{suffix}"
    queue = EmergencyQueue(
        client,
        b"e" * 32,
        stream=stream,
        group=group,
        reclaim_after_ms=0,
    )
    repo = repository()
    route = await repo.resolve_binding("binding-unpredictable-a")
    assert route is not None
    prepared = TenantRuntime(repo, routing_key=b"r" * 32).prepare(route, envelope())
    try:
        message_id = await queue.enqueue(prepared)
        abandoned = await queue.consume(consumer="crashed-worker", block_ms=1)
        assert abandoned[0].message_id == message_id

        drainer = EmergencyQueueDrainer(repo, queue, consumer_id="recovery-worker")
        assert await drainer.drain_once() == 1
        assert len(repo._acceptances) == 1
        assert await client.xpending_range(stream, group, "-", "+", 10) == []
    finally:
        await client.delete(stream)
        await client.aclose()


@pytest.mark.asyncio
async def test_real_s3_staged_commit_is_tenant_scoped() -> None:
    endpoint = _required("TRPC_TEST_S3_ENDPOINT")
    access_key = _required("TRPC_TEST_S3_ACCESS_KEY")
    secret_key = _required("TRPC_TEST_S3_SECRET_KEY")
    bucket = _required("TRPC_TEST_S3_BUCKET")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )
    tenant_id = f"s3-{uuid4().hex}"
    artifact_id = f"artifact-{uuid4().hex}"
    content = b"trpc-agent-service S3 contract"
    store = S3ArtifactStore(client, bucket=bucket)
    committed: str | None = None
    staged: str | None = None
    try:
        client.head_bucket(Bucket=bucket)
        staged = await store.stage(
            tenant_id,
            artifact_id,
            content,
            checksum=hashlib.sha256(content).hexdigest(),
        )
        with pytest.raises(ValueError, match="belong"):
            await store.commit(f"other-{tenant_id}", artifact_id, staged)
        committed = await store.commit(tenant_id, artifact_id, staged)
        staged = None
        response = client.get_object(Bucket=bucket, Key=committed)
        assert response["Body"].read() == content
    finally:
        if staged is not None:
            client.delete_object(Bucket=bucket, Key=staged)
        if committed is not None:
            client.delete_object(Bucket=bucket, Key=committed)
