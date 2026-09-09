"""Artifact, Knowledge, Approval, PII and metric-boundary tests.

These tests use local deterministic providers and should show that a tenant
cannot read/search another tenant's data and an approval token is single use.
No external dependency or model fee is involved.
"""

import json

import pytest

from trpc_service.log import mask_sensitive_text
from trpc_service.metrics import MetricsRegistry
from trpc_service.resources import ArtifactNotFoundError
from trpc_service.resources import InMemoryArtifactStore
from trpc_service.resources import InMemoryKnowledgeProvider
from trpc_service.resources import KnowledgeDocument
from trpc_service.tenant import ApprovalError
from trpc_service.tenant import ApprovalState
from trpc_service.tenant import InMemoryApprovalStore
from trpc_service.tool import InMemoryToolExecutionStore
from trpc_service.tool import ToolExecutionState

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_artifact_and_knowledge_are_tenant_scoped():
    artifacts = InMemoryArtifactStore()
    metadata = await artifacts.put("a", "app", "../secret.txt", "text/plain", b"owned by a")
    assert metadata.original_name == "secret.txt"
    with pytest.raises(ArtifactNotFoundError):
        await artifacts.get("b", metadata.artifact_id)

    knowledge = InMemoryKnowledgeProvider()
    await knowledge.add(KnowledgeDocument(tenant_id="a", app_id="app", title="Redis", text="redis session lock"))
    assert len(await knowledge.search("a", "app", "redis")) == 1
    assert await knowledge.search("b", "app", "redis") == []


@pytest.mark.asyncio
async def test_approval_token_is_bound_and_consumed_once():
    store = InMemoryApprovalStore()
    arguments_hash = store.arguments_hash(json.dumps({"amount": 1}, sort_keys=True))
    pending = await store.create("a", "u", "s", "pay", arguments_hash)
    _, token = await store.decide(pending.approval_id, approve=True, actor="admin")
    used = await store.consume(pending.approval_id,
                               token,
                               tenant_id="a",
                               user_id="u",
                               session_id="s",
                               tool_name="pay",
                               arguments_sha256=arguments_hash)
    assert used.state == ApprovalState.USED
    with pytest.raises(ApprovalError):
        await store.consume(pending.approval_id,
                            token,
                            tenant_id="a",
                            user_id="u",
                            session_id="s",
                            tool_name="pay",
                            arguments_sha256=arguments_hash)


def test_secret_masking_and_metric_label_cardinality():
    masked = mask_sensitive_text("Authorization: Bearer abc.def token=plain postgresql://u:pass@db/name")
    assert "abc.def" not in masked
    assert "plain" not in masked
    assert "pass@" not in masked
    metrics = MetricsRegistry()
    with pytest.raises(ValueError):
        metrics.increment("requests_total", request_id="high-cardinality")


@pytest.mark.asyncio
async def test_side_effect_tool_reservation_is_idempotent():
    store = InMemoryToolExecutionStore()
    first, should_run = await store.reserve("a", "r", "pay", {"amount": 1})
    duplicate, should_run_again = await store.reserve("a", "r", "pay", {"amount": 1})
    assert should_run is True
    assert should_run_again is False
    assert duplicate.execution_id == first.execution_id
    unknown = await store.finish(first.execution_id, ToolExecutionState.UNKNOWN, error_code="external_result_unknown")
    assert unknown.state == ToolExecutionState.UNKNOWN
