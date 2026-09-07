from __future__ import annotations

import importlib.metadata
from types import SimpleNamespace

import httpx
import pytest

from tenant_agent.agent import tools as tools_module
from tenant_agent.agent.tools import build_tools, calculator, current_time
from tenant_agent.agent.trpc import TrpcAgentEngine
from tenant_agent.governance.policies import ConfirmationManager, GovernanceService
from tenant_agent.security import CompositeSecretResolver, Redactor
from tenant_agent.services.capacity import CapacityInputs, estimate_capacity
from tenant_agent.settings import Settings
from tenant_agent.storage.base import TenantDataPlane
from tenant_agent.storage.memory import InMemoryPlane
from tests.helpers import make_tenant


def test_capacity_uses_littles_law_and_headroom() -> None:
    result = estimate_capacity(
        CapacityInputs(
            peak_callbacks_per_second=100,
            p95_agent_latency_seconds=4,
            max_concurrent_sessions_per_worker=50,
            average_input_tokens=1_000,
            average_output_tokens=500,
            headroom_ratio=1.5,
        )
    )
    assert result.peak_concurrent_sessions == 600
    assert result.recommended_worker_replicas == 12
    assert result.average_tokens_per_second == 150_000


def test_calculator_rejects_code_execution() -> None:
    assert calculator("2 + 3 * 4")["result"] == 14
    assert calculator("-(8 // 3) + 8 % 3")["result"] == -2 + 2
    assert calculator("2 ** 3")["result"] == 8
    assert "error" in calculator("__import__('os').system('whoami')")
    assert "error" in calculator("2 ** 100")
    assert "error" in calculator("1 / 0")
    assert "error" in calculator("1" * 257)
    assert "error" in calculator("1e101")


def test_current_time_handles_valid_and_invalid_zones() -> None:
    assert current_time("UTC")["timezone"] == "UTC"
    assert current_time("This/Zone/DoesNotExist") == {"error": "unknown_timezone"}


@pytest.mark.asyncio
async def test_pinned_trpc_version_and_inmemory_session_adapter(tmp_path: object) -> None:
    assert importlib.metadata.version("trpc-agent-py") == "1.1.19"
    tenant = make_tenant()
    memory = InMemoryPlane()
    plane = TenantDataPlane(
        sessions=memory,
        memories=memory,
        summaries=memory,
        artifacts=memory,
        knowledge=memory,
        audit=memory,
        receipts=memory,
        usage=memory,
        concurrency=memory,
        outbox=memory,
        leases=memory,
    )
    settings = Settings(
        control_database_url="inmemory://",
        bootstrap_config_path=None,
        session_hmac_key="a-long-enough-test-session-hmac-key",
    )
    secrets = CompositeSecretResolver(file_root=tmp_path)  # type: ignore[arg-type]
    governance = GovernanceService(Redactor())
    engine = TrpcAgentEngine(
        settings=settings,
        secrets=secrets,
        governance=governance,
        confirmations=ConfirmationManager(b"a-long-enough-test-session-hmac-key", memory),
    )
    service = await engine._session_service(tenant)
    session = await service.create_session(app_name="compat", user_id="user", session_id="session")
    assert session.id == "session"
    tools = build_tools(
        tenant=tenant,
        app_id="assistant",
        plane=plane,
        governance=governance,
        confirmations=ConfirmationManager(b"a-long-enough-test-session-hmac-key", memory),
    )
    assert {tool.name for tool in tools} == tenant.apps["assistant"].allowed_tools
    await service.close()


@pytest.mark.asyncio
async def test_tool_registry_memory_artifact_fetch_and_unknown_tool(
    tmp_path: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    memory = InMemoryPlane()
    data = TenantDataPlane(
        sessions=memory,
        memories=memory,
        summaries=memory,
        artifacts=memory,
        knowledge=memory,
        audit=memory,
        receipts=memory,
        usage=memory,
        concurrency=memory,
        outbox=memory,
        leases=memory,
    )
    tenant = make_tenant(
        tools=frozenset({"memory_search", "save_text_artifact", "fetch_url", "calculator", "current_time"})
    ).model_copy(update={"metadata": {"http_tool_allowlist": "example.com"}})
    governance = GovernanceService(Redactor())
    confirmations = ConfirmationManager(b"a-long-enough-test-hmac-key", memory)
    registered = build_tools(
        tenant=tenant,
        app_id="assistant",
        plane=data,
        governance=governance,
        confirmations=confirmations,
    )
    functions = {tool.name: tool.func for tool in registered}
    context = SimpleNamespace(user_id="user", session_id="session")
    from tenant_agent.models import MemoryRecord

    await memory.put_memory(
        MemoryRecord(
            memory_id="m1",
            tenant_id=tenant.tenant_id,
            user_id="user",
            content="remember blue",
        )
    )
    found = await functions["memory_search"]("blue", context)
    assert found["items"][0]["content"] == "remember blue"
    saved = await functions["save_text_artifact"]("note.txt", "content", context)
    repeated = await functions["save_text_artifact"]("note.txt", "content", context)
    assert repeated["artifact_id"] == saved["artifact_id"]
    assert await memory.get_artifact(tenant.tenant_id, saved["artifact_id"]) is not None
    too_large = await functions["save_text_artifact"]("huge.txt", "x" * (512 * 1024 + 1), context)
    assert too_large == {"error": "artifact_too_large"}
    assert await functions["fetch_url"]("http://example.com") == {"error": "https_url_required"}
    assert await functions["fetch_url"]("https://not-example.net") == {"error": "host_not_allowed"}
    assert await functions["fetch_url"]("https://user:password@example.com") == {
        "error": "inline_credentials_forbidden"
    }

    async def public_addresses(hostname: str) -> tuple[str, ...]:
        assert hostname == "example.com"
        return ("93.184.216.34",)

    monkeypatch.setattr(tools_module, "_public_addresses", public_addresses)
    real_client = httpx.AsyncClient

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "93.184.216.34"
        assert request.headers["host"] == "example.com"
        assert request.extensions["sni_hostname"] == "example.com"
        return httpx.Response(200, headers={"content-type": "text/plain"}, content=b"fetched")

    monkeypatch.setattr(
        tools_module.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler)),
    )
    fetched = await functions["fetch_url"]("https://example.com/data")
    assert fetched["text"] == "fetched"

    unknown = make_tenant(tools=frozenset({"not_registered"}))
    with pytest.raises(ValueError, match="not registered"):
        build_tools(
            tenant=unknown,
            app_id="assistant",
            plane=data,
            governance=governance,
            confirmations=confirmations,
        )
