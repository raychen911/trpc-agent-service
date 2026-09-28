from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select
from trpc_agent_sdk.types import FunctionDeclaration

from trpc_service.agent.contracts import (
    AgentExecutionClaim,
    AgentExecutionContext,
    AgentExecutionRequest,
    AgentRuntimeConfig,
    AgentToolCall,
    AgentToolKind,
    AgentToolResult,
    PolicyAction,
    PolicyDecision,
)
from trpc_service.agent.adapters.trpc_tools import CapabilityCallSequence
from trpc_service.agent.governance import GovernedToolInvoker, ToolApprovalRequired
from trpc_service.agent.approval import ApprovalRequestSnapshot, ApprovalStatus
from trpc_service.admin.models import TenantSecret
from trpc_service.channels.contracts import ChannelBindingConfig, IncomingMessage, MessageKind
from trpc_service.mcp import MCPConnection, TenantMCPService
from trpc_service.mcp.service import (
    GovernedMCPTool,
    _TRPCMCPTool,
    _TRPCMCPToolset,
    _default_toolset_factory,
    _exposed_tool_name,
    _canonical_mcp_context,
    _public_addresses,
    _risk_level,
)
from trpc_service.tenant.context import TenantContext


async def _create_tenant(client: httpx.AsyncClient, name: str) -> str:
    response = await client.post("/api/v1/tenants", json={"name": name})
    assert response.status_code == 201
    return str(response.json()["tenant_id"])


@pytest.mark.anyio
async def test_mcp_connection_crud_is_tenant_scoped_and_never_echoes_credentials(
    api_client: httpx.AsyncClient, ) -> None:
    tenant_id = await _create_tenant(api_client, "MCP Owner")
    other_id = await _create_tenant(api_client, "MCP Other")

    created = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/mcp-connections",
        json={
            "name": "GitHub",
            "endpoint_url": "https://mcp.example.com/api",
            "auth_type": "bearer",
            "secret_value": "github-test-token",
        },
    )

    assert created.status_code == 201
    body = created.json()
    connection_id = body["connection_id"]
    UUID(connection_id)
    assert body["credential_configured"] is True
    assert "github-test-token" not in created.text
    assert "secret_ref" not in created.text

    listed = await api_client.get(f"/api/v1/tenants/{tenant_id}/mcp-connections")
    cross_tenant = await api_client.get(
        f"/api/v1/tenants/{other_id}/mcp-connections/{connection_id}")
    disabled = await api_client.delete(
        f"/api/v1/tenants/{tenant_id}/mcp-connections/{connection_id}")
    skills = await api_client.get(f"/api/v1/tenants/{tenant_id}/skills")

    assert listed.status_code == 200
    assert listed.json()["total"] == 1
    assert cross_tenant.status_code == 404
    assert disabled.status_code == 204
    assert {item["name"]
            for item in skills.json()["items"]} == {
                "code-review",
                "hr-assistant",
            }


@pytest.mark.anyio
async def test_mcp_secret_uses_an_independent_tenant_scope(api_client: httpx.AsyncClient, ) -> None:
    tenant_id = await _create_tenant(api_client, "MCP Secret")
    created = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/mcp-connections",
        json={
            "name": "Internal",
            "endpoint_url": "https://mcp.example.com/api",
            "auth_type": "bearer",
            "secret_value": "private-mcp-token",
        },
    )
    assert created.status_code == 201

    # This checks persistence through the application's public composition seam;
    # plaintext is resolved only for a provider call and never returned by HTTP.
    app = api_client._transport.app  # type: ignore[attr-defined]
    async with app.state.session_factory() as database:
        row = await database.scalar(select(MCPConnection))
        secret = await database.scalar(select(TenantSecret).where(TenantSecret.name.like("mcp/%")))
    assert row is not None
    assert secret is not None
    assert row.secret_ref is not None
    assert f"/tenants/{tenant_id}/mcp/" in row.secret_ref
    assert "private-mcp-token" not in secret.ciphertext
    assert await app.state.container.tenant_secrets.resolve(
        row.secret_ref,
        UUID(tenant_id),
        scope="mcp",
    ) == "private-mcp-token"


class _FakeRemoteTool:
    name = "get_file_contents"
    description = "Read one repository file"
    annotations = {
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
    }

    def declaration(self) -> dict[str, object]:
        return {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string"
                }
            },
            "required": ["path"],
        }

    async def invoke(self, arguments: dict[str, object]) -> str:
        return f"content:{arguments['path']}"


class _FakeToolset:

    def __init__(self, tools: list[_FakeRemoteTool] | None = None) -> None:
        self._tools = tools or [_FakeRemoteTool()]
        self.closed = False

    async def tools(self) -> list[_FakeRemoteTool]:
        return self._tools

    async def close(self) -> None:
        self.closed = True


class _OversizedSchemaTool(_FakeRemoteTool):

    def declaration(self) -> dict[str, object]:
        return {
            "type": "object",
            "properties": {
                "value": {
                    "type": "string",
                    "description": "x" * (64 * 1024),
                }
            },
        }


class _OversizedResultTool(_FakeRemoteTool):

    async def invoke(self, arguments: dict[str, object]) -> str:
        del arguments
        return "x" * (256 * 1024 + 1)


def _execution_context(
    tenant_id: UUID,
    connection_id: UUID,
    exposed_name: str,
) -> AgentExecutionContext:
    agent_id = uuid4()
    tenant = TenantContext(
        tenant_id=tenant_id,
        agent_app_id=agent_id,
        config_version=1,
        request_id="mcp-request",
        trace_id="mcp-trace",
    )
    return AgentExecutionContext(
        request=AgentExecutionRequest(
            tenant=tenant,
            session_id="mcp-session",
            incoming=IncomingMessage(
                external_message_id="mcp-message",
                principal_id="mcp-user",
                conversation_id="mcp-conversation",
                kind=MessageKind.TEXT,
                occurred_at=datetime.now(timezone.utc),
                text="读取 README",
            ),
            channel=ChannelBindingConfig(
                binding_id=uuid4(),
                tenant_id=tenant_id,
                agent_app_id=agent_id,
                channel_type="test",
            ),
        ),
        config=AgentRuntimeConfig(
            config_version=1,
            runner_name="trpc_agent",
            tools={
                "grants": [{
                    "kind": "mcp",
                    "name": exposed_name,
                    "actions": ["execute"],
                    "resources": [str(connection_id)],
                    "risk_level": 2,
                }]
            },
        ),
        policy=PolicyDecision(action=PolicyAction.ALLOW),
        claim=AgentExecutionClaim(claim_id="mcp-claim"),
    )


def test_mcp_context_uses_catalog_risk_without_mutating_snapshot() -> None:
    tenant_id = uuid4()
    connection_id = uuid4()
    exposed_name = "mcp_legacy_search_repositories"
    context = _execution_context(tenant_id, connection_id, exposed_name)
    legacy = replace(
        context,
        config=replace(
            context.config,
            tools={
                "grants": [{
                    "kind": "mcp",
                    "name": exposed_name,
                    "actions": ["execute"],
                    "resources": [str(connection_id)],
                    "risk_level": 2,
                }]
            },
        ),
    )

    canonical = _canonical_mcp_context(legacy, connection_id, exposed_name, 0)

    assert canonical.config.tools["grants"][0]["risk_level"] == 0  # type: ignore[index]
    assert legacy.config.tools["grants"][0]["risk_level"] == 2  # type: ignore[index]


@pytest.mark.anyio
async def test_mcp_tools_keep_explicit_legacy_grants_visible() -> None:
    tenant_id = uuid4()
    connection_id = uuid4()
    exposed_name = "mcp_legacy_search_repositories"
    context = _execution_context(tenant_id, connection_id, exposed_name)
    context = replace(
        context,
        config=replace(
            context.config,
            tools={
                "grants": [{
                    "kind": "mcp",
                    "name": exposed_name,
                    "actions": ["execute"],
                    "resources": [str(connection_id)],
                    "risk_level": 0,
                }]
            },
        ),
    )
    connection = MCPConnection(
        connection_id=connection_id,
        tenant_id=tenant_id,
        name="Legacy GitHub",
        endpoint_url="https://mcp.example.com/api",
        auth_type="none",
        status="active",
        tool_catalog=[{
            "name": exposed_name,
            "remote_name": "search_repositories",
            "description": "Search repositories",
            "input_schema": {
                "type": "object",
                "properties": {}
            },
            "risk_level": 2,
        }],
    )

    class _Rows:

        def all(self) -> list[MCPConnection]:
            return [connection]

    class _Database:

        async def __aenter__(self) -> "_Database":
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def scalars(self, _: object) -> _Rows:
            return _Rows()

    class _Sessions:

        def __call__(self) -> _Database:
            return _Database()

    service = TenantMCPService(
        _Sessions(),  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        resolve_host=lambda _: ("8.8.8.8", ),
    )

    visible = await service.tools_for(
        context,
        GovernedToolInvoker(service),
        CapabilityCallSequence(),
    )

    assert [tool.name for tool in visible] == [exposed_name]
    with pytest.raises(ToolApprovalRequired, match="trusted approval"):
        await visible[0]._run_async_impl(  # pylint: disable=protected-access
            tool_context=None,  # type: ignore[arg-type]
            args={"query": "user:Eternally72"},
        )


@pytest.mark.anyio
async def test_mcp_service_discovers_and_invokes_tenant_tool_through_public_port(
    api_client: httpx.AsyncClient, ) -> None:
    tenant_id = UUID(await _create_tenant(api_client, "MCP Runtime"))
    app = api_client._transport.app  # type: ignore[attr-defined]
    service = TenantMCPService(
        app.state.session_factory,
        app.state.container.tenant_secrets,
        toolset_factory=lambda *_: _FakeToolset(),
        resolve_host=lambda _: ("8.8.8.8", ),
    )
    async with app.state.session_factory.begin() as database:
        connection = MCPConnection(
            tenant_id=tenant_id,
            name="GitHub",
            endpoint_url="https://mcp.example.com/api",
            auth_type="none",
        )
        database.add(connection)
        await database.flush()
        connection_id = connection.connection_id

    catalog = await service.refresh(tenant_id, connection_id)
    exposed_name = str(catalog[0]["name"])
    context = _execution_context(tenant_id, connection_id, exposed_name)
    result = await service.invoke(
        context,
        AgentToolCall(
            call_id="mcp-request:0:mcp",
            name=exposed_name,
            kind=AgentToolKind.MCP,
            logical_call_index=0,
            resource=str(connection_id),
            arguments={"path": "README.md"},
        ),
    )

    assert catalog[0]["remote_name"] == "get_file_contents"
    assert catalog[0]["risk_level"] == 0
    assert catalog[0]["risk_policy_version"] == 1
    assert result.content == "content:README.md"

    changed_tool = _FakeRemoteTool()
    changed_tool.annotations = {"readOnlyHint": False, "destructiveHint": True}
    changed_toolset = _FakeToolset([changed_tool])
    changed_service = TenantMCPService(
        app.state.session_factory,
        app.state.container.tenant_secrets,
        toolset_factory=lambda *_: changed_toolset,
        resolve_host=lambda _: ("8.8.8.8", ),
    )
    with pytest.raises(PermissionError, match="risk"):
        await changed_service.invoke(
            context,
            AgentToolCall(
                call_id="changed:0:mcp",
                name=exposed_name,
                kind=AgentToolKind.MCP,
                logical_call_index=0,
                resource=str(connection_id),
                arguments={"path": "README.md"},
            ))
    assert changed_toolset.closed

    visible = await service.tools_for(
        context,
        GovernedToolInvoker(service),
        CapabilityCallSequence(),
    )
    assert [tool.name for tool in visible] == [exposed_name]
    # The immutable Agent snapshot may still contain the former conservative
    # risk level. Runtime catalog classification is authoritative for this
    # explicitly granted connection/tool pair.
    response = await visible[0]._run_async_impl(  # pylint: disable=protected-access
        tool_context=None,  # type: ignore[arg-type]
        args={"path": "README.md"},
    )
    assert response == {"result": "content:README.md"}

    # Deployments created before read/write classification are upgraded lazily,
    # so an existing tenant does not need to discover a hidden refresh step.
    async with app.state.session_factory.begin() as database:
        persisted = await database.get(MCPConnection, connection_id)
        assert persisted is not None
        persisted.tool_catalog = [{
            **{
                key: value
                for key, value in catalog[0].items() if key != "risk_policy_version"
            },
            "risk_level": 2,
        }]
    upgraded_service = TenantMCPService(
        app.state.session_factory,
        app.state.container.tenant_secrets,
        toolset_factory=lambda *_: _FakeToolset(),
        resolve_host=lambda _: ("8.8.8.8", ),
    )
    upgraded = await upgraded_service.tools_for(
        context,
        GovernedToolInvoker(upgraded_service),
        CapabilityCallSequence(),
    )
    upgraded_response = await upgraded[0]._run_async_impl(  # pylint: disable=protected-access
        tool_context=None,  # type: ignore[arg-type]
        args={"path": "README.md"},
    )
    assert upgraded_response == {"result": "content:README.md"}
    async with app.state.session_factory() as database:
        persisted = await database.get(MCPConnection, connection_id)
        assert persisted is not None
        assert persisted.tool_catalog[0]["risk_policy_version"] == 1


@pytest.mark.anyio
async def test_mcp_service_bounds_untrusted_catalogs_and_results(
        api_client: httpx.AsyncClient) -> None:
    tenant_id = UUID(await _create_tenant(api_client, "MCP Bounds"))
    app = api_client._transport.app  # type: ignore[attr-defined]
    async with app.state.session_factory.begin() as database:
        connection = MCPConnection(
            tenant_id=tenant_id,
            name="Bounded",
            endpoint_url="https://mcp.example.com/api",
            auth_type="none",
        )
        database.add(connection)
        await database.flush()
        connection_id = connection.connection_id

    oversized_schema = TenantMCPService(
        app.state.session_factory,
        app.state.container.tenant_secrets,
        toolset_factory=lambda *_: _FakeToolset([_OversizedSchemaTool()]),
        resolve_host=lambda _: ("8.8.8.8", ),
    )
    with pytest.raises(ValueError, match="schema exceeds"):
        await oversized_schema.refresh(tenant_id, connection_id)

    oversized_result = TenantMCPService(
        app.state.session_factory,
        app.state.container.tenant_secrets,
        toolset_factory=lambda *_: _FakeToolset([_OversizedResultTool()]),
        resolve_host=lambda _: ("8.8.8.8", ),
    )
    catalog = await oversized_result.refresh(tenant_id, connection_id)
    context = _execution_context(tenant_id, connection_id, str(catalog[0]["name"]))
    with pytest.raises(ValueError, match="result exceeds"):
        await oversized_result.invoke(
            context,
            AgentToolCall(
                call_id="mcp-request:0:oversized",
                name=str(catalog[0]["name"]),
                kind=AgentToolKind.MCP,
                logical_call_index=0,
                resource=str(connection_id),
                arguments={},
            ),
        )


@pytest.mark.anyio
async def test_mcp_api_rotates_credentials_and_reports_refresh_errors(
        api_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    tenant_id = await _create_tenant(api_client, "MCP Lifecycle")
    created = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/mcp-connections",
        json={
            "name": "Lifecycle",
            "endpoint_url": "https://mcp.example.com/api",
            "auth_type": "none",
        },
    )
    connection_id = created.json()["connection_id"]

    missing_credential = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/mcp-connections/{connection_id}",
        json={"auth_type": "bearer"},
    )
    invalid_reference = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/mcp-connections/{connection_id}",
        json={"secret_ref": "env://DASHSCOPE_API_KEY"},
    )
    rotated = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/mcp-connections/{connection_id}",
        json={
            "auth_type": "bearer",
            "secret_value": "rotated-token",
            "timeout_seconds": 12
        },
    )
    duplicate = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/mcp-connections",
        json={
            "name": "Lifecycle",
            "endpoint_url": "https://other.example.com/mcp",
            "auth_type": "none",
        },
    )

    app = api_client._transport.app  # type: ignore[attr-defined]

    async def successful_refresh(target_tenant: UUID,
                                 target_connection: UUID) -> list[dict[str, object]]:
        catalog = [{
            "name": "mcp_lifecycle_read",
            "remote_name": "read",
            "description": "Read data",
            "input_schema": {
                "type": "object",
                "properties": {}
            },
            "risk_level": 0,
        }]
        async with app.state.session_factory.begin() as database:
            row = await database.scalar(
                select(MCPConnection).where(
                    MCPConnection.tenant_id == target_tenant,
                    MCPConnection.connection_id == target_connection,
                ))
            assert row is not None
            row.tool_catalog = catalog
            row.catalog_refreshed_at = datetime.now(timezone.utc)
        return catalog

    monkeypatch.setattr(app.state.container.mcp, "refresh", successful_refresh)
    refreshed = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/mcp-connections/{connection_id}/refresh")

    async def failed_refresh(*_: object) -> list[dict[str, object]]:
        raise TimeoutError("provider detail must not escape")

    monkeypatch.setattr(app.state.container.mcp, "refresh", failed_refresh)
    failed = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/mcp-connections/{connection_id}/refresh")
    reread = await api_client.get(f"/api/v1/tenants/{tenant_id}/mcp-connections/{connection_id}")

    assert missing_credential.status_code == 422
    assert invalid_reference.status_code == 422
    assert rotated.status_code == 200
    assert rotated.json()["credential_configured"] is True
    assert duplicate.status_code == 409
    assert refreshed.status_code == 200
    assert refreshed.json()["tool_catalog"][0]["remote_name"] == "read"
    assert refreshed.json()["tool_catalog"][0]["risk_level"] == 0
    assert failed.status_code == 502
    assert "provider detail" not in failed.text
    assert reread.json()["last_error_code"] == "TimeoutError"


class _ResultInvoker:

    async def invoke(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> AgentToolResult:
        del context
        return AgentToolResult(call.call_id, content=f"called:{call.arguments['path']}")


class _DeniedInvoker:

    async def invoke(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> AgentToolResult:
        del context, call
        raise PermissionError("denied")


class _ApprovalInvoker:

    def __init__(self, context: AgentExecutionContext) -> None:
        self._context = context

    async def invoke(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> AgentToolResult:
        del context
        request = self._context.request
        approval = ApprovalRequestSnapshot(
            approval_id=uuid4(),
            short_code="A1B2C3D4",
            tenant_id=request.tenant.tenant_id,
            agent_app_id=request.tenant.agent_app_id,
            binding_id=request.channel.binding_id,
            principal_id=request.incoming.principal_id,
            session_id=request.session_id,
            tool_call_id=call.call_id,
            capability_kind="mcp",
            capability_name=call.name,
            action="execute",
            resource=call.resource,
            arguments_hash="hash",
            risk_level=2,
            status=ApprovalStatus.PENDING,
            expires_at=datetime.now(timezone.utc),
        )
        raise ToolApprovalRequired("approval", approval=approval)


@pytest.mark.anyio
async def test_governed_mcp_tool_returns_bounded_runtime_results() -> None:
    tenant_id = uuid4()
    connection_id = uuid4()
    context = _execution_context(tenant_id, connection_id, "mcp_test_read")
    entry = {
        "name": "mcp_test_read",
        "description": "Read",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string"
                }
            },
        },
    }

    success = GovernedMCPTool(
        context=context,
        invoker=_ResultInvoker(),
        sequence=CapabilityCallSequence(),
        connection_id=connection_id,
        catalog_entry=entry,
    )
    denied = GovernedMCPTool(
        context=context,
        invoker=_DeniedInvoker(),
        sequence=CapabilityCallSequence(),
        connection_id=connection_id,
        catalog_entry=entry,
    )
    approval = GovernedMCPTool(
        context=context,
        invoker=_ApprovalInvoker(context),
        sequence=CapabilityCallSequence(),
        connection_id=connection_id,
        catalog_entry=entry,
    )

    assert (await success._run_async_impl(tool_context=None, args={"path": "README"}) == {
        "result": "called:README"
    })  # type: ignore[arg-type]
    assert "未执行" in (await
                     denied._run_async_impl(tool_context=None,
                                            args={"path":
                                                  "README"}))["result"]  # type: ignore[arg-type]
    assert "确认 A1B2C3D4" in (await
                             approval._run_async_impl(tool_context=None,
                                                      args={"path": "README"
                                                            }))["result"]  # type: ignore[arg-type]


class _FakeSDKTool:
    name = "sdk-read"
    description = "SDK adapter"

    def __init__(self, *, with_schema: bool = True) -> None:
        self._with_schema = with_schema
        self._mcp_tool = SimpleNamespace(annotations={"readOnlyHint": True})

    def _get_declaration(self) -> FunctionDeclaration:
        return FunctionDeclaration(
            name=self.name,
            description=self.description,
            parameters=({
                "type": "object",
                "properties": {}
            } if self._with_schema else None),
        )

    async def _run_async_impl(self, *, args: dict[str, object], tool_context: object) -> object:
        del tool_context
        return {"args": args}


class _FakeSDKToolset:

    def __init__(self) -> None:
        self.closed = False

    async def get_tools(self) -> list[_FakeSDKTool]:
        return [_FakeSDKTool()]

    async def close(self) -> None:
        self.closed = True


@pytest.mark.anyio
async def test_trpc_mcp_adapter_uses_upstream_toolset_contract(
        monkeypatch: pytest.MonkeyPatch) -> None:
    adapted = _TRPCMCPTool(_FakeSDKTool())
    no_schema = _TRPCMCPTool(_FakeSDKTool(with_schema=False))
    sdk_toolset = _FakeSDKToolset()
    adapted_toolset = _TRPCMCPToolset(sdk_toolset)  # type: ignore[arg-type]

    assert adapted.name == "sdk-read"
    assert adapted.annotations == {"readOnlyHint": True}
    assert adapted.declaration()["type"] == "OBJECT"
    assert no_schema.declaration() == {"type": "object", "properties": {}}
    assert await adapted.invoke({"path": "README"}) == {"args": {"path": "README"}}
    assert [tool.name for tool in await adapted_toolset.tools()] == ["sdk-read"]
    await adapted_toolset.close()
    assert sdk_toolset.closed is True

    monkeypatch.setattr(
        "trpc_service.mcp.service.socket.getaddrinfo",
        lambda *_args, **_kwargs: [(None, None, None, None, ("8.8.8.8", 443))],
    )
    assert _public_addresses("mcp.example.com") == ("8.8.8.8", )
    assert _exposed_tool_name(uuid4(), "***").endswith("_tool")

    read_only = SimpleNamespace(annotations={"readOnlyHint": True})
    assert _risk_level(read_only) == 0  # type: ignore[arg-type]
    unclassified = SimpleNamespace(annotations={})
    mutating = SimpleNamespace(annotations={"readOnlyHint": False})
    contradictory = SimpleNamespace(annotations={"readOnlyHint": True, "destructiveHint": True})
    assert _risk_level(unclassified) == 2  # type: ignore[arg-type]
    assert _risk_level(mutating) == 2  # type: ignore[arg-type]
    assert _risk_level(contradictory) == 2  # type: ignore[arg-type]

    connection = MCPConnection(
        endpoint_url="https://mcp.example.com/api",
        tenant_id=uuid4(),
        name="Factory",
        timeout_seconds=3,
    )
    factory_toolset = _default_toolset_factory(connection, {}, ("8.8.8.8", ))
    await factory_toolset.close()


@pytest.mark.anyio
async def test_mcp_service_rejects_invalid_network_and_invocation_boundaries(
        api_client: httpx.AsyncClient) -> None:
    app = api_client._transport.app  # type: ignore[attr-defined]
    empty_dns = TenantMCPService(
        app.state.session_factory,
        app.state.container.tenant_secrets,
        resolve_host=lambda _: (),
    )
    private_dns = TenantMCPService(
        app.state.session_factory,
        app.state.container.tenant_secrets,
        resolve_host=lambda _: ("127.0.0.1", ),
    )
    allowed_private = TenantMCPService(
        app.state.session_factory,
        app.state.container.tenant_secrets,
        resolve_host=lambda _: ("127.0.0.1", ),
        private_allowed_hosts=("internal.example.com", ),
    )

    with pytest.raises(PermissionError, match="HTTPS"):
        await empty_dns._validate_network("http://mcp.example.com", 2)
    with pytest.raises(ConnectionError, match="did not resolve"):
        await empty_dns._validate_network("https://mcp.example.com", 2)
    with pytest.raises(PermissionError, match="non-public"):
        await private_dns._validate_network("https://mcp.example.com", 2)
    await allowed_private._validate_network("https://internal.example.com/mcp", 2)

    context = _execution_context(uuid4(), uuid4(), "missing")
    with pytest.raises(PermissionError, match="not a registered MCP"):
        await empty_dns.invoke(
            context,
            AgentToolCall(
                call_id="bad-kind",
                name="missing",
                kind=AgentToolKind.TOOL,
                logical_call_index=0,
            ),
        )
    with pytest.raises(PermissionError, match="resource is invalid"):
        await empty_dns.invoke(
            context,
            AgentToolCall(
                call_id="bad-resource",
                name="missing",
                kind=AgentToolKind.MCP,
                logical_call_index=0,
                resource="invalid",
            ),
        )
    with pytest.raises(LookupError, match="does not exist"):
        await empty_dns.invoke(
            context,
            AgentToolCall(
                call_id="missing-connection",
                name="missing",
                kind=AgentToolKind.MCP,
                logical_call_index=0,
                resource=str(uuid4()),
            ),
        )

    invalid_grants = replace(context, config=replace(context.config, tools={"grants": "invalid"}))
    non_mapping = replace(context, config=replace(context.config, tools={"grants": ["invalid"]}))
    assert await empty_dns.tools_for(invalid_grants, empty_dns, CapabilityCallSequence()) == []
    assert await empty_dns.tools_for(non_mapping, empty_dns, CapabilityCallSequence()) == []


@pytest.mark.anyio
@pytest.mark.parametrize("host,remote_name,has_guidance", [
    ("api.githubcopilot.com", "search_repositories", True),
    ("mcp.example.com", "search_repositories", False),
    ("api.githubcopilot.com.evil.example", "search_repositories", False),
    ("api.githubcopilot.com", "get_file_contents", False),
])
async def test_github_search_guidance_reaches_model_without_changing_grants(
        api_client, host, remote_name, has_guidance):
    tenant_id = UUID(await _create_tenant(api_client, "MCP search guidance"))
    app = api_client._transport.app
    connection_id = uuid4()
    name = _exposed_tool_name(connection_id, remote_name)
    entry = {
        "name": name,
        "remote_name": remote_name,
        "description": "Provider description",
        "input_schema": {
            "type": "object",
            "properties": {}
        },
        "risk_level": 0,
        "risk_policy_version": 1,
    }
    async with app.state.session_factory.begin() as db:
        db.add(
            MCPConnection(connection_id=connection_id,
                          tenant_id=tenant_id,
                          name="GitHub",
                          endpoint_url=f"https://{host}/mcp/",
                          tool_catalog=[entry]))
    context = _execution_context(tenant_id, connection_id, name)
    service = app.state.container.mcp
    calls = []

    class SearchInvoker:

        async def invoke(self, context, call):
            calls.append(call)
            return AgentToolResult(call.call_id, content='{"total_count":0,"items":[]}')

    visible = await service.tools_for(context, SearchInvoker(), CapabilityCallSequence())
    assert len(visible) == 1
    declaration = visible[0]._get_declaration()
    assert ("fork:true" in declaration.description) is has_guidance
    assert "Provider description" in declaration.description
    arguments = {"query": "user:example project fork:false"}
    result = await visible[0]._run_async_impl(tool_context=None, args=arguments)
    assert result["result"] == '{"total_count":0,"items":[]}'
    assert ("fork:true" in result.get("guidance", "")) is has_guidance
    assert dict(calls[0].arguments) == arguments
    assert context.config.tools["grants"][0]["risk_level"] == 2
    async with app.state.session_factory() as db:
        row = await db.get(MCPConnection, connection_id)
        assert row.tool_catalog == [entry]
    ungranted = replace(context, config=replace(context.config, tools={"grants": []}))
    assert await service.tools_for(ungranted, service, CapabilityCallSequence()) == []
