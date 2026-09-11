import pytest

from trpc_service.agent.tool_catalog import clear_extension_tools, register_tool
from trpc_service.tenant import MCPServerConfig


def external_lookup(value: str):
    return {"value": value}


def test_trusted_extension_registry_and_mcp_config():
    clear_extension_tools()
    register_tool("external_lookup", external_lookup)
    from trpc_service.agent.tool_catalog import extension_tools
    assert extension_tools() == (external_lookup,)
    with pytest.raises(ValueError):
        register_tool("external_lookup", external_lookup)
    config = MCPServerConfig(server_id="mcp_server", endpoint="https://mcp.example.test")
    assert config.endpoint == "https://mcp.example.test"
    clear_extension_tools()


def test_mcp_endpoint_must_be_absolute_http_url():
    with pytest.raises(ValueError):
        MCPServerConfig(server_id="mcp_server", endpoint="stdio://server")
