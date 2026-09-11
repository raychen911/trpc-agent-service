"""Exercise the pinned SDK schema parser, not only direct Python invocation."""

from types import SimpleNamespace

import pytest
from trpc_agent_sdk.tools import FunctionTool
from trpc_service.storage.runtime_resources import resource_tools


@pytest.mark.parametrize("embedding,expected", [(None, "vector"), (object(), "query")])
def test_runtime_search_schema_matches_configured_mode(embedding, expected):
    tenant = SimpleNamespace(embedding=embedding)
    tools = resource_tools(None, tenant, None)
    declarations = {tool.__name__: FunctionTool(tool)._get_declaration() for tool in tools}
    parameters = declarations["search_knowledge"].parameters.properties
    assert set(parameters) == {"knowledge_base_id", expected}
    assert "tool_context" not in parameters
