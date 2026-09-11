"""One trusted assembly path for durable SDK tools and confirmation proposals."""

from trpc_agent_sdk.filter import FilterType
from trpc_agent_sdk.tools import BaseTool

from trpc_service.governance.action_tools import ProposalTool, ConfirmationBoundaryFilter
from trpc_service.governance.filters import ToolGovernanceFilter
from trpc_service.reliability.tool_execution import DurableTools
from .runtime import select_tools_for_runner


def assemble_tools(database, tenant, app, functions, *, model, action_service=None, action_definitions=()):
    """Configuration is server-owned; a required approval cannot silently remove a tool.

    Declarations for confirmation tools are never executed. Only ActionWorker may
    invoke their registered definitions after durable approval and revalidation.
    """
    definitions = {item.name: item for item in action_definitions}
    functions = tuple(functions)
    names = [item.name if isinstance(item, BaseTool) else item.__name__ for item in functions]
    if len(set(names)) != len(functions):
        raise ValueError("duplicate tool declarations")
    tools = []
    for function in functions:
        is_sdk_tool = isinstance(function, BaseTool)
        name = function.name if is_sdk_tool else function.__name__
        requires = (name in app.tool_policy.require_confirmation and name in app.tool_policy.allow
                    and name not in app.tool_policy.deny)
        if requires:
            if is_sdk_tool:
                raise ValueError("MCP tools cannot use confirmation until registered as durable actions")
            definition = definitions.get(name)
            if action_service is None or definition is None:
                raise ValueError("confirmation tool requires a registered durable action definition")
            tool = ProposalTool(function, definition, action_service, tenant_id=tenant.tenant_id, app_id=app.app_id)
            # This filter permits creating the proposal, which has no business side
            # effect. The original policy is rechecked by ActionService/ActionWorker.
            policy = app.tool_policy.model_copy(update={"require_confirmation": frozenset()})
        else:
            tool = function if is_sdk_tool else DurableTools(database).wrap(
                function,
                tenant_id=tenant.tenant_id,
                timeout_seconds=app.run_timeout_seconds,
                max_calls=app.tool_policy.max_calls_per_run)
            policy = app.tool_policy
        tool.add_one_filter(ToolGovernanceFilter(tenant.tenant_id, app.app_id, name, policy))
        if action_service is not None:
            tool.add_one_filter(
                ConfirmationBoundaryFilter(action_service, tenant.tenant_id, app.app_id, filter_type=FilterType.TOOL))
        tools.append(tool)
    selected = select_tools_for_runner(tools, app.tool_policy)
    # Validate model-facing schemas at startup, before accepting any IM message.
    for tool in selected:
        tool._get_declaration()
    if action_service is not None:
        model.add_one_filter(ConfirmationBoundaryFilter(action_service, tenant.tenant_id, app.app_id))
    return selected
