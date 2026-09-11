"""One executable protected-runtime contract for publication and worker startup."""


def validate_protected(tenant, profiles):
    from trpc_service.agent.openrouter import ENDPOINT
    import re
    if tenant.data_backends.session.kind.value not in {"sql", "redis"}:
        raise ValueError("protected runtime requires SQL or Redis Session")
    if tenant.data_backends.memory.kind.value != "sql":
        raise ValueError("protected runtime currently requires SQL Memory")
    for model in tenant.models:
        if (model.provider != "openrouter" or not model.model_name
                or model.max_output_tokens > 128 or model.max_retries != 0 or model.currency != "USD"
                or model.base_url not in {None, ENDPOINT}):
            raise ValueError("model does not match the bounded OpenRouter accounting contract")
    for app in tenant.applications:
        if app.max_tool_iterations > 14:
            raise ValueError("bounded model accounting permits at most 14 tool iterations")
        names = app.tool_policy.allow | app.tool_policy.deny | app.tool_policy.require_confirmation
        if any(not isinstance(name, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.:-]{0,127}", name) is None
               for name in names):
            raise ValueError("invalid runtime tool name")
        from trpc_service.agent.tool_catalog import extension_tools
        known = {"read_artifact", "write_artifact", "search_knowledge"}
        known.update(getattr(tool, "__name__", "") for tool in extension_tools())
        mcp_ids = {server.server_id for server in app.mcp_servers if server.enabled}
        for name in names:
            if name in known:
                continue
            if any(name.startswith(f"mcp__{server_id}__") for server_id in mcp_ids):
                continue
            raise ValueError("unknown runtime tool")
        if any(name.startswith("mcp__") for name in app.tool_policy.require_confirmation):
            raise ValueError("MCP confirmation tools require a registered durable action")
    for role in ("session", "summary"):
        if profiles[role].options.get("session_format") != "protected_v1":
            raise ValueError("protected runtime refuses an unconverted native Session profile")
