"""SDK tool boundary enforcing tenant governance before side effects."""

from __future__ import annotations

from typing import Any

from trpc_agent_sdk.context import InvocationContext
from trpc_agent_sdk.tools import BaseTool
from trpc_agent_sdk.types import FunctionDeclaration
from typing_extensions import override

from trpc_service.tenant.models import TenantConfig, TenantContext, ToolRisk
from trpc_service.tool.execution import ToolExecutor
from trpc_service.tool.governance import Decision, GovernancePipeline


class GovernedTool(BaseTool):
    """Delegate to an SDK tool only after tenant policy and execution checks pass."""

    def __init__(
        self,
        tool: BaseTool,
        *,
        config: TenantConfig,
        governance: GovernancePipeline,
        executor: ToolExecutor,
    ) -> None:
        if tool.is_progress_streaming:
            raise ValueError("progress-streaming tools need a governed streaming adapter")
        super().__init__(name=tool.name, description=tool.description)
        self._tool = tool
        self._config = config
        self._governance = governance
        self._executor = executor

    @property
    def is_streaming(self) -> bool:
        return bool(self._tool.is_streaming)

    @property
    def api_variant(self) -> str:
        return str(self._tool.api_variant)

    @override
    def _get_declaration(self) -> FunctionDeclaration | None:
        return self._tool._get_declaration()

    @override
    async def _run_async_impl(
        self,
        *,
        tool_context: InvocationContext,
        args: dict[str, Any],
    ) -> Any:
        metadata = tool_context.agent_context.metadata
        context = _tenant_context(metadata)
        confirmation_token = _confirmation_token(metadata, self.name)
        result = await self._governance.evaluate(
            context=context,
            config=self._config,
            tool_name=self.name,
            arguments=args,
            estimated_cost=1,
            confirmation_token=confirmation_token,
        )
        if result.decision != Decision.ALLOW:
            return {
                "error": result.reason,
                "status": result.decision.value,
            }

        risk = self._config.tools.classifications.get(self.name, ToolRisk.UNKNOWN)
        turn_id = str(metadata.get("turn_id") or context.request_id)
        lease_owner, lease_epoch = _lease_identity(metadata)
        return await self._executor.execute(
            context,
            turn_id=turn_id,
            tool_name=self.name,
            arguments=args,
            risk=risk,
            owner_id=lease_owner,
            fencing_token=lease_epoch,
            call=lambda: self._tool.run_async(tool_context=tool_context, args=args),
        )


def _tenant_context(metadata: dict[str, Any]) -> TenantContext:
    required = (
        "tenant_id",
        "app_id",
        "config_version",
        "binding_id",
        "principal_id",
        "session_id",
        "request_id",
        "trace_id",
    )
    missing = [name for name in required if metadata.get(name) in (None, "")]
    if missing:
        raise ValueError(f"tool context is missing tenant metadata: {', '.join(missing)}")
    return TenantContext(
        tenant_id=str(metadata["tenant_id"]),
        app_id=str(metadata["app_id"]),
        config_version=int(metadata["config_version"]),
        channel_binding_id=str(metadata["binding_id"]),
        principal_id=str(metadata["principal_id"]),
        session_id=str(metadata["session_id"]),
        request_id=str(metadata["request_id"]),
        trace_id=str(metadata["trace_id"]),
    )


def _confirmation_token(metadata: dict[str, Any], tool_name: str) -> str | None:
    tokens = metadata.get("tool_confirmation_tokens")
    if not isinstance(tokens, dict):
        return None
    token = tokens.get(tool_name)
    return token if isinstance(token, str) else None


def _lease_identity(metadata: dict[str, Any]) -> tuple[str | None, int | None]:
    owner = metadata.get("lease_owner")
    epoch = metadata.get("lease_epoch")
    if owner is None and epoch is None:
        return None, None
    if not isinstance(owner, str) or not owner or not isinstance(epoch, int) or epoch < 1:
        raise ValueError("tool context has an invalid lease identity")
    return owner, epoch


__all__ = ["GovernedTool"]
