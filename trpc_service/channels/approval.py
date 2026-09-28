"""Provider-neutral parsing of trusted human approval commands."""

from dataclasses import replace
import re

from trpc_service.agent.approval import ApprovalDecision, ApprovalService
from trpc_service.channels.contracts import ChannelBindingConfig, IncomingMessage

_COMMAND = re.compile(
    r"^\s*(?P<decision>确认|批准|拒绝|approve|reject)\s+(?P<code>[A-Fa-f0-9]{8})\s*$",
    re.IGNORECASE,
)


class ApprovalCommandProcessor:
    """Turn an exact Channel command into non-forgeable runtime evidence."""

    def __init__(self, approvals: ApprovalService) -> None:
        self._approvals = approvals

    async def process(
        self,
        incoming: IncomingMessage,
        binding: ChannelBindingConfig,
        session_id: str,
    ) -> IncomingMessage:
        """Resolve approval only after Channel identity normalization."""

        match = _COMMAND.fullmatch(incoming.text or "")
        if match is None:
            return incoming
        raw_decision = match.group("decision").casefold()
        decision = (ApprovalDecision.REJECT
                    if raw_decision in {"拒绝", "reject"} else ApprovalDecision.APPROVE)
        try:
            approval = await self._approvals.decide(
                short_code=match.group("code"),
                tenant_id=binding.tenant_id,
                agent_app_id=binding.agent_app_id,
                principal_id=incoming.principal_id,
                session_id=session_id,
                decision=decision,
            )
        except (LookupError, PermissionError):
            # Treat invalid, expired, copied, or replayed codes identically so
            # the Channel does not reveal another tenant's approval state.
            return replace(
                incoming,
                text="确认失败：审批不存在、已失效或不属于当前会话。",
                attributes={
                    **incoming.attributes,
                    "approval_verified": False,
                },
            )
        approved = decision is ApprovalDecision.APPROVE
        message = (f"已确认 {approval.capability_name}，请继续执行原操作。"
                   if approved else f"已拒绝 {approval.capability_name}，不要执行原操作。")
        attributes: dict[str, object] = {
            **incoming.attributes,
            "approval_verified": True,
            "approval_decision": decision.value,
        }
        if approved:
            # Only the durable UUID crosses into Worker context. The short code
            # remains a single-purpose Channel credential and is not persisted
            # in message attributes or model-visible context.
            attributes["approval_id"] = str(approval.approval_id)
            # The message service restores the exact immutable configuration
            # version before the request enters the durable Worker queue.
            attributes["approval_config_version"] = approval.config_version
        return replace(
            incoming,
            text=message,
            # The approval record is the durable authority for any attachments.
            # Never trust attachments supplied by the confirmation message.
            artifact_refs=(approval.artifact_refs if approved else ()),
            attributes=attributes,
        )
