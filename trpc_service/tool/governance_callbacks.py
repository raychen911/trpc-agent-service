"""Official Tool callback adapter (implemented after foundational RED tests)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


async def before_tool_callback(
    *, context: Any, tool_name: str, arguments: Mapping[str, object]
) -> Any:
    """Adapter hook for the official Agent tool callback boundary.

    Full policy evaluation is supplied by the Governance Coordinator in later
    phases; this hook deliberately has no Runner implementation of its own.
    """

    coordinator = getattr(context, "governance", None) or (context.get("governance") if isinstance(context, Mapping) else None)
    if coordinator is None:
        return None
    descriptor = getattr(context, "tool_descriptors", {}).get(tool_name) if hasattr(context, "tool_descriptors") else None
    if descriptor is None:
        return None
    admission = await coordinator.authorize(
        tenant_id=getattr(context, "tenant_id", ""), agent_name=getattr(context, "agent_name", ""),
        binding_id=getattr(context, "binding_id", ""), principal=getattr(context, "principal", None),
        tool=descriptor, session_id=getattr(context, "session_id", ""), execution_id=getattr(context, "execution_id", ""),
    )
    if getattr(admission.decision, "decision", admission.decision).value != "allow":
        from trpc_service.governance.errors import PrincipalUnauthorized
        raise PrincipalUnauthorized()
    return None
