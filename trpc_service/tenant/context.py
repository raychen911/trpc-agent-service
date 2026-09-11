"""Pure-domain tenant context. No web framework or agent imports."""

from __future__ import annotations

import re
from dataclasses import dataclass

_TENANT_ID_PATTERN: re.Pattern[str] = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")

_FIXED_ERROR_MESSAGE = "Invalid tenant ID format."


class InvalidTenantIdError(ValueError):
    """Raised when a tenant ID does not match the required pattern."""


def validate_tenant_id(tenant_id: str) -> None:
    """Validate *tenant_id* against the tenant ID pattern.

    Raises :class:`InvalidTenantIdError` with a fixed message that never
    includes the raw input value.
    """
    if not isinstance(tenant_id, str) or _TENANT_ID_PATTERN.fullmatch(tenant_id) is None:
        raise InvalidTenantIdError(_FIXED_ERROR_MESSAGE)


@dataclass(frozen=True, slots=True)
class TenantContext:
    """Immutable snapshot of the routing dimensions for one request."""

    tenant_id: str
    app_id: str
    user_id: str
    channel: str
    session_id: str | None = None

    def __post_init__(self) -> None:
        validate_tenant_id(self.tenant_id)

    @property
    def sdk_user_id(self) -> str:
        """Project tenant/channel/session into the SDK ``user_id`` namespace.

        Using the session-scoped identity prevents cross-session memory leaks:
        direct chat, group chat, and different groups each get a distinct SDK
        user key, while members of the same group still share the same group
        session id.
        """
        if self.session_id is not None:
            return f"{self.tenant_id}:{self.channel}:{self.session_id}"
        return f"{self.tenant_id}:{self.channel}:{self.user_id}"


__all__ = [
    "InvalidTenantIdError",
    "TenantContext",
    "validate_tenant_id",
]
