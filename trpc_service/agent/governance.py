# mypy: disable-error-code="import-untyped"
"""Tenant-scoped content governance implemented on the tRPC-Agent Filter path."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from typing import Any

from trpc_agent_sdk.abc import FilterResult, FilterType
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.filter import BaseFilter
from trpc_agent_sdk.types import Content

from trpc_service.tenant.context import TenantContext
from trpc_service.tenant.models import GovernancePolicy
from trpc_service.tool import TENANT_CONTEXT_METADATA_KEY


class GovernanceViolationError(PermissionError):
    """A turn violates an immutable tenant governance policy."""


_REDACTION_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"(?<![\w.+-])[\w.+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?![\w.-])"),
        "[EMAIL_REDACTED]",
    ),
    (re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)"), "[PHONE_REDACTED]"),
    (re.compile(r"(?<!\d)\d{17}[0-9Xx](?!\d)"), "[ID_REDACTED]"),
    (re.compile(r"(?<!\d)(?:\d[ -]?){15,18}\d(?!\d)"), "[CARD_REDACTED]"),
    (
        re.compile(r"(?i)\b(?:sk|key|token)[-_][A-Za-z0-9_-]{16,}\b"),
        "[CREDENTIAL_REDACTED]",
    ),
)


class TenantGovernanceFilter(BaseFilter):
    """Verify tenant context and sanitize every streamed Agent event.

    A fresh instance is built for every turn by :class:`AgentFactory`. This is
    deliberately an Agent filter instead of a presentation-layer cleanup: the
    sanitized event is what the SDK Runner and platform projection observe.
    """

    def __init__(self, tenant_context: TenantContext, policy: GovernancePolicy) -> None:
        super().__init__()
        self._type = FilterType.AGENT
        self._name = "tenant_governance"
        self._tenant_context = tenant_context
        self._policy = policy

    async def _before(
        self,
        ctx: AgentContext,
        req: Any,
        rsp: FilterResult[Any],
    ) -> None:
        del req
        trusted = ctx.get_metadata(TENANT_CONTEXT_METADATA_KEY)
        if not isinstance(trusted, TenantContext) or trusted != self._tenant_context:
            rsp.error = GovernanceViolationError("trusted tenant context mismatch")
            rsp.is_continue = False

    async def _after_every_stream(
        self,
        ctx: AgentContext,
        req: Any,
        rsp: FilterResult[Any],
    ) -> None:
        del ctx, req
        if isinstance(rsp.rsp, Event):
            rsp.rsp = _sanitize_event(rsp.rsp, self._policy)


def govern_agent_input(
    value: str | Content | list[Content],
    policy: GovernancePolicy,
) -> str | Content | list[Content]:
    """Apply the published input policy before content reaches the session/model."""

    if isinstance(value, str):
        return _govern_input_text(value, policy)
    if isinstance(value, Content):
        return _map_content(value, lambda text: _govern_input_text(text, policy))
    return [_map_content(item, lambda text: _govern_input_text(text, policy)) for item in value]


def redact_sensitive_text(value: str) -> str:
    """Redact common direct identifiers and credential-like tokens."""

    sanitized = value
    for pattern, replacement in _REDACTION_RULES:
        sanitized = pattern.sub(replacement, sanitized)
    return sanitized


def _govern_input_text(value: str, policy: GovernancePolicy) -> str:
    if len(value) > policy.max_input_chars:
        raise GovernanceViolationError("input exceeds the tenant character limit")
    if _contains_term(value, policy.blocked_input_terms):
        raise GovernanceViolationError("input contains a tenant-blocked term")
    return redact_sensitive_text(value) if policy.redact_sensitive_data else value


def _sanitize_event(event: Event, policy: GovernancePolicy) -> Event:
    if event.content is None:
        return event

    def sanitize_output(value: str) -> str:
        sanitized = redact_sensitive_text(value) if policy.redact_sensitive_data else value
        sanitized = _replace_terms(sanitized, policy.blocked_output_terms)
        if len(sanitized) > policy.max_output_chars:
            sanitized = sanitized[: policy.max_output_chars] + "…[TRUNCATED_BY_POLICY]"
        return sanitized

    content = _map_content(event.content, sanitize_output)
    return event.model_copy(update={"content": content})


def _map_content(content: Content, transform: Callable[[str], str]) -> Content:
    parts = []
    for part in content.parts or []:
        text = getattr(part, "text", None)
        parts.append(part.model_copy(update={"text": transform(text)}) if text else part)
    return content.model_copy(update={"parts": parts})


def _contains_term(value: str, terms: Iterable[str]) -> bool:
    folded = value.casefold()
    return any(term.casefold() in folded for term in terms)


def _replace_terms(value: str, terms: Iterable[str]) -> str:
    sanitized = value
    for term in terms:
        sanitized = re.sub(re.escape(term), "[BLOCKED_TERM]", sanitized, flags=re.IGNORECASE)
    return sanitized
