"""Composable tenant governance filters for inbound, Tool, and output paths."""

from __future__ import annotations

import re
import time
from collections import defaultdict, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from trpc_service.tool import ConfirmationRequired, ToolPolicy, ToolPolicyFilter


class PolicyViolation(PermissionError):
    """A filter denied an operation before an external effect could occur."""


@dataclass(frozen=True, slots=True)
class Principal:
    tenant_id: str
    subject_id: str
    roles: frozenset[str] = frozenset()
    enabled: bool = True


class PrincipalFilter:
    def check(self, principal: Principal, *, required_roles: Sequence[str] = ()) -> None:
        if not principal.tenant_id or not principal.subject_id or not principal.enabled:
            raise PolicyViolation("principal is not active in this tenant")
        if required_roles and not principal.roles.intersection(required_roles):
            raise PolicyViolation("principal lacks a required tenant role")


class RateLimitFilter:
    """Local token window; production callers may replace it with Redis storage."""

    def __init__(self, *, limit: int = 60, window_seconds: float = 60) -> None:
        self.limit = limit
        self.window_seconds = window_seconds
        self._events: dict[tuple[str, str], deque[float]] = defaultdict(deque)

    def check(self, tenant_id: str, subject_id: str) -> None:
        key, now = (tenant_id, subject_id), time.monotonic()
        events = self._events[key]
        while events and events[0] <= now - self.window_seconds:
            events.popleft()
        if len(events) >= self.limit:
            raise PolicyViolation("soft rate limit exceeded")
        events.append(now)


class BudgetFilter:
    """Marker filter: hard reservation is delegated to PlatformRuntime/SQL facts."""

    def estimates(
        self,
        text: str,
        *,
        max_output_tokens: int,
        input_overhead_tokens: int = 256,
        minimum: int = 1,
    ) -> dict[str, int]:
        """Reserve an upper bound before the model provider can charge us.

        The prompt includes release instructions and bounded session context, so
        word-counting the inbound text alone is insufficient.  The configurable
        input overhead is deliberately conservative; actual reported usage is
        reconciled at commit time and an absent report settles at this reserve.
        """

        if max_output_tokens < 1 or input_overhead_tokens < 0:
            raise ValueError("model token reservation bounds must be non-negative")
        estimated_input = max(minimum, len(text.split())) + input_overhead_tokens
        return {"model_tokens": estimated_input + max_output_tokens}


class _DLPFilter:
    def __init__(self, patterns: Sequence[str] = ()) -> None:
        default = (r"\b(?:api[_-]?key|password|secret)\s*[:=]",)
        self._patterns = tuple(re.compile(pattern, re.I) for pattern in (patterns or default))

    def check(self, text: str) -> None:
        if any(pattern.search(text) for pattern in self._patterns):
            raise PolicyViolation("DLP policy blocked sensitive plaintext")

    def redact(self, text: str) -> str:
        for pattern in self._patterns:
            text = pattern.sub("[REDACTED_FIELD]=", text)
        return text


class InputDLPFilter(_DLPFilter):
    """DLP policy applied before model input is constructed."""


class ToolResultFilter(_DLPFilter):
    def check_result(self, result: Mapping[str, Any]) -> dict[str, Any]:
        rendered = str(dict(result))
        self.check(rendered)
        return dict(result)


class OutputDLPFilter(_DLPFilter):
    """DLP policy applied before a reply becomes an Outbox fact."""


class ConfirmationFilter:
    def check(self, *, required: bool, confirmed: bool) -> None:
        if required and not confirmed:
            raise ConfirmationRequired("explicit confirmation is required for this operation")


__all__ = [
    "BudgetFilter",
    "ConfirmationFilter",
    "InputDLPFilter",
    "OutputDLPFilter",
    "PolicyViolation",
    "Principal",
    "PrincipalFilter",
    "RateLimitFilter",
    "ToolPolicy",
    "ToolPolicyFilter",
    "ToolResultFilter",
]
