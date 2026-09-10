"""Credential-free deterministic fixtures shared by governance tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(slots=True)
class DeterministicUsage:
    """Bounded usage returned by a fake runner or tool."""

    requests: int = 1
    tool_calls: int = 1
    tokens: int = 10
    cost_units: int = 1


@dataclass(slots=True)
class GovernanceScenario:
    """A tenant-scoped scenario with no real model, tool, or IM credentials."""

    tenant_id: str
    agent_id: str
    allowed_tools: frozenset[str] = frozenset()
    dangerous_tools: frozenset[str] = frozenset()
    maximum_usage: DeterministicUsage = field(default_factory=DeterministicUsage)
    actual_usage: DeterministicUsage = field(default_factory=DeterministicUsage)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DeterministicClock:
    """Virtual clock used by TTL, confirmation, and budget tests."""

    current: datetime = field(
        default_factory=lambda: datetime(2026, 9, 10, tzinfo=timezone.utc)
    )

    def now(self) -> datetime:
        return self.current

    def advance(self, seconds: float) -> datetime:
        self.current += timedelta(seconds=seconds)
        return self.current


def build_two_tenant_scenarios() -> tuple[GovernanceScenario, GovernanceScenario]:
    """Return deliberately opposite policies for isolation tests."""

    return (
        GovernanceScenario(
            tenant_id="tenant-alpha",
            agent_id="agent-alpha",
            allowed_tools=frozenset({"lookup"}),
            dangerous_tools=frozenset({"delete_record"}),
        ),
        GovernanceScenario(
            tenant_id="tenant-beta",
            agent_id="agent-beta",
            allowed_tools=frozenset({"summarize"}),
            dangerous_tools=frozenset(),
        ),
    )


def build_node_ids() -> tuple[str, str]:
    """Return the stable node pair used by cross-node tests."""

    return ("worker-a", "worker-b")
