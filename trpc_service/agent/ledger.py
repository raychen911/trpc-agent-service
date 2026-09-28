"""Idempotent Tool call ledger contracts and local implementation."""

from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass
from enum import StrEnum
import hashlib
import json

from trpc_service.agent.contracts import AgentExecutionContext, AgentToolCall, AgentToolResult


class ToolLedgerStatus(StrEnum):
    """Durable outcomes that control whether a logical Tool call may run."""

    PREPARED = "PREPARED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class ToolLedgerConflict(RuntimeError):
    """Raised when a call ID is reused for a different logical operation."""


@dataclass(frozen=True, slots=True)
class ToolLedgerClaim:
    """Preparation result returned before touching a Tool provider."""

    status: ToolLedgerStatus
    should_execute: bool
    result: AgentToolResult | None = None


class ToolLedger(ABC):
    """Persistence boundary for exactly-once logical Tool invocation."""

    @abstractmethod
    async def prepare(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> ToolLedgerClaim:
        """Create a call intent or replay its durable terminal outcome."""

    @abstractmethod
    async def complete(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
        result: AgentToolResult,
    ) -> None:
        """Persist the successful provider result before returning it."""

    @abstractmethod
    async def fail(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
        summary: str,
    ) -> None:
        """Persist a known failure where the provider did not apply a side effect."""

    @abstractmethod
    async def mark_unknown(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
        summary: str,
    ) -> None:
        """Quarantine an ambiguous provider outcome from automatic replay."""


@dataclass(slots=True)
class _MemoryEntry:
    fingerprint: str
    status: ToolLedgerStatus
    result: AgentToolResult | None = None
    summary: str | None = None


def tool_call_fingerprint(call: AgentToolCall) -> str:
    """Hash canonical call intent without persisting secrets in an index."""

    payload = {
        "name": call.name,
        "kind": call.kind.value,
        "action": call.action,
        "resource": call.resource,
        "arguments": call.arguments,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


class InMemoryToolLedger(ToolLedger):
    """Process-local reference implementation used by isolated tests."""

    def __init__(self) -> None:
        self._entries: dict[tuple[str, str, str], _MemoryEntry] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _key(context: AgentExecutionContext, call: AgentToolCall) -> tuple[str, str, str]:
        tenant = context.request.tenant
        return str(tenant.tenant_id), str(tenant.agent_app_id), call.call_id

    async def prepare(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> ToolLedgerClaim:
        fingerprint = tool_call_fingerprint(call)
        async with self._lock:
            key = self._key(context, call)
            entry = self._entries.get(key)
            if entry is None:
                self._entries[key] = _MemoryEntry(fingerprint, ToolLedgerStatus.PREPARED)
                return ToolLedgerClaim(ToolLedgerStatus.PREPARED, should_execute=True)
            if entry.fingerprint != fingerprint:
                raise ToolLedgerConflict("Tool call ID was reused with different arguments")
            return ToolLedgerClaim(
                entry.status,
                should_execute=False,
                result=entry.result,
            )

    async def complete(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
        result: AgentToolResult,
    ) -> None:
        await self._transition(context, call, ToolLedgerStatus.SUCCEEDED, result=result)

    async def fail(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
        summary: str,
    ) -> None:
        await self._transition(context, call, ToolLedgerStatus.FAILED, summary=summary)

    async def mark_unknown(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
        summary: str,
    ) -> None:
        await self._transition(context, call, ToolLedgerStatus.UNKNOWN, summary=summary)

    async def _transition(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
        status: ToolLedgerStatus,
        *,
        result: AgentToolResult | None = None,
        summary: str | None = None,
    ) -> None:
        async with self._lock:
            entry = self._entries.get(self._key(context, call))
            if entry is None or entry.fingerprint != tool_call_fingerprint(call):
                raise ToolLedgerConflict("Tool call was not prepared with this intent")
            if entry.status is ToolLedgerStatus.SUCCEEDED:
                if status is ToolLedgerStatus.SUCCEEDED and entry.result == result:
                    return
                raise ToolLedgerConflict("completed Tool call cannot change outcome")
            if entry.status is ToolLedgerStatus.UNKNOWN and status is not ToolLedgerStatus.UNKNOWN:
                raise ToolLedgerConflict("unknown Tool outcome requires operator reconciliation")
            entry.status = status
            entry.result = result
            entry.summary = summary
