"""Policy-enforced, fence-aware Tool execution.

This module defines the safety boundary *outside* any Agent framework.  A function is
not allowed to issue an external request until the deterministic intent was persisted
and moved to ``running`` while the current Session fence and security epochs match.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol

from trpc_service.metrics import current_trace_context, traced


class RetryCapability(StrEnum):
    IDEMPOTENT = "idempotent"
    QUERYABLE = "queryable"
    NON_RETRIABLE = "non_retriable"


class ToolError(RuntimeError):
    code = "tool_error"


class ToolDenied(ToolError):
    code = "tool_denied"


class ConfirmationRequired(ToolError):
    code = "confirmation_required"


class ToolValidationError(ToolError):
    code = "invalid_tool_arguments"


class ExecutionDivergence(ToolError):
    code = "execution_divergence"


class LostFence(ToolError):
    code = "lost_fence"


class AmbiguousToolOutcome(ToolError):
    """The provider may have accepted the request but no outcome can be proved."""

    code = "ambiguous_provider_outcome"


@dataclass(frozen=True, slots=True)
class FenceClaim:
    tenant_id: str
    inbox_id: str
    execution_id: str
    session_id: str
    lease_fence: int
    routing_epoch: int
    security_epoch: int
    worker_id: str = "worker"


@dataclass(frozen=True, slots=True)
class ToolInvocation:
    name: str
    step: int
    arguments: Mapping[str, Any]
    principal_id: str
    principal_roles: frozenset[str] = frozenset()
    confirmed: bool = False
    trace_id: str | None = None


@dataclass(frozen=True, slots=True)
class ToolExecutionRecord:
    tenant_id: str
    tool_call_id: str
    inbox_id: str
    execution_id: str
    session_id: str
    tool_step: int
    tool_name: str
    arguments_hash: str
    retry_capability: RetryCapability
    provider_idempotency_key: str | None
    lease_fence: int
    routing_epoch: int
    security_epoch: int
    status: str = "prepared"
    result: Mapping[str, Any] | None = None
    provider_operation_id: str | None = None
    last_error_code: str | None = None
    trace_id: str = ""
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True, slots=True)
class ToolOutcome:
    status: str
    tool_call_id: str
    result: Mapping[str, Any] | None = None
    error_code: str | None = None
    requires_manual_resolution: bool = False


class FenceVerifier(Protocol):
    async def verify(self, claim: FenceClaim) -> bool:
        """Check active Inbox, owner/fence, routing/security epochs, and live mode."""


class AlwaysValidFence:
    """Only for focused unit tests and local demo. Production injects DB fence checks."""

    async def verify(self, claim: FenceClaim) -> bool:
        return True


class Tool(Protocol):
    name: str
    description: str
    parameters: Mapping[str, Any]
    retry_capability: RetryCapability

    async def call(
        self, arguments: Mapping[str, Any], *, claim: FenceClaim, idempotency_key: str | None
    ) -> Mapping[str, Any]: ...

    async def reconcile(
        self,
        arguments: Mapping[str, Any],
        *,
        claim: FenceClaim,
        provider_operation_id: str | None,
        idempotency_key: str | None,
    ) -> Mapping[str, Any] | None: ...


class ToolLedger(Protocol):
    async def prepare(self, record: ToolExecutionRecord) -> ToolExecutionRecord:
        """Insert/return an intent; reject argument changes for the same execution step."""

    async def mark_running(self, tool_call_id: str, claim: FenceClaim) -> ToolExecutionRecord:
        """Persist running after an atomic fence check and before the external request."""

    async def complete(
        self,
        tool_call_id: str,
        result: Mapping[str, Any],
        *,
        provider_operation_id: str | None = None,
    ) -> ToolExecutionRecord: ...

    async def fail(self, tool_call_id: str, code: str) -> ToolExecutionRecord: ...

    async def unknown(
        self, tool_call_id: str, code: str, *, manual_review: bool = True
    ) -> ToolExecutionRecord: ...


def deterministic_tool_call_id(claim: FenceClaim, step: int) -> str:
    if step < 0:
        raise ValueError("tool step must be non-negative")
    raw = f"{claim.tenant_id}\x1f{claim.inbox_id}\x1f{claim.execution_id}\x1f{step}"
    return "tool_" + hashlib.sha256(raw.encode()).hexdigest()[:32]


def arguments_hash(arguments: Mapping[str, Any]) -> str:
    try:
        encoded = json.dumps(
            arguments,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            default=_reject_non_json,
        )
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("tool arguments must be JSON values") from exc
    return hashlib.sha256(encoded.encode()).hexdigest()


def _reject_non_json(value: object) -> None:
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


class InMemoryToolLedger:
    """Concurrency-safe functional ledger with the same invariants as ``tool_execution``."""

    def __init__(self, fence_verifier: FenceVerifier | None = None) -> None:
        self._records: dict[str, ToolExecutionRecord] = {}
        self._step_index: dict[tuple[str, str, int], str] = {}
        self._lock = asyncio.Lock()
        self._fence = fence_verifier or AlwaysValidFence()

    async def prepare(self, record: ToolExecutionRecord) -> ToolExecutionRecord:
        async with self._lock:
            key = (record.tenant_id, record.execution_id, record.tool_step)
            known_id = self._step_index.get(key)
            if known_id:
                existing = self._records[known_id]
                if (
                    existing.arguments_hash != record.arguments_hash
                    or existing.tool_name != record.tool_name
                ):
                    raise ExecutionDivergence(
                        "same deterministic tool step has different arguments or tool"
                    )
                return existing
            if record.tool_call_id in self._records:
                return self._records[record.tool_call_id]
            self._records[record.tool_call_id] = record
            self._step_index[key] = record.tool_call_id
            return record

    async def mark_running(self, tool_call_id: str, claim: FenceClaim) -> ToolExecutionRecord:
        if not await self._fence.verify(claim):
            raise LostFence("tool intent cannot start after session fence was lost")
        async with self._lock:
            existing = self._records[tool_call_id]
            _assert_claim_matches(existing, claim)
            if existing.status not in {"prepared", "confirmed"}:
                return existing
            updated = replace(existing, status="running", updated_at=datetime.now(UTC))
            self._records[tool_call_id] = updated
            return updated

    async def complete(
        self,
        tool_call_id: str,
        result: Mapping[str, Any],
        *,
        provider_operation_id: str | None = None,
    ) -> ToolExecutionRecord:
        async with self._lock:
            existing = self._records[tool_call_id]
            updated = replace(
                existing,
                status="succeeded",
                result=dict(result),
                provider_operation_id=provider_operation_id or existing.provider_operation_id,
                last_error_code=None,
                updated_at=datetime.now(UTC),
            )
            self._records[tool_call_id] = updated
            return updated

    async def fail(self, tool_call_id: str, code: str) -> ToolExecutionRecord:
        async with self._lock:
            existing = self._records[tool_call_id]
            updated = replace(
                existing, status="failed", last_error_code=code, updated_at=datetime.now(UTC)
            )
            self._records[tool_call_id] = updated
            return updated

    async def unknown(
        self, tool_call_id: str, code: str, *, manual_review: bool = True
    ) -> ToolExecutionRecord:
        async with self._lock:
            existing = self._records[tool_call_id]
            updated = replace(
                existing,
                status="manual_review" if manual_review else "unknown",
                last_error_code=code,
                updated_at=datetime.now(UTC),
            )
            self._records[tool_call_id] = updated
            return updated

    async def get(self, tool_call_id: str) -> ToolExecutionRecord | None:
        async with self._lock:
            return self._records.get(tool_call_id)


def _assert_claim_matches(record: ToolExecutionRecord, claim: FenceClaim) -> None:
    if (
        record.tenant_id != claim.tenant_id
        or record.inbox_id != claim.inbox_id
        or record.execution_id != claim.execution_id
        or record.session_id != claim.session_id
        or record.lease_fence != claim.lease_fence
        or record.routing_epoch != claim.routing_epoch
        or record.security_epoch != claim.security_epoch
    ):
        raise LostFence("Tool intent claim does not match the current fenced execution")


@dataclass(frozen=True, slots=True)
class ToolPolicy:
    allowlist: frozenset[str]
    denylist: frozenset[str] = frozenset()
    required_roles: Mapping[str, frozenset[str]] = field(default_factory=dict)
    confirmation_required: frozenset[str] = frozenset()
    parameter_rules: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_release(
        cls, policy: Mapping[str, Any], *, live_denylist: Sequence[str] = ()
    ) -> ToolPolicy:
        allow = policy.get("allow") or policy.get("allowlist") or []
        deny = list(policy.get("deny") or policy.get("denylist") or []) + list(live_denylist)
        roles_raw = policy.get("required_roles") or policy.get("roles") or {}
        roles = {
            str(name): frozenset(str(role) for role in required)
            for name, required in roles_raw.items()
            if isinstance(required, Sequence) and not isinstance(required, str)
        }
        confirmation = policy.get("confirm") or policy.get("confirmation_required") or []
        rules = policy.get("parameter_rules") or {}
        return cls(
            allowlist=frozenset(str(item) for item in allow),
            denylist=frozenset(str(item) for item in deny),
            required_roles=roles,
            confirmation_required=frozenset(str(item) for item in confirmation),
            parameter_rules={
                str(key): value for key, value in rules.items() if isinstance(value, Mapping)
            },
        )


class ToolPolicyFilter:
    """Enforces allow/deny, principal authorization, confirmation, and JSON parameters."""

    def __init__(self, policy: ToolPolicy) -> None:
        self._policy = policy

    def authorize(self, tool: Tool, invocation: ToolInvocation) -> None:
        if tool.name in self._policy.denylist:
            raise ToolDenied(f"tool {tool.name!r} is denied by the live security envelope")
        if tool.name not in self._policy.allowlist:
            raise ToolDenied(f"tool {tool.name!r} is not in the release allowlist")
        required_roles = self._policy.required_roles.get(tool.name, frozenset())
        if required_roles and not required_roles.intersection(invocation.principal_roles):
            raise ToolDenied(f"principal lacks a required role for {tool.name!r}")
        _validate_json_schema(invocation.arguments, tool.parameters)
        custom_rule = self._policy.parameter_rules.get(tool.name)
        if custom_rule:
            _validate_json_schema(invocation.arguments, custom_rule)
        if tool.name in self._policy.confirmation_required and not invocation.confirmed:
            raise ConfirmationRequired(f"tool {tool.name!r} requires an explicit confirmation")


class ToolRegistry:
    def __init__(self, tools: Sequence[Tool] = ()) -> None:
        self._tools = {tool.name: tool for tool in tools}

    def register(self, tool: Tool) -> None:
        if tool.name in self._tools:
            raise ValueError(f"duplicate tool registration: {tool.name}")
        self._tools[tool.name] = tool

    def get(self, name: str) -> Tool:
        try:
            return self._tools[name]
        except KeyError as exc:
            raise ToolDenied(f"unknown tool: {name!r}") from exc

    def values(self) -> tuple[Tool, ...]:
        return tuple(self._tools.values())


class ToolExecutor:
    """Executes registered tools following persistent intent and recovery rules."""

    def __init__(
        self,
        registry: ToolRegistry,
        ledger: ToolLedger,
        fence_verifier: FenceVerifier | None = None,
    ) -> None:
        self._registry = registry
        self._ledger = ledger
        self._fence = fence_verifier or AlwaysValidFence()

    async def execute(
        self, claim: FenceClaim, invocation: ToolInvocation, policy: ToolPolicy
    ) -> ToolOutcome:
        tool = self._registry.get(invocation.name)
        ToolPolicyFilter(policy).authorize(tool, invocation)
        if not await self._fence.verify(claim):
            raise LostFence("Tool call blocked because the execution fence is no longer valid")
        call_id = deterministic_tool_call_id(claim, invocation.step)
        trace = current_trace_context()
        record = ToolExecutionRecord(
            tenant_id=claim.tenant_id,
            tool_call_id=call_id,
            inbox_id=claim.inbox_id,
            execution_id=claim.execution_id,
            session_id=claim.session_id,
            tool_step=invocation.step,
            tool_name=tool.name,
            arguments_hash=arguments_hash(invocation.arguments),
            retry_capability=tool.retry_capability,
            provider_idempotency_key=call_id
            if tool.retry_capability is RetryCapability.IDEMPOTENT
            else None,
            lease_fence=claim.lease_fence,
            routing_epoch=claim.routing_epoch,
            security_epoch=claim.security_epoch,
            trace_id=invocation.trace_id or trace.trace_id,
        )
        record = await self._ledger.prepare(record)
        if record.status == "succeeded":
            return ToolOutcome("succeeded", call_id, result=record.result)
        if record.status in {"unknown", "manual_review", "reconciling"}:
            return ToolOutcome(
                record.status,
                call_id,
                error_code=record.last_error_code,
                requires_manual_resolution=True,
            )
        if record.status == "running":
            return await self._recover_running(tool, record, claim, invocation)

        # This durable state transition is the last action before the provider boundary.
        record = await self._ledger.mark_running(call_id, claim)
        if record.status == "succeeded":
            return ToolOutcome("succeeded", call_id, result=record.result)
        if (
            record.status == "running" and record.updated_at != record.updated_at
        ):  # defensive unreachable branch
            raise RuntimeError("invalid tool ledger timestamp")
        with traced(
            "tool.call", tool_name=tool.name, tenant_id=claim.tenant_id, tool_call_id=call_id
        ):
            try:
                result = await tool.call(
                    invocation.arguments,
                    claim=claim,
                    idempotency_key=record.provider_idempotency_key,
                )
            except AmbiguousToolOutcome as exc:
                updated = await self._ledger.unknown(call_id, exc.code, manual_review=True)
                return ToolOutcome(
                    updated.status, call_id, error_code=exc.code, requires_manual_resolution=True
                )
            except ToolError as exc:
                updated = await self._ledger.fail(call_id, exc.code)
                return ToolOutcome(updated.status, call_id, error_code=exc.code)
            except Exception:
                # A generic transport exception after "running" is necessarily ambiguous
                # for a side-effect Tool.  Do not silently convert it to a safe retry.
                updated = await self._ledger.unknown(
                    call_id, "provider_transport_ambiguous", manual_review=True
                )
                return ToolOutcome(
                    updated.status,
                    call_id,
                    error_code=updated.last_error_code,
                    requires_manual_resolution=True,
                )
        updated = await self._ledger.complete(
            call_id, result, provider_operation_id=_provider_operation_id(result)
        )
        return ToolOutcome("succeeded", call_id, result=updated.result)

    async def _recover_running(
        self, tool: Tool, record: ToolExecutionRecord, claim: FenceClaim, invocation: ToolInvocation
    ) -> ToolOutcome:
        """Never recreate a running request without first applying capability-specific recovery."""

        if not await self._fence.verify(claim):
            raise LostFence("recovery blocked because the execution fence is no longer valid")
        if tool.retry_capability is RetryCapability.NON_RETRIABLE:
            updated = await self._ledger.unknown(
                record.tool_call_id, "non_retriable_running_after_crash", manual_review=True
            )
            return ToolOutcome(
                updated.status,
                record.tool_call_id,
                error_code=updated.last_error_code,
                requires_manual_resolution=True,
            )
        reconciled = await tool.reconcile(
            invocation.arguments,
            claim=claim,
            provider_operation_id=record.provider_operation_id,
            idempotency_key=record.provider_idempotency_key,
        )
        if reconciled is not None:
            updated = await self._ledger.complete(
                record.tool_call_id,
                reconciled,
                provider_operation_id=_provider_operation_id(reconciled),
            )
            return ToolOutcome("succeeded", record.tool_call_id, result=updated.result)
        if tool.retry_capability is RetryCapability.IDEMPOTENT:
            # Safe only because the provider receives the unchanged deterministic key.
            try:
                result = await tool.call(
                    invocation.arguments,
                    claim=claim,
                    idempotency_key=record.provider_idempotency_key,
                )
            except Exception:
                updated = await self._ledger.unknown(
                    record.tool_call_id, "idempotent_recovery_unresolved", manual_review=False
                )
                return ToolOutcome(
                    updated.status,
                    record.tool_call_id,
                    error_code=updated.last_error_code,
                    requires_manual_resolution=True,
                )
            updated = await self._ledger.complete(
                record.tool_call_id, result, provider_operation_id=_provider_operation_id(result)
            )
            return ToolOutcome("succeeded", record.tool_call_id, result=updated.result)
        # Queryable but not idempotent: no reconciled operation is insufficient proof to
        # send another side effect.  A human or provider-specific reconciler decides.
        updated = await self._ledger.unknown(
            record.tool_call_id, "queryable_reconciliation_inconclusive", manual_review=True
        )
        return ToolOutcome(
            updated.status,
            record.tool_call_id,
            error_code=updated.last_error_code,
            requires_manual_resolution=True,
        )


def _provider_operation_id(result: Mapping[str, Any]) -> str | None:
    raw = result.get("provider_operation_id")
    return str(raw) if raw is not None else None


def _validate_json_schema(value: Any, schema: Mapping[str, Any], path: str = "arguments") -> None:
    """Small fail-closed JSON Schema subset sufficient for platform Tool parameters."""

    expected = schema.get("type")
    if expected == "object":
        if not isinstance(value, Mapping):
            raise ToolValidationError(f"{path} must be an object")
        properties = (
            schema.get("properties") if isinstance(schema.get("properties"), Mapping) else {}
        )
        required = schema.get("required") if isinstance(schema.get("required"), Sequence) else []
        for name in required:
            if str(name) not in value:
                raise ToolValidationError(f"{path}.{name} is required")
        if schema.get("additionalProperties") is False:
            unexpected = set(value) - set(properties)
            if unexpected:
                raise ToolValidationError(
                    f"{path} contains unsupported fields: {sorted(unexpected)!r}"
                )
        for name, item in value.items():
            child_schema = properties.get(str(name)) if isinstance(properties, Mapping) else None
            if isinstance(child_schema, Mapping):
                _validate_json_schema(item, child_schema, f"{path}.{name}")
    elif expected == "array":
        if not isinstance(value, list):
            raise ToolValidationError(f"{path} must be an array")
        item_schema = schema.get("items")
        if isinstance(item_schema, Mapping):
            for index, item in enumerate(value):
                _validate_json_schema(item, item_schema, f"{path}[{index}]")
    elif expected == "string" and not isinstance(value, str):
        raise ToolValidationError(f"{path} must be a string")
    elif expected == "integer" and (not isinstance(value, int) or isinstance(value, bool)):
        raise ToolValidationError(f"{path} must be an integer")
    elif expected == "number" and (not isinstance(value, (int, float)) or isinstance(value, bool)):
        raise ToolValidationError(f"{path} must be a number")
    elif expected == "boolean" and not isinstance(value, bool):
        raise ToolValidationError(f"{path} must be a boolean")
    enum = schema.get("enum")
    if isinstance(enum, Sequence) and not isinstance(enum, str) and value not in enum:
        raise ToolValidationError(f"{path} is not an allowed value")
    max_length = schema.get("maxLength")
    if isinstance(max_length, int) and isinstance(value, str) and len(value) > max_length:
        raise ToolValidationError(f"{path} exceeds maximum length")


class TicketLookupTool:
    """Safe tenant-scoped read-only Tool used by the demo and test suite."""

    name = "ticket.lookup"
    description = "Look up a ticket in the current tenant only."
    parameters: Mapping[str, Any] = {
        "type": "object",
        "properties": {"ticket_id": {"type": "string", "maxLength": 128}},
        "required": ["ticket_id"],
        "additionalProperties": False,
    }
    retry_capability = RetryCapability.IDEMPOTENT

    def __init__(
        self, tickets_by_tenant: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None
    ) -> None:
        self._tickets = {
            tenant: {ticket: dict(value) for ticket, value in items.items()}
            for tenant, items in (tickets_by_tenant or {}).items()
        }

    async def call(
        self, arguments: Mapping[str, Any], *, claim: FenceClaim, idempotency_key: str | None
    ) -> Mapping[str, Any]:
        ticket_id = str(arguments["ticket_id"])
        ticket = self._tickets.get(claim.tenant_id, {}).get(ticket_id)
        if ticket is None:
            return {"found": False, "ticket_id": ticket_id}
        return {"found": True, "ticket_id": ticket_id, "ticket": dict(ticket)}

    async def reconcile(
        self,
        arguments: Mapping[str, Any],
        *,
        claim: FenceClaim,
        provider_operation_id: str | None,
        idempotency_key: str | None,
    ) -> Mapping[str, Any] | None:
        return await self.call(arguments, claim=claim, idempotency_key=idempotency_key)


class MockSideEffectTool:
    """Example provider whose ambiguous non-idempotent effects are deliberately unknown."""

    name = "mock.side_effect"
    description = "Create a mock external effect for recovery-path demonstrations."
    parameters: Mapping[str, Any] = {
        "type": "object",
        "properties": {
            "value": {"type": "string", "maxLength": 256},
            "simulate": {"type": "string", "enum": ["ok", "ambiguous"]},
        },
        "required": ["value"],
        "additionalProperties": False,
    }
    retry_capability = RetryCapability.NON_RETRIABLE

    def __init__(self) -> None:
        self.effects: list[Mapping[str, Any]] = []

    async def call(
        self, arguments: Mapping[str, Any], *, claim: FenceClaim, idempotency_key: str | None
    ) -> Mapping[str, Any]:
        operation_id = (
            "effect_"
            + hashlib.sha256(
                f"{claim.tenant_id}\x1f{claim.execution_id}\x1f{len(self.effects)}".encode()
            ).hexdigest()[:16]
        )
        effect = {
            "provider_operation_id": operation_id,
            "tenant_id": claim.tenant_id,
            "value": str(arguments["value"]),
        }
        self.effects.append(effect)
        if arguments.get("simulate") == "ambiguous":
            # Simulates a provider accepting the effect just before a connection loss.
            raise AmbiguousToolOutcome("mock provider accepted but did not acknowledge the effect")
        return {"accepted": True, **effect}

    async def reconcile(
        self,
        arguments: Mapping[str, Any],
        *,
        claim: FenceClaim,
        provider_operation_id: str | None,
        idempotency_key: str | None,
    ) -> Mapping[str, Any] | None:
        if provider_operation_id:
            return next(
                (
                    effect
                    for effect in self.effects
                    if effect["provider_operation_id"] == provider_operation_id
                ),
                None,
            )
        return None
