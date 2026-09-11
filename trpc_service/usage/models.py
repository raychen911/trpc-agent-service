"""Usage accounting models (Stage 6C).

Token counts come exclusively from ``Event.usage_metadata`` (the SDK's
``GenerateContentResponseUsageMetadata``: ``prompt_token_count`` /
``candidates_token_count`` / ``total_token_count`` — verified against
``trpc-agent-python`` ``agents/core/_llm_processor.py`` (Event copies
``response.usage_metadata``) and ``models/_openai_model.py`` (streaming
usage is attached once to the final accumulated ``partial=False``
response; tool-call streams return usage on a no-choices chunk)).

The SDK never fabricates usage: a provider that omits it yields
``usage_metadata is None``.  This module mirrors that honesty — missing
tokens or a missing price are recorded as ``None`` (unknown), never zero.
Costs are integer micro-currency units only; floats are rejected at every
boundary.
"""

from __future__ import annotations

from datetime import date, datetime
from uuid import UUID
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt


class UsageIncrement(BaseModel):
    """One request's contribution to the daily aggregate.

    ``input_tokens`` / ``output_tokens`` / ``cost_microunits`` are None when
    the provider reported no usage or the price profile is missing — SQL
    NULL propagation keeps the day "unknown" instead of undercounting.
    ``requests`` is always a real counted request (never unknown).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    usage_date: date
    tenant_id: str
    model_profile: str
    requests: StrictInt = Field(ge=0)
    input_tokens: StrictInt | None = Field(default=None, ge=0)
    output_tokens: StrictInt | None = Field(default=None, ge=0)
    total_tokens: StrictInt | None = Field(default=None, ge=0)
    cost_microunits: StrictInt | None = Field(default=None, ge=0)
    cost_state: Literal["known", "unknown", "not_applicable"] = "not_applicable"
    # A request fact is stored alongside the daily aggregate.  Keeping this
    # identity here makes the aggregate + fact one repository operation.
    request_id: UUID | None = None
    receipt_id: UUID | None = None
    config_version: StrictInt | None = Field(default=None, ge=1)
    occurred_at: datetime | None = None

    @classmethod
    def for_request(
        cls,
        *,
        usage_date: date,
        tenant_id: str,
        model_profile: str,
        input_tokens: int | None,
        output_tokens: int | None,
        cost_microunits: int | None,
    ) -> "UsageIncrement":
        cost_state: str = "not_applicable"
        if input_tokens is not None and output_tokens is not None:
            cost_state = "known" if cost_microunits is not None else "unknown"
        return cls(
            usage_date=usage_date,
            tenant_id=tenant_id,
            model_profile=model_profile,
            requests=1,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=None if input_tokens is None or output_tokens is None else input_tokens + output_tokens,
            cost_microunits=cost_microunits,
            cost_state=cost_state,  # type: ignore[arg-type]
        )


class TenantDailyUsage(BaseModel):
    """Aggregated daily usage rows for one tenant (all model profiles)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    usage_date: date
    tenant_id: str
    profiles: tuple[ProfileDailyUsage, ...]

    @property
    def total_requests(self) -> int:
        return sum(p.requests for p in self.profiles)

    def tokens_or_none(self) -> tuple[int | None, int | None]:
        """(input, output) sums with NULL propagation — any unknown row
        makes the tenant-day total unknown (never an undercount presented
        as fact)."""
        inputs: list[int] = []
        outputs: list[int] = []
        for p in self.profiles:
            if p.input_tokens is None or p.output_tokens is None:
                return (None, None)
            inputs.append(p.input_tokens)
            outputs.append(p.output_tokens)
        return (sum(inputs), sum(outputs))

    def cost_microunits_or_none(self) -> int | None:
        total = 0
        for p in self.profiles:
            if p.cost_microunits is None:
                return None
            total += p.cost_microunits
        return total


class RequestUsageRecord(BaseModel):
    """One tenant-scoped request usage fact; unknown values are SQL NULL."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    tenant_id: str
    request_id: UUID
    receipt_id: UUID | None
    config_version: StrictInt
    model_profile: str
    input_tokens: StrictInt | None = Field(default=None, ge=0)
    output_tokens: StrictInt | None = Field(default=None, ge=0)
    cost_microunits: StrictInt | None = Field(default=None, ge=0)
    occurred_at: datetime


class ProfileDailyUsage(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    model_profile: str
    requests: StrictInt
    input_tokens: StrictInt | None
    output_tokens: StrictInt | None
    cost_microunits: StrictInt | None

    @property
    def cost_state(self) -> Literal["known", "unknown"]:
        return "known" if self.cost_microunits is not None else "unknown"


class UsageAccumulator:
    """Deduplicates per-event usage facts within one invocation.

    The SDK emits usage exactly once per LLM call on a final (non-partial)
    event whose ``id`` is that call's event id; multi-call tool loops produce
    several such ids.  A repeated id (final re-emission of the same event)
    contributes only the first time.  A call whose usage is missing marks
    the affected counters unknown — partial sums are never presented as
    totals.
    """

    def __init__(self) -> None:
        self._seen_ids: set[str] = set()
        self.input_tokens: int | None = 0
        self.output_tokens: int | None = 0
        # An event was consumed from the agent stream at all (model turn or
        # not): distinguishes "no LLM executed" (record nothing) from "LLM
        # ran but reported no usage" (record unknown).
        self.stream_consumed = False
        # A usage-bearing event declared incomplete fields.
        self._degraded = False

    def add_event(self, event_id: str | None, usage_metadata: object | None) -> None:
        if usage_metadata is not None:
            from trpc_agent_sdk.types import GenerateContentResponseUsageMetadata

            if not isinstance(usage_metadata, GenerateContentResponseUsageMetadata):
                # Foreign/test object (e.g. an unconfigured mock attribute) —
                # not a usage fact at all.
                return None
        if event_id is None:
            self.stream_consumed = True
            return
        if usage_metadata is None:
            # Partial chunks, tool-response turns and stream bookkeeping
            # events carry no usage by design; only an ENTIRE execution
            # without any usage event is unknowable (see snapshot()).
            self.stream_consumed = True
            return
        if event_id in self._seen_ids:
            return
        self._seen_ids.add(event_id)
        self.stream_consumed = True
        prompt = getattr(usage_metadata, "prompt_token_count", None)
        candidates = getattr(usage_metadata, "candidates_token_count", None)
        if prompt is None or candidates is None:
            self._degraded = True
            return
        self.input_tokens = (self.input_tokens or 0) + int(prompt)
        self.output_tokens = (self.output_tokens or 0) + int(candidates)

    @property
    def usage_seen(self) -> bool:
        return bool(self._seen_ids) or self.stream_consumed

    def snapshot(self) -> tuple[int | None, int | None]:
        """(input, output) totals.  Unknown — never a fabricated zero — when
        nothing at all was observed, when the execution consumed the stream
        but NO usage event arrived, or when any usage event was
        field-incomplete."""
        if self._degraded or not self._seen_ids:
            return (None, None)
        return (self.input_tokens, self.output_tokens)


__all__ = [
    "ProfileDailyUsage",
    "RequestUsageRecord",
    "TenantDailyUsage",
    "UsageAccumulator",
    "UsageIncrement",
]
