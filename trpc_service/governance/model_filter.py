"""Official Model Filter integration with explicit provider accounting contracts."""

from dataclasses import dataclass
from contextlib import nullcontext
import asyncio
import uuid

from trpc_agent_sdk.filter import FilterType

from .filters import TenantBoundaryFilter
from .budget import BudgetDenied, BudgetConflict, digest


@dataclass(frozen=True)
class RequestEstimate:
    """A provider adapter must bound its final wire payload, including tool schemas.

    No approximate tokenizer is supplied as a universal cost upper bound. The
    adapter must enforce this output limit and disable all transport retries.
    """
    request_hash: str
    max_input_tokens: int
    max_output_tokens: int


class ModelCallFailed(RuntimeError):
    pass


async def database_call(function, *args, **kwargs):
    """Keep lease heartbeats responsive; cancellation waits for the SQL result.

    Callers update lifecycle flags inside the thread before this helper propagates
    cancellation, so a committed reservation or dispatch permit cannot be forgotten.
    """
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    interrupted = None
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError as error:
            interrupted = error
    if interrupted:
        raise interrupted
    return result


def text_usage(response):
    """Accept only complete, simple text usage covered by the two-rate price schema."""
    usage = response.usage_metadata
    if usage is None or response.partial:
        return None
    if any(
            getattr(usage, name, None)
            for name in ("cached_content_token_count", "thoughts_token_count", "tool_use_prompt_token_count",
                         "cache_read_input_tokens", "cache_creation_input_tokens")):
        return None
    incoming, outgoing = usage.prompt_token_count, usage.candidates_token_count
    if incoming is None or outgoing is None:
        return None
    if usage.total_token_count is not None and usage.total_token_count != incoming + outgoing:
        return None
    return incoming, outgoing, digest(usage.model_dump(mode="json"))


class ModelBudgetFilter(TenantBoundaryFilter):
    """One invocation creates one durable attempt, even when its caller retries.

    Construction requires a provider-specific estimator; it must reject unsupported
    billing dimensions before network I/O. This filter never supplies free fallback
    usage or inferred prices. No request-specific state is held on this instance.
    """

    def __init__(self,
                 tenant_id,
                 app_id,
                 ledger,
                 model_id,
                 price_id,
                 estimator,
                 *,
                 max_calls=20,
                 authorize=None,
                 accounting=None):
        super().__init__(tenant_id, app_id, filter_type=FilterType.MODEL, authorize=authorize)
        self.name = "model_budget"
        self.ledger, self.model_id, self.price_id = ledger, model_id, price_id
        self.estimator, self.max_calls = estimator, max_calls
        self.accounting = accounting

    async def run_stream(self, ctx, req, handle):
        with self.accounting.capture() if self.accounting else nullcontext() as capture:
            async for envelope in self._run_accounted(ctx, req, handle, capture):
                yield envelope

    async def _run_accounted(self, ctx, req, handle, capture):
        await self.check(ctx)
        execution = ctx.metadata.get("execution_id")
        if not execution:
            raise BudgetDenied("trusted execution identity is missing")
        estimate = self.estimator(req)
        if not isinstance(estimate, RequestEstimate):
            raise BudgetDenied("model has no supported request accounting contract")
        attempt_id = uuid.uuid4().hex
        reserved = False
        sent = False
        finished = False
        evidence = None
        unverifiable_usage = False
        response_failed = False
        finals = []

        def reserve():
            nonlocal reserved
            self.ledger.reserve(self.tenant_id,
                                attempt_id,
                                execution,
                                self.price_id,
                                self.model_id,
                                estimate.request_hash,
                                estimate.max_input_tokens,
                                estimate.max_output_tokens,
                                max_calls=self.max_calls)
            reserved = True

        def dispatch():
            nonlocal sent
            self.ledger.mark_sent(self.tenant_id, attempt_id)
            sent = True

        try:
            await database_call(reserve)
            await database_call(dispatch)
            async for envelope in handle():
                if envelope.error or not envelope.is_continue:
                    raise ModelCallFailed("model filter failed; cost requires reconciliation")
                response = envelope.rsp
                if response is None:
                    continue
                response_failed = response_failed or bool(response.error_code or response.interrupted)
                if response.usage_metadata is not None and not response.partial:
                    observed = capture.evidence(response) if capture else text_usage(response)
                    if observed is None or evidence is not None and observed != evidence:
                        unverifiable_usage = True
                    else:
                        evidence = observed
                if response.partial:
                    yield envelope
                else:
                    finals.append(envelope)
            if evidence is not None and not unverifiable_usage:
                result = await database_call(self.ledger.settle,
                                             self.tenant_id,
                                             attempt_id,
                                             *evidence[:3],
                                             provider_cost=evidence[3] if len(evidence) == 4 else None)
                finished = True
                if result["overrun"]:
                    raise BudgetDenied("model exceeded its accounting contract; further calls blocked")
                if response_failed:
                    raise ModelCallFailed("model response failed; verified usage was settled")
                for envelope in finals:
                    yield envelope
            else:
                await database_call(self.ledger.pending, self.tenant_id, attempt_id)
                finished = True
                raise ModelCallFailed("model usage unavailable; cost requires reconciliation")
        except (BudgetConflict, BudgetDenied, ModelCallFailed):
            raise
        except Exception:
            # Do not let upstream URL/token/body-bearing exception strings escape.
            raise ModelCallFailed("model request failed; cost requires reconciliation") from None
        finally:
            try:
                if capture is not None and getattr(capture, "generation_id", None) and sent:
                    await database_call(self.ledger.record_provider_receipt, self.tenant_id, attempt_id,
                                        capture.generation_id)
            finally:
                if reserved and not finished:
                    if sent:
                        await database_call(self.ledger.pending, self.tenant_id, attempt_id)
                    else:
                        await database_call(self.ledger.cancel_unsent, self.tenant_id, attempt_id)


def attach_budget_filter(model, guard):
    """SDK retry loop is inside Model Filters, so implicit retry must be disabled."""
    if model.model_retry_config is None or model.model_retry_config.num_retries != 0:
        raise ValueError("budgeted model requires explicit zero SDK retries")
    from trpc_service.telemetry.sdk_logging import configure_sdk_logging
    configure_sdk_logging()
    model.add_one_filter(guard)
    return model
