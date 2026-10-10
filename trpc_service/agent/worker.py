"""Stateless worker orchestration with leases, heartbeats, and fenced commit."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import inspect
import logging
import re
from collections.abc import Awaitable, Mapping
from datetime import timedelta
from enum import StrEnum
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid4, uuid5

from trpc_agent_sdk.agents import BaseAgent

from trpc_service.agent.media import MediaExtractor, MediaLimits
from trpc_service.agent.registry import RevisionRegistry
from trpc_service.agent.runner import (
    AgentLoader,
    PreparedMedia,
    QueryEmbeddingProvider,
    TenantRunner,
)
from trpc_service.channels.envelopes import MediaReference, OutboundEnvelope, PayloadKind
from trpc_service.metrics.privacy import inject_trace_headers
from trpc_service.metrics.prometheus import LEASE_CONFLICTS, TENANT_COST, TOKENS, TURN_LATENCY
from trpc_service.metrics.telemetry import get_tracer, mark_span_error, stable_tenant_label
from trpc_service.storage.models import (
    Acceptance,
    CommitResult,
    SessionClaim,
    SessionLease,
    TurnCommit,
)
from trpc_service.storage.protocols import FencingConflict, RuntimeRepository
from trpc_service.storage.services import TenantDataServices, TenantServiceFactory
from trpc_service.tenant.models import Channel, ChannelBinding, TenantConfig
from trpc_service.workspace import WorkspaceManager

_LOGGER = logging.getLogger(__name__)


class DownloadedMedia(Protocol):
    @property
    def data(self) -> bytes: ...

    @property
    def content_type(self) -> str: ...

    @property
    def filename(self) -> str | None: ...


class MediaDownloader(Protocol):
    async def download_media(
        self,
        binding: ChannelBinding,
        message_id: str,
        media_key: str,
        *,
        media_type: str = "file",
        filename: str | None = None,
        media_reference: MediaReference | None = None,
    ) -> DownloadedMedia: ...


class ProcessStatus(StrEnum):
    COMMITTED = "committed"
    BUSY = "busy"
    DUPLICATE = "duplicate"


class WorkerResult:
    def __init__(self, status: ProcessStatus, *, commit: CommitResult | None = None) -> None:
        self.status = status
        self.commit = commit


class AgentWorker:
    def __init__(
        self,
        repository: RuntimeRepository,
        *,
        worker_id: str,
        agent_loader: AgentLoader,
        lease_for: timedelta = timedelta(seconds=60),
        registry: RevisionRegistry[BaseAgent] | None = None,
        service_factory: TenantServiceFactory | None = None,
        media_downloaders: Mapping[Channel, MediaDownloader] | None = None,
        media_extractor: MediaExtractor | None = None,
        workspace_manager: WorkspaceManager | None = None,
        query_embedding_provider: QueryEmbeddingProvider | None = None,
        max_turn_attempts: int = 3,
    ) -> None:
        if max_turn_attempts < 1:
            raise ValueError("max_turn_attempts must be positive")
        self._repository = repository
        self._worker_id = worker_id
        self._agent_loader = agent_loader
        self._lease_for = lease_for
        self._registry = registry or RevisionRegistry()
        self._service_factory = service_factory
        self._media_downloaders = dict(media_downloaders or {})
        self._media_extractor = media_extractor or MediaExtractor()
        self._workspace_manager = workspace_manager
        self._query_embedding_provider = query_embedding_provider
        self._max_turn_attempts = max_turn_attempts

    async def process(self, acceptance: Acceptance) -> WorkerResult:
        """Execute the legacy v1 path, including its inbound-level acquire."""

        return await self._instrument(acceptance, self._process(acceptance))

    async def process_claimed(self, claim: SessionClaim) -> WorkerResult:
        """Execute an already claimed v2 mailbox turn without acquiring again."""

        if not claim.claimed or claim.execution_lease is None:
            raise ValueError("claimed session must include execution lease")
        lease = claim.execution_lease
        try:
            # PostgreSQL v2 deliberately leaves the full inbound envelope out
            # of the short claim transaction.  The Redis delivery has already
            # been ACKed by this point, so hydrate the authoritative record
            # here and let the normal lease recovery path handle a transient
            # read failure.
            acceptance = claim.acceptance
            if acceptance is None:
                acceptance = await self._repository.get_acceptance(
                    lease.tenant_id,
                    lease.inbound_id,
                )
            if acceptance is None:
                raise LookupError("claimed inbound does not exist")
            if (
                acceptance.inbound_id != lease.inbound_id
                or acceptance.context.tenant_id != lease.tenant_id
                or acceptance.context.session_id != lease.session_id
            ):
                raise FencingConflict("claimed inbound does not match session lease")
        except BaseException as error:
            await self._release_mailbox_failure(lease, error)
            raise
        return await self._instrument(
            acceptance,
            self._execute_lease(
                acceptance,
                lease,
                mailbox_runtime=True,
            ),
        )

    async def _instrument(
        self,
        acceptance: Acceptance,
        operation: Awaitable[WorkerResult],
    ) -> WorkerResult:
        started = asyncio.get_running_loop().time()
        outcome = "error"
        channel = acceptance.envelope.channel.value
        tracer = get_tracer()
        with tracer.start_as_current_span("agent.run", attributes={"channel": channel}) as span:
            try:
                result = await operation
                outcome = result.status.value
                return result
            except asyncio.CancelledError:
                outcome = "cancelled"
                mark_span_error(span, "cancelled")
                raise
            except FencingConflict:
                outcome = "fencing_conflict"
                LEASE_CONFLICTS.inc()
                mark_span_error(span, "fencing_conflict")
                raise
            except Exception as exc:
                mark_span_error(span, type(exc).__name__)
                raise
            finally:
                span.set_attribute("outcome", outcome)
                TURN_LATENCY.labels(outcome=outcome).observe(
                    asyncio.get_running_loop().time() - started
                )

    async def _process(self, acceptance: Acceptance) -> WorkerResult:
        lease = await self._repository.acquire(
            acceptance=acceptance,
            worker_id=self._worker_id,
            lease_for=self._lease_for,
        )
        if lease is None:
            if acceptance.duplicate:
                return WorkerResult(ProcessStatus.DUPLICATE)
            refreshed = await self._repository.get_acceptance(
                acceptance.context.tenant_id,
                acceptance.inbound_id,
            )
            if refreshed is not None and refreshed.duplicate:
                return WorkerResult(ProcessStatus.DUPLICATE)
            LEASE_CONFLICTS.inc()
            return WorkerResult(ProcessStatus.BUSY)
        return await self._execute_lease(acceptance, lease, mailbox_runtime=False)

    async def _execute_lease(
        self,
        acceptance: Acceptance,
        lease: SessionLease,
        *,
        mailbox_runtime: bool,
    ) -> WorkerResult:
        current = lease
        heartbeat_error: BaseException | None = None
        stop_heartbeat = asyncio.Event()

        async def heartbeat() -> None:
            nonlocal current, heartbeat_error
            interval = max(self._lease_for.total_seconds() / 3, 0.1)
            try:
                while not stop_heartbeat.is_set():
                    try:
                        await asyncio.wait_for(stop_heartbeat.wait(), timeout=interval)
                    except TimeoutError:
                        if mailbox_runtime:
                            current = await self._repository.renew_session_ready(
                                current,
                                lease_for=self._lease_for,
                            )
                        else:
                            current = await self._repository.renew(
                                current,
                                lease_for=self._lease_for,
                            )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # task boundary; checked before commit
                heartbeat_error = exc

        heartbeat_task = asyncio.create_task(heartbeat())
        turn_task: asyncio.Task[tuple[TenantRunner, str, TenantDataServices | None]] | None = None
        heartbeat_shutdown_attempted = False

        async def stop_heartbeat_task() -> bool:
            """Stop the lease refresher without allowing shutdown to hang the turn."""

            nonlocal heartbeat_shutdown_attempted
            if heartbeat_shutdown_attempted:
                return heartbeat_task.done()
            heartbeat_shutdown_attempted = True
            stop_heartbeat.set()
            if heartbeat_task.done():
                return True
            try:
                await asyncio.wait_for(asyncio.shield(heartbeat_task), timeout=1.0)
            except TimeoutError:
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)
                return False
            return True

        try:

            async def run_turn() -> tuple[TenantRunner, str, TenantDataServices | None]:
                nonlocal current
                if mailbox_runtime and not current.snapshot_hydrated:
                    anchor = current.snapshot
                    snapshot = await self._repository.get_session_snapshot(
                        current.tenant_id,
                        current.session_id,
                    )
                    if snapshot is None:
                        raise LookupError("claimed session does not exist")
                    if (
                        snapshot.tenant_id != current.tenant_id
                        or snapshot.session_id != current.session_id
                        or snapshot.version != anchor.version
                        or snapshot.next_sequence != anchor.next_sequence
                    ):
                        raise FencingConflict("claimed session changed before hydration")
                    current = current.model_copy(
                        update={
                            "snapshot": snapshot,
                            "snapshot_hydrated": True,
                        }
                    )
                    # Confirm the fenced lease after the potentially long
                    # history read and before any model, tool, or media work.
                    current = await self._repository.renew_session_ready(
                        current,
                        lease_for=self._lease_for,
                    )
                config = await self._repository.get_config(
                    acceptance.context.tenant_id,
                    acceptance.context.app_id,
                    acceptance.context.config_version,
                )
                services = (
                    await self._service_factory.for_context(acceptance.context, config)
                    if self._service_factory is not None
                    else None
                )
                prepared_media = await self._prepare_media(acceptance, config, services)
                runner = TenantRunner(
                    config=config,
                    lease=current,
                    registry=self._registry,
                    agent_loader=self._agent_loader,
                    services=services,
                    query_embedding_provider=self._query_embedding_provider,
                    workspace=(
                        self._workspace_manager.for_context(acceptance.context)
                        if self._workspace_manager is not None
                        else None
                    ),
                )
                final_text = ""
                run_options = {"prepared_media": prepared_media} if prepared_media else {}
                async for event in runner.run(
                    acceptance.context,
                    acceptance.envelope,
                    **run_options,
                ):
                    _record_usage(event, acceptance.context.tenant_id)
                    if event.visible and event.is_final_response() and event.get_text():
                        final_text = event.get_text()
                return runner, final_text, services

            turn_task = asyncio.create_task(run_turn())
            done, _ = await asyncio.wait(
                {turn_task, heartbeat_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if turn_task in done:
                runner, final_text, services = await turn_task
            else:
                if turn_task.done():
                    # If both tasks finished together, preserve the turn error.
                    runner, final_text, services = await turn_task
                else:
                    turn_task.cancel()
                    await asyncio.gather(turn_task, return_exceptions=True)
                raise FencingConflict("session heartbeat failed") from heartbeat_error

            heartbeat_stopped = await stop_heartbeat_task()
            if not heartbeat_stopped:
                raise FencingConflict("session heartbeat shutdown timed out")
            if heartbeat_error:
                raise FencingConflict("session heartbeat failed") from heartbeat_error

            outbound = OutboundEnvelope(
                outbound_id=str(uuid4()),
                tenant_id=acceptance.context.tenant_id,
                binding_id=acceptance.context.channel_binding_id,
                channel=acceptance.envelope.channel,
                target_id=_target_id(acceptance),
                session_id=acceptance.context.session_id,
                payload_kind=PayloadKind.TEXT,
                text=final_text or "The agent completed without a text response.",
                in_reply_to=acceptance.envelope.external_message_id,
                trace_headers=_trace_headers(),
            )
            turn_commit = TurnCommit(
                context=acceptance.context,
                lease=current,
                state=runner.state,
                events=runner.buffered_events,
                outbound=outbound,
            )
            result = (
                await self._repository.commit_session_ready(turn_commit)
                if mailbox_runtime
                else await self._repository.commit(turn_commit)
            )
            await self._post_commit_context(
                acceptance,
                current,
                result,
                final_text,
                services,
            )
            return WorkerResult(ProcessStatus.COMMITTED, commit=result)
        except BaseException as error:
            if turn_task is not None and not turn_task.done():
                turn_task.cancel()
                await asyncio.gather(turn_task, return_exceptions=True)
            await stop_heartbeat_task()
            if mailbox_runtime:
                await self._release_mailbox_failure(current, error)
            else:
                await self._repository.fail(current, error_type="agent_turn_failed")
            raise

    async def _post_commit_context(
        self,
        acceptance: Acceptance,
        lease: SessionLease,
        result: CommitResult,
        final_text: str,
        services: TenantDataServices | None,
    ) -> None:
        """Persist derived context only after the fenced turn commit succeeds.

        These writes are intentionally best-effort and independently guarded:
        a broken memory or summary backend must never turn an already committed
        session turn into a failed turn.  Stable IDs and source sequence make
        retries idempotent and prevent an older projection from winning.
        """

        if services is None:
            return
        sequence = result.last_sequence
        if sequence is None:
            sequence = max(0, lease.snapshot.next_sequence - 1)
        tenant_id = acceptance.context.tenant_id
        principal_id = acceptance.context.principal_id
        session_id = acceptance.context.session_id
        memory_put = getattr(services.memory, "put", None)
        if callable(memory_put):
            try:
                memory_id = str(uuid5(NAMESPACE_URL, f"trpc-agent-turn:{lease.turn_id}"))
                await _maybe_await(
                    memory_put(
                        tenant_id,
                        principal_id,
                        {
                            "turn_id": lease.turn_id,
                            "session_id": session_id,
                            "user_message": _bounded_text(acceptance.envelope.text),
                            "agent_response": _bounded_text(final_text),
                        },
                        memory_id=memory_id,
                        session_id=session_id,
                        source_sequence=sequence,
                    )
                )
            except BaseException as error:
                _LOGGER.warning("memory post-turn write skipped: %s", type(error).__name__)

        summary_get = getattr(services.summary, "get", None)
        summary_put = getattr(services.summary, "put", None)
        if not callable(summary_put):
            return
        previous = None
        if callable(summary_get):
            try:
                previous = await _maybe_await(summary_get(tenant_id, session_id))
            except BaseException as error:
                _LOGGER.warning("summary post-turn read skipped: %s", type(error).__name__)
        if previous is not None and getattr(previous, "up_to_sequence", -1) >= sequence:
            return
        expected_version = getattr(previous, "version", None)
        try:
            await _maybe_await(
                summary_put(
                    tenant_id,
                    session_id,
                    up_to_sequence=sequence,
                    summary={
                        "turn_id": lease.turn_id,
                        "last_user_message": _bounded_text(acceptance.envelope.text),
                        "last_agent_response": _bounded_text(final_text),
                    },
                    expected_version=expected_version,
                )
            )
        except BaseException as error:
            _LOGGER.warning("summary post-turn write skipped: %s", type(error).__name__)

    async def _release_mailbox_failure(
        self,
        lease: SessionLease,
        error: BaseException,
    ) -> None:
        """Release a v2 lease without sleeping in an Agent execution slot."""

        error_type = _safe_error_type(error)
        try:
            if lease.attempt >= self._max_turn_attempts:
                await self._repository.fail_session_ready(lease, error_type=error_type)
                return
            delay_seconds = min(2 ** max(lease.attempt - 1, 0), 60)
            await self._repository.retry_session_ready(
                lease,
                error_type=error_type,
                delay=timedelta(seconds=delay_seconds),
            )
        except FencingConflict:
            # The PG sweeper or a replacement worker already owns recovery.
            return

    async def _prepare_media(
        self,
        acceptance: Acceptance,
        config: TenantConfig,
        services: Any | None,
    ) -> tuple[PreparedMedia, ...]:
        references = acceptance.envelope.media
        policy = config.media
        if not references or not policy.enabled or services is None:
            return ()
        downloader = self._media_downloaders.get(acceptance.envelope.channel)
        if downloader is None:
            return ()
        route = await self._repository.resolve_binding(acceptance.context.channel_binding_id)
        if route is None or route.binding.tenant_id != acceptance.context.tenant_id:
            raise LookupError("media channel binding is unavailable")

        prepared: list[PreparedMedia] = []
        total_bytes = 0
        remaining_chars = policy.max_extracted_chars
        selected = references[: policy.max_items_per_turn]
        for index, reference in enumerate(selected):
            provider_id = reference.provider_media_id
            if not provider_id and reference.provider_url:
                provider_id = _stable_media_key(reference.provider_url)
            if not provider_id:
                prepared.append(_unavailable_media(reference, "provider media id is unavailable"))
                continue
            try:
                download_kwargs: dict[str, Any] = {
                    "media_type": _resource_type(acceptance.envelope.payload_kind, reference),
                    "filename": reference.filename,
                }
                if _supports_media_reference(downloader):
                    download_kwargs["media_reference"] = reference
                downloaded = await downloader.download_media(
                    route.binding,
                    acceptance.envelope.external_message_id,
                    provider_id,
                    **download_kwargs,
                )
            except Exception as exc:
                await _audit_media(
                    services.audit,
                    acceptance,
                    decision="media_download_failed",
                    error_type=_safe_error_type(exc),
                    metadata={"item_index": index},
                )
                prepared.append(_unavailable_media(reference, "download failed"))
                continue

            body = downloaded.data
            if (
                len(body) > policy.max_bytes_per_item
                or total_bytes + len(body) > policy.max_total_bytes
            ):
                await _audit_media(
                    services.audit,
                    acceptance,
                    decision="media_rejected",
                    error_type="media_too_large",
                    metadata={"item_index": index, "size_bytes": len(body)},
                )
                prepared.append(_unavailable_media(reference, "size limit exceeded"))
                continue
            total_bytes += len(body)

            checksum = hashlib.sha256(body).hexdigest()
            artifact_id = hashlib.sha256(
                f"{acceptance.inbound_id}:{index}:{provider_id}".encode()
            ).hexdigest()
            staged_key: str | None = None
            try:
                staged_key = await services.artifact.stage(
                    acceptance.context.tenant_id,
                    artifact_id,
                    body,
                    checksum=checksum,
                )
                await services.artifact.commit(
                    acceptance.context.tenant_id,
                    artifact_id,
                    staged_key,
                )
            except BaseException:
                if staged_key is not None:
                    with contextlib.suppress(Exception):
                        await services.artifact.discard(staged_key)
                raise

            if remaining_chars <= 0:
                prepared.append(
                    PreparedMedia(
                        filename=None,
                        content_type="application/octet-stream",
                        text="[media content unavailable: extraction character limit exceeded]",
                    )
                )
                await _audit_media(
                    services.audit,
                    acceptance,
                    decision="media_ingested",
                    error_type="extraction_limit",
                    metadata={"item_index": index, "size_bytes": len(body)},
                )
                continue
            extractor = MediaExtractor(
                limits=MediaLimits(
                    max_bytes=policy.max_bytes_per_item,
                    max_pdf_pages=policy.max_pdf_pages,
                    max_chars=remaining_chars,
                ),
                pdf_extractor=self._media_extractor.pdf_extractor,
                ocr_extractor=self._media_extractor.ocr_extractor,
            )
            result = await asyncio.to_thread(
                extractor.extract,
                body,
                downloaded.filename or reference.filename,
                downloaded.content_type or reference.content_type,
            )
            extracted_chars = len(result.text) if result.success else 0
            remaining_chars = max(0, remaining_chars - extracted_chars)
            normalized_type = result.metadata.get("content_type")
            content_type = (
                normalized_type if isinstance(normalized_type, str) else "application/octet-stream"
            )
            normalized_name = result.metadata.get("filename")
            filename = normalized_name if isinstance(normalized_name, str) else None
            inline_data = (
                body
                if policy.inline_images
                and result.kind == "image"
                and content_type.startswith("image/")
                else None
            )
            text = result.text if result.success or inline_data is None else None
            prepared.append(
                PreparedMedia(
                    filename=filename,
                    content_type=content_type,
                    text=text,
                    inline_data=inline_data,
                )
            )
            await _audit_media(
                services.audit,
                acceptance,
                decision="media_ingested",
                error_type=result.error_type,
                metadata={
                    "item_index": index,
                    "size_bytes": len(body),
                    "kind": result.kind,
                    "status": result.status,
                    "truncated": result.truncated,
                },
            )

        if len(references) > len(selected):
            prepared.append(
                PreparedMedia(
                    filename=None,
                    content_type="application/octet-stream",
                    text="[additional media rejected: item limit exceeded]",
                )
            )
        return tuple(prepared)


def _target_id(acceptance: Acceptance) -> str:
    envelope = acceptance.envelope
    if envelope.conversation_kind.value == "group":
        if not envelope.external_conversation_id:
            raise ValueError("group message has no target conversation")
        return envelope.external_conversation_id
    return envelope.external_user_id


async def _maybe_await(value: object) -> object:
    if inspect.isawaitable(value):
        return await value
    return value


def _bounded_text(value: str | None, limit: int = 2_000) -> str:
    text = value or ""
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "…"


def _trace_headers() -> dict[str, str]:
    headers: dict[str, str] = {}
    inject_trace_headers(headers)
    return headers


def _record_usage(event: Any, tenant_id: str) -> None:
    usage = getattr(event, "usage_metadata", None)
    if usage is None:
        return
    input_tokens = _usage_int(usage, "prompt_token_count") + _usage_int(
        usage, "tool_use_prompt_token_count"
    )
    output_tokens = _usage_int(usage, "candidates_token_count") + _usage_int(
        usage, "thoughts_token_count"
    )
    total_tokens = _usage_int(usage, "total_token_count") or input_tokens + output_tokens
    if input_tokens:
        TOKENS.labels(direction="input").inc(input_tokens)
    if output_tokens:
        TOKENS.labels(direction="output").inc(output_tokens)
    if total_tokens:
        TENANT_COST.labels(tenant=stable_tenant_label(tenant_id)).inc(total_tokens)


def _usage_int(usage: Any, name: str) -> int:
    value = getattr(usage, name, 0)
    return value if isinstance(value, int) and value > 0 else 0


def _resource_type(payload_kind: PayloadKind, reference: MediaReference) -> str:
    content_type = reference.content_type or ""
    provider_id = reference.provider_media_id or ""
    if payload_kind == PayloadKind.VIDEO or content_type.startswith("video/"):
        return "video"
    if (
        payload_kind == PayloadKind.IMAGE
        or content_type.startswith("image/")
        or provider_id.startswith("img_")
    ):
        return "image"
    return "file"


def _stable_media_key(provider_url: str) -> str:
    """Use a non-sensitive, stable key when a provider has no media id."""

    return "url_" + hashlib.sha256(provider_url.encode("utf-8")).hexdigest()


def _supports_media_reference(downloader: MediaDownloader) -> bool:
    try:
        signature = inspect.signature(downloader.download_media)
    except (TypeError, ValueError):
        return True
    parameters = signature.parameters.values()
    return "media_reference" in signature.parameters or any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters
    )


def _unavailable_media(reference: MediaReference, reason: str) -> PreparedMedia:
    content_type = reference.content_type or "application/octet-stream"
    return PreparedMedia(
        filename=None,
        content_type=content_type,
        text=f"[media content unavailable: {reason}]",
    )


def _safe_error_type(error: BaseException) -> str:
    provider_code = getattr(error, "provider_code", None)
    if isinstance(provider_code, str) and re.fullmatch(r"[a-z0-9_]{1,64}", provider_code):
        return provider_code
    return type(error).__name__[:64]


async def _audit_media(
    audit: Any,
    acceptance: Acceptance,
    *,
    decision: str,
    error_type: str | None,
    metadata: Mapping[str, object],
) -> None:
    await audit.append(
        acceptance.context.tenant_id,
        decision=decision,
        trace_id=acceptance.context.trace_id,
        channel=acceptance.envelope.channel.value,
        user_id=acceptance.context.principal_id,
        session_id=acceptance.context.session_id,
        error_type=error_type,
        config_version=acceptance.context.config_version,
        idempotency_key=acceptance.envelope.external_message_id,
        metadata=metadata,
    )


__all__ = [
    "AgentWorker",
    "DownloadedMedia",
    "MediaDownloader",
    "ProcessStatus",
    "WorkerResult",
]
