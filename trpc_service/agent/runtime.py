"""Concrete runtime stages shared by every normalized Channel request."""

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
import hashlib
import httpx
import json
import logging
from time import perf_counter
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.agent.approval import ApprovalService
from trpc_service.agent.audit import StorageAuditRecorder
from trpc_service.agent.contracts import (
    AgentExecutionClaim,
    AgentExecutionContext,
    AgentExecutionOutcome,
    AgentExecutionReceipt,
    AgentExecutionRequest,
    AgentRuntimeConfig,
    AgentRunResult,
    AgentInputArtifact,
    PolicyAction,
    PolicyDecision,
)
from trpc_service.agent.pipeline import AgentExecutionPipeline
from trpc_service.agent.configuration import resolve_active_model_policy
from trpc_service.agent.ports import (
    AgentConfigProvider,
    AgentContextBuilder,
    AgentExecutionCoordinator,
    AgentOutputFilter,
    AgentResultCommitter,
    AgentResultPublisher,
    AgentRunner,
    AgentToolInvoker,
)
from trpc_service.agent.models import AgentApp, AgentConfigVersion
from trpc_service.agent.governance import (
    GovernanceContextBuilder,
    GovernanceOutputFilter,
    GovernedToolInvoker,
    TenantGovernancePolicy,
)
from trpc_service.agent.observability import ObservedAgentRunner
from trpc_service.agent.recovery import FailureDisposition, RecoveryPolicy
from trpc_service.agent.usage import UsageReader, UsageRecorder
from trpc_service.agent.ledger import ToolLedger
from trpc_service.channels.contracts import MessageKind
from trpc_service.channels.media import detect_image_media_type
from trpc_service.config import Settings
from trpc_service.storage.router import BackendProfile, ResolvedStorage, StorageRouter
from trpc_service.storage.knowledge import TenantKnowledgeService
from trpc_service.storage.types import (
    ExecutionCommit,
    InboxClaimRequest,
    MemoryHit,
    KnowledgeHit,
    OutboxMessage,
    SessionSnapshot,
)
from trpc_service.storage.session_cache import SessionSnapshotCache
from trpc_service.log import SensitiveDataRedactor
from trpc_service.metrics import PlatformTelemetry
from trpc_service.tenant.models import Tenant
from trpc_service.tenant.context import TenantContext
from trpc_service.tool import (
    BuiltinToolInvoker,
    CompositeToolInvoker,
    EnterpriseToolInvoker,
    KnowledgeToolInvoker,
)
from trpc_service.workspace import WorkspaceProvider

logger = logging.getLogger(__name__)


def _configuration_mapping(
    snapshot: Mapping[str, object],
    field_name: str,
) -> Mapping[str, object]:
    """Validate and normalize one flexible configuration snapshot field."""

    value = snapshot.get(field_name, {})
    if not isinstance(value, Mapping):
        raise ValueError(f"Agent {field_name} must be an object")
    return {str(key): item for key, item in value.items()}


class _RuntimeStorageResolver:
    """Resolve an immutable Backend Profile for each Agent execution."""

    def __init__(self, storage: StorageRouter | ResolvedStorage) -> None:
        self._storage = storage

    def resolve(self, config: AgentRuntimeConfig) -> ResolvedStorage:
        """Route tenant-selected capability names or retain a fixed test store."""

        if isinstance(self._storage, ResolvedStorage):
            return self._storage
        return self._storage.resolve(BackendProfile.from_mapping(config.backends))


class SettingsAgentConfigProvider(AgentConfigProvider):
    """Expose one immutable settings snapshot through the runtime config port."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def load(self, request: AgentExecutionRequest) -> AgentRuntimeConfig:
        """Return the model and instruction fixed when the process started."""

        return AgentRuntimeConfig(
            config_version=request.tenant.config_version,
            runner_name="trpc_agent",
            application={
                "name": "default-agent",
                "instruction": self._settings.agent_instruction,
            },
            model=self._settings.llm.model_dump(mode="json"),
            backends=self._settings.storage_profile.model_dump(mode="json"),
            policy={
                "limits": {},
                "governance": {
                    "redact_pii": True
                },
                "audit": {
                    "required": True
                },
            },
        )

    async def load_backends(self, context: TenantContext) -> Mapping[str, object]:
        """Return the settings profile for Channel media received in this version."""

        del context
        return self._settings.storage_profile.model_dump(mode="json")


class DatabaseAgentConfigProvider(AgentConfigProvider):
    """Load the Agent and tenant Model Profile selected in the control plane."""

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory

    async def load_backends(self, context: TenantContext) -> Mapping[str, object]:
        """Load the exact released Backend Profile selected before IM download."""

        async with self._session_factory() as session:
            agent = await session.scalar(
                select(AgentApp).where(
                    AgentApp.tenant_id == context.tenant_id,
                    AgentApp.agent_app_id == context.agent_app_id,
                    AgentApp.status == "active",
                ))
            if agent is None:
                raise PermissionError("Agent App is not active for this tenant")
            version = await session.scalar(
                select(AgentConfigVersion).where(
                    AgentConfigVersion.tenant_id == context.tenant_id,
                    AgentConfigVersion.agent_app_id == context.agent_app_id,
                    AgentConfigVersion.version == context.config_version,
                    AgentConfigVersion.status == "released",
                ))
            if version is None:
                if context.config_version != 1 or agent.stable_config_version != 1:
                    raise PermissionError("Agent configuration version is not released")
                configured: object = agent.backend_config
            else:
                configured = version.snapshot.get("backend_config", {})
            if not isinstance(configured, Mapping):
                raise ValueError("Agent backend_config must be an object")
            return ({
                str(key): value
                for key, value in configured.items()
            } or self._settings.storage_profile.model_dump(mode="json"))

    async def load(self, request: AgentExecutionRequest) -> AgentRuntimeConfig:
        """Return a fail-closed model selection for the request's Agent App."""

        async with self._session_factory() as session:
            agent = await session.scalar(
                select(AgentApp).where(
                    AgentApp.tenant_id == request.tenant.tenant_id,
                    AgentApp.agent_app_id == request.tenant.agent_app_id,
                    AgentApp.status == "active",
                ))
            if agent is None:
                raise PermissionError("Agent App is not active for this tenant")
            tenant = await session.get(Tenant, request.tenant.tenant_id)
            if tenant is None or tenant.status != "active":
                raise PermissionError("Tenant is not active")
            version = await session.scalar(
                select(AgentConfigVersion).where(
                    AgentConfigVersion.tenant_id == request.tenant.tenant_id,
                    AgentConfigVersion.agent_app_id == request.tenant.agent_app_id,
                    AgentConfigVersion.version == request.tenant.config_version,
                    AgentConfigVersion.status == "released",
                ))
            if version is None:
                # Compatibility is limited to pre-migration test fixtures. A
                # version other than the legacy stable pointer always fails.
                if (request.tenant.config_version != 1 or agent.stable_config_version != 1):
                    raise PermissionError("Agent configuration version is not released")
                snapshot: Mapping[str, object] = {
                    "model_profile_id":
                    (None if agent.model_profile_id is None else str(agent.model_profile_id)),
                    "application_config":
                    agent.application_config,
                    "model_settings":
                    agent.model_settings,
                    "tool_permissions":
                    agent.tool_permissions,
                    "knowledge_config":
                    agent.knowledge_config,
                    "backend_config":
                    agent.backend_config,
                }
            else:
                snapshot = version.snapshot

            profile_value = snapshot.get("model_profile_id")
            profile_id = None if profile_value is None else UUID(str(profile_value))
            resolved_policy = await resolve_active_model_policy(
                session,
                request.tenant.tenant_id,
                profile_id,
            )
            profile = resolved_policy.profile
            catalog = resolved_policy.catalog
            secret_ref = resolved_policy.secret_ref
            if catalog.provider not in {"bailian", "bailian_openai"}:
                raise RuntimeError(f"model provider is not installed: {catalog.provider}")

            # Agent-level model overrides are retained only in legacy snapshots.
            # Runtime policy comes exclusively from the platform-owned Profile.
            _configuration_mapping(snapshot, "model_settings")
            application_config = _configuration_mapping(snapshot, "application_config")
            tool_permissions = _configuration_mapping(snapshot, "tool_permissions")
            knowledge_config = _configuration_mapping(snapshot, "knowledge_config")
            backend_config = _configuration_mapping(snapshot, "backend_config")

            self._settings.validate_execution_backends(backend_config)

            parameters = {
                **catalog.default_limits,
                **profile.parameter_config,
            }
            configured_governance = application_config.get("governance", {})
            if not isinstance(configured_governance, Mapping):
                raise ValueError("Agent governance configuration must be an object")
            if not isinstance(tenant.audit_policy, Mapping):
                raise ValueError("Tenant audit policy must be an object")
            return AgentRuntimeConfig(
                config_version=request.tenant.config_version,
                runner_name="trpc_agent",
                application={
                    "name": agent.name,
                    "instruction": self._settings.agent_instruction,
                    **application_config,
                },
                model={
                    "provider":
                    "bailian_openai",
                    "model_name":
                    catalog.model_name,
                    "base_url":
                    self._settings.llm.base_url,
                    "api_key_ref":
                    secret_ref,
                    "temperature":
                    parameters.get("temperature", self._settings.llm.temperature),
                    "max_output_tokens":
                    parameters.get(
                        "max_output_tokens",
                        self._settings.llm.max_output_tokens,
                    ),
                    "context_window_tokens":
                    parameters.get("context_window_tokens"),
                    "timeout_seconds":
                    parameters.get("timeout_seconds", self._settings.model_timeout_seconds),
                    "enable_thinking":
                    parameters.get("enable_thinking"),
                    "input_cost_per_million":
                    parameters.get("input_cost_per_million", 0),
                    "output_cost_per_million":
                    parameters.get("output_cost_per_million", 0),
                },
                tools=tool_permissions,
                knowledge=knowledge_config,
                policy={
                    "limits": profile.limits,
                    "governance": {
                        "redact_pii": True,
                        **configured_governance,
                    },
                    "audit": {
                        "required": True,
                        **tenant.audit_policy,
                    },
                },
                backends=backend_config or self._settings.storage_profile.model_dump(mode="json"),
            )


class StorageExecutionCoordinator(AgentExecutionCoordinator):
    """Claim normalized IM messages before any model or Tool work starts."""

    def __init__(self, storage: StorageRouter | ResolvedStorage, lease_seconds: int = 120) -> None:
        self._storage = _RuntimeStorageResolver(storage)
        self._lease_seconds = lease_seconds
        self._worker_id = f"agent-{uuid4()}"
        self._recovery = RecoveryPolicy()

    @property
    def lease_renewal_interval_seconds(self) -> float:
        """Renew at one third of the lease to tolerate transient SQL latency."""

        return self._lease_seconds / 3

    async def renew(self, claim: AgentExecutionClaim) -> bool:
        """Keep Inbox and Session fencing authority alive during long model calls."""

        if claim.request is None or claim.fencing_token is None:
            return False
        if claim.runtime_config is None:
            return False
        storage = self._storage.resolve(claim.runtime_config)
        return await storage.session.renew_execution(
            claim.request.tenant,
            claim.claim_id,
            worker_id=self._worker_id,
            fencing_token=claim.fencing_token,
            lease_until=datetime.now(timezone.utc) + timedelta(seconds=self._lease_seconds),
        )

    @staticmethod
    def _payload_hash(request: AgentExecutionRequest) -> str:
        """Hash normalized message semantics to detect provider ID reuse."""

        incoming = request.incoming
        canonical = json.dumps(
            {
                "artifact_refs": list(incoming.artifact_refs),
                "attributes": dict(incoming.attributes),
                "conversation_id": incoming.conversation_id,
                "external_message_id": incoming.external_message_id,
                "kind": incoming.kind.value,
                "occurred_at": incoming.occurred_at.astimezone(timezone.utc).isoformat(),
                "principal_id": incoming.principal_id,
                "text": incoming.text,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
        return hashlib.sha256(canonical).hexdigest()

    async def begin(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
    ) -> AgentExecutionClaim:
        """Create a durable Inbox lease or reuse a prior committed result."""

        storage = self._storage.resolve(config)
        claim = await storage.session.claim_execution(
            request.tenant,
            InboxClaimRequest(
                binding_id=request.channel.binding_id,
                external_message_id=request.incoming.external_message_id,
                payload_hash=self._payload_hash(request),
                session_id=request.session_id,
                received_at=request.incoming.occurred_at,
            ),
            self._worker_id,
            datetime.now(timezone.utc) + timedelta(seconds=self._lease_seconds),
        )
        if claim.completed is not None:
            return AgentExecutionClaim(
                claim_id=claim.inbox_id,
                request_id=claim.request_id,
                fencing_token=claim.fencing_token,
                session_version=claim.session_version,
                request=request,
                runtime_config=config,
                completed=AgentExecutionReceipt(
                    session=claim.completed,
                    committed_outbox_ids=claim.committed_outbox_ids,
                    replayed=True,
                ),
            )
        return AgentExecutionClaim(
            claim_id=claim.inbox_id,
            request_id=claim.request_id,
            fencing_token=claim.fencing_token,
            session_version=claim.session_version,
            request=request,
            runtime_config=config,
        )

    async def reject(
        self,
        claim: AgentExecutionClaim,
        decision: PolicyDecision,
    ) -> AgentExecutionReceipt:
        """Persist a permanent policy rejection without executing the Agent."""

        if claim.request is None:
            raise RuntimeError("durable execution claim has no request context")
        if claim.runtime_config is None:
            raise RuntimeError("durable execution claim has no runtime configuration")
        request = claim.request
        storage = self._storage.resolve(claim.runtime_config)
        await storage.session.fail_execution(
            request.tenant,
            claim.claim_id,
            fencing_token=self._required_fencing_token(claim),
            error_code="POLICY_REJECTED",
            error_summary=decision.reason or decision.action.value,
            next_attempt_at=None,
        )
        current = await storage.session.load(request.tenant, request.session_id)
        outcome = (AgentExecutionOutcome.DENIED if decision.action is PolicyAction.DENY else
                   AgentExecutionOutcome.REVIEW_REQUIRED)
        return AgentExecutionReceipt(
            session=(current if current is not None else self._empty_session(request.session_id)),
            outcome=outcome,
            policy=decision,
        )

    @staticmethod
    def _empty_session(session_id: str) -> SessionSnapshot:
        return SessionSnapshot(session_id=session_id, version=0)

    @staticmethod
    def _required_fencing_token(claim: AgentExecutionClaim) -> int:
        """Return the durable token required to mutate a claimed Inbox."""

        if claim.fencing_token is None:
            raise RuntimeError("durable execution claim has no fencing token")
        return claim.fencing_token

    async def fail(self, claim: AgentExecutionClaim, error: Exception) -> None:
        """Release the Inbox using the same safe classification as its task."""

        request = claim.request
        if request is None or claim.runtime_config is None:
            return
        storage = self._storage.resolve(claim.runtime_config)
        decision = self._recovery.classify_agent(error)
        next_attempt_at = None
        if decision.disposition is FailureDisposition.RETRY:
            # The outer durable Agent task queue owns the global backoff and
            # attempt budget. Keeping a second independent timer here can make
            # the next valid task attempt hit ExecutionAlreadyRunning and waste
            # its retry slot before any model work starts.
            next_attempt_at = datetime.now(timezone.utc)
        await storage.session.fail_execution(
            request.tenant,
            claim.claim_id,
            fencing_token=self._required_fencing_token(claim),
            error_code=decision.error_code,
            error_summary=decision.safe_summary,
            next_attempt_at=next_attempt_at,
        )
        logger.warning(
            "Agent execution claim %s failed with %s",
            claim.claim_id,
            type(error).__name__,
        )


class StorageContextBuilder(AgentContextBuilder):
    """Load the current platform Session before invoking the stateless pipeline."""

    def __init__(
        self,
        storage: StorageRouter | ResolvedStorage,
        telemetry: PlatformTelemetry | None = None,
        knowledge_service: TenantKnowledgeService | None = None,
        session_cache: SessionSnapshotCache | None = None,
    ) -> None:
        self._storage = _RuntimeStorageResolver(storage)
        self._telemetry = telemetry
        self._knowledge_service = knowledge_service
        self._session_cache = session_cache

    async def build(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
        policy: PolicyDecision,
        claim: AgentExecutionClaim,
    ) -> AgentExecutionContext:
        """Build context from the configured Session backend."""

        storage = self._storage.resolve(config)
        started = perf_counter()
        span_context = (self._telemetry.start_span(
            "storage.session.load",
            attributes={
                "tenant.id": str(request.tenant.tenant_id),
                "request.id": request.tenant.request_id,
                "storage.operation": "session.load",
            },
        ) if self._telemetry is not None else None)
        try:
            session = None
            if self._session_cache is not None and claim.session_version is not None:
                session = await self._session_cache.get(
                    request.tenant,
                    request.session_id,
                    expected_version=claim.session_version,
                )
            if session is None:
                if span_context is None:
                    session = await storage.session.load(request.tenant, request.session_id)
                else:
                    with span_context:
                        session = await storage.session.load(request.tenant, request.session_id)
                if session is not None and self._session_cache is not None:
                    await self._session_cache.put(request.tenant, session)
        except Exception:
            if self._telemetry is not None:
                self._telemetry.record_storage(
                    operation="session.load",
                    result="error",
                    duration_seconds=perf_counter() - started,
                )
            raise
        if self._telemetry is not None:
            self._telemetry.record_storage(
                operation="session.load",
                result="success",
                duration_seconds=perf_counter() - started,
            )
        memories: tuple[MemoryHit, ...] = ()
        if storage.memory is not None and request.incoming.text:
            started = perf_counter()
            span_context = (self._telemetry.start_span(
                "storage.memory.search",
                attributes={
                    "tenant.id": str(request.tenant.tenant_id),
                    "request.id": request.tenant.request_id,
                    "storage.operation": "memory.search",
                },
            ) if self._telemetry is not None else None)
            try:
                if span_context is None:
                    found = await storage.memory.search(
                        request.tenant,
                        request.incoming.principal_id,
                        request.incoming.text,
                        limit=10,
                    )
                else:
                    with span_context:
                        found = await storage.memory.search(
                            request.tenant,
                            request.incoming.principal_id,
                            request.incoming.text,
                            limit=10,
                        )
            except Exception:
                if self._telemetry is not None:
                    self._telemetry.record_storage(
                        operation="memory.search",
                        result="error",
                        duration_seconds=perf_counter() - started,
                    )
                raise
            if self._telemetry is not None:
                self._telemetry.record_storage(
                    operation="memory.search",
                    result="success",
                    duration_seconds=perf_counter() - started,
                )
            memories = tuple(found)
        knowledge: tuple[KnowledgeHit, ...] = ()
        auto_retrieve = config.knowledge.get("auto_retrieve", True)
        if not isinstance(auto_retrieve, bool):
            raise ValueError("knowledge auto_retrieve must be boolean")
        raw_limit = config.knowledge.get("retrieval_limit", 5)
        if isinstance(raw_limit, bool) or not isinstance(raw_limit, int):
            raise ValueError("knowledge retrieval_limit must be an integer")
        if (self._knowledge_service is not None and auto_retrieve and request.incoming.text
                and config.knowledge.get("knowledge_base_names")):
            started = perf_counter()
            try:
                knowledge = await self._knowledge_service.search(
                    request.tenant,
                    config.knowledge,
                    request.incoming.text,
                    limit=raw_limit,
                    backends=config.backends,
                )
            except Exception:
                if self._telemetry is not None:
                    self._telemetry.record_storage(
                        operation="knowledge.search",
                        result="error",
                        duration_seconds=perf_counter() - started,
                    )
                raise
            if self._telemetry is not None:
                self._telemetry.record_storage(
                    operation="knowledge.search",
                    result="success",
                    duration_seconds=perf_counter() - started,
                )
        input_artifacts = await self._load_input_artifacts(request, storage)
        return AgentExecutionContext(
            request=request,
            config=config,
            policy=policy,
            claim=claim,
            session=session,
            memories=memories,
            knowledge=knowledge,
            input_artifacts=input_artifacts,
        )

    @staticmethod
    async def _load_input_artifacts(
        request: AgentExecutionRequest,
        storage: ResolvedStorage,
    ) -> tuple[AgentInputArtifact, ...]:
        """Load bounded image bytes only after tenant storage routing is fixed."""

        incoming = request.incoming
        if incoming.kind is not MessageKind.IMAGE:
            return ()
        if not incoming.artifact_refs:
            raise ValueError("image message does not contain a persisted Artifact")
        if storage.artifact is None:
            raise LookupError("image input requires the configured Artifact backend")
        raw_limit = request.channel.capabilities.get("max_model_image_bytes", 10 * 1024 * 1024)
        if isinstance(raw_limit, bool) or not isinstance(raw_limit, int) or raw_limit < 1:
            raise ValueError("model image size limit must be a positive integer")
        raw_count = request.channel.capabilities.get("max_model_images", 4)
        if isinstance(raw_count, bool) or not isinstance(raw_count, int) or raw_count < 1:
            raise ValueError("model image count limit must be a positive integer")
        if len(incoming.artifact_refs) > raw_count:
            raise ValueError("image message exceeds the configured model image count")

        provider_media = incoming.attributes.get("provider_media", ())
        descriptors = (provider_media if isinstance(provider_media, list) else ())
        resolved: list[AgentInputArtifact] = []
        for index, artifact_id in enumerate(incoming.artifact_refs):
            blocks: list[bytes] = []
            size = 0
            async for block in storage.artifact.open(request.tenant, artifact_id):
                size += len(block)
                if size > raw_limit:
                    raise ValueError("inbound image exceeds the configured model size limit")
                blocks.append(block)
            content = b"".join(blocks)
            media_type = detect_image_media_type(content)
            descriptor = descriptors[index] if index < len(descriptors) else {}
            filename_value = (descriptor.get("filename") or descriptor.get("file_name")
                              if isinstance(descriptor, Mapping) else None)
            filename = (filename_value if isinstance(filename_value, str)
                        and filename_value.strip() else f"image-{index + 1}")
            resolved.append(
                AgentInputArtifact(
                    artifact_id=artifact_id,
                    media_type=media_type,
                    filename=filename,
                    content=content,
                ))
        return tuple(resolved)


class PassThroughOutputFilter(AgentOutputFilter):
    """Initial output-filter implementation with a stable replacement boundary."""

    async def apply(
        self,
        context: AgentExecutionContext,
        result: AgentRunResult,
    ) -> AgentRunResult:
        """Return text output unchanged at the provider-neutral boundary."""

        del context
        return result


class StorageResultCommitter(AgentResultCommitter):
    """Commit Session events and complete reply payloads through storage ports."""

    def __init__(
        self,
        storage: StorageRouter | ResolvedStorage,
        telemetry: PlatformTelemetry | None = None,
        session_cache: SessionSnapshotCache | None = None,
    ) -> None:
        self._storage = _RuntimeStorageResolver(storage)
        self._telemetry = telemetry
        self._session_cache = session_cache

    async def commit(
        self,
        context: AgentExecutionContext,
        result: AgentRunResult,
    ) -> AgentExecutionReceipt:
        """Persist the turn and delivery work before exposing a reply."""

        request = context.request
        storage = self._storage.resolve(context.config)
        canonical_request_id = context.claim.request_id or request.tenant.request_id
        outbox: list[OutboxMessage] = []
        for index, reply in enumerate(result.replies):
            if reply.kind is not MessageKind.TEXT or reply.text is None:
                raise ValueError("the initial Channel delivery supports text replies only")
            delivery_attributes = dict(reply.attributes)
            reply_context = request.incoming.attributes.get("reply_context")
            if reply_context is not None:
                if not isinstance(reply_context, Mapping):
                    raise ValueError("incoming reply_context must be an object")
                # Provider correlation is durable rather than process-local so
                # another delivery node can finish the response after a retry.
                delivery_attributes["reply_context"] = dict(reply_context)
            outbox_id = str(uuid4())
            delivery_id = str(uuid4())
            outbox.append(
                OutboxMessage(
                    outbox_id=outbox_id,
                    category="IM_REPLY",
                    destination=request.channel.channel_type,
                    binding_id=request.channel.binding_id,
                    request_id=canonical_request_id,
                    session_id=request.session_id,
                    sequence_no=index,
                    idempotency_key=(f"{request.channel.binding_id}:"
                                     f"{request.incoming.external_message_id}:reply:{index}"),
                    payload={
                        "artifact_refs": list(reply.artifact_refs),
                        "attributes": {
                            **delivery_attributes,
                            "in_reply_to": request.incoming.external_message_id,
                        },
                        "conversation_id": request.incoming.conversation_id,
                        "delivery_id": delivery_id,
                        "kind": reply.kind.value,
                        "text": reply.text,
                    },
                ))

        expected_version = 0 if context.session is None else context.session.version
        prior_state = {} if context.session is None else dict(context.session.state)
        commit = ExecutionCommit(
            session_id=request.session_id,
            expected_version=expected_version,
            fencing_token=context.claim.fencing_token,
            events=result.events,
            state={
                **prior_state,
                **result.state
            },
            inbox_id=context.claim.claim_id,
            runner_request_id=canonical_request_id,
            outbox=tuple(outbox) + result.derived_outbox,
        )
        started = perf_counter()
        span_context = (self._telemetry.start_span(
            "storage.session.commit",
            attributes={
                "tenant.id": str(request.tenant.tenant_id),
                "request.id": request.tenant.request_id,
                "storage.operation": "session.commit",
            },
        ) if self._telemetry is not None else None)
        try:
            if span_context is None:
                snapshot = await storage.session.commit_execution(request.tenant, commit)
            else:
                with span_context:
                    snapshot = await storage.session.commit_execution(request.tenant, commit)
        except Exception:
            if self._telemetry is not None:
                self._telemetry.record_storage(
                    operation="session.commit",
                    result="error",
                    duration_seconds=perf_counter() - started,
                )
            raise
        if self._telemetry is not None:
            self._telemetry.record_storage(
                operation="session.commit",
                result="success",
                duration_seconds=perf_counter() - started,
            )
        if self._session_cache is not None:
            await self._session_cache.put(request.tenant, snapshot)
        return AgentExecutionReceipt(
            session=snapshot,
            # Only the IM publisher consumes these IDs. Other Outbox categories
            # are claimed by their own capability-specific consumers later.
            committed_outbox_ids=tuple(message.outbox_id for message in outbox),
        )


class DeferredOutboxPublisher(AgentResultPublisher):
    """Leave durable replies to the independently scalable Delivery Worker."""

    async def publish(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
        receipt: AgentExecutionReceipt,
    ) -> None:
        """A successful commit is enough; delivery is owned by the SQL queue."""

        del request, config, receipt


def build_local_agent_pipeline(
    *,
    settings: Settings,
    storage: StorageRouter,
    runner: AgentRunner,
    telemetry: PlatformTelemetry,
    config_provider: AgentConfigProvider,
    approvals: ApprovalService,
    usage_recorder: UsageRecorder,
    knowledge_service: TenantKnowledgeService,
    tool_ledger: ToolLedger,
    workspace_provider: WorkspaceProvider,
    mcp_service: AgentToolInvoker,
    http_client: httpx.AsyncClient,
    session_cache: SessionSnapshotCache | None = None,
) -> AgentExecutionPipeline:
    """Compose the concrete Agent chain from explicit production components."""

    redactor = SensitiveDataRedactor()
    audit = StorageAuditRecorder(storage, redactor)
    observed_runner = ObservedAgentRunner(
        runner,
        telemetry,
        audit,
        timeout_seconds=settings.model_timeout_seconds,
        usage_recorder=usage_recorder,
    )
    builtin_tools = BuiltinToolInvoker()
    tool_routes: dict[str, AgentToolInvoker] = {"calculate": builtin_tools}
    enterprise_tools = EnterpriseToolInvoker(workspace_provider, http_client=http_client)
    tool_routes.update({name: enterprise_tools for name in enterprise_tools.TOOL_NAMES})
    knowledge_tools = KnowledgeToolInvoker(knowledge_service)
    tool_routes.update({name: knowledge_tools for name in knowledge_tools.TOOL_NAMES})
    return AgentExecutionPipeline(
        config_provider=config_provider,
        coordinator=StorageExecutionCoordinator(
            storage,
            lease_seconds=settings.worker_lease_seconds,
        ),
        policy_engine=TenantGovernancePolicy(
            telemetry=telemetry,
            audit=audit,
            usage_reader=(usage_recorder if isinstance(usage_recorder, UsageReader) else None),
        ),
        context_builder=GovernanceContextBuilder(
            StorageContextBuilder(
                storage,
                telemetry,
                knowledge_service,
                session_cache,
            ),
            redactor,
        ),
        runner=observed_runner,
        tool_invoker=GovernedToolInvoker(
            CompositeToolInvoker(tool_routes, fallback=mcp_service),
            telemetry=telemetry,
            audit=audit,
            approvals=approvals,
            ledger=tool_ledger,
        ),
        output_filter=GovernanceOutputFilter(redactor),
        committer=StorageResultCommitter(storage, telemetry, session_cache),
        publisher=DeferredOutboxPublisher(),
        usage_recorder=usage_recorder,
        workspace_provider=workspace_provider,
    )
