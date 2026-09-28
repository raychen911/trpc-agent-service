"""PostgreSQL-backed dispatch queue for stateless Agent Worker nodes."""

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import hashlib
import json
from uuid import UUID, uuid4

from sqlalchemy import and_, exists, or_, select
from sqlalchemy.orm import aliased
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.agent.contracts import (
    AgentExecutionRequest,
    AgentReply,
    AgentTaskSnapshot,
    AgentTaskClaim,
    AgentTaskStatus,
)
from trpc_service.agent.ports import AgentTaskQueue
from trpc_service.channels.contracts import (
    ChannelBindingConfig,
    IncomingMessage,
    MessageKind,
)
from trpc_service.storage.errors import IdempotencyConflict
from trpc_service.storage.errors import StaleExecutionLease
from trpc_service.storage.orm import as_utc, utc_now
from trpc_service.storage.runtime_orm import (
    AgentTaskRow,
    InboxMessageRow,
    OutboxMessageRow,
    RunnerRequestRow,
)
from trpc_service.tenant.context import TenantContext


def _json_mapping(value: Mapping[str, object]) -> dict[str, object]:
    """Copy a provider mapping into values accepted by every JSON backend."""

    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    decoded = json.loads(encoded)
    if not isinstance(decoded, dict):
        raise TypeError("Agent task mapping must encode to a JSON object")
    return {str(key): item for key, item in decoded.items()}


def _serialize_request(request: AgentExecutionRequest) -> dict[str, object]:
    """Persist a secret-free immutable request snapshot for another process."""

    return {
        "tenant": request.tenant.model_dump(mode="json"),
        "session_id": request.session_id,
        "attempt": request.attempt,
        "trace_context": dict(request.trace_context),
        "incoming": {
            "external_message_id": request.incoming.external_message_id,
            "principal_id": request.incoming.principal_id,
            "conversation_id": request.incoming.conversation_id,
            "kind": request.incoming.kind.value,
            "occurred_at": request.incoming.occurred_at.astimezone(timezone.utc).isoformat(),
            "text": request.incoming.text,
            "artifact_refs": list(request.incoming.artifact_refs),
            "attributes": _json_mapping(request.incoming.attributes),
        },
        "channel": {
            "binding_id": str(request.channel.binding_id),
            "tenant_id": str(request.channel.tenant_id),
            "agent_app_id": str(request.channel.agent_app_id),
            "channel_type": request.channel.channel_type,
            "account_config": _json_mapping(request.channel.account_config),
            # Only SecretRefs are queued; resolved secret values never cross this boundary.
            "secret_ref_map": dict(request.channel.secret_ref_map),
            "capabilities": _json_mapping(request.channel.capabilities),
        },
    }


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise RuntimeError(f"persisted Agent task {field} must be an object")
    return value


def _strings(value: object, field: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise RuntimeError(f"persisted Agent task {field} must be an array")
    return value


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise RuntimeError(f"persisted Agent task {field} must be an integer")
    try:
        return int(value)
    except ValueError as error:
        raise RuntimeError(f"persisted Agent task {field} must be an integer") from error


def _deserialize_request(payload: Mapping[str, object]) -> AgentExecutionRequest:
    """Validate a durable snapshot again before it reaches the Agent pipeline."""

    tenant = _mapping(payload.get("tenant"), "tenant")
    incoming = _mapping(payload.get("incoming"), "incoming")
    channel = _mapping(payload.get("channel"), "channel")
    return AgentExecutionRequest(
        tenant=TenantContext.model_validate(tenant),
        session_id=str(payload["session_id"]),
        attempt=_integer(payload.get("attempt", 1), "attempt"),
        trace_context={
            str(key): str(value)
            for key, value in _mapping(payload.get("trace_context", {}), "trace_context").items()
        },
        incoming=IncomingMessage(
            external_message_id=str(incoming["external_message_id"]),
            principal_id=str(incoming["principal_id"]),
            conversation_id=str(incoming["conversation_id"]),
            kind=MessageKind(str(incoming["kind"])),
            occurred_at=datetime.fromisoformat(str(incoming["occurred_at"])),
            text=None if incoming.get("text") is None else str(incoming["text"]),
            artifact_refs=tuple(
                str(value)
                for value in _strings(incoming.get("artifact_refs", []), "artifact_refs")),
            attributes=_mapping(incoming.get("attributes", {}), "attributes"),
        ),
        channel=ChannelBindingConfig(
            binding_id=UUID(str(channel["binding_id"])),
            tenant_id=UUID(str(channel["tenant_id"])),
            agent_app_id=UUID(str(channel["agent_app_id"])),
            channel_type=str(channel["channel_type"]),
            account_config=_mapping(channel.get("account_config", {}), "account_config"),
            secret_ref_map={
                str(key): str(value)
                for key, value in _mapping(channel.get("secret_ref_map", {}),
                                           "secret_ref_map").items()
            },
            capabilities=_mapping(channel.get("capabilities", {}), "capabilities"),
        ),
    )


class PostgreSQLAgentTaskQueue(AgentTaskQueue):
    """Use the primary SQL database as the durable multi-node task authority."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    @staticmethod
    def _identity_query(request: AgentExecutionRequest):  # type: ignore[no-untyped-def]
        return select(AgentTaskRow).where(
            AgentTaskRow.tenant_id == request.tenant.tenant_id,
            AgentTaskRow.binding_id == request.channel.binding_id,
            AgentTaskRow.external_message_id == request.incoming.external_message_id,
        )

    async def enqueue(self, request: AgentExecutionRequest) -> str:
        """Persist one normalized callback, returning the prior ID for a retry."""

        payload = _serialize_request(request)
        # Provider retries arrive through a new HTTP request and therefore carry
        # new request/trace IDs. Hash only message semantics and ownership so an
        # exact provider retry resolves to the original durable task snapshot.
        semantic_payload = {
            "tenant_id": str(request.tenant.tenant_id),
            "agent_app_id": str(request.tenant.agent_app_id),
            "binding_id": str(request.channel.binding_id),
            "session_id": request.session_id,
            "incoming": payload["incoming"],
        }
        canonical = json.dumps(
            semantic_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        payload_hash = hashlib.sha256(canonical).hexdigest()
        routing_key = hashlib.sha256(
            f"{request.tenant.tenant_id}:{request.session_id}".encode()).hexdigest()
        async with self._sessions.begin() as database:
            row = await database.scalar(self._identity_query(request).with_for_update())
            if row is None:
                candidate = AgentTaskRow(
                    tenant_id=request.tenant.tenant_id,
                    agent_app_id=request.tenant.agent_app_id,
                    binding_id=request.channel.binding_id,
                    external_message_id=request.incoming.external_message_id,
                    request_id=request.tenant.request_id,
                    trace_id=request.tenant.trace_id,
                    session_id=request.session_id,
                    config_version=request.tenant.config_version,
                    routing_key=routing_key,
                    payload_hash=payload_hash,
                    request_payload=payload,
                    status=AgentTaskStatus.QUEUED.value,
                )
                try:
                    async with database.begin_nested():
                        database.add(candidate)
                        await database.flush()
                except IntegrityError:
                    row = await database.scalar(self._identity_query(request).with_for_update())
                    if row is None:
                        raise
                else:
                    row = candidate
            if row.payload_hash != payload_hash:
                raise IdempotencyConflict(
                    "external message ID was reused with a different queued payload")
            return str(row.task_id)

    async def claim(self, worker_id: str, *, lease_until: datetime) -> AgentTaskClaim | None:
        """Claim the oldest due task using a lease safe for competing nodes."""

        if worker_id.strip() == "":
            raise ValueError("Agent Worker ID cannot be empty")
        now = utc_now()
        if as_utc(lease_until) <= now:
            raise ValueError("Agent task lease must expire in the future")
        earlier = aliased(AgentTaskRow)
        # Only the oldest unfinished task for a routing key may run. This keeps
        # one Session FIFO across competing Worker nodes without sticky routing.
        unfinished = (
            AgentTaskStatus.QUEUED.value,
            AgentTaskStatus.RUNNING.value,
            AgentTaskStatus.RETRYABLE_FAILED.value,
        )
        has_earlier_unfinished = exists(
            select(earlier.task_id).where(
                earlier.routing_key == AgentTaskRow.routing_key,
                earlier.status.in_(unfinished),
                or_(
                    earlier.created_at < AgentTaskRow.created_at,
                    and_(
                        earlier.created_at == AgentTaskRow.created_at,
                        earlier.task_id < AgentTaskRow.task_id,
                    ),
                ),
            ))
        async with self._sessions.begin() as database:
            row = await database.scalar(
                select(AgentTaskRow).where(
                    or_(
                        AgentTaskRow.status.in_((
                            AgentTaskStatus.QUEUED.value,
                            AgentTaskStatus.RETRYABLE_FAILED.value,
                        )),
                        (AgentTaskRow.status == AgentTaskStatus.RUNNING.value)
                        & (AgentTaskRow.lease_until.is_not(None))
                        & (AgentTaskRow.lease_until <= now),
                    ),
                    or_(AgentTaskRow.next_attempt_at.is_(None), AgentTaskRow.next_attempt_at
                        <= now),
                    ~has_earlier_unfinished,
                ).order_by(AgentTaskRow.created_at,
                           AgentTaskRow.task_id).limit(1).with_for_update(skip_locked=True))
            if row is None:
                return None
            row.status = AgentTaskStatus.RUNNING.value
            row.attempt_count += 1
            row.lease_token += 1
            row.lease_owner = worker_id
            row.lease_until = lease_until
            row.started_at = now
            row.next_attempt_at = None
            row.last_error_code = None
            row.last_error_summary = None
            return AgentTaskClaim(
                task_id=str(row.task_id),
                request=_deserialize_request(row.request_payload),
                status=AgentTaskStatus.RUNNING,
                attempt_count=row.attempt_count,
                fencing_token=row.lease_token,
            )

    @staticmethod
    async def _owned_running_task(
        database: AsyncSession,
        task_id: str,
        worker_id: str,
        fencing_token: int,
    ) -> AgentTaskRow:
        """Lock one task and reject stale or cross-node mutations."""

        row = await database.scalar(
            select(AgentTaskRow).where(AgentTaskRow.task_id == UUID(task_id)).with_for_update())
        if row is None:
            raise LookupError("Agent task does not exist")
        if (row.status != AgentTaskStatus.RUNNING.value or row.lease_owner != worker_id
                or row.lease_token != fencing_token or row.lease_until is None
                or as_utc(row.lease_until) <= utc_now()):
            raise StaleExecutionLease("Agent task lease is stale or expired")
        return row

    async def renew(
        self,
        task_id: str,
        *,
        worker_id: str,
        fencing_token: int,
        lease_until: datetime,
    ) -> bool:
        """Renew an owned task lease while a long Agent execution is running."""

        if as_utc(lease_until) <= utc_now():
            raise ValueError("renewed Agent task lease must expire in the future")
        async with self._sessions.begin() as database:
            try:
                row = await self._owned_running_task(
                    database,
                    task_id,
                    worker_id,
                    fencing_token,
                )
            except (LookupError, StaleExecutionLease):
                return False
            row.lease_until = lease_until
            return True

    async def complete(self, task_id: str, *, worker_id: str, fencing_token: int) -> None:
        """Persist success only for the Worker that still owns the task."""

        async with self._sessions.begin() as database:
            row = await self._owned_running_task(database, task_id, worker_id, fencing_token)
            row.status = AgentTaskStatus.SUCCEEDED.value
            row.completed_at = utc_now()
            row.lease_owner = None
            row.lease_until = None

    async def fail(
        self,
        task_id: str,
        *,
        worker_id: str,
        fencing_token: int,
        error_code: str,
        error_summary: str,
        next_attempt_at: datetime | None,
    ) -> None:
        """Release an owned task and close the IM progress reply on terminal failure."""

        async with self._sessions.begin() as database:
            row = await self._owned_running_task(database, task_id, worker_id, fencing_token)
            row.status = (AgentTaskStatus.RETRYABLE_FAILED.value if next_attempt_at is not None else
                          AgentTaskStatus.PERMANENT_FAILED.value)
            row.next_attempt_at = next_attempt_at
            row.lease_owner = None
            row.lease_until = None
            row.last_error_code = error_code[:100]
            row.last_error_summary = error_summary[:2000]
            if next_attempt_at is None:
                now = utc_now()
                row.completed_at = now
                await self._finalize_failed_request(
                    database,
                    row,
                    error_code=error_code,
                    error_summary=error_summary,
                    completed_at=now,
                )

    @staticmethod
    async def _finalize_failed_request(
        database: AsyncSession,
        task: AgentTaskRow,
        *,
        error_code: str,
        error_summary: str,
        completed_at: datetime,
    ) -> None:
        """Atomically persist one provider-routable, secret-free failure reply."""

        request = _deserialize_request(task.request_payload)
        inbox = await database.scalar(
            select(InboxMessageRow).where(
                InboxMessageRow.tenant_id == task.tenant_id,
                InboxMessageRow.agent_app_id == task.agent_app_id,
                InboxMessageRow.binding_id == task.binding_id,
                InboxMessageRow.external_message_id == task.external_message_id,
            ).with_for_update())
        if inbox is not None and inbox.status not in {"SUCCEEDED", "REPLIED"}:
            # The inner execution stage makes its own retry decision before the
            # outer Worker applies the global attempt budget. Reconcile that
            # intermediate state when the outer task is definitively terminal.
            inbox.status = "PERMANENT_FAILED"
            inbox.next_attempt_at = None
            inbox.lease_owner = None
            inbox.lease_until = None
            inbox.last_error_code = error_code[:100]
            inbox.last_error_summary = error_summary[:2000]
            inbox.completed_at = completed_at

        runner = await database.scalar(
            select(RunnerRequestRow).where(
                RunnerRequestRow.tenant_id == task.tenant_id,
                RunnerRequestRow.request_id == task.request_id,
            ).with_for_update())
        if runner is not None and runner.status not in {"COMPLETED", "CANCELLED"}:
            runner.status = "PERMANENT_FAILED"
            runner.last_error = error_summary[:2000]
            runner.completed_at = completed_at

        # A commit may have created the normal reply before a later publisher
        # error. Never add a second response for the same logical request.
        prior_reply = await database.scalar(
            select(OutboxMessageRow.outbox_id).where(
                OutboxMessageRow.tenant_id == task.tenant_id,
                OutboxMessageRow.agent_app_id == task.agent_app_id,
                OutboxMessageRow.category == "IM_REPLY",
                OutboxMessageRow.request_id == task.request_id,
            ).limit(1))
        if prior_reply is not None:
            return

        reply_context = request.incoming.attributes.get("reply_context")
        delivery_attributes: dict[str, object] = {
            "in_reply_to": request.incoming.external_message_id,
        }
        if isinstance(reply_context, Mapping):
            # The provider-specific cursor is already normalized and persisted
            # by the Channel adapter; it contains no resolved secret values.
            delivery_attributes["reply_context"] = _json_mapping(reply_context)
        text = ("请求处理失败，请检查 Agent 配置或联系管理员后重试。" if error_summary == "Agent task cannot be executed"
                else "请求处理暂时失败，请稍后重试；如持续发生，请联系管理员。")
        outbox_id = str(uuid4())
        database.add(
            OutboxMessageRow(
                tenant_id=task.tenant_id,
                agent_app_id=task.agent_app_id,
                outbox_id=outbox_id,
                request_id=task.request_id,
                session_id=task.session_id,
                category="IM_REPLY",
                destination=request.channel.channel_type,
                binding_id=task.binding_id,
                sequence_no=0,
                idempotency_key=(f"{task.binding_id}:{task.external_message_id}:terminal-error"),
                payload={
                    "artifact_refs": [],
                    "attributes": delivery_attributes,
                    "conversation_id": request.incoming.conversation_id,
                    "delivery_id": str(uuid4()),
                    "kind": MessageKind.TEXT.value,
                    "text": text,
                },
                status="PENDING",
            ))
        if inbox is not None:
            inbox.reply_outbox_id = outbox_id

    async def get(
        self,
        context: TenantContext,
        binding_id: UUID,
        external_message_id: str,
    ) -> AgentTaskSnapshot | None:
        """Read a task only through its complete Tenant, Agent and Binding scope."""

        async with self._sessions() as database:
            row = await database.scalar(
                select(AgentTaskRow).where(
                    AgentTaskRow.tenant_id == context.tenant_id,
                    AgentTaskRow.agent_app_id == context.agent_app_id,
                    AgentTaskRow.binding_id == binding_id,
                    AgentTaskRow.external_message_id == external_message_id,
                ))
            if row is None:
                return None
            outbox_rows = (await database.scalars(
                select(OutboxMessageRow).where(
                    OutboxMessageRow.tenant_id == context.tenant_id,
                    OutboxMessageRow.agent_app_id == context.agent_app_id,
                    OutboxMessageRow.binding_id == binding_id,
                    OutboxMessageRow.request_id == row.request_id,
                    OutboxMessageRow.category == "IM_REPLY",
                ).order_by(OutboxMessageRow.sequence_no))).all()
            replies = tuple(
                self._reply_from_outbox(item) for item in outbox_rows if item.status == "DELIVERED")
            delivery_status: str | None = None
            delivery_error: str | None = None
            if outbox_rows:
                statuses = {item.status for item in outbox_rows}
                if "UNKNOWN" in statuses:
                    delivery_status = "UNKNOWN"
                elif "DEAD_LETTER" in statuses:
                    delivery_status = "DEAD_LETTER"
                elif statuses == {"DELIVERED"}:
                    delivery_status = "DELIVERED"
                else:
                    delivery_status = "PENDING"
                if delivery_status in {"UNKNOWN", "DEAD_LETTER"}:
                    delivery_error = next(
                        (item.last_error_summary for item in outbox_rows
                         if item.status == delivery_status and item.last_error_summary),
                        "Channel delivery requires operator attention",
                    )
            return AgentTaskSnapshot(
                task_id=str(row.task_id),
                external_message_id=row.external_message_id,
                status=AgentTaskStatus(row.status),
                attempt_count=row.attempt_count,
                replies=replies,
                safe_error=row.last_error_summary,
                delivery_status=delivery_status,
                delivery_error=delivery_error,
            )

    @staticmethod
    def _reply_from_outbox(row: OutboxMessageRow) -> AgentReply:
        """Validate a delivered reply before exposing it through a Gateway."""

        payload = row.payload
        artifact_refs = _strings(payload.get("artifact_refs", []), "artifact_refs")
        attributes = _mapping(payload.get("attributes", {}), "attributes")
        return AgentReply(
            kind=MessageKind(str(payload["kind"])),
            text=None if payload.get("text") is None else str(payload["text"]),
            artifact_refs=tuple(str(value) for value in artifact_refs),
            attributes={
                **attributes,
                "delivery_id": str(payload["delivery_id"]),
            },
        )
