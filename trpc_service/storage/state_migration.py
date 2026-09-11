"""Offline, bounded tenant state migration between Redis and SQL (R1B).

The tenant's versioned ``backend_profile.state_backend`` is the single switch
that decides where a tenant's Session/Memory state lives.  Moving a tenant
between the two backends is intentionally OFFLINE (operators stop
Gateway/Workers first) and BOUNDED (only public SDK Session/Memory methods
are used — no tables, pools, or private SDK internals are touched):

1. Preconditions are checked BEFORE any data movement (explicit ``offline``
   flag, known target, tenant exists at ``expected_version``, source!=target,
   zero ``processing`` receipts, a repository that can actually CAS).
2. The current config-version app namespace is copied into the PREDICTED
   NEXT-version namespace on the target backend, preserving Event IDs,
   timestamps, state (session/app/user scopes), historical and summary
   Events, user/session identity and the tenant namespace prefix.
3. Target counts and a canonical digest of both backends' read paths must
   match before the ONE ``expected_version`` CAS flips
   ``backend_profile.state_backend``.  Source data is never modified, any
   failure leaves the current profile unchanged, re-running is
   deterministic, and SQL->Redis uses the exact same path for rollback.

Every message raised by the errors below is a FIXED safe constant — tenant
identifiers, DSNs, content, digests and upstream exception text never leak
into exception strings or logs.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Literal

from trpc_agent_sdk.types import State

if TYPE_CHECKING:
    from trpc_service.storage.backend_resolver import TenantStateBackendResolver
    from trpc_service.config.tenant_repository import TenantConfigRepository
    from trpc_service.storage.message_repository import MessageReceiptRepository

from trpc_service.config.tenant import StateBackendKind, TenantConfigDraft
from trpc_service.config.tenant_repository import (
    TenantConfigVersionConflictError,
    TenantNotFoundError,
)

logger = logging.getLogger(__name__)

# Fixed, sanitized messages (also asserted verbatim by the CLI contract).
MSG_NOT_OFFLINE = "migration must be explicitly marked offline"
MSG_UNKNOWN_TARGET = "unknown target state backend"
MSG_BAD_EXPECTED_VERSION = "expected_version must be a positive integer"
MSG_REPOSITORY_READONLY = "tenant configuration repository cannot perform CAS updates"
MSG_TENANT_NOT_FOUND = "tenant not found"
MSG_VERSION_MISMATCH = "tenant configuration version does not match expected_version"
MSG_SAME_BACKEND = "source and target state backend are identical"
MSG_PROCESSING_RECEIPTS = "tenant has processing message executions; stop traffic first"
MSG_UNAVAILABLE = "state backend is not available"
MSG_VALIDATION_FAILED = "migrated state failed target validation; configuration not switched"
MSG_CAS_CONFLICT = "tenant configuration changed during migration; configuration not switched"


class StateMigrationError(Exception):
    """Base class for tenant state migration failures (fixed safe messages)."""


class StateMigrationPreconditionError(StateMigrationError):
    """Migration refused: a precondition or validation check failed."""


class StateMigrationUnavailableError(StateMigrationError):
    """Migration aborted: a backend or repository could not be used."""


@dataclass(frozen=True)
class StateMigrationResult:
    """Sanitized outcome of one completed (validated + CASed) migration."""

    tenant_id: str
    source_backend: StateBackendKind
    target_backend: StateBackendKind
    source_version: int
    target_version: int
    session_count: int
    event_count: int
    """Total Events copied (active + historical) across all sessions."""


def _app_namespace(tenant_id: str, app_id: str, version: int) -> str:
    # Must stay byte-identical to TenantAgentRuntime's app_name namespace
    # (trpc_service/agent/runtime.py).
    return f"{tenant_id}:{app_id}:v{version}"


def _session_scoped_state(state: dict) -> dict:
    """Drop the merged app/user (and temp) view keys from a session state.

    ``get_session`` returns app-/user-scoped entries merged into the session
    view under their prefixes; the session document itself must store only
    session-scoped keys (exactly what the live write paths produce).
    """
    return {
        key: value
        for key, value in state.items() if not key.startswith((State.APP_PREFIX, State.USER_PREFIX, State.TEMP_PREFIX))
    }


def _canonical_timestamp(timestamp: float) -> float:
    """Snap a float timestamp onto the microsecond grid the SQL storage uses.

    SQL persists ``datetime.fromtimestamp(ts)``; Redis keeps the raw float.
    Snapping BOTH sides through the same conversion makes the canonical
    digest backend-independent without loosening it.
    """
    return datetime.fromtimestamp(timestamp).timestamp()


def _json_canon(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


# SQL session/event storage drops parts without a payload when persisting
# (the SDK's content sanitizer); Redis keeps whatever was in memory.  The
# canonical view applies the same drop rule so the cross-backend digest
# compares the persisted SEMANTICS, not each backend's retention quirks.
_PART_PAYLOAD_FIELDS = (
    "text",
    "function_call",
    "function_response",
    "code_execution_result",
    "executable_code",
    "inline_data",
)


def _canonical_content(event) -> object:
    if event.content is None:
        return None
    dump = event.content.model_dump(mode="json", exclude_none=True)
    parts = [
        part for part in (dump.get("parts") or [])
        if isinstance(part, dict) and any(part.get(field) for field in _PART_PAYLOAD_FIELDS)
    ]
    if not parts:
        return None
    dump["parts"] = parts
    return dump


def _canonical_event(event) -> list:
    """Canonical payload of one Event as visible through EITHER backend's
    public read path.  Deliberately excludes backend-derived fields that
    cannot round-trip identically by design (e.g. session last_update_time).
    """
    content = _canonical_content(event)
    state_delta = event.actions.state_delta if (event.actions is not None and event.actions.state_delta) else None
    return [
        event.id,
        event.invocation_id,
        event.author,
        event.branch,
        _canonical_timestamp(event.timestamp),
        int(event.model_flags or 0),
        int(event.version or 0),
        bool(event.requires_completion),
        bool(event.visible),
        bool(event.partial),
        bool(event.turn_complete),
        event.error_code,
        event.error_message,
        event.tag,
        event.filter_key,
        event.request_id,
        event.parent_invocation_id,
        sorted(event.long_running_tool_ids or ()),
        _json_canon(content),
        _json_canon(state_delta),
    ]


async def _read_snapshot(session_service, app_name: str) -> dict[str, list]:
    """Read the whole namespace through the service's public read path."""
    response = await session_service.list_sessions(app_name=app_name)
    seen: dict[str, list] = {}
    for listed in response.sessions:
        session = await session_service.get_session(app_name=app_name, user_id=listed.user_id, session_id=listed.id)
        if session is None:
            raise StateMigrationUnavailableError(MSG_UNAVAILABLE)
        seen[f"{listed.user_id}/{listed.id}"] = [
            _json_canon(_session_scoped_state(session.state)),
            int(session.conversation_count),
            [_canonical_event(event) for event in session.events],
            [_canonical_event(event) for event in session.historical_events],
        ]
    return seen


def _digest(snapshot: dict[str, list]) -> str:
    return hashlib.sha256(_json_canon(snapshot).encode("utf-8")).hexdigest()


async def _copy_session(
    *,
    source_service,
    target_service,
    target_memory,
    source_app: str,
    target_app: str,
    user_id: str,
    session_id: str,
) -> int:
    """Copy one session via public Session/Memory methods; returns event count.

    Deterministic on re-entry: ``create_session`` against an existing session
    merges state, and the single ``update_session`` then rewrites the full
    event list, state and conversation counter — no duplicate Events.
    """
    source = await source_service.get_session(app_name=source_app, user_id=user_id, session_id=session_id)
    if source is None:
        raise StateMigrationUnavailableError(MSG_UNAVAILABLE)

    # create_session routes app:/user:-prefixed entries into the target's
    # shared app/user state buckets and stores the rest session-scoped.
    target = await target_service.create_session(app_name=target_app,
                                                 user_id=user_id,
                                                 session_id=session_id,
                                                 state=dict(source.state))
    target.events = [event.model_copy(deep=True) for event in source.events]
    target.historical_events = [event.model_copy(deep=True) for event in source.historical_events]
    target.conversation_count = source.conversation_count
    target.state = _session_scoped_state(source.state)
    await target_service.update_session(target)

    # Memory: re-materialise the session's stored Events (the active window
    # plus the retained historical Events the summary compressed) under the
    # TARGET namespace's save_key, so the next Worker's memory search keeps
    # working exactly like post-turn storage would.
    memory_view = target.model_copy(update={"events": target.events + target.historical_events})
    await target_memory.store_session(memory_view)
    return len(target.events) + len(target.historical_events)


async def migrate_tenant_state(
    *,
    tenant_id: str,
    target_backend: Literal["redis", "sql"],
    expected_version: int,
    offline: bool,
    tenant_repository: "TenantConfigRepository",
    receipt_repository: "MessageReceiptRepository",
    resolver: "TenantStateBackendResolver",
) -> StateMigrationResult:
    """Copy tenant state to the predicted next-version target namespace,
    validate it, then perform the single ``expected_version`` CAS on
    ``backend_profile.state_backend``.

    Raises only :class:`StateMigrationPreconditionError` (refused / failed
    validation) or :class:`StateMigrationUnavailableError` (backend or
    repository failure).  Both carry fixed safe messages; on any raise the
    tenant configuration is unchanged and the source data is untouched.
    """
    if offline is not True:
        raise StateMigrationPreconditionError(MSG_NOT_OFFLINE)
    if target_backend not in ("redis", "sql"):
        raise StateMigrationPreconditionError(MSG_UNKNOWN_TARGET)
    if isinstance(expected_version, bool) or not isinstance(expected_version, int) or expected_version < 1:
        raise StateMigrationPreconditionError(MSG_BAD_EXPECTED_VERSION)
    cas_update = getattr(tenant_repository, "update", None)
    if not callable(cas_update):
        # A read-only repository (JSON snapshot) cannot host a CAS cutover;
        # refuse before touching any state.
        raise StateMigrationPreconditionError(MSG_REPOSITORY_READONLY)

    try:
        config = await tenant_repository.get(tenant_id)
    except Exception:
        raise StateMigrationUnavailableError(MSG_UNAVAILABLE) from None
    if config is None:
        raise StateMigrationPreconditionError(MSG_TENANT_NOT_FOUND)
    if config.version != expected_version:
        raise StateMigrationPreconditionError(MSG_VERSION_MISMATCH)
    source_backend = config.backend_profile.state_backend
    if source_backend == target_backend:
        raise StateMigrationPreconditionError(MSG_SAME_BACKEND)

    try:
        processing = await receipt_repository.count_processing_by_tenant(tenant_id)
    except Exception:
        raise StateMigrationUnavailableError(MSG_UNAVAILABLE) from None
    if processing > 0:
        raise StateMigrationPreconditionError(MSG_PROCESSING_RECEIPTS)

    # Backend services are obtained ONLY through the Worker's resolver —
    # never by constructing SDK services directly.
    try:
        target_profile = config.backend_profile.model_copy(update={"state_backend": target_backend})
        source_backend_obj = resolver.resolve(config.backend_profile)
        target_backend_obj = resolver.resolve(target_profile)
        source_backend_obj.check_ready()
        target_backend_obj.check_ready()
    except Exception:
        raise StateMigrationUnavailableError(MSG_UNAVAILABLE) from None

    source_app = _app_namespace(tenant_id, config.app.app_id, expected_version)
    target_app = _app_namespace(tenant_id, config.app.app_id, expected_version + 1)
    source_service = source_backend_obj.session_service
    target_service = target_backend_obj.session_service
    target_memory = target_backend_obj.memory_service

    try:
        listed = await source_service.list_sessions(app_name=source_app)
        session_keys = sorted({(session.user_id, session.id) for session in listed.sessions})
        event_count = 0
        for user_id, session_id in session_keys:
            event_count += await _copy_session(
                source_service=source_service,
                target_service=target_service,
                target_memory=target_memory,
                source_app=source_app,
                target_app=target_app,
                user_id=user_id,
                session_id=session_id,
            )
    except StateMigrationError:
        raise
    except Exception:
        # Any backend failure mid-copy: sanitized; configuration untouched.
        logger.warning("state migration copy failed for tenant=%s target=%s", tenant_id, target_backend)
        raise StateMigrationUnavailableError(MSG_UNAVAILABLE) from None

    try:
        source_snapshot = await _read_snapshot(source_service, source_app)
        target_snapshot = await _read_snapshot(target_service, target_app)
    except StateMigrationError:
        raise
    except Exception:
        logger.warning("state migration validation read failed for tenant=%s", tenant_id)
        raise StateMigrationUnavailableError(MSG_UNAVAILABLE) from None
    if set(source_snapshot) != set(target_snapshot) or _digest(source_snapshot) != _digest(target_snapshot):
        logger.warning("state migration validation mismatch for tenant=%s; configuration not switched", tenant_id)
        raise StateMigrationPreconditionError(MSG_VALIDATION_FAILED)

    desired = TenantConfigDraft(
        enabled=config.enabled,
        app=config.app,
        governance=config.governance,
        backend_profile=target_profile,
        audit_policy=config.audit_policy,
    )
    try:
        updated = await cas_update(tenant_id, expected_version, desired)
    except (TenantConfigVersionConflictError, TenantNotFoundError):
        raise StateMigrationPreconditionError(MSG_CAS_CONFLICT) from None
    except Exception:
        logger.warning("state migration CAS failed for tenant=%s", tenant_id)
        raise StateMigrationUnavailableError(MSG_UNAVAILABLE) from None

    logger.info(
        "state migration complete: tenant=%s sessions=%s events=%s %s->%s v%s->v%s",
        tenant_id,
        len(session_keys),
        event_count,
        source_backend,
        target_backend,
        expected_version,
        updated.version,
    )
    return StateMigrationResult(
        tenant_id=tenant_id,
        source_backend=source_backend,
        target_backend=target_backend,
        source_version=expected_version,
        target_version=updated.version,
        session_count=len(session_keys),
        event_count=event_count,
    )


__all__ = [
    "StateMigrationError",
    "StateMigrationPreconditionError",
    "StateMigrationResult",
    "StateMigrationUnavailableError",
    "migrate_tenant_state",
]
