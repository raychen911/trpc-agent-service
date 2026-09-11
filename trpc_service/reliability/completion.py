"""One platform transaction for final output, derived work, audit and completion."""

import uuid
from sqlalchemy import select

from trpc_service.persistence.models import (
    AuditLogRow,
    ExecutionAttemptRow,
    InboundMessageRow,
    OutboxMessageRow,
    PostTurnTaskRow,
    SessionEventRow,
    utcnow,
)
from trpc_service.storage import ConflictError
from .execution import timezone_utc


class ExecutionCompletion:

    def __init__(self, database):
        self.database = database

    def finish(self, work, worker_id, outputs, route, status, error_type):
        with self.database.sessions.begin() as session:
            row = session.get(InboundMessageRow, work.inbound_message_id, with_for_update=True)
            if (row is None or row.tenant_id != route.tenant_id or row.lease_owner != worker_id
                    or row.status != "processing" or row.lease_expires_at is None
                    or row.lease_expires_at.replace(tzinfo=timezone_utc(row.lease_expires_at)) <= utcnow()):
                raise ConflictError("execution completion requires a current input lease")
            for part, event in enumerate(outputs):
                payload = event.model_dump(mode="json")
                output = session.scalar(
                    select(OutboxMessageRow).where(OutboxMessageRow.tenant_id == route.tenant_id,
                                                   OutboxMessageRow.inbound_message_id == row.inbound_message_id,
                                                   OutboxMessageRow.part_no == part))
                if output is None:
                    session.add(
                        OutboxMessageRow(outbox_message_id=uuid.uuid4().hex,
                                         tenant_id=route.tenant_id,
                                         inbound_message_id=row.inbound_message_id,
                                         part_no=part,
                                         payload_json=payload,
                                         status="pending"))
                elif output.payload_json != payload:
                    raise ConflictError("final output differs from persisted output")
            boundary = session.scalar(
                select(SessionEventRow.seq_no).where(
                    SessionEventRow.tenant_id == route.tenant_id, SessionEventRow.session_id == route.session_id,
                    SessionEventRow.execution_id == work.execution_id).order_by(SessionEventRow.seq_no.desc()).limit(1))
            if status == "succeeded" and boundary is not None:
                for kind in ("summary", "memory"):
                    old = session.scalar(
                        select(PostTurnTaskRow.task_id).where(PostTurnTaskRow.tenant_id == route.tenant_id,
                                                              PostTurnTaskRow.session_id == route.session_id,
                                                              PostTurnTaskRow.task_type == kind,
                                                              PostTurnTaskRow.source_event_seq == boundary))
                    if old is None:
                        session.add(
                            PostTurnTaskRow(task_id=uuid.uuid4().hex,
                                            tenant_id=route.tenant_id,
                                            session_id=route.session_id,
                                            task_type=kind,
                                            source_event_seq=boundary,
                                            status="pending"))
            for attempt in session.scalars(
                    select(ExecutionAttemptRow).where(ExecutionAttemptRow.execution_id == work.execution_id,
                                                      ExecutionAttemptRow.status == "running")):
                attempt.status, attempt.error_type, attempt.ended_at = status, error_type, utcnow()
            row.status, row.error_type = status, error_type
            row.lease_owner = row.lease_expires_at = row.next_retry_at = None
            session.add(
                AuditLogRow(audit_id=uuid.uuid4().hex,
                            tenant_id=route.tenant_id,
                            user_id=route.actor_id,
                            session_id=route.session_id,
                            decision="execution_" + status,
                            error_type=error_type,
                            channel=work.payload.get("message", {}).get("channel"),
                            trace_id=work.trace_id,
                            request_id=work.request_id,
                            config_version=route.config_version,
                            redacted=True,
                            metadata_json={"execution_id": work.execution_id}))
