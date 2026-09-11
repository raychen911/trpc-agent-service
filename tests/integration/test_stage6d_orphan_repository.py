"""Stage 6D (integration, real PostgreSQL): orphaned-approval disposition.

Proves on real data: the stale-executing query, the ONE atomic
disposition transaction (approval -> failed + approval audit + decision
receipt -> failed + message audit), idempotent replay, the ACTIVE/PENDING/
INCONSISTENT/tenant rejections, and — the safety core — that after a
disposition NO code path can move the approval back or EXECUTE the tool
again (claim_decision never returns EXECUTE for a non-pending row; the
decision message replays the fixed failed terminal).
"""

from __future__ import annotations

import asyncio
import uuid

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from .pg_helpers import requires_docker, run_alembic

TENANT = "tenant_default"


def _run(coro):
    return asyncio.run(coro)


async def _with_repo(db_url, fn):
    from trpc_service.storage.approval_repository import SqlToolApprovalRepository
    engine = create_async_engine(db_url)
    repo = SqlToolApprovalRepository(engine)
    try:
        return await fn(repo)
    finally:
        await repo.close()


async def _set_decided_age(db_url, approval_id: uuid.UUID, age_seconds: int) -> None:
    """Backdate decided_at — the observable shape of a crashed worker that
    claimed a decision and died mid-execution."""
    engine = create_async_engine(db_url)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(
                sa.text("UPDATE tool_approval_requests"
                        " SET decided_at = now() - make_interval(secs => :age)"
                        " WHERE approval_id = :aid AND state = 'executing'"),
                {
                    "age": age_seconds,
                    "aid": str(approval_id)
                },
            )
            assert result.rowcount == 1
    finally:
        await engine.dispose()


async def _make_orphan(
    db_url: str,
    *,
    age_seconds: int | None = 7200,
    do_claim: bool = True,
    seed_decision_receipt: bool = True,
    decision: str = "approve",
    tenant: str = TENANT,
    message_tag: str | None = None,
):
    """Drive the real 6A2 transactions up to 'executing' and return a dict
    describing the orphan (approval_id, decision_message_id, ids)."""
    from trpc_service.governance.approval import ApprovalAction
    from trpc_service.storage.message_repository import SqlMessageReceiptRepository
    from trpc_service.transport.models import WorkerApprovalTask, WorkerTask

    tag = message_tag or uuid.uuid4().hex[:8]
    box: dict = {}

    async def _drive():
        engine = create_async_engine(db_url)
        from trpc_service.storage.approval_repository import SqlToolApprovalRepository
        approvals = SqlToolApprovalRepository(engine, owns_engine=False)
        receipts = SqlMessageReceiptRepository(engine, owns_engine=False)
        try:
            user_id = "usr_v1_" + uuid.uuid4().hex + uuid.uuid4().hex[:16]
            session_id = "ses_v1_" + uuid.uuid4().hex + uuid.uuid4().hex[:16]
            original_task = WorkerTask(
                protocol_version=1,
                request_id=uuid.uuid4(),
                tenant_id=tenant,
                app_id="app_demo",
                config_version=1,
                user_id=user_id,
                channel="web_console",
                session_id=session_id,
                message_id=f"msg-orig-{tag}",
                message="run the tool please",
            )
            claim = await receipts.claim(original_task, "run the tool please")
            approval_id = uuid.uuid4()
            req = await approvals.pause_first_request(
                approval_id=approval_id,
                tenant_id=tenant,
                app_id="app_demo",
                config_version=1,
                channel="web_console",
                user_id=user_id,
                session_id=session_id,
                receipt_id=claim.receipt_id,
                function_call_id=f"call-{tag}",
                tool_name="get_current_time",
                tool_args={"query": "now"},
                pending_response="pending reply",
                latency_ms=5,
            )
            dmid = f"msg-decide-{tag}"
            if do_claim:
                got = await approvals.claim_decision(
                    approval_id,
                    tenant_id=tenant,
                    channel="web_console",
                    user_id=user_id,
                    session_id=session_id,
                    decision=decision,
                    decision_message_id=dmid,
                )
                assert got.action == ApprovalAction.EXECUTE
            if do_claim and seed_decision_receipt:
                decision_task = WorkerApprovalTask(
                    protocol_version=1,
                    request_id=uuid.uuid4(),
                    tenant_id=tenant,
                    app_id="app_demo",
                    config_version=1,
                    user_id=user_id,
                    channel="web_console",
                    session_id=session_id,
                    message_id=dmid,
                    approval_id=approval_id,
                    decision=decision,
                )
                dclaim = await receipts.claim(decision_task, f"/{decision} {approval_id}")
                assert dclaim.action.value == "execute"
                box["decision_receipt_id"] = dclaim.receipt_id
            box.update({
                "approval_id": approval_id,
                "decision_message_id": dmid,
                "user_id": user_id,
                "session_id": session_id,
                "request": req,
            })
        finally:
            await approvals.close()
            await receipts.close()

    await _drive()
    if age_seconds is not None:
        await _set_decided_age(db_url, box["approval_id"], age_seconds)
    return box


@requires_docker
class TestOrphanDisposition:

    def test_query_lists_only_stale_executing_for_tenant(self, postgres_url):
        run_alembic(postgres_url, "upgrade", "head", check=True)
        # fresh (must not appear), 1h stale, 5h stale (must appear, oldest first)
        _run(_make_orphan(postgres_url, age_seconds=None, message_tag="fresh"))
        young = _run(_make_orphan(postgres_url, age_seconds=3600, message_tag="young"))
        old = _run(_make_orphan(postgres_url, age_seconds=18000, message_tag="old"))

        async def _list(repo, stale):
            return await repo.list_stale_executing(tenant_id=TENANT, stale_seconds=stale, limit=50)

        four_h = _run(_with_repo(postgres_url, lambda r: _list(r, 4 * 3600)))
        assert [o.approval_id for o in four_h] == [old["approval_id"]]
        two_h = _run(_with_repo(postgres_url, lambda r: _list(r, 2 * 3600)))
        assert [o.approval_id for o in two_h] == [old["approval_id"]]
        exact = _run(_with_repo(postgres_url, lambda r: _list(r, 3600)))
        assert [o.approval_id for o in exact] == [old["approval_id"], young["approval_id"]]
        orphan = exact[1]
        assert orphan.state == "executing"
        assert orphan.decision == "approve"
        assert len(orphan.args_digest) == 64
        assert not hasattr(orphan, "tool_args")
        assert orphan.age_seconds >= 3600

    def test_terminate_is_atomic_and_audited_then_idempotent(self, postgres_url):
        run_alembic(postgres_url, "upgrade", "head", check=True)
        box = _run(_make_orphan(postgres_url, age_seconds=7200, message_tag="atomic"))
        aid = box["approval_id"]

        from trpc_service.governance.approval import OrphanTerminationAction

        async def _term(repo):
            return await repo.terminate_orphan(aid, tenant_id=TENANT, stale_seconds=3600)

        first = _run(_with_repo(postgres_url, _term))
        assert first.action == OrphanTerminationAction.TERMINATED
        assert first.state == "failed"

        async def _inspect():
            engine = create_async_engine(postgres_url)
            try:
                async with engine.connect() as conn:
                    appr = (await conn.execute(
                        sa.text("SELECT state, finished_at IS NOT NULL AS fin, response_text"
                                " FROM tool_approval_requests WHERE approval_id = :a"), {"a": str(aid)})).first()
                    appr_events = (await conn.execute(
                        sa.text("SELECT event_type FROM tool_approval_audit_events"
                                " WHERE approval_id = :a ORDER BY occurred_at, event_type"),
                        {"a": str(aid)})).scalars().all()
                    receipt = (await conn.execute(
                        sa.text("SELECT state, error_code FROM message_receipts WHERE receipt_id = :r"),
                        {"r": str(box["decision_receipt_id"])})).first()
                    msg_audit = (await conn.execute(
                        sa.text("SELECT event_type, error_code FROM message_audit_events"
                                " WHERE receipt_id = :r AND event_type = 'failed'"),
                        {"r": str(box["decision_receipt_id"])})).first()
                    pending_executing = (await conn.execute(
                        sa.text("SELECT COUNT(*) FROM tool_approval_requests WHERE state = 'executing'"
                                " AND approval_id = :a"), {"a": str(aid)})).scalar()
                    return appr, list(appr_events), receipt, msg_audit, pending_executing
            finally:
                await engine.dispose()

        appr, events, receipt, msg_audit, still = _run(_inspect())
        assert appr.state == "failed" and appr.fin is True and appr.response_text is None
        assert events == ["created", "decided", "failed"]
        assert receipt.state == "failed"
        assert receipt.error_code == "approval_execution_failed"
        assert msg_audit.event_type == "failed"
        assert msg_audit.error_code == "approval_execution_failed"
        assert still == 0

        # idempotent: second call replays the terminal fact, rewrites nothing
        second = _run(_with_repo(postgres_url, _term))
        assert second.action == OrphanTerminationAction.ALREADY_TERMINATED
        assert second.state == "failed"
        _, events2, _, _, _ = _run(_inspect())
        assert events2 == ["created", "decided", "failed"]

        # the orphan left the candidate list
        remaining = _run(
            _with_repo(postgres_url, lambda r: r.list_stale_executing(tenant_id=TENANT, stale_seconds=3600, limit=50)))
        assert aid not in {o.approval_id for o in remaining}

    def test_disposition_never_allows_reexecution(self, postgres_url):
        """After terminate: same decision replays FAILED, a fresh decision
        conflicts — and the row can never return to EXECUTE."""
        run_alembic(postgres_url, "upgrade", "head", check=True)
        box = _run(_make_orphan(postgres_url, age_seconds=7200, message_tag="noreexec"))
        aid = box["approval_id"]

        from trpc_service.governance.approval import ApprovalAction, OrphanTerminationAction
        from trpc_service.storage.message_repository import ReceiptAction, SqlMessageReceiptRepository
        from trpc_service.transport.models import WorkerApprovalTask

        _run(_with_repo(postgres_url, lambda r: r.terminate_orphan(aid, tenant_id=TENANT, stale_seconds=3600)))
        assert _run(_with_repo(postgres_url, lambda r: r.terminate_orphan(aid, tenant_id=TENANT, stale_seconds=3600))
                    ).action == OrphanTerminationAction.ALREADY_TERMINATED

        req = box["request"]

        async def _attempts():
            from trpc_service.storage.approval_repository import SqlToolApprovalRepository
            engine = create_async_engine(postgres_url)
            approvals = SqlToolApprovalRepository(engine, owns_engine=False)
            receipts = SqlMessageReceiptRepository(engine, owns_engine=False)
            try:
                base = dict(
                    tenant_id=TENANT,
                    channel="web_console",
                    user_id=req.user_id,
                    session_id=req.session_id,
                )
                same = await approvals.claim_decision(
                    aid,
                    decision="approve",
                    decision_message_id=box["decision_message_id"],
                    **base,
                )
                fresh = await approvals.claim_decision(
                    aid,
                    decision="approve",
                    decision_message_id=f"msg-another-{uuid.uuid4().hex[:8]}",
                    **base,
                )
                reject = await approvals.claim_decision(
                    aid,
                    decision="reject",
                    decision_message_id=f"msg-reject-{uuid.uuid4().hex[:8]}",
                    **base,
                )
                # platform redelivery of the ORIGINAL decision message
                task = WorkerApprovalTask(
                    protocol_version=1,
                    request_id=uuid.uuid4(),
                    tenant_id=TENANT,
                    app_id="app_demo",
                    config_version=1,
                    user_id=req.user_id,
                    channel="web_console",
                    session_id=req.session_id,
                    message_id=box["decision_message_id"],
                    approval_id=aid,
                    decision="approve",
                )
                receipt_claim = await receipts.claim(task, f"/approve {aid}")
                return same.action, fresh.action, reject.action, receipt_claim
            finally:
                await approvals.close()
                await receipts.close()

        same, fresh, reject, receipt_claim = _run(_attempts())
        assert same != ApprovalAction.EXECUTE
        assert fresh != ApprovalAction.EXECUTE
        assert reject != ApprovalAction.EXECUTE
        # redelivered decision message replays the fixed failed terminal
        assert receipt_claim.action == ReceiptAction.REPLAY
        assert receipt_claim.error_code is not None
        assert receipt_claim.error_code.value == "approval_execution_failed"

    def test_active_and_pending_and_mismatch_are_rejected(self, postgres_url):
        run_alembic(postgres_url, "upgrade", "head", check=True)
        from trpc_service.governance.approval import OrphanTerminationAction

        # (a) executing but within the operator threshold -> ACTIVE
        box = _run(_make_orphan(postgres_url, age_seconds=60, message_tag="active"))
        res = _run(
            _with_repo(postgres_url,
                       lambda r: r.terminate_orphan(box["approval_id"], tenant_id=TENANT, stale_seconds=3600)))
        assert res.action == OrphanTerminationAction.ACTIVE
        assert _run(
            _with_repo(postgres_url, lambda r: r.terminate_orphan(
                box["approval_id"], tenant_id=TENANT, stale_seconds=30))).action == OrphanTerminationAction.TERMINATED

        # (b) pending -> PENDING, row untouched
        pend = _run(
            _make_orphan(postgres_url,
                         age_seconds=None,
                         do_claim=False,
                         seed_decision_receipt=False,
                         message_tag="pend"))
        res = _run(
            _with_repo(postgres_url,
                       lambda r: r.terminate_orphan(pend["approval_id"], tenant_id=TENANT, stale_seconds=1)))
        assert res.action == OrphanTerminationAction.PENDING
        got = _run(_with_repo(postgres_url, lambda r: r.get(pend["approval_id"])))
        assert got.state == "pending"

        # (c) executing WITHOUT the decision receipt (crash-before-claim data
        # cannot exist through the product path, but a disposition must still
        # refuse to invent one)
        box2 = _run(_make_orphan(postgres_url, age_seconds=7200, seed_decision_receipt=False, message_tag="noreceipt"))
        res2 = _run(
            _with_repo(postgres_url,
                       lambda r: r.terminate_orphan(box2["approval_id"], tenant_id=TENANT, stale_seconds=3600)))
        assert res2.action == OrphanTerminationAction.INCONSISTENT
        got2 = _run(_with_repo(postgres_url, lambda r: r.get(box2["approval_id"])))
        assert got2.state == "executing"

    def test_cross_tenant_disposition_is_not_available(self, postgres_url):
        run_alembic(postgres_url, "upgrade", "head", check=True)
        box = _run(_make_orphan(postgres_url, age_seconds=7200, message_tag="xt"))
        from trpc_service.governance.approval import OrphanTerminationAction
        res = _run(
            _with_repo(postgres_url,
                       lambda r: r.terminate_orphan(box["approval_id"], tenant_id="tenant_other", stale_seconds=3600)))
        assert res.action == OrphanTerminationAction.NOT_AVAILABLE
        got = _run(_with_repo(postgres_url, lambda r: r.get(box["approval_id"])))
        assert got.state == "executing"

    def test_completed_orphan_shape_replays_terminal_only(self, postgres_url):
        """A finalized (completed) approval is a mismatch for disposition;
        it must never be flipped to failed."""
        run_alembic(postgres_url, "upgrade", "head", check=True)
        box = _run(_make_orphan(postgres_url, age_seconds=7200, message_tag="done"))
        from trpc_service.governance.approval import OrphanTerminationAction

        async def _finalize():
            from trpc_service.storage.approval_repository import SqlToolApprovalRepository
            engine = create_async_engine(postgres_url)
            repo = SqlToolApprovalRepository(engine)
            try:
                await repo.finalize(
                    box["approval_id"],
                    receipt_id=box["decision_receipt_id"],
                    terminal_state="completed",
                    response_text="tool ran, all good",
                    error_code=None,
                    latency_ms=50,
                )
                return await repo.terminate_orphan(box["approval_id"], tenant_id=TENANT, stale_seconds=3600)
            finally:
                await repo.close()

        res = _run(_finalize())
        assert res.action == OrphanTerminationAction.INCONSISTENT
        got = _run(_with_repo(postgres_url, lambda r: r.get(box["approval_id"])))
        assert got.state == "completed"
        assert got.response_text == "tool ran, all good"
