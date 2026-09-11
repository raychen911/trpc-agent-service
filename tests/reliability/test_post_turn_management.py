"""Only failed owned derived work can be retried; unknown costs remain blocked."""

from tests.reliability.test_management import managed as managed_fixture
from tests.reliability.test_budget import seed, reserve
from trpc_service.persistence.models import PostTurnTaskRow, SessionEventRow

managed = managed_fixture


def test_retry_requires_reconciled_cost_and_exact_task_revision(managed):
    client, database = managed
    ledger = seed(database)
    with database.sessions.begin() as session:
        session.add(
            SessionEventRow(tenant_id="tenant_acme",
                            session_id="session",
                            seq_no=1,
                            event_id="event",
                            execution_id="tenant_acme-execution",
                            app_id="app",
                            user_id="user",
                            event_type="message",
                            content_json={}))
        session.add(
            PostTurnTaskRow(task_id="summary",
                            tenant_id="tenant_acme",
                            session_id="session",
                            source_event_seq=1,
                            task_type="summary",
                            status="failed",
                            attempt=3))
    reserve(ledger)
    ledger.mark_sent("tenant_acme", "attempt-1")
    ledger.pending("tenant_acme", "attempt-1")
    path = "/admin/tenants/tenant_acme/post-turn/summary/retry"
    assert client.post(path, json={"expected_attempt": 3}).status_code == 409
    assert client.post(path.replace("tenant_acme", "tenant_other"), json={"expected_attempt": 3}).status_code == 404
    # Supply known provider usage to the ledger, not an untrusted HTTP amount.
    ledger.settle("tenant_acme", "attempt-1", 1, 1, "b" * 64)
    assert client.post(path, json={"expected_attempt": 2}).status_code == 409
    assert client.post(path, json={"expected_attempt": 3}).status_code == 200
    assert client.post(path, json={"expected_attempt": 3}).status_code == 409
    rows = client.get("/admin/tenants/tenant_acme/post-turn").json()
    assert rows[0]["status"] == "pending"
