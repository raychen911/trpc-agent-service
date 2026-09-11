"""IM enrollment is an audited operator decision with optimistic concurrency."""

import uuid
from sqlalchemy import select

from tests.reliability.test_management import managed as managed_fixture, register_config
from trpc_service.persistence.models import IMEventReceiptRow, IMMembershipRow, AuditLogRow

managed = managed_fixture


def test_enrollment_revocation_and_tenant_scope(managed):
    client, database = managed
    body = register_config(client)
    tenant_id = body["config"]["tenant_id"]
    binding_id = body["config"]["channel_bindings"][0]["binding_id"]
    base = f"/admin/tenants/{tenant_id}"
    assert client.post(base + "/configs", json=body).status_code == 201
    assert client.post(base + "/configs/3/publish", json={"expected_active_version": 0}).status_code == 200
    actor_id = "usr_" + uuid.uuid4().hex
    with database.sessions.begin() as session:
        session.add(
            IMEventReceiptRow(tenant_id=tenant_id,
                              binding_id=binding_id,
                              external_event_id="test-1",
                              event_kind="chat",
                              content_hash="synthetic-hash",
                              disposition="rejected",
                              reason="membership_required",
                              actor_id=actor_id,
                              owner_generation=1))
    events = client.get(base + "/im/events").json()
    assert events[0]["actor_id"] == actor_id and "context_ciphertext" not in events[0]
    path = base + f"/im/bindings/{binding_id}/members/{actor_id}"
    grant = {"active": True, "grants": ["chat"], "expected_epoch": 0}
    assert client.put(path, json=grant).status_code == 200
    assert client.put(path, json=grant).status_code == 409
    assert client.put(path.replace(tenant_id, "tenant_other"), json=grant).status_code == 404
    assert client.put(path, json={"active": False, "grants": [], "expected_epoch": 1}).status_code == 200
    with database.sessions() as session:
        member = session.get(IMMembershipRow, (tenant_id, binding_id, actor_id))
        assert not member.active and member.authz_epoch == 2
        assert len(list(session.scalars(
            select(AuditLogRow).where(AuditLogRow.decision == "im_membership_changed")))) == 2


def test_validation_response_never_echoes_pasted_secret(managed):
    client, _ = managed
    response = client.post("/admin/backend-profiles", json={"secret_ref": {"uri": "synthetic-pasted-secret"}})
    assert response.status_code == 422
    assert "synthetic-pasted-secret" not in response.text
