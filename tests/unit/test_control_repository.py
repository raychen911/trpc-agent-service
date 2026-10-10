from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.conftest import binding, envelope, tenant_config
from tests.unit.test_postgres_repository import Connection, Pool
from trpc_service.channels.envelopes import MediaReference
from trpc_service.tenant.control import (
    ControlVersionConflict,
    IdempotencyConflict,
    PostgresControlPlaneRepository,
    _decode_cursor,
    _encode_cursor,
    _record_json,
    _safe_provider_code,
)
from trpc_service.tenant.models import Channel

RUN_NONCE = "acceptance-nonce-123456"


def control(connection: Connection) -> PostgresControlPlaneRepository:
    return PostgresControlPlaneRepository(Pool(connection))


def tenant_row(version=1):
    return {
        "tenant_id": "tenant-a",
        "display_name": "Tenant",
        "status": "active",
        "control_version": version,
        "created_at": datetime(2026, 1, 1, tzinfo=UTC),
        "updated_at": datetime(2026, 1, 2, tzinfo=UTC),
    }


def kwargs():
    return {
        "tenant_id": "tenant-a",
        "actor": "admin",
        "idempotency_key": "idem-key",
        "request_hash": "request-hash",
    }


@pytest.mark.asyncio
async def test_create_and_get_tenant_cached_conflict_and_success() -> None:
    cached = Connection(
        fetchrows=[{"request_hash": "request-hash", "response_json": '{"cached":true}'}]
    )
    assert await control(cached).create_tenant(display_name="Tenant", **kwargs()) == {
        "cached": True
    }

    created = await control(Connection(fetchrows=[None, tenant_row()])).create_tenant(
        display_name="Tenant", **kwargs()
    )
    assert created["created_at"].startswith("2026-")
    with pytest.raises(ControlVersionConflict, match="exists"):
        await control(Connection(fetchrows=[None, None])).create_tenant(
            display_name="Tenant", **kwargs()
        )
    assert await control(Connection(fetchrows=[None])).get_tenant("tenant-a") is None
    assert (await control(Connection(fetchrows=[tenant_row()])).get_tenant("tenant-a"))[
        "tenant_id"
    ] == "tenant-a"


@pytest.mark.asyncio
async def test_binding_idempotency_and_version_paths() -> None:
    route_binding = binding()
    with pytest.raises(ValueError, match="identity"):
        await control(Connection()).put_binding(
            binding_id="wrong",
            binding=route_binding,
            expected_version=1,
            **kwargs(),
        )
    cached = Connection(
        fetchrows=[{"request_hash": "request-hash", "response_json": {"cached": True}}]
    )
    assert (
        await control(cached).put_binding(
            binding_id=route_binding.binding_id,
            binding=route_binding,
            expected_version=1,
            **kwargs(),
        )
    )["cached"]

    row = {
        "binding_id": route_binding.binding_id,
        "app_id": route_binding.app_id,
        "channel": route_binding.channel.value,
        "account_id": route_binding.account_id,
        "enabled": True,
        "control_version": 1,
    }
    connection = Connection(fetchrows=[None, row], fetchvals=[1, 2])
    result = await control(connection).put_binding(
        binding_id=route_binding.binding_id,
        binding=route_binding,
        expected_version=1,
        **kwargs(),
    )
    assert result["tenant_control_version"] == 2

    with pytest.raises(LookupError, match="tenant"):
        await control(Connection(fetchrows=[None], fetchvals=[None])).put_binding(
            binding_id=route_binding.binding_id,
            binding=route_binding,
            expected_version=1,
            **kwargs(),
        )
    with pytest.raises(ControlVersionConflict, match="changed"):
        await control(Connection(fetchrows=[None], fetchvals=[2])).put_binding(
            binding_id=route_binding.binding_id,
            binding=route_binding,
            expected_version=1,
            **kwargs(),
        )


@pytest.mark.asyncio
async def test_create_config_for_new_and_existing_app() -> None:
    payload = tenant_config().model_dump(
        mode="json", exclude={"tenant_id", "app_id", "version", "created_at"}
    )
    connection = Connection(fetchrows=[None], fetchvals=[1, None, 1, 2])
    result = await control(connection).create_config_revision(
        app_id="support", config=payload, expected_version=1, **kwargs()
    )
    assert result["version"] == 1 and len(result["checksum"]) == 64
    assert any("INSERT INTO agent_apps" in call[1][0] for call in connection.calls)

    existing = Connection(fetchrows=[None], fetchvals=[2, 1, 2, 3])
    result = await control(existing).create_config_revision(
        app_id="support", config=payload, expected_version=2, **kwargs()
    )
    assert result["tenant_control_version"] == 3
    assert not any("INSERT INTO agent_apps" in call[1][0] for call in existing.calls)

    cached = Connection(
        fetchrows=[{"request_hash": "request-hash", "response_json": '{"version":9}'}]
    )
    assert (
        await control(cached).create_config_revision(
            app_id="support", config=payload, expected_version=1, **kwargs()
        )
    )["version"] == 9


async def activate(percentage, *, exists=1, operation="activate_config"):
    connection = Connection(fetchrows=[None], fetchvals=[1, exists, 2])
    result = await control(connection).activate_config(
        app_id="support",
        version=1,
        percentage=percentage,
        expected_version=1,
        operation=operation,
        **kwargs(),
    )
    return result, connection


@pytest.mark.asyncio
async def test_activate_gray_cancel_full_and_rollback_labels() -> None:
    with pytest.raises(ValueError, match="percentage"):
        await control(Connection()).activate_config(
            app_id="support", version=1, percentage=101, expected_version=1, **kwargs()
        )
    with pytest.raises(LookupError, match="revision"):
        await activate(50, exists=None)
    for percentage in (0, 25, 100):
        result, connection = await activate(percentage)
        assert result["percentage"] == percentage
        updates = [call for call in connection.calls if call[0] == "execute"]
        assert updates
    _, rollback = await activate(100, operation="rollback_config")
    assert any("rollback_config" in str(call[1]) for call in rollback.calls if call[0] == "execute")

    cached = Connection(
        fetchrows=[{"request_hash": "request-hash", "response_json": {"percentage": 10}}]
    )
    assert (
        await control(cached).activate_config(
            app_id="support", version=1, percentage=10, expected_version=1, **kwargs()
        )
    )["percentage"] == 10


@pytest.mark.asyncio
async def test_audit_cursor_dead_letters_and_helpers() -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    ids = [uuid4(), uuid4()]
    rows = [
        {"occurred_at": timestamp, "audit_id": ids[0], "decision": "one"},
        {"occurred_at": timestamp, "audit_id": ids[1], "decision": "two"},
    ]
    first = await control(Connection(fetches=[rows])).audit_page("tenant-a", cursor=None, limit=1)
    assert len(first["items"]) == 1 and first["next_cursor"]
    cursor = _encode_cursor(timestamp, ids[0])
    assert _decode_cursor(cursor) == (timestamp, ids[0])
    second = await control(Connection(fetches=[rows[:1]])).audit_page(
        "tenant-a", cursor=cursor, limit=999
    )
    assert second["next_cursor"] is None
    with pytest.raises(ValueError, match="cursor"):
        _decode_cursor("not-a-cursor")

    dead = await control(Connection(fetches=[[{"dead_letter_id": ids[0]}]])).dead_letters(
        "tenant-a", limit=0
    )
    assert dead[0]["dead_letter_id"] == str(ids[0])
    converted = _record_json({"when": timestamp, "id": ids[0], "value": 1})
    assert converted["when"].endswith("+00:00") and converted["id"] == str(ids[0])


@pytest.mark.asyncio
async def test_wecom_acceptance_snapshot_is_tenant_scoped_bounded_and_hash_only() -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    event_id = uuid4()
    state = {
        "owner_hash": "a" * 64,
        "epoch": 7,
        "phase": "authenticated",
        "acquired_at": timestamp,
        "authenticated_at": timestamp,
        "disconnected_at": None,
        "released_at": None,
        "last_provider_event_hash": "b" * 64,
        "last_provider_event_at": timestamp,
        "updated_at": timestamp,
        "owner_id": "must-not-leak",
        "secret": "must-not-leak",
    }
    events = [
        {
            "event_id": event_id,
            "connection_epoch": 7,
            "event_type": "provider_event",
            "owner_hash": "a" * 64,
            "provider_event_hash": "b" * 64,
            "occurred_at": timestamp,
            "provider_event_id": "must-not-leak",
            "body": "must-not-leak",
        }
    ]
    connection = Connection(fetchrows=[state], fetches=[events], fetchvals=[1])
    result = await control(connection).wecom_acceptance_snapshot("tenant-a", "binding-a", limit=999)

    assert result is not None
    assert set(result) == {"state", "events"}
    assert set(result["state"]) == {
        "owner_hash",
        "epoch",
        "phase",
        "acquired_at",
        "authenticated_at",
        "disconnected_at",
        "released_at",
        "last_provider_event_hash",
        "last_provider_event_at",
        "updated_at",
    }
    assert set(result["events"][0]) == {
        "event_id",
        "connection_epoch",
        "event_type",
        "owner_hash",
        "provider_event_hash",
        "occurred_at",
    }
    assert result["events"][0]["event_id"] == str(event_id)
    calls = connection.calls
    assert calls[0][0] == "execute" and "app.tenant_id" in calls[0][1][0]
    event_query = next(call for call in calls if call[0] == "fetch")
    assert "ORDER BY occurred_at DESC,event_id DESC" in event_query[1][0]
    assert event_query[1][-1] == 200
    assert "must-not-leak" not in repr(result)


@pytest.mark.asyncio
async def test_wecom_acceptance_snapshot_hides_unknown_or_wrong_channel_binding() -> None:
    connection = Connection(fetchvals=[None])
    assert (
        await control(connection).wecom_acceptance_snapshot(
            "tenant-a", "missing-or-wrong-channel", limit=0
        )
        is None
    )
    assert not any(call[0] in {"fetch", "fetchrow"} for call in connection.calls)
    validation = next(call for call in connection.calls if call[0] == "fetchval")
    assert "tenant_id=$1" in validation[1][0]
    assert "channel='wecom_ai_bot'" in validation[1][0]


@pytest.mark.asyncio
async def test_wecom_acceptance_snapshot_clamps_the_repository_lower_limit() -> None:
    connection = Connection(fetchrows=[None], fetches=[[]], fetchvals=[1])
    result = await control(connection).wecom_acceptance_snapshot("tenant-a", "binding-a", limit=0)

    assert result == {"state": None, "events": []}
    event_query = next(call for call in connection.calls if call[0] == "fetch")
    assert event_query[1][-1] == 1


@pytest.mark.asyncio
async def test_im_acceptance_outbound_evidence_is_scoped_hash_only_and_bounded() -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    outbound_id = uuid4()
    outbound = {
        "status": "delivered",
        "provider_message_id": "raw-provider-message",
        "pending_count": 0,
        "dlq_count": 0,
        "created_at": timestamp,
        "updated_at": timestamp,
        "payload_json": {"secret": "must-not-leak"},
        "target_id": "must-not-leak",
    }
    attempts = [
        {
            "attempt_number": 1,
            "status": "failed",
            "provider_code": "429",
            "retry_after_seconds": 1.25,
            "started_at": timestamp,
            "completed_at": timestamp,
            "total_count": 2,
        },
        {
            "attempt_number": 2,
            "status": "delivered",
            "provider_code": "sk-live-ABC123",
            "retry_after_seconds": None,
            "started_at": timestamp,
            "completed_at": timestamp,
            "total_count": 2,
        },
    ]
    connection = Connection(fetchrows=[outbound], fetches=[attempts], fetchvals=[1])

    result = await control(connection).im_acceptance_outbound_evidence(
        "tenant-a",
        "binding-a",
        run_id="im-run-123",
        outbound_id=outbound_id,
    )

    assert result is not None
    assert result["schema_version"] == 1
    assert result["run_correlation"] == {
        "availability": "unavailable",
        "reason": "run_id_not_persisted_on_outbound_records",
    }
    assert result["artifact"] == {
        "availability": "unavailable",
        "reason": "artifact_not_correlated_to_run_or_binding",
    }
    evidence = result["outbound"]
    assert evidence["availability"] == "available"
    assert evidence["delivery_status"] == "delivered"
    assert evidence["attempt_count"] == 2
    assert evidence["attempts_truncated"] is False
    assert evidence["attempts"][0]["provider_code"] == "429"
    assert evidence["attempts"][0]["retry_after_seconds"] == 1.25
    assert evidence["attempts"][1]["provider_code"] is None
    assert evidence["pending_count"] == 0
    assert evidence["dlq_count"] == 0
    assert evidence["provider_message_id_sha256"] is not None
    assert len(evidence["provider_message_id_sha256"]) == 64
    rendered = repr(result)
    assert "im-run-123" not in rendered
    assert str(outbound_id) not in rendered
    assert "raw-provider-message" not in rendered
    assert "must-not-leak" not in rendered

    binding_query = next(call for call in connection.calls if call[0] == "fetchval")
    assert "tenant_id=$1 AND binding_id=$2" in binding_query[1][0]
    assert binding_query[1][1:] == ("tenant-a", "binding-a")
    outbound_query = next(call for call in connection.calls if call[0] == "fetchrow")
    assert "message.tenant_id=$1 AND message.binding_id=$2" in outbound_query[1][0]
    assert "payload_json" not in outbound_query[1][0]
    assert "target_id" not in outbound_query[1][0]
    assert "trace_headers" not in outbound_query[1][0]
    assert "source_type" not in outbound_query[1][0]
    assert outbound_query[1][1:] == ("tenant-a", "binding-a", outbound_id)
    attempt_query = next(call for call in connection.calls if call[0] == "fetch")
    assert "count(*) OVER ()" in attempt_query[1][0]
    assert "ORDER BY attempt_number DESC" in attempt_query[1][0]
    assert "LIMIT 100" in attempt_query[1][0]
    assert attempt_query[1][0].rfind("ORDER BY attempt_number") > attempt_query[1][0].find(
        "LIMIT 100"
    )


@pytest.mark.parametrize(
    "provider_code",
    ["0", "200", "429", "45009", "45011", "99991400", "99991401", "99991402", "99991672"],
)
def test_im_acceptance_provider_code_allowlist(provider_code: str) -> None:
    assert _safe_provider_code(provider_code) == provider_code


@pytest.mark.parametrize("provider_code", [None, "201", "transport_error", "sk-live-ABC123"])
def test_im_acceptance_provider_code_rejects_unapproved_values(provider_code: object) -> None:
    assert _safe_provider_code(provider_code) is None


@pytest.mark.asyncio
async def test_im_acceptance_outbound_evidence_keeps_latest_attempts_in_order() -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    outbound_id = uuid4()
    outbound = {
        "status": "delivered",
        "provider_message_id": None,
        "pending_count": 0,
        "dlq_count": 0,
        "created_at": timestamp,
        "updated_at": timestamp,
    }
    attempts = [
        {
            "attempt_number": attempt_number,
            "status": "delivered" if attempt_number == 101 else "failed",
            "provider_code": "0" if attempt_number == 101 else "429",
            "retry_after_seconds": None,
            "started_at": timestamp,
            "completed_at": timestamp,
            "total_count": 101,
        }
        for attempt_number in range(2, 102)
    ]
    connection = Connection(fetchrows=[outbound], fetches=[attempts], fetchvals=[1])

    result = await control(connection).im_acceptance_outbound_evidence(
        "tenant-a",
        "binding-a",
        run_id="im-run-123",
        outbound_id=outbound_id,
    )

    assert result is not None
    evidence = result["outbound"]
    assert evidence["attempt_count"] == 101
    assert evidence["attempts_truncated"] is True
    assert len(evidence["attempts"]) == 100
    assert [attempt["attempt_number"] for attempt in evidence["attempts"]] == list(range(2, 102))
    assert evidence["attempts"][-1]["status"] == "delivered"
    assert evidence["attempts"][-1]["provider_code"] == "0"


@pytest.mark.asyncio
async def test_im_acceptance_outbound_evidence_handles_missing_binding_or_outbound() -> None:
    missing_binding = Connection(fetchvals=[None])
    assert (
        await control(missing_binding).im_acceptance_outbound_evidence(
            "tenant-a",
            "missing",
            run_id="im-run-123",
            outbound_id=uuid4(),
        )
        is None
    )
    assert not any(call[0] in {"fetchrow", "fetch"} for call in missing_binding.calls)

    missing_outbound = Connection(fetchrows=[None], fetchvals=[1])
    result = await control(missing_outbound).im_acceptance_outbound_evidence(
        "tenant-a",
        "binding-a",
        run_id="im-run-123",
        outbound_id=uuid4(),
    )
    assert result is not None
    assert result["outbound"] == {"availability": "not_found"}
    assert not any(call[0] == "fetch" for call in missing_outbound.calls)


@pytest.mark.asyncio
async def test_im_acceptance_event_evidence_correlates_delivery_and_artifact_content_free() -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    inbound_id = str(uuid4())
    outbound_id = uuid4()
    provider_id = "raw-provider-media-id"
    media_envelope = envelope().model_copy(
        update={"media": (MediaReference(provider_media_id=provider_id),)}
    )
    artifact_id = hashlib.sha256(f"{inbound_id}:0:{provider_id}".encode()).hexdigest()
    persisted_run_and_inbound = {
        "run_tenant_id": "tenant-a",
        "run_binding_id": "binding-a",
        "run_channel": Channel.FEISHU.value,
        "run_id_sha256": "b" * 64,
        "run_binding_sha256": "c" * 64,
        "run_created_at": timestamp - timedelta(seconds=1),
        "run_expires_at": timestamp + timedelta(minutes=4),
        "run_provider_event_hash": "a" * 64,
        "inbound_id": inbound_id,
        "external_message_id": "raw-external-message-id",
        "status": "committed",
        "accepted_at": timestamp,
        "delivery_count": 2,
        "envelope_json": media_envelope.model_dump_json(),
    }
    outbounds = [
        {
            "outbound_id": outbound_id,
            "status": "delivered",
            "provider_message_id": "raw-provider-reply-id",
            "pending_count": 0,
            "dlq_count": 0,
            "created_at": timestamp,
            "updated_at": timestamp,
        }
    ]
    attempts = [
        {
            "outbound_id": outbound_id,
            "attempt_number": 1,
            "status": "delivered",
            "provider_code": "0",
            "retry_after_seconds": 0.5,
            "started_at": timestamp,
            "completed_at": timestamp,
        }
    ]
    artifacts = [
        {
            "artifact_id": artifact_id,
            "checksum": "d" * 64,
            "size_bytes": 123,
            "status": "committed",
            "created_at": timestamp,
        }
    ]
    connection = Connection(
        fetchvals=[Channel.FEISHU.value],
        fetchrows=[persisted_run_and_inbound],
        fetches=[outbounds, attempts, artifacts],
    )

    result = await control(connection).im_acceptance_event_evidence(
        "tenant-a",
        "binding-a",
        channel=Channel.FEISHU,
        run_id="im-run-456",
        run_nonce=RUN_NONCE,
        provider_event_hash="a" * 64,
    )

    assert result is not None
    assert result["requested_run_id_sha256"] == "b" * 64
    assert result["run_binding_sha256"] == "c" * 64
    assert result["provider_event_hash"] == "a" * 64
    assert result["correlation"]["availability"] == "available"
    assert result["correlation"]["delivery_count"] == 2
    assert result["outbounds"]["count"] == 1
    assert result["outbounds"]["items"][0]["delivery_status"] == "delivered"
    assert result["outbounds"]["items"][0]["attempts"][0]["retry_after_seconds"] == 0.5
    assert result["artifact"] == {
        "availability": "available",
        "count": 1,
        "items": [
            {
                "sha256": "d" * 64,
                "bytes": 123,
                "status": "available",
                "created_at": timestamp.isoformat(),
            }
        ],
    }
    artifact_query = next(
        call for call in connection.calls if call[0] == "fetch" and "FROM artifacts" in call[1][0]
    )
    assert "status='committed'" in artifact_query[1][0]
    rendered = repr(result)
    assert "im-run-456" not in rendered
    assert inbound_id not in rendered
    assert str(outbound_id) not in rendered
    assert "raw-external-message-id" not in rendered
    assert "raw-provider-media-id" not in rendered
    assert "raw-provider-reply-id" not in rendered
    binding_query = next(call for call in connection.calls if call[0] == "fetchval")
    correlation_query = next(call for call in connection.calls if call[0] == "fetchrow")
    assert "tenant_id=$1 AND binding_id=$2" in binding_query[1][0]
    assert "expires_at > clock_timestamp()" in correlation_query[1][0]
    assert "inbound.accepted_at >= acceptance.created_at" in correlation_query[1][0]
    assert "provider_event_hash IS NULL" in correlation_query[1][0]


@pytest.mark.asyncio
async def test_im_acceptance_event_evidence_rejects_binding_expiry_rebind_and_reuse() -> None:
    wrong_channel = Connection(fetchvals=[Channel.WECOM_AI_BOT.value])
    assert (
        await control(wrong_channel).im_acceptance_event_evidence(
            "tenant-a",
            "binding-a",
            channel=Channel.FEISHU,
            run_id="im-run-456",
            run_nonce=RUN_NONCE,
            provider_event_hash="a" * 64,
        )
        is None
    )
    assert not any(call[0] == "fetchrow" for call in wrong_channel.calls)

    expired = Connection(fetchvals=[Channel.FEISHU.value], fetchrows=[None])
    assert (
        await control(expired).im_acceptance_event_evidence(
            "tenant-a",
            "binding-a",
            channel=Channel.FEISHU,
            run_id="im-run-expired",
            run_nonce=RUN_NONCE,
            provider_event_hash="b" * 64,
        )
        is None
    )
    expired_query = next(call for call in expired.calls if call[0] == "fetchrow")
    assert "expires_at > clock_timestamp()" in expired_query[1][0]

    rebound = Connection(fetchvals=[Channel.FEISHU.value], fetchrows=[None])
    assert (
        await control(rebound).im_acceptance_event_evidence(
            "tenant-a",
            "binding-a",
            channel=Channel.FEISHU,
            run_id="im-run-rebind",
            run_nonce=RUN_NONCE,
            provider_event_hash="c" * 64,
        )
        is None
    )
    rebound_query = next(call for call in rebound.calls if call[0] == "fetchrow")
    assert "acceptance.provider_event_hash=$7" in rebound_query[1][0]
    assert "used.provider_event_hash=$7" in rebound_query[1][0]


@pytest.mark.asyncio
async def test_im_acceptance_event_evidence_stale_event_is_not_bound_or_echoed() -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    stale = Connection(
        fetchvals=[Channel.FEISHU.value],
        fetchrows=[
            {
                "run_tenant_id": "tenant-a",
                "run_binding_id": "binding-a",
                "run_channel": Channel.FEISHU.value,
                "run_id_sha256": "d" * 64,
                "run_binding_sha256": "e" * 64,
                "run_created_at": timestamp,
                "run_expires_at": timestamp + timedelta(minutes=5),
                "run_provider_event_hash": None,
                "inbound_id": None,
                "external_message_id": None,
                "status": None,
                "accepted_at": None,
                "delivery_count": None,
                "envelope_json": None,
            }
        ],
    )
    result = await control(stale).im_acceptance_event_evidence(
        "tenant-a",
        "binding-a",
        channel=Channel.FEISHU,
        run_id="im-run-stale",
        run_nonce=RUN_NONCE,
        provider_event_hash="b" * 64,
    )
    assert result is not None
    assert result["requested_run_id_sha256"] == "d" * 64
    assert result["provider_event_hash"] is None
    assert result["correlation"] == {"availability": "not_found"}
    assert result["outbounds"] == {"count": 0, "truncated": False, "items": []}
    assert result["artifact"] == {"availability": "not_found", "count": 0, "items": []}
    assert not any(call[0] == "fetch" for call in stale.calls)
    rendered = repr(result)
    assert "im-run-stale" not in rendered
    assert "b" * 64 not in rendered


@pytest.mark.asyncio
async def test_register_im_acceptance_run_persists_hash_and_server_lifetime() -> None:
    created_at = datetime(2026, 1, 1, tzinfo=UTC)
    expires_at = created_at + timedelta(minutes=5)
    connection = Connection(
        fetchrows=[
            {
                "tenant_id": "tenant-a",
                "binding_id": "binding-a",
                "channel": Channel.FEISHU.value,
                "run_id_sha256": "e" * 64,
                "run_binding_sha256": "f" * 64,
                "created_at": created_at,
                "expires_at": expires_at,
            }
        ]
    )

    result = await control(connection).register_im_acceptance_run(
        "tenant-a",
        "binding-a",
        channel=Channel.FEISHU,
        run_id="raw-run-id",
        run_nonce=RUN_NONCE,
        expires_in_seconds=300,
    )

    assert result == {
        "schema_version": 1,
        "tenant_id": "tenant-a",
        "binding_id": "binding-a",
        "channel": Channel.FEISHU.value,
        "run_id_sha256": "e" * 64,
        "run_binding_sha256": "f" * 64,
        "created_at": created_at.isoformat(),
        "expires_at": expires_at.isoformat(),
    }
    query = next(call for call in connection.calls if call[0] == "fetchrow")
    cleanup = next(
        call
        for call in connection.calls
        if call[0] == "execute" and "DELETE FROM im_acceptance_runs" in call[1][0]
    )
    assert "clock_timestamp()" in query[1][0]
    assert "ON CONFLICT DO NOTHING" in query[1][0]
    assert "raw-run-id" not in repr(query)
    assert RUN_NONCE not in repr(query)
    assert cleanup[1][1:] == ("tenant-a", "binding-a")


@pytest.mark.asyncio
async def test_replay_outbound_guards_and_success() -> None:
    outbound_id = str(uuid4())
    base = [None]
    with pytest.raises(LookupError, match="outbound"):
        await control(Connection(fetchrows=[*base, None], fetchvals=[1])).replay_outbound(
            outbound_id=outbound_id,
            confirm_ambiguous=False,
            expected_version=1,
            **kwargs(),
        )
    ambiguous_row = {
        "status": "ambiguous",
        "payload_json": "{}",
        "trace_headers": "{}",
        "channel": "feishu",
    }
    with pytest.raises(ValueError, match="confirmation"):
        await control(Connection(fetchrows=[*base, ambiguous_row], fetchvals=[1])).replay_outbound(
            outbound_id=outbound_id,
            confirm_ambiguous=False,
            expected_version=1,
            **kwargs(),
        )
    success = Connection(fetchrows=[*base, ambiguous_row], fetchvals=[1, 2])
    result = await control(success).replay_outbound(
        outbound_id=outbound_id,
        confirm_ambiguous=True,
        expected_version=1,
        **kwargs(),
    )
    assert result["status"] == "pending" and result["tenant_control_version"] == 2

    cached = Connection(
        fetchrows=[{"request_hash": "request-hash", "response_json": '{"status":"cached"}'}]
    )
    assert (
        await control(cached).replay_outbound(
            outbound_id=outbound_id,
            confirm_ambiguous=False,
            expected_version=1,
            **kwargs(),
        )
    )["status"] == "cached"


@pytest.mark.asyncio
async def test_cached_idempotency_rejects_reuse_and_invalid_storage() -> None:
    mismatch = Connection(fetchrows=[{"request_hash": "other", "response_json": "{}"}])
    with pytest.raises(IdempotencyConflict, match="another"):
        await PostgresControlPlaneRepository._cached(mismatch, "tenant", "key", "request-hash")
    invalid = Connection(fetchrows=[{"request_hash": "request-hash", "response_json": 1}])
    with pytest.raises(RuntimeError, match="invalid"):
        await PostgresControlPlaneRepository._cached(invalid, "tenant", "key", "request-hash")
