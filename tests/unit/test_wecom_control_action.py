from __future__ import annotations

import hashlib
import json
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from deploy.im_probe import wecom_control_action as action
from deploy.im_probe import wecom_provider_driver as driver

TENANT = "tenant-probe"
BINDING = "wecom-probe"
ACCOUNT_HASH = "a" * 64
PROFILE_HASH = "b" * 64
IMAGE = "sha256:" + "c" * 64
RELEASE_ID = "release-wecom-live"
RELEASE_NONCE_SHA256 = "d" * 64
SOURCE_FINGERPRINT = "e" * 64
RUN_ID = "wecom-live-run-1"
RUN_NONCE = "wecom_live_nonce_123456"


def _now(offset: int = 0) -> str:
    return (datetime.now(UTC) + timedelta(seconds=offset)).isoformat().replace("+00:00", "Z")


def _opaque(label: str) -> str:
    return "wcctl_" + hashlib.sha256(label.encode()).hexdigest()


def _request(case: str) -> action.ActionRequest:
    return action._parse_request(
        {
            "schema_version": 1,
            "channel": "wecom",
            "action": f"wecom_{case}",
            "run_id": RUN_ID,
            "run_nonce": RUN_NONCE,
            "control_profile_sha256": PROFILE_HASH,
            "payload": {
                "case": case,
                "expected_image_digest": IMAGE,
                "tenant_id": TENANT,
                "binding_id": BINDING,
                "account_id_sha256": ACCOUNT_HASH,
            },
        }
    )


def _secure(path: Path, value: str) -> Path:
    path.write_text(value, encoding="utf-8")
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    return path


def _config(tmp_path: Path) -> action.ActionConfig:
    return action.ActionConfig(
        tenant_id=TENANT,
        binding_id=BINDING,
        account_id_sha256=ACCOUNT_HASH,
        admin_base_url="https://admin.example.test",
        admin_token_file=_secure(tmp_path / "admin.token", "admin-secret-token"),
        control_base_url="https://control.example.test",
        control_token_file=_secure(tmp_path / "control.token", "control-secret-token"),
        timeout_seconds=5.0,
    )


def _observation(case: str) -> dict[str, Any]:
    common = {"provider_event_id": _opaque(f"{case}-provider"), "observed_at": _now()}
    values: dict[str, dict[str, Any]] = {
        "round_trip": {
            "callback_event_id": _opaque("round-callback"),
            "outbound_request_id": _opaque("round-outbound"),
            "provider_code": "0",
        },
        "idempotency": {
            "duplicate_event_id": _opaque("duplicate"),
            "unique_inbound_id": _opaque("unique-inbound"),
            "duplicate_count": 1,
            "duplicate_source": "service_replay_of_provider_event",
            "original_event_id": _opaque("processing-original"),
            "replayed_event_id": _opaque("processing-replay"),
        },
        "media": {
            "media_id_hash": "d" * 64,
            "sha256": "e" * 64,
            "bytes": 1024,
        },
        "reconnect": {
            "disconnect_event_id": _opaque("reconnect-disconnect"),
            "reconnect_event_id": _opaque("reconnect-connect"),
            "received_after_reconnect_event_id": _opaque("reconnect-received"),
            "lock_takeover_event_id": _opaque("reconnect-takeover"),
            "old_lock_owner_released": True,
            "new_lock_owner_acquired": True,
            "lock_epoch": 2,
            "outbound_request_id": _opaque("reconnect-outbound"),
            "acknowledged_request_id": _opaque("reconnect-outbound"),
            "provider_code": "0",
        },
        "rate_limit_retry_after": {
            "provider_error_code": "429",
            "retry_after_seconds": 1.0,
            "retry_request_id": _opaque("retry-request"),
            "retry_attempts": 2,
            "retry_elapsed_seconds": 1.1,
        },
        "credential_rotation": {
            "old_credential_event_id": _opaque("old-credential"),
            "new_credential_event_id": _opaque("new-credential"),
            "post_rotation_event_id": _opaque("post-rotation"),
            "old_credential_rejected": True,
            "outbound_request_id": _opaque("rotation-outbound"),
            "acknowledged_request_id": _opaque("rotation-outbound"),
            "provider_code": "0",
        },
        "prolonged_outage": {
            "outage_event_id": _opaque("outage"),
            "recovery_event_id": _opaque("recovery"),
            "outage_seconds": 60.0,
            "outage_mode": "service_failover",
            "failed_instance_id": _opaque("failed-instance"),
            "takeover_instance_id": _opaque("takeover-instance"),
            "old_lock_owner_released": True,
            "new_lock_owner_acquired": True,
            "connection_epoch": 2,
            "event_during_outage_id": _opaque("outage-inbound"),
            "reply_for_event_id": _opaque("outage-inbound"),
            "outbound_request_id": _opaque("outage-outbound"),
            "acknowledged_request_id": _opaque("outage-outbound"),
            "reply_count": 1,
            "ack_count": 1,
            "pending_count": 0,
            "dlq_count": 0,
        },
        "ambiguous": {
            "ambiguous_event_id": _opaque("ambiguous"),
            "manual_review_id": _opaque("manual-review"),
            "drop_response_observed": True,
            "auto_replay_count": 0,
        },
    }
    return {**common, **values[case]}


def _hook(case: str, request: action.ActionRequest, ack_hash: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": "pass",
        "channel": "wecom",
        "case": case,
        "run_binding_sha256": action._run_binding_hash(request),
        "expected_image_digest": IMAGE,
        "tenant_id": TENANT,
        "binding_id": BINDING,
        "account_id_sha256": ACCOUNT_HASH,
        "ack_provider_event_hash": ack_hash,
        "observation": _observation(case),
        "provider_witness": {
            "status": "pass",
            "source": "wecom_provider_control",
            "provider_event_hash": ack_hash,
            "observed_at": _now(),
        },
    }


def _ack_hash(case: str, request: action.ActionRequest) -> str:
    del request
    return hashlib.sha256(f"{case}-database-provider-event".encode()).hexdigest()


def _event(
    event_type: str,
    epoch: int,
    *,
    ack_hash: str | None = None,
    offset: int = 0,
) -> dict[str, Any]:
    return {
        "event_id": str(UUID(int=epoch * 10 + offset + 1)),
        "connection_epoch": epoch,
        "event_type": event_type,
        "owner_hash": "f" * 64,
        "provider_event_hash": ack_hash,
        "occurred_at": _now(offset),
    }


def _snapshot(case: str, ack_hash: str) -> dict[str, Any]:
    events = [_event("provider_event", 2, ack_hash=ack_hash, offset=5)]
    if case in {"reconnect", "prolonged_outage"}:
        events.extend(
            [
                _event("disconnected", 1, offset=1),
                _event("released", 1, offset=2),
                _event("acquired", 2, offset=3),
                _event("authenticated", 2, offset=4),
            ]
        )
    return {
        "state": {
            "owner_hash": "f" * 64,
            "epoch": 2,
            "phase": "authenticated",
            "acquired_at": _now(-5),
            "authenticated_at": _now(-4),
            "disconnected_at": None,
            "released_at": None,
            "last_provider_event_hash": ack_hash,
            "last_provider_event_at": _now(),
            "updated_at": _now(),
        },
        "events": events,
    }


def _event_evidence(
    case: str, request: action.ActionRequest, provider_event_hash: str
) -> dict[str, Any]:
    observation = _observation(case)
    codes_by_case: dict[str, list[str | None]] = {
        "round_trip": ["0"],
        "idempotency": ["0"],
        "reconnect": ["0"],
        "rate_limit_retry_after": ["429", "0"],
        "credential_rotation": ["0"],
        "prolonged_outage": ["0"],
        "ambiguous": [None],
    }
    outbound_cases = set(codes_by_case)
    codes = codes_by_case.get(case, [])
    attempts = []
    for index, code in enumerate(codes, start=1):
        attempts.append(
            {
                "attempt_number": index,
                "status": "delivered" if code == "0" else "failed",
                "provider_code": code,
                "retry_after_seconds": (
                    observation["retry_after_seconds"]
                    if case == "rate_limit_retry_after" and code == "429"
                    else None
                ),
                "started_at": _now(index),
                "completed_at": _now(index),
            }
        )
    outbound_items = (
        [
            {
                "outbound_id_sha256": "1" * 64,
                "delivery_status": "ambiguous" if case == "ambiguous" else "delivered",
                "provider_message_id_sha256": "2" * 64,
                "attempt_count": len(attempts),
                "attempts": attempts,
                "pending_count": 0,
                "dlq_count": 0,
                "created_at": _now(-2),
                "updated_at": _now(),
            }
        ]
        if case in outbound_cases
        else []
    )
    run_hash = action._acceptance_hash("run", request, action._acceptance_run_id(request))
    result: dict[str, Any] = {
        "schema_version": 1,
        "tenant_id": TENANT,
        "binding_id": BINDING,
        "channel": "wecom_ai_bot",
        "requested_run_id_sha256": run_hash,
        "run_binding_sha256": action._acceptance_run_binding_hash(request),
        "provider_event_hash": provider_event_hash,
        "correlation": {
            "availability": "available",
            "inbound_id_sha256": "3" * 64,
            "status": "committed",
            "delivery_count": (observation["duplicate_count"] + 1 if case == "idempotency" else 1),
            "accepted_at": _now(-3),
        },
        "outbounds": {
            "count": len(outbound_items),
            "truncated": False,
            "items": outbound_items,
        },
        "artifact": {"availability": "not_found", "count": 0, "items": []},
    }
    if case == "media":
        result["artifact"] = {
            "availability": "available",
            "count": 1,
            "items": [
                {
                    "sha256": observation["sha256"],
                    "bytes": observation["bytes"],
                    "status": "available",
                    "created_at": _now(),
                }
            ],
        }
    return result


def _run_registration(request: action.ActionRequest) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "tenant_id": TENANT,
        "binding_id": BINDING,
        "channel": "wecom_ai_bot",
        "run_id_sha256": action._acceptance_hash(
            "run", request, action._acceptance_run_id(request)
        ),
        "run_binding_sha256": action._acceptance_run_binding_hash(request),
        "created_at": _now(),
        "expires_at": _now(300),
    }


@pytest.mark.parametrize("case", action.CASES)
def test_all_cases_require_matching_control_admin_and_provider_evidence(
    case: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(case)
    ack_hash = _ack_hash(case, request)
    responses = [
        _run_registration(request),
        _hook(case, request, ack_hash),
        _snapshot(case, ack_hash),
        _event_evidence(case, request, ack_hash),
    ]
    calls: list[tuple[str, str, object | None]] = []

    def fake_request(method: str, url: str, **_kwargs: Any) -> object:
        calls.append((method, url, _kwargs.get("body")))
        return responses.pop(0)

    monkeypatch.setattr(action, "_request_json", fake_request)
    result = action._execute(request, _config(tmp_path))

    assert result["observation"]["status"] == "pass"
    assert result["observation"]["run_nonce"] == RUN_NONCE
    assert result["wecom_snapshot"]["case"] == case
    assert calls[0] == (
        "POST",
        (
            "https://admin.example.test/v1/tenants/tenant-probe/bindings/"
            "wecom-probe/im-acceptance/runs"
        ),
        {
            "channel": "wecom_ai_bot",
            "run_id": action._acceptance_run_id(request),
            "run_nonce": RUN_NONCE,
            "expires_in_seconds": 300,
        },
    )
    assert calls[1] == (
        "POST",
        f"https://control.example.test/v1/wecom/control/actions/{case}",
        action._control_request(request),
    )
    assert calls[2][0] == "GET"
    assert calls[3] == (
        "POST",
        (
            "https://admin.example.test/v1/tenants/tenant-probe/bindings/"
            "wecom-probe/im-acceptance/event-evidence"
        ),
        {
            "channel": "wecom_ai_bot",
            "run_id": action._acceptance_run_id(request),
            "run_nonce": RUN_NONCE,
            "provider_event_hash": ack_hash,
        },
    )
    assert len(calls) == 4

    rendered = json.dumps(result)
    assert "admin-secret-token" not in rendered
    assert "control-secret-token" not in rendered
    assert "raw-provider" not in rendered
    id_values = [
        value
        for key, value in result["observation"].items()
        if isinstance(value, str) and (key.endswith("_id") or key.endswith("_request_id"))
    ]
    assert all(action.OPAQUE_ID_RE.fullmatch(value) for value in id_values)

    profile = driver.ControlProfile(TENANT, BINDING, ACCOUNT_HASH, {case: f"wecom_{case}"})
    driver_request = driver.DriverRequest(
        channel="wecom",
        run_id=RUN_ID,
        run_nonce=RUN_NONCE,
        expected_image_digest=IMAGE,
        release_id=RELEASE_ID,
        release_nonce_sha256=RELEASE_NONCE_SHA256,
        source_fingerprint=SOURCE_FINGERPRINT,
        control_profile_sha256=PROFILE_HASH,
    )
    driver._validate_broker_response(
        case,
        {"status": "pass", "result": result},
        driver_request,
        profile,
    )


def test_event_correlation_and_artifact_gaps_fail_closed() -> None:
    request = _request("round_trip")
    ack_hash = "4" * 64
    current = _event_evidence("round_trip", request, ack_hash)
    current["correlation"] = {"availability": "not_found"}
    with pytest.raises(action.ActionNotRun, match="admin_event_correlation_unavailable"):
        action._validate_event_evidence(
            current,
            request,
            ack_hash,
            _observation("round_trip"),
        )

    request = _request("media")
    current = _event_evidence("media", request, ack_hash)
    current["artifact"] = {
        "availability": "not_found",
        "count": 0,
        "items": [],
    }
    with pytest.raises(action.ActionNotRun, match="admin_artifact_evidence_unavailable"):
        action._validate_event_evidence(current, request, ack_hash, _observation("media"))


def test_event_evidence_rejects_truncation_retry_mismatch_and_wrong_hash() -> None:
    request = _request("rate_limit_retry_after")
    ack_hash = "5" * 64
    value = _event_evidence("rate_limit_retry_after", request, ack_hash)
    value["outbounds"]["truncated"] = True
    with pytest.raises(action.ActionNotRun, match="admin_event_evidence_truncated"):
        action._validate_event_evidence(value, request, ack_hash, _observation(request.case))

    value = _event_evidence("rate_limit_retry_after", request, ack_hash)
    value["outbounds"]["items"][0]["attempts"][0]["retry_after_seconds"] = 2.0
    with pytest.raises(action.ActionNotRun, match="admin_retry_evidence_mismatch"):
        action._validate_event_evidence(value, request, ack_hash, _observation(request.case))

    value = _event_evidence("rate_limit_retry_after", request, ack_hash)
    value["provider_event_hash"] = "6" * 64
    with pytest.raises(action.ActionNotRun, match="admin_event_evidence_mismatch"):
        action._validate_event_evidence(value, request, ack_hash, _observation(request.case))

    value = _event_evidence("rate_limit_retry_after", request, ack_hash)
    value["run_binding_sha256"] = "0" * 64
    with pytest.raises(action.ActionNotRun, match="admin_event_evidence_mismatch"):
        action._validate_event_evidence(value, request, ack_hash, _observation(request.case))


def test_run_registration_must_match_nonce_bound_run() -> None:
    request = _request("round_trip")
    value = _run_registration(request)
    action._validate_run_registration(value, request)

    value["run_binding_sha256"] = "0" * 64
    with pytest.raises(action.ActionNotRun, match="admin_run_registration_mismatch"):
        action._validate_run_registration(value, request)


def test_each_case_uses_an_independent_acceptance_run() -> None:
    run_ids = {action._acceptance_run_id(_request(case)) for case in action.CASE_OBSERVATION_FIELDS}

    assert len(run_ids) == len(action.CASE_OBSERVATION_FIELDS)


def test_lifecycle_rejects_provider_events_from_before_the_registered_run() -> None:
    request = _request("round_trip")
    ack_hash = "8" * 64
    snapshot = _snapshot("round_trip", ack_hash)
    snapshot["events"][0]["occurred_at"] = _now(-30)

    with pytest.raises(action.ActionNotRun, match="admin_provider_event_unavailable"):
        action._lifecycle(
            snapshot,
            request,
            ack_hash,
            _observation("round_trip"),
            datetime.now(UTC),
        )


@pytest.mark.parametrize("case", ["reconnect", "credential_rotation"])
def test_reconnect_and_rotation_require_delivered_ack(case: str) -> None:
    request = _request(case)
    ack_hash = "7" * 64
    value = _event_evidence(case, request, ack_hash)
    value["outbounds"] = {"count": 0, "truncated": False, "items": []}
    with pytest.raises(action.ActionNotRun, match="admin_ack_evidence_mismatch"):
        action._validate_event_evidence(value, request, ack_hash, _observation(case))

    value = _event_evidence(case, request, ack_hash)
    value["outbounds"]["items"][0]["attempts"][0]["provider_code"] = "429"
    with pytest.raises(action.ActionNotRun, match="admin_ack_evidence_mismatch"):
        action._validate_event_evidence(value, request, ack_hash, _observation(case))


def test_missing_provider_hash_or_raw_control_identifier_cannot_pass(tmp_path: Path) -> None:
    request = _request("idempotency")
    ack_hash = "2" * 64
    hook = _hook("idempotency", request, ack_hash)
    hook["observation"]["provider_event_id"] = "raw-provider-event-id"
    with pytest.raises(action.ActionNotRun, match="raw_identifier"):
        action._validate_hook(hook, request)

    with pytest.raises(action.ActionNotRun, match="admin_provider_event_unavailable"):
        action._lifecycle(
            _snapshot("idempotency", "3" * 64),
            request,
            ack_hash,
            _observation("idempotency"),
            datetime.now(UTC),
        )


def test_hook_ack_hash_is_bound_by_provider_witness_and_admin_not_public_id() -> None:
    request = _request("round_trip")
    hook = _hook("round_trip", request, "2" * 64)

    observation, provider_hash = action._validate_hook(hook, request)

    assert observation["provider_event_id"].startswith("wcctl_")
    assert provider_hash == "2" * 64


def test_config_is_strict_https_and_uses_private_token_files(tmp_path: Path) -> None:
    admin_token = _secure(tmp_path / "admin.token", "admin-token")
    control_token = _secure(tmp_path / "control.token", "control-token")
    value = {
        "schema_version": 1,
        "channel": "wecom",
        "tenant_id": TENANT,
        "binding_id": BINDING,
        "account_id_sha256": ACCOUNT_HASH,
        "admin_base_url": "https://admin.example.test",
        "admin_token_file": str(admin_token),
        "control_base_url": "https://control.example.test",
        "control_token_file": str(control_token),
        "timeout_seconds": 5.0,
    }
    config_path = _secure(tmp_path / "controller.json", json.dumps(value))
    assert action._load_config(config_path).binding_id == BINDING

    value["admin_base_url"] = "http://admin.example.test"
    config_path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(action.ActionNotRun, match="configuration_unavailable"):
        action._load_config(config_path)

    value["admin_base_url"] = "https://admin.example.test"
    value["extra"] = True
    config_path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(action.ActionNotRun, match="configuration_unavailable"):
        action._load_config(config_path)


def test_main_not_run_reason_is_machine_readable_and_content_free(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsysbinary: pytest.CaptureFixture[bytes],
) -> None:
    config_path = _secure(tmp_path / "controller.json", "{}")
    request = _request("round_trip")
    monkeypatch.setenv(action.ACTION_ENV, request.action)
    monkeypatch.setattr(action, "_read_request", lambda: request)

    def unavailable(_request: action.ActionRequest, _config: action.ActionConfig) -> dict[str, Any]:
        raise action.ActionNotRun("control_hook_unavailable")

    monkeypatch.setattr(action, "_load_config", lambda _path: _config(tmp_path))
    monkeypatch.setattr(action, "_execute", unavailable)
    assert action.main(["--config", str(config_path)]) == 1
    captured = capsysbinary.readouterr()
    assert captured.err == b""
    assert json.loads(captured.out) == {
        "reason": "control_hook_unavailable",
        "schema_version": 1,
        "status": "not_run",
    }
    assert b"token" not in captured.out
    assert b"provider" not in captured.out


def test_main_invalid_arguments_are_content_free(
    capsysbinary: pytest.CaptureFixture[bytes],
) -> None:
    assert action.main([]) == 1
    captured = capsysbinary.readouterr()
    assert captured.err == b""
    assert json.loads(captured.out) == {
        "reason": "invalid_arguments",
        "schema_version": 1,
        "status": "not_run",
    }


def test_request_rejects_extra_fields_wrong_action_and_bad_nonce() -> None:
    base = {
        "schema_version": 1,
        "channel": "wecom",
        "action": "wecom_round_trip",
        "run_id": RUN_ID,
        "run_nonce": RUN_NONCE,
        "control_profile_sha256": PROFILE_HASH,
        "payload": {
            "case": "round_trip",
            "expected_image_digest": IMAGE,
            "tenant_id": TENANT,
            "binding_id": BINDING,
            "account_id_sha256": ACCOUNT_HASH,
        },
    }
    value = {**base, "extra": True}
    with pytest.raises(action.ActionNotRun, match="invalid_request"):
        action._parse_request(value)

    value = {**base, "action": "wecom_media"}
    with pytest.raises(action.ActionNotRun, match="invalid_request"):
        action._parse_request(value)

    value = {**base, "run_nonce": "short"}
    with pytest.raises(action.ActionNotRun, match="invalid_request"):
        action._parse_request(value)


def test_controller_source_has_no_provider_sdk_or_secret_literal() -> None:
    source = Path(action.__file__).read_text(encoding="utf-8").lower()
    assert "wecom_aibot" not in source
    assert "bot_secret" not in source
    assert "raw_provider" not in source
    assert "synthetic" not in source
    assert "subprocess" not in source
    assert "ack_outbound_id" not in source
    assert "run_correlation" not in source
    assert "/im-acceptance/evidence?" not in source
    assert "/im-acceptance/event-evidence" in source
    assert "opened.st_uid != 0" in source
    assert "_validate_root_owned_parent_chain(path)" in source
