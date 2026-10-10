from __future__ import annotations

import hashlib
import json
import stat
import struct
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from deploy.im_probe import provider_runner
from deploy.im_probe import wecom_provider_driver as driver

RUN_NONCE = "offline_nonce_123456"
IMAGE_DIGEST = "sha256:" + "1" * 64
ACCOUNT_ID = "offline-wecom-bot"
RELEASE_ID = "release-offline-wecom"
RELEASE_NONCE_SHA256 = "c" * 64
SOURCE_FINGERPRINT = "d" * 64


def _provider_hash(tenant_id: str, binding_id: str, value: str) -> str:
    return hashlib.sha256(
        "\0".join(
            ("trpc-wecom-evidence-v1", "provider-event", tenant_id, binding_id, value)
        ).encode()
    ).hexdigest()


def _profile_value() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "channel": "wecom",
        "tenant_id": "tenant-one",
        "binding_id": "binding-one",
        "account_id_sha256": hashlib.sha256(ACCOUNT_ID.encode()).hexdigest(),
        "actions": {case: f"wecom-{case}" for case in driver.REQUIRED_CASES},
    }


def _write_profile(tmp_path: Path, value: dict[str, Any] | None = None) -> tuple[Path, str]:
    path = tmp_path / "wecom-control-profile.json"
    raw = json.dumps(value or _profile_value(), sort_keys=True).encode()
    path.write_bytes(raw)
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    return path, hashlib.sha256(raw).hexdigest()


def _request(profile_hash: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "channel": "wecom",
        "run_id": "offline-wecom-run",
        "run_nonce": RUN_NONCE,
        "expected_image_digest": IMAGE_DIGEST,
        "release_id": RELEASE_ID,
        "release_nonce_sha256": RELEASE_NONCE_SHA256,
        "source_fingerprint": SOURCE_FINGERPRINT,
        "control_profile_sha256": profile_hash,
        "cases": list(driver.REQUIRED_CASES),
    }


def _observation(case: str, provider_event_id: str) -> dict[str, Any]:
    common: dict[str, Any] = {
        "status": "pass",
        "run_nonce": RUN_NONCE,
        "provider_event_id": provider_event_id,
        "observed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    cases: dict[str, dict[str, Any]] = {
        "round_trip": {
            **common,
            "callback_event_id": "raw-callback",
            "outbound_request_id": "raw-outbound",
            "provider_code": 0,
        },
        "idempotency": {
            **common,
            "duplicate_event_id": "raw-duplicate-action",
            "unique_inbound_id": "raw-unique-inbound",
            "duplicate_count": 1,
            "duplicate_source": "service_replay_of_provider_event",
            "original_event_id": "raw-original-processing",
            "replayed_event_id": "raw-replayed-processing",
        },
        "media": {
            **common,
            "media_id_hash": "a" * 64,
            "sha256": "b" * 64,
            "bytes": 16,
        },
        "reconnect": {
            **common,
            "disconnect_event_id": "raw-disconnect",
            "reconnect_event_id": "raw-reconnect",
            "received_after_reconnect_event_id": "raw-after-reconnect",
            "lock_takeover_event_id": "raw-lock-takeover",
            "old_lock_owner_released": True,
            "new_lock_owner_acquired": True,
            "lock_epoch": 2,
            "outbound_request_id": "raw-reconnect-outbound",
            "acknowledged_request_id": "raw-reconnect-outbound",
            "provider_code": 0,
        },
        "rate_limit_retry_after": {
            **common,
            "provider_error_code": 429,
            "retry_after_seconds": 1.0,
            "retry_request_id": "raw-retry-request",
            "retry_attempts": 2,
            "retry_elapsed_seconds": 1.0,
        },
        "credential_rotation": {
            **common,
            "old_credential_event_id": "raw-old-credential",
            "new_credential_event_id": "raw-new-credential",
            "post_rotation_event_id": "raw-post-rotation",
            "old_credential_rejected": True,
            "outbound_request_id": "raw-rotation-outbound",
            "acknowledged_request_id": "raw-rotation-outbound",
            "provider_code": 0,
        },
        "prolonged_outage": {
            **common,
            "outage_event_id": "raw-outage",
            "recovery_event_id": "raw-recovery",
            "outage_seconds": 60.0,
            "outage_mode": "service_failover",
            "failed_instance_id": "raw-failed-instance",
            "takeover_instance_id": "raw-takeover-instance",
            "old_lock_owner_released": True,
            "new_lock_owner_acquired": True,
            "connection_epoch": 2,
            "event_during_outage_id": "raw-outage-inbound",
            "reply_for_event_id": "raw-outage-inbound",
            "outbound_request_id": "raw-outage-request",
            "acknowledged_request_id": "raw-outage-request",
            "reply_count": 1,
            "ack_count": 1,
            "pending_count": 0,
            "dlq_count": 0,
        },
        "ambiguous": {
            **common,
            "ambiguous_event_id": "raw-ambiguous",
            "manual_review_id": "raw-manual-review",
            "drop_response_observed": True,
            "auto_replay_count": 0,
        },
    }
    return cases[case]


def _snapshot(
    case: str,
    provider_event_id: str,
    *,
    lifecycle: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if lifecycle is None:
        lifecycle = [{"state": "authenticated", "epoch": 1}]
    return {
        "schema_version": 1,
        "channel": "wecom",
        "case": case,
        "tenant_id": "tenant-one",
        "binding_id": "binding-one",
        "account_id_sha256": hashlib.sha256(ACCOUNT_ID.encode()).hexdigest(),
        "provider_event_hash": _provider_hash("tenant-one", "binding-one", provider_event_id),
        "lifecycle": lifecycle,
    }


def _broker_result(case: str) -> dict[str, Any]:
    provider_id = f"raw-provider-{case}"
    lifecycle = None
    if case in {"reconnect", "prolonged_outage"}:
        lifecycle = [
            {"state": "disconnected", "epoch": 1},
            {"state": "released", "epoch": 1},
            {"state": "acquired", "epoch": 2},
            {"state": "authenticated", "epoch": 2},
        ]
    return {
        "status": "pass",
        "result": {
            "observation": _observation(case, provider_id),
            "wecom_snapshot": _snapshot(case, provider_id, lifecycle=lifecycle),
        },
    }


def test_request_and_profile_are_strict_and_account_bound(tmp_path: Path) -> None:
    profile_path, profile_hash = _write_profile(tmp_path)
    request = _request(profile_hash)
    parsed_request = driver._parse_request(request)
    profile = driver._load_profile(profile_path, profile_hash, ACCOUNT_ID)
    assert parsed_request.channel == "wecom"
    assert profile.actions["round_trip"] == "wecom-round_trip"

    request["extra"] = True
    with pytest.raises(driver.DriverError, match="request schema"):
        driver._parse_request(request)

    bad_profile = _profile_value()
    bad_profile["account_id_sha256"] = "f" * 64
    path, digest = _write_profile(tmp_path, bad_profile)
    with pytest.raises(driver.DriverError, match="account"):
        driver._load_profile(path, digest, ACCOUNT_ID)


def test_profile_requires_exact_unique_actions_and_matching_hash(tmp_path: Path) -> None:
    value = _profile_value()
    value["actions"].pop("media")
    path, digest = _write_profile(tmp_path, value)
    with pytest.raises(driver.DriverError, match="actions"):
        driver._load_profile(path, digest, ACCOUNT_ID)

    value = _profile_value()
    value["actions"]["media"] = value["actions"]["round_trip"]
    path, digest = _write_profile(tmp_path, value)
    with pytest.raises(driver.DriverError, match="unique"):
        driver._load_profile(path, digest, ACCOUNT_ID)

    path, _ = _write_profile(tmp_path)
    with pytest.raises(driver.DriverError, match="hash"):
        driver._load_profile(path, "f" * 64, ACCOUNT_ID)


def test_run_calls_only_the_eight_profile_actions_and_hashes_all_ids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_path, profile_hash = _write_profile(tmp_path)
    calls: list[dict[str, Any]] = []

    def fake_broker_call(
        socket_path: Path, request: dict[str, Any], _uid: int, _gid: int
    ) -> dict[str, Any]:
        calls.append(request)
        return _broker_result(request["payload"]["case"])

    monkeypatch.setattr(driver, "_broker_call", fake_broker_call)
    result = driver._run(
        driver._parse_request(_request(profile_hash)),
        driver._load_profile(profile_path, profile_hash, ACCOUNT_ID),
        Path("/run/trpc-im/control.sock"),
        12345,
        23456,
    )

    assert result["schema_version"] == 1
    assert set(result["observations"]) == set(driver.REQUIRED_CASES)
    assert [call["action"] for call in calls] == [f"wecom-{case}" for case in driver.REQUIRED_CASES]
    assert all(call["channel"] == "wecom" for call in calls)
    assert all(set(call) == driver.BROKER_REQUEST_FIELDS for call in calls)
    rendered = json.dumps(result)
    assert "raw-" not in rendered
    assert "raw-provider-" not in rendered
    assert "raw-callback" not in rendered
    assert "raw-outbound" not in rendered
    assert ACCOUNT_ID not in rendered
    assert result["observations"]["idempotency"]["duplicate_source"] == (
        "service_replay_of_provider_event"
    )
    assert (
        result["observations"]["idempotency"]["original_event_id"]
        != result["observations"]["idempotency"]["replayed_event_id"]
    )
    assert (
        provider_runner._validate_driver_result(
            result,
            channel="wecom",
            run_nonce=RUN_NONCE,
        )
        == result["observations"]
    )


def test_snapshot_provider_hashes_must_be_unique_across_cases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_path, profile_hash = _write_profile(tmp_path)

    def fake_broker_call(
        _socket_path: Path, request: dict[str, Any], _uid: int, _gid: int
    ) -> dict[str, Any]:
        result = _broker_result(request["payload"]["case"])
        result["result"]["wecom_snapshot"]["provider_event_hash"] = "f" * 64
        return result

    monkeypatch.setattr(driver, "_broker_call", fake_broker_call)
    with pytest.raises(driver.DriverError, match="provider event identifiers must be unique"):
        driver._run(
            driver._parse_request(_request(profile_hash)),
            driver._load_profile(profile_path, profile_hash, ACCOUNT_ID),
            Path("/run/trpc-im/control.sock"),
            12345,
            23456,
        )


@pytest.mark.parametrize("case", ["reconnect", "prolonged_outage"])
@pytest.mark.parametrize("mode", ["graceful", "hard"])
def test_failover_cases_accept_real_graceful_or_hard_lifecycle(
    tmp_path: Path,
    case: str,
    mode: str,
) -> None:
    profile_path, profile_hash = _write_profile(tmp_path)
    profile = driver._load_profile(profile_path, profile_hash, ACCOUNT_ID)
    response = _broker_result(case)
    if mode == "hard":
        response["result"]["observation"]["old_lock_owner_released"] = False
        response["result"]["wecom_snapshot"]["lifecycle"] = [
            {"state": "takeover", "epoch": 2},
            {"state": "authenticated", "epoch": 2},
        ]

    driver._validate_broker_response(
        case,
        response,
        driver._parse_request(_request(profile_hash)),
        profile,
    )


@pytest.mark.parametrize(
    ("released", "lifecycle"),
    [
        (
            True,
            [
                {"state": "disconnected", "epoch": 1},
                {"state": "released", "epoch": 1},
                {"state": "takeover", "epoch": 2},
                {"state": "authenticated", "epoch": 2},
            ],
        ),
        (
            True,
            [
                {"state": "disconnected", "epoch": 1},
                {"state": "acquired", "epoch": 2},
                {"state": "released", "epoch": 1},
                {"state": "authenticated", "epoch": 2},
            ],
        ),
        (
            True,
            [
                {"state": "disconnected", "epoch": 1},
                {"state": "released", "epoch": 1},
                {"state": "authenticated", "epoch": 2},
            ],
        ),
        (
            True,
            [
                {"state": "disconnected", "epoch": 1},
                {"state": "released", "epoch": 1},
                {"state": "acquired", "epoch": 1},
                {"state": "authenticated", "epoch": 1},
            ],
        ),
        (
            False,
            [
                {"state": "takeover", "epoch": 1},
                {"state": "authenticated", "epoch": 1},
            ],
        ),
        (
            False,
            [
                {"state": "authenticated", "epoch": 2},
                {"state": "takeover", "epoch": 2},
            ],
        ),
        (
            False,
            [
                {"state": "disconnected", "epoch": 1},
                {"state": "released", "epoch": 1},
                {"state": "acquired", "epoch": 2},
                {"state": "authenticated", "epoch": 2},
            ],
        ),
    ],
)
def test_failover_cases_reject_mixed_missing_out_of_order_or_stale_lifecycle(
    tmp_path: Path,
    released: bool,
    lifecycle: list[dict[str, Any]],
) -> None:
    profile_path, profile_hash = _write_profile(tmp_path)
    profile = driver._load_profile(profile_path, profile_hash, ACCOUNT_ID)
    response = _broker_result("reconnect")
    response["result"]["observation"]["old_lock_owner_released"] = released
    response["result"]["wecom_snapshot"]["lifecycle"] = lifecycle

    with pytest.raises(driver.DriverError, match="lifecycle"):
        driver._validate_broker_response(
            "reconnect",
            response,
            driver._parse_request(_request(profile_hash)),
            profile,
        )


def test_failover_epoch_must_match_observation(tmp_path: Path) -> None:
    profile_path, profile_hash = _write_profile(tmp_path)
    profile = driver._load_profile(profile_path, profile_hash, ACCOUNT_ID)
    response = _broker_result("prolonged_outage")
    response["result"]["observation"]["connection_epoch"] = 3

    with pytest.raises(driver.DriverError, match="epoch"):
        driver._validate_broker_response(
            "prolonged_outage",
            response,
            driver._parse_request(_request(profile_hash)),
            profile,
        )


def test_broker_failure_or_extra_snapshot_fields_fail_closed(tmp_path: Path) -> None:
    profile_path, profile_hash = _write_profile(tmp_path)
    request = driver._parse_request(_request(profile_hash))
    profile = driver._load_profile(profile_path, profile_hash, ACCOUNT_ID)
    with pytest.raises(driver.DriverError, match="broker"):
        driver._validate_broker_response(
            "round_trip",
            {"status": "not_run", "error_code": "handler_failed"},
            request,
            profile,
        )

    response = _broker_result("round_trip")
    response["result"]["wecom_snapshot"]["raw_provider_event_id"] = "must-not-pass"
    with pytest.raises(driver.DriverError, match="snapshot schema"):
        driver._validate_broker_response("round_trip", response, request, profile)


def test_driver_source_has_no_websocket_or_provider_sdk_dependency() -> None:
    source = Path(driver.__file__).read_text(encoding="utf-8").lower()
    assert "websocket" not in source
    assert "wecom_aibot" not in source
    assert "bot_secret" not in source


def test_main_emits_only_strict_runner_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsysbinary: pytest.CaptureFixture[bytes],
) -> None:
    profile_path, profile_hash = _write_profile(tmp_path)
    request = driver._parse_request(_request(profile_hash))
    profile = driver._load_profile(profile_path, profile_hash, ACCOUNT_ID)
    expected = {"schema_version": 1, "observations": {}}
    monkeypatch.setattr(driver, "_read_request", lambda: request)
    monkeypatch.setattr(
        driver,
        "_configuration",
        lambda _request: (profile, Path("/run/trpc-im/control.sock"), 12345, 23456),
    )
    monkeypatch.setattr(driver, "_run", lambda *_args: expected)

    assert driver.main() == 0
    raw = capsysbinary.readouterr().out
    assert raw == b'{"observations":{},"schema_version":1}\n'
    assert driver._strict_json(raw.decode()) == expected


def test_main_failure_is_content_free(
    monkeypatch: pytest.MonkeyPatch,
    capsysbinary: pytest.CaptureFixture[bytes],
) -> None:
    def fail() -> driver.DriverRequest:
        raise driver.DriverError("secret path and raw provider ID")

    monkeypatch.setattr(driver, "_read_request", fail)

    assert driver.main() == 1
    captured = capsysbinary.readouterr()
    assert captured.out == captured.err == b""


def test_broker_peer_is_verified_on_the_same_socket_before_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent = False

    class FakeConnection:
        def __enter__(self) -> FakeConnection:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def settimeout(self, _timeout: float) -> None:
            return None

        def connect(self, _path: str) -> None:
            return None

        def getsockopt(self, *_args: object) -> bytes:
            return struct.pack("3i", 99, 1001, 1002)

        def sendall(self, _payload: bytes) -> None:
            nonlocal sent
            sent = True

    monkeypatch.setattr(driver.sys, "platform", "linux")
    monkeypatch.setattr(driver.socket, "AF_UNIX", 1, raising=False)
    monkeypatch.setattr(driver.socket, "SO_PEERCRED", 17, raising=False)
    monkeypatch.setattr(driver.socket, "socket", lambda *_args: FakeConnection())
    monkeypatch.setattr(driver, "_validate_socket_path", lambda _path: None)

    with pytest.raises(driver.DriverError, match="identity does not match"):
        driver._broker_call(
            Path("/run/trpc-im-probe/control.sock"),
            {"schema_version": 1},
            1001,
            9999,
        )
    assert sent is False
