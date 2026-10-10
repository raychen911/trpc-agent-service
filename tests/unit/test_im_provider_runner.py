from __future__ import annotations

import hashlib
import json
import os
import socket
import stat
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from deploy.im_probe import provider_runner as runner

IMAGE_DIGEST = "sha256:" + "1" * 64
RUN_NONCE = "offline_nonce_123456"
CONTROL_PROFILE = b'{"control_profile":"offline"}\n'
CONTROL_PROFILE_SHA256 = hashlib.sha256(CONTROL_PROFILE).hexdigest()
RELEASE_ID = "release-offline-runner"
RELEASE_NONCE_SHA256 = "c" * 64
SOURCE_FINGERPRINT = "d" * 64


def _request(channel: str = "feishu") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "channel": channel,
        "run_id": f"offline-{channel}-run",
        "run_nonce": RUN_NONCE,
        "expected_image_digest": IMAGE_DIGEST,
        "release_id": RELEASE_ID,
        "release_nonce_sha256": RELEASE_NONCE_SHA256,
        "source_fingerprint": SOURCE_FINGERPRINT,
        "control_profile_sha256": CONTROL_PROFILE_SHA256,
        "cases": list(runner.REQUIRED_CASES),
    }


def _observations(channel: str = "feishu") -> dict[str, dict[str, Any]]:
    timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    common: dict[str, Any] = {
        "status": "pass",
        "run_nonce": RUN_NONCE,
        "observed_at": timestamp,
    }
    observations = {
        "round_trip": {
            **common,
            "provider_event_id": f"{channel}-round-trip",
            "callback_event_id": f"{channel}-callback",
            "outbound_request_id": f"{channel}-request",
            "provider_code": 0,
        },
        "idempotency": {
            **common,
            "provider_event_id": f"{channel}-first-event",
            "duplicate_event_id": f"{channel}-duplicate-event",
            "unique_inbound_id": f"{channel}-inbound",
            "duplicate_count": 1,
            **(
                {
                    "original_event_id": f"{channel}-duplicate-event",
                    "provider_delivery_count": 2,
                }
                if channel == "feishu"
                else {
                    "duplicate_source": "service_replay_of_provider_event",
                    "original_event_id": f"{channel}-original-processing",
                    "replayed_event_id": f"{channel}-replayed-processing",
                }
            ),
        },
        "media": {
            **common,
            "provider_event_id": f"{channel}-media-event",
            "media_id_hash": "a" * 64,
            "sha256": "b" * 64,
            "bytes": 16,
        },
        "reconnect": {
            **common,
            "provider_event_id": f"{channel}-reconnect-event",
            **(
                {
                    "failed_endpoint_id": "feishu-gateway-old",
                    "replacement_endpoint_id": "feishu-gateway-new",
                    "endpoint_set_observed": True,
                    "received_after_failover_event_id": "feishu-after-failover",
                    "outbound_request_id": "feishu-failover-outbound",
                    "acknowledged_request_id": "feishu-failover-outbound",
                    "ready_endpoint_count": 1,
                    "unready_endpoint_count": 0,
                    "terminating_endpoint_count": 0,
                }
                if channel == "feishu"
                else {
                    "disconnect_event_id": f"{channel}-disconnect",
                    "reconnect_event_id": f"{channel}-reconnect",
                    "received_after_reconnect_event_id": f"{channel}-after-reconnect",
                    "lock_takeover_event_id": f"{channel}-lock-takeover",
                    "old_lock_owner_released": True,
                    "new_lock_owner_acquired": True,
                    "lock_epoch": 2,
                    "outbound_request_id": "wecom-reconnect-outbound",
                    "acknowledged_request_id": "wecom-reconnect-outbound",
                    "provider_code": 0,
                }
            ),
        },
        "rate_limit_retry_after": {
            **common,
            "provider_event_id": f"{channel}-rate-event",
            "provider_error_code": 429,
            "retry_after_seconds": 1.0,
            "retry_request_id": f"{channel}-retry",
            "retry_attempts": 2,
            "retry_elapsed_seconds": 1.0,
        },
        "credential_rotation": {
            **common,
            "provider_event_id": f"{channel}-rotation-event",
            "old_credential_event_id": f"{channel}-old",
            "new_credential_event_id": f"{channel}-new",
            "post_rotation_event_id": f"{channel}-post-rotation",
            "old_credential_rejected": True,
            **(
                {
                    "outbound_request_id": "wecom-rotation-outbound",
                    "acknowledged_request_id": "wecom-rotation-outbound",
                    "provider_code": 0,
                }
                if channel == "wecom"
                else {}
            ),
        },
        "prolonged_outage": {
            **common,
            "provider_event_id": f"{channel}-outage-event",
            "outage_event_id": f"{channel}-outage",
            "recovery_event_id": f"{channel}-recovery",
            "outage_seconds": 60.0,
        },
        "ambiguous": {
            **common,
            "provider_event_id": f"{channel}-ambiguous-event",
            "ambiguous_event_id": f"{channel}-ambiguous",
            "manual_review_id": f"{channel}-manual-review",
            "drop_response_observed": True,
            "auto_replay_count": 0,
        },
    }
    if channel == "wecom":
        observations["prolonged_outage"].update(
            {
                "outage_mode": "service_failover",
                "failed_instance_id": "wecom-primary",
                "takeover_instance_id": "wecom-standby",
                "old_lock_owner_released": True,
                "new_lock_owner_acquired": True,
                "connection_epoch": 3,
                "event_during_outage_id": "wecom-outage-inbound",
                "reply_for_event_id": "wecom-outage-inbound",
                "outbound_request_id": "wecom-outbound-request",
                "acknowledged_request_id": "wecom-outbound-request",
                "reply_count": 1,
                "ack_count": 1,
                "pending_count": 0,
                "dlq_count": 0,
            }
        )
    return observations


def _driver_result(channel: str = "feishu") -> dict[str, Any]:
    return {"schema_version": 1, "observations": _observations(channel)}


def _secret_files(tmp_path: Path, channel: str) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    variables = (
        *runner.REQUIRED_SECRET_ENV_BY_CHANNEL[channel],
        *runner.OPTIONAL_SECRET_ENV_BY_CHANNEL[channel],
    )
    for index, variable in enumerate(variables):
        path = tmp_path / f"secret-{channel}-{index}"
        path.write_text("not-used-by-runner", encoding="utf-8")
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)
        paths[variable] = path
    return paths


def _control_paths(tmp_path: Path, channel: str) -> tuple[Path, Path, socket.socket | None]:
    if os.name != "nt":
        tmp_path.chmod(0o750)
    profile = tmp_path / f"control-profile-{channel}.json"
    profile.write_bytes(CONTROL_PROFILE)
    profile.chmod(stat.S_IRUSR | stat.S_IWUSR)
    control_socket = tmp_path / f"control-{channel}.sock"
    listener = None
    if hasattr(socket, "AF_UNIX"):
        listener = socket.socket(socket.AF_UNIX)
        listener.bind(str(control_socket))
        control_socket.chmod(0o660)
    else:
        control_socket.write_text("windows-unix-socket-placeholder", encoding="utf-8")
    return profile, control_socket, listener


def _set_channel_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    channel: str,
    request: pytest.FixtureRequest,
) -> Path:
    app_root = tmp_path / "application"
    app_root.mkdir()
    monkeypatch.setenv("TRPC_IM_PROBE_APPLICATION_ROOT", str(app_root))
    monkeypatch.setenv(
        runner.ACCOUNT_ENV_BY_CHANNEL[channel],
        f"cli-{channel}-account",
    )
    for variable, path in _secret_files(tmp_path, channel).items():
        monkeypatch.setenv(variable, str(path))
    driver = tmp_path / f"driver-{channel}"
    driver.write_bytes(b"#!/bin/sh\nexit 0\n")
    driver.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    monkeypatch.setenv(runner.DRIVER_ENV_BY_CHANNEL[channel], str(driver))
    monkeypatch.setenv(
        f"{runner.DRIVER_ENV_BY_CHANNEL[channel]}_SHA256",
        hashlib.sha256(driver.read_bytes()).hexdigest(),
    )
    monkeypatch.setattr(
        runner,
        "_trusted_artifact_sha256",
        lambda path, *, label: hashlib.sha256(path.read_bytes()).hexdigest(),
    )
    profile, control_socket, listener = _control_paths(tmp_path, channel)
    if listener is not None:
        request.addfinalizer(listener.close)
    monkeypatch.setenv(runner.CONTROL_PROFILE_ENV_BY_CHANNEL[channel], str(profile))
    monkeypatch.setenv(runner.CONTROL_SOCKET_ENV, str(control_socket))
    monkeypatch.setenv(runner.BROKER_UID_ENV, str(os.getuid() if hasattr(os, "getuid") else 0))
    monkeypatch.setenv(runner.BROKER_GID_ENV, str(os.getgid() if hasattr(os, "getgid") else 0))
    if os.name != "nt":
        monkeypatch.setattr(os, "geteuid", lambda: os.getuid() + 1)
    return driver


def test_request_parser_is_strict_about_shape_duplicates_and_non_finite_values() -> None:
    valid = _request()
    assert runner._parse_request(valid) == valid

    missing = dict(valid)
    missing.pop("run_id")
    with pytest.raises(runner.RunnerError, match="schema"):
        runner._parse_request(missing)

    extra = dict(valid)
    extra["unexpected"] = "value"
    with pytest.raises(runner.RunnerError, match="schema"):
        runner._parse_request(extra)

    with pytest.raises(ValueError, match="duplicate"):
        runner._strict_json('{"a":1,"a":2}')
    with pytest.raises(ValueError, match="non-finite"):
        runner._strict_json('{"a":NaN}')


@pytest.mark.parametrize(
    "mutate",
    [
        lambda result: result["observations"].pop("media"),
        lambda result: result["observations"]["media"].update({"extra": "field"}),
        lambda result: result.update({"provider_evidence": {}}),
        lambda result: result["observations"]["round_trip"].update({"status": "not_run"}),
        lambda result: result["observations"]["round_trip"].update(
            {"provider_event_id": "synthetic-event"}
        ),
    ],
)
def test_driver_result_partial_extra_and_fake_results_are_rejected(
    mutate: Callable[[dict[str, Any]], Any],
) -> None:
    result = _driver_result()
    mutate(result)
    with pytest.raises(runner.RunnerError):
        runner._validate_driver_result(result, channel="feishu", run_nonce=RUN_NONCE)


def test_driver_result_rejects_duplicate_provider_event_ids() -> None:
    result = _driver_result()
    result["observations"]["media"]["provider_event_id"] = result["observations"]["round_trip"][
        "provider_event_id"
    ]
    with pytest.raises(runner.RunnerError, match="unique"):
        runner._validate_driver_result(result, channel="feishu", run_nonce=RUN_NONCE)


def test_feishu_idempotency_accepts_only_real_provider_redelivery() -> None:
    validated = runner._validate_driver_result(
        _driver_result("feishu"),
        channel="feishu",
        run_nonce=RUN_NONCE,
    )

    idempotency = validated["idempotency"]
    assert idempotency["provider_delivery_count"] == 2
    assert idempotency["duplicate_event_id"] == idempotency["original_event_id"]
    assert "duplicate_source" not in idempotency
    assert "replayed_event_id" not in idempotency


@pytest.mark.parametrize("value", [True, 0, 1, 1.5, "2"])
def test_feishu_idempotency_rejects_invalid_provider_delivery_count(value: object) -> None:
    result = _driver_result("feishu")
    result["observations"]["idempotency"]["provider_delivery_count"] = value

    with pytest.raises(runner.RunnerError, match="provider_delivery_count"):
        runner._validate_driver_result(result, channel="feishu", run_nonce=RUN_NONCE)


def test_feishu_idempotency_rejects_distinct_original_and_duplicate_provider_ids() -> None:
    result = _driver_result("feishu")
    result["observations"]["idempotency"]["original_event_id"] = "feishu-other-provider-event"

    with pytest.raises(runner.RunnerError, match="must match original_event_id"):
        runner._validate_driver_result(result, channel="feishu", run_nonce=RUN_NONCE)


def test_wecom_idempotency_accepts_only_service_replay_actions() -> None:
    validated = runner._validate_driver_result(
        _driver_result("wecom"),
        channel="wecom",
        run_nonce=RUN_NONCE,
    )

    idempotency = validated["idempotency"]
    assert idempotency["duplicate_source"] == "service_replay_of_provider_event"
    assert idempotency["original_event_id"] != idempotency["replayed_event_id"]
    assert idempotency["duplicate_count"] == 1
    assert "provider_delivery_count" not in idempotency


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("duplicate_source", "provider_redelivery", "duplicate_source"),
        ("replayed_event_id", "wecom-original-processing", "processing actions must differ"),
        ("original_event_id", "unsafe event id", "original_event_id"),
    ],
)
def test_wecom_idempotency_rejects_false_provider_duplicate_or_invalid_actions(
    field: str,
    value: object,
    message: str,
) -> None:
    result = _driver_result("wecom")
    result["observations"]["idempotency"][field] = value

    with pytest.raises(runner.RunnerError, match=message):
        runner._validate_driver_result(result, channel="wecom", run_nonce=RUN_NONCE)


@pytest.mark.parametrize(
    ("channel", "field", "value"),
    [
        ("feishu", "duplicate_source", "service_replay_of_provider_event"),
        ("feishu", "replayed_event_id", "feishu-service-replay"),
        ("wecom", "provider_delivery_count", 2),
    ],
)
def test_idempotency_channel_schemas_reject_mixed_contract_fields(
    channel: str,
    field: str,
    value: object,
) -> None:
    result = _driver_result(channel)
    result["observations"]["idempotency"][field] = value

    with pytest.raises(runner.RunnerError, match="invalid schema"):
        runner._validate_driver_result(result, channel=channel, run_nonce=RUN_NONCE)


def test_feishu_driver_result_accepts_endpoint_failover_reconnect_contract() -> None:
    validated = runner._validate_driver_result(
        _driver_result("feishu"),
        channel="feishu",
        run_nonce=RUN_NONCE,
    )

    reconnect = validated["reconnect"]
    assert reconnect["failed_endpoint_id"] != reconnect["replacement_endpoint_id"]
    assert reconnect["endpoint_set_observed"] is True
    assert reconnect["acknowledged_request_id"] == reconnect["outbound_request_id"]
    assert reconnect["ready_endpoint_count"] >= 1
    assert reconnect["unready_endpoint_count"] == reconnect["terminating_endpoint_count"] == 0


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("replacement_endpoint_id", "feishu-gateway-old", "endpoints must differ"),
        ("endpoint_set_observed", False, "endpoint set was not observed"),
        ("ready_endpoint_count", 0, "ready_endpoint_count must be positive"),
        ("unready_endpoint_count", 1, "unready_endpoint_count must be 0"),
        ("terminating_endpoint_count", 1, "terminating_endpoint_count must be 0"),
        (
            "acknowledged_request_id",
            "feishu-other-outbound",
            "acknowledgement must match the outbound request",
        ),
    ],
)
def test_feishu_driver_result_rejects_invalid_endpoint_failover_reconnect_contract(
    field: str,
    value: object,
    message: str,
) -> None:
    result = _driver_result("feishu")
    result["observations"]["reconnect"][field] = value

    with pytest.raises(runner.RunnerError, match=message):
        runner._validate_driver_result(result, channel="feishu", run_nonce=RUN_NONCE)


def test_feishu_reconnect_schema_rejects_wecom_lock_fields() -> None:
    result = _driver_result("feishu")
    result["observations"]["reconnect"]["lock_epoch"] = 2

    with pytest.raises(runner.RunnerError, match="invalid schema"):
        runner._validate_driver_result(result, channel="feishu", run_nonce=RUN_NONCE)


def test_wecom_reconnect_schema_rejects_feishu_endpoint_fields() -> None:
    result = _driver_result("wecom")
    result["observations"]["reconnect"]["failed_endpoint_id"] = "feishu-gateway-old"

    with pytest.raises(runner.RunnerError, match="invalid schema"):
        runner._validate_driver_result(result, channel="wecom", run_nonce=RUN_NONCE)


@pytest.mark.parametrize("released", [True, False])
def test_wecom_driver_result_accepts_graceful_or_hard_service_failover_contract(
    released: bool,
) -> None:
    result = _driver_result("wecom")
    result["observations"]["reconnect"]["old_lock_owner_released"] = released
    result["observations"]["prolonged_outage"]["old_lock_owner_released"] = released
    validated = runner._validate_driver_result(
        result,
        channel="wecom",
        run_nonce=RUN_NONCE,
    )
    outage = validated["prolonged_outage"]
    assert outage["outage_mode"] == "service_failover"
    assert outage["failed_instance_id"] != outage["takeover_instance_id"]
    assert outage["reply_for_event_id"] == outage["event_during_outage_id"]
    assert outage["acknowledged_request_id"] == outage["outbound_request_id"]
    assert outage["reply_count"] == outage["ack_count"] == 1
    assert outage["pending_count"] == outage["dlq_count"] == 0


@pytest.mark.parametrize(
    ("case", "field", "value"),
    [
        ("reconnect", "old_lock_owner_released", None),
        ("reconnect", "new_lock_owner_acquired", False),
        ("reconnect", "lock_epoch", 1),
        ("prolonged_outage", "old_lock_owner_released", 0),
        ("prolonged_outage", "new_lock_owner_acquired", False),
        ("prolonged_outage", "connection_epoch", 1),
    ],
)
def test_wecom_driver_result_rejects_invalid_handoff_contract(
    case: str,
    field: str,
    value: object,
) -> None:
    result = _driver_result("wecom")
    result["observations"][case][field] = value

    with pytest.raises(runner.RunnerError):
        runner._validate_driver_result(result, channel="wecom", run_nonce=RUN_NONCE)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("takeover_instance_id", "wecom-primary", "instances must differ"),
        ("reply_for_event_id", "wecom-new-marker", "must match the outage event"),
        (
            "acknowledged_request_id",
            "wecom-other-request",
            "acknowledgement must match the outbound request",
        ),
        ("pending_count", 1, "pending_count must be 0"),
        ("dlq_count", 1, "dlq_count must be 0"),
    ],
)
def test_wecom_driver_result_rejects_invalid_service_failover_contract(
    field: str,
    value: object,
    message: str,
) -> None:
    result = _driver_result("wecom")
    result["observations"]["prolonged_outage"][field] = value
    with pytest.raises(runner.RunnerError, match=message):
        runner._validate_driver_result(result, channel="wecom", run_nonce=RUN_NONCE)


def test_wecom_driver_result_rejects_provider_gap_without_contract_v1_replay() -> None:
    result = _driver_result("wecom")
    observation = result["observations"]["prolonged_outage"]
    for field in runner.WECOM_SERVICE_FAILOVER_FIELDS:
        observation.pop(field)
    observation["outage_mode"] = "provider_delivery_gap"
    with pytest.raises(runner.RunnerError, match="not supported by contract v1"):
        runner._validate_driver_result(result, channel="wecom", run_nonce=RUN_NONCE)


def test_driver_environment_is_current_channel_allowlist_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    feishu_paths = _secret_files(tmp_path, "feishu")
    profile, control_socket, listener = _control_paths(tmp_path, "feishu")
    if listener is not None:
        request.addfinalizer(listener.close)
    monkeypatch.setenv(runner.BROKER_UID_ENV, "12345")
    monkeypatch.setenv(runner.BROKER_GID_ENV, "23456")
    for variable in (
        "PYTHONPATH",
        "KUBECONFIG",
        "DATABASE_URL",
        "OIDC_CLIENT_SECRET",
        "TRPC_IM_PROBE_WECOM_CONTROL_PROFILE_FILE",
        "TRPC_IM_PROBE_PROVIDER_ADMIN_TOKEN",
    ):
        monkeypatch.setenv(variable, "must-not-reach-driver")
    environment = runner._driver_environment(
        "feishu",
        runner.ACCOUNT_ENV_BY_CHANNEL["feishu"],
        "cli-feishu-account",
        feishu_paths,
        profile,
        control_socket,
    )
    assert set(environment) == {
        runner.ACCOUNT_ENV_BY_CHANNEL["feishu"],
        *feishu_paths,
        runner.CONTROL_PROFILE_ENV_BY_CHANNEL["feishu"],
        runner.CONTROL_SOCKET_ENV,
        runner.BROKER_UID_ENV,
        runner.BROKER_GID_ENV,
    }
    assert all("WECOM" not in key for key in environment)
    assert "PYTHONPATH" not in environment
    assert "TRPC_IM_PROBE_FEISHU_DRIVER" not in environment
    assert all("not-used-by-runner" not in value for value in environment.values())
    assert all(str(path) in environment.values() for path in feishu_paths.values())
    assert environment[runner.CONTROL_PROFILE_ENV_BY_CHANNEL["feishu"]] == str(profile)
    assert environment[runner.CONTROL_SOCKET_ENV] == str(control_socket)


def test_control_profile_is_bounded_and_hash_bound(
    tmp_path: Path,
) -> None:
    profile = tmp_path / "control-profile.json"
    profile.write_bytes(CONTROL_PROFILE)
    profile.chmod(stat.S_IRUSR | stat.S_IWUSR)

    assert (
        runner._validated_control_profile_path(
            str(profile),
            label=runner.CONTROL_PROFILE_ENV_BY_CHANNEL["feishu"],
            expected_sha256=CONTROL_PROFILE_SHA256,
        )
        == profile.resolve()
    )
    with pytest.raises(runner.RunnerError, match="hash"):
        runner._validated_control_profile_path(
            str(profile),
            label=runner.CONTROL_PROFILE_ENV_BY_CHANNEL["feishu"],
            expected_sha256="e" * 64,
        )

    profile.write_bytes(b"x" * (runner.MAX_CONTROL_PROFILE_BYTES + 1))
    with pytest.raises(runner.RunnerError, match="too large"):
        runner._validated_control_profile_path(
            str(profile),
            label=runner.CONTROL_PROFILE_ENV_BY_CHANNEL["feishu"],
        )


def test_driver_path_rejects_checkout_and_group_or_other_writable_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    app_root = tmp_path / "application"
    app_root.mkdir()
    monkeypatch.setenv("TRPC_IM_PROBE_APPLICATION_ROOT", str(app_root))

    inside = app_root / "driver"
    inside.write_text("driver", encoding="utf-8")
    inside.chmod(stat.S_IRUSR | stat.S_IXUSR)
    monkeypatch.setenv(runner.DRIVER_ENV_BY_CHANNEL["feishu"], str(inside))
    with pytest.raises(runner.RunnerError, match="outside"):
        runner._driver_path("feishu")

    outside = tmp_path / "outside-driver"
    outside.write_text("driver", encoding="utf-8")
    outside.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IWGRP)
    monkeypatch.setenv(runner.DRIVER_ENV_BY_CHANNEL["feishu"], str(outside))
    monkeypatch.setenv(
        f"{runner.DRIVER_ENV_BY_CHANNEL['feishu']}_SHA256",
        hashlib.sha256(outside.read_bytes()).hexdigest(),
    )
    if os.name != "nt" and outside.stat().st_mode & 0o022:
        with pytest.raises(runner.RunnerError, match="writable"):
            runner._driver_path("feishu")


def test_driver_invocation_timeout_and_oversized_output_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    driver = tmp_path / "driver"
    driver.write_text("driver", encoding="utf-8")
    driver.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    artifact_hash = hashlib.sha256(driver.read_bytes()).hexdigest()
    monkeypatch.setenv(f"{runner.DRIVER_ENV_BY_CHANNEL['feishu']}_SHA256", artifact_hash)
    monkeypatch.setattr(runner, "_trusted_artifact_sha256", lambda _path, *, label: artifact_hash)
    request = _request()
    environment = {"TRPC_IM_PROBE_FEISHU_APP_ID": "cli-feishu-account"}

    class TimeoutProcess:
        returncode = None
        killed = False

        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def communicate(self, *, input: bytes | None = None, timeout: float | None = None) -> None:
            if not self.killed:
                raise subprocess.TimeoutExpired("driver", 1)

        def kill(self) -> None:
            self.killed = True

    monkeypatch.setattr(subprocess, "Popen", TimeoutProcess)
    with pytest.raises(runner.RunnerError, match="timed out"):
        runner._invoke_driver(driver, request, environment)

    class OversizedProcess:
        returncode = 0

        def __init__(self, *_args: Any, **kwargs: Any) -> None:
            self.output = kwargs["stdout"]

        def communicate(self, *, input: bytes | None = None, timeout: float | None = None) -> None:
            self.output.write(b"x" * (runner.MAX_DRIVER_OUTPUT_BYTES + 1))

    monkeypatch.setattr(subprocess, "Popen", OversizedProcess)
    with pytest.raises(runner.RunnerError, match="too large"):
        runner._invoke_driver(driver, request, environment)


def test_run_aggregates_valid_driver_observations_and_binds_channel(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> None:
    paths = _secret_files(tmp_path, "feishu")
    driver = tmp_path / "driver"
    profile, control_socket, listener = _control_paths(tmp_path, "feishu")
    if listener is not None:
        request.addfinalizer(listener.close)
    observed: dict[str, Any] = {}
    artifact_hash = "a" * 64
    monkeypatch.setenv(runner.RUNNER_SHA256_ENV, artifact_hash)
    monkeypatch.setenv(f"{runner.DRIVER_ENV_BY_CHANNEL['feishu']}_SHA256", artifact_hash)
    monkeypatch.setenv(runner.BROKER_UID_ENV, "12345")
    monkeypatch.setenv(runner.BROKER_GID_ENV, "23456")
    monkeypatch.setattr(runner, "_trusted_artifact_sha256", lambda _path, *, label: artifact_hash)

    class ValidProcess:
        returncode = 0

        def __init__(self, *_args: Any, **kwargs: Any) -> None:
            observed.update(kwargs)
            self.output = kwargs["stdout"]

        def communicate(self, *, input: bytes | None = None, timeout: float | None = None) -> None:
            observed["input"] = input
            self.output.write(json.dumps(_driver_result()).encode("utf-8"))

    def fake_popen(*args: Any, **kwargs: Any) -> ValidProcess:
        observed.update(kwargs)
        return ValidProcess(*args, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    monkeypatch.setattr(
        runner,
        "_validate_channel_configuration",
        lambda _channel, _expected_hash: (
            driver,
            "cli-feishu-account",
            paths,
            profile,
            control_socket,
        ),
    )
    output = runner._run(_request())

    evidence = output["provider_evidence"]
    assert evidence["source"] == "feishu_api_and_webhook"
    assert evidence["independent_paths"] == ["provider_callback", "provider_send_ack"]
    assert evidence["run_nonce"] == RUN_NONCE
    assert evidence["account_fingerprint"] == runner._account_fingerprint(
        "feishu", "cli-feishu-account"
    )
    assert set(evidence["observations"]) == set(runner.REQUIRED_CASES)
    assert output["artifact_attestation"] == {
        "schema_version": 1,
        "runner_sha256": artifact_hash,
        "runner_contract_version": 1,
        "driver_sha256": artifact_hash,
        "driver_contract_version": 1,
    }
    driver_input = json.loads(observed["input"])
    assert set(driver_input) == {
        "schema_version",
        "channel",
        "run_id",
        "run_nonce",
        "expected_image_digest",
        "release_id",
        "release_nonce_sha256",
        "source_fingerprint",
        "control_profile_sha256",
        "cases",
    }
    assert driver_input["channel"] == "feishu"
    assert set(observed["env"]) == {
        runner.ACCOUNT_ENV_BY_CHANNEL["feishu"],
        *paths,
        runner.CONTROL_PROFILE_ENV_BY_CHANNEL["feishu"],
        runner.CONTROL_SOCKET_ENV,
        runner.BROKER_UID_ENV,
        runner.BROKER_GID_ENV,
    }
    assert "PYTHONPATH" not in observed["env"]
    assert "WECOM" not in " ".join(observed["env"])
    assert observed["cwd"] == str(driver.parent)


def test_check_is_content_free_and_never_invokes_driver(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    request: pytest.FixtureRequest,
) -> None:
    _set_channel_env(monkeypatch, tmp_path, "feishu", request)
    monkeypatch.delenv(runner.DRIVER_ENV_BY_CHANNEL["wecom"], raising=False)
    monkeypatch.setattr(
        subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail("--check must not invoke a driver"),
    )
    assert runner.main(["--check", "--channel", "feishu"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output == {"status": "ready"}
    assert "provider_evidence" not in output


def test_check_failure_has_no_configuration_content(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv(runner.DRIVER_ENV_BY_CHANNEL["feishu"], "/missing/driver")
    assert runner.main(["--check", "--channel", "feishu"]) == 2
    output = json.loads(capsys.readouterr().out)
    assert output == {"status": "not_ready"}


def test_main_protocol_failure_writes_no_stdout(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        runner,
        "_read_request",
        lambda: (_ for _ in ()).throw(runner.RunnerError("invalid request")),
    )
    assert runner.main([]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "provider runner failed" in captured.err


def test_self_contained_module_has_no_application_path_import() -> None:
    source = Path(runner.__file__).read_text(encoding="utf-8")
    assert "scripts.im_online_gate" not in source
    assert "PYTHONPATH" in source
