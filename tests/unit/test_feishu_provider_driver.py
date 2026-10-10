from __future__ import annotations

import hashlib
import json
import struct
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from deploy.im_probe import feishu_provider_driver as driver

APP_ID = "cli_driver_app"
RUN_NONCE = "driver-nonce-0123456789"
IMAGE_DIGEST = "sha256:" + "a" * 64
RELEASE_ID = "release-offline-feishu"
RELEASE_NONCE_SHA256 = "d" * 64
SOURCE_FINGERPRINT = "e" * 64


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _profile(observer_socket: Path) -> dict[str, object]:
    return {
        "schema_version": 1,
        "channel": "feishu",
        "account_id_sha256": driver.account_id_sha256(APP_ID),
        "observer_socket": str(observer_socket),
        "observer_profile_sha256": _hash("observer-profile"),
        "openapi_witness_socket": str(observer_socket.parent / "openapi-witness.sock"),
        "broker_actions": dict(driver.BROKER_ACTIONS),
    }


def _write_profile(tmp_path: Path, value: dict[str, object] | None = None) -> Path:
    path = tmp_path / "feishu-control.json"
    path.write_text(
        json.dumps(value or _profile(tmp_path / "observer.sock"), separators=(",", ":")),
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def _request(profile_path: Path) -> dict[str, object]:
    return {
        "schema_version": 1,
        "channel": "feishu",
        "run_id": "feishu-live-run",
        "run_nonce": RUN_NONCE,
        "expected_image_digest": IMAGE_DIGEST,
        "release_id": RELEASE_ID,
        "release_nonce_sha256": RELEASE_NONCE_SHA256,
        "source_fingerprint": SOURCE_FINGERPRINT,
        "control_profile_sha256": hashlib.sha256(profile_path.read_bytes()).hexdigest(),
        "cases": list(driver.REQUIRED_CASES),
    }


def _environment(tmp_path: Path, profile_path: Path) -> dict[str, str]:
    return {
        "TRPC_IM_PROBE_FEISHU_APP_ID": APP_ID,
        "TRPC_IM_PROBE_FEISHU_CONTROL_PROFILE_FILE": str(profile_path),
        "TRPC_IM_PROBE_CONTROL_SOCKET": str(tmp_path / "broker.sock"),
        driver.BROKER_UID_ENV: "12345",
        driver.BROKER_GID_ENV: "23456",
        # A real runner may pass secret paths too.  This driver intentionally
        # never reads them.
        "TRPC_IM_PROBE_FEISHU_APP_SECRET_FILE": str(tmp_path / "unused-secret"),
    }


def _observation(case: str) -> dict[str, object]:
    common: dict[str, object] = {
        "status": "pass",
        "run_nonce": RUN_NONCE,
        "provider_event_id": f"provider-{case}",
        "observed_at": datetime.now(UTC).isoformat(),
    }
    values: dict[str, dict[str, object]] = {
        "round_trip": {
            "callback_event_id": "callback-round-trip",
            "outbound_request_id": "outbound-round-trip",
            "provider_code": 0,
        },
        "idempotency": {
            "duplicate_event_id": "duplicate-event",
            "unique_inbound_id": "unique-inbound",
            "duplicate_count": 1,
            "original_event_id": "duplicate-event",
            "provider_delivery_count": 2,
        },
        "media": {"media_id_hash": "b" * 64, "sha256": "c" * 64, "bytes": 16},
        "reconnect": {
            "failed_endpoint_id": "gateway-old",
            "replacement_endpoint_id": "gateway-new",
            "endpoint_set_observed": True,
            "received_after_failover_event_id": "after-failover",
            "outbound_request_id": "failover-outbound",
            "acknowledged_request_id": "failover-outbound",
            "ready_endpoint_count": 1,
            "unready_endpoint_count": 0,
            "terminating_endpoint_count": 0,
        },
        "rate_limit_retry_after": {
            "provider_error_code": 429,
            "retry_after_seconds": 1.0,
            "retry_request_id": "retry-request",
            "retry_attempts": 2,
            "retry_elapsed_seconds": 1.0,
        },
        "credential_rotation": {
            "old_credential_event_id": "old-credential",
            "new_credential_event_id": "new-credential",
            "post_rotation_event_id": "post-rotation",
            "old_credential_rejected": True,
        },
        "prolonged_outage": {
            "outage_event_id": "outage-event",
            "recovery_event_id": "recovery-event",
            "outage_seconds": 60.0,
        },
        "ambiguous": {
            "ambiguous_event_id": "ambiguous-event",
            "manual_review_id": "manual-review",
            "drop_response_observed": True,
            "auto_replay_count": 0,
        },
    }
    return {**common, **values[case]}


class _FakeEvidenceSockets:
    def __init__(self, tmp_path: Path) -> None:
        self.broker = (tmp_path / "broker.sock").resolve()
        self.observer = (tmp_path / "observer.sock").resolve()
        self.witness = (tmp_path / "openapi-witness.sock").resolve()
        self.actions: list[str] = []
        self.observer_queries: list[dict[str, object]] = []
        self.observations: dict[str, dict[str, object]] = {}
        self.broker_requests: list[dict[str, object]] = []
        self.receipts: dict[str, tuple[str, str]] = {}
        self.witness_receipts: dict[int, list[dict[str, object]]] = {}
        self.witness_queries: list[dict[str, object]] = []
        self.observer_status = "found"
        self.forge_expected = False
        self.extra_broker_field = False
        self.witness_failure: str | None = None
        self.witness_failure_case: str | None = None

    def validate(self, raw: str, _label: str) -> Path:
        path = Path(raw)
        if not path.is_absolute() or path.is_symlink():
            raise driver.DriverError("socket is invalid")
        return path.resolve()

    def exchange(self, path: Path, payload: bytes, _timeout: float, _limit: int) -> bytes:
        request = json.loads(payload)
        if path == self.broker:
            self.broker_requests.append(request)
            case = request["payload"]["case"]
            action = request["action"]
            self.actions.append(action)
            event_hash = _hash(f"event-{case}")
            message_hash = _hash(f"message-{case}")
            marker_hash = _hash(f"marker-{case}")
            self.receipts[marker_hash] = (event_hash, message_hash)
            observation = _observation(case)
            self.observations[case] = observation
            response: dict[str, object] = {
                "status": "pass",
                "result": {
                    "observation": observation,
                    "callback_query": {
                        "marker_sha256": marker_hash,
                        "profile_sha256": _hash("observer-profile"),
                    },
                    "callback_expected": {
                        "event_id_sha256": (
                            _hash("forged-event") if self.forge_expected else event_hash
                        ),
                        "message_id_sha256": message_hash,
                    },
                },
            }
            if case in driver.ACK_CASES:
                after_sequence = len(self.actions) * 10
                path_hash = _hash(f"path-{case}")
                body_hash = _hash(f"body-{case}")
                result = response["result"]
                assert isinstance(result, dict)
                result["openapi_witness"] = {
                    "after_sequence": after_sequence,
                    "path_sha256": path_hash,
                    "body_sha256": body_hash,
                }
                self.witness_receipts[after_sequence] = self._witness_receipts(
                    case,
                    after_sequence=after_sequence,
                    path_hash=path_hash,
                    body_hash=body_hash,
                )
            if self.extra_broker_field:
                response["secret"] = "must-not-pass"
            return json.dumps(response, separators=(",", ":")).encode()
        if path == self.witness:
            self.witness_queries.append(request)
            receipts = self.witness_receipts[request["after_sequence"]]
            return json.dumps(
                {"status": "pass", "receipts": receipts}, separators=(",", ":")
            ).encode()
        assert path == self.observer
        self.observer_queries.append(request)
        if self.observer_status != "found":
            return b'{"status":"not_found"}\n'
        marker_hash = request["marker_sha256"]
        event_hash, message_hash = self.receipts[marker_hash]
        return json.dumps(
            {
                "status": "found",
                "receipt": {
                    "event_id_sha256": event_hash,
                    "marker_sha256": marker_hash,
                    "media_locator_sha256": [],
                    "message_id_sha256": message_hash,
                    "observed_at": datetime.now(UTC).isoformat(),
                    "profile_sha256": _hash("observer-profile"),
                    "provider_time_sha256": _hash("provider-time"),
                    "receipt_sha256": _hash(f"receipt-{marker_hash}"),
                },
            },
            separators=(",", ":"),
        ).encode()

    def _witness_receipts(
        self,
        case: str,
        *,
        after_sequence: int,
        path_hash: str,
        body_hash: str,
    ) -> list[dict[str, object]]:
        now = datetime.now(UTC)
        failure = self.witness_failure if self.witness_failure_case in {None, case} else None

        def receipt(
            sequence: int,
            *,
            status: int,
            code: int,
            acknowledged: bool,
            dropped: bool = False,
            retry_after: float | None = None,
            observed_at: datetime = now,
        ) -> dict[str, object]:
            return {
                "sequence": sequence,
                "path_sha256": path_hash,
                "body_sha256": (_hash("wrong-body") if failure == "hash_mismatch" else body_hash),
                "provider_status": status,
                "provider_code": code,
                "provider_request_id_sha256": (
                    None
                    if failure == "no_request_id"
                    else _hash(f"provider-request-{case}-{sequence}")
                ),
                "retry_after_seconds": retry_after,
                "provider_acknowledged": acknowledged,
                "downstream_response_dropped": (False if failure == "bad_drop" else dropped),
                "observed_at": observed_at.isoformat(),
            }

        if failure == "missing":
            return []
        success = receipt(
            after_sequence + 2,
            status=200,
            code=1 if failure == "bad_provider_code" else 0,
            acknowledged=failure != "bad_ack",
            dropped=case == "ambiguous",
        )
        if case != "rate_limit_retry_after" or failure == "rate_missing_limited":
            return [success]
        limited = receipt(
            after_sequence + 1,
            status=429,
            code=99991400,
            acknowledged=False,
            retry_after=1.0,
            observed_at=now - timedelta(seconds=1.1),
        )
        return [limited, success]


def test_driver_runs_all_fixed_actions_and_returns_only_broker_observations(
    tmp_path: Path,
) -> None:
    profile = _write_profile(tmp_path)
    sockets = _FakeEvidenceSockets(tmp_path)

    result = driver.run_driver(
        _request(profile),
        _environment(tmp_path, profile),
        exchange=sockets.exchange,
        socket_validator=sockets.validate,
    )

    assert result == {
        "schema_version": 1,
        "observations": sockets.observations,
    }
    assert sockets.actions == [driver.BROKER_ACTIONS[case] for case in driver.REQUIRED_CASES]
    for broker_request in sockets.broker_requests:
        assert set(broker_request) == {
            "schema_version",
            "channel",
            "action",
            "run_id",
            "run_nonce",
            "control_profile_sha256",
            "payload",
        }
        payload = broker_request["payload"]
        assert isinstance(payload, dict)
        assert set(payload) == {
            "case",
            "expected_image_digest",
            "account_id_sha256",
            "observer_profile_sha256",
        }
    assert len(sockets.observer_queries) == 8
    assert all(
        query["profile_sha256"] == _hash("observer-profile") for query in sockets.observer_queries
    )
    assert len(sockets.witness_queries) == len(driver.ACK_CASES)
    assert all(
        set(query) == {"schema_version", "action", "after_sequence", "limit"}
        and query["action"] == "query"
        and query["limit"] == 100
        for query in sockets.witness_queries
    )


def test_profile_file_and_account_hash_are_bound_to_request(tmp_path: Path) -> None:
    profile = _write_profile(tmp_path)
    sockets = _FakeEvidenceSockets(tmp_path)
    request = _request(profile)
    request["control_profile_sha256"] = "d" * 64

    with pytest.raises(driver.DriverError, match="profile"):
        driver.run_driver(
            request,
            _environment(tmp_path, profile),
            exchange=sockets.exchange,
            socket_validator=sockets.validate,
        )
    assert sockets.actions == []

    invalid = _profile(tmp_path / "observer.sock")
    invalid["account_id_sha256"] = "e" * 64
    profile = _write_profile(tmp_path, invalid)
    with pytest.raises(driver.DriverError, match="account"):
        driver.run_driver(
            _request(profile),
            _environment(tmp_path, profile),
            exchange=sockets.exchange,
            socket_validator=sockets.validate,
        )


@pytest.mark.parametrize("mode", ["missing", "forged", "profile"])
def test_missing_or_forged_observer_receipt_fails(tmp_path: Path, mode: str) -> None:
    profile_value = _profile(tmp_path / "observer.sock")
    if mode == "profile":
        profile_value["observer_profile_sha256"] = _hash("configured-profile")
    profile = _write_profile(tmp_path, profile_value)
    sockets = _FakeEvidenceSockets(tmp_path)
    if mode == "missing":
        sockets.observer_status = "not_found"
    elif mode == "forged":
        sockets.forge_expected = True

    with pytest.raises(driver.DriverError, match="receipt"):
        driver.run_driver(
            _request(profile),
            _environment(tmp_path, profile),
            exchange=sockets.exchange,
            socket_validator=sockets.validate,
        )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.update({"secret_file": "must-not-be-read"}),
        lambda value: value.update({"channel": "wecom"}),
        lambda value: value["broker_actions"].update({"round_trip": "arbitrary"}),
        lambda value: value.update({"schema_version": True}),
    ],
)
def test_profile_schema_is_strict_and_rejects_secret_or_unknown_fields(
    tmp_path: Path,
    mutation: Any,
) -> None:
    value = _profile(tmp_path / "observer.sock")
    mutation(value)
    profile = _write_profile(tmp_path, value)
    sockets = _FakeEvidenceSockets(tmp_path)

    with pytest.raises(driver.DriverError, match="profile"):
        driver.run_driver(
            _request(profile),
            _environment(tmp_path, profile),
            exchange=sockets.exchange,
            socket_validator=sockets.validate,
        )


def test_request_broker_schema_and_response_size_are_strict(tmp_path: Path) -> None:
    profile = _write_profile(tmp_path)
    sockets = _FakeEvidenceSockets(tmp_path)
    request = _request(profile)
    request["extra"] = "forbidden"
    with pytest.raises(driver.DriverError, match="request"):
        driver.run_driver(
            request,
            _environment(tmp_path, profile),
            exchange=sockets.exchange,
            socket_validator=sockets.validate,
        )


@pytest.mark.parametrize(
    ("mode", "message"),
    [
        ("missing", "receipt"),
        ("bad_ack", "acknowledgement"),
        ("bad_provider_code", "acknowledgement"),
        ("no_request_id", "request ID"),
        ("bad_drop", "response-drop"),
        ("rate_missing_limited", "rate-limit"),
        ("hash_mismatch", "receipt"),
    ],
)
def test_openapi_witness_must_independently_prove_ack_retry_and_drop(
    tmp_path: Path,
    mode: str,
    message: str,
) -> None:
    profile = _write_profile(tmp_path)
    sockets = _FakeEvidenceSockets(tmp_path)
    sockets.witness_failure = mode

    with pytest.raises(driver.DriverError, match=message):
        driver.run_driver(
            _request(profile),
            _environment(tmp_path, profile),
            exchange=sockets.exchange,
            socket_validator=sockets.validate,
        )

    sockets.extra_broker_field = True
    with pytest.raises(driver.DriverError, match="broker"):
        driver.run_driver(
            _request(profile),
            _environment(tmp_path, profile),
            exchange=sockets.exchange,
            socket_validator=sockets.validate,
        )

    profile.write_bytes(b"x" * (driver.MAX_PROFILE_BYTES + 1))
    with pytest.raises(driver.DriverError, match="size"):
        driver.run_driver(
            _request(profile),
            _environment(tmp_path, profile),
            exchange=sockets.exchange,
            socket_validator=sockets.validate,
        )

    def oversized(_path: Path, _payload: bytes, _timeout: float, limit: int) -> bytes:
        return b"x" * (limit + 1)

    profile = _write_profile(tmp_path)
    with pytest.raises(driver.DriverError, match="large"):
        driver.run_driver(
            _request(profile),
            _environment(tmp_path, profile),
            exchange=oversized,
            socket_validator=sockets.validate,
        )


def test_credential_rotation_requires_independent_openapi_ack(tmp_path: Path) -> None:
    profile = _write_profile(tmp_path)
    sockets = _FakeEvidenceSockets(tmp_path)
    sockets.witness_failure = "bad_ack"
    sockets.witness_failure_case = "credential_rotation"

    with pytest.raises(driver.DriverError, match="acknowledgement"):
        driver.run_driver(
            _request(profile),
            _environment(tmp_path, profile),
            exchange=sockets.exchange,
            socket_validator=sockets.validate,
        )
    assert any(
        request["after_sequence"]
        == next(
            sequence
            for sequence, receipts in sockets.witness_receipts.items()
            if any(
                receipt["provider_acknowledged"] is False and receipt["provider_status"] == 200
                for receipt in receipts
            )
        )
        for request in sockets.witness_queries
    )


def test_prolonged_outage_requires_independent_openapi_ack_even_when_admin_delivered(
    tmp_path: Path,
) -> None:
    profile = _write_profile(tmp_path)
    sockets = _FakeEvidenceSockets(tmp_path)
    sockets.witness_failure = "missing"
    sockets.witness_failure_case = "prolonged_outage"

    with pytest.raises(driver.DriverError, match="witness receipt is missing"):
        driver.run_driver(
            _request(profile),
            _environment(tmp_path, profile),
            exchange=sockets.exchange,
            socket_validator=sockets.validate,
        )
    assert any(
        request["after_sequence"] in sockets.witness_receipts
        and sockets.witness_receipts[request["after_sequence"]] == []
        for request in sockets.witness_queries
    )


def test_check_validates_profile_and_both_sockets_without_invoking_broker(
    tmp_path: Path,
) -> None:
    profile = _write_profile(tmp_path)
    sockets = _FakeEvidenceSockets(tmp_path)
    calls: list[str] = []

    def validate(raw: str, label: str) -> Path:
        calls.append(label)
        return sockets.validate(raw, label)

    assert driver.check_configuration(
        _environment(tmp_path, profile), socket_validator=validate
    ) == {"status": "ready"}
    assert calls == ["broker socket", "observer socket", "OpenAPI witness socket"]
    assert sockets.actions == []


def test_main_failure_is_content_free(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    def fail() -> None:
        raise RuntimeError

    monkeypatch.setattr(driver, "_run_from_environment", fail)
    assert driver.main([]) != 0
    captured = capsys.readouterr()
    assert captured.out == '{"status":"not_run"}\n'
    assert captured.err == ""


def test_broker_peer_is_verified_on_the_same_socket_before_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent = False

    class FakeConnection:
        def settimeout(self, _timeout: float) -> None:
            return None

        def connect(self, _path: str) -> None:
            return None

        def getsockopt(self, *_args: object) -> bytes:
            return struct.pack("3i", 99, 1001, 1002)

        def sendall(self, _payload: bytes) -> None:
            nonlocal sent
            sent = True

        def close(self) -> None:
            return None

    monkeypatch.setattr(driver.sys, "platform", "linux")
    monkeypatch.setattr(driver, "_AF_UNIX", 1)
    monkeypatch.setattr(driver.socket, "SO_PEERCRED", 17, raising=False)
    monkeypatch.setattr(driver.socket, "socket", lambda *_args: FakeConnection())

    with pytest.raises(driver.DriverError, match="identity does not match"):
        driver._authenticated_broker_exchange(
            Path("/run/trpc-im-probe/control.sock"),
            b"{}",
            1.0,
            1024,
            1001,
            9999,
        )
    assert sent is False
