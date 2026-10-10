from __future__ import annotations

import hashlib
import http.client
import json
from pathlib import Path
from typing import Any

import pytest

from deploy.im_probe import feishu_control_action as action

RUN_NONCE = "feishu-action-nonce-123456"
IMAGE_DIGEST = "sha256:" + "a" * 64
TENANT_ID = "tenant-feishu"
BINDING_ID = "feishu-binding"


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _opaque(value: str) -> str:
    return "imref-" + _hash(value)


def _write_private(path: Path, contents: str) -> Path:
    path.write_text(contents, encoding="utf-8")
    path.chmod(0o600)
    return path


def _config(tmp_path: Path, *, actions: tuple[str, ...] = ("feishu_round_trip",)) -> Path:
    control_token_file = _write_private(
        tmp_path / "control-token", "private-control-token-123456\n"
    )
    evidence_token_file = _write_private(
        tmp_path / "evidence-token", "private-evidence-token-123456\n"
    )
    value = {
        "schema_version": 1,
        "channel": "feishu",
        "tenant_id": TENANT_ID,
        "binding_id": BINDING_ID,
        "control_token_file": str(control_token_file),
        "evidence_base_url": "https://ack-admin.internal",
        "evidence_token_file": str(evidence_token_file),
        "hooks": {
            name: {
                "url": f"https://ack-control.internal/v1/im/feishu/{name.removeprefix('feishu_')}",
                "timeout_seconds": 30,
            }
            for name in actions
        },
    }
    return _write_private(tmp_path / "control-action.json", json.dumps(value))


def _request(case: str = "round_trip") -> dict[str, object]:
    return {
        "schema_version": 1,
        "channel": "feishu",
        "action": f"feishu_{case}",
        "run_id": "feishu-live-run",
        "run_nonce": RUN_NONCE,
        "control_profile_sha256": _hash("control-profile"),
        "payload": {
            "case": case,
            "expected_image_digest": IMAGE_DIGEST,
            "account_id_sha256": _hash("account"),
            "observer_profile_sha256": _hash("observer-profile"),
        },
    }


def _pass_response(case: str = "round_trip") -> bytes:
    common: dict[str, object] = {
        "status": "pass",
        "run_nonce": RUN_NONCE,
        "provider_event_id": _opaque(f"provider-{case}"),
        "observed_at": "2026-09-01T05:00:00+00:00",
    }
    values: dict[str, dict[str, object]] = {
        "round_trip": {
            "callback_event_id": _opaque("callback"),
            "outbound_request_id": _opaque("outbound"),
            "provider_code": 0,
        },
        "idempotency": {
            "duplicate_event_id": _opaque("duplicate"),
            "unique_inbound_id": _opaque("unique-inbound"),
            "duplicate_count": 1,
            "original_event_id": _opaque("duplicate"),
            "provider_delivery_count": 2,
        },
        "media": {
            "media_id_hash": "b" * 64,
            "sha256": "c" * 64,
            "bytes": 16,
        },
        "reconnect": {
            "failed_endpoint_id": _opaque("failed-endpoint"),
            "replacement_endpoint_id": _opaque("replacement-endpoint"),
            "endpoint_set_observed": True,
            "received_after_failover_event_id": _opaque("failover-event"),
            "outbound_request_id": _opaque("failover-outbound"),
            "acknowledged_request_id": _opaque("failover-outbound"),
            "ready_endpoint_count": 1,
            "unready_endpoint_count": 0,
            "terminating_endpoint_count": 0,
        },
        "rate_limit_retry_after": {
            "provider_error_code": 99991400,
            "retry_after_seconds": 1.0,
            "retry_request_id": _opaque("retry-request"),
            "retry_attempts": 2,
            "retry_elapsed_seconds": 1.0,
        },
        "credential_rotation": {
            "old_credential_event_id": _opaque("old-credential"),
            "new_credential_event_id": _opaque("new-credential"),
            "post_rotation_event_id": _opaque("post-rotation"),
            "old_credential_rejected": True,
        },
        "prolonged_outage": {
            "outage_event_id": _opaque("outage"),
            "recovery_event_id": _opaque("recovery"),
            "outage_seconds": 60.0,
        },
        "ambiguous": {
            "ambiguous_event_id": _opaque("ambiguous"),
            "manual_review_id": _opaque("manual-review"),
            "drop_response_observed": True,
            "auto_replay_count": 0,
        },
    }
    evidence = {
        "observation": {**common, **values[case]},
        "callback_query": {
            "marker_sha256": _hash("marker"),
            "profile_sha256": _hash("observer-profile"),
        },
        "callback_expected": {
            "event_id_sha256": _hash("event"),
            "message_id_sha256": _hash("provider-event"),
        },
    }
    if case in action.ACK_CASES:
        evidence["openapi_witness"] = {
            "after_sequence": 10,
            "path_sha256": _hash("path"),
            "body_sha256": _hash("body"),
        }
    return json.dumps(
        {
            "schema_version": 1,
            "status": "pass",
            "provider_event_hash": _hash("provider-event"),
            "evidence": evidence,
        },
        separators=(",", ":"),
    ).encode()


def _acceptance_run_id(case: str = "round_trip") -> str:
    return (
        "im-"
        + hashlib.sha256(
            "\0".join(
                (
                    "trpc-im-acceptance-run-v1",
                    "feishu",
                    TENANT_ID,
                    BINDING_ID,
                    "feishu-live-run",
                    case,
                )
            ).encode()
        ).hexdigest()
    )


def test_each_case_uses_an_independent_acceptance_run() -> None:
    config = action.ControlConfig(
        tenant_id=TENANT_ID,
        binding_id=BINDING_ID,
        control_token_file=Path("control-token"),
        evidence_base_url="https://ack-admin.internal",
        evidence_token_file=Path("evidence-token"),
        hooks={},
    )
    run_ids = {
        action._acceptance_run_id(config, _request(case)) for case in action.ACTION_CASES.values()
    }

    assert len(run_ids) == len(action.ACTION_CASES)


def _run_hash(case: str = "round_trip") -> str:
    return hashlib.sha256(
        "\0".join(
            (
                "trpc-im-acceptance-evidence-v1",
                "run",
                TENANT_ID,
                BINDING_ID,
                _acceptance_run_id(case),
            )
        ).encode()
    ).hexdigest()


def _run_binding_hash(case: str = "round_trip") -> str:
    run_nonce_hash = hashlib.sha256(
        "\0".join(
            (
                "trpc-im-acceptance-evidence-v1",
                "run-nonce",
                TENANT_ID,
                BINDING_ID,
                RUN_NONCE,
            )
        ).encode()
    ).hexdigest()
    return hashlib.sha256(
        "\0".join(
            (
                "trpc-im-acceptance-evidence-v1",
                "run-binding",
                TENANT_ID,
                BINDING_ID,
                "feishu",
                _run_hash(case),
                run_nonce_hash,
            )
        ).encode()
    ).hexdigest()


def _registration_response(case: str = "round_trip") -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "tenant_id": TENANT_ID,
            "binding_id": BINDING_ID,
            "channel": "feishu",
            "run_id_sha256": _run_hash(case),
            "run_binding_sha256": _run_binding_hash(case),
            "created_at": "2026-09-01T04:59:59+00:00",
            "expires_at": "2026-09-01T05:04:59+00:00",
        },
        separators=(",", ":"),
    ).encode()


def _case_invoker(
    case: str = "round_trip",
    *,
    control_response: bytes | None = None,
    ack_response: bytes | None = None,
) -> action.HookInvoker:
    def invoke(hook: action.HookConfig, _token: str, _payload: bytes) -> bytes:
        if hook.action == "feishu_run_registration":
            return _registration_response(case)
        if hook.action == "feishu_event_evidence":
            return ack_response if ack_response is not None else _ack_response(case)
        return control_response if control_response is not None else _pass_response(case)

    return invoke


def _ack_response(case: str = "round_trip") -> bytes:
    status = "ambiguous" if case == "ambiguous" else "delivered"
    attempts = [
        {
            "attempt_number": 1,
            "status": status,
            "provider_code": "0",
            "retry_after_seconds": None,
            "started_at": "2026-09-01T05:00:00+00:00",
            "completed_at": "2026-09-01T05:00:01+00:00",
        }
    ]
    if case == "rate_limit_retry_after":
        attempts = [
            {
                "attempt_number": 1,
                "status": "failed",
                "provider_code": "99991400",
                "retry_after_seconds": 1.0,
                "started_at": "2026-09-01T05:00:00+00:00",
                "completed_at": "2026-09-01T05:00:00+00:00",
            },
            {
                "attempt_number": 2,
                "status": "delivered",
                "provider_code": "0",
                "retry_after_seconds": None,
                "started_at": "2026-09-01T05:00:01+00:00",
                "completed_at": "2026-09-01T05:00:01+00:00",
            },
        ]
    artifact_items = (
        [
            {
                "sha256": "c" * 64,
                "bytes": 16,
                "status": "available",
                "created_at": "2026-09-01T05:00:01+00:00",
            }
        ]
        if case == "media"
        else []
    )
    value = {
        "schema_version": 1,
        "tenant_id": TENANT_ID,
        "binding_id": BINDING_ID,
        "channel": "feishu",
        "requested_run_id_sha256": _run_hash(case),
        "run_binding_sha256": _run_binding_hash(case),
        "provider_event_hash": _hash("provider-event"),
        "correlation": {
            "availability": "available",
            "inbound_id_sha256": _hash("inbound"),
            "status": "committed",
            "delivery_count": 2 if case == "idempotency" else 1,
            "accepted_at": "2026-09-01T05:00:00+00:00",
        },
        "outbounds": {
            "count": 1,
            "truncated": False,
            "items": [
                {
                    "outbound_id_sha256": _hash("outbound"),
                    "delivery_status": status,
                    "provider_message_id_sha256": _hash("provider-message"),
                    "attempt_count": len(attempts),
                    "attempts": attempts,
                    "pending_count": 0,
                    "dlq_count": 0,
                    "created_at": "2026-09-01T05:00:00+00:00",
                    "updated_at": "2026-09-01T05:00:01+00:00",
                }
            ],
        },
        "artifact": {
            "availability": "available" if artifact_items else "not_found",
            "count": len(artifact_items),
            "items": artifact_items,
        },
    }
    return json.dumps(value, separators=(",", ":")).encode()


def test_action_calls_only_configured_https_hook_and_returns_opaque_evidence(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    calls: list[tuple[action.HookConfig, str, bytes]] = []

    def invoke(hook: action.HookConfig, token: str, payload: bytes) -> bytes:
        calls.append((hook, token, payload))
        if hook.action == "feishu_run_registration":
            return _registration_response()
        return _ack_response() if hook.action == "feishu_event_evidence" else _pass_response()

    result = action.run_action(
        _request(),
        action_name="feishu_round_trip",
        config_path=config,
        invoke=invoke,
    )

    assert set(result) == {
        "observation",
        "callback_query",
        "callback_expected",
        "openapi_witness",
    }
    assert len(calls) == 3
    registration_hook, registration_token, registration_payload = calls[0]
    assert registration_hook.url == (
        "https://ack-admin.internal/v1/tenants/tenant-feishu/bindings/feishu-binding/"
        "im-acceptance/runs"
    )
    assert registration_token == "private-evidence-token-123456"
    assert json.loads(registration_payload) == {
        "channel": "feishu",
        "run_id": _acceptance_run_id(),
        "run_nonce": RUN_NONCE,
        "expires_in_seconds": 300,
    }
    hook, token, payload = calls[1]
    assert hook.url == "https://ack-control.internal/v1/im/feishu/round_trip"
    assert token == "private-control-token-123456"
    assert json.loads(payload) == _request()
    evidence_hook, evidence_token, evidence_payload = calls[2]
    assert evidence_hook.url == (
        "https://ack-admin.internal/v1/tenants/tenant-feishu/bindings/feishu-binding/"
        "im-acceptance/event-evidence"
    )
    assert evidence_token == "private-evidence-token-123456"
    assert json.loads(evidence_payload) == {
        "channel": "feishu",
        "run_id": _acceptance_run_id(),
        "run_nonce": RUN_NONCE,
        "provider_event_hash": _hash("provider-event"),
    }
    rendered = json.dumps(result, sort_keys=True)
    assert token not in rendered
    assert "provider-round_trip" not in rendered


def test_https_invoker_posts_bearer_request_to_the_exact_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {}

    class Response:
        status = 200

        def read(self, _limit: int) -> bytes:
            return b'{"schema_version":1,"status":"not_run","error_code":"not_ready"}'

        def getheader(self, name: str, default: str) -> str:
            assert name == "Content-Type"
            return "application/json" if default == "" else default

    class Connection:
        def __init__(self, host: str, port: int, **kwargs: object) -> None:
            calls["connect"] = (host, port, kwargs)

        def request(
            self,
            method: str,
            path: str,
            *,
            body: bytes,
            headers: dict[str, str],
        ) -> None:
            calls["request"] = (method, path, body, headers)

        def getresponse(self) -> Response:
            return Response()

        def close(self) -> None:
            calls["closed"] = True

    monkeypatch.setattr(http.client, "HTTPSConnection", Connection)
    hook = action._parse_hook(
        "feishu_round_trip",
        {
            "url": "https://ack-control.internal/v1/im/feishu/round_trip",
            "timeout_seconds": 30,
        },
    )

    raw = action._invoke_https_hook(hook, "private-token-123456", b"{}")

    assert json.loads(raw) == {
        "schema_version": 1,
        "status": "not_run",
        "error_code": "not_ready",
    }
    host, port, kwargs = calls["connect"]
    assert (host, port) == ("ack-control.internal", 443)
    assert kwargs["timeout"] == 30
    method, path, body, headers = calls["request"]
    assert (method, path, body) == ("POST", "/v1/im/feishu/round_trip", b"{}")
    assert headers["Authorization"] == "Bearer private-token-123456"
    assert headers["X-TRPC-IM-Action"] == "feishu_round_trip"
    assert calls["closed"] is True


def test_missing_hook_and_external_not_run_fail_closed(tmp_path: Path) -> None:
    config = _config(tmp_path, actions=())

    def must_not_call(_hook: action.HookConfig, _token: str, _payload: bytes) -> bytes:
        raise AssertionError("external hook must not be called")

    assert action.run_action(
        _request(),
        action_name="feishu_round_trip",
        config_path=config,
        invoke=must_not_call,
    ) == {"schema_version": 1, "status": "not_run", "error_code": "hook_not_configured"}

    config = _config(tmp_path)
    assert action.run_action(
        _request(),
        action_name="feishu_round_trip",
        config_path=config,
        invoke=_case_invoker(
            control_response=(
                b'{"schema_version":1,"status":"not_run","error_code":"private-detail"}'
            )
        ),
    ) == {"schema_version": 1, "status": "not_run", "error_code": "external_not_run"}


@pytest.mark.parametrize("mode", ["missing_witness", "raw_identifier", "extra_secret"])
def test_incomplete_or_sensitive_evidence_is_never_forwarded(
    tmp_path: Path,
    mode: str,
) -> None:
    config = _config(tmp_path)
    response = json.loads(_pass_response())
    evidence = response["evidence"]
    if mode == "missing_witness":
        evidence.pop("openapi_witness")
    elif mode == "raw_identifier":
        evidence["observation"]["provider_event_id"] = "raw-provider-id-must-not-leak"
    else:
        evidence["secret"] = "must-not-leak"

    result = action.run_action(
        _request(),
        action_name="feishu_round_trip",
        config_path=config,
        invoke=_case_invoker(control_response=json.dumps(response).encode()),
    )

    assert result["status"] == "not_run"
    assert result["error_code"] in {"evidence_incomplete", "evidence_invalid"}
    rendered = json.dumps(result)
    assert "raw-provider-id" not in rendered
    assert "must-not-leak" not in rendered


def test_prolonged_outage_cannot_pass_on_admin_delivery_without_openapi_witness(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, actions=("feishu_prolonged_outage",))
    response = json.loads(_pass_response("prolonged_outage"))
    response["evidence"].pop("openapi_witness", None)

    result = action.run_action(
        _request("prolonged_outage"),
        action_name="feishu_prolonged_outage",
        config_path=config,
        invoke=_case_invoker("prolonged_outage", control_response=json.dumps(response).encode()),
    )

    assert result == {
        "schema_version": 1,
        "status": "not_run",
        "error_code": "evidence_incomplete",
    }


def test_request_and_private_config_are_strict(tmp_path: Path) -> None:
    config = _config(tmp_path)
    request = _request()
    request["token"] = "must-not-be-accepted"
    assert (
        action.run_action(
            request,
            action_name="feishu_round_trip",
            config_path=config,
            invoke=lambda *_args: _pass_response(),
        )["error_code"]
        == "input_invalid"
    )

    value = json.loads(config.read_text(encoding="utf-8"))
    value["token"] = "must-not-be-accepted"
    config.write_text(json.dumps(value), encoding="utf-8")
    config.chmod(0o600)
    result = action.run_action(
        _request(),
        action_name="feishu_round_trip",
        config_path=config,
        invoke=lambda *_args: _pass_response(),
    )
    assert result == {"schema_version": 1, "status": "not_run", "error_code": "config_invalid"}


@pytest.mark.parametrize(
    "url",
    [
        "http://ack-control.internal/v1/im/feishu/round_trip",
        "https://ack-control.internal/v1/im/feishu/wrong_action",
        "https://ack-control.internal/v1/im/feishu/round_trip?redirect=1",
    ],
)
def test_hook_url_is_https_and_action_specific(tmp_path: Path, url: str) -> None:
    config = _config(tmp_path)
    value = json.loads(config.read_text(encoding="utf-8"))
    value["hooks"]["feishu_round_trip"]["url"] = url
    config.write_text(json.dumps(value), encoding="utf-8")
    config.chmod(0o600)

    result = action.run_action(
        _request(),
        action_name="feishu_round_trip",
        config_path=config,
        invoke=lambda *_args: pytest.fail("invalid hook must not be called"),
    )

    assert result == {"schema_version": 1, "status": "not_run", "error_code": "config_invalid"}


def test_hook_cannot_echo_the_private_token_as_evidence(tmp_path: Path) -> None:
    config = _config(tmp_path)
    value = json.loads(config.read_text(encoding="utf-8"))
    token = "d" * 64
    token_file = Path(value["control_token_file"])
    token_file.write_text(token + "\n", encoding="utf-8")
    token_file.chmod(0o600)
    response = json.loads(_pass_response())
    response["evidence"]["openapi_witness"]["body_sha256"] = token

    result = action.run_action(
        _request(),
        action_name="feishu_round_trip",
        config_path=config,
        invoke=_case_invoker(control_response=json.dumps(response).encode()),
    )

    assert result == {"schema_version": 1, "status": "not_run", "error_code": "evidence_invalid"}
    assert token not in json.dumps(result)


def test_rotation_requires_independent_ack_binding_from_control_hook(tmp_path: Path) -> None:
    config = _config(tmp_path, actions=("feishu_credential_rotation",))
    result = action.run_action(
        _request("credential_rotation"),
        action_name="feishu_credential_rotation",
        config_path=config,
        invoke=_case_invoker("credential_rotation"),
    )

    assert set(result) == {
        "observation",
        "callback_query",
        "callback_expected",
        "openapi_witness",
    }


def test_hook_provider_event_hash_must_match_callback_message(tmp_path: Path) -> None:
    config = _config(tmp_path)
    response = json.loads(_pass_response())
    response["provider_event_hash"] = "e" * 64

    result = action.run_action(
        _request(),
        action_name="feishu_round_trip",
        config_path=config,
        invoke=_case_invoker(control_response=json.dumps(response).encode()),
    )

    assert result == {
        "schema_version": 1,
        "status": "not_run",
        "error_code": "evidence_invalid",
    }


@pytest.mark.parametrize("case", tuple(action.ACTION_CASES.values()))
def test_all_cases_require_deterministic_ack_event_evidence(
    tmp_path: Path,
    case: str,
) -> None:
    action_name = f"feishu_{case}"
    config = _config(tmp_path, actions=(action_name,))

    result = action.run_action(
        _request(case),
        action_name=action_name,
        config_path=config,
        invoke=_case_invoker(case),
    )

    assert "status" not in result


@pytest.mark.parametrize(
    ("case", "mode"),
    [
        ("idempotency", "wrong_delivery_count"),
        ("media", "missing_artifact"),
        ("reconnect", "missing_outbound"),
        ("rate_limit_retry_after", "wrong_retry_after"),
        ("credential_rotation", "pending_outbound"),
        ("prolonged_outage", "missing_outbound"),
        ("ambiguous", "delivered_instead_of_ambiguous"),
    ],
)
def test_case_specific_ack_evidence_must_match_control_observation(
    tmp_path: Path,
    case: str,
    mode: str,
) -> None:
    action_name = f"feishu_{case}"
    config = _config(tmp_path, actions=(action_name,))
    ack = json.loads(_ack_response(case))
    if mode == "wrong_delivery_count":
        ack["correlation"]["delivery_count"] = 1
    elif mode == "missing_artifact":
        ack["artifact"] = {"availability": "not_found", "count": 0, "items": []}
    elif mode == "missing_outbound":
        ack["outbounds"] = {"count": 0, "truncated": False, "items": []}
    elif mode == "wrong_retry_after":
        ack["outbounds"]["items"][0]["attempts"][0]["retry_after_seconds"] = 2.0
    elif mode == "pending_outbound":
        ack["outbounds"]["items"][0]["pending_count"] = 1
    else:
        ack["outbounds"]["items"][0]["delivery_status"] = "delivered"

    result = action.run_action(
        _request(case),
        action_name=action_name,
        config_path=config,
        invoke=_case_invoker(case, ack_response=json.dumps(ack).encode()),
    )

    assert result == {
        "schema_version": 1,
        "status": "not_run",
        "error_code": "ack_evidence_mismatch",
    }


def test_check_requires_private_token_and_all_fixed_hooks(tmp_path: Path) -> None:
    partial = _config(tmp_path)
    assert action.check_configuration(partial) is False

    complete = _config(tmp_path, actions=tuple(action.ACTION_CASES))
    assert action.check_configuration(complete) is True

    value = json.loads(complete.read_text(encoding="utf-8"))
    Path(value["control_token_file"]).write_text("too-short\n", encoding="utf-8")
    assert action.check_configuration(complete) is False


@pytest.mark.parametrize("mode", ["not_found", "hash_mismatch", "raw_outbound_uuid"])
def test_ack_event_evidence_is_required_and_content_free(
    tmp_path: Path,
    mode: str,
) -> None:
    config = _config(tmp_path)
    ack = json.loads(_ack_response())
    if mode == "not_found":
        ack["correlation"] = {"availability": "not_found"}
        ack["outbounds"] = {"count": 0, "truncated": False, "items": []}
    elif mode == "hash_mismatch":
        ack["provider_event_hash"] = "e" * 64
    else:
        ack["outbounds"]["items"][0]["outbound_id"] = "raw-uuid-must-not-pass"

    result = action.run_action(
        _request(),
        action_name="feishu_round_trip",
        config_path=config,
        invoke=_case_invoker(ack_response=json.dumps(ack).encode()),
    )

    assert result["status"] == "not_run"
    assert result["error_code"] in {
        "ack_evidence_unavailable",
        "ack_evidence_invalid",
        "ack_evidence_mismatch",
    }
    assert "raw-uuid" not in json.dumps(result)


def test_action_requires_root_owned_private_path_chain_on_posix() -> None:
    source = Path(action.__file__).read_text(encoding="utf-8")
    assert "metadata.st_uid != 0" in source
    assert "_validate_root_owned_parent_chain(path)" in source
