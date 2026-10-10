from __future__ import annotations

import json
from collections.abc import Mapping

import pytest

from deploy.im_probe import feishu_openapi_witness as witness_module


def _response(
    *,
    status: int = 200,
    code: int = 0,
    headers: dict[str, str] | None = None,
) -> witness_module.ForwardedResponse:
    return witness_module.ForwardedResponse(
        status=status,
        headers=headers or {"X-Request-Id": "provider-request-1"},
        body=json.dumps({"code": code, "msg": "ok"}).encode(),
    )


def test_send_ack_is_forwarded_and_recorded_without_content() -> None:
    observed: list[tuple[str, dict[str, str], bytes]] = []

    def forward(
        path: str, headers: Mapping[str, str], body: bytes
    ) -> witness_module.ForwardedResponse:
        observed.append((path, dict(headers), body))
        return _response()

    target = witness_module.FeishuOpenAPIWitness(forward)
    response, dropped = target.forward(
        "/open-apis/im/v1/messages/message-1/reply",
        {"Authorization": "Bearer secret", "Content-Type": "application/json"},
        b'{"content":"private"}',
    )

    assert response.status == 200 and not dropped
    assert observed and observed[0][2] == b'{"content":"private"}'
    receipts = target.query(0, 10)
    assert receipts[0]["provider_acknowledged"] is True
    assert receipts[0]["provider_code"] == 0
    serialized = json.dumps(receipts)
    assert "private" not in serialized
    assert "secret" not in serialized
    assert "provider-request-1" not in serialized


def test_token_response_is_never_retained() -> None:
    target = witness_module.FeishuOpenAPIWitness(lambda *_args: _response())
    target.forward(witness_module.AUTH_PATH, {}, b'{"app_secret":"secret"}')
    assert target.query(0, 10) == []


def test_drop_happens_only_after_real_provider_ack() -> None:
    now = [100.0]
    responses = iter([_response(code=99991400), _response()])
    target = witness_module.FeishuOpenAPIWitness(
        lambda *_args: next(responses),
        clock=lambda: now[0],
    )
    target.arm_drop_next_ack("n" * 20, 10)
    _, first_dropped = target.forward(MESSAGE_PATH, {}, b"{}")
    assert not first_dropped

    target.arm_drop_next_ack("m" * 20, 10)
    _, second_dropped = target.forward(MESSAGE_PATH, {}, b"{}")
    assert second_dropped
    receipts = target.query(0, 10)
    assert receipts[-1]["downstream_response_dropped"] is True
    assert receipts[-1]["provider_acknowledged"] is True


def test_drop_rule_expires_and_is_one_shot() -> None:
    now = [100.0]
    target = witness_module.FeishuOpenAPIWitness(lambda *_args: _response(), clock=lambda: now[0])
    target.arm_drop_next_ack("n" * 20, 1)
    now[0] = 102.0
    _, dropped = target.forward(MESSAGE_PATH, {}, b"{}")
    assert not dropped
    _, second = target.forward(MESSAGE_PATH, {}, b"{}")
    assert not second


def test_rate_limit_receipt_preserves_only_code_and_retry_after() -> None:
    target = witness_module.FeishuOpenAPIWitness(
        lambda *_args: _response(
            status=429,
            code=99991400,
            headers={"Retry-After": "2.5", "X-Tt-Logid": "private-request"},
        )
    )
    target.forward(MESSAGE_PATH, {}, b"{}")
    receipt = target.query(0, 1)[0]
    assert receipt["provider_status"] == 429
    assert receipt["provider_code"] == 99991400
    assert receipt["retry_after_seconds"] == 2.5
    assert receipt["provider_acknowledged"] is False
    assert "private-request" not in json.dumps(receipt)


def test_control_protocol_is_strict_and_content_free() -> None:
    target = witness_module.FeishuOpenAPIWitness(lambda *_args: _response())
    arm = witness_module.control_request(
        json.dumps(
            {
                "schema_version": 1,
                "action": "arm_drop_next_ack",
                "run_nonce": "n" * 20,
                "ttl_seconds": 5,
            }
        ).encode(),
        target,
    )
    assert json.loads(arm) == {"status": "pass"}
    assert json.loads(witness_module.control_request(b'{"schema_version":1}', target)) == {
        "error_code": "request_invalid",
        "status": "not_run",
    }


def test_query_is_bounded_and_rejects_bool_integers() -> None:
    target = witness_module.FeishuOpenAPIWitness(lambda *_args: _response())
    target.forward(MESSAGE_PATH, {}, b"one")
    target.forward(MESSAGE_PATH, {}, b"two")
    assert len(target.query(0, 1)) == 1
    with pytest.raises(witness_module.WitnessError):
        target.query(True, 1)
    with pytest.raises(witness_module.WitnessError):
        target.query(0, True)


def test_disallowed_path_and_oversized_payload_fail_closed() -> None:
    target = witness_module.FeishuOpenAPIWitness(lambda *_args: _response())
    with pytest.raises(witness_module.WitnessError):
        target.forward("/open-apis/contact/v3/users", {}, b"{}")
    with pytest.raises(witness_module.WitnessError):
        target.forward(MESSAGE_PATH, {}, b"x" * (witness_module.MAX_REQUEST_BYTES + 1))


def test_check_validates_both_listener_boundaries(monkeypatch, tmp_path, capsys) -> None:
    closed: list[str] = []

    class Server:
        def server_close(self) -> None:
            closed.append("closed")

    monkeypatch.setattr(witness_module, "create_http_server", lambda *_args: Server())
    monkeypatch.setattr(witness_module, "create_control_server", lambda *_args: Server())
    socket_path = tmp_path / "witness.sock"

    assert witness_module.main(["--check", "--control-socket", str(socket_path)]) == 0
    assert closed == ["closed", "closed"]
    assert capsys.readouterr().out.strip() == '{"status":"ready"}'


MESSAGE_PATH = "/open-apis/im/v1/messages/message-1/reply"
