"""Connection probe never emits secrets, signed connection URLs or API messages."""

import logging

import httpx
import pytest

from trpc_service.channels.feishu_probe import authentication, ProbeLogHandler


@pytest.mark.parametrize("code", [0, 10014])
def test_authentication_returns_only_safe_status(code):

    def handle(request):
        assert request.url.host == "open.feishu.cn"
        return httpx.Response(200,
                              json={
                                  "code": code,
                                  "msg": "private-server-body",
                                  "tenant_access_token": "private-token"
                              })

    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        result = authentication("cli_synthetic", "synthetic-secret", client)
    assert result["status"] == ("succeeded" if code == 0 else "rejected")
    assert "private" not in repr(result) and "synthetic-secret" not in repr(result)


def test_sdk_log_observation_only_exports_connection_and_pong():
    events = []
    handler = ProbeLogHandler(events.append)
    for function, message in [("_connect", "connected to wss://example.invalid/?secret=private"),
                              ("_handle_data_frame", "message body private"), ("_handle_control_frame", "receive pong"),
                              ("_handle_control_frame", "receive pong")]:
        handler.emit(logging.LogRecord("Lark", logging.DEBUG, "test", 1, message, (), None, func=function))
    assert events == [{"stage": "websocket", "status": "connected"}, {"stage": "heartbeat", "status": "pong_received"}]
    assert "private" not in repr(events)
