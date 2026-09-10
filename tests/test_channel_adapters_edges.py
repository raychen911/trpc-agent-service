"""Provider protocol edge cases that do not require network credentials."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest

from trpc_service.channels.base import (
    Attachment,
    ChannelVerificationError,
    OutboundMessage,
    hmac_signature,
    normalize_attachment,
    parse_webhook_body,
    revoke_target_message_id,
    sanitize_event_metadata,
    sanitize_event_payload,
)
from trpc_service.channels.media import attachment_kind, post_multipart_json, prepare_attachment_file
from trpc_service.channels.wechat_crypto import verify_handshake
from trpc_service.channels.wechat_official_account import WeChatOfficialAccountAdapter
from trpc_service.channels.wecom import WeComAdapter
from trpc_service.tenant.models import ChannelBinding


def binding(channel="wecom", **config):
    return ChannelBinding("tenant-a", "binding-1", channel, "account-1", "app-1", config=config)


def test_base_protocol_sanitizes_nested_events_and_parses_formats():
    assert hmac_signature("secret", "body")
    assert revoke_target_message_id({"MsgId": "m-1"}) == "m-1"
    assert revoke_target_message_id({}) is None
    normalized = normalize_attachment({"type": "image", "file_id": "f-1", "content_base64": "ZGF0YQ=="})
    assert normalized.kind == "image" and normalized.metadata["file_id"] == "f-1"
    with pytest.raises(ValueError):
        normalize_attachment({"content_base64": "not base64"})
    with pytest.raises(ValueError):
        parse_webhook_body(b"[]", "application/json")
    assert parse_webhook_body(b'{"x": 1}', "application/vnd.api+json") == {"x": 1}
    assert parse_webhook_body(b"<root><x>1</x></root>", "application/xml")["x"] == "1"
    assert parse_webhook_body(b"x=1&x=2", "application/x-www-form-urlencoded") == {"x": "2"}
    with pytest.raises(ValueError):
        parse_webhook_body(b"raw", "text/plain")
    with pytest.raises(ValueError):
        parse_webhook_body(b"{", "application/json")
    nested = sanitize_event_payload({"token": "hidden", "safe": {"value": "ok"}, "callback_verified": True})
    assert "token" not in nested and nested["callback_verified"] is True
    assert sanitize_event_metadata("x" * 20_000).endswith("[truncated]")
    assert attachment_kind(None) == "text"
    assert attachment_kind(Attachment("photo")) == "image"
    assert attachment_kind(Attachment("file")) == "file"


def test_attachment_materialization_supports_base64_file_and_http(monkeypatch, tmp_path):
    attachment = Attachment("file", name="data.txt", content_type="text/plain", metadata={"content_base64": "ZGF0YQ=="})
    with prepare_attachment_file(attachment) as prepared:
        assert prepared is not None and prepared.path.read_bytes() == b"data"
        assert prepared.filename == "data.txt"
    local = tmp_path / "local.bin"
    local.write_bytes(b"local")
    with prepare_attachment_file(Attachment("file", url=str(local))) as prepared:
        assert prepared is not None and prepared.path.read_bytes() == b"local"
    with pytest.raises(ValueError), prepare_attachment_file(Attachment("file", url=str(tmp_path / "missing"))):
        pass

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self, _limit):
            return b"remote"

    monkeypatch.setattr("trpc_service.channels.media.urlopen", lambda *args, **kwargs: Response())
    with prepare_attachment_file(Attachment("file", url="https://cdn.example/file", name="remote.bin")) as prepared:
        assert prepared is not None and prepared.path.read_bytes() == b"remote"
    with pytest.raises(ValueError), prepare_attachment_file(Attachment("file", metadata={"content_base64": "bad"})):
        pass

    calls = []
    monkeypatch.setattr(
        "trpc_service.channels.media.urlopen",
        lambda request, timeout: SimpleNamespace(
            __enter__=lambda self: self,
            __exit__=lambda self, *args: False,
            read=lambda self: b'{"ok":true}',
        ),
    )
    # A real context-manager fake keeps the request body assertion explicit.
    class MultipartResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return b'{"ok": true}'

    def multipart_open(request, timeout):
        calls.append((request.get_method(), request.data, request.headers))
        return MultipartResponse()

    monkeypatch.setattr("trpc_service.channels.media.urlopen", multipart_open)
    result = post_multipart_json("https://upload.example", fields={"kind": 1}, file_field="file", file_path=local)
    assert result == {"ok": True} and b"local" in calls[0][1]


def test_wechat_handshake_and_provider_parsing_and_fallback_sends(monkeypatch):
    token = "wechat-token"
    timestamp, nonce, echo = "1", "2", "echo"
    signature = hashlib.sha1("".join(sorted((token, timestamp, nonce))).encode()).hexdigest()
    monkeypatch.setattr("trpc_service.security.secrets.SecretManager.resolve", lambda self, ref: token)
    assert verify_handshake({"timestamp": timestamp, "nonce": nonce, "echostr": echo, "signature": signature}, "secret://token") == echo
    official = WeChatOfficialAccountAdapter()
    official_binding = ChannelBinding(
        "tenant-a", "binding-1", "wechat_official_account", "account-1", "app-1",
        token_ref="secret://token", config={"api_base_url": "https://wechat.example", "app_id": "app", "sdk_enabled": False}
    )
    payload = {"MsgType": "image", "MediaId": "media-1", "FromUserName": "user-1", "MsgId": "msg-1", "PicUrl": "https://cdn/image"}
    parsed = official.parse_event(payload, official_binding)
    assert parsed.external_user_id == "user-1" and parsed.attachments[0].kind == "image"
    official.verify_callback({}, official_binding)
    with pytest.raises(ChannelVerificationError):
        official.verify_callback({"timestamp": "1", "nonce": "2", "signature": "bad"}, official_binding)

    responses = iter([{"access_token": "access", "expires_in": 100}, {"errcode": 0, "msgid": "msg-1"}])
    monkeypatch.setattr("trpc_service.channels.wechat_official_account._wechat_json", lambda *args, **kwargs: next(responses))
    message = OutboundMessage("wechat_official_account", "account-1", "session-1", "user-1", "hello")
    result = official.send(message, official_binding)
    assert result.ok

    wecom = WeComAdapter()
    wecom_binding = binding("wecom", webhook_url="https://wecom.example/hook")
    parsed = wecom.parse_event({"MsgType": "text", "Content": "hello", "FromUserName": "u", "MsgId": "m"}, wecom_binding)
    assert parsed.text == "hello"
    sent = []
    monkeypatch.setattr("trpc_service.channels.wecom._post_json", lambda url, payload: sent.append(payload) or {"errcode": 0})
    result = wecom.send(message, wecom_binding)
    assert result.ok and sent[0]["msgtype"] == "text"
    unsupported = wecom.send(
        OutboundMessage("wecom", "account-1", "session-1", "u", "file", attachments=[Attachment("file")]),
        wecom_binding,
    )
    assert not unsupported.ok and unsupported.metadata["unsupported"]


def test_wecom_verification_and_http_media_failure_paths(monkeypatch, tmp_path):
    adapter = WeComAdapter()
    signed_binding = binding("wecom", token_ref="secret://token", sdk_enabled=False)
    signed_binding.token_ref = "secret://token"

    def unavailable(_self, _reference):
        raise RuntimeError("secret backend unavailable")

    monkeypatch.setattr("trpc_service.channels.wecom.SecretManager.resolve", unavailable)
    with pytest.raises(ChannelVerificationError, match="unavailable"):
        adapter.verify_callback({"timestamp": "1", "nonce": "2", "signature": "sig"}, signed_binding)
    with pytest.raises(ChannelVerificationError, match="incomplete"):
        adapter.verify_callback({"timestamp": "1", "nonce": "2", "signature": "sig"}, binding("wecom"))

    image = tmp_path / "image.png"
    image.write_bytes(b"image")
    webhook_binding = binding("wecom", webhook_url="https://wecom.example/hook", sdk_enabled=False)
    monkeypatch.setattr(
        "trpc_service.channels.wecom._post_json",
        lambda *_args, **_kwargs: {"errcode": 93000, "errmsg": "provider rejected"},
    )
    failed_text = adapter.send(
        OutboundMessage("wecom", "account-1", "session-1", "u", "hello"), webhook_binding
    )
    failed_image = adapter.send(
        OutboundMessage(
            "wecom", "account-1", "session-2", "u", "image", attachments=[Attachment("image", url=str(image))]
        ),
        webhook_binding,
    )
    assert not failed_text.ok and not failed_image.ok

    secret_url_binding = binding("wecom", webhook_url_ref="secret://hook", sdk_enabled=False)
    unavailable_url = adapter.send(
        OutboundMessage("wecom", "account-1", "session-3", "u", "hello"), secret_url_binding
    )
    assert not unavailable_url.ok and "unavailable" in unavailable_url.error

    api_binding = binding(
        "wecom",
        token_ref="secret://token",
        api_base_url="https://qyapi.example",
        agent_id=1001,
        sdk_enabled=False,
    )
    api_binding.token_ref = "secret://token"
    monkeypatch.setattr("trpc_service.channels.wecom.SecretManager.resolve", lambda *_args: "access-token")
    monkeypatch.setattr("trpc_service.channels.wecom.post_multipart_json", lambda *_args, **_kwargs: {})
    missing_media = adapter.send(
        OutboundMessage(
            "wecom", "account-1", "session-4", "u", "image", attachments=[Attachment("image", url=str(image))]
        ),
        api_binding,
    )
    assert not missing_media.ok

    monkeypatch.setattr(
        "trpc_service.channels.wecom._post_json",
        lambda *_args, **_kwargs: {"errcode": 1, "errmsg": "send rejected"},
    )
    failed_api = adapter.send(
        OutboundMessage("wecom", "account-1", "session-5", "u", "hello"), api_binding
    )
    assert not failed_api.ok

    monkeypatch.setattr(
        "trpc_service.channels.wecom._post_json",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("transport")),
    )
    exception_api = adapter.send(
        OutboundMessage("wecom", "account-1", "session-6", "u", "hello"), api_binding
    )
    assert not exception_api.ok and "RuntimeError" in exception_api.error
