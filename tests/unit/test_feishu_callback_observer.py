from __future__ import annotations

import base64
import hashlib
import http.client
import json
import threading
import time
from collections.abc import Mapping

import pytest
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from deploy.im_probe.feishu_callback_observer import (
    CallbackRejected,
    FeishuCallbackObserver,
    ReceiptStore,
    create_mirror_server,
    marker_sha256,
    profile_sha256,
    query_receipt,
)

TOKEN = "observer-verification-token"
KEY = "observer-encrypt-key"
APP_ID = "cli_observer_app"
MARKER = "TRPC-FEISHU-OBSERVER-TEST"


def _payload(
    *,
    event_id: str = "event-1",
    message_id: str = "message-1",
    message_type: str = "text",
    content: Mapping[str, object] | None = None,
    sender_type: str = "user",
) -> dict[str, object]:
    return {
        "schema": "2.0",
        "header": {
            "event_id": event_id,
            "event_type": "im.message.receive_v1",
            "create_time": "1788148800000",
            "token": TOKEN,
            "app_id": APP_ID,
        },
        "event": {
            "sender": {"sender_type": sender_type},
            "message": {
                "message_id": message_id,
                "create_time": "1788148800000",
                "chat_type": "p2p",
                "message_type": message_type,
                "content": json.dumps(content or {"text": MARKER}),
            },
        },
    }


def _encrypted_request(
    payload: Mapping[str, object],
    *,
    timestamp: int | None = None,
    key: str = KEY,
) -> tuple[dict[str, str], bytes]:
    plaintext = json.dumps(payload, separators=(",", ":")).encode()
    iv = bytes(range(16))
    padder = padding.PKCS7(128).padder()
    padded = padder.update(plaintext) + padder.finalize()
    cipher_key = hashlib.sha256(key.encode()).digest()
    encryptor = Cipher(algorithms.AES(cipher_key), modes.CBC(iv)).encryptor()
    encrypted = base64.b64encode(iv + encryptor.update(padded) + encryptor.finalize()).decode()
    body = json.dumps({"encrypt": encrypted}, separators=(",", ":")).encode()
    signed_at = int(time.time()) if timestamp is None else timestamp
    nonce = "observer-nonce"
    signature = hashlib.sha256(
        str(signed_at).encode() + nonce.encode() + key.encode() + body
    ).hexdigest()
    return (
        {
            "X-Lark-Request-Timestamp": str(signed_at),
            "X-Lark-Request-Nonce": nonce,
            "X-Lark-Signature": signature,
        },
        body,
    )


def _observer(*, store: ReceiptStore | None = None) -> FeishuCallbackObserver:
    return FeishuCallbackObserver(
        verification_token=TOKEN,
        encrypt_key=KEY,
        account_id=APP_ID,
        store=store,
    )


def test_verified_encrypted_callback_is_stored_as_content_free_hashes() -> None:
    observer = _observer()
    headers, body = _encrypted_request(_payload())

    receipt = observer.observe(headers, body)

    assert receipt is not None
    assert receipt.marker_sha256 == marker_sha256(MARKER)
    assert receipt.profile_sha256 == profile_sha256(
        account_id=APP_ID,
        event_type="im.message.receive_v1",
        message_type="text",
        chat_type="p2p",
    )
    rendered = json.dumps(receipt.as_dict(), sort_keys=True)
    for sensitive in (
        MARKER,
        "event-1",
        "message-1",
        APP_ID,
        TOKEN,
        KEY,
        "chat",
        "sender",
        "content",
    ):
        assert sensitive not in rendered


def test_signature_token_and_ciphertext_fail_closed() -> None:
    observer = _observer()
    headers, body = _encrypted_request(_payload())

    bad_signature = dict(headers)
    bad_signature["X-Lark-Signature"] = "0" * 64
    with pytest.raises(CallbackRejected, match="invalid"):
        observer.observe(bad_signature, body)

    wrong_token = _payload()
    assert isinstance(wrong_token["header"], dict)
    wrong_token["header"]["token"] = "wrong"
    with pytest.raises(CallbackRejected, match="invalid"):
        observer.observe(*_encrypted_request(wrong_token))

    with pytest.raises(CallbackRejected, match="invalid"):
        observer.observe(headers, b'{"encrypt":"not-base64"}')


def test_duplicate_event_is_idempotent_and_query_requires_both_hashes() -> None:
    observer = _observer()
    headers, body = _encrypted_request(_payload())
    first = observer.observe(headers, body)
    second = observer.observe(headers, body)

    assert first is second
    assert len(observer.store) == 1
    assert first is not None
    found = query_receipt(
        json.dumps(
            {
                "marker_sha256": first.marker_sha256,
                "profile_sha256": first.profile_sha256,
            }
        ).encode(),
        observer.store,
    )
    assert json.loads(found) == {"receipt": first.as_dict(), "status": "found"}
    assert json.loads(
        query_receipt(
            b'{"marker_sha256":"' + b"0" * 64 + b'","profile_sha256":"' + b"0" * 64 + b'"}',
            observer.store,
        )
    ) == {"status": "not_found"}


def test_media_locator_is_hashed_and_raw_locator_is_not_retained() -> None:
    observer = _observer()
    headers, body = _encrypted_request(
        _payload(message_type="image", content={"image_key": "img-sensitive-locator"})
    )

    receipt = observer.observe(headers, body)

    assert receipt is not None
    assert len(receipt.media_locator_sha256) == 1
    assert "img-sensitive-locator" not in json.dumps(receipt.as_dict())


def test_challenge_non_message_and_bot_events_are_ignored() -> None:
    observer = _observer()
    challenge = {"type": "url_verification", "token": TOKEN, "challenge": "secret"}
    headers, body = _encrypted_request(challenge)
    assert observer.observe(headers, body) is None

    non_message = _payload()
    assert isinstance(non_message["header"], dict)
    non_message["header"]["event_type"] = "application.bot.menu_v6"
    assert observer.observe(*_encrypted_request(non_message)) is None
    assert observer.observe(*_encrypted_request(_payload(sender_type="bot"))) is None
    assert len(observer.store) == 0


def test_store_prunes_by_ttl_and_capacity() -> None:
    now = [100.0]
    store = ReceiptStore(ttl_seconds=10, capacity=2, clock=lambda: now[0])
    observer = _observer(store=store)

    receipts = []
    for index in range(3):
        receipt = observer.observe(
            *_encrypted_request(
                _payload(
                    event_id=f"event-{index}",
                    message_id=f"message-{index}",
                    content={"text": f"marker-{index}"},
                )
            )
        )
        assert receipt is not None
        receipts.append(receipt)
    assert len(store) == 2
    assert store.query(receipts[0].marker_sha256, receipts[0].profile_sha256) is None

    now[0] = 111.0
    assert len(store) == 0


@pytest.mark.parametrize(
    "body",
    [
        b"[]",
        b'{"encrypt":"x","encrypt":"y"}',
        b'{"encrypt":NaN}',
        b"{",
    ],
)
def test_strict_json_and_invalid_input_are_rejected(body: bytes) -> None:
    with pytest.raises(CallbackRejected, match="invalid"):
        _observer().observe(
            {
                "X-Lark-Request-Timestamp": str(int(time.time())),
                "X-Lark-Request-Nonce": "nonce",
                "X-Lark-Signature": "0" * 64,
            },
            body,
        )


def test_loopback_mirror_always_returns_204_and_never_logs_payload() -> None:
    observer = _observer()
    server = create_mirror_server("127.0.0.1", 0, observer)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=2)
        connection.request(
            "POST",
            "/mirror",
            body=b'{"sensitive":"must-not-echo"}',
            headers={"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        assert response.status == 204
        assert response.read() == b""
        connection.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_non_loopback_bind_is_rejected() -> None:
    with pytest.raises(ValueError, match="loopback"):
        create_mirror_server("0.0.0.0", 0, _observer())  # noqa: S104
