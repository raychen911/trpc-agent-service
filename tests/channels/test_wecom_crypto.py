"""Static vectors and adversarial tests for WeCom callback cryptography."""

from __future__ import annotations

import base64
import json
import struct
from collections.abc import Mapping
from typing import Any

import pytest
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from tests.channels.helpers import load_json
from trpc_service.channels.wecom import (
    WeComCallbackError,
    WeComCrypto,
    WeComCryptoError,
    WeComProtocolError,
    WeComSignatureError,
)


@pytest.fixture
def vector() -> dict[str, Any]:
    return load_json("wecom_vectors.json")


@pytest.fixture
def crypto(vector: Mapping[str, Any]) -> WeComCrypto:
    prefix = bytes.fromhex(str(vector["random_prefix_hex"]))
    return WeComCrypto(
        str(vector["token"]),
        str(vector["encoding_aes_key"]),
        random_source=lambda size: prefix,
    )


def test_static_vector_signature_decrypt_and_callback(
    crypto: WeComCrypto,
    vector: Mapping[str, Any],
) -> None:
    assert (
        crypto.signature(
            timestamp=str(vector["timestamp"]),
            nonce=str(vector["nonce"]),
            ciphertext=str(vector["ciphertext"]),
        )
        == vector["signature"]
    )
    assert crypto.decrypt_ciphertext(str(vector["ciphertext"])).decode() == vector["plaintext"]
    assert (
        crypto.decrypt_callback(
            str(vector["callback_body"]).encode(),
            msg_signature=str(vector["signature"]),
            timestamp=str(vector["timestamp"]),
            nonce=str(vector["nonce"]),
        )
        == vector["payload"]
    )


def test_static_passive_reply_vector(
    crypto: WeComCrypto,
    vector: Mapping[str, Any],
) -> None:
    result = crypto.encrypt_reply(
        vector["passive_reply_payload"],
        nonce=str(vector["nonce"]),
        timestamp=str(vector["timestamp"]),
    )

    assert result.decode() == vector["passive_reply_body"]
    envelope = json.loads(result)
    crypto.verify_signature(
        msg_signature=envelope["msgsignature"],
        timestamp=str(envelope["timestamp"]),
        nonce=envelope["nonce"],
        ciphertext=envelope["encrypt"],
    )
    assert (
        json.loads(crypto.decrypt_ciphertext(envelope["encrypt"]))
        == vector["passive_reply_payload"]
    )


def test_verify_url_returns_exact_plaintext_bytes(
    crypto: WeComCrypto,
    vector: Mapping[str, Any],
) -> None:
    result = crypto.verify_url(
        msg_signature=str(vector["signature"]),
        timestamp=str(vector["timestamp"]),
        nonce=str(vector["nonce"]),
        echostr=str(vector["ciphertext"]),
    )

    assert result == str(vector["plaintext"]).encode()


@pytest.mark.parametrize(
    ("token", "aes_key", "receive_id", "match"),
    [
        ("", "A" * 43, "", "token"),
        ("token", "!", "", "EncodingAESKey"),
        ("token", base64.b64encode(b"short").decode().rstrip("="), "", "exactly 32"),
        ("token", "A" * 43, "corp-id", "empty string"),
    ],
)
def test_constructor_rejects_invalid_configuration(
    token: str,
    aes_key: str,
    receive_id: str,
    match: str,
) -> None:
    with pytest.raises(ValueError, match=match):
        WeComCrypto(token, aes_key, receive_id=receive_id)


def test_signature_rejects_empty_inputs_and_tampering(
    crypto: WeComCrypto,
    vector: Mapping[str, Any],
) -> None:
    with pytest.raises(WeComProtocolError, match="must not be empty"):
        crypto.signature(timestamp="", nonce="n", ciphertext="c")
    with pytest.raises(WeComSignatureError, match="invalid callback signature"):
        crypto.verify_signature(
            msg_signature="0" * 40,
            timestamp=str(vector["timestamp"]),
            nonce=str(vector["nonce"]),
            ciphertext=str(vector["ciphertext"]),
        )


@pytest.mark.parametrize("ciphertext", ["not-base64!", "YQ==", ""])
def test_decrypt_rejects_bad_ciphertext(crypto: WeComCrypto, ciphertext: str) -> None:
    with pytest.raises(WeComCryptoError, match="invalid encrypted callback"):
        crypto.decrypt_ciphertext(ciphertext)


def test_decrypt_rejects_bad_padding_length_and_receive_id(
    vector: Mapping[str, Any],
    crypto: WeComCrypto,
) -> None:
    key = base64.b64decode(str(vector["encoding_aes_key"]) + "=")
    bad_padding = _encrypt_padded(key, b"x" * 31 + b"\x00")
    declared_too_long = _encrypt_message(key, b"{}", declared_length=100)
    non_empty_receive_id = _encrypt_message(key, b"{}", tail=b"corp-id")

    for ciphertext in (bad_padding, declared_too_long, non_empty_receive_id):
        with pytest.raises(WeComCryptoError, match="invalid encrypted callback"):
            crypto.decrypt_ciphertext(ciphertext)


def test_random_source_must_return_exactly_16_bytes(vector: Mapping[str, Any]) -> None:
    crypto = WeComCrypto(
        str(vector["token"]),
        str(vector["encoding_aes_key"]),
        random_source=lambda size: b"too-short",
    )

    with pytest.raises(WeComCryptoError, match="exactly 16"):
        crypto.encrypt_plaintext(b"hello")


def test_encrypt_reply_rejects_empty_nonce(
    crypto: WeComCrypto,
) -> None:
    with pytest.raises(WeComProtocolError, match="nonce"):
        crypto.encrypt_reply({"msgtype": "text"}, nonce="")


def test_callback_rejects_oversize_duplicate_keys_and_missing_encrypt(
    crypto: WeComCrypto,
) -> None:
    with pytest.raises(WeComCallbackError, match="exceeds"):
        crypto.decrypt_callback(
            b"{}",
            msg_signature="x",
            timestamp="1",
            nonce="n",
            max_body_bytes=1,
        )
    with pytest.raises(WeComCallbackError, match="invalid JSON"):
        crypto.decrypt_callback(
            b'{"encrypt":"a","encrypt":"b"}',
            msg_signature="x",
            timestamp="1",
            nonce="n",
        )
    with pytest.raises(WeComCallbackError, match="requires string encrypt"):
        crypto.decrypt_callback(
            b"{}",
            msg_signature="x",
            timestamp="1",
            nonce="n",
        )


@pytest.mark.parametrize("plaintext", [b"[]", b"{", b'{"value":NaN}'])
def test_callback_rejects_non_object_or_invalid_business_json(
    crypto: WeComCrypto,
    plaintext: bytes,
) -> None:
    ciphertext = crypto.encrypt_plaintext(plaintext)
    signature = crypto.signature(timestamp="1", nonce="n", ciphertext=ciphertext)
    body = json.dumps({"encrypt": ciphertext}).encode()

    with pytest.raises(WeComCallbackError):
        crypto.decrypt_callback(
            body,
            msg_signature=signature,
            timestamp="1",
            nonce="n",
        )


def _encrypt_padded(key: bytes, padded: bytes) -> str:
    assert len(padded) % 16 == 0
    encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    return base64.b64encode(encryptor.update(padded) + encryptor.finalize()).decode()


def _encrypt_message(
    key: bytes,
    message: bytes,
    *,
    declared_length: int | None = None,
    tail: bytes = b"",
) -> str:
    length = len(message) if declared_length is None else declared_length
    plaintext = b"0123456789abcdef" + struct.pack("!I", length) + message + tail
    padding = 32 - (len(plaintext) % 32)
    return _encrypt_padded(key, plaintext + bytes((padding,)) * padding)
