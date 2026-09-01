"""End-to-end HTTP ingress tests through binding, crypto, and durable Inbox."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import func, select

from trpc_service.channels.wecom import WeComCrypto
from trpc_service.config import Environment, Settings
from trpc_service.security import EnvelopeCipher
from trpc_service.storage import Database
from trpc_service.storage.models import ChannelReplyCredential, InboxMessage
from trpc_service.web import create_app

FIXTURES = Path(__file__).parents[1] / "channels" / "fixtures"
ROOT_KEY = "ingress-test-root-key-at-least-32-bytes"
TELEGRAM_SECRET = "telegram_secret_2026"  # noqa: S105 - synthetic fixture
TELEGRAM_TOKEN = "123456:test-token-never-real"  # noqa: S105 - synthetic fixture


def _load_fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _tenant_spec() -> dict[str, object]:
    return {
        "tenant_id": "tenant-001",
        "revision": 1,
        "display_name": "Ingress tenant",
        "apps": [
            {
                "app_id": "app-001",
                "revision": 1,
                "name": "ingress_agent",
                "prompt": "Answer only from verified tenant context.",
                "model": {"provider": "mock", "model": "deterministic"},
            }
        ],
        "channels": [
            {
                "binding_id": "binding-001",
                "app_id": "app-001",
                "app_revision": 1,
                "channel": "wecom",
                "external_account_id": "bot-001",
                "callback_path": "/v1/channels/wecom/wecom-public/callback",
                "public_callback_id": "wecom-public",
                "secret_refs": {
                    "token": "secret://env/TEST_WECOM_TOKEN",
                    "aes_key": "secret://env/TEST_WECOM_AES_KEY",
                },
            },
            {
                "binding_id": "binding-telegram",
                "app_id": "app-001",
                "app_revision": 1,
                "channel": "telegram",
                "external_account_id": "telegram-bot-001",
                "callback_path": "/v1/channels/telegram/telegram-public/callback",
                "public_callback_id": "telegram-public",
                "secret_refs": {
                    "webhook_secret": "secret://env/TEST_TELEGRAM_SECRET",
                    "bot_token": "secret://env/TEST_TELEGRAM_TOKEN",
                },
            },
        ],
    }


@pytest.fixture
def ingress_client(tmp_path, monkeypatch):
    wecom = _load_fixture("wecom_vectors.json")
    monkeypatch.setenv("TEST_WECOM_TOKEN", str(wecom["token"]))
    monkeypatch.setenv("TEST_WECOM_AES_KEY", str(wecom["encoding_aes_key"]))
    monkeypatch.setenv("TEST_TELEGRAM_SECRET", TELEGRAM_SECRET)
    monkeypatch.setenv("TEST_TELEGRAM_TOKEN", TELEGRAM_TOKEN)
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'ingress.db'}"

    async def prepare() -> None:
        database = Database(database_url)
        await database.create_schema()
        await database.dispose()

    asyncio.run(prepare())
    settings = Settings(
        _env_file=None,
        env=Environment.TEST,
        database_url=database_url,
        secret_key=SecretStr(ROOT_KEY),
        admin_api_key=SecretStr("test-admin-key"),
        secret_env_allowlist=(
            "TEST_WECOM_TOKEN",
            "TEST_WECOM_AES_KEY",
            "TEST_TELEGRAM_SECRET",
            "TEST_TELEGRAM_TOKEN",
        ),
        inbox_payload_limit_bytes=1_024,
    )
    with TestClient(create_app(settings)) as client:
        response = client.post(
            "/v1/admin/tenants/tenant-001/revisions",
            json=_tenant_spec(),
            headers={"x-admin-key": "test-admin-key"},
        )
        assert response.status_code == 201
        yield client, database_url


def _wecom_query(vector: dict[str, object]) -> dict[str, str]:
    timestamp = str(int(datetime.now(UTC).timestamp()))
    nonce = str(vector["nonce"])
    ciphertext = str(json.loads(str(vector["callback_body"]))["encrypt"])
    signature = WeComCrypto(
        str(vector["token"]),
        str(vector["encoding_aes_key"]),
    ).signature(timestamp=timestamp, nonce=nonce, ciphertext=ciphertext)
    return {
        "msg_signature": signature,
        "timestamp": timestamp,
        "nonce": nonce,
    }


def test_wecom_ack_happens_after_inbox_and_encrypted_route_commit(ingress_client) -> None:
    client, database_url = ingress_client
    vector = _load_fixture("wecom_vectors.json")
    path = "/v1/channels/wecom/wecom-public/callback"
    response = client.post(
        path,
        params=_wecom_query(vector),
        content=str(vector["callback_body"]).encode(),
        headers={"content-type": "application/json", "x-request-id": "wecom-request-1"},
    )
    assert response.status_code == 200
    assert response.content == b""

    async def inspect() -> tuple[InboxMessage, ChannelReplyCredential, int]:
        database = Database(database_url)
        try:
            async with database.session_factory() as session:
                inbox = await session.scalar(select(InboxMessage))
                credential = await session.scalar(select(ChannelReplyCredential))
                count = await session.scalar(select(func.count()).select_from(InboxMessage))
                assert inbox is not None and credential is not None
                return inbox, credential, int(count or 0)
        finally:
            await database.dispose()

    inbox, credential, count = asyncio.run(inspect())
    assert count == 1
    serialized_payload = json.dumps(inbox.payload, ensure_ascii=False)
    assert "user-001" not in serialized_payload
    assert "response_code" not in serialized_payload
    assert "TEST_SECRET" not in credential.ciphertext
    plaintext = EnvelopeCipher(ROOT_KEY).decrypt(
        credential.ciphertext,
        context={
            "tenant_id": "tenant-001",
            "binding_id": "binding-001",
            "delivery_id": "msg-vector-001",
            "credential_kind": "wecom_response_url",
        },
    )
    assert "response_code=TEST_SECRET" in plaintext.get_secret_value()

    # The second delivery gets a fresh AES-GCM nonce but reuses the original T0
    # credential by keyed plaintext fingerprint and does not create another Inbox.
    duplicate = client.post(
        path,
        params=_wecom_query(vector),
        content=str(vector["callback_body"]).encode(),
        headers={"content-type": "application/json"},
    )
    assert duplicate.status_code == 200
    _, _, duplicate_count = asyncio.run(inspect())
    assert duplicate_count == 1


def test_telegram_authentication_deduplication_and_no_raw_chat_in_payload(
    ingress_client,
) -> None:
    client, database_url = ingress_client
    update = _load_fixture("telegram_updates.json")["private_text"]
    path = "/v1/channels/telegram/telegram-public/callback"
    wrong = client.post(
        path,
        json=update,
        headers={"X-Telegram-Bot-Api-Secret-Token": "wrong"},
    )
    assert wrong.status_code == 401

    body = json.dumps(update, separators=(",", ":")).encode()
    headers = {
        "content-type": "application/json",
        "X-Telegram-Bot-Api-Secret-Token": TELEGRAM_SECRET,
    }
    first = client.post(path, content=body, headers=headers)
    second = client.post(path, content=body, headers=headers)
    assert first.status_code == second.status_code == 200

    async def inspect() -> tuple[int, str]:
        database = Database(database_url)
        try:
            async with database.session_factory() as session:
                rows = list((await session.scalars(select(InboxMessage))).all())
                telegram = next(row for row in rows if row.binding_id == "binding-telegram")
                return len(rows), json.dumps(telegram.payload, ensure_ascii=False)
        finally:
            await database.dispose()

    row_count, payload = asyncio.run(inspect())
    assert row_count == 1
    assert '"chat_id"' not in payload
    assert '"101"' not in payload


def test_wecom_url_verification_and_ingress_limits(ingress_client) -> None:
    client, _ = ingress_client
    vector = _load_fixture("wecom_vectors.json")
    verify = client.get(
        "/v1/channels/wecom/wecom-public/callback",
        params={**_wecom_query(vector), "echostr": str(vector["ciphertext"])},
    )
    assert verify.status_code == 200
    assert verify.content == str(vector["plaintext"]).encode()

    oversized = client.post(
        "/v1/channels/telegram/telegram-public/callback",
        content=b"x" * 1_025,
        headers={
            "content-type": "application/json",
            "X-Telegram-Bot-Api-Secret-Token": TELEGRAM_SECRET,
        },
    )
    assert oversized.status_code == 413
