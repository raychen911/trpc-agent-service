"""Customer-service component tests: no credentials, real IM or model cost.

Run: python -m pytest tests/test_customer_service.py -vv
Uses real crypto/HTTP adapters with Fake API and in-memory repositories.
Expected: authenticated callback -> durable cursor/inbox -> SDK -> Outbox ->
one reply; invalid signatures and human-owned conversations never run Agent.
Failure lookup: channels/customer_*, then container.admit_channel/dispatcher.
"""
import base64
import hashlib
import struct
import time

import httpx
import pytest
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from trpc_service.channels.customer_service import (CallbackCrypto, CustomerServiceAdapter, CustomerServiceError,
                                                    FakeCustomerServiceClient, HttpCustomerServiceClient)
from trpc_service.channels.customer_store import InMemoryCustomerStore
from trpc_service.channels.base import ChannelAuthenticationError
from trpc_service.config import ChannelBindingConfig, ServiceSettings
from trpc_service.agent import AgentWorker, TenantRuntimeManager
from trpc_service.offline import OfflineRuntimeFactory
from trpc_service.gateway.service import GatewayService
from trpc_service.gateway.dispatcher import AgentTaskProcessor
from trpc_service.gateway.models import OutboundMessage
from trpc_service.web import build_container, create_app

pytestmark = pytest.mark.component
KEY = bytes(range(32))
AES_KEY = base64.b64encode(KEY).decode().rstrip("=")


def encrypted_callback(plaintext, receiver="corp", padding_override=None):
    # Independent encoder, deliberately not a production encrypt/decrypt roundtrip.
    raw = bytes(range(16)) + struct.pack("!I", len(plaintext.encode())) + plaintext.encode() + receiver.encode()
    padding = 32 - len(raw) % 32
    raw += bytes([padding]) * padding if padding_override is None else padding_override
    encryptor = Cipher(algorithms.AES(KEY), modes.CBC(KEY[:16])).encryptor()
    encrypted = base64.b64encode(encryptor.update(raw) + encryptor.finalize()).decode()
    timestamp, nonce = "1700000000", "test-nonce"
    signature = hashlib.sha1("".join(sorted(["callback-token", timestamp, nonce, encrypted])).encode()).hexdigest()
    return encrypted, dict(msg_signature=signature, timestamp=timestamp, nonce=nonce)


def binding():
    return ChannelBindingConfig(binding_id="kf-bot",
                                app_id="assistant",
                                channel="wecom_kf",
                                corp_id="corp",
                                open_kfid="kf-account")


def customer(msgid="m1", origin=3, kind="text"):
    return {
        "msgid": msgid,
        "open_kfid": "kf-account",
        "external_userid": "customer",
        "origin": origin,
        "send_time": int(time.time()),
        "msgtype": kind,
        kind: {
            "content": "hello"
        } if kind == "text" else {
            "media_id": "media-1"
        }
    }


async def setup(tenant, pages=None):
    tenant.channels = [binding()]
    store, client = InMemoryCustomerStore(), FakeCustomerServiceClient(pages)
    adapter = CustomerServiceAdapter(binding(), client, store, CallbackCrypto("callback-token", AES_KEY, "corp"))
    container = build_container(ServiceSettings(), [tenant], channel_adapters={"kf-bot": adapter})
    container.customer_store = store
    factory = OfflineRuntimeFactory()
    container.runtimes = TenantRuntimeManager(container.registry, factory)
    container.gateway = GatewayService(container.registry, AgentWorker(container.runtimes, container.guard),
                                       container.idempotency, container.gateway._requests)
    container.task_processor = AgentTaskProcessor(container.queue,
                                                  container.gateway,
                                                  container.outbox,
                                                  consumer="kf-test")
    await container.setup_customer_channels()
    return container, client, store, factory


def test_callback_crypto_rejects_tampering_and_wrong_receiver():
    crypto = CallbackCrypto("callback-token", AES_KEY, "corp")
    encrypted, params = encrypted_callback("echo-value")
    assert crypto.decrypt(encrypted, params["msg_signature"], params["timestamp"], params["nonce"]) == "echo-value"
    for invalid in ("bad", "非" * 40):
        with pytest.raises(ChannelAuthenticationError):
            crypto.decrypt(encrypted, invalid, params["timestamp"], params["nonce"])
    wrong, params = encrypted_callback("echo-value", receiver="other-corp")
    with pytest.raises(ChannelAuthenticationError):
        crypto.decrypt(wrong, params["msg_signature"], params["timestamp"], params["nonce"])


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_encrypted_callback_to_one_reply(tenant_config):
    container, fake, store, factory = await setup(
        tenant_config, {"": {
            "msg_list": [customer()],
            "next_cursor": "cursor-1",
            "has_more": 0
        }})
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(container)),
                                     base_url="http://test") as web:
            echo, params = encrypted_callback("echo")
            response = await web.get("/api/v1/channels/kf-bot/webhook", params={**params, "echostr": echo})
            assert response.text == "echo"
            encrypted, params = encrypted_callback(
                "<xml><Event>kf_msg_or_event</Event><Token>sync-token</Token><OpenKfId>kf-account</OpenKfId></xml>")
            xml = f"<xml><Encrypt>{encrypted}</Encrypt></xml>"
            rejected = await web.post("/api/v1/channels/kf-bot/webhook",
                                      params={
                                          **params, "msg_signature": "bad"
                                      },
                                      content=xml)
            assert rejected.status_code == 403
            assert not (await store.snapshot("kf-bot"))["pending"]
            for _ in range(2):
                response = await web.post("/api/v1/channels/kf-bot/webhook", params=params, content=xml)
                assert response.status_code == 200 and response.text == "success"
            runtime = container.customer_runtimes["kf-bot"]
            assert await runtime.step()
            assert await container.task_processor.process_one(.1)
            assert await container.delivery_worker.deliver_due() == 1
            assert len(fake.sent) == 1 and "echo:hello" in fake.sent[0]["text"]["content"]
            assert sum(model.calls for model in factory.models) == 1
            state = await store.snapshot("kf-bot")
            assert state["cursor"] == "cursor-1"
            assert state["inbox"]["m1"]["state"] == "admitted"
    finally:
        await container.close()


@pytest.mark.asyncio
async def test_empty_page_has_more_and_human_handoff(tenant_config):
    container, fake, store, factory = await setup(
        tenant_config, {
            "": {
                "msg_list": [],
                "next_cursor": "p2",
                "has_more": 1
            },
            "p2": {
                "msg_list": [customer(), customer("staff", origin=4)],
                "next_cursor": "end",
                "has_more": 0
            }
        })
    try:
        fake.states["customer"] = 3
        await store.notify("kf-bot", "token", "n1")
        runtime = container.customer_runtimes["kf-bot"]
        await runtime.step()
        await runtime.step()
        state = await store.snapshot("kf-bot")
        assert state["cursor"] == "end"
        assert state["inbox"]["m1"]["state"] == "human_or_closed"
        assert state["inbox"]["staff"]["state"] == "observed"
        assert not factory.models
    finally:
        await container.close()


@pytest.mark.asyncio
@pytest.mark.fault
async def test_page_rollback_and_stale_claim():
    store = InMemoryCustomerStore()
    await store.notify("b", "t", "n")
    claim = await store.claim_sync("b")
    bad = customer()
    bad["open_kfid"] = "wrong"
    with pytest.raises(PermissionError):
        await store.save_page("b", claim, {"msg_list": [bad], "next_cursor": "bad"}, "kf-account")
    assert (await store.snapshot("b"))["cursor"] == ""
    await store.save_page("b", claim, {"msg_list": [customer()], "next_cursor": "good"}, "kf-account")
    with pytest.raises(RuntimeError):
        await store.save_page("b", claim, {"next_cursor": "stale"}, "kf-account")


@pytest.mark.asyncio
async def test_delivery_unknown_does_not_repeat_and_quota():
    store, client = InMemoryCustomerStore(), FakeCustomerServiceClient()
    adapter = CustomerServiceAdapter(binding(), client, store)
    await store.notify("kf-bot", "t", "n")
    claim = await store.claim_sync("kf-bot")
    await store.save_page("kf-bot", claim, {"msg_list": [customer()], "next_cursor": "1"}, "kf-account")
    message = OutboundMessage(outbound_id="o1",
                              request_id="r",
                              tenant_id="t",
                              binding_id="kf-bot",
                              channel="wecom_kf",
                              external_conversation_id="kf-account:customer",
                              text="reply")
    client.send_error = CustomerServiceError("kf_delivery_unknown", uncertain=True)
    assert (await adapter.deliver(message)).uncertain
    from trpc_service.gateway.dispatcher import DeliveryWorker
    from trpc_service.gateway.outbox import InMemoryOutboxStore, OutboxState
    from trpc_service.metrics import MetricsRegistry
    outbox = InMemoryOutboxStore()
    await outbox.add(message)
    metrics = MetricsRegistry()
    assert await DeliveryWorker(outbox, {"kf-bot": adapter}, metrics).deliver_due() == 0
    assert (await outbox.get("o1")).state == OutboxState.UNKNOWN
    assert 'trpc_service_delivery_total{channel="wecom_kf",result="unknown"} 1' in metrics.render()
    assert "trpc_service_delivery_duration_seconds_count" in metrics.render()
    assert not await outbox.claim()
    client.send_error = None
    assert (await adapter.deliver(message)).uncertain
    assert not client.sent
    for index in range(4):
        assert (await adapter.deliver(message.model_copy(update={"outbound_id": f"other-{index}"}))).delivered
    assert (await
            adapter.deliver(message.model_copy(update={"outbound_id": "over"}))).error_code == "kf_reply_quota_exceeded"


@pytest.mark.asyncio
async def test_http_token_refresh_and_uncertain_send():
    token_calls = 0

    def handle(request):
        nonlocal token_calls
        if request.url.path.endswith("gettoken"):
            token_calls += 1
            return httpx.Response(200, json={"access_token": f"token-{token_calls}", "expires_in": 7200})
        if request.url.params.get("access_token") == "token-1":
            return httpx.Response(200, json={"errcode": 42001})
        return httpx.Response(200, json={"errcode": 0, "service_state": 1})

    async with httpx.AsyncClient(base_url="https://fake/", transport=httpx.MockTransport(handle)) as http:
        client = HttpCustomerServiceClient("corp", "secret", http)
        assert await client.service_state("kf", "u") == 1
        assert token_calls == 2
        assert await client.service_state("kf", "u") == 1
        assert token_calls == 2


@pytest.mark.asyncio
async def test_media_ingestion_reply_and_tenant_boundary():
    from trpc_service.resources import InMemoryArtifactStore, ArtifactNotFoundError
    from trpc_service.resources.attachments import AttachmentIngestor
    from trpc_service.gateway.models import AgentRequest
    store, client, artifacts = InMemoryCustomerStore(), FakeCustomerServiceClient(), InMemoryArtifactStore()
    adapter = CustomerServiceAdapter(binding(), client, store, artifacts=artifacts)
    client.files["media-1"] = (b"example document", "text/plain")
    await store.notify("kf-bot", "t", "n")
    claim = await store.claim_sync("kf-bot")
    await store.save_page("kf-bot", claim, {"msg_list": [customer(kind="file")], "next_cursor": "1"}, "kf-account")
    normalized = await adapter.normalize("kf-bot", customer(kind="file"), {})
    request = AgentRequest(request_id="r",
                           tenant_id="t",
                           app_id="a",
                           config_version=1,
                           user_id="u",
                           session_id="s",
                           channel="wecom_kf",
                           binding_id="kf-bot",
                           text="",
                           attachments=normalized.attachments)
    await AttachmentIngestor(artifacts, {"kf-bot": adapter}).materialize(request)
    attachment = request.attachments[0]
    with pytest.raises(ArtifactNotFoundError):
        await artifacts.get("other-tenant", attachment.attachment_id)
    message = OutboundMessage(outbound_id="file-reply",
                              request_id="r",
                              tenant_id="t",
                              binding_id="kf-bot",
                              channel="wecom_kf",
                              external_conversation_id="kf-account:customer",
                              text="",
                              attachments=[attachment])
    assert (await adapter.deliver(message)).delivered
    assert client.sent[0]["msgtype"] == "file"


@pytest.mark.asyncio
async def test_send_timeout_is_unknown_not_retryable():

    def handle(request):
        if request.url.path.endswith("gettoken"):
            return httpx.Response(200, json={"access_token": "test-access-token", "expires_in": 7200})
        raise httpx.ReadTimeout("response lost", request=request)

    async with httpx.AsyncClient(base_url="https://fake/", transport=httpx.MockTransport(handle)) as http:
        with pytest.raises(CustomerServiceError) as error:
            await HttpCustomerServiceClient("corp", "secret", http).send({"text": {"content": "hello"}})
        assert error.value.uncertain and not error.value.retryable


@pytest.mark.asyncio
async def test_human_handoff_before_delivery_and_expired_reply_window():
    store, client = InMemoryCustomerStore(), FakeCustomerServiceClient()
    adapter = CustomerServiceAdapter(binding(), client, store)
    await store.notify("kf-bot", "t", "n")
    claim = await store.claim_sync("kf-bot")
    old = customer()
    old["send_time"] = time.time() - 49 * 3600
    await store.save_page("kf-bot", claim, {"msg_list": [old], "next_cursor": "1"}, "kf-account")
    msg = OutboundMessage(outbound_id="paused",
                          request_id="r",
                          tenant_id="t",
                          binding_id="kf-bot",
                          channel="wecom_kf",
                          external_conversation_id="kf-account:customer",
                          text="hi")
    client.states["customer"] = 3
    assert (await adapter.deliver(msg)).error_code == "kf_human_or_closed"
    client.states["customer"] = 1
    assert (await adapter.deliver(msg)).error_code == "kf_reply_window_expired"
    assert not client.sent


@pytest.mark.asyncio
async def test_rate_limit_keeps_retry_after():

    def handle(request):
        if request.url.path.endswith("gettoken"):
            return httpx.Response(200, json={"access_token": "test-token"})
        return httpx.Response(429, headers={"Retry-After": "7"})

    async with httpx.AsyncClient(base_url="https://fake/", transport=httpx.MockTransport(handle)) as http:
        with pytest.raises(CustomerServiceError) as error:
            await HttpCustomerServiceClient("corp", "secret", http).send({})
        assert error.value.retryable and error.value.retry_after == 7 and not error.value.uncertain


@pytest.mark.asyncio
async def test_inbox_order_does_not_depend_on_json_object_key_order():
    store = InMemoryCustomerStore()
    await store.notify("b", "t", "n")
    claim = await store.claim_sync("b")
    await store.save_page("b", claim, {"msg_list": [customer("z"), customer("a")], "next_cursor": "1"}, "kf-account")
    state = store.states["b"]
    state["inbox"] = dict(sorted(state["inbox"].items()))
    assert (await store.claim_message("b"))["msgid"] == "z"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["download", "upload"])
async def test_media_refreshes_expired_access_token_once(operation):
    tokens = 0

    def handle(request):
        nonlocal tokens
        if request.url.path.endswith("gettoken"):
            tokens += 1
            return httpx.Response(200, json={"access_token": str(tokens)})
        if request.url.params["access_token"] == "1":
            return httpx.Response(200, json={"errcode": 42001})
        if operation == "download":
            return httpx.Response(200, content=b"test image", headers={"content-type": "image/png"})
        return httpx.Response(200, json={"errcode": 0, "media_id": "media-refreshed"})

    async with httpx.AsyncClient(base_url="https://fake/", transport=httpx.MockTransport(handle)) as http:
        client = HttpCustomerServiceClient("corp", "secret", http)
        if operation == "download":
            assert await client.download_file("media") == (b"test image", "image/png")
        else:
            assert await client.upload_file("image.png", "image/png", b"data", "image") == "media-refreshed"
        assert tokens == 2


@pytest.mark.asyncio
@pytest.mark.fault
async def test_callback_storage_failure_does_not_acknowledge(tenant_config):
    from unittest.mock import AsyncMock
    container, fake, store, factory = await setup(tenant_config)
    store.notify = AsyncMock(side_effect=ConnectionError("injected persistence failure"))
    encrypted, params = encrypted_callback(
        "<xml><Event>kf_msg_or_event</Event><Token>sync-token</Token><OpenKfId>kf-account</OpenKfId></xml>")
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(container)),
                                     base_url="http://test") as web:
            response = await web.post("/api/v1/channels/kf-bot/webhook",
                                      params=params,
                                      content=f"<xml><Encrypt>{encrypted}</Encrypt></xml>")
            assert response.status_code == 503
            assert response.json()["code"] == "customer_notification_not_saved"
            assert not fake.sent and not factory.models
    finally:
        await container.close()
