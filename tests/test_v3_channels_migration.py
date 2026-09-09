"""Telegram/WeCom fake-channel and migration state-machine tests.

Telegram uses httpx MockTransport and WeCom uses FakeWeComClient. The migration
test uses an in-memory checkpoint store. No real bot, database or model is used.
"""

import asyncio
import httpx
import pytest

from trpc_service.channels import FakeWeComClient
from trpc_service.channels import TelegramChannelAdapter
from trpc_service.channels import WeComChannelAdapter
from trpc_service.channels import WeComChannelRuntime
from trpc_service.gateway import MessageKind
from trpc_service.gateway import Attachment
from trpc_service.gateway import OutboundMessage
from trpc_service.config import ChannelType
from trpc_service.migration import InMemoryMigrationStore
from trpc_service.migration import MigrationCoordinator
from trpc_service.migration import MigrationPhase
from trpc_service.migration.records import RecordMigrationProvider
from trpc_service.storage import InMemorySessionExecutionGuard
from trpc_service.storage import SessionLockTimeoutError

pytestmark = pytest.mark.component


@pytest.mark.asyncio
async def test_telegram_photo_and_document_normalize_to_attachments():
    adapter = TelegramChannelAdapter("token", "secret", client=httpx.AsyncClient())
    message = await adapter.normalize(
        "binding", {
            "update_id": 7,
            "message": {
                "message_id": 4,
                "chat": {
                    "id": 2,
                    "type": "private"
                },
                "from": {
                    "id": 3
                },
                "caption": "files",
                "photo": [{
                    "file_id": "p",
                    "file_unique_id": "pu",
                    "file_size": 9
                }],
                "document": {
                    "file_id": "d",
                    "file_unique_id": "du",
                    "file_name": "a.txt",
                    "mime_type": "text/plain"
                },
            },
        }, {"x-telegram-bot-api-secret-token": "secret"})
    assert [item.kind for item in message.attachments] == [MessageKind.IMAGE, MessageKind.FILE]
    await adapter.close()


@pytest.mark.asyncio
async def test_wecom_fake_client_and_normalized_contract():
    client = FakeWeComClient()
    adapter = WeComChannelAdapter(client.send_message, expected_bot_id="bot")
    message = await adapter.normalize(
        "binding", {
            "cmd": "aibot_msg_callback",
            "headers": {
                "req_id": "request-m"
            },
            "body": {
                "msgid": "m",
                "aibotid": "bot",
                "chattype": "single",
                "from": {
                    "userid": "u"
                },
                "msgtype": "text",
                "text": {
                    "content": "hello"
                }
            }
        }, {})
    assert message.binding_id == "binding"
    assert message.channel.value == "wecom"
    streamed = FakeWeComClient()

    async def send_chunk(conversation_id, text, reply_to_message_id):
        del reply_to_message_id
        return await streamed.send_message(conversation_id, text)

    chunking = WeComChannelAdapter(send_chunk, streamed.reply_stream, expected_bot_id="bot", max_chunk_bytes=8)
    result = await chunking.deliver(
        OutboundMessage(outbound_id="long",
                        request_id="r",
                        tenant_id="t",
                        binding_id="binding",
                        channel="wecom",
                        external_conversation_id="u",
                        reply_to_message_id="request-m",
                        text="中文消息长度分片"))
    assert result.delivered
    assert len(streamed.sent) > 1
    assert all(len(chunk.encode("utf-8")) <= 8 for _, chunk in streamed.sent)
    unsupported = await chunking.deliver(
        OutboundMessage(outbound_id="media",
                        request_id="r",
                        tenant_id="t",
                        binding_id="binding",
                        channel="wecom",
                        external_conversation_id="u",
                        text="",
                        attachments=[Attachment(attachment_id="a", kind="image", name="a.png", mime_type="image/png")]))
    assert unsupported.error_code == "wecom_outbound_attachment_not_supported"


@pytest.mark.asyncio
async def test_telegram_retry_after_is_exposed_to_outbox():

    async def handler(request):
        del request
        return httpx.Response(429, json={"parameters": {"retry_after": 3}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = TelegramChannelAdapter("token", "secret", client=client)
    result = await adapter.deliver(
        OutboundMessage(outbound_id="o",
                        request_id="r",
                        tenant_id="t",
                        binding_id="b",
                        channel=ChannelType.TELEGRAM,
                        external_conversation_id="c",
                        text="hello"))
    assert result.retryable is True
    assert result.retry_after_seconds == 3
    await client.aclose()


@pytest.mark.asyncio
async def test_migration_advances_all_phases_and_can_resume():
    store = InMemoryMigrationStore()
    provider = RecordMigrationProvider({"event-1": {"text": "hello"}}, {})
    coordinator = MigrationCoordinator(store, provider.steps)
    job = await coordinator.create("tenant", "record", "local-source", "local-target")
    for _ in range(6):
        job = await coordinator.advance(job.job_id)
    assert job.phase == MigrationPhase.COMPLETED
    assert (await coordinator.advance(job.job_id)).phase == MigrationPhase.COMPLETED
    assert provider.source == provider.target
    assert provider.read_target
    await coordinator.rollback(job.job_id)
    assert not provider.read_target


@pytest.mark.asyncio
async def test_missing_migration_provider_cannot_claim_success():
    coordinator = MigrationCoordinator(InMemoryMigrationStore())
    job = await coordinator.create("tenant", "session", "redis", "sql")
    with pytest.raises(NotImplementedError):
        await coordinator.advance(job.job_id)


@pytest.mark.asyncio
async def test_wecom_binding_has_one_owner_then_backup_takes_over():
    guard = InMemorySessionExecutionGuard()
    first_client = FakeWeComClient()
    second_client = FakeWeComClient()

    async def handler(frame):
        del frame

    first = WeComChannelRuntime("bot", first_client, guard, handler, wait_timeout=0.05)
    second = WeComChannelRuntime("bot", second_client, guard, handler, wait_timeout=0.05)
    first_stop = asyncio.Event()
    first_task = asyncio.create_task(first.run(first_stop))
    await asyncio.sleep(0)
    with pytest.raises(SessionLockTimeoutError):
        await second.run(asyncio.Event())
    first_stop.set()
    await first_task
    takeover_stop = asyncio.Event()
    takeover_task = asyncio.create_task(second.run(takeover_stop))
    await asyncio.sleep(0)
    assert second_client.connected is True
    takeover_stop.set()
    await takeover_task
