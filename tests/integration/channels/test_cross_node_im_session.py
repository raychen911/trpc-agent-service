from __future__ import annotations

import pytest

from tests.integration.channels.multinode_support import TwoNodeIMHarness
from trpc_service.channels.contracts import Channel


@pytest.mark.asyncio
@pytest.mark.parametrize("channel", [Channel.FEISHU, Channel.WECOM])
@pytest.mark.parametrize("group", [False, True], ids=["direct", "group"])
async def test_three_turn_context_crosses_workers_and_group_sender_isolated(
    channel: Channel,
    group: bool,
) -> None:
    harness = await TwoNodeIMHarness.create(channel)
    conversation = f"{channel.value}-{'group' if group else 'direct'}-conversation"
    sender = f"{channel.value}-sender-a"
    try:
        texts = [
            "Remember validation token ALPHA.",
            "Recall the validation token.",
            "Recall the validation token.",
        ]
        results = []
        for index, text in enumerate(texts):
            event = harness.event(
                message_id=f"{channel.value}-turn-{group}-{index}",
                conversation_id=conversation,
                sender_id=sender,
                text=text,
                group=group,
            )
            results.append(
                await harness.adapters[index % 2].handle_provider_event(event)
            )
        sent_texts = [
            text for provider in harness.providers for _, text in provider.sent
        ]
        assert [result.safe_code for result in results] == ["reply_delivered"] * 3
        assert sorted(sent_texts) == ["recalled:ALPHA", "recalled:ALPHA", "stored:ALPHA"]
        assert harness.agent_calls == 3

        if group:
            isolated = harness.event(
                message_id=f"{channel.value}-isolated-sender",
                conversation_id=conversation,
                sender_id=f"{channel.value}-sender-b",
                text="Recall the validation token.",
                group=True,
            )
            result = await harness.adapters[1].handle_provider_event(isolated)
            assert result.safe_code == "reply_delivered"
            assert harness.providers[1].sent[-1][1] == "context-missing"
            assert harness.agent_calls == 4
    finally:
        await harness.close()
