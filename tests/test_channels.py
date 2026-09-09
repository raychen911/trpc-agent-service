# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.

import httpx
import pytest

from trpc_service.channels import ChannelAuthenticationError
from trpc_service.channels import TelegramChannelAdapter


@pytest.mark.asyncio
async def test_telegram_update_normalization():
    adapter = TelegramChannelAdapter("token", "secret", client=httpx.AsyncClient())
    payload = {
        "update_id": 100,
        "message": {
            "message_id": 9,
            "from": {
                "id": 10
            },
            "chat": {
                "id": -20,
                "type": "supergroup"
            },
            "text": "hello",
        },
    }
    message = await adapter.normalize("binding", payload, {"x-telegram-bot-api-secret-token": "secret"})
    assert message.message_id == "100"
    assert message.is_group is True
    with pytest.raises(ChannelAuthenticationError):
        await adapter.normalize("binding", payload, {})
    await adapter.close()
