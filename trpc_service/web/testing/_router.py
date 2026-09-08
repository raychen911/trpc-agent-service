"""Private synthetic-message endpoint, isolated from public webhook routing."""

from __future__ import annotations

import logging
import secrets
from typing import Any
from typing import Optional

from fastapi import APIRouter
from fastapi import Header
from pydantic import BaseModel
from pydantic import Field

from trpc_service.channels import InboundMessage
from trpc_service.web._dispatch import run_turn

logger = logging.getLogger(__name__)


class TestMessageRequest(BaseModel):
    """Synthetic channel message accepted only by the private test API."""

    user_id: str = Field(min_length=1)
    chat_id: str = Field(min_length=1)
    message_id: str = Field(min_length=1)
    text: str = ""
    chat_type: str = "private"
    channel: str = "qq"


def create_test_message_router(*, worker: Any, api_key: str, queue: Optional[Any] = None) -> APIRouter:
    """Build an opt-in router that never invokes a real channel adapter."""
    router = APIRouter(prefix="/internal/test", tags=["acceptance"])

    @router.post("/messages/{tenant_id}")
    async def test_message(
            tenant_id: str,
            message: TestMessageRequest,
            x_test_api_key: Optional[str] = Header(default=None),
    ) -> dict[str, Any]:
        if not x_test_api_key or not secrets.compare_digest(x_test_api_key, api_key):
            from fastapi import HTTPException

            raise HTTPException(status_code=401, detail="invalid test api key")
        if queue is not None:
            from fastapi import HTTPException

            raise HTTPException(status_code=503, detail="test message endpoint requires in-process Worker")
        if worker.resolve_tenant(tenant_id) is None:
            from fastapi import HTTPException

            raise HTTPException(status_code=404, detail="tenant not found or disabled")
        if message.channel != "qq":
            from fastapi import HTTPException

            raise HTTPException(status_code=400, detail="test endpoint currently supports channel=qq only")

        inbound = InboundMessage(
            channel=message.channel,
            chat_id=message.chat_id,
            chat_type=message.chat_type,
            sender_id=message.user_id,
            message_id=message.message_id,
            text=message.text,
            metadata={
                "simulated": True,
                "qq_scope": "c2c" if message.chat_type == "private" else "group",
            },
        )
        try:
            text = await run_turn(
                tenant_id=tenant_id,
                channel=message.channel,
                inbound=inbound,
                worker=worker,
            )
        except Exception as exc:  # noqa: BLE001 - response must not leak internals
            logger.exception("synthetic test message failed for tenant=%s", tenant_id)
            from fastapi import HTTPException

            raise HTTPException(status_code=500, detail=type(exc).__name__) from exc
        return {
            "tenant_id": tenant_id,
            "channel": message.channel,
            "user_id": message.user_id,
            "chat_id": message.chat_id,
            "message_id": message.message_id,
            "reply": text,
            "simulated": True,
        }

    return router
