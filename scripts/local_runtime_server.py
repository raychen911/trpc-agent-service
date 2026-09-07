"""Isolated loopback validation server; never used by production deployments."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from fastapi import FastAPI

from tenant_agent.main import create_app
from tenant_agent.models import AgentEvent
from tenant_agent.settings import Settings


def create_validation_app() -> FastAPI:
    app = create_app(Settings())
    marker = os.getenv("TAP_VALIDATION_PAUSE_MARKER")
    if marker:
        engine = app.state.container.engines.deterministic
        original_stream = engine.stream

        async def delayed_stream(**kwargs: Any) -> AsyncIterator[AgentEvent]:
            if kwargs["routed"].inbound.text == "local-validation-pause":
                await asyncio.to_thread(Path(marker).write_text, "entered-model", encoding="utf-8")
                await asyncio.sleep(120)
            async for event in original_stream(**kwargs):
                yield event

        engine.stream = delayed_stream
    return app
