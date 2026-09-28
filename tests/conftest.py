from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from pydantic import SecretStr
from sqlalchemy import insert

from trpc_service.admin.models import ChannelAdapterType
from trpc_service.channels import (
    ChannelAdapter,
    ChannelBindingConfig,
    ChannelResponse,
    DeliveryReceipt,
    IncomingEnvelope,
    IncomingMessage,
    OutgoingMessage,
)
from trpc_service.config import Settings
from trpc_service.container import build_application_container
from trpc_service.storage import build_engine, build_session_factory
from trpc_service.web import create_app


def create_test_app(settings: Settings) -> FastAPI:
    """Compose the real application with test-selected settings."""

    engine = build_engine(settings)
    session_factory = build_session_factory(engine)
    container = build_application_container(
        settings=settings,
        session_factory=session_factory,
    )
    return create_app(settings, engine, container)


class CatalogOnlyTestAdapter(ChannelAdapter):
    """Mark fixture-only adapter implementations as deployed on the test node."""

    def __init__(self, channel_type: str) -> None:
        self._channel_type = channel_type

    @property
    def channel_type(self) -> str:
        return self._channel_type

    async def decode(
        self,
        envelope: IncomingEnvelope,
        binding: ChannelBindingConfig,
    ) -> IncomingMessage:
        raise NotImplementedError

    async def acknowledge(
        self,
        envelope: IncomingEnvelope,
        binding: ChannelBindingConfig,
    ) -> ChannelResponse:
        raise NotImplementedError

    async def deliver(
        self,
        message: OutgoingMessage,
        binding: ChannelBindingConfig,
    ) -> DeliveryReceipt:
        raise NotImplementedError


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def api_client(tmp_path: Path) -> AsyncIterator[httpx.AsyncClient]:
    database_path = tmp_path / "test.db"
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{database_path}",
        auto_create_schema=True,
        admin_bootstrap_token=SecretStr("test-platform-admin-token"),
        tenant_secret_master_key=SecretStr("01" * 32),
    )
    app = create_test_app(settings)
    # Binding APIs now verify both catalog activation and node-local deployment.
    app.state.container.channels.register(CatalogOnlyTestAdapter("web"))
    transport = httpx.ASGITransport(app=app)

    async with app.router.lifespan_context(app):
        # These adapters represent implementations shipped in the test application.
        # Individual tests still register custom types through the management API.
        async with app.state.engine.begin() as connection:
            await connection.execute(
                insert(ChannelAdapterType),
                [
                    {
                        "channel_type": "web",
                        "display_name": "Generic Web",
                        "adapter_version": "test",
                    },
                    {
                        "channel_type": "wecom",
                        "display_name": "WeCom",
                        "adapter_version": "test",
                    },
                ],
            )
        async with httpx.AsyncClient(
                transport=transport,
                base_url="http://test",
                headers={
                    "Authorization": "Bearer test-platform-admin-token",
                    "X-Support-Reason": "automated control-plane test",
                },
        ) as client:
            yield client

    await app.state.engine.dispose()
