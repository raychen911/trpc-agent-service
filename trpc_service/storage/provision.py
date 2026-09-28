"""Explicit infrastructure setup, executed with deployment-owned credentials."""

import asyncio

from trpc_service.config import get_settings
from trpc_service.storage.factory import build_storage_composition


async def provision_storage() -> None:
    """Provision configured vector databases and buckets, releasing every client."""

    composition = build_storage_composition(get_settings())
    try:
        await composition.provision()
        await composition.initialize()
    finally:
        await composition.close()


if __name__ == "__main__":
    asyncio.run(provision_storage())
