"""Standalone action consumer with a deployment-owned, revisioned tool catalog."""

import asyncio
import importlib
import os
import re

from trpc_service.channels.persistence import ContextCipher
from trpc_service.worker import consume
from .actions import ActionService
from .action_worker import ActionDefinition, ActionWorker


def load_catalog(database):
    # This is operator environment configuration, never an HTTP/model parameter.
    reference = os.environ.get("TRPC_ACTION_CATALOG_FACTORY",
                               "trpc_service.governance.resource_actions:resource_catalog")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*", reference):
        raise ValueError("configure a deployment-owned TRPC_ACTION_CATALOG_FACTORY module:function")
    module, name = reference.split(":")
    factory = getattr(importlib.import_module(module), name)
    catalog = factory(database)
    if not isinstance(catalog, dict) or not catalog:
        raise ValueError("action catalog must contain explicitly registered tools")
    for key, definition in catalog.items():
        if (not isinstance(key, tuple) or len(key) != 3 or not isinstance(key[0], str)
                or not isinstance(definition, ActionDefinition) or key[1:] != (definition.name, definition.revision)
                or not all(callable(value) for value in (definition.prepare, definition.check, definition.execute))):
            raise ValueError("invalid tenant, tool revision or action implementation")
    return catalog


async def run_action_process(database, stop):
    if database.engine.dialect.name != "postgresql":
        raise ValueError("independent action processes require PostgreSQL row locks")
    keys = os.environ.get("TRPC_IM_CONTEXT_KEYS", "")
    cipher = ContextCipher([key.encode() for key in keys.split(",")])
    consumer = ActionWorker(ActionService(database, cipher), load_catalog(database))
    task = asyncio.create_task(consume(consumer.run_once, stop, poll_seconds=0.5))
    try:
        await stop.wait()
        try:
            await asyncio.wait_for(asyncio.shield(task), 20)
        except asyncio.TimeoutError:
            pass
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
