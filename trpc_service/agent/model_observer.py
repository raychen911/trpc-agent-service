# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Service-side accounting around each underlying SDK model attempt."""

from __future__ import annotations

from types import MethodType

_START_CALLBACK = "trpc_service_model_call_started"
_SUCCESS_CALLBACK = "trpc_service_model_call_succeeded"


def instrument_model_call_accounting(model):
    """Wrap one model instance without changing the SDK package.

    The SDK retry loop invokes ``_generate_async_impl`` once per provider
    attempt, so this boundary counts retries as real model attempts too.
    """
    if getattr(model, "_trpc_service_accounting_installed", False):
        return model
    original = model._generate_async_impl

    async def observed(_model, request, stream=False, ctx=None):
        agent_context = getattr(ctx, "agent_context", None)
        started = agent_context.get_metadata(_START_CALLBACK) if agent_context else None
        succeeded = agent_context.get_metadata(_SUCCESS_CALLBACK) if agent_context else None
        if started:
            await started()
        success_recorded = False
        async for response in original(request, stream, ctx):
            if (not success_recorded and not getattr(response, "partial", False)
                    and not getattr(response, "error_code", None)):
                if succeeded:
                    await succeeded()
                success_recorded = True
            yield response

    model._generate_async_impl = MethodType(observed, model)
    model._trpc_service_accounting_installed = True
    return model
