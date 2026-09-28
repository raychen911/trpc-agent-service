"""Provider configuration errors must survive conversion to SDK error events."""

from collections.abc import AsyncIterator

import httpx
from openai import APIStatusError
from pydantic import SecretStr
import pytest
from trpc_agent_sdk.models import LlmRequest, LlmResponse, OpenAIModel

from trpc_service.agent.adapters.model import configuration_failure, ModelConfigurationError
from trpc_service.agent.adapters.trpc import TRPCAgentRunner
from trpc_service.agent.recovery import RecoveryPolicy, FailureDisposition
from trpc_service.config import Settings
from trpc_service.skill import BuiltinSkillCatalog
from test_trpc_agent_runner import _context, UnusedToolInvoker


def provider_error(status: int, code: str = "provider-code") -> APIStatusError:
    return APIStatusError(
        "sensitive provider diagnostics",
        response=httpx.Response(status, request=httpx.Request("POST", "https://model.example/v1")),
        body={
            "code": code,
            "message": "sensitive provider diagnostics"
        },
    )


@pytest.mark.parametrize("status,code,expected", [
    (400, "AllocationQuota.FreeTierOnly", "MODEL_FREE_QUOTA_EXHAUSTED"),
    (403, "AllocationQuota.FreeTierOnly", "MODEL_FREE_QUOTA_EXHAUSTED"),
    (400, "bad-request", "MODEL_REQUEST_INVALID"),
    (401, "bad-key", "MODEL_AUTHENTICATION_FAILED"),
    (403, "forbidden", "MODEL_ACCESS_DENIED"),
    (404, "not-found", "MODEL_NOT_FOUND"),
    (422, "validation", "MODEL_REQUEST_INVALID"),
    (429, "limit", None),
    (500, "server-error", None),
    (503, "unavailable", None),
])
def test_structured_provider_errors_have_safe_bounded_classification(status, code, expected):
    result = configuration_failure(provider_error(status, code))
    if expected is None:
        assert result is None
    else:
        assert result.error_code == expected
        assert "sensitive" not in str(result)
        assert RecoveryPolicy().classify_agent(result).disposition is FailureDisposition.PERMANENT


@pytest.mark.anyio
async def test_real_sdk_retains_permanent_provider_failure_without_context_leak(
        monkeypatch, caplog):

    async def fail(self: object, request: LlmRequest, stream: bool,
                   ctx: object) -> AsyncIterator[LlmResponse]:
        raise provider_error(403, "AllocationQuota.FreeTierOnly")
        yield  # Make this the same asynchronous generator seam used by the SDK.

    monkeypatch.setattr(OpenAIModel, "_generate_async_impl", fail)
    runner = TRPCAgentRunner(Settings(_env_file=None, dashscope_api_key=SecretStr("test")),
                             skills=BuiltinSkillCatalog())
    with pytest.raises(ModelConfigurationError, match="MODEL_FREE_QUOTA_EXHAUSTED") as failure:
        await runner.run(_context(), UnusedToolInvoker())
    decision = RecoveryPolicy().classify_agent(failure.value)
    assert decision.disposition is FailureDisposition.PERMANENT
    assert decision.error_code == "MODEL_FREE_QUOTA_EXHAUSTED"
    assert "Failed to detach context" not in caplog.text
    assert "sensitive" not in str(failure.value)
    assert "sensitive provider diagnostics" not in caplog.text


@pytest.mark.parametrize("thinking", [None, True, False])
@pytest.mark.anyio
async def test_thinking_choice_reaches_sdk_http_options(thinking):
    from dataclasses import replace
    from trpc_service.agent.adapters.trpc import _close_sdk_runtime

    context = _context()
    context = replace(context, config=replace(context.config, model={"enable_thinking": thinking}))
    runner = TRPCAgentRunner(Settings(_env_file=None, dashscope_api_key=SecretStr("test")),
                             skills=BuiltinSkillCatalog())
    runtime = await runner._build_runtime(context, UnusedToolInvoker())
    try:
        config = runtime.model.generate_content_config
        options = runtime.model._extract_http_options(config)
        if thinking is None:
            assert "extra_body" not in options
        else:
            assert options["extra_body"]["enable_thinking"] is thinking
    finally:
        await _close_sdk_runtime(runtime)


@pytest.mark.parametrize("value", ["false", 0, None, {}])
def test_generation_policy_rejects_non_boolean_thinking(value):
    from pydantic import ValidationError
    from trpc_service.admin.schemas import (
        ModelCatalogCreate,
        ModelProfileCreate,
        ModelProfileUpdate,
    )
    from uuid import uuid4

    with pytest.raises(ValidationError):
        ModelProfileCreate(name="test",
                           model_catalog_id=uuid4(),
                           credential_id=uuid4(),
                           parameter_config={"enable_thinking": value})
    with pytest.raises(ValidationError):
        ModelProfileUpdate(parameter_config={"enable_thinking": value})
    with pytest.raises(ValidationError):
        ModelCatalogCreate(provider="bailian",
                           model_name="test",
                           display_name="test",
                           default_limits={"enable_thinking": value})


@pytest.mark.anyio
async def test_legacy_runtime_policy_cannot_coerce_a_thinking_string():
    from dataclasses import replace

    context = _context()
    context = replace(context, config=replace(context.config, model={"enable_thinking": "false"}))
    runner = TRPCAgentRunner(Settings(_env_file=None, dashscope_api_key=SecretStr("test")),
                             skills=BuiltinSkillCatalog())
    with pytest.raises(ValueError, match="enable_thinking must be a boolean"):
        await runner._build_runtime(context, UnusedToolInvoker())
