"""Public SDK filters enforce configured privacy before model and tool dispatch."""

import pytest
from trpc_agent_sdk.models import LlmRequest, LlmResponse
from trpc_agent_sdk.types import Content, Part, GenerateContentConfig
from trpc_agent_sdk.filter import FilterResult, run_stream_filters

from tests.integration.test_governance_filters import context
from trpc_service.governance.privacy import PrivacyFilter
from trpc_service.tenant import AuditPolicy


@pytest.mark.asyncio
async def test_privacy_applies_before_model_and_to_final_output():
    guard = PrivacyFilter("tenant_acme", "support", AuditPolicy(), secrets=["private-key-for-test"])
    request = LlmRequest(model="test",
                         contents=[
                             Content(role="user",
                                     parts=[Part.from_text(text="private-key-for-test alice@example.com 13812345678")])
                         ],
                         config=GenerateContentConfig())

    original_content = request.contents[0]

    async def model():
        assert "private-key-for-test" not in request.contents[0].parts[0].text
        assert "alice@example.com" not in request.contents[0].parts[0].text
        assert "13812345678" not in request.contents[0].parts[0].text
        yield FilterResult(
            rsp=LlmResponse(content=Content(role="model", parts=[Part.from_text(text="private-")]), partial=True))
        yield FilterResult(rsp=LlmResponse(content=Content(
            role="model", parts=[Part.from_text(text="private-key-for-test bob@example.com")]),
                                           partial=False))

    results = [value async for value in run_stream_filters(context(), request, [guard], model)]
    assert len(results) == 1
    assert results[0].content.parts[0].text == "[REDACTED_SECRET] [REDACTED_EMAIL]"
    assert "alice@example.com" in original_content.parts[0].text


def test_privacy_flags_and_structured_tool_data():
    enabled = PrivacyFilter("tenant_acme", "support", AuditPolicy())
    assert enabled.value({"text": "password=hidden user@example.com"}) == {
        "text": "password=[REDACTED_SECRET] [REDACTED_EMAIL]"
    }
    disabled = PrivacyFilter("tenant_acme", "support", AuditPolicy(redact_pii=False, redact_secrets=False))
    assert disabled.text("password=hidden user@example.com") == "password=hidden user@example.com"
