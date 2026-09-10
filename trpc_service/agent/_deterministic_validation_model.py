"""Deterministic, credential-free model used only by SDK validation."""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator

from trpc_agent_sdk.context import InvocationContext
from trpc_agent_sdk.models import LLMModel, LlmRequest, LlmResponse
from trpc_agent_sdk.types import Content, Part


_TOKEN_PATTERN = re.compile(r"Remember validation token ([A-Z0-9_-]+)\.", re.IGNORECASE)


class DeterministicValidationModel(LLMModel):
    """Return fixed responses derived solely from the SDK-provided history."""

    def __init__(self) -> None:
        super().__init__(model_name="offline-sdk-validation")
        self.call_count = 0

    @classmethod
    def supported_models(cls) -> list[str]:
        return [r"^offline-sdk-validation$"]

    async def _generate_async_impl(
        self,
        request: LlmRequest,
        stream: bool = False,
        ctx: InvocationContext | None = None,
    ) -> AsyncGenerator[LlmResponse, None]:
        del stream, ctx
        self.call_count += 1
        texts = [
            part.text
            for content in request.contents
            for part in (content.parts or [])
            if part.text
        ]
        latest = texts[-1] if texts else ""

        current_match = _TOKEN_PATTERN.search(latest)
        if current_match:
            response_text = f"stored:{current_match.group(1).upper()}"
        elif latest.strip() == "Recall the validation token.":
            remembered = next(
                (
                    match.group(1).upper()
                    for text in reversed(texts[:-1])
                    if (match := _TOKEN_PATTERN.search(text))
                ),
                None,
            )
            response_text = f"recalled:{remembered}" if remembered else "context-missing"
        else:
            response_text = "unsupported-validation-input"

        yield LlmResponse(
            content=Content(
                role="model",
                parts=[Part.from_text(text=response_text)],
            )
        )
