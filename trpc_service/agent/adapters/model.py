"""Retain safe provider failure classification across SDK error events."""

from collections.abc import AsyncGenerator

from openai import APIStatusError
from trpc_agent_sdk.models import LlmRequest, LlmResponse, OpenAIModel
from trpc_agent_sdk.agents import InvocationContext

from trpc_service.agent.recovery import PermanentOperationError


class ModelConfigurationError(PermanentOperationError):
    """A bounded public code; never retain a provider body or credential."""

    def __init__(self, error_code: str) -> None:
        self.error_code = error_code
        super().__init__(error_code)


def configuration_failure(error: APIStatusError) -> ModelConfigurationError | None:
    """Only classify structured provider status/code, never match response text."""
    if error.code == "AllocationQuota.FreeTierOnly":
        return ModelConfigurationError("MODEL_FREE_QUOTA_EXHAUSTED")
    codes = {
        400: "MODEL_REQUEST_INVALID",
        401: "MODEL_AUTHENTICATION_FAILED",
        403: "MODEL_ACCESS_DENIED",
        404: "MODEL_NOT_FOUND",
        422: "MODEL_REQUEST_INVALID",
    }
    code = codes.get(error.status_code)
    return None if code is None else ModelConfigurationError(code)


class PlatformOpenAIModel(OpenAIModel):  # type: ignore[misc]
    """Capture typed failures before SDK 1.1.19 reduces them to error events.

    One instance is owned by one request-scoped Runner. The SDK's retry layer
    still owns transient retries; the platform stops requeuing invalid config.
    """

    configuration_error: ModelConfigurationError | None = None

    async def _generate_async_impl(
        self,
        request: LlmRequest,
        stream: bool = False,
        ctx: InvocationContext | None = None,
    ) -> AsyncGenerator[LlmResponse, None]:
        self.configuration_error = None
        try:
            async for response in super()._generate_async_impl(request, stream, ctx):
                yield response
        except APIStatusError as error:
            self.configuration_error = configuration_failure(error)
            if self.configuration_error is not None:
                # The SDK logs exception text and tracebacks. Preserve HTTP
                # status for its retry policy, but drop the provider's body.
                code = self.configuration_error.error_code
                raise APIStatusError(code, response=error.response, body={"code": code}) from None
            raise
