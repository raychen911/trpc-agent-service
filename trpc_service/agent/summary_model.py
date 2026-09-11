"""Public model adapter supplying the accounting contract for SDK summaries."""

from trpc_agent_sdk.configs import ModelRetryConfig
from trpc_agent_sdk.models import LLMModel
from trpc_agent_sdk.filter import BaseFilter, FilterType

from trpc_service.governance.model_filter import ModelCallFailed


class SummaryResultFilter(BaseFilter):
    """Never let an SDK error response become persisted summary text."""

    def __init__(self):
        super().__init__()
        self.type, self.name = FilterType.MODEL, "summary_result"

    async def run_stream(self, ctx, req, handle):
        finals = []
        async for envelope in handle():
            response = envelope.rsp
            if envelope.error or not envelope.is_continue or response is None or (response.error_code
                                                                                  or response.interrupted):
                raise ModelCallFailed("summary generation did not complete")
            if not response.partial:
                finals.append(envelope)
        for envelope in finals:
            yield envelope


class ConfiguredSummaryModel(LLMModel):

    def __init__(self, budgeted_model):
        super().__init__(model_name=budgeted_model.model.name, model_retry_config=ModelRetryConfig(num_retries=0))
        self.budgeted_model = budgeted_model
        self.add_one_filter(SummaryResultFilter())

    @classmethod
    def supported_models(cls):
        return []

    async def _generate_async_impl(self, request, stream=False, ctx=None):
        # SessionSummarizer builds an empty-config LlmRequest. Copying it here
        # supplies the same bounded contract as foreground calls, including cost.
        request = request.model_copy(deep=True)
        request.model = self.budgeted_model.model.name
        request.config = self.budgeted_model.accounting.generation_config()
        async for response in self.budgeted_model.model.generate_async(request, stream=stream, ctx=ctx):
            yield response
