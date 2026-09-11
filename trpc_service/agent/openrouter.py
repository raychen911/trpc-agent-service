"""Budgeted OpenRouter text requests through public SDK and HTTPX extension points.

The complete advertised context window is reserved, rather than pretending an
approximate tokenizer is an upper bound. Real cost comes from usage.cost, including
cache/reasoning charges. SSE is buffered before parsing and IM final text is emitted
only after settlement; the upstream LlmAgent always requests a streaming model call.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
import json
import re

import httpx
from trpc_agent_sdk.configs import ModelRetryConfig
from trpc_agent_sdk.models import OpenAIModel
from trpc_agent_sdk.types import GenerateContentConfig, HttpOptions

from trpc_service.governance.budget import BudgetDenied, QUANTUM, digest, token_count, money
from trpc_service.governance.model_filter import ModelBudgetFilter, RequestEstimate, attach_budget_filter
from trpc_service.telemetry.runtime import operation, observe

ENDPOINT = "https://openrouter.ai/api/v1"
MODEL = "deepseek/deepseek-v3.2"
PRICE_ID = "openrouter-text-cap-v1"
MODEL_ID = "model_openrouter"


@dataclass
class UsageCapture:
    usage: dict | None = None
    dispatched: bool = False
    generation_id: str | None = None

    def evidence(self, response):
        if self.usage is None or response.partial or response.usage_metadata is None:
            return None
        incoming = token_count(self.usage.get("prompt_tokens"))
        outgoing = token_count(self.usage.get("completion_tokens"))
        native = response.usage_metadata
        if (native.prompt_token_count, native.candidates_token_count) != (incoming, outgoing):
            raise BudgetDenied("SDK and provider usage differ")
        amount = self.usage.get("cost")
        if not isinstance(amount, (int, Decimal)) or isinstance(amount, bool):
            return None
        amount = money(Decimal(amount).quantize(QUANTUM, rounding=ROUND_CEILING))
        # Only billing fields enter evidence; neither prompt nor provider message text.
        return incoming, outgoing, digest([incoming, outgoing, str(amount)]), amount


class OpenRouterAccounting:

    def __init__(self, context_window, max_output_tokens=128, *, model_name=MODEL, endpoint=ENDPOINT,
                 temperature=0, allowed_tools=()):
        if type(context_window) is not int or not 1 <= context_window <= 2_000_000:
            raise ValueError("invalid provider context window")
        if type(max_output_tokens) is not int or not 1 <= max_output_tokens <= 256:
            raise ValueError("bounded text contract permits at most 256 output tokens")
        if not isinstance(model_name, str) or not model_name or not isinstance(endpoint, str) or not endpoint:
            raise ValueError("model and endpoint are required")
        if not isinstance(temperature, (int, float)) or isinstance(temperature, bool) or not 0 <= temperature <= 2:
            raise ValueError("invalid model temperature")
        self.context_window, self.max_output_tokens = context_window, max_output_tokens
        self.model_name, self.endpoint, self.temperature = model_name, endpoint.rstrip("/"), temperature
        self.allowed_tools = frozenset(allowed_tools)
        if any(not isinstance(name, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.:-]{0,127}", name) is None
               for name in self.allowed_tools):
            raise ValueError("tool names must be registered identifiers")
        self.current = ContextVar("openrouter_usage_" + str(id(self)), default=None)

    @contextmanager
    def capture(self):
        state = UsageCapture()
        token = self.current.set(state)
        try:
            yield state
        finally:
            self.current.reset(token)

    def estimate(self, request):
        if request.model != self.model_name or request.config.max_output_tokens != self.max_output_tokens:
            raise BudgetDenied("model request differs from approved accounting contract")
        return RequestEstimate(digest(request.model_dump(mode="json")), self.context_window, self.max_output_tokens)

    def generation_config(self):
        return GenerateContentConfig(max_output_tokens=self.max_output_tokens,
                                     temperature=self.temperature,
                                     http_options=HttpOptions(timeout=40000,
                                                              extra_body={
                                                                  "reasoning": {
                                                                      "enabled": False
                                                                  },
                                                                  "provider": {
                                                                      "allow_fallbacks": False,
                                                                      "max_price": {
                                                                          "prompt": 1,
                                                                          "completion": 2
                                                                      }
                                                                  }
                                                              }))

    async def before_request(self, request):
        state = self.current.get()
        if state is None or state.dispatched:
            raise BudgetDenied("network request has no single-use budget permit")
        if str(request.url) != self.endpoint + "/chat/completions" or request.method != "POST":
            raise BudgetDenied("unexpected model endpoint")
        body = json.loads(request.content)
        if (body.get("model") != self.model_name or type(body.get("stream")) is not bool
                or body.get("max_tokens") != self.max_output_tokens
                or body.get("max_completion_tokens") != self.max_output_tokens or body.get("provider") != {
                    "allow_fallbacks": False,
                    "max_price": {
                        "prompt": 1,
                        "completion": 2
                    }
                } or body.get("reasoning") != {
                    "enabled": False
                }):
            raise BudgetDenied("wire request violates approved accounting contract")
        if any(key in body for key in ("plugins", "models", "route", "transforms", "functions")):
            raise BudgetDenied("unsupported provider features in accounting contract")
        self.validate_tools_and_messages(body)
        state.dispatched = True

    def validate_tools_and_messages(self, body):
        # Tool schemas and results consume the same reserved full context window.
        # Opt-in is supplied by trusted assembly, never by an IM request or model.
        definitions = body.get("tools", [])
        if not isinstance(definitions, list) or "tools" in body and not self.allowed_tools:
            raise BudgetDenied("tools require an explicit accounting contract")
        names = set()
        for item in definitions:
            function = item.get("function", {}) if isinstance(item, dict) else {}
            name = function.get("name") if isinstance(function, dict) else None
            if (not isinstance(item, dict) or item.get("type") != "function" or not isinstance(name, str)
                    or name not in self.allowed_tools or name in names):
                raise BudgetDenied("tool is outside the approved accounting contract")
            names.add(name)
        for message in body.get("messages", []):
            calls = message.get("tool_calls")
            if calls is not None:
                if not self.allowed_tools or message.get("role") != "assistant" or not isinstance(calls, list):
                    raise BudgetDenied("invalid tool-call history")
                for call in calls:
                    function = call.get("function", {}) if isinstance(call, dict) else {}
                    if (not isinstance(call, dict) or call.get("type") != "function" or not isinstance(function, dict)
                            or function.get("name") not in self.allowed_tools
                            or not isinstance(function.get("arguments"), str)):
                        raise BudgetDenied("tool history is outside the approved contract")
            content = message.get("content")
            if not isinstance(content, str) and not (content is None and calls):
                raise BudgetDenied("multimedia requests require a separate accounting contract")
            if message.get("role") == "tool" and not self.allowed_tools:
                raise BudgetDenied("tool results require an explicit accounting contract")

    async def after_response(self, response):
        state = self.current.get()
        if state is None or not state.dispatched or state.usage is not None:
            raise BudgetDenied("unexpected provider response")
        if response.status_code != 200:
            return
        with operation("model.provider_response"):
            await response.aread()
            if response.headers.get("content-type", "").startswith("text/event-stream"):
                values = [
                    json.loads(line[5:].strip(), parse_float=Decimal) for line in response.text.splitlines()
                    if line.startswith("data:") and line[5:].strip() not in {"", "[DONE]"}
                ]
            else:
                values = [json.loads(response.content, parse_float=Decimal)]
            identifiers = {value["id"] for value in values if isinstance(value.get("id"), str)
                           and re.fullmatch(r"gen-[A-Za-z0-9_-]{1,200}", value["id"])}
            if len(identifiers) == 1:
                state.generation_id = identifiers.pop()
            usages = [value["usage"] for value in values if isinstance(value.get("usage"), dict)]
            if usages:
                if any(usage != usages[-1] for usage in usages):
                    raise BudgetDenied("conflicting provider usage")
                state.usage = usage = usages[-1]
                for source, metric in (("prompt_tokens", "model.input_tokens"), ("completion_tokens",
                                                                                 "model.output_tokens")):
                    if type(usage.get(source)) is int:
                        observe(metric, usage[source])


async def context_window(client, model_name=MODEL, endpoint=ENDPOINT):
    """Read only public catalog metadata; never expose response bodies on failure."""
    try:
        response = await client.get(endpoint.rstrip("/") + "/models")
        response.raise_for_status()
        item = next(row for row in response.json()["data"] if row["id"] == model_name)
        window = item["context_length"]
        OpenRouterAccounting(window, model_name=model_name, endpoint=endpoint)
        return window
    except Exception:
        raise ValueError("approved model catalog metadata unavailable") from None


class BudgetedOpenRouter:
    """Owns its injected public HTTP client and must be closed with the Runner."""

    def __init__(self,
                 api_key,
                 ledger,
                 tenant_id,
                 app_id,
                 context_length,
                 *,
                 model_name=MODEL,
                 model_id=MODEL_ID,
                 price_id=PRICE_ID,
                 base_url=ENDPOINT,
                 temperature=0,
                 transport=None,
                 max_output_tokens=128,
                 allowed_tools=(),
                 max_calls=1):
        if type(max_calls) is not int or not 1 <= max_calls <= 16:
            raise ValueError("model call limit must be between 1 and 16")
        self.accounting = OpenRouterAccounting(context_length,
                                               max_output_tokens,
                                               model_name=model_name,
                                               endpoint=base_url,
                                               temperature=temperature,
                                               allowed_tools=allowed_tools)
        self.client = httpx.AsyncClient(timeout=40,
                                        follow_redirects=False,
                                        transport=transport,
                                        event_hooks={
                                            "request": [self.accounting.before_request],
                                            "response": [self.accounting.after_response]
                                        })
        self.model = attach_budget_filter(
            OpenAIModel(model_name=model_name,
                        api_key=api_key,
                        base_url=base_url,
                        model_retry_config=ModelRetryConfig(num_retries=0),
                        http_client_provider_factory=lambda: self,
                        client_args={"timeout": 40}),
            ModelBudgetFilter(tenant_id,
                              app_id,
                              ledger,
                              model_id,
                              price_id,
                              self.accounting.estimate,
                              max_calls=max_calls,
                              accounting=self.accounting))

    def create_http_client(self):
        return self.client

    async def close_http_client(self, client):
        # Public provider contract: the factory owns this shared client lifecycle.
        return None

    async def close(self):
        await self.client.aclose()
