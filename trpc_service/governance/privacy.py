"""Tenant model-boundary redaction, including tool inputs and tool results."""

import re

from trpc_agent_sdk.filter import FilterType

from .filters import TenantBoundaryFilter


class PrivacyFilter(TenantBoundaryFilter):

    def __init__(self, tenant_id, app_id, policy, *, secrets=()):
        super().__init__(tenant_id, app_id, filter_type=FilterType.MODEL)
        self.name, self.policy = "tenant_privacy", policy
        self.secrets = tuple(sorted({value for value in secrets if len(value) >= 8}, key=len, reverse=True))

    def text(self, value):
        if self.policy.redact_secrets:
            for secret in self.secrets:
                value = value.replace(secret, "[REDACTED_SECRET]")
            value = re.sub(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+", "Bearer [REDACTED_SECRET]", value)
            value = re.sub(r"(?i)\b(api[_-]?key|password|bot[_-]?secret|access[_-]?token)\s*[:=]\s*[^\s,;]+",
                           r"\1=[REDACTED_SECRET]", value)
            value = re.sub(r"(\w+://[^\s/:]+:)[^\s@]+@", r"\1[REDACTED_SECRET]@", value)
        if self.policy.redact_pii:
            value = re.sub(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", "[REDACTED_EMAIL]", value)
            value = re.sub(r"(?<!\w)(?:\+?86[- ]?)?1[3-9]\d{9}(?!\w)", "[REDACTED_PHONE]", value)
        return value

    def value(self, value):
        if isinstance(value, str):
            return self.text(value)
        if isinstance(value, list):
            return [self.value(item) for item in value]
        if isinstance(value, dict):
            return {key: self.value(item) for key, item in value.items()}
        return value

    def content(self, content):
        if content is None:
            return
        for part in content.parts:
            if part.text is not None:
                part.text = self.text(part.text)
            if part.function_call is not None:
                part.function_call.args = self.value(part.function_call.args)
            if part.function_response is not None:
                part.function_response.response = self.value(part.function_response.response)

    async def run_stream(self, ctx, req, handle):
        await self.check(ctx)
        req.contents = [content.model_copy(deep=True) for content in req.contents]
        for content in req.contents:
            self.content(content)
        instruction = req.config.system_instruction
        if isinstance(instruction, str):
            req.config.system_instruction = self.text(instruction)
        elif instruction is not None:
            req.config.system_instruction = instruction.model_copy(deep=True)
            self.content(req.config.system_instruction)
        async for envelope in handle():
            response = envelope.rsp
            if response is not None:
                # Final-only IM delivery also prevents secrets split across deltas
                # from escaping a per-chunk regular expression.
                if response.partial:
                    continue
                self.content(response.content)
            yield envelope
