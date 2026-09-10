"""Opt-in provider integration tests.

Normal unit-test runs do not make network calls. Set RUN_REAL_MODEL_TESTS=1
and provide a newly generated provider key to execute the real test.
"""

from __future__ import annotations

import os
import unittest

from trpc_service.agent.model_client import ResponsesModelClient
from trpc_service.tenant.models import ModelConfig


@unittest.skipUnless(
    os.getenv("RUN_REAL_MODEL_TESTS") == "1",
    "set RUN_REAL_MODEL_TESTS=1 for the opt-in provider test",
)
class RealModelIntegrationTests(unittest.TestCase):
    def test_responses_api_with_configured_model(self):
        base_url = os.environ["CPA_BASE_URL"]
        model = os.environ["CPA_MODEL"]
        config = ModelConfig(
            provider="openai-compatible",
            model=model,
            base_url=base_url,
            api_key_env="OPENAI_API_KEY",
            wire_api=os.getenv("CPA_WIRE_API", "responses"),
            timeout_ms=int(os.getenv("CPA_TIMEOUT_MS", "30000")),
        )
        client = ResponsesModelClient.from_config(config)
        self.assertIsInstance(client, ResponsesModelClient)
        response = client.generate_with_usage(
            model=model,
            system_prompt="Reply with the single word OK.",
            conversation=[{"role": "user", "content": "Health check"}],
            temperature=0,
            max_output_tokens=16,
        )
        self.assertTrue(response.text)
