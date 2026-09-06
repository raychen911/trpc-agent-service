"""Run an opt-in real OpenAI-compatible Responses API smoke test.

The script never prints the API key or raw provider response. It is skipped
unless the caller explicitly sets RUN_REAL_MODEL_TESTS=1.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from time import monotonic
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from trpc_service.agent.model_client import ModelClientError, ResponsesModelClient
from trpc_service.tenant.models import ModelConfig


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"{name} must be set")
    return value


def _run_success(client: ResponsesModelClient, model: str) -> dict[str, object]:
    started = monotonic()
    response = client.generate_with_usage(
        model=model,
        system_prompt="Reply with the single word OK.",
        conversation=[{"role": "user", "content": "Health check"}],
        temperature=0,
        max_output_tokens=16,
    )
    elapsed_ms = int((monotonic() - started) * 1000)
    if not response.text:
        raise RuntimeError("Responses API returned empty text")
    if not response.model:
        raise RuntimeError("Responses API response did not include the actual model id")
    return {
        "status": "ok",
        "requested_model": model,
        "actual_model": response.model,
        "wire_api": client.wire_api,
        "endpoint": client.base_url.rstrip("/")
        + ("/chat/completions" if client.wire_api in {"chat", "chat_completions"} else "/responses"),
        "elapsed_ms": elapsed_ms,
        "input_tokens": response.input_tokens,
        "output_tokens": response.output_tokens,
        "total_tokens": response.total_tokens,
        "timeout_ms": client.timeout_ms,
    }


def _run_invalid_model(base_url: str, api_key: str, model: str, timeout_ms: int) -> dict[str, str]:
    request = Request(
        base_url.rstrip("/") + "/responses",
        data=json.dumps(
            {
                "model": model,
                "input": [{"role": "user", "content": [{"type": "input_text", "text": "test"}]}],
                "max_output_tokens": 1,
            }
        ).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_ms / 1000):
            raise RuntimeError("invalid-model probe unexpectedly succeeded")
    except HTTPError as exc:
        if exc.code < 400:
            raise RuntimeError("invalid-model probe returned a non-error status") from exc
        return {"status": "ok", "error_status": str(exc.code)}
    except (TimeoutError, URLError) as exc:
        raise RuntimeError("invalid-model probe could not reach provider") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-invalid-model", action="store_true")
    args = parser.parse_args()
    if os.getenv("RUN_REAL_MODEL_TESTS", "").strip() != "1":
        print("skipped: set RUN_REAL_MODEL_TESTS=1 to call the configured provider")
        return 0

    base_url = _required("CPA_BASE_URL")
    api_key = _required("OPENAI_API_KEY")
    model = _required("CPA_MODEL")
    timeout_ms = int(os.getenv("CPA_TIMEOUT_MS", "30000"))
    config = ModelConfig(
        provider="openai-compatible",
        model=model,
        base_url=base_url,
        api_key_env="OPENAI_API_KEY",
        wire_api=os.getenv("CPA_WIRE_API", "responses"),
        timeout_ms=timeout_ms,
    )
    client = ResponsesModelClient.from_config(config)
    if not isinstance(client, ResponsesModelClient):
        raise SystemExit("ResponsesModelClient could not be created")

    results: dict[str, object] = {"success": _run_success(client, model)}
    if args.check_invalid_model:
        invalid_model = os.getenv("CPA_INVALID_MODEL", f"{model}-invalid")
        results["invalid_model"] = _run_invalid_model(base_url, api_key, invalid_model, timeout_ms)
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ModelClientError as exc:
        print(f"failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
