"""Run the Step 5 real-model acceptance flow without persisting secrets."""

from __future__ import annotations

import argparse
import os
import secrets
import tempfile
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from trpc_service.config.settings import ServiceSettings
from trpc_service.storage.database import Database
from trpc_service.web.app import create_app


def _require_success(response: Any, operation: str) -> dict[str, Any]:
    if not response.is_success:
        raise RuntimeError(f"{operation} failed with HTTP {response.status_code}: {response.text}")
    body = response.json()
    if not isinstance(body, dict):
        raise RuntimeError(f"{operation} returned an unexpected response")
    return body


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--model", required=True)
    args = parser.parse_args()

    if not args.base_url.startswith("https://"):
        raise SystemExit("--base-url must use HTTPS")
    if not os.environ.get("TRPC_AGENT_API_KEY"):
        raise SystemExit("TRPC_AGENT_API_KEY is not set")

    admin_key = secrets.token_urlsafe(32)
    session_key = secrets.token_urlsafe(32)
    os.environ["TRPC_STEP5_ADMIN_KEY"] = admin_key
    os.environ["TRPC_STEP5_SESSION_KEY"] = session_key

    with tempfile.TemporaryDirectory(prefix="trpc-step5-") as temporary_directory:
        database_path = Path(temporary_directory) / "acceptance.db"
        settings = ServiceSettings(
            _env_file=None,
            app_env="development",
            database_url=f"sqlite+aiosqlite:///{database_path.as_posix()}",
            model_provider="openai",
            model_name=args.model,
            model_base_url=args.base_url,
            model_api_key_ref="env://TRPC_AGENT_API_KEY",
            admin_api_key_ref="env://TRPC_STEP5_ADMIN_KEY",
            session_hmac_key_ref="env://TRPC_STEP5_SESSION_KEY",
        )
        application = create_app(settings=settings, database=Database(settings.database_url))
        admin_headers = {"X-Admin-API-Key": admin_key}

        with TestClient(application) as client:
            _require_success(
                client.post(
                    "/admin/tenants",
                    headers=admin_headers,
                    json={"tenant_id": "acceptance", "name": "Acceptance Tenant"},
                ),
                "tenant creation",
            )
            _require_success(
                client.post(
                    "/admin/tenants/acceptance/apps",
                    headers=admin_headers,
                    json={
                        "app_id": "assistant",
                        "name": "Acceptance Assistant",
                        "system_prompt": (
                            "You are a concise assistant. When arithmetic is requested, "
                            "you must use the calculator tool and report its result."
                        ),
                        "tool_policy": {"allow": ["calculator"]},
                    },
                ),
                "agent app creation",
            )

            chat_headers = {"X-Tenant-ID": "acceptance"}
            ordinary = _require_success(
                client.post(
                    "/v1/chat",
                    headers=chat_headers,
                    json={
                        "app_id": "assistant",
                        "user_id": "acceptance-user",
                        "session_id": "ordinary-chat",
                        "message": "Reply with exactly: platform-ok",
                    },
                ),
                "ordinary chat",
            )
            calculation = _require_success(
                client.post(
                    "/v1/chat",
                    headers=chat_headers,
                    json={
                        "app_id": "assistant",
                        "user_id": "acceptance-user",
                        "session_id": "calculator-chat",
                        "message": (
                            "Use the calculator tool to compute 12345 * 6789. "
                            "Do not calculate it mentally."
                        ),
                    },
                ),
                "calculator chat",
            )

    tool_events = calculation.get("tool_events", [])
    tool_types = {event.get("type") for event in tool_events if isinstance(event, dict)}
    tool_names = {event.get("name") for event in tool_events if isinstance(event, dict)}
    if "calculator" not in tool_names or not {"tool_call", "tool_result"}.issubset(tool_types):
        raise RuntimeError("the model response did not complete a calculator tool call")

    print("STEP5_REAL_MODEL=PASS")
    print(f"ORDINARY_REPLY={ordinary.get('reply', '')[:200]}")
    print(f"CALCULATOR_REPLY={calculation.get('reply', '')[:200]}")
    print(f"CALCULATOR_TOOL_EVENTS={len(tool_events)}")
    print(f"TRACE_IDS_PRESENT={bool(ordinary.get('trace_id') and calculation.get('trace_id'))}")


if __name__ == "__main__":
    main()
