#!/usr/bin/env python3
"""Exercise the response-drop provider endpoint once and replay it explicitly.

The endpoint is deliberately outside the service process.  The first Feishu
send is accepted into its SQLite ledger and then loses the TCP response, so
the adapter must return ``ambiguous``.  The acceptance script queries the
ledger before making a second request.  That second request is the explicit
manual replay; the endpoint acknowledges the existing idempotency key without
creating a second side-effect row.

This module is intentionally small so the real fault gate can call
``run_acceptance`` and merge its result into the existing ambiguous scenario
contract.  It does not claim the PostgreSQL control-plane confirmation on its
own; callers that exercise the production control repository should set the
corresponding manual-confirmation fields only after that transaction passes.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import secrets
import sys
import tempfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

# Keep direct ``python scripts/ambiguous_provider_acceptance.py`` invocation
# equivalent to ``python -m scripts.ambiguous_provider_acceptance``.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from deploy.ambiguous_provider.server import AmbiguousProviderServer
from scripts.report_io import atomic_write_json
from trpc_service.channels.envelopes import DeliveryStatus, OutboundEnvelope, PayloadKind
from trpc_service.channels.feishu import FeishuAdapter
from trpc_service.config.secrets import LocalSecretProvider, SecretRef
from trpc_service.tenant.models import Channel, ChannelBinding

SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$")
UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
    r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)
REAL_REQUIRED_ENV = (
    "TRPC_REAL_DATABASE_DSN",
    "TRPC_REAL_GLOBAL_WORKER_DATABASE_DSN",
    "TRPC_REAL_TENANT_ID",
    "TRPC_REAL_BINDING_ID",
    "TRPC_REAL_RUN_ID",
)
PROVIDER_URL_ENV = "TRPC_FAULT_AMBIGUOUS_PROVIDER_URL"
PROVIDER_URL_ALIASES = (PROVIDER_URL_ENV, "TRPC_REAL_AMBIGUOUS_PROVIDER_URL")
STAGE_MARKERS = (
    "delivery.ambiguous_observed",
    "delivery.replay_confirmation_required",
    "delivery.replay_verified",
)


def _safe_url(value: str) -> str:
    if not isinstance(value, str) or len(value) > 2048 or any(char.isspace() for char in value):
        raise ValueError("provider endpoint URL is invalid")
    parsed = urlsplit(value)
    if (
        parsed.scheme.lower() not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or "\\" in value
    ):
        raise ValueError("provider endpoint URL is invalid")
    host = parsed.hostname.lower()
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = parsed.port
    default_port = 80 if parsed.scheme.lower() == "http" else 443
    netloc = host if port in (None, default_port) else f"{host}:{port}"
    return f"{parsed.scheme.lower()}://{netloc}{parsed.path.rstrip('/')}"


def _marker(name: str, *, status: str, reason: str) -> dict[str, str]:
    return {
        "name": name,
        "status": status,
        "reason": reason,
        "observed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


def _binding() -> ChannelBinding:
    return ChannelBinding(
        binding_id="ambiguous-provider-binding",
        tenant_id="ambiguous-provider-tenant",
        app_id="acceptance",
        channel=Channel.FEISHU,
        account_id="cli_ambiguous_provider",
        secret_refs={"app_secret": SecretRef(uri="literal://ambiguous-test-secret")},
        capabilities=frozenset({"text", "reply"}),
    )


def _envelope(outbound_id: str) -> OutboundEnvelope:
    return OutboundEnvelope(
        outbound_id=outbound_id,
        tenant_id="ambiguous-provider-tenant",
        binding_id="ambiguous-provider-binding",
        channel=Channel.FEISHU,
        target_id="ou_ambiguous_provider",
        session_id="ambiguous-provider-session",
        payload_kind=PayloadKind.TEXT,
        text="ambiguous provider acceptance",
    )


async def run_acceptance(
    provider_url: str,
    *,
    outbound_id: str = "0a57ad9b-7c77-4f00-b34e-9b2acdc47ec1",
    timeout_seconds: float = 5.0,
) -> dict[str, Any]:
    """Run one response-drop/query/idempotent-replay acceptance sequence."""

    base_url = _safe_url(provider_url)
    if UUID_RE.fullmatch(outbound_id) is None and SAFE_ID_RE.fullmatch(outbound_id) is None:
        raise ValueError("outbound id is invalid")
    if not 0.1 <= timeout_seconds <= 60:
        raise ValueError("timeout_seconds is outside the safe range")

    endpoint_hash = hashlib.sha256(base_url.encode("utf-8")).hexdigest()
    state_url = f"{base_url}/state/{quote(outbound_id, safe='')}"
    adapter: FeishuAdapter | None = None
    client = httpx.AsyncClient(timeout=timeout_seconds)
    try:
        adapter = FeishuAdapter(
            LocalSecretProvider(allow_literal=True),
            http_client=client,
            api_root=base_url,
        )
        binding = _binding()
        envelope = _envelope(outbound_id)

        first = await adapter.send(envelope, binding)
        first_ambiguous = (
            first.status == DeliveryStatus.AMBIGUOUS and first.provider_code == "transport_unknown"
        )
        first_state_response = await client.get(state_url)
        first_state = first_state_response.json() if first_state_response.status_code == 200 else {}
        first_recorded = (
            first_state_response.status_code == 200
            and isinstance(first_state, dict)
            and first_state.get("status") == "accepted"
            and first_state.get("receive_count") == 1
            and first_state.get("duplicate") is False
        )

        # No adapter retry occurs before the operator has queried the provider
        # ledger.  The second request below is the explicit manual replay.
        second = await adapter.send(envelope, binding)
        second_delivered = (
            second.status == DeliveryStatus.DELIVERED
            and second.provider_code == "0"
            and second.provider_message_id == outbound_id
        )
        final_state_response = await client.get(state_url)
        final_state = final_state_response.json() if final_state_response.status_code == 200 else {}
        idempotent = (
            final_state_response.status_code == 200
            and isinstance(final_state, dict)
            and final_state.get("receive_count") == 2
            and final_state.get("duplicate") is True
            and final_state.get("request_sha256") == first_state.get("request_sha256")
        )
    finally:
        await client.aclose()

    passed = first_ambiguous and first_recorded and second_delivered and idempotent
    reason = "response drop, ledger query, and explicit idempotent replay verified"
    if not passed:
        reason = "provider response-drop acceptance sequence did not satisfy the contract"
    return {
        "status": "pass" if passed else "fail",
        "mode": "real_ambiguous_provider_endpoint",
        "endpoint_sha256": endpoint_hash,
        "outbound_id_sha256": hashlib.sha256(outbound_id.encode("utf-8")).hexdigest(),
        "first_attempt_status": first.status.value,
        "first_attempt_provider_code": first.provider_code,
        "first_ledger_status": first_state.get("status") if isinstance(first_state, dict) else None,
        "first_ledger_receive_count": (
            first_state.get("receive_count") if isinstance(first_state, dict) else None
        ),
        "manual_confirmation_required": True,
        "automatic_replay_count": 0,
        "confirmed_replay_status": "pass" if second_delivered else "fail",
        "final_ledger_receive_count": (
            final_state.get("receive_count") if isinstance(final_state, dict) else None
        ),
        "idempotent_side_effect_count": 1 if idempotent else 0,
        "provider_ledger": {
            "accepted_count": 1 if first_recorded else 0,
            "delivery_attempt_count": (
                final_state.get("receive_count") if isinstance(final_state, dict) else None
            ),
            "duplicate_replay_count": (
                max(0, int(final_state.get("receive_count", 0)) - 1)
                if isinstance(final_state, dict)
                and isinstance(final_state.get("receive_count"), int)
                else 0
            ),
            "side_effect_count": 1 if idempotent else 0,
        },
        "stage_markers": [
            _marker(
                STAGE_MARKERS[0],
                status="pass" if first_ambiguous and first_recorded else "fail",
                reason=(
                    "provider accepted and committed before dropping the response"
                    if first_ambiguous and first_recorded
                    else "first provider attempt was not observed as ambiguous after ledger commit"
                ),
            ),
            _marker(
                STAGE_MARKERS[1],
                status="pass" if first_recorded else "fail",
                reason=(
                    "manual confirmation is required before replay"
                    if first_recorded
                    else "provider ledger could not confirm the accepted request"
                ),
            ),
            _marker(
                STAGE_MARKERS[2],
                status="pass" if second_delivered and idempotent else "fail",
                reason=(
                    "explicit replay acknowledged the existing idempotency key"
                    if second_delivered and idempotent
                    else "explicit replay or idempotency verification failed"
                ),
            ),
        ],
        "reason": reason,
    }


def _not_run_result(reason: str) -> dict[str, Any]:
    return {
        "status": "not_run",
        "mode": "real_ambiguous_provider_endpoint",
        "reason": reason,
        "manual_confirmation_required": True,
        "automatic_replay_count": 0,
        "confirmed_replay_status": "not_run",
        "provider_ledger": {"accepted_count": 0, "side_effect_count": 0},
        "stage_markers": [_marker(name, status="not_run", reason=reason) for name in STAGE_MARKERS],
    }


def _child_report(
    result: dict[str, Any],
    *,
    run_id: str,
    started_at: str,
    ended_at: str,
    nonce_sha256: str,
) -> dict[str, Any]:
    status = result.get("status")
    candidate = {
        # The direct field is the stable acceptance-script contract.  ``faults``
        # is a compatibility view for existing fault-gate child extraction.
        "ambiguous": result,
        "faults": {"ambiguous": result},
    }
    return {
        "schema_version": 1,
        "run_id": run_id,
        "run_nonce_sha256": nonce_sha256,
        "started_at": started_at,
        "ended_at": ended_at,
        "gate": "pass" if status == "pass" else status,
        "production_gate": "not_run",
        "candidate": candidate,
        "case_deltas": {
            "requested_phase": "ambiguous",
            "requested": ["ambiguous"],
            "passed": ["ambiguous"] if status == "pass" else [],
        },
        "reason": result.get("reason", ""),
    }


def _execution_environment_missing() -> list[str]:
    return [name for name in REAL_REQUIRED_ENV if not os.getenv(name, "").strip()]


def _execution_run_id() -> str:
    value = os.getenv("TRPC_REAL_RUN_ID", "").strip()
    if value and SAFE_ID_RE.fullmatch(value) is not None:
        return value
    return f"ambiguous-{secrets.token_hex(12)}"


@contextmanager
def _local_endpoint(ledger_path: Path | None) -> Iterator[str]:
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if ledger_path is None:
        temporary = tempfile.TemporaryDirectory(prefix="trpc-ambiguous-provider-")
        ledger_path = Path(temporary.name) / "ledger.sqlite3"
    server = AmbiguousProviderServer(("127.0.0.1", 0), ledger_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        if temporary is not None:
            temporary.cleanup()


def _run_endpoint_acceptance(
    provider_url: str, *, outbound_id: str, timeout_seconds: float
) -> dict[str, Any]:
    try:
        return asyncio.run(
            run_acceptance(
                provider_url,
                outbound_id=outbound_id,
                timeout_seconds=timeout_seconds,
            )
        )
    except (OSError, ValueError, httpx.HTTPError) as error:
        return _not_run_result(f"provider endpoint acceptance unavailable: {type(error).__name__}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="run against the explicitly configured acceptance endpoint",
    )
    parser.add_argument(
        "--project", default=os.getenv("TRPC_REAL_COMPOSE_PROJECT", "trpc-agent-service")
    )
    parser.add_argument("--provider-url")
    parser.add_argument("--outbound-id", default="0a57ad9b-7c77-4f00-b34e-9b2acdc47ec1")
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument(
        "--ledger",
        type=Path,
        help="SQLite path for an auto-started local endpoint (temporary by default)",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    del args.project  # Accepted for fault-gate symmetry; the endpoint is external.
    started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    run_id = _execution_run_id()
    nonce_sha256 = hashlib.sha256(secrets.token_bytes(32)).hexdigest()
    provider_url = args.provider_url or next(
        (
            os.getenv(name, "").strip()
            for name in PROVIDER_URL_ALIASES
            if os.getenv(name, "").strip()
        ),
        "",
    )
    if args.execute:
        missing = _execution_environment_missing()
        if missing:
            result = _not_run_result("missing real prerequisite: " + ", ".join(missing))
            ended_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
            report = _child_report(
                result,
                run_id=run_id,
                started_at=started_at,
                ended_at=ended_at,
                nonce_sha256=nonce_sha256,
            )
            if args.output is not None:
                atomic_write_json(args.output, report)
            print(json.dumps(report, ensure_ascii=True, sort_keys=True, indent=2) + "\n", end="")
            return 1
    if provider_url:
        result = _run_endpoint_acceptance(
            provider_url,
            outbound_id=args.outbound_id,
            timeout_seconds=args.timeout_seconds,
        )
    else:
        # A local endpoint makes the acceptance command self-contained for
        # deterministic development runs.  Production gate callers should
        # pass an independently deployed URL through the explicit flag/env.
        with _local_endpoint(args.ledger) as local_url:
            result = _run_endpoint_acceptance(
                local_url,
                outbound_id=args.outbound_id,
                timeout_seconds=args.timeout_seconds,
            )
    ended_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    report = _child_report(
        result,
        run_id=run_id,
        started_at=started_at,
        ended_at=ended_at,
        nonce_sha256=nonce_sha256,
    )
    rendered = json.dumps(report, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if args.output is not None:
        atomic_write_json(args.output, report)
    print(rendered, end="")
    return 0 if result.get("status") == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
