"""Command-line entry points for the local validation service."""

from __future__ import annotations

import argparse
import asyncio
from hashlib import sha256
import json
import os
import sys
import time
from dataclasses import dataclass, field
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import httpx
import uvicorn

from trpc_service.channels.hmac_auth import body_sha256, canonical_string, sign_request
from trpc_service.channels.contracts import Channel
from trpc_service.audit.models import AuditRecord, TenantScope
from trpc_service.config.settings import (
    ConfigurationError,
    PlatformSettings,
    RuntimeProfile,
    build_runtime_channel_binding,
    load_runtime_settings,
)
from trpc_service.storage.contracts import ConfigurationUnavailable, SecretUnavailable
from trpc_service.storage.models import NodeIdentity
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.web.app import create_app, create_shared_app
from trpc_service.config.settings import build_demo_settings
from trpc_service.storage.postgres.repositories import (
    PostgresAuditRepository,
    PostgresConfigurationRepository,
)


def build_serve_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trpc-agent-local-serve")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    return parser


def serve_main(argv: list[str] | None = None) -> int:
    args = build_serve_parser().parse_args(argv)
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        raise SystemExit("The local validation service may only listen on loopback.")
    app = create_app(os.environ)
    uvicorn.run(app, host=args.host, port=args.port)
    return 0


def build_shared_serve_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trpc-agent-shared-serve")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--node-id", required=True)
    return parser


def _shared_bind_is_allowed(host: str, environ: object) -> bool:
    if host in {"127.0.0.1", "localhost", "::1"}:
        return True
    return host == "0.0.0.0" and environ.get("TRPC_CONTAINERIZED") == "1"


def shared_serve_main(argv: list[str] | None = None) -> int:
    args = build_shared_serve_parser().parse_args(argv)
    if not _shared_bind_is_allowed(args.host, os.environ):
        raise SystemExit("The shared validation service may only listen on loopback.")
    runtime_environ = dict(os.environ)
    runtime_environ["TRPC_RUNTIME_PROFILE"] = "shared"
    runtime_environ["TRPC_NODE_ID"] = args.node_id
    app = create_shared_app(runtime_environ)
    uvicorn.run(app, host=args.host, port=args.port)
    return 0


async def _initialize_shared() -> None:
    runtime_environ = dict(os.environ)
    runtime_environ["TRPC_RUNTIME_PROFILE"] = "shared"
    runtime_environ["TRPC_NODE_ID"] = "shared-init"
    settings = load_runtime_settings(runtime_environ)
    if settings.profile != RuntimeProfile.SHARED or settings.database_url is None:
        raise SystemExit("Shared runtime configuration is required.")
    database = PostgresDatabase(settings.database_url.get_secret_value())
    try:
        await database.initialize_schema()
        await PostgresConfigurationRepository(database).seed(build_demo_settings())
    finally:
        await database.close()


def shared_init_main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(prog="trpc-agent-shared-init").parse_args(argv)
    import asyncio

    asyncio.run(_initialize_shared())
    return 0


def build_shared_channel_init_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trpc-agent-shared-channel-init")
    parser.add_argument(
        "--channel",
        choices=(Channel.FEISHU.value, Channel.WECOM.value),
        required=True,
    )
    return parser


async def _initialize_shared_channel_binding(channel: Channel):
    runtime_environ = dict(os.environ)
    runtime_environ["TRPC_RUNTIME_PROFILE"] = "shared"
    runtime_environ["TRPC_NODE_ID"] = "shared-channel-init"
    settings = load_runtime_settings(runtime_environ)
    if settings.profile != RuntimeProfile.SHARED or settings.database_url is None:
        raise ConfigurationUnavailable("Shared runtime configuration is required.")
    binding = build_runtime_channel_binding(channel, os.environ)
    database = PostgresDatabase(settings.database_url.get_secret_value())
    try:
        await database.verify_schema()
        await PostgresConfigurationRepository(database).seed(
            PlatformSettings(tenants=(), agents=(), bindings=(binding,))
        )
        return binding
    finally:
        await database.close()


def shared_channel_init_main(argv: list[str] | None = None) -> int:
    args = build_shared_channel_init_parser().parse_args(argv)
    try:
        binding = asyncio.run(_initialize_shared_channel_binding(Channel(args.channel)))
    except (ConfigurationError, ConfigurationUnavailable):
        print(
            json.dumps(
                {
                    "channel": args.channel,
                    "status": "failed",
                    "error": "configuration_unavailable",
                },
                separators=(",", ":"),
            )
        )
        return 2
    print(
        json.dumps(
            {
                "channel": args.channel,
                "binding_id": binding.binding_id,
                "identity_digest": binding.channel_identity_digest,
                "status": binding.status.value,
            },
            separators=(",", ":"),
        )
    )
    return 0


def build_channel_serve_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trpc-agent-channel-serve")
    parser.add_argument("--channel", choices=(Channel.FEISHU.value, Channel.WECOM.value), required=True)
    parser.add_argument("--node-id", required=True)
    return parser


def channel_serve_main(argv: list[str] | None = None) -> int:
    args = build_channel_serve_parser().parse_args(argv)
    from trpc_service.channels.runtime import run_channel_process

    try:
        return asyncio.run(
            run_channel_process(
                Channel(args.channel),
                NodeIdentity(node_id=args.node_id),
                os.environ,
            )
        )
    except (ConfigurationUnavailable, SecretUnavailable):
        print(json.dumps({"channel": args.channel, "readiness": "not_ready", "error": "configuration_unavailable"}, separators=(",", ":")))
        return 2


def build_trace_diagnostic_summary(
    scope: TenantScope,
    trace_id: UUID,
    records: list[AuditRecord],
) -> dict[str, object]:
    """Return only bounded and pseudonymous trace evidence."""

    tenant_digest = "sha256:" + sha256(scope.tenant_id.encode("utf-8")).hexdigest()[:16]
    items: list[dict[str, object]] = []
    for record in records:
        item = {
            "decision": record.decision.value,
            "channel": record.channel.value,
            "audit_kind": record.audit_kind,
            "first_claim_trace_id": (
                str(record.first_claim_trace_id) if record.first_claim_trace_id else None
            ),
            "owner_trace_id": str(record.owner_trace_id) if record.owner_trace_id else None,
            "execution_trace_id": (
                str(record.execution_trace_id) if record.execution_trace_id else None
            ),
            "message_generation": record.generation,
            "session_id": record.session_id,
            "adapter_node_id": record.adapter_node_id,
            "adapter_generation": record.adapter_generation,
            "delivery_id": str(record.delivery_id) if record.delivery_id else None,
            "delivery_attempt_no": record.delivery_attempt_no,
            "delivery_status": record.delivery_status,
            "error_type": record.error_type,
        }
        items.append({key: value for key, value in item.items() if value is not None})

    # Phase-eight safe rendering: trace reference digest, central stage graph,
    # stable errors and an explicit partial-telemetry marker (never a fake
    # complete trace when the audit evidence is missing).
    from trpc_service.observability import taxonomy
    from trpc_service.observability.context import trace_digest

    decision_outcomes = {
        "received": ("adapter.receive", "success"),
        "authorized": ("session.lock", "success"),
        "execution_started": ("runner.invoke", "success"),
        "succeeded": ("reply.compose", "success"),
        "delivered": ("delivery.result", "success"),
        "already_delivered": ("delivery.result", "success"),
        "delivery_failed": ("delivery.result", "failed"),
        "delivery_unknown": ("delivery.result", "unknown"),
        "agent_failed": ("runner.invoke", "failed"),
        "outcome_unknown": ("runner.invoke", "unknown"),
        "duplicate": ("idempotency.claim", "recovered"),
        "processing": ("idempotency.claim", "recovered"),
        "idempotency_conflict": ("idempotency.claim", "rejected"),
    }
    observed: dict[str, str] = {}
    for item in items:
        mapped = decision_outcomes.get(str(item.get("decision")))
        if mapped is not None:
            observed.setdefault(mapped[0], mapped[1])
    stage_graph = [
        {"stage": stage, "outcome": observed.get(stage, "not_applicable")}
        for stage in taxonomy.STAGES
    ]
    stable_errors = sorted(
        {str(item["error_type"]) for item in items if item.get("error_type")}
    )

    return {
        "tenant_digest": tenant_digest,
        "trace_id": str(trace_id),
        "trace_reference": trace_digest(trace_id),
        "record_count": len(items),
        "records": items,
        "stage_graph": stage_graph,
        "stable_errors": stable_errors,
        "partial_telemetry": not bool(items),
    }


def build_trace_diagnose_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trpc-agent-trace-diagnose")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--trace-id", required=True, type=UUID)
    parser.add_argument("--node-id", default="trace-diagnostic")
    return parser


async def _diagnose_trace(args: object, environ: object) -> dict[str, object]:
    settings = load_runtime_settings(environ)
    if settings.profile != RuntimeProfile.SHARED or settings.database_url is None:
        raise ConfigurationUnavailable("Shared runtime configuration is required.")
    database = PostgresDatabase(settings.database_url.get_secret_value())
    try:
        await database.verify_schema()
        scope = TenantScope(tenant_id=getattr(args, "tenant_id"))
        records = await PostgresAuditRepository(
            database, node_id=getattr(args, "node_id")
        ).list_by_trace(scope, getattr(args, "trace_id"))
        return build_trace_diagnostic_summary(scope, getattr(args, "trace_id"), records)
    finally:
        await database.close()


def trace_diagnose_main(argv: list[str] | None = None) -> int:
    args = build_trace_diagnose_parser().parse_args(argv)
    try:
        payload = asyncio.run(_diagnose_trace(args, os.environ))
    except ConfigurationUnavailable:
        payload = {"status": "failed", "error": "configuration_unavailable"}
        print(json.dumps(payload, separators=(",", ":")))
        return 2
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    return 0


@dataclass(frozen=True, slots=True)
class SignedRequest:
    url: str
    content: bytes = field(repr=False)
    headers: dict[str, str] = field(repr=False)


def build_signed_request(args: object, environ: object, *, timestamp: int | None = None) -> SignedRequest:
    secret_name = getattr(args, "secret_env")
    secret = environ.get(secret_name, "")
    if not secret:
        raise SystemExit("Runtime secret is unavailable.")
    parsed_url = urlsplit(getattr(args, "url"))
    if parsed_url.scheme != "http" or parsed_url.hostname not in {"127.0.0.1", "localhost", "::1"} or parsed_url.username or parsed_url.password:
        raise SystemExit("The local sender may only connect to a loopback HTTP URL.")
    payload = {
        "channel": "local_http",
        "external_message_id": getattr(args, "external_message_id"),
        "external_user_id": getattr(args, "external_user_id"),
        "conversation_type": getattr(args, "conversation_type"),
        "external_conversation_id": getattr(args, "external_conversation_id"),
        "text": getattr(args, "text"),
    }
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    stamp = str(timestamp if timestamp is not None else int(time.time()))
    binding_id = getattr(args, "binding_id")
    signature = sign_request(
        secret.encode("utf-8"),
        canonical_string(stamp, binding_id, payload["external_message_id"], body_sha256(raw)),
    )
    base_url = getattr(args, "url").rstrip("/")
    return SignedRequest(
        url=base_url + "/v1/local/messages",
        content=raw,
        headers={
            "content-type": "application/json",
            "x-channel-binding": binding_id,
            "x-request-timestamp": stamp,
            "x-signature": signature,
            "x-trace-id": getattr(args, "trace_id", None) or str(uuid4()),
        },
    )


def build_send_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trpc-agent-local-send")
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--binding-id", required=True)
    parser.add_argument("--secret-env", required=True)
    parser.add_argument("--external-message-id", required=True)
    parser.add_argument("--external-user-id", required=True)
    parser.add_argument("--conversation-type", choices=("direct", "group"), required=True)
    parser.add_argument("--external-conversation-id", required=True)
    parser.add_argument("--text", required=True)
    parser.add_argument("--trace-id")
    return parser


def _post_local(request: SignedRequest) -> httpx.Response:
    with httpx.Client(trust_env=False) as client:
        return client.post(request.url, content=request.content, headers=request.headers, timeout=35)


def send_main(argv: list[str] | None = None) -> int:
    args = build_send_parser().parse_args(argv)
    request = build_signed_request(args, os.environ)
    try:
        response = _post_local(request)
    except httpx.HTTPError:
        print(json.dumps({"status": "failed", "error": {"code": "transport_error", "message": "Local service request failed."}}))
        return 2
    try:
        payload = response.json()
    except (ValueError, json.JSONDecodeError):
        print(json.dumps({"status": "failed", "error": {"code": "invalid_response", "message": "Local service returned an invalid response."}}))
        return 2
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    return 0 if response.status_code < 400 else 1


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments:
        raise SystemExit("A command is required.")
    command, rest = arguments[0], arguments[1:]
    commands = {
        "shared-init": shared_init_main,
        "shared-channel-init": shared_channel_init_main,
        "shared-serve": shared_serve_main,
        "channel-serve": channel_serve_main,
        "trace-diagnose": trace_diagnose_main,
        "local-serve": serve_main,
        "local-send": send_main,
    }
    try:
        handler = commands[command]
    except KeyError:
        raise SystemExit("Unknown command.") from None
    return handler(rest)


if __name__ == "__main__":
    raise SystemExit(main())
