"""Operational commands for the local reference deployment."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from typing import Any

from trpc_service.channels import CallbackRequest
from trpc_service.config import AppSettings
from trpc_service.runtime import (
    BudgetAccount,
    BudgetExceeded,
    InboundEnvelope,
    TenantContext,
    ToolCapability,
    ToolStatus,
)
from trpc_service.web.app import ServiceContainer, create_app
from trpc_service.web.schemas import AgentCreate, ChannelCreate, ReleaseCreate, TenantCreate


def _dump(value: Any) -> None:
    print(json.dumps(value, default=str, ensure_ascii=False, sort_keys=True))


def _seed(services: ServiceContainer) -> dict[str, Any]:
    created: list[str] = []
    for tenant_id, name in (("demo-acme", "Acme Support"), ("demo-globex", "Globex Operations")):
        try:
            services.create_tenant(TenantCreate(tenant_id=tenant_id, display_name=name))
            created.append(tenant_id)
        except Exception:
            continue
        try:
            services.create_agent(tenant_id, AgentCreate(agent_id="support", name=f"{name} Agent"))
            services.create_release(
                tenant_id,
                "support",
                ReleaseCreate(
                    version=1,
                    app_config={
                        "system_prompt": "You are a concise tenant-scoped support assistant."
                    },
                    model_config={"mode": "mock"},
                    tool_policy={"allow": ["ticket.lookup", "effect.create"]},
                    created_by="seed",
                    change_reason="initial deterministic demonstration release",
                ),
            )
            services.activate_release(tenant_id, "support", 1)
            services.create_binding(
                tenant_id,
                ChannelCreate(
                    binding_id="mock-support",
                    agent_id="support",
                    provider="mock",
                    external_account_id=f"{tenant_id}-account",
                    webhook_key=f"{tenant_id}-webhook-key-0123456789",
                    capabilities={"callback_secret": "demo-callback-secret"},
                ),
            )
        except Exception:
            continue
    return {"seeded": created or ["demo-acme", "demo-globex"]}


async def _demo_async(services: ServiceContainer) -> dict[str, Any]:
    _seed(services)
    request_id, trace_id = "req_demo_001", "a" * 32
    callback = await services.accept_callback(
        "mock",
        "demo-acme-webhook-key-0123456789",
        CallbackRequest(
            body={"message_id": "callback-1", "user_id": "alice", "text": "ticket 123"},
            headers={"x-mock-secret": "demo-callback-secret"},
        ),
        request_id,
        trace_id,
    )
    services.dispatch("demo-acme", request_id, trace_id)
    execution = (await services.process_published("demo-acme", request_id, trace_id))[0]
    services.dispatch("demo-acme", request_id, trace_id)
    delivery = await services.deliver_mock_replies("demo-acme", request_id, trace_id)
    # A second callback has the same provider idempotency key and therefore never
    # starts a second model execution.
    duplicate = await services.accept_callback(
        "mock",
        "demo-acme-webhook-key-0123456789",
        CallbackRequest(
            body={"message_id": "callback-1", "user_id": "alice", "text": "ticket 123"},
            headers={"x-mock-secret": "demo-callback-secret"},
        ),
        "req_demo_duplicate",
        trace_id,
    )
    # Create a separate fenced execution and show an ambiguous non-idempotent
    # provider operation is retained for human resolution, never auto-retried.
    context = TenantContext("demo-acme", request_id="req_unknown", trace_id=trace_id)
    unknown_inbox = services.runtime.accept_inbound(
        context,
        InboundEnvelope(
            tenant_id="demo-acme",
            channel_binding_id="mock-support",
            agent_id="support",
            session_id="ses_unknown",
            idempotency_key="demo:unknown",
            external_message_id="unknown",
            payload={"text": "effect"},
            config_version=1,
        ),
    ).inbox
    claim = services.runtime.claim_execution(context, unknown_inbox.inbox_id, "demo-worker")
    intent = services.runtime.prepare_tool(
        context,
        claim,
        tool_step=0,
        tool_name="effect.create",
        arguments={"name": "irreversible"},
        capability=ToolCapability.NON_RETRIABLE,
    )
    services.runtime.start_tool(context, claim, intent.tool_call_id)
    services.runtime.finish_tool(
        context,
        claim,
        intent.tool_call_id,
        status=ToolStatus.UNKNOWN,
        error_code="simulated_timeout",
    )
    # Hard budget atomically admits only one independent claim.
    budget_context = TenantContext("demo-globex", request_id="req_budget", trace_id=trace_id)
    services.runtime.put_budget_account(
        budget_context, BudgetAccount("demo-globex", "model", "tokens", limit_units=1)
    )
    first = services.runtime.accept_inbound(
        budget_context,
        InboundEnvelope(
            "demo-globex",
            "mock-support",
            "support",
            "ses_budget_1",
            "budget-1",
            "budget-1",
            {"text": "one"},
            config_version=1,
        ),
    ).inbox
    second = services.runtime.accept_inbound(
        budget_context,
        InboundEnvelope(
            "demo-globex",
            "mock-support",
            "support",
            "ses_budget_2",
            "budget-2",
            "budget-2",
            {"text": "two"},
            config_version=1,
        ),
    ).inbox
    services.runtime.claim_execution(
        budget_context, first.inbox_id, "budget-worker", budget_estimates={"model": 1}
    )
    try:
        services.runtime.claim_execution(
            budget_context, second.inbox_id, "budget-worker", budget_estimates={"model": 1}
        )
        budget_rejected = False
    except BudgetExceeded:
        budget_rejected = True
    isolation = {
        "acme_inboxes": len(services.runtime.snapshot(context)["inboxes"]),
        "globex_inboxes": len(services.runtime.snapshot(budget_context)["inboxes"]),
    }
    return {
        "tenants": ["demo-acme", "demo-globex"],
        "callback": callback,
        "reply": execution["reply"],
        "delivery": delivery,
        "duplicate_callback": duplicate["duplicate"],
        "execution_id": execution["execution_id"],
        "tenant_isolation": isolation,
        "trace_id": trace_id,
        "request_id": request_id,
        "unknown_non_idempotent_tool": services.runtime.tool_recovery_action(
            context, intent.tool_call_id
        ).value,
        "hard_budget_rejected": budget_rejected,
    }


def command_seed(_: argparse.Namespace) -> int:
    settings = AppSettings.from_env()
    settings.validate_startup(require_http_auth=False)
    _dump(_seed(ServiceContainer(settings)))
    return 0


def command_demo(_: argparse.Namespace) -> int:
    settings = AppSettings.from_env()
    settings.validate_startup(require_http_auth=False)
    _dump(asyncio.run(_demo_async(ServiceContainer(settings))))
    return 0


def command_api(_: argparse.Namespace) -> int:
    import uvicorn

    settings = AppSettings.from_env()
    uvicorn.run(
        create_app(ServiceContainer(settings)),
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
    )
    return 0


def command_dispatcher(args: argparse.Namespace) -> int:
    # In a multi-process deployment this command uses the shared SQL/Redis adapters.
    # Memory mode is still useful as a deterministic one-shot health check.
    settings = AppSettings.from_env()
    settings.validate_startup(require_http_auth=False)
    services = ServiceContainer(settings)
    if services.settings.runtime_backend == "memory":
        _seed(services)
    while True:
        _dump(
            {
                "published": services.dispatch_all(),
                "delivered": asyncio.run(services.process_delivery_published()),
            }
        )
        if args.once:
            break
        time.sleep(1)
    return 0


def command_worker(args: argparse.Namespace) -> int:
    settings = AppSettings.from_env()
    settings.validate_startup(require_http_auth=False)
    services = ServiceContainer(settings)
    if services.settings.runtime_backend == "memory":
        _seed(services)
    while True:
        tenant_id = "demo-acme" if services.settings.runtime_backend == "memory" else None
        _dump(
            {
                "processed": asyncio.run(services.process_published(tenant_id)),
                "storage_migrations": services.run_storage_migrations(),
            }
        )
        if args.once:
            break
        time.sleep(1)
    return 0


async def _run_wecom_aibot_gateway(args: argparse.Namespace) -> int:
    """Own Smart Bot WebSockets and their separate durable reply consumer group."""

    settings = AppSettings.from_env()
    settings.validate_startup(require_http_auth=False)
    services = ServiceContainer(settings)
    supervisor = services.aibot_supervisor()
    await supervisor.start()
    try:
        while True:
            published = services.dispatch_all()
            delivered = await services.process_delivery_published(
                provider_filter="wecom_aibot", consumer_group="trpc-agent-wecom-aibot"
            )
            if delivered or any(events for events in published.values()):
                _dump({"published": published, "delivered": delivered})
            if args.once:
                break
            await asyncio.sleep(1)
    finally:
        await supervisor.close()
    return 0


def command_wecom_aibot(args: argparse.Namespace) -> int:
    return asyncio.run(_run_wecom_aibot_gateway(args))


def command_migrate(_: argparse.Namespace) -> int:
    """Apply the executable Alembic revision (requires a PostgreSQL DATABASE_URL)."""

    from alembic.config import Config

    from alembic import command

    AppSettings.from_env().validate_startup(require_http_auth=False)
    command.upgrade(Config("alembic.ini"), "head")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m trpc_service._cli")
    sub = parser.add_subparsers(dest="command", required=True)
    for name, handler in (
        ("api", command_api),
        ("worker", command_worker),
        ("dispatcher", command_dispatcher),
        ("wecom-aibot", command_wecom_aibot),
        ("migrate", command_migrate),
        ("seed", command_seed),
        ("demo", command_demo),
    ):
        command = sub.add_parser(name)
        command.set_defaults(handler=handler)
        if name in {"worker", "dispatcher", "wecom-aibot"}:
            command.add_argument("--once", action=argparse.BooleanOptionalAction, default=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    sys.exit(main())
