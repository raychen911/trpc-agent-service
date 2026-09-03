"""
Command Line Interface (CLI) for tRPC-Agent-Service.
"""

import argparse
import asyncio
import sys
import uvicorn
from trpc_service.version import __version__, __description__
from trpc_service.config.models import (
    TenantConfig,
    AgentAppConfig,
    InboundMessage,
    ChannelType,
)
from trpc_service.tenant.manager import tenant_manager
from trpc_service.agent.runner import agent_runner


def main():
    parser = argparse.ArgumentParser(
        prog="trpc-agent",
        description=f"tRPC-Agent Multi-tenant Service CLI (v{__version__}) - {__description__}",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    # Command: start-server
    server_parser = subparsers.add_parser("start-server", help="Start FastAPI Web & Webhook Server")
    server_parser.add_argument("--host", default="0.0.0.0", help="Binding host (default: 0.0.0.0)")
    server_parser.add_argument("--port", type=int, default=8000, help="Port to listen on (default: 8000)")
    server_parser.add_argument("--reload", action="store_true", help="Enable hot reload for development")

    # Command: create-tenant
    tenant_parser = subparsers.add_parser("create-tenant", help="Create a new tenant")
    tenant_parser.add_argument("--id", required=True, help="Tenant ID")
    tenant_parser.add_argument("--name", required=True, help="Tenant Display Name")
    tenant_parser.add_argument("--budget", type=int, default=1_000_000, help="Daily Token Budget")

    # Command: chat
    chat_parser = subparsers.add_parser("chat", help="Interactive terminal chat with an agent")
    chat_parser.add_argument("--tenant", default="default_corp", help="Tenant ID")
    chat_parser.add_argument("--user", default="cli_user", help="User ID")

    # Command: version
    subparsers.add_parser("version", help="Show system version")

    args = parser.parse_args()

    if args.command == "version" or not args.command:
        print(f"tRPC-Agent Multi-tenant Platform v{__version__}")
        if not args.command:
            parser.print_help()
        sys.exit(0)

    if args.command == "start-server":
        print(f"Starting tRPC-Agent Gateway on http://{args.host}:{args.port}")
        uvicorn.run("trpc_service.web.app:app", host=args.host, port=args.port, reload=args.reload)

    elif args.command == "create-tenant":
        tenant = TenantConfig(
            tenant_id=args.id,
            name=args.name,
        )
        tenant.quota_policy.daily_token_budget = args.budget
        tenant_manager.register_tenant(tenant)
        print(f"Tenant successfully created: {tenant.tenant_id} ({tenant.name})")

    elif args.command == "chat":
        asyncio.run(_run_interactive_chat(args.tenant, args.user))


async def _run_interactive_chat(tenant_id: str, user_id: str):
    print(f"=== tRPC-Agent Interactive Terminal (Tenant: {tenant_id}, User: {user_id}) ===")
    print("Type 'exit' or 'quit' to end.\n")

    tenant = tenant_manager.get_tenant(tenant_id)
    if not tenant:
        tenant = TenantConfig(tenant_id=tenant_id, name=f"Tenant-{tenant_id}")
        tenant_manager.register_tenant(tenant)

    while True:
        try:
            prompt = input(f"[{user_id}] > ").strip()
            if not prompt:
                continue
            if prompt.lower() in ["exit", "quit", "q"]:
                print("Exiting chat session. Goodbye!")
                break

            import uuid
            inbound = InboundMessage(
                trace_id=uuid.uuid4().hex[:16],
                tenant_id=tenant_id,
                channel_type=ChannelType.CUSTOM,
                raw_user_id=user_id,
                message_id=uuid.uuid4().hex[:16],
                content=prompt,
            )

            outbound = await agent_runner.execute(inbound)
            print(f"[Agent] > {outbound.content}\n")
        except (KeyboardInterrupt, EOFError):
            print("\nSession terminated.")
            break


if __name__ == "__main__":
    main()
