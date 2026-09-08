"""Command-line entry point for the service gateway."""

from __future__ import annotations

import argparse
import os
from typing import Optional
from trpc_service.config import ServiceSettings


def build_parser(settings: Optional[ServiceSettings] = None) -> argparse.ArgumentParser:
    settings = settings or ServiceSettings.from_env()
    parser = argparse.ArgumentParser(description="Run the tRPC-Agent multi-tenant service gateway")
    parser.add_argument("--host", default=settings.host, help="Gateway bind host")
    parser.add_argument("--port", default=settings.port, type=int, help="Gateway bind port")
    parser.add_argument("--tenants-config", help="Path to the tenant YAML/JSON configuration")
    parser.add_argument("--reload", action="store_true", help="Reload the process when source files change")
    return parser


def main(argv: Optional[list[str]] = None) -> None:
    settings = ServiceSettings.from_env()
    args = build_parser(settings).parse_args(argv)
    if args.tenants_config:
        os.environ["TRPC_SERVICE_TENANTS_CONFIG"] = args.tenants_config

    import uvicorn

    uvicorn.run(
        "trpc_service.web.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
