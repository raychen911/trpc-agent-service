import argparse
import ssl
from collections.abc import Sequence

import uvicorn

from trpc_service.config import get_settings
from trpc_service.version import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trpc-agent-service")
    parser.add_argument("--version", action="version", version=__version__)

    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve", help="start the HTTP service")
    settings = get_settings()
    serve.add_argument("--host", default=settings.host)
    serve.add_argument("--port", default=settings.port, type=int)
    serve.add_argument("--reload", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    if args.command == "serve":
        settings = get_settings()
        options: dict[str, object] = {
            "host": args.host,
            "port": args.port,
            "reload": args.reload,
        }
        if settings.tls_server_cert_file:
            options.update(
                ssl_certfile=settings.tls_server_cert_file,
                ssl_keyfile=settings.tls_server_key_file,
                ssl_ca_certs=settings.tls_client_ca_file,
                ssl_cert_reqs=(
                    ssl.CERT_REQUIRED if settings.tls_client_ca_file else ssl.CERT_NONE
                ),
            )
        uvicorn.run("trpc_service.web.app:app", **options)


if __name__ == "__main__":
    main()
