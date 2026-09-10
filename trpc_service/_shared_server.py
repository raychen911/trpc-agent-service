"""Module entry point used by two-process shared-state tests."""

from trpc_service._cli import shared_serve_main


if __name__ == "__main__":
    raise SystemExit(shared_serve_main())
