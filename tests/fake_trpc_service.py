#!/usr/bin/env python
"""Fake tRPC service for PID identity testing.

This module mimics the cmdline structure of trpc_service._cli but just sleeps.
Usage: python -m tests.fake_trpc_service <subcommand> --host <host> --port <port>
"""
import sys
import time


def main():
    # Parse arguments to validate structure
    args = sys.argv[1:]
    if len(args) < 5:
        print("Usage: fake_trpc_service <subcommand> --host <host> --port <port>", file=sys.stderr)
        sys.exit(1)

    subcommand = args[0]
    if subcommand not in ("worker", "gateway", "admin"):
        print(f"Invalid subcommand: {subcommand}", file=sys.stderr)
        sys.exit(1)

    # Just sleep to keep the process alive
    try:
        time.sleep(300)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
