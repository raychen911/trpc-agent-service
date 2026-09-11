"""Real IM/model deployment acceptance; synthetic channel submission was removed."""

from trpc_service.live_acceptance import main


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
