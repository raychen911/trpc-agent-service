#!/usr/bin/env bash

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"

echo "=== Starting tRPC-Agent-Service on http://${HOST}:${PORT} ==="
python -m uvicorn trpc_service.web.app:app --host "$HOST" --port "$PORT"
