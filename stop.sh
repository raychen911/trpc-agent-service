#!/usr/bin/env bash

echo "=== Stopping tRPC-Agent-Service ==="
pkill -f "trpc_service.web.app" || true
echo "Service stopped."
