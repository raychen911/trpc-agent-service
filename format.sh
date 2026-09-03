#!/usr/bin/env bash

echo "=== Formatting codebase styles ==="
yapf -ir trpc_service tests 2>/dev/null || echo "yapf format done or skipped"
echo "Code format finished."
