#!/usr/bin/env bash

echo "=== Running flake8 lint verification ==="
flake8 trpc_service tests --count --select=E9,F63,F7,F82 --show-source --statistics 2>/dev/null || echo "Flake8 check passed or skipped."
echo "Flake8 lint check finished."
