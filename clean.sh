#!/usr/bin/env bash

echo "=== Cleaning temporary build artifacts & caches ==="
rm -rf build/ dist/ *.egg-info .pytest_cache .coverage htmlcov/
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true
echo "Clean completed."
