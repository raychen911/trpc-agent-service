#!/usr/bin/env bash

echo "=== Running unit test suite with coverage ==="
python -m pytest tests/ -v
