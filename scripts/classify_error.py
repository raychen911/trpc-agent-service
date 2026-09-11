#!/usr/bin/env python3
"""Helper script for acceptance tests to classify errors.

Usage:
    python classify_error.py gateway <http_code> <response_json_file> <marker>
    python classify_error.py worker <http_code> <response_json_file> <marker>

Exit codes:
    0 - success (marker found)
    1 - retry (transient error)
    2 - fail (non-retryable error)
"""

from __future__ import annotations

import json
import sys


def main() -> int:
    if len(sys.argv) != 5:
        print("Usage: classify_error.py <gateway|worker> <http_code> <json_file> <marker>", file=sys.stderr)
        return 2

    mode = sys.argv[1]
    http_code = sys.argv[2]
    json_file = sys.argv[3]
    marker = sys.argv[4]

    # Read JSON response
    try:
        with open(json_file) as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"fail:JSON error: {e}", file=sys.stderr)
        return 2

    # Import classifier
    from acceptance_error_classifier import (
        RetryDecision,
        classify_gateway_response,
        classify_worker_response,
    )

    # Classify based on mode
    if mode == "gateway":
        response_text = data.get("response", "")
        decision, reason = classify_gateway_response(http_code, response_text, marker)
    elif mode == "worker":
        error_code = data.get("error_code")
        response_text = data.get("response", "")
        decision, reason = classify_worker_response(http_code, error_code, response_text, marker)
    else:
        print(f"fail:unknown mode: {mode}", file=sys.stderr)
        return 2

    # Map decision to exit code
    if decision == RetryDecision.SUCCESS:
        print(f"success:{reason}", file=sys.stderr)
        return 0
    elif decision == RetryDecision.RETRY:
        print(f"retry:{reason}", file=sys.stderr)
        return 1
    else:  # FAIL
        print(f"fail:{reason}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
