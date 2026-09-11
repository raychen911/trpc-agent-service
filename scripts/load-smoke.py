#!/usr/bin/env python3
"""Bounded concurrent HTTP load probe for an explicitly selected endpoint."""

from __future__ import annotations

import argparse
import concurrent.futures
import statistics
import time
import urllib.error
import urllib.request


def request(url: str, timeout: float) -> tuple[int, float]:
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310 - operator input
            response.read()
            return response.status, (time.perf_counter() - started) * 1000
    except urllib.error.HTTPError as exc:
        return exc.code, (time.perf_counter() - started) * 1000
    except OSError:
        return 0, (time.perf_counter() - started) * 1000


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="fully qualified service URL")
    parser.add_argument("--path", default="/health/ready")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=5)
    args = parser.parse_args()
    if args.requests < 1 or args.concurrency < 1:
        parser.error("--requests and --concurrency must be positive")
    url = args.url.rstrip("/") + "/" + args.path.lstrip("/")
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        results = list(pool.map(lambda _: request(url, args.timeout), range(args.requests)))
    elapsed = time.perf_counter() - started
    latencies = sorted(latency for _, latency in results)
    successes = sum(200 <= status < 300 for status, _ in results)
    index = max(0, min(len(latencies) - 1, round(len(latencies) * 0.95) - 1))
    print(
        f"requests={args.requests} successes={successes} failures={args.requests - successes} "
        f"rps={args.requests / elapsed:.2f} p50_ms={statistics.median(latencies):.2f} "
        f"p95_ms={latencies[index]:.2f}"
    )
    return 0 if successes == args.requests else 1


if __name__ == "__main__":
    raise SystemExit(main())
