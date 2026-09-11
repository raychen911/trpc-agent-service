#!/usr/bin/env python3
"""Bounded, secret-safe capacity probe for an already running Gateway.

This is an observation aid, not a load generator: it sends a small, bounded
number of unique console messages and emits exactly one aggregate JSON object.
Unavailable optional observations stay ``null`` so a report never claims zero.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import Any, Mapping
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

RESULT_KEYS = (
    "concurrent_sessions",
    "successful_requests",
    "ingress_peak_rps",
    "latency_ms_p50",
    "latency_ms_p95",
    "average_tokens",
    "redis_ops",
    "postgres_transactions",
)


def percentile(values: list[int], percent: int) -> int | None:
    """Nearest-rank percentile, with no invented value for no observations."""
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, (len(ordered) * percent + 99) // 100 - 1))
    return ordered[index]


def _difference(before: int | None, after: int | None) -> int | None:
    if before is None or after is None or after < before:
        return None
    return after - before


def build_result(
    concurrent_sessions: int,
    successes: int,
    elapsed_seconds: float,
    latencies_ms: list[int],
    tokens_before: int | None,
    tokens_after: int | None,
    redis_before: int | None,
    redis_after: int | None,
    postgres_before: int | None,
    postgres_after: int | None,
) -> dict[str, int | float | None]:
    token_delta = _difference(tokens_before, tokens_after)
    return {
        "concurrent_sessions": concurrent_sessions,
        "successful_requests": successes,
        "ingress_peak_rps": None if elapsed_seconds <= 0 else round(successes / elapsed_seconds, 3),
        "latency_ms_p50": percentile(latencies_ms, 50),
        "latency_ms_p95": percentile(latencies_ms, 95),
        "average_tokens": None if token_delta is None or successes == 0 else round(token_delta / successes, 3),
        "redis_ops": _difference(redis_before, redis_after),
        "postgres_transactions": _difference(postgres_before, postgres_after),
    }


def _http_json(url: str,
               *,
               method: str = "GET",
               body: dict[str, Any] | None = None,
               headers: Mapping[str, str] | None = None,
               timeout: float = 15) -> tuple[int, dict[str, Any] | None]:
    encoded = None if body is None else json.dumps(body).encode("utf-8")
    request = Request(url, data=encoded, method=method)
    request.add_header("Accept", "application/json")
    if encoded is not None:
        request.add_header("Content-Type", "application/json")
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urlopen(request, timeout=timeout) as response:  # nosec B310: operator-provided local endpoint
            raw = response.read()
            return response.status, json.loads(raw) if raw else None
    except (URLError, TimeoutError, ValueError, OSError):
        return 0, None


def _daily_tokens(admin_url: str | None, admin_token: str | None, tenant_id: str) -> int | None:
    if not admin_url or not admin_token:
        return None
    day = datetime.now(UTC).date().isoformat()
    status, payload = _http_json(
        f"{admin_url.rstrip('/')}/admin/v1/tenants/{tenant_id}/usage?day={day}",
        headers={"X-TRPC-Admin-Token": admin_token},
    )
    if status != 200 or not isinstance(payload, dict) or not isinstance(payload.get("profiles"), list):
        return None
    total = 0
    for profile in payload["profiles"]:
        if not isinstance(profile, dict):
            return None
        input_tokens, output_tokens = profile.get("input_tokens"), profile.get("output_tokens")
        if not isinstance(input_tokens, int) or not isinstance(output_tokens, int):
            return None
        total += input_tokens + output_tokens
    return total


def _redis_commandstats(redis_url: str | None) -> int | None:
    if not redis_url:
        return None
    parsed = urlsplit(redis_url)
    if parsed.scheme not in {"redis", "rediss"} or not parsed.hostname or parsed.scheme == "rediss":
        return None
    try:
        with socket.create_connection((parsed.hostname, parsed.port or 6379), timeout=2) as client:
            client.sendall(b"*2\r\n$4\r\nINFO\r\n$12\r\ncommandstats\r\n")
            data = client.recv(65536).decode("utf-8", "replace")
    except OSError:
        return None
    total = 0
    found = False
    for line in data.splitlines():
        if "calls=" not in line:
            continue
        try:
            total += int(line.split("calls=", 1)[1].split(",", 1)[0])
            found = True
        except (IndexError, ValueError):
            return None
    return total if found else None


async def _postgres_transactions(database_url: str | None) -> int | None:
    if not database_url:
        return None
    try:
        import asyncpg

        dsn = database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
        connection = await asyncpg.connect(dsn, timeout=2)
        try:
            row = await connection.fetchrow("SELECT xact_commit + xact_rollback AS total "
                                            "FROM pg_stat_database WHERE datname = current_database()")
            return int(row["total"]) if row and row["total"] is not None else None
        finally:
            await connection.close()
    except Exception:  # optional observation must not reveal backend details
        return None


def _send_message(base_url: str, tenant_id: str, sequence: int) -> tuple[bool, int]:
    marker = uuid.uuid4().hex
    body = {
        "tenant_id": tenant_id,
        "user_id": f"capacity-{marker}",
        "conversation_id": f"capacity-{marker}",
        "message_id": f"capacity-{sequence}-{marker}",
        "message": "capacity probe",
    }
    started = time.monotonic()
    status, _ = _http_json(f"{base_url.rstrip('/')}/api/console/messages", method="POST", body=body)
    return status == 200, round((time.monotonic() - started) * 1000)


def run_probe(*, base_url: str, tenant_id: str, concurrency: int, duration: float,
              environ: Mapping[str, str]) -> dict[str, int | float | None]:
    admin_url = environ.get("TRPC_ADMIN_URL")
    admin_token = environ.get("TRPC_ADMIN_TOKEN")
    tokens_before = _daily_tokens(admin_url, admin_token, tenant_id)
    redis_before = _redis_commandstats(environ.get("TRPC_REDIS_URL"))
    postgres_before = asyncio.run(_postgres_transactions(environ.get("TRPC_DATABASE_URL")))
    started = time.monotonic()
    deadline = started + duration
    successes = 0
    latencies: list[int] = []
    sequence = 0
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        while time.monotonic() < deadline:
            futures = [
                executor.submit(_send_message, base_url, tenant_id, sequence + index) for index in range(concurrency)
            ]
            sequence += concurrency
            for future in as_completed(futures):
                succeeded, latency = future.result()
                if succeeded:
                    successes += 1
                    latencies.append(latency)
    elapsed = time.monotonic() - started
    tokens_after = _daily_tokens(admin_url, admin_token, tenant_id)
    redis_after = _redis_commandstats(environ.get("TRPC_REDIS_URL"))
    postgres_after = asyncio.run(_postgres_transactions(environ.get("TRPC_DATABASE_URL")))
    return build_result(concurrency, successes, elapsed, latencies, tokens_before, tokens_after, redis_before,
                        redis_after, postgres_before, postgres_after)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a bounded aggregate capacity probe.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--tenant-id", default="tenant_default")
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--duration", type=float, default=1.0)
    args = parser.parse_args(argv)
    if args.concurrency < 1 or args.concurrency > 32 or args.duration <= 0 or args.duration > 60:
        parser.error("concurrency must be 1..32 and duration must be >0..60")
    print(
        json.dumps(run_probe(base_url=args.base_url,
                             tenant_id=args.tenant_id,
                             concurrency=args.concurrency,
                             duration=args.duration,
                             environ=os.environ),
                   separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
