"""Bounded local HTTP load smoke for the deterministic browser fallback."""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
import uuid
from collections import Counter

import httpx

EXPECTED_STATUSES = {"processed", "duplicate_completed", "policy_denied", "capacity_limited"}


async def run(
    *,
    base_url: str,
    binding_id: str,
    token: str,
    requests: int,
    concurrency: int,
) -> dict[str, object]:
    semaphore = asyncio.Semaphore(concurrency)
    latencies: list[float] = []
    failures: list[str] = []
    statuses: Counter[str] = Counter()

    async with httpx.AsyncClient(base_url=base_url, timeout=120.0) as client:

        async def send(index: int) -> None:
            async with semaphore:
                started = time.perf_counter()
                try:
                    response = await client.post(
                        f"/v1/channels/web/{binding_id}/webhook?synchronous=true",
                        headers={"x-webhook-token": token},
                        json={
                            "user_id": f"load-user-{index}",
                            "conversation_id": f"load-conversation-{index}",
                            "message_id": uuid.uuid4().hex,
                            "text": "load smoke",
                        },
                    )
                    response.raise_for_status()
                    status = response.json()["results"][0]["status"]
                    statuses[status] += 1
                    if status not in EXPECTED_STATUSES:
                        failures.append(f"application:{status}")
                except httpx.HTTPStatusError as exc:
                    failures.append(f"http:{exc.response.status_code}")
                except Exception as exc:  # safe class-only reporting
                    failures.append(exc.__class__.__name__)
                finally:
                    latencies.append(time.perf_counter() - started)

        wall_started = time.perf_counter()
        await asyncio.gather(*(send(index) for index in range(requests)))
        wall_seconds = time.perf_counter() - wall_started

    ordered = sorted(latencies)

    def percentile(fraction: float) -> float:
        if not ordered:
            return 0.0
        return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]

    report: dict[str, object] = {
        "requests": requests,
        "concurrency": concurrency,
        "failures": len(failures),
        "throughput_rps": round(requests / max(wall_seconds, 0.001), 2),
        "processed_throughput_rps": round(statuses["processed"] / max(wall_seconds, 0.001), 2),
        "policy_rejections": statuses["policy_denied"] + statuses["capacity_limited"],
        "mean_ms": round(statistics.fmean(latencies) * 1_000, 2),
        "p50_ms": round(percentile(0.50) * 1_000, 2),
        "p95_ms": round(percentile(0.95) * 1_000, 2),
        "p99_ms": round(percentile(0.99) * 1_000, 2),
        "error_types": sorted(set(failures)),
        "error_counts": dict(Counter(failures)),
        "application_statuses": dict(statuses),
    }
    print(json.dumps(report, ensure_ascii=False), flush=True)
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--binding-id", default="web-demo-001")
    parser.add_argument("--token", default="development-webhook-token")
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=20)
    args = parser.parse_args()
    if args.requests < 1 or args.concurrency < 1:
        parser.error("requests and concurrency must be positive")
    report = asyncio.run(
        run(
            base_url=args.base_url.rstrip("/"),
            binding_id=args.binding_id,
            token=args.token,
            requests=args.requests,
            concurrency=args.concurrency,
        )
    )
    if report["failures"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
