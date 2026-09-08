import argparse
import asyncio
import statistics
import time
import uuid

import httpx


async def main(
    url: str,
    token: str,
    concurrency: int,
    requests: int,
    sessions: int,
    input_tokens: int,
    output_tokens: int,
) -> None:
    semaphore = asyncio.Semaphore(concurrency)
    latencies: list[float] = []
    failures = 0

    overall_started = time.perf_counter()
    async with httpx.AsyncClient(timeout=120) as client:

        async def one(index: int) -> None:
            nonlocal failures
            async with semaphore:
                started = time.perf_counter()
                response = await client.post(
                    f"{url.rstrip('/')}/gateway/v1/messages",
                    headers={"x-gateway-token": token},
                    json={
                        "tenant_id": "load-tenant",
                        "agent_app_id": "load-agent",
                        "channel": "load",
                        "account_id": "load",
                        "external_message_id": str(uuid.uuid4()),
                        "sender_user_id": f"user-{index % sessions}",
                        "conversation_id": f"user-{index % sessions}",
                        "text": "capacity test",
                    },
                )
                latencies.append(time.perf_counter() - started)
                failures += int(response.status_code >= 400)

        await asyncio.gather(*(one(index) for index in range(requests)))
    ordered = sorted(latencies)
    duration = max(time.perf_counter() - overall_started, 0.001)
    successful = requests - failures
    estimated_tokens = successful * (input_tokens + output_tokens)

    def percentile(value: float) -> float:
        return ordered[min(len(ordered) - 1, int(len(ordered) * value))]

    print(
        {
            "requests": requests,
            "successful": successful,
            "failures": failures,
            "failure_rate": round(failures / requests, 6),
            "concurrency": concurrency,
            "sessions": sessions,
            "duration_seconds": round(duration, 3),
            "requests_per_second": round(requests / duration, 2),
            "estimated_tokens": estimated_tokens,
            "estimated_tokens_per_second": round(estimated_tokens / duration, 2),
            "mean_ms": round(statistics.mean(latencies) * 1000, 2),
            "p95_ms": round(percentile(0.95) * 1000, 2),
            "p99_ms": round(percentile(0.99) * 1000, 2),
        }
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--token", required=True)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--sessions", type=int, default=20)
    parser.add_argument("--input-tokens", type=int, default=100)
    parser.add_argument("--output-tokens", type=int, default=300)
    args = parser.parse_args()
    asyncio.run(
        main(
            args.url,
            args.token,
            args.concurrency,
            args.requests,
            args.sessions,
            args.input_tokens,
            args.output_tokens,
        )
    )
