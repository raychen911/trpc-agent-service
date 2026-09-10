"""Run a standard-library concurrent load test against the Web UI endpoint."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def metrics(base_url: str) -> str:
    try:
        with urlopen(f"{base_url.rstrip('/')}/metrics", timeout=10) as response:
            return response.read().decode("utf-8", errors="replace")
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        return f"# metrics_error {exc}"


def metric_value(text: str, name: str, tenant: str) -> float:
    prefix = f"{name}{{"
    total = 0.0
    for line in text.splitlines():
        if not line.startswith(prefix) or f'tenant="{tenant}"' not in line:
            continue
        try:
            total += float(line.rsplit(" ", 1)[1])
        except (IndexError, ValueError):
            continue
    return total


def one_request(base_url: str, index: int, tenant: str, timeout: float, text_template: str) -> dict[str, object]:
    started = time.perf_counter()
    payload = json.dumps(
        {
            "channel": "web",
            "account_id": "web_demo",
            "user_id": f"load-user-{index}",
            "text": text_template.format(index=index),
        }
    ).encode("utf-8")
    req = Request(
        f"{base_url.rstrip('/')}/ui/api/chat",
        data=payload,
        headers={"Content-Type": "application/json", "X-Load-Tenant": tenant},
        method="POST",
    )
    try:
        with urlopen(req, timeout=timeout) as response:
            body = json.loads(response.read().decode("utf-8"))
            return {
                "status": response.status,
                "model_mode": body.get("model_mode"),
                "elapsed_ms": (time.perf_counter() - started) * 1000,
            }
    except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
        return {
            "status": None,
            "error": str(exc)[:300],
            "elapsed_ms": (time.perf_counter() - started) * 1000,
        }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--tenant", default="tenant_demo")
    parser.add_argument("--text-template", default="load test request {index}")
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    if args.requests < 1 or args.concurrency < 1:
        parser.error("--requests and --concurrency must be positive")

    before = metrics(args.base_url)
    started_at = datetime.now(UTC).isoformat()
    started = time.perf_counter()
    results: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [
            pool.submit(one_request, args.base_url, index, args.tenant, args.timeout, args.text_template)
            for index in range(args.requests)
        ]
        for future in as_completed(futures):
            results.append(future.result())
    elapsed = time.perf_counter() - started
    after = metrics(args.base_url)

    latencies = sorted(float(item["elapsed_ms"]) for item in results)
    successes = [item for item in results if item.get("status") == 200]

    def percentile(value: float) -> float:
        if not latencies:
            return 0.0
        index = min(len(latencies) - 1, max(0, round((len(latencies) - 1) * value)))
        return round(latencies[index], 2)

    report = {
        "started_at": started_at,
        "finished_at": datetime.now(UTC).isoformat(),
        "base_url": args.base_url,
        "requests": args.requests,
        "concurrency": args.concurrency,
        "model_mode_counts": dict(Counter(str(item.get("model_mode")) for item in successes)),
        "status_counts": dict(Counter(str(item.get("status")) for item in results)),
        "successes": len(successes),
        "failures": len(results) - len(successes),
        "elapsed_seconds": round(elapsed, 4),
        "qps": round(len(results) / elapsed, 3) if elapsed else 0.0,
        "latency_ms": {
            "min": round(min(latencies), 2) if latencies else 0.0,
            "p50": percentile(0.50),
            "p95": percentile(0.95),
            "p99": percentile(0.99),
            "max": round(max(latencies), 2) if latencies else 0.0,
            "mean": round(statistics.mean(latencies), 2) if latencies else 0.0,
        },
        "metrics_delta": {
            "requests": round(
                metric_value(after, "trpc_agent_requests_total", args.tenant)
                - metric_value(before, "trpc_agent_requests_total", args.tenant),
                3,
            ),
            "tokens": round(
                metric_value(after, "trpc_agent_tokens_total", args.tenant)
                - metric_value(before, "trpc_agent_tokens_total", args.tenant),
                3,
            ),
            "cost": round(
                metric_value(after, "trpc_agent_cost_total", args.tenant)
                - metric_value(before, "trpc_agent_cost_total", args.tenant),
                6,
            ),
        },
    }
    if args.output:
        output = Path(args.output)
        if not output.is_absolute():
            output = Path(__file__).resolve().parents[1] / output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if not report["failures"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
