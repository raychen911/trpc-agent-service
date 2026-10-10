#!/usr/bin/env python3
# ruff: noqa: E402
"""Run a deterministic, infrastructure-free throughput smoke gate."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
import time
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:  # Keep both ``python -m`` and the documented script-path invocation usable.
    from scripts.evidence_lineage import build_evidence
except ModuleNotFoundError:  # pragma: no cover - exercised by direct CLI startup
    from evidence_lineage import build_evidence  # type: ignore[import-not-found, no-redef]
from trpc_service.agent.fake import DeterministicAgent
from trpc_service.agent.worker import AgentWorker, ProcessStatus, WorkerResult
from trpc_service.channels.envelopes import InboundEnvelope, PayloadKind
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.memory import InMemoryRuntimeRepository
from trpc_service.storage.models import Acceptance, BindingRoute
from trpc_service.tenant.models import (
    Channel,
    ChannelBinding,
    ConversationKind,
    ModelPolicy,
    StorageSelection,
    TenantConfig,
)

DEFAULT_CALLBACKS = 200
DEFAULT_TURNS = 50
DEFAULT_CONCURRENCY = 16
DEFAULT_TIMEOUT_SECONDS = 60.0
MAX_CALLBACKS = 2_000
MAX_TURNS = 500
MAX_CONCURRENCY = 64
MAX_TIMEOUT_SECONDS = 120.0
NO_LATENCY_P95_MS = 1_000_000.0


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * percentile) - 1)
    return ordered[index]


def _validate_workload(
    callbacks: int,
    turns: int,
    concurrency: int,
    timeout_seconds: float,
) -> None:
    if any(
        not isinstance(value, int) or isinstance(value, bool)
        for value in (callbacks, turns, concurrency)
    ):
        raise ValueError("counts and concurrency must be integers")
    if callbacks < turns or min(callbacks, turns, concurrency) < 1:
        raise ValueError("callbacks must cover turns and all counts must be positive")
    if callbacks > MAX_CALLBACKS or turns > MAX_TURNS or concurrency > MAX_CONCURRENCY:
        raise ValueError(
            f"workload exceeds limits callbacks<={MAX_CALLBACKS}, turns<={MAX_TURNS}, "
            f"concurrency<={MAX_CONCURRENCY}"
        )
    if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= MAX_TIMEOUT_SECONDS:
        raise ValueError(f"timeout must be between 0 and {MAX_TIMEOUT_SECONDS:g} seconds")


async def _bounded_gather(
    awaitables: list[Awaitable[Any]], timeout_seconds: float
) -> tuple[list[Any], bool]:
    """Gather one phase with cancellation and a hard wall-clock bound."""

    if not awaitables:
        return [], False
    tasks = [asyncio.ensure_future(awaitable) for awaitable in awaitables]
    try:
        values = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True), timeout=timeout_seconds
        )
    except TimeoutError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        return [], True
    return list(values), False


async def _run(
    callbacks: int,
    turns: int,
    concurrency: int,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, object]:
    _validate_workload(callbacks, turns, concurrency, timeout_seconds)
    repository = InMemoryRuntimeRepository()
    config = TenantConfig(
        tenant_id="performance-tenant",
        app_id="performance-app",
        version=1,
        model=ModelPolicy(provider="offline", model="deterministic"),
        storage=StorageSelection(profile_id="in-memory"),
    )
    binding = ChannelBinding(
        binding_id="performance-binding",
        tenant_id=config.tenant_id,
        app_id=config.app_id,
        channel=Channel.FEISHU,
        account_id="performance-account",
    )
    repository.add_config(config)
    repository.add_route(BindingRoute(binding=binding, active_config_version=1))
    runtime = TenantRuntime(repository, routing_key=b"p" * 32)
    semaphore = asyncio.Semaphore(concurrency)
    latencies: list[float] = []

    async def accept(index: int) -> Acceptance:
        envelope = InboundEnvelope(
            channel=Channel.FEISHU,
            account_id=binding.account_id,
            external_message_id=f"performance-{index}",
            external_user_id=f"user-{index}",
            conversation_kind=ConversationKind.DIRECT,
            payload_kind=PayloadKind.TEXT,
            text="offline performance payload",
            occurred_at=datetime.now(UTC),
        )
        async with semaphore:
            started = time.perf_counter()
            accepted = await runtime.accept(binding.binding_id, envelope)
            latencies.append((time.perf_counter() - started) * 1000)
            return accepted

    callback_started = time.perf_counter()
    acceptance_results, acceptance_timed_out = await _bounded_gather(
        [accept(index) for index in range(callbacks)], timeout_seconds
    )
    acceptances = [result for result in acceptance_results if isinstance(result, Acceptance)]
    acceptance_errors = sum(not isinstance(result, Acceptance) for result in acceptance_results)
    callback_elapsed = time.perf_counter() - callback_started

    class PerformanceAgentLoader:
        async def __call__(self, config: TenantConfig) -> DeterministicAgent:
            del config
            return DeterministicAgent(name="performance-agent", response="ok")

    workers = [
        AgentWorker(
            repository,
            worker_id=f"performance-worker-{index}",
            agent_loader=PerformanceAgentLoader(),
            lease_for=timedelta(seconds=30),
        )
        for index in range(min(concurrency, len(acceptances)))
    ]
    turn_started = time.perf_counter()
    turn_results, turn_timed_out = await _bounded_gather(
        [
            workers[index % len(workers)].process(acceptances[index])
            for index in range(min(turns, len(acceptances)))
        ],
        timeout_seconds,
    )
    turn_elapsed = time.perf_counter() - turn_started
    errors = acceptance_errors + sum(isinstance(result, BaseException) for result in turn_results)
    committed = sum(
        not isinstance(result, BaseException)
        and isinstance(result, WorkerResult)
        and result.status == ProcessStatus.COMMITTED
        for result in turn_results
    )
    callback_throughput = len(acceptances) / max(callback_elapsed, 1e-9)
    callback_p95_ms = _percentile(latencies, 0.95) if latencies else NO_LATENCY_P95_MS
    error_rate = errors / max(1, turns)
    passed = (
        not acceptance_timed_out
        and not turn_timed_out
        and callback_throughput >= 100
        and callback_p95_ms < 200
        and len({item.inbound_id for item in acceptances}) == callbacks
        and committed == min(turns, len(acceptances)) == turns
        and error_rate < 0.001
    )
    evidence = build_evidence(root=ROOT, producer="scripts.performance_gate")
    return {
        "baseline": {
            "callback_per_second": 100,
            "concurrent_turns": 200,
            "callback_p95_ms_max": 200,
            "error_rate_max": 0.001,
        },
        "candidate": {
            "mode": "offline_inmemory_smoke",
            "callbacks": callbacks,
            "turns": turns,
            "concurrency": concurrency,
            "callback_per_second": callback_throughput,
            "callback_p95_ms": callback_p95_ms,
            "committed_turns": committed,
            "error_rate": error_rate,
            "callback_elapsed_seconds": callback_elapsed,
            "turn_elapsed_seconds": turn_elapsed,
            "acceptance_errors": acceptance_errors,
            "timed_out": acceptance_timed_out or turn_timed_out,
            "timeout_seconds": timeout_seconds,
            "effective_turn_workers": len(workers),
        },
        "case_deltas": {
            "callback_per_second": callback_throughput - 100,
            "callback_p95_ms_headroom": 200 - callback_p95_ms,
            "accepted_message_loss": callbacks - len({item.inbound_id for item in acceptances}),
            "uncommitted_turns": turns - committed,
        },
        "gate": "pass" if passed else "fail",
        "rejection_reasons": [] if passed else ["offline performance smoke threshold failed"],
        "production_gate": "not_run",
        "production_rejection_reasons": [
            "requires PostgreSQL, Redis Streams, MinIO, pgvector and multi-process load"
        ],
        "evidence": evidence,
    }


def _write_report(path: Path, result: dict[str, object]) -> None:
    if path.is_symlink():
        raise RuntimeError("refusing to write a performance report through a symlink")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--callbacks", type=int, default=DEFAULT_CALLBACKS)
    parser.add_argument("--turns", type=int, default=DEFAULT_TURNS)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument(
        "--output", type=Path, default=Path("runs/multitenant/performance-offline.json")
    )
    args = parser.parse_args()
    try:
        result = asyncio.run(
            _run(args.callbacks, args.turns, args.concurrency, args.timeout_seconds)
        )
        _write_report(args.output, result)
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2))
    return 0 if result["gate"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
