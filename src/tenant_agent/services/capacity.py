"""Transparent capacity-estimation formulas for defense and operations."""

from __future__ import annotations

import math

from pydantic import BaseModel, Field


class CapacityInputs(BaseModel):
    peak_callbacks_per_second: float = Field(gt=0)
    p95_agent_latency_seconds: float = Field(gt=0)
    max_concurrent_sessions_per_worker: int = Field(gt=0)
    average_input_tokens: int = Field(ge=0)
    average_output_tokens: int = Field(ge=0)
    session_reads_per_turn: int = Field(default=2, ge=0)
    session_writes_per_turn: int = Field(default=4, ge=0)
    memory_operations_per_turn: int = Field(default=2, ge=0)
    headroom_ratio: float = Field(default=1.5, ge=1.0, le=5.0)


class CapacityEstimate(BaseModel):
    peak_concurrent_sessions: int
    recommended_worker_replicas: int
    redis_or_sql_qps: int
    average_tokens_per_second: int
    callback_queue_buffer: int
    assumptions: list[str]


def estimate_capacity(inputs: CapacityInputs) -> CapacityEstimate:
    concurrency = math.ceil(
        inputs.peak_callbacks_per_second * inputs.p95_agent_latency_seconds * inputs.headroom_ratio
    )
    workers = max(2, math.ceil(concurrency / inputs.max_concurrent_sessions_per_worker))
    operations = (
        inputs.session_reads_per_turn + inputs.session_writes_per_turn + inputs.memory_operations_per_turn
    )
    qps = math.ceil(inputs.peak_callbacks_per_second * operations * inputs.headroom_ratio)
    tokens = math.ceil(
        inputs.peak_callbacks_per_second * (inputs.average_input_tokens + inputs.average_output_tokens)
    )
    buffer = math.ceil(inputs.peak_callbacks_per_second * inputs.p95_agent_latency_seconds * 3)
    return CapacityEstimate(
        peak_concurrent_sessions=concurrency,
        recommended_worker_replicas=workers,
        redis_or_sql_qps=qps,
        average_tokens_per_second=tokens,
        callback_queue_buffer=buffer,
        assumptions=[
            "Little's Law: concurrency = arrival rate x p95 service time.",
            f"A {inputs.headroom_ratio:.2f}x headroom factor covers bursts and node loss.",
            "At least two workers are recommended so one node may fail without total outage.",
            "Load-test model-provider quotas separately; local worker capacity is not the provider quota.",
        ],
    )
