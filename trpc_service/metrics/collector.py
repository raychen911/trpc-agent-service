"""业务指标采集器。"""
from __future__ import annotations

import threading
from dataclasses import dataclass

from opentelemetry import metrics as otel_metrics


@dataclass
class TenantStats:
    """单个租户的进程内累计指标。"""

    requests: int = 0
    errors: int = 0
    tool_calls: int = 0
    total_tool_latency_ms: int = 0
    im_delivered: int = 0
    im_failed: int = 0
    tokens: int = 0
    total_latency_ms: int = 0

    def as_dict(self) -> dict:
        avg_latency = self.total_latency_ms / self.requests if self.requests else 0
        avg_tool = self.total_tool_latency_ms / self.tool_calls if self.tool_calls else 0
        success_rate = (
            self.im_delivered / (self.im_delivered + self.im_failed)
            if (self.im_delivered + self.im_failed)
            else 1.0
        )
        return {
            "requests": self.requests,
            "errors": self.errors,
            "error_rate": round(self.errors / self.requests, 4) if self.requests else 0.0,
            "tool_calls": self.tool_calls,
            "avg_tool_latency_ms": round(avg_tool, 1),
            "im_delivered": self.im_delivered,
            "im_failed": self.im_failed,
            "im_success_rate": round(success_rate, 4),
            "tokens": self.tokens,
            "avg_latency_ms": round(avg_latency, 1),
        }


class MetricsCollector:
    """租户维度业务指标（进程内聚合 + OTel 双写）。"""

    def __init__(self) -> None:
        # RLock：_stats 与调用方方法都要持锁，需可重入（Lock 会自锁死锁）
        self._lock = threading.RLock()
        self._tenants: dict[str, TenantStats] = {}
        self._backend_stats: dict[str, dict[str, list]] = {}  # tenant → backend → [count, total_ms]
        meter = otel_metrics.get_meter("trpc_agent_service")
        self._otel_requests = meter.create_counter(
            "agent.requests", description="聊天请求量（含被拦截请求）"
        )
        self._otel_latency = meter.create_histogram(
            "agent.request.latency", unit="ms", description="请求端到端耗时"
        )
        self._otel_tool_calls = meter.create_counter(
            "agent.tool.calls", description="工具调用量"
        )
        self._otel_tool_latency = meter.create_histogram(
            "agent.tool.latency", unit="ms", description="单次工具调用耗时"
        )
        self._otel_backend_latency = meter.create_histogram(
            "agent.session_backend.latency", unit="ms", description="Session 后端读写延迟"
        )
        self._otel_im = meter.create_counter(
            "agent.im.deliveries", description="IM 消息投递结果"
        )
        self._otel_tokens = meter.create_counter(
            "agent.tokens", unit="tokens", description="token 消耗"
        )

    def _stats(self, tenant_id: str) -> TenantStats:
        with self._lock:
            stats = self._tenants.get(tenant_id)
            if stats is None:
                stats = TenantStats()
                self._tenants[tenant_id] = stats
            return stats

    def inc_request(
        self, tenant_id: str, channel: str, error: bool = False, latency_ms: int = 0
    ) -> None:
        """记录一次请求（error=True 含预算拦截/执行失败）。"""
        stats = self._stats(tenant_id)
        with self._lock:
            stats.requests += 1
            if error:
                stats.errors += 1
            stats.total_latency_ms += latency_ms
        attrs = {"tenant_id": tenant_id, "channel": channel}
        self._otel_requests.add(1, attrs)
        if latency_ms:
            self._otel_latency.record(latency_ms, attrs)

    def inc_tool_call(self, tenant_id: str, tool_name: str) -> None:
        """记录一次工具调用。"""
        with self._lock:
            self._stats(tenant_id).tool_calls += 1
        self._otel_tool_calls.add(1, {"tenant_id": tenant_id, "tool_name": tool_name})

    def inc_tool_latency(self, tenant_id: str, tool_name: str, latency_ms: int) -> None:
        """记录一次工具调用耗时（直方图 + 每租户平均）。"""
        with self._lock:
            stats = self._stats(tenant_id)
            stats.tool_calls += 1
            stats.total_tool_latency_ms += latency_ms
        attrs = {"tenant_id": tenant_id, "tool_name": tool_name}
        self._otel_tool_calls.add(1, attrs)
        self._otel_tool_latency.record(latency_ms, attrs)

    def inc_backend_latency(
        self, tenant_id: str, backend: str, method: str, latency_ms: float
    ) -> None:
        """记录 Session 后端单次读写延迟（维度：租户/后端/方法）。"""
        with self._lock:
            backends = self._backend_stats.setdefault(tenant_id, {})
            pair = backends.setdefault(backend, [0, 0.0])
            pair[0] += 1
            pair[1] += latency_ms
        self._otel_backend_latency.record(
            latency_ms, {"tenant_id": tenant_id, "backend": backend, "method": method}
        )

    def backend_snapshot(self, tenant_id: str) -> dict:
        """租户的 Session 后端延迟聚合（/api/v1/metrics 附加段）。"""
        with self._lock:
            backends = self._backend_stats.get(tenant_id, {})
            return {
                backend: {
                    "calls": pair[0],
                    "avg_ms": round(pair[1] / pair[0], 2) if pair[0] else 0.0,
                }
                for backend, pair in backends.items()
            }

    def inc_im_delivery(self, tenant_id: str, delivered: bool = True) -> None:
        """记录一次 IM 消息投递结果（成功率分子/分母）。"""
        with self._lock:
            stats = self._stats(tenant_id)
            if delivered:
                stats.im_delivered += 1
            else:
                stats.im_failed += 1
        self._otel_im.add(1, {"tenant_id": tenant_id, "delivered": delivered})

    def add_tokens(self, tenant_id: str, tokens: int) -> None:
        """累计 token 消耗（每租户成本核算基础）。"""
        if tokens <= 0:
            return
        with self._lock:
            self._stats(tenant_id).tokens += tokens
        self._otel_tokens.add(tokens, {"tenant_id": tenant_id})

    def snapshot(self) -> dict[str, dict]:
        """全部租户指标快照（/api/v1/metrics 返回体）。"""
        with self._lock:
            snap = {tid: s.as_dict() for tid, s in self._tenants.items()}
        for tid in snap:
            snap[tid]["session_backend"] = self.backend_snapshot(tid)
        return snap

    def reset(self) -> None:
        with self._lock:
            self._tenants.clear()
            self._backend_stats.clear()


metrics_collector = MetricsCollector()
