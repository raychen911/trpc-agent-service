"""
Capacity Planning and Load Estimation Calculator for Multi-tenant Agent Platform.
Estimates resource requirements (Nodes, CPU/RAM, Redis QPS/RAM, SQL QPS, IM Peak throughput).
"""

from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class WorkloadInput:
    total_tenants: int = 50
    concurrent_sessions_peak: int = 1000
    avg_turns_per_session: int = 6
    avg_tokens_per_turn: int = 1200
    target_p95_latency_ms: int = 1500


class CapacityPlanner:
    """Calculates compute, storage, and throughput capacities."""

    def evaluate(self, workload: WorkloadInput) -> Dict[str, Any]:
        # 1. Throughput calculations
        peak_qps = workload.concurrent_sessions_peak / 10.0  # Assumes 1 query per 10s per active session
        daily_requests = workload.concurrent_sessions_peak * workload.avg_turns_per_session * 8
        daily_tokens = daily_requests * workload.avg_tokens_per_turn

        # 2. Worker node capacity (Single 4C8G node can sustain ~50-80 concurrent async coroutine streams)
        qps_per_worker = 60.0
        workers_needed = max(2, int(peak_qps / qps_per_worker) + 1)

        # 3. Redis Sizing (Session state cache + Locks + Idempotency)
        # Each session metadata ~ 2KB, 7 days retention
        redis_session_cache_mb = (workload.concurrent_sessions_peak * 20 * 2) / 1024.0  # MB
        redis_qps = peak_qps * 5  # Lock acquire + release + session fetch + save + idempotency
        redis_recommended_ram_gb = max(2.0, round((redis_session_cache_mb * 3) / 1024.0, 1))

        # 4. SQL Sizing (Event sourcing & Audit logs)
        # Append 2 events + 1 audit log per turn
        sql_write_qps = peak_qps * 3
        sql_read_qps = peak_qps * 1.5

        return {
            "workload_summary": {
                "total_tenants": workload.total_tenants,
                "peak_concurrent_sessions": workload.concurrent_sessions_peak,
                "peak_inbound_qps": round(peak_qps, 1),
                "estimated_daily_tokens": f"{daily_tokens:,}",
            },
            "compute_capacity": {
                "worker_nodes_recommended (4C8G)": workers_needed,
                "gateway_nodes_recommended (2C4G)": 2,  # Active-Active redundant
                "hpa_scaling_limits": {"min_replicas": 2, "max_replicas": workers_needed * 3},
            },
            "redis_capacity": {
                "estimated_redis_qps": int(redis_qps),
                "recommended_redis_ram": f"{redis_recommended_ram_gb} GB",
                "cluster_mode": "Redis Sentinel / Master-Replica",
            },
            "sql_capacity": {
                "estimated_sql_write_qps": int(sql_write_qps),
                "estimated_sql_read_qps": int(sql_read_qps),
                "recommended_db_spec": "8C16G PostgreSQL 16 (Connection Pool size = 100)",
            },
            "im_gateway_bandwidth": {
                "peak_callback_bandwidth_mbps": round((peak_qps * 4 * 8) / 1024, 2),  # 4KB per callback
            },
        }


if __name__ == "__main__":
    planner = CapacityPlanner()
    result = planner.evaluate(WorkloadInput())
    import json
    print(json.dumps(result, indent=2, ensure_ascii=False))
