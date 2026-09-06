"""Run a fixed Web workload and reject measurable performance regressions.

The gate is intentionally independent of a particular load-test runner.  It
executes the repository's Web workload, validates its report, and can compare
the result against an approved prior report.  When release context and a
candidate lock are configured, the output is a release-bound evidence envelope.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.candidate_lock import verify_lock
from scripts.evidence_lineage import ROOT, make_evidence, release_binding, write_json
from scripts.release_context import verify_context

WORKLOAD_ID = "web-fallback-v1"


def _number(value: object, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be numeric")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc


def _extract_report(value: dict[str, Any]) -> dict[str, Any]:
    payload = value.get("payload")
    if isinstance(payload, dict) and isinstance(payload.get("result"), dict):
        return payload["result"]
    return value


def evaluate(
    result: dict[str, Any],
    thresholds: dict[str, float],
    baseline: dict[str, Any] | None = None,
) -> tuple[bool, list[dict[str, Any]]]:
    """Evaluate a load report without making HTTP requests."""

    report = _extract_report(result)
    requests = _number(report.get("requests"), "requests")
    successes = _number(report.get("successes"), "successes")
    failures = _number(report.get("failures"), "failures")
    qps = _number(report.get("qps"), "qps")
    latency = report.get("latency_ms")
    if not isinstance(latency, dict):
        raise ValueError("latency_ms must be an object")
    p95 = _number(latency.get("p95"), "latency_ms.p95")
    p99 = _number(latency.get("p99"), "latency_ms.p99")
    if requests < 1 or successes < 0 or failures < 0 or successes + failures != requests:
        raise ValueError("request accounting is invalid")

    error_rate = failures / requests
    checks = [
        {
            "name": "error rate",
            "ok": error_rate <= thresholds["max_error_rate"],
            "actual": round(error_rate, 6),
            "limit": thresholds["max_error_rate"],
        },
        {
            "name": "p95 latency",
            "ok": p95 <= thresholds["max_p95_ms"],
            "actual_ms": p95,
            "limit_ms": thresholds["max_p95_ms"],
        },
        {
            "name": "p99 latency",
            "ok": p99 <= thresholds["max_p99_ms"],
            "actual_ms": p99,
            "limit_ms": thresholds["max_p99_ms"],
        },
        {
            "name": "throughput",
            "ok": qps >= thresholds["min_qps"],
            "actual_qps": qps,
            "minimum_qps": thresholds["min_qps"],
        },
    ]
    if baseline is not None:
        baseline_report = _extract_report(baseline)
        baseline_latency = baseline_report.get("latency_ms")
        if not isinstance(baseline_latency, dict):
            raise ValueError("baseline latency_ms must be an object")
        baseline_p95 = _number(baseline_latency.get("p95"), "baseline latency_ms.p95")
        baseline_qps = _number(baseline_report.get("qps"), "baseline qps")
        p95_growth = 0.0 if baseline_p95 == 0 else (p95 - baseline_p95) / baseline_p95
        qps_drop = 0.0 if baseline_qps == 0 else (baseline_qps - qps) / baseline_qps
        checks.extend(
            [
                {
                    "name": "p95 regression",
                    "ok": p95_growth <= thresholds["max_p95_regression"],
                    "actual": round(p95_growth, 6),
                    "limit": thresholds["max_p95_regression"],
                    "baseline_ms": baseline_p95,
                },
                {
                    "name": "throughput regression",
                    "ok": qps_drop <= thresholds["max_qps_regression"],
                    "actual": round(qps_drop, 6),
                    "limit": thresholds["max_qps_regression"],
                    "baseline_qps": baseline_qps,
                },
            ]
        )
    return all(bool(check["ok"]) for check in checks), checks


def _run_workload(args: argparse.Namespace, raw_output: Path) -> dict[str, Any]:
    command = [
        sys.executable,
        str(ROOT / "scripts" / "load_test_web_ui.py"),
        "--base-url",
        args.base_url,
        "--requests",
        str(args.requests),
        "--concurrency",
        str(args.concurrency),
        "--timeout",
        str(args.timeout),
        "--tenant",
        args.tenant,
        "--text-template",
        args.text_template,
        "--output",
        str(raw_output),
    ]
    completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
    try:
        report = json.loads(raw_output.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"load workload did not produce a valid report: {exc}") from exc
    if not isinstance(report, dict):
        raise ValueError("load workload report must be an object")
    report["runner"] = {
        "command": command,
        "returncode": completed.returncode,
        "output": (completed.stdout + completed.stderr)[-2000:],
    }
    return report


def _binding() -> dict[str, Any] | None:
    context_value = os.getenv("TRPC_RELEASE_CONTEXT", "").strip()
    lock_value = os.getenv("TRPC_CANDIDATE_LOCK", "").strip()
    if not context_value and not lock_value:
        return None
    if not context_value or not lock_value:
        raise ValueError("TRPC_RELEASE_CONTEXT and TRPC_CANDIDATE_LOCK must be configured together")
    context_path = Path(context_value)
    lock_path = Path(lock_value)
    context = verify_context(context_path, root=ROOT)
    lock = verify_lock(context_path, lock_path, root=ROOT)
    return release_binding(context, lock)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--tenant", default="tenant_demo")
    parser.add_argument("--text-template", default="performance workload v1 request {index}")
    parser.add_argument("--max-error-rate", type=float, default=0.0)
    parser.add_argument("--max-p95-ms", type=float, default=5000)
    parser.add_argument("--max-p99-ms", type=float, default=10000)
    parser.add_argument("--min-qps", type=float, default=1.0)
    parser.add_argument("--max-p95-regression", type=float, default=0.25)
    parser.add_argument("--max-qps-regression", type=float, default=0.25)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--raw-output", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.requests < 1 or args.concurrency < 1 or args.timeout <= 0:
        parser.error("requests, concurrency, and timeout must be positive")
    thresholds = {
        "max_error_rate": args.max_error_rate,
        "max_p95_ms": args.max_p95_ms,
        "max_p99_ms": args.max_p99_ms,
        "min_qps": args.min_qps,
        "max_p95_regression": args.max_p95_regression,
        "max_qps_regression": args.max_qps_regression,
    }
    if any(value < 0 for value in thresholds.values()):
        parser.error("performance thresholds must be non-negative")
    raw_output = args.raw_output if args.raw_output.is_absolute() else ROOT / args.raw_output
    output = args.output if args.output.is_absolute() else ROOT / args.output
    try:
        result = _run_workload(args, raw_output)
        baseline = None
        if args.baseline is not None:
            baseline_path = args.baseline if args.baseline.is_absolute() else ROOT / args.baseline
            baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
            if not isinstance(baseline, dict):
                raise ValueError("baseline must be a JSON object")
        passed, checks = evaluate(result, thresholds, baseline)
        report: dict[str, Any] = {
            "schema_version": 1,
            "gate": "performance",
            "status": "pass" if passed else "fail",
            "generated_at": datetime.now(UTC).isoformat(),
            "workload": {
                "id": WORKLOAD_ID,
                "base_url": args.base_url,
                "requests": args.requests,
                "concurrency": args.concurrency,
                "timeout_seconds": args.timeout,
                "tenant": args.tenant,
                "model_mode_expected": "fallback",
            },
            "thresholds": thresholds,
            "checks": checks,
            "result": result,
            "baseline": str(args.baseline) if args.baseline is not None else None,
        }
        binding = _binding()
        if binding is not None:
            envelope = make_evidence("performance", "scripts.performance_gate", report, binding)
            write_json(output, envelope)
            report["evidence"] = {
                "path": str(output),
                "evidence_sha256": envelope["evidence_sha256"],
            }
        else:
            write_json(output, report)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        report = {
            "schema_version": 1,
            "gate": "performance",
            "status": "fail",
            "generated_at": datetime.now(UTC).isoformat(),
            "error": str(exc),
        }
        write_json(output, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
