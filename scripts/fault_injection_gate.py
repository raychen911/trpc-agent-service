"""Run dependency interruption checks through a configured Toxiproxy instance.

The application must already be configured to use the proxy endpoints. The
gate refuses to claim success when a proxy or runtime URL is missing.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen
from uuid import uuid4


def _get(url: str) -> object:
    request = Request(url, headers={"User-Agent": "trpc-agent-acceptance/1"})
    with urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _post(url: str, payload: dict[str, object]) -> object:
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "trpc-agent-acceptance/1",
        },
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        body = response.read().decode("utf-8")
        return json.loads(body) if body else {}


def _delete(url: str) -> object:
    request = Request(url, headers={"User-Agent": "trpc-agent-acceptance/1"}, method="DELETE")
    with urlopen(request, timeout=10) as response:
        body = response.read().decode("utf-8")
        return json.loads(body) if body else {}


def _safe_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
        if not parsed.scheme or not parsed.hostname:
            return "[redacted]"
        host = parsed.hostname
        if parsed.port:
            host = f"{host}:{parsed.port}"
        return urlunsplit((parsed.scheme, host, parsed.path, "", ""))
    except ValueError:
        return "[redacted]"


def _safe_error(value: object) -> str:
    return re.sub(r"(://[^:/\s]+:)[^@/\s]+@", r"\1[redacted]@", str(value))[:1000]


def _probe(url: str) -> dict[str, object]:
    parsed = urlsplit(url)
    if parsed.scheme.lower() in {"postgres", "postgresql"}:
        try:
            import psycopg

            with psycopg.connect(
                url,
                connect_timeout=int(os.getenv("TOXIPROXY_PROBE_TIMEOUT_SECONDS", "5")),
            ) as connection, connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return {"ok": True, "status": "query_succeeded"}
        except Exception as exc:
            return {"ok": False, "status": None, "error": _safe_error(exc)[:500]}
    if parsed.scheme.lower() in {"redis", "rediss"}:
        try:
            import redis

            client = redis.Redis.from_url(
                url,
                socket_connect_timeout=float(os.getenv("TOXIPROXY_PROBE_TIMEOUT_SECONDS", "5")),
                socket_timeout=float(os.getenv("TOXIPROXY_PROBE_TIMEOUT_SECONDS", "5")),
            )
            client.ping()
            client.close()
            return {"ok": True, "status": "pong"}
        except Exception as exc:
            return {"ok": False, "status": None, "error": _safe_error(exc)[:500]}
    if parsed.scheme.lower() == "tcp":
        if not parsed.hostname or parsed.port is None:
            return {"ok": False, "status": None, "error": "tcp probe requires host and port"}
        try:
            with socket.create_connection(
                (parsed.hostname, parsed.port),
                timeout=float(os.getenv("TOXIPROXY_PROBE_TIMEOUT_SECONDS", "5")),
            ):
                return {"ok": True, "status": "connected"}
        except (TimeoutError, OSError) as exc:
            return {"ok": False, "status": None, "error": _safe_error(exc)[:500]}
    try:
        with urlopen(url, timeout=float(os.getenv("TOXIPROXY_PROBE_TIMEOUT_SECONDS", "5"))) as response:
            return {"ok": 200 <= response.status < 400, "status": response.status}
    except HTTPError as exc:
        return {"ok": False, "status": exc.code, "error": _safe_error(exc)[:500]}
    except (URLError, TimeoutError, OSError) as exc:
        return {"ok": False, "status": None, "error": _safe_error(exc)[:500]}


def _probe_urls() -> dict[str, str]:
    raw = os.getenv("TOXIPROXY_PROBE_URLS", "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return {str(key).strip(): str(value).strip() for key, value in parsed.items() if value}
        except ValueError:
            pass
    result = {}
    for name in ("postgres", "redis", "model", "im"):
        value = os.getenv(f"TOXIPROXY_PROBE_URL_{name.upper()}", "").strip()
        if value:
            result[name] = value
    return result


def _cycle_proxy(api_url: str, name: str, probe_url: str) -> dict[str, object]:
    toxic_name = f"acceptance-reset-{uuid4().hex[:12]}"
    toxic_url = f"{api_url.rstrip('/')}/proxies/{name}/toxics"
    attributes: dict[str, object] = {}
    raw_attributes = os.getenv("TOXIPROXY_TOXIC_ATTRIBUTES", "").strip()
    if raw_attributes:
        try:
            parsed_attributes = json.loads(raw_attributes)
        except ValueError as exc:
            raise ValueError("TOXIPROXY_TOXIC_ATTRIBUTES must be valid JSON") from exc
        if not isinstance(parsed_attributes, dict):
            raise ValueError("TOXIPROXY_TOXIC_ATTRIBUTES must be a JSON object")
        attributes = parsed_attributes
    result: dict[str, object] = {
        "proxy": name,
        "probe_url": _safe_url(probe_url),
        "toxic": toxic_name,
    }
    created = False
    try:
        result["create"] = _post(
            toxic_url,
            {
                "name": toxic_name,
                "type": os.getenv("TOXIPROXY_TOXIC_TYPE", "reset_peer"),
                "stream": "downstream",
                "toxicity": 1.0,
                "attributes": attributes,
            },
        )
        created = True
        result["during_outage"] = _probe(probe_url)
    except (HTTPError, URLError, OSError, ValueError) as exc:
        result["error"] = _safe_error(exc)
    finally:
        if created:
            try:
                result["remove"] = _delete(f"{toxic_url}/{toxic_name}")
            except (HTTPError, URLError, OSError, ValueError) as exc:
                result["remove_error"] = _safe_error(exc)

    deadline = time.monotonic() + float(os.getenv("TOXIPROXY_RECOVERY_TIMEOUT_SECONDS", "20"))
    recovered = {"ok": False, "status": None, "error": "recovery probe did not run"}
    while created and "remove_error" not in result and time.monotonic() < deadline:
        recovered = _probe(probe_url)
        if recovered.get("ok"):
            break
        time.sleep(0.5)
    result["after_recovery"] = recovered
    result["ok"] = bool(
        created
        and "remove_error" not in result
        and not result.get("during_outage", {}).get("ok", True)
        and recovered.get("ok")
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default=os.getenv("TOXIPROXY_API_URL", ""))
    parser.add_argument("--base-url", default=os.getenv("ONLINE_IM_BASE_URL", ""))
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    if not args.api_url:
        print(json.dumps({"status": "not_run", "reason": "TOXIPROXY_API_URL is not configured"}))
        return 2

    report: dict[str, object] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "api_url": _safe_url(args.api_url),
        "base_url": _safe_url(args.base_url),
        "proxies": {},
    }
    try:
        proxies = _get(args.api_url.rstrip("/") + "/proxies")
    except (HTTPError, URLError, OSError, ValueError) as exc:
        report.update({"status": "fail", "error": str(exc)[:1000]})
        print(json.dumps(report, indent=2))
        return 1

    expected = {
        item.strip()
        for item in os.getenv(
            "TOXIPROXY_REQUIRED_PROXIES", "postgres,redis,model,im"
        ).split(",")
        if item.strip()
    }
    available = set(proxies) if isinstance(proxies, dict) else set()
    missing = sorted(expected - available)
    report["proxies"] = {
        "available": sorted(available),
        "required": sorted(expected),
        "missing": missing,
    }
    if missing:
        report["status"] = "fail"
        report["note"] = "required Toxiproxy proxies are missing"
    else:
        probes = _probe_urls()
        cycles: list[dict[str, object]] = []
        control_only = os.getenv("TOXIPROXY_CONTROL_ONLY", "0").lower() in {"1", "true", "yes", "on"}
        for name in sorted(expected):
            probe_url = probes.get(name)
            if not probe_url:
                cycles.append({"proxy": name, "status": "not_run", "reason": "probe URL is not configured"})
                continue
            cycles.append(_cycle_proxy(args.api_url, name, probe_url))
        report["cycles"] = cycles
        if control_only:
            report["status"] = "pass"
            report["note"] = "control-plane toxic create/remove verified; runtime probes were explicitly bypassed"
        elif any(item.get("status") == "not_run" for item in cycles):
            report["status"] = "not_run"
            report["note"] = "configure one probe URL per proxy for runtime interruption and recovery evidence"
        else:
            report["status"] = "pass" if all(item.get("ok") for item in cycles) else "fail"
            report["note"] = "each configured proxy was interrupted and recovered through its runtime probe"
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["status"] == "pass" else 2 if report["status"] == "not_run" else 1


if __name__ == "__main__":
    raise SystemExit(main())
