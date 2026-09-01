"""Validate optional integration endpoints without printing secrets."""

from __future__ import annotations

import json
import os
import socket
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from trpc_service.security.secrets import SecretManager, redact_secret_text


@dataclass(slots=True)
class CheckResult:
    name: str
    status: str
    detail: str = ""


def _ok(name: str, detail: str = "ok") -> CheckResult:
    return CheckResult(name, "ok", detail)


def _skip(name: str, detail: str) -> CheckResult:
    return CheckResult(name, "skipped", detail)


def _fail(name: str, exc: Exception | str) -> CheckResult:
    return CheckResult(name, "failed", redact_secret_text(str(exc))[:500])


def _resolve_optional_secret(reference: str | None) -> str:
    if not reference:
        return ""
    return SecretManager().resolve(reference)


def check_secret_manager() -> CheckResult:
    references = [value for key, value in os.environ.items() if key.endswith("_REF") and value.startswith("secret://")]
    if not references:
        return _skip("secrets", "no secret:// references in environment")
    try:
        manager = SecretManager()
        resolved = 0
        for reference in references[:20]:
            manager.resolve(reference)
            resolved += 1
        return _ok("secrets", f"resolved {resolved} secret references")
    except Exception as exc:  # pragma: no cover - depends on local secret backend
        return _fail("secrets", exc)


def check_redis() -> CheckResult:
    url = os.getenv("REDIS_URL", "").strip()
    if not url:
        return _skip("redis", "REDIS_URL is not set")
    try:
        import redis

        client = redis.Redis.from_url(url, socket_timeout=3, socket_connect_timeout=3)
        client.ping()
        return _ok("redis")
    except Exception as exc:  # pragma: no cover - optional service
        return _fail("redis", exc)


def check_postgres() -> CheckResult:
    dsn = (os.getenv("POSTGRES_DSN") or os.getenv("TENANT_DB_DSN") or "").strip()
    if not dsn:
        return _skip("postgres", "POSTGRES_DSN/TENANT_DB_DSN is not set")
    try:
        import psycopg

        with psycopg.connect(dsn, connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return _ok("postgres")
    except Exception as exc:  # pragma: no cover - optional service
        return _fail("postgres", exc)


def check_object_store() -> CheckResult:
    bucket = os.getenv("OBJECT_BUCKET", "").strip()
    endpoint = os.getenv("OBJECT_ENDPOINT", "").strip()
    if not bucket:
        return _skip("object_store", "OBJECT_BUCKET is not set")
    try:
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=endpoint or None,
            region_name=os.getenv("OBJECT_REGION") or "us-east-1",
            aws_access_key_id=_resolve_optional_secret(os.getenv("OBJECT_ACCESS_KEY_REF")) or None,
            aws_secret_access_key=_resolve_optional_secret(os.getenv("OBJECT_SECRET_KEY_REF")) or None,
        )
        client.head_bucket(Bucket=bucket)
        return _ok("object_store", f"bucket {bucket} reachable")
    except Exception as exc:  # pragma: no cover - optional service
        return _fail("object_store", exc)


def check_vector_store() -> CheckResult:
    url = os.getenv("VECTOR_URL", "").strip().rstrip("/")
    if not url:
        return _skip("vector_store", "VECTOR_URL is not set")
    try:
        token = _resolve_optional_secret(os.getenv("VECTOR_TOKEN_REF"))
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        with urlopen(Request(f"{url}/healthz", headers=headers), timeout=3) as response:
            if response.status >= 400:
                raise RuntimeError(f"HTTP {response.status}")
        return _ok("vector_store")
    except (HTTPError, URLError, TimeoutError, RuntimeError) as exc:
        try:
            parsed = urlparse(url)
            with socket.create_connection((parsed.hostname or "", parsed.port or 443), timeout=3):
                return _ok("vector_store", "tcp reachable; health endpoint not available")
        except Exception:  # pragma: no cover - optional service
            return _fail("vector_store", exc)


def check_external_memory() -> CheckResult:
    url = os.getenv("EXTERNAL_MEMORY_URL", "").strip().rstrip("/")
    if not url:
        return _skip("external_memory", "EXTERNAL_MEMORY_URL is not set")
    try:
        token = _resolve_optional_secret(os.getenv("EXTERNAL_MEMORY_TOKEN_REF"))
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        with urlopen(Request(f"{url}/health", headers=headers), timeout=3) as response:
            if response.status >= 400:
                raise RuntimeError(f"HTTP {response.status}")
        return _ok("external_memory")
    except Exception as exc:  # pragma: no cover - optional service
        return _fail("external_memory", exc)


def main() -> int:
    checks = [
        check_secret_manager(),
        check_redis(),
        check_postgres(),
        check_object_store(),
        check_vector_store(),
        check_external_memory(),
    ]
    print(json.dumps({"checks": [asdict(item) for item in checks]}, ensure_ascii=False, indent=2))
    return 1 if any(item.status == "failed" for item in checks) else 0


if __name__ == "__main__":
    raise SystemExit(main())
