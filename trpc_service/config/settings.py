"""Environment-backed service settings and secret references."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


class SecretResolutionError(ValueError):
    pass


def load_environment(path: str | os.PathLike[str] | None = None) -> bool:
    """Load local settings without overriding explicitly exported variables."""
    env_path = Path(path or os.getenv("TRPC_SERVICE_ENV_FILE", ".env")).expanduser()
    return load_dotenv(dotenv_path=env_path, override=False, encoding="utf-8")


def resolve_secret(reference: str | None, environ: dict[str, str] | None = None) -> str | None:
    if reference is None:
        return None
    values = environ if environ is not None else os.environ
    if reference.startswith("env://"):
        name = reference[6:]
        if not name or name not in values:
            raise SecretResolutionError(f"secret environment variable is missing: {name}")
        return values[name]
    if reference.startswith("literal://"):
        # Useful only for local tests; production policy should reject it.
        return reference[10:]
    if reference.startswith("file://"):
        raw_path = reference[7:]
        if os.name == "nt" and raw_path.startswith("/") and len(raw_path) > 2 and raw_path[2] == ":":
            raw_path = raw_path[1:]
        if not raw_path:
            raise SecretResolutionError("secret file path is missing")
        try:
            value = Path(raw_path).expanduser().read_text(encoding="utf-8").rstrip("\r\n")
        except (OSError, UnicodeError) as exc:
            raise SecretResolutionError("secret file cannot be read") from exc
        if not value:
            raise SecretResolutionError("secret file is empty")
        return value
    raise SecretResolutionError("secret must use env://, file://, or literal:// reference")


@dataclass(frozen=True)
class ServiceSettings:
    host: str = "0.0.0.0"
    port: int = 8080
    admin_token: str | None = None
    admin_operator_token: str | None = None
    admin_viewer_token: str | None = None
    session_hmac_key: str = "development-only-change-me"
    max_queue_size: int = 1000
    worker_count: int = 1
    environment: str = "development"
    backend: str = "inmemory"
    database_url: str | None = None
    migration_database_url: str | None = None
    control_database_url: str | None = None
    redis_url: str | None = None
    auto_migrate: bool = False
    queue_reclaim_ms: int = 60_000
    queue_stream_maxlen: int = 100_000
    queue_dlq_maxlen: int = 10_000
    max_delivery_attempts: int = 3
    otlp_endpoint: str | None = None
    service_name: str = "trpc-agent-service"
    role: str = "all"

    @classmethod
    def from_env(cls) -> ServiceSettings:
        return cls(
            host=os.getenv("TRPC_SERVICE_HOST", "0.0.0.0"),
            port=int(os.getenv("TRPC_SERVICE_PORT", "8080")),
            admin_token=resolve_secret(os.getenv("TRPC_SERVICE_ADMIN_TOKEN_REF")),
            admin_operator_token=resolve_secret(os.getenv("TRPC_SERVICE_ADMIN_OPERATOR_TOKEN_REF")),
            admin_viewer_token=resolve_secret(os.getenv("TRPC_SERVICE_ADMIN_VIEWER_TOKEN_REF")),
            session_hmac_key=os.getenv("TRPC_SERVICE_SESSION_HMAC_KEY", "development-only-change-me"),
            max_queue_size=int(os.getenv("TRPC_SERVICE_MAX_QUEUE_SIZE", "1000")),
            worker_count=max(1, int(os.getenv("TRPC_SERVICE_WORKER_COUNT", "1"))),
            environment=os.getenv("TRPC_SERVICE_ENV", "development"),
            backend=os.getenv("TRPC_SERVICE_BACKEND", "inmemory").lower(),
            database_url=os.getenv("TRPC_SERVICE_DATABASE_URL"),
            migration_database_url=os.getenv("TRPC_SERVICE_MIGRATION_DATABASE_URL"),
            control_database_url=os.getenv("TRPC_SERVICE_CONTROL_DATABASE_URL"),
            redis_url=os.getenv("TRPC_SERVICE_REDIS_URL"),
            auto_migrate=os.getenv("TRPC_SERVICE_AUTO_MIGRATE", "false").lower() in {"1", "true", "yes"},
            queue_reclaim_ms=int(os.getenv("TRPC_SERVICE_QUEUE_RECLAIM_MS", "60000")),
            queue_stream_maxlen=int(os.getenv("TRPC_SERVICE_QUEUE_STREAM_MAXLEN", "100000")),
            queue_dlq_maxlen=int(os.getenv("TRPC_SERVICE_QUEUE_DLQ_MAXLEN", "10000")),
            max_delivery_attempts=int(os.getenv("TRPC_SERVICE_MAX_DELIVERY_ATTEMPTS", "3")),
            otlp_endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
            service_name=os.getenv("OTEL_SERVICE_NAME", "trpc-agent-service"),
            role=os.getenv("TRPC_SERVICE_ROLE", "all").lower(),
        )

    def admin_role_tokens(self) -> dict[str, str]:
        return {
            role: token
            for role, token in {
                "admin": self.admin_token,
                "operator": self.admin_operator_token,
                "viewer": self.admin_viewer_token,
            }.items()
            if token
        }
