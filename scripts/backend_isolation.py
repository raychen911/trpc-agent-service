"""Ephemeral, zero-cost backend isolation for the live ACK contract.

The acceptance runner connects to the existing support PostgreSQL/Redis/MinIO
instances through port-forwards.  This module gives one runner an exclusive
logical database in each service instead of stopping production consumers.
Secrets are kept in private fields and are never included in the public
summary used by acceptance reports.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import asyncpg
import boto3
import redis.asyncio as redis_async
from botocore.exceptions import ClientError

_TOKEN_RE = re.compile(r"^[a-z][a-z0-9-]{0,31}$")
_DATABASE_RE = re.compile(r"^trpc_backend_[a-z0-9]{1,45}$")
_BUCKET_RE = re.compile(r"^trpc-backend-[a-z0-9-]{1,50}$")
_REDIS_DATABASES = tuple(range(1, 16))
_MIGRATION_ROLE = "trpc_migration"
_DATABASE_ROLES = ("trpc_runtime", "trpc_worker", "trpc_migration", "trpc_metrics")
_LOGGER = logging.getLogger(__name__)
_REDIS_RESERVE_SCRIPT = """
if redis.call('DBSIZE') ~= 0 then
    return 0
end
if redis.call('SET', KEYS[1], ARGV[1], 'NX') then
    return 1
end
return 0
"""


class BackendIsolationError(RuntimeError):
    """A safe, stage-labelled provisioning or cleanup error."""

    def __init__(self, stage: str, *, cleanup_errors: Sequence[str] = ()) -> None:
        self.stage = stage
        self.cleanup_errors = tuple(cleanup_errors)
        super().__init__(f"backend isolation {stage} failed")


def _safe_token(value: str) -> str:
    token = value.strip()
    if _TOKEN_RE.fullmatch(token) is None:
        raise ValueError("run_id must contain only lowercase letters, digits, and hyphens")
    return token


def database_name(run_id: str) -> str:
    """Build a bounded PostgreSQL identifier owned by this runner."""

    token = _safe_token(run_id).replace("-", "")
    name = f"trpc_backend_{token}"
    if _DATABASE_RE.fullmatch(name) is None:
        raise ValueError("generated PostgreSQL database name is invalid")
    return name


def redis_marker(run_id: str) -> str:
    """Return the exclusive marker key used to reserve a Redis database."""

    return f"trpc:backend:isolation:{_safe_token(run_id)}"


def bucket_name(run_id: str) -> str:
    """Build a bounded, run-owned S3 bucket name."""

    name = f"trpc-backend-{_safe_token(run_id)}"
    if _BUCKET_RE.fullmatch(name) is None:
        raise ValueError("generated S3 bucket name is invalid")
    return name


def s3_marker(run_id: str) -> str:
    """Return the object key proving ownership of a run-owned bucket."""

    return f".trpc-backend-isolation/{_safe_token(run_id)}"


def _authority(parsed: Any, *, username: str | None = None, password: str | None = None) -> str:
    host = parsed.hostname
    if not host:
        raise ValueError("backend URL has no host")
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = parsed.port
    rendered = host if port is None else f"{host}:{port}"
    user = (
        unquote(parsed.username) if username is None and parsed.username is not None else username
    )
    secret = (
        unquote(parsed.password) if password is None and parsed.password is not None else password
    )
    if user is not None:
        rendered = quote(user, safe="")
        if secret is not None:
            rendered += f":{quote(secret, safe='')}"
        rendered += f"@{host if port is None else f'{host}:{port}'}"
    return rendered


def replace_database_url(
    value: str,
    database: str,
    *,
    username: str | None = None,
    password: str | None = None,
    scheme: str = "postgresql",
) -> str:
    """Replace only the database and optional credentials in a PostgreSQL URL."""

    if _DATABASE_RE.fullmatch(database) is None and database != "postgres":
        raise ValueError("database name is not a safe generated identifier")
    parsed = urlsplit(value)
    if parsed.scheme not in {"postgres", "postgresql", "postgresql+asyncpg"}:
        raise ValueError("unsupported PostgreSQL URL scheme")
    authority = _authority(parsed, username=username, password=password)
    return urlunsplit((scheme, authority, f"/{quote(database, safe='')}", parsed.query, ""))


def replace_redis_database(value: str, database: int) -> str:
    """Replace a Redis logical database without retaining a fragment."""

    if database not in _REDIS_DATABASES:
        raise ValueError("Redis database must be in the isolated database range")
    parsed = urlsplit(value)
    if parsed.scheme not in {"redis", "rediss"}:
        raise ValueError("unsupported Redis URL scheme")
    return urlunsplit((parsed.scheme, _authority(parsed), f"/{database}", parsed.query, ""))


def public_summary(
    *,
    run_id: str,
    database: str,
    redis_database: int,
    marker: str,
    bucket: str,
    s3_marker_key: str,
) -> dict[str, object]:
    """Return the allowlisted resource identity for a report."""

    _safe_token(run_id)
    if _DATABASE_RE.fullmatch(database) is None:
        raise ValueError("database name is not a generated identifier")
    if _BUCKET_RE.fullmatch(bucket) is None:
        raise ValueError("bucket name is not a generated identifier")
    if s3_marker_key != s3_marker(run_id):
        raise ValueError("S3 marker is not a generated identifier")
    if redis_database not in _REDIS_DATABASES:
        raise ValueError("Redis database is outside the isolated range")
    return {
        "run_id": run_id,
        "postgres_database": database,
        "redis_database": redis_database,
        "redis_marker": marker,
        "s3_bucket": bucket,
        "s3_marker": s3_marker_key,
        "cleanup": "only resources reserved and created by this run",
    }


async def reserve_redis_database(
    value: str,
    run_id: str,
    *,
    candidates: Sequence[int] = _REDIS_DATABASES,
) -> tuple[int, str, str]:
    """Reserve an empty logical Redis DB atomically with a run marker.

    Database zero is deliberately excluded because the live service uses it.
    A non-empty candidate is not reused, so an old or unknown workload is never
    flushed by the cleanup path.
    """

    marker = redis_marker(run_id)
    for database in candidates:
        if database not in _REDIS_DATABASES:
            continue
        client: Any = redis_async.from_url(
            replace_redis_database(value, database), decode_responses=False
        )
        try:
            await client.ping()
            claimed = await client.eval(
                _REDIS_RESERVE_SCRIPT,
                1,
                marker,
                run_id.encode("ascii"),
            )
            if claimed == 1:
                return database, replace_redis_database(value, database), marker
        except Exception:  # noqa: S112 - an unavailable candidate is probed next
            continue
        finally:
            await client.aclose()
    raise BackendIsolationError("redis reservation")


async def release_redis_database(value: str, database: int, marker: str, run_id: str) -> None:
    """Flush and release only a database whose marker proves our ownership."""

    client: Any = redis_async.from_url(
        replace_redis_database(value, database), decode_responses=False
    )
    try:
        actual = await client.get(marker)
        expected = run_id.encode("ascii")
        if actual != expected:
            raise BackendIsolationError("redis ownership verification")
        await client.flushdb()
    except BackendIsolationError:
        raise
    except Exception as error:
        raise BackendIsolationError("redis cleanup") from error
    finally:
        await client.aclose()


def _quote_identifier(value: str) -> str:
    if _DATABASE_RE.fullmatch(value) is None:
        raise ValueError("unsafe PostgreSQL identifier")
    return '"' + value.replace('"', '""') + '"'


async def _admin_connection(admin_dsn: str, database: str = "postgres") -> asyncpg.Connection:
    return await asyncpg.connect(
        replace_database_url(admin_dsn, database), timeout=30, command_timeout=30
    )


async def create_postgres_database(admin_dsn: str, database: str) -> None:
    """Create and permission an isolated database, refusing name collisions."""

    quoted = _quote_identifier(database)
    created = False
    connection = await _admin_connection(admin_dsn)
    try:
        exists = await connection.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", database)
        if exists is not None:
            raise BackendIsolationError("postgres database collision")
        for role in _DATABASE_ROLES:
            role_exists = await connection.fetchval(
                "SELECT 1 FROM pg_roles WHERE rolname = $1", role
            )
            if role_exists is None:
                raise BackendIsolationError("postgres required role unavailable")
        await connection.execute(f"CREATE DATABASE {quoted} OWNER {_MIGRATION_ROLE}")
        created = True
    except BackendIsolationError:
        if created:
            try:
                await drop_postgres_database(admin_dsn, database)
            except BackendIsolationError:
                pass
        raise
    except Exception as error:
        if created:
            try:
                await drop_postgres_database(admin_dsn, database)
            except BackendIsolationError:
                pass
        raise BackendIsolationError("postgres database creation") from error
    finally:
        await connection.close()

    target = await _admin_connection(admin_dsn, database)
    try:
        # Install the two extensions before Alembic as the administrator.  The
        # migration role owns the database but may not be allowed to install
        # extensions on every PostgreSQL/pgvector build.
        await target.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
        await target.execute("CREATE EXTENSION IF NOT EXISTS vector")
        await target.execute(
            "GRANT CONNECT ON DATABASE "
            f"{quoted} TO trpc_runtime, trpc_worker, trpc_migration, trpc_metrics"
        )
        await target.execute(
            "GRANT USAGE ON SCHEMA public TO trpc_runtime, trpc_worker, trpc_metrics"
        )
        await target.execute("GRANT USAGE, CREATE ON SCHEMA public TO trpc_migration")
    except Exception as error:
        if created:
            try:
                await drop_postgres_database(admin_dsn, database)
            except BackendIsolationError:
                pass
        raise BackendIsolationError("postgres database permissions") from error
    finally:
        await target.close()


async def verify_postgres_owner(admin_dsn: str, database: str) -> bool:
    """Prove that the exact generated database is still ours."""

    connection = await _admin_connection(admin_dsn)
    try:
        owner = await connection.fetchval(
            "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = $1", database
        )
        return isinstance(owner, str) and owner == _MIGRATION_ROLE
    finally:
        await connection.close()


async def drop_postgres_database(admin_dsn: str, database: str) -> None:
    """Drop only an exact generated database while proving its owner first."""

    if not await verify_postgres_owner(admin_dsn, database):
        raise BackendIsolationError("postgres ownership verification")
    quoted = _quote_identifier(database)
    connection = await _admin_connection(admin_dsn)
    try:
        await connection.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = $1 AND pid <> pg_backend_pid()",
            database,
        )
        await connection.execute(f"DROP DATABASE {quoted}")
    except Exception as error:
        raise BackendIsolationError("postgres cleanup") from error
    finally:
        await connection.close()


def _s3_client(endpoint: str, access_key: str, secret_key: str) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def create_s3_bucket(
    endpoint: str,
    access_key: str,
    secret_key: str,
    bucket: str,
    marker: str,
    run_id: str,
) -> None:
    """Create a unique bucket; never silently fall back to the shared bucket."""

    if _BUCKET_RE.fullmatch(bucket) is None:
        raise BackendIsolationError("s3 bucket name validation")
    if marker != s3_marker(run_id):
        raise BackendIsolationError("s3 marker validation")
    client = _s3_client(endpoint, access_key, secret_key)
    created = False
    try:
        try:
            client.head_bucket(Bucket=bucket)
        except ClientError as error:
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status != 404:
                raise BackendIsolationError("s3 bucket collision check") from error
        else:
            raise BackendIsolationError("s3 bucket collision")
        client.create_bucket(Bucket=bucket)
        created = True
        client.put_object(Bucket=bucket, Key=marker, Body=run_id.encode("ascii"))
        response = client.get_object(Bucket=bucket, Key=marker)
        body = response["Body"].read()
        if body != run_id.encode("ascii"):
            raise BackendIsolationError("s3 marker verification")
        client.head_bucket(Bucket=bucket)
    except BackendIsolationError:
        if created:
            try:
                client.delete_object(Bucket=bucket, Key=marker)
                client.delete_bucket(Bucket=bucket)
            except Exception as error:
                _LOGGER.debug("isolated bucket rollback failed: %s", type(error).__name__)
        raise
    except Exception as error:
        if created:
            try:
                client.delete_object(Bucket=bucket, Key=marker)
                client.delete_bucket(Bucket=bucket)
            except Exception as rollback_error:
                _LOGGER.debug("isolated bucket rollback failed: %s", type(rollback_error).__name__)
        raise BackendIsolationError("s3 bucket creation") from error


def delete_s3_bucket(
    endpoint: str,
    access_key: str,
    secret_key: str,
    bucket: str,
    marker: str,
    run_id: str,
) -> None:
    """Delete all objects and then the bucket created by this run."""

    if _BUCKET_RE.fullmatch(bucket) is None:
        raise BackendIsolationError("s3 ownership verification")
    if marker != s3_marker(run_id):
        raise BackendIsolationError("s3 marker validation")
    client = _s3_client(endpoint, access_key, secret_key)
    try:
        marker_response = client.get_object(Bucket=bucket, Key=marker)
        marker_body = marker_response["Body"].read()
        if marker_body != run_id.encode("ascii"):
            raise BackendIsolationError("s3 ownership verification")
        continuation: str | None = None
        while True:
            request: dict[str, object] = {"Bucket": bucket}
            if continuation:
                request["ContinuationToken"] = continuation
            response = client.list_objects_v2(**request)
            objects = response.get("Contents", [])
            if isinstance(objects, list) and objects:
                keys = [{"Key": item["Key"]} for item in objects if isinstance(item, dict)]
                if keys:
                    client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
            if not response.get("IsTruncated"):
                break
            next_token = response.get("NextContinuationToken")
            if not isinstance(next_token, str) or not next_token:
                raise BackendIsolationError("s3 cleanup pagination")
            continuation = next_token
        client.delete_bucket(Bucket=bucket)
    except BackendIsolationError:
        raise
    except Exception as error:
        raise BackendIsolationError("s3 cleanup") from error


@dataclass(slots=True)
class BackendIsolation:
    """Private credentials and public resource identity for one ACK run."""

    run_id: str
    database: str
    redis_database: int
    redis_marker: str
    redis_url: str
    runtime_dsn: str
    worker_dsn: str
    migration_dsn: str
    admin_dsn: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str
    s3_marker: str
    _root: Path
    _python_executable: str
    _redis_source_url: str
    _postgres_created: bool = False
    _s3_created: bool = False
    _redis_claimed: bool = False

    @property
    def summary(self) -> dict[str, object]:
        return public_summary(
            run_id=self.run_id,
            database=self.database,
            redis_database=self.redis_database,
            marker=self.redis_marker,
            bucket=self.s3_bucket,
            s3_marker_key=self.s3_marker,
        )

    @property
    def environment(self) -> dict[str, str]:
        return {
            "TRPC_BACKEND_ISOLATION_RUN_ID": self.run_id,
            "TRPC_TEST_POSTGRES_DSN": self.runtime_dsn,
            "TRPC_TEST_POSTGRES_WORKER_DSN": self.worker_dsn,
            "TRPC_TEST_REDIS_URL": self.redis_url,
            "TRPC_TEST_S3_ENDPOINT": self.s3_endpoint,
            "TRPC_TEST_S3_BUCKET": self.s3_bucket,
            "TRPC_MIGRATION_SOURCE_REDIS_URL": self.redis_url,
            "TRPC_MIGRATION_TARGET_DATABASE_DSN": self.runtime_dsn,
        }

    async def migrate_schema(self, environment: Mapping[str, str]) -> None:
        """Run Alembic against the new database without exposing subprocess output."""

        child_environment = os.environ.copy()
        child_environment.update(environment)
        child_environment["TRPC_MIGRATION_DATABASE_DSN"] = self.migration_dsn
        try:
            await asyncio.to_thread(
                _run_alembic,
                self._root,
                self._python_executable,
                child_environment,
            )
        except BackendIsolationError:
            raise
        except Exception as error:
            raise BackendIsolationError("postgres schema migration") from error

    async def cleanup(self) -> list[str]:
        """Best-effort cleanup, returning only safe stage labels."""

        errors: list[str] = []
        if self._postgres_created:
            try:
                await drop_postgres_database(self.admin_dsn, self.database)
            except BackendIsolationError as error:
                errors.append(error.stage)
        if self._s3_created:
            try:
                await asyncio.to_thread(
                    delete_s3_bucket,
                    self.s3_endpoint,
                    self.s3_access_key,
                    self.s3_secret_key,
                    self.s3_bucket,
                    self.s3_marker,
                    self.run_id,
                )
            except BackendIsolationError as error:
                errors.append(error.stage)
        if self._redis_claimed:
            try:
                await release_redis_database(
                    self._redis_source_url,
                    self.redis_database,
                    self.redis_marker,
                    self.run_id,
                )
            except BackendIsolationError as error:
                errors.append(error.stage)
        return errors


def _run_alembic(root: Path, executable: str, environment: Mapping[str, str]) -> None:
    try:
        completed = subprocess.run(  # noqa: S603 - command and module are fixed
            [executable, "-m", "alembic", "upgrade", "head"],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            env=dict(environment),
            timeout=900,
        )
    except subprocess.TimeoutExpired as error:
        raise BackendIsolationError("postgres schema migration timeout") from error
    except OSError as error:
        raise BackendIsolationError("postgres schema migration process") from error
    if completed.returncode != 0:
        raise BackendIsolationError("postgres schema migration")


async def provision_backend_isolation(
    *,
    run_id: str,
    runtime_dsn: str,
    worker_dsn: str,
    migration_dsn: str,
    admin_password: str,
    redis_url: str,
    s3_endpoint: str,
    s3_access_key: str,
    s3_secret_key: str,
    root: Path,
    python_executable: str = sys.executable,
) -> BackendIsolation:
    """Reserve all three data planes and migrate the private PostgreSQL DB."""

    token = _safe_token(run_id)
    database = database_name(token)
    marker = redis_marker(token)
    bucket = bucket_name(token)
    admin_dsn = replace_database_url(
        runtime_dsn,
        "postgres",
        username="postgres",
        password=admin_password,
        scheme="postgresql",
    )
    instance: BackendIsolation | None = None
    try:
        redis_database, isolated_redis_url, _ = await reserve_redis_database(redis_url, token)
        instance = BackendIsolation(
            run_id=token,
            database=database,
            redis_database=redis_database,
            redis_marker=marker,
            redis_url=isolated_redis_url,
            runtime_dsn=replace_database_url(runtime_dsn, database),
            worker_dsn=replace_database_url(worker_dsn, database),
            migration_dsn=replace_database_url(
                migration_dsn, database, scheme="postgresql+asyncpg"
            ),
            admin_dsn=admin_dsn,
            s3_endpoint=s3_endpoint,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            s3_bucket=bucket,
            s3_marker=s3_marker(token),
            _root=root,
            _python_executable=python_executable,
            _redis_source_url=redis_url,
            _redis_claimed=True,
        )
        await asyncio.to_thread(
            create_s3_bucket,
            s3_endpoint,
            s3_access_key,
            s3_secret_key,
            bucket,
            instance.s3_marker,
            token,
        )
        instance._s3_created = True
        await create_postgres_database(admin_dsn, database)
        instance._postgres_created = True
        await instance.migrate_schema(instance.environment)
        return instance
    except BackendIsolationError as error:
        if instance is not None:
            rollback_errors = await instance.cleanup()
            if rollback_errors:
                raise BackendIsolationError(error.stage, cleanup_errors=rollback_errors) from error
        raise
    except Exception as error:
        cleanup_errors: list[str] = []
        if instance is not None:
            cleanup_errors = await instance.cleanup()
        raise BackendIsolationError("provisioning", cleanup_errors=cleanup_errors) from error
