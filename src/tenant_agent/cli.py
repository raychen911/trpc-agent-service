"""Operations CLI."""

from __future__ import annotations

import asyncio
import getpass
import importlib.metadata
import json
import os
import platform
from dataclasses import asdict
from pathlib import Path
from typing import Annotated

import httpx
import typer
import uvicorn
import yaml
from sqlalchemy.engine import make_url

from tenant_agent.channels.wecom_bot import WeComBotConnection, validate_bot_credentials
from tenant_agent.models import ChannelType, KnowledgeRecord, TenantConfig
from tenant_agent.resources import resource_root
from tenant_agent.services.capacity import CapacityInputs, estimate_capacity
from tenant_agent.services.config import configuration_checksum
from tenant_agent.services.migration import (
    DataMigrator,
    EmbeddingTransform,
    GoldenQuery,
    migrate_trpc_session_history,
)
from tenant_agent.services.native_session import (
    ProvisionedSqlSessionService,
    provision_native_sql_schema,
    validate_native_sql_schema,
)
from tenant_agent.settings import ServiceRole, Settings
from tenant_agent.storage.base import TenantDataPlane
from tenant_agent.storage.external import QdrantKnowledgeRepository
from tenant_agent.storage.redis import RedisPlane
from tenant_agent.storage.sql import SqlPlane

app = typer.Typer(no_args_is_help=True, help="Operate the multi-tenant tRPC-Agent platform.")


@app.command("wecom-bot")
def wecom_bot(
    probe_only: Annotated[
        bool, typer.Option(help="Authenticate and ping, then disconnect without replying")
    ] = False,
    port: Annotated[int, typer.Option(min=1, max=65535)] = 8081,
    config: Annotated[
        Path | None,
        typer.Option(help="Tenant YAML; omit to use the offline connection-test profile"),
    ] = None,
    bot_id_env: Annotated[
        str, typer.Option(help="Environment variable referenced by the Bot binding")
    ] = "TENANT_BOTDEMO_WECOM_BOT_ID",
    bot_secret_env: Annotated[
        str, typer.Option(help="Environment variable referenced by the Bot binding")
    ] = "TENANT_BOTDEMO_WECOM_BOT_SECRET",  # noqa: S107 - this is an environment variable name
) -> None:
    """Connect the local WeCom bot using secret environment variables or hidden prompts.

    The default profile is an offline transport test; production model/backends
    remain configurable through the standard tenant YAML and serve command.
    """

    bot_id = os.getenv(bot_id_env) or getpass.getpass("WeCom Bot ID (hidden): ")
    secret = os.getenv(bot_secret_env) or getpass.getpass("WeCom Secret (hidden): ")
    validate_bot_credentials(bot_id, secret)
    if probe_only:

        async def probe() -> None:
            async def ignore_frame(frame: dict[str, object]) -> None:
                del frame

            connection = WeComBotConnection()
            async with connection.connected(bot_id, secret, ignore_frame):
                await connection.request("ping")

        try:
            asyncio.run(probe())
        except Exception as exc:
            typer.echo(json.dumps({"ok": False, "error_type": exc.__class__.__name__}))
            raise typer.Exit(code=2) from None
        typer.echo(json.dumps({"ok": True, "check": "authenticated_and_heartbeat_acknowledged"}))
        return
    from tenant_agent.main import create_app

    os.environ[bot_id_env] = bot_id
    os.environ[bot_secret_env] = secret
    if config is None:
        config = resource_root() / "config/tenant.wecom-bot.example.yaml"
        settings = Settings(
            environment="development",
            service_role=ServiceRole.ALL,
            bootstrap_config_path=config,
            control_database_url="sqlite+aiosqlite:///./wecom-bot.db",
            auto_create_schema=True,
            broker_mode="inline",
            host="127.0.0.1",
            port=port,
            enable_browser_ui=False,
        )
        typer.echo("Starting the offline WeCom connection-test profile. Model credentials are separate.")
    else:
        settings = Settings(bootstrap_config_path=config, service_role=ServiceRole.ALL, port=port)
        typer.echo(f"Starting WeCom Bot with configured tenant model/backends from {config}.")
    uvicorn.run(create_app(settings), host=settings.host, port=port, log_config=None)


@app.command()
def serve(
    role: Annotated[ServiceRole, typer.Option()] = ServiceRole.ALL,
    host: Annotated[str, typer.Option()] = "0.0.0.0",
    port: Annotated[int, typer.Option()] = 8080,
) -> None:
    """Start an all-in-one or role-specific service node."""

    from tenant_agent.main import create_app

    settings = Settings(service_role=role, host=host, port=port)
    uvicorn.run(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.casefold(),
        log_config=None,
    )


@app.command("validate-config")
def validate_config(path: Path) -> None:
    """Validate tenant YAML without resolving or printing secrets."""

    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        rows = payload if isinstance(payload, list) else payload.get("tenants", [])
        results = []
        for row in rows:
            tenant = TenantConfig.model_validate(row)
            results.append(
                {
                    "tenant_id": tenant.tenant_id,
                    "revision": tenant.revision,
                    "checksum_sha256": configuration_checksum(tenant),
                }
            )
    except Exception as exc:
        typer.echo(
            json.dumps({"valid": False, "error_type": exc.__class__.__name__}, indent=2),
            err=True,
        )
        raise typer.Exit(code=2) from None
    typer.echo(json.dumps({"valid": True, "tenants": results}, indent=2))


@app.command("db-init")
def db_init(
    database_url_env: Annotated[
        str,
        typer.Option(help="Environment variable containing the platform SQL DSN"),
    ] = "TAP_CONTROL_DATABASE_URL",
) -> None:
    """Apply platform migrations to a control or tenant resource database."""

    from alembic import command
    from alembic.config import Config

    if database_url_env == "TAP_CONTROL_DATABASE_URL":
        url = Settings().control_database_url.get_secret_value()
    else:
        url = os.getenv(database_url_env) or ""
        if not url:
            raise typer.BadParameter("database URL environment variable is not set")
    if url == "inmemory://":
        typer.echo("InMemory control plane does not require schema migration.")
        return
    project_root = resource_root()
    config = Config(str(project_root / "alembic.ini"))
    config.attributes["tenant_agent_explicit_url"] = True
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    try:
        command.upgrade(config, "head")
    except Exception as exc:
        typer.echo(
            json.dumps({"ok": False, "error_type": exc.__class__.__name__}, indent=2),
            err=True,
        )
        raise typer.Exit(code=2) from None
    typer.echo("Platform SQL migrations applied.")


@app.command("native-session-init")
def native_session_init(
    database_url_env: Annotated[
        str,
        typer.Option(help="Environment variable containing the isolated native SQL DSN"),
    ],
) -> None:
    """Provision the pinned tRPC native Session schema in an isolated database."""

    url = os.getenv(database_url_env) or ""
    if not url:
        raise typer.BadParameter("native Session URL environment variable is not set")
    try:
        asyncio.run(provision_native_sql_schema(url))
    except Exception as exc:
        typer.echo(
            json.dumps({"ok": False, "error_type": exc.__class__.__name__}, indent=2),
            err=True,
        )
        raise typer.Exit(code=2) from None
    typer.echo("Native tRPC Session schema provisioned.")


@app.command()
def doctor() -> None:
    """Report a credential-safe local readiness summary."""

    settings = Settings()
    output = {
        "python": platform.python_version(),
        "trpc_agent_py": importlib.metadata.version("trpc-agent-py"),
        "service_role": settings.service_role.value,
        "broker_mode": settings.broker_mode,
        "bootstrap_exists": bool(settings.bootstrap_config_path and settings.bootstrap_config_path.exists()),
        "control_backend": settings.control_database_url.get_secret_value().split(":", 1)[0],
        "redis_configured": settings.redis_url is not None,
    }
    typer.echo(json.dumps(output, indent=2))


@app.command("probe-channel")
def probe_channel(
    channel: Annotated[ChannelType, typer.Option()],
    binding_id: Annotated[str, typer.Option()],
) -> None:
    """Safely verify configured IM credentials without sending a message."""

    from tenant_agent.container import ApplicationContainer

    async def run() -> dict[str, object]:
        container = ApplicationContainer.build(Settings())
        try:
            await container.initialize()
            tenant = await container.configs.resolve_binding(
                channel.value,
                binding_id,
            )
            binding = next(
                item for item in tenant.channels if item.binding_id == binding_id and item.channel == channel
            )
            if channel is ChannelType.WEB:
                reference = binding.credential_refs["webhook_token"]
                await container.secrets.resolve(reference)
                return {
                    "ok": True,
                    "channel": channel.value,
                    "tenant_id": tenant.tenant_id,
                    "check": "webhook credential resolved",
                }
            adapter = container.channels.get(channel)
            if channel is ChannelType.TELEGRAM:
                token = await container.secrets.resolve(binding.credential_refs["bot_token"])
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.get(
                        f"{adapter.api_base}/bot{token}/getMe"  # type: ignore[attr-defined]
                    )
                payload = response.json()
                if not response.is_success or not payload.get("ok"):
                    raise RuntimeError("Telegram credential check failed")
                return {
                    "ok": True,
                    "channel": channel.value,
                    "tenant_id": tenant.tenant_id,
                    "bot_id": str(payload.get("result", {}).get("id", "")),
                    "username": str(payload.get("result", {}).get("username", "")),
                }
            if channel is ChannelType.WECOM_BOT:
                bot_id, bot_secret = await asyncio.gather(
                    container.secrets.resolve(binding.credential_refs["bot_id"]),
                    container.secrets.resolve(binding.credential_refs["bot_secret"]),
                )
                validate_bot_credentials(bot_id, bot_secret)

                async def ignore_frame(frame: dict[str, object]) -> None:
                    del frame

                connection = WeComBotConnection()
                async with connection.connected(bot_id, bot_secret, ignore_frame):
                    await connection.request("ping")
                return {
                    "ok": True,
                    "channel": channel.value,
                    "tenant_id": tenant.tenant_id,
                    "check": "bot websocket authenticated and heartbeat acknowledged",
                }
            corp_id, corp_secret = await asyncio.gather(
                container.secrets.resolve(binding.credential_refs["corp_id"]),
                container.secrets.resolve(binding.credential_refs["corp_secret"]),
            )
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"{adapter.api_base}/cgi-bin/gettoken",  # type: ignore[attr-defined]
                    params={"corpid": corp_id, "corpsecret": corp_secret},
                )
            payload = response.json()
            if not response.is_success or payload.get("errcode") != 0:
                raise RuntimeError("WeCom credential check failed")
            return {
                "ok": True,
                "channel": channel.value,
                "tenant_id": tenant.tenant_id,
                "check": "access token acquired and discarded",
            }
        finally:
            await container.close()

    try:
        typer.echo(json.dumps(asyncio.run(run()), indent=2))
    except Exception as exc:
        typer.echo(
            json.dumps(
                {"ok": False, "error_type": exc.__class__.__name__},
                indent=2,
            ),
            err=True,
        )
        raise typer.Exit(code=2) from None


@app.command()
def capacity(
    peak_rps: Annotated[float, typer.Option(min=0.01)],
    p95_latency: Annotated[float, typer.Option(min=0.01)],
    concurrency_per_worker: Annotated[int, typer.Option(min=1)] = 32,
    average_input_tokens: Annotated[int, typer.Option(min=0)] = 1_000,
    average_output_tokens: Annotated[int, typer.Option(min=0)] = 500,
    headroom: Annotated[float, typer.Option(min=1.0, max=5.0)] = 1.5,
) -> None:
    """Estimate worker count, backend QPS, and queue capacity."""

    result = estimate_capacity(
        CapacityInputs(
            peak_callbacks_per_second=peak_rps,
            p95_agent_latency_seconds=p95_latency,
            max_concurrent_sessions_per_worker=concurrency_per_worker,
            average_input_tokens=average_input_tokens,
            average_output_tokens=average_output_tokens,
            headroom_ratio=headroom,
        )
    )
    typer.echo(result.model_dump_json(indent=2))


@app.command("migrate-data")
def migrate_data(
    tenant_id: Annotated[str, typer.Option()],
    source_kind: Annotated[str, typer.Option(help="sql, redis, local-vector, or qdrant")],
    source_dsn_env: Annotated[str, typer.Option(help="Environment variable containing the source DSN")],
    target_kind: Annotated[str, typer.Option(help="sql, redis, local-vector, or qdrant")],
    target_dsn_env: Annotated[str, typer.Option(help="Environment variable containing the target DSN")],
    source_namespace: Annotated[str, typer.Option()] = "tap:v1",
    target_namespace: Annotated[str, typer.Option()] = "tap:v1",
    resources: Annotated[
        str,
        typer.Option(help="Comma-separated sessions,summaries,memories,artifacts,knowledge"),
    ] = "sessions,summaries,memories",
    embedding_map: Annotated[
        Path | None,
        typer.Option(help="JSON map of document/chunk IDs to replacement embeddings"),
    ] = None,
    golden_queries: Annotated[
        Path | None,
        typer.Option(help="JSON golden-query recall cases required with --embedding-map"),
    ] = None,
    allow_empty_source: Annotated[
        bool,
        typer.Option(help="Explicitly allow a selected source with zero records"),
    ] = False,
    include_native_session_history: Annotated[
        bool,
        typer.Option(help="Also migrate native tRPC SessionService history"),
    ] = False,
    source_native_dsn_env: Annotated[
        str | None,
        typer.Option(help="Environment variable containing the isolated source native Session DSN"),
    ] = None,
    target_native_dsn_env: Annotated[
        str | None,
        typer.Option(help="Environment variable containing the isolated target native Session DSN"),
    ] = None,
    source_redis_cluster: Annotated[
        bool,
        typer.Option(help="Use Redis Cluster clients for the source"),
    ] = False,
    target_redis_cluster: Annotated[
        bool,
        typer.Option(help="Use Redis Cluster clients for the target"),
    ] = False,
) -> None:
    """Idempotently copy and hash-verify one tenant without printing DSNs."""

    source_dsn = os.getenv(source_dsn_env)
    target_dsn = os.getenv(target_dsn_env)
    if not source_dsn or not target_dsn:
        raise typer.BadParameter("both DSN environment variables must be set")
    selected_resources = tuple(item.strip() for item in resources.split(",") if item.strip())
    if (embedding_map is None) != (golden_queries is None):
        raise typer.BadParameter("--embedding-map and --golden-queries must be supplied together")
    if embedding_map is not None and "knowledge" not in selected_resources:
        raise typer.BadParameter("embedding migration requires the knowledge resource")

    transform: EmbeddingTransform | None = None
    recall_cases: tuple[GoldenQuery, ...] = ()
    if embedding_map is not None and golden_queries is not None:
        try:
            embedding_payload = json.loads(embedding_map.read_text(encoding="utf-8"))
            raw_embeddings = embedding_payload.get("chunks", embedding_payload)
            if not isinstance(raw_embeddings, dict):
                raise ValueError("embedding map must be an object")
            query_payload = json.loads(golden_queries.read_text(encoding="utf-8"))
            raw_queries = query_payload.get("queries", query_payload)
            if not isinstance(raw_queries, list):
                raise ValueError("golden queries must be a list")
            recall_cases = tuple(
                GoldenQuery(
                    embedding=tuple(float(value) for value in row["embedding"]),
                    expected_ids=tuple(str(value) for value in row["expected_ids"]),
                    min_matches=int(row.get("min_matches", 1)),
                    limit=int(row.get("limit", 10)),
                    metadata_filter=row.get("metadata_filter"),
                )
                for row in raw_queries
            )

            async def mapped_embedding(record: KnowledgeRecord) -> KnowledgeRecord:
                key = f"{record.document_id}/{record.chunk_id}"
                row = raw_embeddings.get(key)
                if not isinstance(row, dict):
                    raise ValueError(f"embedding map is missing {key}")
                return record.model_copy(
                    update={
                        "embedding": tuple(float(value) for value in row["embedding"]),
                        "embedding_model": str(row["embedding_model"]),
                    }
                )

            transform = mapped_embedding
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise typer.BadParameter(
                f"invalid embedding migration input ({exc.__class__.__name__})"
            ) from None

    def sqlite_path(dsn: str) -> Path | None:
        if "://" not in dsn:
            return Path(dsn).resolve()
        url = make_url(dsn)
        if not url.drivername.startswith("sqlite"):
            return None
        return Path(str(url.database)).resolve() if url.database else None

    def adapter(kind: str, dsn: str, namespace: str, *, role: str) -> object:
        if kind == "sql":
            path = sqlite_path(dsn)
            if path is not None and not path.exists():
                raise typer.BadParameter(f"{role} SQLite database must be pre-provisioned")
            return SqlPlane(dsn, create_schema=False)
        if kind == "redis":
            cluster = source_redis_cluster if role == "source" else target_redis_cluster
            return RedisPlane(dsn, namespace=namespace, cluster=cluster)
        if kind == "local-vector":
            path = sqlite_path(dsn)
            if path is None or not path.exists():
                raise typer.BadParameter(f"{role} local-vector database must be pre-provisioned")
            url = dsn if "://" in dsn else f"sqlite+aiosqlite:///{path.as_posix()}"
            return SqlPlane(url, create_schema=False)
        if kind == "qdrant":
            return QdrantKnowledgeRepository(dsn, namespace=namespace)
        raise typer.BadParameter(f"unsupported backend kind: {kind}")

    async def native_session_service(kind: str, dsn: str, *, cluster: bool) -> object:
        from trpc_agent_sdk.sessions import (
            RedisClusterSessionService,
            RedisSessionService,
            SessionServiceConfig,
        )

        config = SessionServiceConfig(max_events=200, store_historical_events=True)
        if kind == "redis":
            if cluster:
                return RedisClusterSessionService(
                    db_url=dsn,
                    is_async=True,
                    session_config=config,
                )
            return RedisSessionService(db_url=dsn, is_async=True, session_config=config)
        if kind != "sql":
            raise typer.BadParameter("native Session history supports only SQL or Redis")
        await validate_native_sql_schema(dsn)
        return ProvisionedSqlSessionService(
            db_url=dsn,
            is_async=True,
            session_config=config,
            expire_on_commit=False,
        )

    def native_dsn(
        kind: str,
        platform_dsn: str,
        environment_name: str | None,
        *,
        role: str,
    ) -> str:
        if kind not in {"sql", "redis"}:
            raise typer.BadParameter("native Session history supports only SQL or Redis")
        if environment_name is None:
            if kind == "sql":
                raise typer.BadParameter(
                    f"--{role}-native-dsn-env is required for isolated SQL native Session history"
                )
            return platform_dsn
        resolved = os.getenv(environment_name) or ""
        if not resolved:
            raise typer.BadParameter(f"{role} native DSN environment variable is not set")
        if kind == "sql":
            if make_url(resolved) == make_url(platform_dsn):
                raise typer.BadParameter(f"{role} platform and native SQL Session DSNs must be different")
            path = sqlite_path(resolved)
            if path is not None and not path.exists():
                raise typer.BadParameter(f"{role} native SQLite database must be pre-provisioned")
        return resolved

    def as_plane(value: object) -> TenantDataPlane:
        return TenantDataPlane(
            sessions=value,  # type: ignore[arg-type]
            memories=value,  # type: ignore[arg-type]
            summaries=value,  # type: ignore[arg-type]
            artifacts=value,  # type: ignore[arg-type]
            knowledge=value,  # type: ignore[arg-type]
            audit=value,  # type: ignore[arg-type]
            receipts=value,  # type: ignore[arg-type]
            usage=value,  # type: ignore[arg-type]
            concurrency=value,  # type: ignore[arg-type]
            outbox=value,  # type: ignore[arg-type]
            leases=value,  # type: ignore[arg-type]
        )

    async def has_selected_source_data(plane: TenantDataPlane) -> bool:
        iterators = {
            "sessions": plane.sessions.iter_sessions,
            "summaries": plane.summaries.iter_summaries,
            "memories": plane.memories.iter_memories,
            "artifacts": plane.artifacts.iter_artifacts,
            "knowledge": plane.knowledge.iter_knowledge,
        }
        for resource in selected_resources:
            iterator = iterators.get(resource)
            if iterator is None:
                continue
            async for _ in iterator(tenant_id):
                return True
        return False

    async def run() -> None:
        source: object | None = None
        target: object | None = None
        try:
            source = adapter(source_kind, source_dsn, source_namespace, role="source")
            target = adapter(target_kind, target_dsn, target_namespace, role="target")
            await source.initialize()  # type: ignore[attr-defined]
            await target.initialize()  # type: ignore[attr-defined]
            source_plane = as_plane(source)
            target_plane = as_plane(target)
            source_nonempty = await has_selected_source_data(source_plane)
            report = await DataMigrator(source_plane, target_plane).migrate(
                tenant_id,
                resources=selected_resources,
                embedding_transform=transform,
                golden_queries=recall_cases,
            )
            if not source_nonempty and not allow_empty_source:
                report.mismatches.append("empty_source")
            if include_native_session_history:
                if "sessions" not in selected_resources:
                    raise typer.BadParameter("native Session history requires the sessions resource")
                resolved_source_native_dsn = native_dsn(
                    source_kind,
                    source_dsn,
                    source_native_dsn_env,
                    role="source",
                )
                resolved_target_native_dsn = native_dsn(
                    target_kind,
                    target_dsn,
                    target_native_dsn_env,
                    role="target",
                )
                source_native = await native_session_service(
                    source_kind,
                    resolved_source_native_dsn,
                    cluster=source_redis_cluster,
                )
                target_native = await native_session_service(
                    target_kind,
                    resolved_target_native_dsn,
                    cluster=target_redis_cluster,
                )
                try:
                    native_report = await migrate_trpc_session_history(
                        tenant_id=tenant_id,
                        manifest_sessions=source,
                        source_service=source_native,
                        target_service=target_native,
                    )
                    report.copied["native_trpc_events"] = native_report.copied
                    report.source_hashes["native_trpc_sessions"] = native_report.source_hash
                    report.target_hashes["native_trpc_sessions"] = native_report.target_hash
                    report.verification_modes["native_trpc_sessions"] = "exact-state-and-events"
                    if native_report.source_hash != native_report.target_hash:
                        report.mismatches.append("native_trpc_sessions")
                finally:
                    await source_native.close()  # type: ignore[attr-defined]
                    await target_native.close()  # type: ignore[attr-defined]
            typer.echo(json.dumps(asdict(report) | {"verified": report.verified}, indent=2))
            if not report.verified:
                raise typer.Exit(code=2)
        finally:
            if source is not None:
                await source.close()  # type: ignore[attr-defined]
            if target is not None:
                await target.close()  # type: ignore[attr-defined]

    try:
        asyncio.run(run())
    except typer.Exit:
        raise
    except Exception as exc:
        typer.echo(
            json.dumps({"ok": False, "error_type": exc.__class__.__name__}, indent=2),
            err=True,
        )
        raise typer.Exit(code=2) from None


if __name__ == "__main__":
    app()
