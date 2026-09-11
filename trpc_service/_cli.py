"""Process entrypoints used by local development and Docker Compose."""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import uuid


def database_url() -> str:
    return os.environ.get("TRPC_DATABASE_URL", "sqlite+pysqlite:///data/trpc-service.db")


def migrate() -> None:
    from trpc_service.persistence import Database

    database = Database(database_url())
    database.create_schema()
    database.close()


def init_resources():
    from qdrant_client import QdrantClient, models
    client = QdrantClient(url=os.environ["TRPC_QDRANT_URL"], timeout=15)
    try:
        collection = os.environ.get("TRPC_VECTOR_COLLECTION", "trpc_knowledge")
        dimensions = int(os.environ["TRPC_VECTOR_DIMENSIONS"])
        if dimensions < 1:
            raise ValueError("vector dimensions must be positive")
        if not client.collection_exists(collection):
            client.create_collection(collection,
                                     vectors_config=models.VectorParams(size=dimensions,
                                                                        distance=models.Distance.COSINE))
        else:
            vectors = client.get_collection(collection).config.params.vectors
            if not isinstance(vectors, models.VectorParams) or vectors.size != dimensions:
                raise ValueError("existing collection dimensions differ; create a new index")
    finally:
        client.close()


async def worker(command="worker") -> None:
    """Load trusted configuration, consume durable work, then drain on SIGTERM."""

    from trpc_service.persistence import Database
    from trpc_service.service_runtime import ServiceRuntime
    from trpc_service.reliability import DurablePostTurnWorker
    from trpc_service.worker import AgentWorker, WorkerSettings, consume
    from trpc_service.worker_health import heartbeat

    database = Database(database_url())
    database.check_schema()
    from trpc_service.telemetry.runtime import configure, shutdown
    configure(command, database, sdk_traces=True)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
    runtime = ServiceRuntime(database)
    health_task = None
    try:
        await runtime.start()
        health_task = asyncio.create_task(heartbeat(database, stop))
        worker_id = f"{os.environ.get('HOSTNAME', 'worker')}-{uuid.uuid4().hex[:12]}"
        settings = WorkerSettings(concurrency=int(os.environ.get("TRPC_WORKER_CONCURRENCY", "4")),
                                  lease_seconds=float(os.environ.get("TRPC_LEASE_SECONDS", "30")),
                                  drain_seconds=float(os.environ.get("TRPC_DRAIN_SECONDS", "20")))
        if command == "worker":
            await AgentWorker(runtime.inbox,
                              runtime.pipeline,
                              runtime.registry,
                              worker_id=worker_id,
                              settings=settings,
                              include_im=runtime.protected_mode).run(stop)
        else:
            consumer = DurablePostTurnWorker(runtime.post, runtime.process_post_turn, worker_id=worker_id)
            task = asyncio.create_task(consume(consumer.run_once, stop))
            await stop.wait()
            try:
                await asyncio.wait_for(task, settings.drain_seconds)
            except asyncio.TimeoutError:
                pass
    finally:
        if health_task is not None:
            health_task.cancel()
            await asyncio.gather(health_task, return_exceptions=True)
        await runtime.close()
        shutdown()
        database.close()


async def channels(command="channel"):
    from trpc_service.persistence import Database
    from trpc_service.management import ConfigurationStore
    from trpc_service.channels.runtime import run_channel_process
    from trpc_service.telemetry.runtime import configure, shutdown
    from trpc_service.worker_health import heartbeat
    database = Database(database_url())
    database.check_schema()
    configure(command, database)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
    health = asyncio.create_task(heartbeat(database, stop))
    try:
        if command == "action-worker":
            from trpc_service.governance.action_process import run_action_process
            await run_action_process(database, stop)
        else:
            await run_channel_process(database, ConfigurationStore(database), stop)
    finally:
        health.cancel()
        await asyncio.gather(health, return_exceptions=True)
        shutdown()
        database.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command",
                        choices=("gateway", "migrate", "init-resources", "worker", "post-turn", "healthcheck",
                                 "channel", "upgrade-schema", "configure-im", "wecom-test", "model-test", "wecom-e2e",
                                 "init-audit-archive", "audit-archive", "audit-retention", "audit-verify",
                                 "telegram-e2e", "action-worker", "configure-feishu", "feishu-e2e", "dual-im-e2e",
                                 "prepare-protected-demo", "protected-demo-e2e", "live-acceptance",
                                 "backup-manifest", "verify-backup", "snapshot", "restore-snapshot",
                                 "consistency-check", "verify-snapshot", "restore-drill", "release", "slo-report",
                                 "alert-test",
                                 ))
    parser.add_argument("--secrets-file")
    parser.add_argument("--feishu-secrets-file", default=".secrets/feishu.json")
    parser.add_argument("--app-id", help="App ID for configure-feishu; never pass App Secret on the command line")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--backup-id")
    parser.add_argument("--tenant-id")
    parser.add_argument("--audit-source", choices=("platform", "tenant_audit"), default="platform")
    parser.add_argument("--after-sequence", type=int, default=0)
    parser.add_argument("--output", default="reports/backup-manifest.json")
    parser.add_argument("--manifest")
    parser.add_argument("--snapshot-dir", default="reports/snapshot")
    parser.add_argument("--destructive", action="store_true")
    parser.add_argument("--target-database-url")
    parser.add_argument("--evidence-output", default="reports/restore-drill.json")
    parser.add_argument("--interval-seconds", type=int, default=0)
    parser.add_argument("--management-url", default=os.environ.get("TRPC_MANAGEMENT_URL", "http://localhost:8080"))
    parser.add_argument("--release-tenant")
    parser.add_argument("--target-version", type=int)
    parser.add_argument("--expected-active-version", type=int)
    parser.add_argument("--phases", default="1,10,25,50,100")
    parser.add_argument("--probe", action="append", default=[])
    parser.add_argument("--alert-webhook")
    parser.add_argument("--alert-token")
    parser.add_argument("--test-timeout", type=int, choices=range(5, 601), metavar="5..600", default=180)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", default=8080, type=int)
    args = parser.parse_args()
    from trpc_service.telemetry.logging import configure_logging
    configure_logging(args.command)
    if args.command == "live-acceptance":
        from trpc_service.live_acceptance import main as live_acceptance
        raise SystemExit(0 if live_acceptance(args.test_timeout) else 1)
    if args.command == "configure-feishu":
        from trpc_service.im_setup import configure_feishu
        configure_feishu(args.secrets_file or ".secrets/feishu.json", app_id=args.app_id)
        return
    if args.command == "configure-im":
        from trpc_service.im_setup import configure
        configure(args.secrets_file or ".secrets/im.json")
        return
    if args.secrets_file:
        from trpc_service.im_setup import load_bundle
        load_bundle(args.secrets_file)
    if args.command in {"prepare-protected-demo", "protected-demo-e2e"}:
        from trpc_service.im_setup import load_bundle
        from trpc_service.persistence import Database
        from trpc_service.protected_demo import prepare
        load_bundle(args.secrets_file or ".secrets/im.json")
        load_bundle(args.feishu_secrets_file)
        database = Database(database_url())
        try:
            if args.command == "prepare-protected-demo":
                print(prepare(database))
            else:
                from trpc_service.channels.protected_acceptance import run
                raise SystemExit(0 if asyncio.run(run(database, args.test_timeout)) else 1)
        finally:
            database.close()
        return
    if args.command == "dual-im-e2e":
        from trpc_service.channels.dual_acceptance import run
        raise SystemExit(0 if asyncio.run(
            run(args.test_timeout, args.secrets_file or ".secrets/im.json", args.feishu_secrets_file)) else 1)
    if args.command == "feishu-e2e":
        from trpc_service.im_setup import load_bundle
        load_bundle(args.feishu_secrets_file)
    if args.command in {"wecom-e2e", "telegram-e2e", "feishu-e2e"}:
        from trpc_service.channels.acceptance import run_from_environment
        raise SystemExit(0 if asyncio.run(run_from_environment(args.command.split("-")[0], args.test_timeout)) else 1)
    elif args.command == "model-test":
        from trpc_service.agent.model_probe import run_from_environment
        raise SystemExit(0 if asyncio.run(run_from_environment()) else 1)
    elif args.command == "wecom-test":
        from trpc_service.channels.wecom_probe import run_from_environment
        raise SystemExit(0 if asyncio.run(run_from_environment(args.test_timeout)) else 1)
    elif args.command == "healthcheck":
        from trpc_service.worker_health import check_health

        check_health()
    elif args.command == "migrate":
        migrate()
    elif args.command == "upgrade-schema":
        from trpc_service.persistence import Database
        from trpc_service.persistence.schema import upgrade_phase_two
        database = Database(database_url())
        try:
            print(upgrade_phase_two(database, offline=args.offline, backup_id=args.backup_id or ""))
        finally:
            database.close()
    elif args.command == "init-resources":
        init_resources()
    elif args.command in {"backup-manifest", "verify-backup"}:
        from trpc_service.operations.backup import create_manifest, verify_manifest
        from trpc_service.persistence import Database
        database = Database(database_url())
        try:
            if args.command == "backup-manifest":
                import json
                print(json.dumps(create_manifest(database, args.output), ensure_ascii=False))
            elif not args.manifest:
                parser.error("--manifest is required for verify-backup")
            else:
                import json
                print(json.dumps(verify_manifest(database, args.manifest), ensure_ascii=False))
        finally:
            database.close()
    elif args.command in {"snapshot", "restore-snapshot"}:
        from trpc_service.operations.snapshots import create_snapshot, restore_snapshot
        from trpc_service.persistence import Database
        database = Database(database_url())
        try:
            import json
            if args.command == "snapshot":
                print(json.dumps(create_snapshot(database, args.snapshot_dir), ensure_ascii=False))
            else:
                print(json.dumps(restore_snapshot(database, args.snapshot_dir, destructive=args.destructive),
                                 ensure_ascii=False))
        finally:
            database.close()
    elif args.command in {"consistency-check", "restore-drill", "verify-snapshot"}:
        import json
        from trpc_service.operations.drill import consistency_check, run_restore_drill, write_drill_evidence
        from trpc_service.persistence import Database
        if args.command == "verify-snapshot":
            from trpc_service.operations.snapshots import validate_snapshot
            result = {"verified": True, "snapshot": args.snapshot_dir,
                      "stores": validate_snapshot(args.snapshot_dir).get("stores", {})}
        elif args.command == "restore-drill":
            if not args.target_database_url:
                parser.error("--target-database-url is required for restore-drill")
            if args.interval_seconds < 0:
                parser.error("--interval-seconds must be non-negative")
            if args.interval_seconds:
                import time
                while True:
                    result = run_restore_drill(args.snapshot_dir, args.target_database_url)
                    write_drill_evidence(result, args.evidence_output)
                    print(json.dumps(result, ensure_ascii=False), flush=True)
                    time.sleep(args.interval_seconds)
            result = run_restore_drill(args.snapshot_dir, args.target_database_url)
        else:
            database = Database(database_url())
            try:
                result = consistency_check(database)
            finally:
                database.close()
        write_drill_evidence(result, args.evidence_output)
        print(json.dumps(result, ensure_ascii=False))
    elif args.command == "release":
        if not args.release_tenant or args.target_version is None or args.expected_active_version is None:
            parser.error("--release-tenant, --target-version and --expected-active-version are required")
        from trpc_service.operations.release import rollout
        token = os.environ.get("TRPC_ADMIN_TOKEN")
        if not token:
            parser.error("TRPC_ADMIN_TOKEN is required for release")
        result = rollout(args.management_url, token, args.release_tenant, args.target_version,
                         args.expected_active_version, tuple(int(item) for item in args.phases.split(",")),
                         tuple(args.probe))
        print(json.dumps(result.__dict__, ensure_ascii=False))
        if result.rolled_back:
            raise SystemExit(1)
    elif args.command == "slo-report":
        from trpc_service.operations.slo import report
        from trpc_service.telemetry.runtime import registry
        print(json.dumps(report(registry.snapshot()), ensure_ascii=False))
    elif args.command == "alert-test":
        if not args.alert_webhook:
            parser.error("--alert-webhook is required for alert-test")
        from trpc_service.operations.slo import send_test
        print(json.dumps(send_test(args.alert_webhook, args.alert_token), ensure_ascii=False))
    elif args.command == "init-audit-archive":
        from trpc_service.governance.audit_retention import initialize_archive
        print(initialize_archive())
    elif args.command in {"audit-archive", "audit-retention", "audit-verify"}:
        import json
        from trpc_service.persistence import Database
        from trpc_service.governance.audit_retention import run_tenant, worker as retention_worker, archive_service
        database = Database(database_url())
        try:
            database.check_schema()
            if args.command == "audit-retention":
                asyncio.run(retention_worker(database))
            elif not args.tenant_id:
                parser.error("--tenant-id is required for one-shot archival and verification")
            elif args.command == "audit-archive":
                print(json.dumps(run_tenant(database, args.tenant_id)))
            else:
                with archive_service(database, args.tenant_id, args.audit_source) as (archive, _):
                    print(json.dumps(archive.verify_chain(args.tenant_id, after_sequence=args.after_sequence)))
        finally:
            database.close()
    elif args.command in {"worker", "post-turn"}:
        if os.name == "nt":
            # Psycopg async connections require Selector on Windows. Keep the
            # channel/test parent on Proactor for asyncio subprocess support.
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(worker(args.command))
    elif args.command in {"channel", "action-worker"}:
        if args.command == "action-worker" and os.name == "nt":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(channels(args.command))
    else:
        import uvicorn

        from trpc_service.web import create_app

        uvicorn.run(create_app(database_url()), host=args.host, port=args.port, log_config=None, access_log=False)


if __name__ == "__main__":
    main()
