import argparse
import asyncio
import os

from minio import Minio
from redis.asyncio import Redis

from trpc_service.storage import Database
from trpc_service.storage.artifacts import MinioArtifactStore
from trpc_service.storage.migration import (
    LocalToMinioArtifactMigrator,
    RedisToSqlSessionMigrator,
    SqlMemoryToVectorMigrator,
)
from trpc_service.storage.vector import QdrantVectorStore


async def run(args: argparse.Namespace) -> None:
    if args.kind == "redis-to-sql":
        database = Database(args.database_url)
        redis = Redis.from_url(args.redis_url)
        try:
            report = await RedisToSqlSessionMigrator(
                redis, database.session_factory, args.key_prefix
            ).run(overwrite=args.overwrite)
        finally:
            await redis.aclose()
            database.dispose()
    elif args.kind == "local-to-minio":
        access_key = os.environ["MINIO_ACCESS_KEY"]
        secret_key = os.environ["MINIO_SECRET_KEY"]
        destination = MinioArtifactStore(
            Minio(
                args.endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=args.secure,
            ),
            args.bucket,
        )
        report = await LocalToMinioArtifactMigrator(args.local_root, destination).run()
    else:
        database = Database(args.database_url)
        destination = QdrantVectorStore(
            args.qdrant_url,
            args.collection,
            os.getenv("QDRANT_API_KEY"),
        )
        try:
            report = await SqlMemoryToVectorMigrator(
                database.session_factory, destination, args.batch_size
            ).run(tenant_id=args.tenant_id)
        finally:
            await destination.close()
            database.dispose()
    print(report)
    if report.failed:
        raise SystemExit(2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="kind", required=True)
    redis_parser = subparsers.add_parser("redis-to-sql")
    redis_parser.add_argument("--redis-url", required=True)
    redis_parser.add_argument("--database-url", required=True)
    redis_parser.add_argument("--key-prefix", default="trpc")
    redis_parser.add_argument("--overwrite", action="store_true")
    minio_parser = subparsers.add_parser("local-to-minio")
    minio_parser.add_argument("--local-root", required=True)
    minio_parser.add_argument("--endpoint", required=True)
    minio_parser.add_argument("--bucket", default="trpc-agent-artifacts")
    minio_parser.add_argument("--secure", action="store_true")
    vector_parser = subparsers.add_parser("sql-memory-to-qdrant")
    vector_parser.add_argument("--database-url", required=True)
    vector_parser.add_argument("--qdrant-url", required=True)
    vector_parser.add_argument("--collection", default="trpc_agent_vectors")
    vector_parser.add_argument("--tenant-id")
    vector_parser.add_argument("--batch-size", type=int, default=200)
    asyncio.run(run(parser.parse_args()))
