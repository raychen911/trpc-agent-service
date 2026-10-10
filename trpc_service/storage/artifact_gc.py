"""Bounded garbage collection for abandoned staged artifact objects."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger(__name__)


class StagedObjectStore(Protocol):
    async def discard(self, staged_key: str, *, tenant_id: str | None = None) -> None: ...

    async def list_staged(
        self,
        *,
        older_than: datetime,
        limit: int,
        continuation_token: str | None = None,
    ) -> tuple[tuple[str, ...], str | None]: ...

    async def discard_unreferenced_staged(self, staged_key: str) -> None: ...


ARTIFACT_GC_RUNS = Counter(
    "trpc_artifact_gc_runs_total",
    "Artifact garbage-collection cycles by outcome.",
    ("outcome",),
)
ARTIFACT_GC_ITEMS = Counter(
    "trpc_artifact_gc_items_total",
    "Staged artifact records handled by garbage collection.",
    ("outcome",),
)
ARTIFACT_GC_DURATION = Histogram(
    "trpc_artifact_gc_seconds",
    "Duration of one artifact garbage-collection cycle.",
    buckets=(0.001, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60),
)
ARTIFACT_GC_LAST_SUCCESS = Gauge(
    "trpc_artifact_gc_last_success_timestamp_seconds",
    "Unix timestamp of the most recent successful artifact GC cycle.",
)


@dataclass(frozen=True, slots=True)
class ArtifactGcResult:
    """Machine-readable outcome of one bounded collection cycle."""

    scanned: int
    deleted: int
    failed: int
    orphan_deleted: int = 0

    @property
    def status(self) -> str:
        return "pass" if self.failed == 0 else "fail"


class ArtifactGarbageCollector:
    """Delete expired staged objects whose PostgreSQL metadata is authoritative.

    Rows are selected with ``SKIP LOCKED`` so multiple replicas can cooperate.
    The object is deleted before its row is marked deleted; an interrupted cycle
    therefore retries an idempotent S3 delete instead of losing cleanup work.
    """

    def __init__(
        self,
        pool: Any,
        objects: StagedObjectStore,
        *,
        ttl_seconds: int = 86_400,
        batch_size: int = 100,
        poll_seconds: float = 60.0,
    ) -> None:
        if ttl_seconds < 60:
            raise ValueError("artifact GC ttl_seconds must be at least 60")
        if batch_size < 1:
            raise ValueError("artifact GC batch_size must be positive")
        if poll_seconds <= 0:
            raise ValueError("artifact GC poll_seconds must be positive")
        self._pool = pool
        self._objects = objects
        self._ttl_seconds = ttl_seconds
        self._batch_size = batch_size
        self._poll_seconds = poll_seconds
        self._stop_event = asyncio.Event()
        self._continuation_token: str | None = None

    async def run_once(self) -> ArtifactGcResult:
        started = time.perf_counter()
        deleted = 0
        failed = 0
        orphan_deleted = 0
        try:
            orphan_keys: tuple[str, ...] = ()
            list_staged = getattr(self._objects, "list_staged", None)
            if callable(list_staged):
                orphan_keys, next_token = await list_staged(
                    older_than=datetime.now(UTC) - timedelta(seconds=self._ttl_seconds),
                    limit=min(self._batch_size, 1_000),
                    continuation_token=self._continuation_token,
                )
                self._continuation_token = next_token
            async with self._pool.acquire() as connection, connection.transaction():
                rows = await connection.fetch(
                    """
                    SELECT tenant_id,artifact_id,object_key
                      FROM artifacts
                     WHERE status='staged'
                       AND created_at <= clock_timestamp()
                           - ($1::double precision * interval '1 second')
                     ORDER BY created_at,tenant_id,artifact_id
                     FOR UPDATE SKIP LOCKED
                     LIMIT $2
                    """,
                    float(self._ttl_seconds),
                    self._batch_size,
                )
                metadata_keys = {str(row["object_key"]) for row in rows}
                for row in rows:
                    try:
                        await self._objects.discard(
                            str(row["object_key"]), tenant_id=str(row["tenant_id"])
                        )
                        status = await connection.execute(
                            """
                            UPDATE artifacts
                               SET status='deleted'
                             WHERE tenant_id=$1 AND artifact_id=$2
                               AND object_key=$3 AND status='staged'
                            """,
                            row["tenant_id"],
                            row["artifact_id"],
                            row["object_key"],
                        )
                        if status != "UPDATE 1":
                            raise RuntimeError("artifact GC metadata CAS was lost")
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        failed += 1
                        logger.warning(
                            "artifact garbage collection item failed",
                            extra={"error_type": type(error).__name__},
                        )
                    else:
                        deleted += 1
                discard_orphan = getattr(self._objects, "discard_unreferenced_staged", None)
                if callable(discard_orphan):
                    for key in orphan_keys:
                        if key in metadata_keys:
                            continue
                        referenced = await connection.fetchval(
                            """
                            SELECT EXISTS (
                                SELECT 1 FROM artifacts
                                 WHERE object_key=$1 AND status='staged'
                            )
                            """,
                            key,
                        )
                        if referenced:
                            continue
                        try:
                            await discard_orphan(key)
                        except asyncio.CancelledError:
                            raise
                        except Exception as error:
                            failed += 1
                            logger.warning(
                                "artifact orphan garbage collection failed",
                                extra={"error_type": type(error).__name__},
                            )
                        else:
                            deleted += 1
                            orphan_deleted += 1
            outcome = "success" if failed == 0 else "partial"
            ARTIFACT_GC_RUNS.labels(outcome=outcome).inc()
            if deleted:
                ARTIFACT_GC_ITEMS.labels(outcome="deleted").inc(deleted)
            if failed:
                ARTIFACT_GC_ITEMS.labels(outcome="failed").inc(failed)
            if failed == 0:
                ARTIFACT_GC_LAST_SUCCESS.set(time.time())
            return ArtifactGcResult(
                scanned=len(rows) + len(orphan_keys),
                deleted=deleted,
                failed=failed,
                orphan_deleted=orphan_deleted,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            ARTIFACT_GC_RUNS.labels(outcome="error").inc()
            raise
        finally:
            ARTIFACT_GC_DURATION.observe(time.perf_counter() - started)

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        external_stop = stop_event or asyncio.Event()
        while not self._stop_event.is_set() and not external_stop.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning(
                    "artifact garbage collection cycle failed",
                    extra={"error_type": type(error).__name__},
                )
            try:
                await asyncio.wait_for(external_stop.wait(), timeout=self._poll_seconds)
            except TimeoutError:
                continue

    def stop(self) -> None:
        self._stop_event.set()


__all__ = ["ArtifactGarbageCollector", "ArtifactGcResult"]
