"""Executable, tenant-isolated storage-profile replication.

The relational database remains the canonical source of records.  A profile is
an independently durable projection used for retrieval.  Moving a tenant
therefore means copying canonical records into the target profile, repeatedly
catching up, then comparing deterministic manifests before the runtime exposes
the target route.  No caller-supplied Boolean can mark a migration verified.
"""

from __future__ import annotations

import json
import os
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from threading import RLock
from typing import Any, Protocol


class StorageMigrationError(RuntimeError):
    """A profile is unavailable or cannot prove a complete copy."""


_PROFILE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


@dataclass(frozen=True, slots=True)
class StorageRecord:
    record_id: str
    kind: str
    version: int
    text: str
    metadata: Mapping[str, Any]

    def primitive(self) -> dict[str, Any]:
        return {
            "record_id": self.record_id,
            "kind": self.kind,
            "version": self.version,
            "text": self.text,
            "metadata": dict(self.metadata),
        }


class StorageProfileAdapter(Protocol):
    def replace_tenant(self, tenant_id: str, records: Sequence[StorageRecord]) -> str: ...

    def manifest(self, tenant_id: str) -> str: ...

    def search(self, tenant_id: str, query: str, *, limit: int = 8) -> list[dict[str, Any]]: ...


def _manifest(records: Sequence[StorageRecord | Mapping[str, Any]]) -> str:
    material: list[dict[str, Any]] = []
    for record in records:
        value = record.primitive() if isinstance(record, StorageRecord) else dict(record)
        material.append(value)
    encoded = json.dumps(
        sorted(material, key=lambda value: str(value["record_id"])),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _terms(text: str) -> Counter[str]:
    return Counter(re.findall(r"[\w-]+", text.lower()))


def _search_records(records: Sequence[Mapping[str, Any]], query: str, *, limit: int) -> list[dict[str, Any]]:
    if limit < 1:
        return []
    wanted = _terms(query)
    if not wanted:
        return []
    matches: list[tuple[float, Mapping[str, Any]]] = []
    for record in records:
        text = record.get("text")
        if not isinstance(text, str):
            continue
        score = sum(min(count, _terms(text).get(term, 0)) for term, count in wanted.items())
        if score:
            matches.append((score / max(1, sum(wanted.values())), record))
    return [
        {
            "record_id": str(record["record_id"]),
            "kind": str(record.get("kind") or "record"),
            "text": str(record["text"]),
            "score": score,
            "metadata": dict(record.get("metadata") or {}),
        }
        for score, record in sorted(matches, key=lambda item: (-item[0], str(item[1]["record_id"])))[:limit]
    ]


class FileStorageProfileAdapter:
    """A small durable profile adapter, rooted beneath an operator-owned directory.

    Target names are identifiers, not filesystem paths.  Atomic replacement
    means a failed backfill leaves the previously verified target untouched.
    """

    def __init__(self, root: str | Path, profile_name: str) -> None:
        if not _PROFILE_NAME.fullmatch(profile_name):
            raise StorageMigrationError("storage profile name is invalid")
        self._root = Path(root)
        self._profile_name = profile_name
        self._lock = RLock()

    def _path(self, tenant_id: str) -> Path:
        if not tenant_id:
            raise StorageMigrationError("tenant_id is required")
        # Hashing avoids exposing tenant names through volume paths and keeps
        # filenames portable while profile names remain operator-visible.
        tenant_key = sha256(tenant_id.encode("utf-8")).hexdigest()
        return self._root / self._profile_name / f"{tenant_key}.json"

    def _load(self, tenant_id: str) -> list[dict[str, Any]]:
        path = self._path(tenant_id)
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return []
        except json.JSONDecodeError as exc:
            raise StorageMigrationError("storage profile data is corrupt") from exc
        if not isinstance(parsed, list) or not all(isinstance(item, dict) for item in parsed):
            raise StorageMigrationError("storage profile data has an invalid shape")
        return [dict(item) for item in parsed]

    def replace_tenant(self, tenant_id: str, records: Sequence[StorageRecord]) -> str:
        target = self._path(tenant_id)
        values = [record.primitive() for record in sorted(records, key=lambda item: item.record_id)]
        encoded = json.dumps(values, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        with self._lock:
            target.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
            temporary = target.with_suffix(".tmp")
            temporary.write_text(encoded, encoding="utf-8")
            os.replace(temporary, target)
        return _manifest(values)

    def manifest(self, tenant_id: str) -> str:
        with self._lock:
            return _manifest(self._load(tenant_id))

    def search(self, tenant_id: str, query: str, *, limit: int = 8) -> list[dict[str, Any]]:
        with self._lock:
            return _search_records(self._load(tenant_id), query, limit=limit)


class MemoryStorageProfileAdapter:
    """Deterministic adapter used by the in-memory runtime tests."""

    def __init__(self) -> None:
        self._records: dict[str, list[dict[str, Any]]] = {}

    def replace_tenant(self, tenant_id: str, records: Sequence[StorageRecord]) -> str:
        values = [record.primitive() for record in sorted(records, key=lambda item: item.record_id)]
        self._records[tenant_id] = values
        return _manifest(values)

    def manifest(self, tenant_id: str) -> str:
        return _manifest(self._records.get(tenant_id, []))

    def search(self, tenant_id: str, query: str, *, limit: int = 8) -> list[dict[str, Any]]:
        return _search_records(self._records.get(tenant_id, []), query, limit=limit)


class StorageProfileRouter:
    """Resolves profile identifiers and computes copy/verification watermarks."""

    def __init__(self, *, root: str | Path, memory_mode: bool = False) -> None:
        self._root = root
        self._memory_mode = memory_mode
        self._adapters: dict[str, StorageProfileAdapter] = {}

    @staticmethod
    def profile_name(profile: Mapping[str, Any]) -> str:
        name = str(profile.get("profile") or profile.get("name") or "")
        if not _PROFILE_NAME.fullmatch(name):
            raise StorageMigrationError("storage profile requires a safe profile identifier")
        return name

    def adapter(self, profile: Mapping[str, Any]) -> StorageProfileAdapter:
        name = self.profile_name(profile)
        adapter = self._adapters.get(name)
        if adapter is not None:
            return adapter
        if self._memory_mode:
            adapter = MemoryStorageProfileAdapter()
        else:
            adapter = FileStorageProfileAdapter(self._root, name)
        self._adapters[name] = adapter
        return adapter

    @staticmethod
    def canonical_records(snapshot: Mapping[str, Any]) -> list[StorageRecord]:
        records: list[StorageRecord] = []
        for memory in snapshot.get("memories", []):
            if isinstance(memory, Mapping) and isinstance(memory.get("content"), str):
                records.append(
                    StorageRecord(
                        record_id=f"memory:{memory['memory_id']}",
                        kind="memory",
                        version=int(memory.get("version", 1)),
                        text=str(memory["content"]),
                        metadata={"memory_type": str(memory.get("memory_type") or "")},
                    )
                )
        for summary in snapshot.get("summaries", []):
            if isinstance(summary, Mapping) and isinstance(summary.get("content"), str):
                records.append(
                    StorageRecord(
                        record_id=f"summary:{summary['session_id']}:{summary['based_on_seq']}",
                        kind="session_summary",
                        version=int(summary.get("based_on_seq", 1)),
                        text=str(summary["content"]),
                        metadata={"session_id": str(summary["session_id"])},
                    )
                )
        for document in snapshot.get("knowledge", []):
            if not isinstance(document, Mapping) or not isinstance(document.get("content"), str):
                continue
            content = str(document["content"])
            if not content.strip():
                continue
            document_id = str(document.get("document_id") or "")
            knowledge_base_id = str(document.get("knowledge_base_id") or "")
            if not document_id or not knowledge_base_id:
                continue
            acl = document.get("acl")
            records.append(
                StorageRecord(
                    record_id=f"knowledge:{document_id}",
                    kind="knowledge",
                    version=int(document.get("version", 1)),
                    text=content,
                    metadata={
                        "knowledge_base_id": knowledge_base_id,
                        "acl": dict(acl) if isinstance(acl, Mapping) else {},
                    },
                )
            )
        return records

    def copy(self, tenant_id: str, profile: Mapping[str, Any], snapshot: Mapping[str, Any]) -> str:
        return self.adapter(profile).replace_tenant(tenant_id, self.canonical_records(snapshot))

    def verify(self, tenant_id: str, profile: Mapping[str, Any], snapshot: Mapping[str, Any]) -> tuple[bool, str, str]:
        expected = _manifest(self.canonical_records(snapshot))
        actual = self.adapter(profile).manifest(tenant_id)
        return actual == expected, expected, actual

    def search(self, tenant_id: str, profile: Mapping[str, Any], query: str, *, limit: int = 8) -> list[dict[str, Any]]:
        return self.adapter(profile).search(tenant_id, query, limit=limit)


__all__ = [
    "FileStorageProfileAdapter",
    "MemoryStorageProfileAdapter",
    "StorageMigrationError",
    "StorageProfileRouter",
    "StorageRecord",
]
