"""HTTP adapter for a tenant-scoped external memory service."""

from __future__ import annotations

import json
from datetime import datetime
from urllib.request import Request, urlopen

from trpc_service.storage.base import MemoryItem


class ExternalMemoryStore:
    backend_name = "external"

    def __init__(self, base_url: str, token: str = "", timeout: float = 10.0) -> None:
        if not base_url:
            raise ValueError("external memory backend requires external_memory_url")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def _request(self, method: str, path: str, payload: dict | None = None):
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        request = Request(
            f"{self.base_url}/{path.lstrip('/')}",
            data=json.dumps(payload).encode("utf-8") if payload is not None else None,
            headers=headers,
            method=method,
        )
        with urlopen(request, timeout=self.timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def put(self, item: MemoryItem) -> None:
        self._request(
            "PUT",
            "/v1/memories",
            {
                "tenant_id": item.tenant_id,
                "memory_id": item.memory_id,
                "scope_key": item.scope_key,
                "content": item.content,
                "metadata": item.metadata,
                "version": item.version,
                "created_at": item.created_at.isoformat(),
            },
        )

    def search(
        self,
        tenant_id: str,
        query: str,
        limit: int = 5,
        scope_keys: tuple[str, ...] | None = None,
    ) -> list[MemoryItem]:
        payload = self._request(
            "POST",
            "/v1/memories/search",
            {
                "tenant_id": tenant_id,
                "query": query,
                "limit": limit,
                "scope_keys": list(scope_keys) if scope_keys is not None else None,
            },
        )
        items = payload if isinstance(payload, list) else payload.get("items", [])
        result = []
        for item in items:
            created_at = item.get("created_at", "")
            result.append(
                MemoryItem(
                    tenant_id=str(item["tenant_id"]),
                    memory_id=str(item["memory_id"]),
                    scope_key=str(item.get("scope_key", "")),
                    content=str(item["content"]),
                    metadata=dict(item.get("metadata", {})),
                    version=int(item.get("version", 1)),
                    created_at=datetime.fromisoformat(created_at) if created_at else datetime.now().astimezone(),
                )
            )
        return result

    def close(self) -> None:
        return None
