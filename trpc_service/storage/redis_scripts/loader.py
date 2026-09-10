"""SCRIPT LOAD/EVALSHA wrapper with one safe NOSCRIPT reload."""

from __future__ import annotations

from typing import Any

from redis.exceptions import NoScriptError


class RedisScriptLoader:
    def __init__(self, client: Any) -> None:
        self._client = client
        self._sha_by_name: dict[str, str] = {}

    async def execute_source(
        self,
        name: str,
        source: str,
        keys: list[str],
        args: list[object],
    ) -> object:
        sha = self._sha_by_name.get(name)
        if sha is None:
            sha = await self._client.script_load(source)
            self._sha_by_name[name] = sha
        try:
            return await self._client.evalsha(sha, len(keys), *keys, *args)
        except NoScriptError:
            sha = await self._client.script_load(source)
            self._sha_by_name[name] = sha
            return await self._client.evalsha(sha, len(keys), *keys, *args)
