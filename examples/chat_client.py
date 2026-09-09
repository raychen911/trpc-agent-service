# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Call a locally running service with one stable session."""

import asyncio

import httpx


async def main() -> None:
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8080", timeout=180) as client:
        response = await client.post(
            "/api/v1/chat",
            json={
                "tenant_id": "demo",
                "app_id": "assistant",
                "user_id": "student-001",
                "session_id": "learning-session",
                "message": "请用一句话介绍 tRPC Agent。",
                "idempotency_key": "example-message-001",
            },
        )
        response.raise_for_status()
        print(response.json()["text"])


if __name__ == "__main__":
    asyncio.run(main())
