# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Run the 3-tenant SaaS demo gateway.

    uvicorn run_gateway:app --host 0.0.0.0 --port 8080

Webhook endpoint: POST /webhook/{tenant_id}/{channel}
"""

from app import app

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
