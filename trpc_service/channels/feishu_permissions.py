"""Read-only app permission diagnostic; no scope changes, chat reads or sends."""

import asyncio
import json
import os
from pathlib import Path
import uuid

from trpc_service.im_setup import load_bundle
from .feishu import FeishuAdapter, API
from .runtime import quiet_transport_logging

IM_SCOPES = {"im:message", "im:message.p2p_msg:readonly", "im:message:send_as_bot", "im:message.group_at_msg:readonly"}


async def inspect(adapter):
    response = await adapter.http.get(API + "/application/v6/scopes",
                                      headers={"Authorization": "Bearer " + await adapter.token()})
    data = response.json()
    code = data.get("code")
    if response.status_code != 200 or type(code) is not int or code != 0:
        return {
            "status": "unavailable",
            "http_status": response.status_code,
            "error_code": code if type(code) is int else None
        }
    scopes = (data.get("data") or {}).get("scopes")
    if not isinstance(scopes, list):
        return {"status": "invalid_response"}
    return {
        "status":
        "queried",
        "im_scopes": [{
            "name": item["scope_name"],
            "grant_status": item.get("grant_status") if type(item.get("grant_status")) is int else None,
            "scope_type": item.get("scope_type") if item.get("scope_type") in {"tenant", "user"} else None
        } for item in scopes if item.get("scope_name") in IM_SCOPES]
    }


async def run():
    quiet_transport_logging()
    load_bundle(".secrets/feishu.json")
    adapter = FeishuAdapter(os.environ["TRPC_FEISHU_APP_ID"], os.environ["TRPC_FEISHU_APP_SECRET"])
    try:
        result = await inspect(adapter)
    except Exception as error:
        result = {"status": "unavailable", "error_type": type(error).__name__}
    finally:
        await adapter.close()
    path = Path("reports") / ("live-feishu-permissions-" + uuid.uuid4().hex + ".json")
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False), flush=True)
    print("report=" + str(path.resolve()), flush=True)


if __name__ == "__main__":
    asyncio.run(run())
