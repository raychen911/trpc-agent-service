"""Two concurrent real IM challenges; one isolated bounded model call per channel."""

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import uuid

from trpc_service.im_setup import load_bundle
from .acceptance import run_from_environment


async def run(timeout=600, im_path=".secrets/im.json", feishu_path=".secrets/feishu.json"):
    load_bundle(im_path)
    load_bundle(feishu_path)
    results = await asyncio.gather(run_from_environment("wecom", timeout), run_from_environment("feishu", timeout))
    report = {
        "scope": "concurrent_isolated_real_im_text_challenges",
        "production_ready": False,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "channels": dict(zip(("wecom", "feishu"), results)),
        "passed": all(results),
        "human_read_confirmation": False
    }
    path = Path("reports") / ("live-dual-im-" + uuid.uuid4().hex + ".json")
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("report=" + str(path.resolve()), flush=True)
    return all(results)
