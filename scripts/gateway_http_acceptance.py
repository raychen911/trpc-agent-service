"""Verify the real Web UI HTTP -> Redis -> worker -> PostgreSQL path."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import Request, urlopen
from uuid import uuid4

import psycopg


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--dsn", default=os.getenv("POSTGRES_ACCEPTANCE_DSN", ""))
    parser.add_argument("--timeout", type=float, default=45.0)
    parser.add_argument("--output", default="runs/real/gateway-http-acceptance.json")
    args = parser.parse_args()
    if not args.dsn.strip():
        raise SystemExit("--dsn or POSTGRES_ACCEPTANCE_DSN is required")

    external_id = f"gateway-http-{uuid4().hex}"
    payload = json.dumps(
        {
            "channel": "web",
            "account_id": "web_demo",
            "user_id": "gateway-acceptance",
            "message_id": external_id,
            "text": "gateway HTTP acceptance",
        }
    ).encode()
    request = Request(
        f"{args.base_url.rstrip('/')}/ui/api/chat",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    started = time.monotonic()
    with urlopen(request, timeout=30) as response:
        http_status = response.status
        http_body = json.loads(response.read().decode())

    inbox: tuple[object, ...] | None = None
    mailbox: tuple[object, ...] | None = None
    with psycopg.connect(args.dsn) as connection:
        while time.monotonic() - started < args.timeout:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT tenant_id, session_id, status, attempts, result_json "
                    "FROM inbox_message WHERE payload_json->>'external_message_id'=%s "
                    "ORDER BY updated_at DESC LIMIT 1",
                    (external_id,),
                )
                inbox = cursor.fetchone()
                if inbox:
                    cursor.execute(
                        "SELECT status, resolved_sequence FROM session_mailbox "
                        "WHERE tenant_id=%s AND session_id=%s",
                        (inbox[0], inbox[1]),
                    )
                    mailbox = cursor.fetchone()
                    if inbox[2] == "completed" and mailbox and mailbox[0] == "idle":
                        break
            time.sleep(0.5)

    result_json = inbox[4] if inbox else None
    result = {
        "generated_at": datetime.now(UTC).isoformat(),
        "base_url": args.base_url,
        "external_message_id": external_id,
        "http_status": http_status,
        "http_accepted": bool(http_body.get("accepted")),
        "http_durable": bool(http_body.get("durable")),
        "http_queued": bool(http_body.get("queued")),
        "session_id": http_body.get("session_id"),
        "response_ref": (result_json or {}).get("response_ref") if isinstance(result_json, dict) else None,
        "inbox_status": inbox[2] if inbox else None,
        "inbox_attempts": inbox[3] if inbox else None,
        "mailbox_status": mailbox[0] if mailbox else None,
        "mailbox_resolved_sequence": mailbox[1] if mailbox else None,
        "completed": bool(
            http_status == 200
            and http_body.get("accepted")
            and http_body.get("durable")
            and inbox
            and inbox[2] == "completed"
            and mailbox
            and mailbox[0] == "idle"
            and isinstance(result_json, dict)
            and result_json.get("response_ref")
        ),
    }
    output = Path(args.output)
    if not output.is_absolute():
        output = Path(__file__).resolve().parents[1] / output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["completed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
