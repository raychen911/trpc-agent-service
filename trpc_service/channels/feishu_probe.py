"""Bounded authentication/WS probe; no message sends, model calls or raw logs."""

from datetime import datetime, timezone
import json
import logging
import multiprocessing
import os
from pathlib import Path
import time
import uuid


class ProbeLogHandler(logging.Handler):
    """Observe SDK handshake/heartbeat evidence without forwarding signed URLs."""

    def __init__(self, emit):
        super().__init__()
        self.emit_result = emit
        self.connected = self.pong = False

    def emit(self, record):
        if record.funcName == "_connect" and "connected to " in str(record.msg) and not self.connected:
            self.connected = True
            self.emit_result({"stage": "websocket", "status": "connected"})
        elif record.funcName == "_handle_control_frame" and "receive pong" in str(record.msg) and not self.pong:
            self.pong = True
            self.emit_result({"stage": "heartbeat", "status": "pong_received"})


def authentication(app_id, app_secret, client):
    response = client.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                           json={
                               "app_id": app_id,
                               "app_secret": app_secret
                           })
    if response.status_code != 200:
        return {"stage": "authentication", "status": "failed", "http_status": response.status_code}
    data = response.json()
    code = data.get("code")
    if type(code) is not int:
        return {"stage": "authentication", "status": "invalid_response"}
    if code != 0 or not isinstance(data.get("tenant_access_token"), str) or not data["tenant_access_token"]:
        return {"stage": "authentication", "status": "rejected", "error_code": code}
    return {"stage": "authentication", "status": "succeeded"}


def child_probe(connection, config_path):
    # Isolate the SDK's blocking public start() and lack of public stop(). The
    # parent always terminates this owned process after evidence or the deadline.
    stage = "configuration"
    try:
        from trpc_service.im_setup import load_bundle
        load_bundle(config_path)
        app_id, secret = os.environ["TRPC_FEISHU_APP_ID"], os.environ["TRPC_FEISHU_APP_SECRET"]
        if not app_id or not secret:
            raise ValueError()
        import httpx
        stage = "authentication"
        with httpx.Client(timeout=15, follow_redirects=False) as client:
            result = authentication(app_id, secret, client)
        connection.send(result)
        if result["status"] != "succeeded":
            return
        stage = "websocket"
        import lark_oapi as lark
        # Library default logger includes signed WS URL and event payloads. Only
        # fixed booleans cross the process boundary; no formatter ever sees them.
        logger = logging.getLogger("Lark")
        logger.handlers.clear()
        logger.propagate = False
        logger.addHandler(ProbeLogHandler(connection.send))
        handler = lark.EventDispatcherHandler.builder("", "").build()
        client = lark.ws.Client(app_id,
                                secret,
                                event_handler=handler,
                                log_level=lark.LogLevel.DEBUG,
                                auto_reconnect=False)
        client.start()
    except BaseException as error:
        result = {"stage": stage, "status": "failed", "error_type": type(error).__name__}
        code = getattr(error, "code", None)
        if type(code) is int:
            result["error_code"] = code
        connection.send(result)
    finally:
        connection.close()


def run(config_path=".secrets/feishu.json", timeout=45):
    if not 5 <= timeout <= 60:
        raise ValueError("probe timeout must be between 5 and 60 seconds")
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(target=child_probe, args=(child, str(Path(config_path).resolve())))
    report = {
        "scope": "feishu_authentication_and_websocket_only",
        "production_ready": False,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "events": [],
        "messages_sent": 0
    }
    process.start()
    child.close()
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            if parent.poll(min(1, max(0, deadline - time.monotonic()))):
                try:
                    event = parent.recv()
                except EOFError:
                    break
                report["events"].append(event)
                print(json.dumps(event, ensure_ascii=False), flush=True)
                if event["status"] in {"failed", "rejected", "invalid_response", "pong_received"}:
                    break
            elif not process.is_alive():
                break
        report["authenticated"] = any(item["stage"] == "authentication" and item["status"] == "succeeded"
                                      for item in report["events"])
        report["websocket_connected"] = any(item["status"] == "connected" for item in report["events"])
        report["heartbeat_confirmed"] = any(item["status"] == "pong_received" for item in report["events"])
        report["timed_out"] = time.monotonic() >= deadline
    finally:
        if process.is_alive():
            process.terminate()
        process.join(5)
        parent.close()
    Path("reports").mkdir(exist_ok=True)
    path = Path("reports") / ("live-feishu-connection-" + uuid.uuid4().hex + ".json")
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print("report=" + str(path.resolve()), flush=True)
    return report


if __name__ == "__main__":
    result = run()
    raise SystemExit(0 if result["authenticated"] and result["heartbeat_confirmed"] else 1)
