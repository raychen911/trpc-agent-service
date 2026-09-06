import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from scripts.online_im_gate import probe_case


class _ProbeHandler(BaseHTTPRequestHandler):
    calls = 0

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length:
            self.rfile.read(length)
        if self.path == "/webhooks/demo/account":
            type(self).calls += 1
            task_id = f"task-{type(self).calls}"
            self._json(200, {"ok": True, "accepted": True, "durable": True, "task_id": task_id})
            return
        self.send_error(404)

    def do_GET(self):
        if self.path == "/admin/v1/webhook-tasks/task-1":
            self._json(200, {"status": "completed", "result": {"duplicate": False}})
            return
        if self.path == "/admin/v1/webhook-tasks/task-2":
            self._json(200, {"status": "completed", "result": {"duplicate": True}})
            return
        self.send_error(404)

    def _json(self, status, body):
        encoded = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, _format, *_args):
        pass


class OnlineImGateTests(unittest.TestCase):
    def setUp(self):
        _ProbeHandler.calls = 0
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _ProbeHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()

    def test_probe_requires_ack_completion_and_duplicate_result(self):
        report = probe_case(
            self.base_url,
            "demo",
            "account",
            {"message_id": "stable-message", "text": "probe"},
            admin_headers={"X-Admin-API-Key": "test"},
            timeout=5,
            interval=0.01,
        )
        self.assertEqual(report.get("reason"), "ok", report)
        self.assertEqual(report["status"], "pass")
        self.assertTrue(report["checks"]["replay_duplicate"])
        self.assertTrue(report["checks"]["durable"])

    def test_probe_fails_when_replay_is_not_duplicate(self):
        def request(_url, payload=None, headers=None, timeout=0):
            del payload, headers, timeout
            return {"status": 200, "body": {"ok": True, "accepted": True, "durable": False}}

        report = probe_case(
            self.base_url,
            "demo",
            "account",
            {"message_id": "stable-message"},
            require_durable=False,
            request=request,
        )
        self.assertEqual(report["status"], "fail")
        self.assertIn("duplicate", report["reason"])


if __name__ == "__main__":
    unittest.main()
