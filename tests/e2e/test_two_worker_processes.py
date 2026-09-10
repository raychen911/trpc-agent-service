from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from uuid import uuid4

import httpx
import pytest

from trpc_service._cli import build_signed_request


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_ready(port: int) -> None:
    deadline = time.monotonic() + 15
    with httpx.Client(trust_env=False) as client:
        while time.monotonic() < deadline:
            try:
                if client.get(f"http://127.0.0.1:{port}/readyz", timeout=0.5).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
    raise AssertionError("shared worker did not become ready")


@pytest.mark.shared_backend
def test_two_worker_processes_continue_session_after_first_exits(
    runtime_secret_env: dict[str, str], shared_redis_url: str, shared_database_url: str
) -> None:
    del shared_redis_url, shared_database_url
    ports = [_free_port(), _free_port()]
    processes: list[subprocess.Popen[str]] = []
    env = dict(os.environ)
    env.update(runtime_secret_env)
    run_id = uuid4().hex
    try:
        for index, port in enumerate(ports):
            process = subprocess.Popen(
                [sys.executable, "-m", "trpc_service._shared_server", "--port", str(port), "--node-id", f"node-{index + 1}"],
                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True,
            )
            processes.append(process)
            _wait_ready(port)

        class Args:
            binding_id = "binding-alpha"
            secret_env = "TRPC_DEMO_ALPHA_SECRET"
            external_user_id = f"process-user-{run_id}"
            conversation_type = "direct"
            external_conversation_id = f"process-conversation-{run_id}"
            trace_id = None

        with httpx.Client(trust_env=False) as client:
            Args.url, Args.external_message_id, Args.text = f"http://127.0.0.1:{ports[0]}", f"process-1-{run_id}", "Remember validation token ALPHA."
            first = build_signed_request(Args, env)
            assert client.post(first.url, content=first.content, headers=first.headers).json()["data"]["text"] == "stored:ALPHA"
            Args.url, Args.external_message_id, Args.text = f"http://127.0.0.1:{ports[1]}", f"process-2-{run_id}", "Recall the validation token."
            second = build_signed_request(Args, env)
            assert client.post(second.url, content=second.content, headers=second.headers).json()["data"]["text"] == "recalled:ALPHA"
            processes[0].terminate()
            processes[0].wait(timeout=10)
            Args.external_message_id = f"process-3-{run_id}"
            third = build_signed_request(Args, env)
            assert client.post(third.url, content=third.content, headers=third.headers).json()["data"]["text"] == "recalled:ALPHA"
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
