# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Local-only web playground for the 3-tenant SaaS demo.

This file is gitignored on purpose — it's a local testing helper, not part of
the repo deliverable. It drives the worker directly (no IM signature check),
so you can chat with all three tenants from a browser.

Run::

    python web_demo.py          # then open http://127.0.0.1:8081

Real LLM mode: set TRPC_SERVICE_MODEL_API_KEY first (falls back to mock otherwise).
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from trpc_service import CHAT_PRIVATE
from trpc_service import InboundMessage
from trpc_service import TenantConfigManager
from trpc_service import TenantWorker
from trpc_service import load_tenants
from trpc_service.web.app import create_session_service

from agent import create_agent

TENANTS_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tenants.yaml")

manager = TenantConfigManager()
for tenant in load_tenants(TENANTS_CONFIG):
    manager.register(tenant)

worker = TenantWorker(
    manager=manager,
    agent_factory=create_agent,
    session_service_factory=create_session_service,
)

TENANT_META = [{
    "tenant_id": t.tenant_id,
    "name": t.name,
    "model": t.model.model_name,
    "instruction": t.app_config.default_instruction or "",
    "tools": list(t.tool_permissions.tool_whitelist or []),
} for t in load_tenants(TENANTS_CONFIG)]

app = FastAPI(title="Multi-Tenant SaaS Demo (local)")

PAGE = """<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>多租户 SaaS 客服中台 · 本地体验</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, -apple-system, sans-serif; background: #f4f6f9; }
  header { background: #1f2d3d; color: #fff; padding: 16px 24px; }
  header h1 { font-size: 18px; }
  header p { font-size: 12px; opacity: .75; margin-top: 4px; }
  .wrap { max-width: 860px; margin: 24px auto; padding: 0 16px; }
  .tenants { display: flex; gap: 12px; margin-bottom: 20px; flex-wrap: wrap; }
  .tenant-card { flex: 1; min-width: 220px; background: #fff; border: 2px solid #e2e8f0;
                 border-radius: 10px; padding: 14px; cursor: pointer; transition: all .15s; }
  .tenant-card.active { border-color: #2563eb; box-shadow: 0 4px 12px rgba(37,99,235,.15); }
  .tenant-card h3 { font-size: 15px; }
  .tenant-card .meta { font-size: 12px; color: #64748b; margin-top: 6px; line-height: 1.6; }
  .tenant-card .tag { display: inline-block; background: #eef2ff; color: #4338ca;
                      border-radius: 4px; padding: 1px 6px; margin-right: 4px; font-size: 11px; }
  .chat { background: #fff; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,.06); overflow: hidden; }
  .chat .log { height: 420px; overflow-y: auto; padding: 20px; }
  .msg { margin-bottom: 12px; display: flex; }
  .msg.user { justify-content: flex-end; }
  .msg .bubble { max-width: 70%; padding: 10px 14px; border-radius: 12px; font-size: 14px;
                 line-height: 1.6; white-space: pre-wrap; }
  .msg.user .bubble { background: #2563eb; color: #fff; border-bottom-right-radius: 2px; }
  .msg.agent .bubble { background: #f1f5f9; color: #0f172a; border-bottom-left-radius: 2px; }
  .msg .who { font-size: 11px; color: #94a3b8; margin-bottom: 2px; }
  .msg.user .who { text-align: right; }
  .input-row { display: flex; border-top: 1px solid #e2e8f0; }
  .input-row input { flex: 1; border: 0; padding: 14px 16px; font-size: 14px; outline: none; }
  .input-row button { border: 0; background: #2563eb; color: #fff; padding: 0 24px; cursor: pointer;
                      font-size: 14px; }
  .input-row button:disabled { background: #94a3b8; cursor: wait; }
  .hint { font-size: 12px; color: #94a3b8; margin-top: 12px; text-align: center; }
</style>
</head>
<body>
<header>
  <h1>多租户 SaaS 客服中台 · 本地体验</h1>
  <p>离线 Mock 模式（无需 API key）。设置 TRPC_SERVICE_MODEL_API_KEY 后重启可切换真实 LLM。</p>
</header>
<div class="wrap">
  <div class="tenants" id="tenants"></div>
  <div class="chat">
    <div class="log" id="log">
      <div class="msg agent"><div><div class="who">系统</div>
        <div class="bubble">选择一个租户，然后发消息体验。<br>每个租户有自己的角色指令、工具白名单和独立会话。</div></div></div>
    </div>
    <div class="input-row">
      <input id="input" placeholder="输入消息，回车发送…" autocomplete="off">
      <button id="send">发送</button>
    </div>
  </div>
  <div class="hint">会话按 租户 + 用户 隔离 —— 切到另一个租户，历史不串。</div>
</div>
<script>
const TENANTS = __TENANTS__;
let active = TENANTS[0]?.tenant_id ?? null;

const tenantsEl = document.getElementById('tenants');
const logEl = document.getElementById('log');
const inputEl = document.getElementById('input');
const sendBtn = document.getElementById('send');

function renderTenants() {
  tenantsEl.innerHTML = '';
  for (const t of TENANTS) {
    const div = document.createElement('div');
    div.className = 'tenant-card' + (t.tenant_id === active ? ' active' : '');
    div.innerHTML = `<h3>${t.name}</h3>
      <div class="meta">model: ${t.model}<br>
      ${(t.tools || []).map(x => `<span class="tag">${x}</span>`).join('') || '<span class="tag">无工具</span>'}</div>`;
    div.onclick = () => { active = t.tenant_id; renderTenants(); };
    tenantsEl.appendChild(div);
  }
}

function appendMsg(who, text) {
  const m = document.createElement('div');
  m.className = 'msg ' + who;
  const label = who === 'user' ? '我' : (TENANTS.find(t => t.tenant_id === active)?.name || active);
  m.innerHTML = `<div><div class="who">${label}</div><div class="bubble"></div></div>`;
  m.querySelector('.bubble').textContent = text;
  logEl.appendChild(m);
  logEl.scrollTop = logEl.scrollHeight;
}

async function send() {
  const text = inputEl.value.trim();
  if (!text || !active) return;
  appendMsg('user', text);
  inputEl.value = '';
  sendBtn.disabled = true;
  try {
    const res = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tenant_id: active, message: text, user_id: 'web_user' }),
    });
    const data = await res.json();
    if (data.error) appendMsg('agent', `[错误] ${data.error}`);
    else appendMsg('agent', data.reply || '(空回复)');
  } catch (e) {
    appendMsg('agent', `[网络错误] ${e}`);
  } finally {
    sendBtn.disabled = false;
    inputEl.focus();
  }
}

sendBtn.onclick = send;
inputEl.onkeydown = e => { if (e.key === 'Enter') send(); };
renderTenants();
inputEl.focus();
</script>
</body>
</html>
"""


class ChatRequest(BaseModel):
    tenant_id: str
    message: str
    user_id: str = "web_user"


class ChatResponse(BaseModel):
    tenant_id: str
    reply: str


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return PAGE.replace("__TENANTS__", str(TENANT_META))


@app.post("/api/chat", response_model=ChatResponse)
async def chat(req: ChatRequest) -> ChatResponse:
    inbound = InboundMessage(
        channel="cli",
        chat_id=req.user_id,
        chat_type=CHAT_PRIVATE,
        sender_id=req.user_id,
        message_id=f"{req.tenant_id}-{abs(hash(req.message + req.user_id))}",
        text=req.message,
    )
    reply = await worker.handle(req.tenant_id, "cli", inbound)
    return ChatResponse(tenant_id=req.tenant_id, reply=reply)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8081, log_level="warning")
