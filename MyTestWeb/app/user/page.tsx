'use client';

import { ArrowUp, Bot, CheckCircle2, Paperclip, Sparkles, UserRound } from 'lucide-react';
import { FormEvent, useEffect, useMemo, useState } from 'react';

type Message = { role: 'user' | 'assistant'; text: string; time: string };

const API_URL = process.env.NEXT_PUBLIC_AGENT_API_URL ?? 'http://127.0.0.1:8765';

export default function UserChatPage() {
  const [tenantId, setTenantId] = useState('acme-retail');
  const [userId, setUserId] = useState('local-user');
  const [draft, setDraft] = useState('');
  const [sending, setSending] = useState(false);
  const [online, setOnline] = useState(false);
  const [messages, setMessages] = useState<Message[]>([
    { role: 'assistant', text: '你好，我是你的智能客服。有什么可以帮你？', time: '现在' },
  ]);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    setTenantId(params.get('tenant_id') || params.get('tenant') || 'acme-retail');
    setUserId(localStorage.getItem('mytest-user-id') || 'local-user');
    fetch(`${API_URL}/healthz`).then((response) => setOnline(response.ok)).catch(() => setOnline(false));
  }, []);

  const tenantName = useMemo(() => ({
    'acme-retail': 'Acme 零售',
    'nova-finance': 'Nova 金融',
    'orbit-lab': 'Orbit 实验室',
  }[tenantId] || tenantId), [tenantId]);

  async function sendMessage(event?: FormEvent) {
    event?.preventDefault();
    const text = draft.trim();
    if (!text || sending) return;
    setDraft('');
    setMessages((current) => [...current, { role: 'user', text, time: '刚刚' }]);
    setSending(true);
    try {
      const response = await fetch(`${API_URL}/api/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tenant_id: tenantId, user_id: userId, channel: 'web', message: text, mode: 'mock' }),
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || 'Agent 暂时不可用');
      setMessages((current) => [...current, { role: 'assistant', text: data.reply, time: '刚刚' }]);
    } catch (error) {
      setMessages((current) => [...current, { role: 'assistant', text: error instanceof Error ? error.message : '请求失败，请稍后再试。', time: '刚刚' }]);
    } finally {
      setSending(false);
    }
  }

  return (
    <main className="user-shell">
      <header className="user-topbar">
        <a href="/user" className="user-brand"><span className="user-brand-mark"><Sparkles size={17} /></span><span>Agent Desk</span></a>
        <div className="user-tenant"><span className="user-tenant-dot" />{tenantName}<small>专属智能客服</small></div>
        <div className="user-status"><span className={online ? 'status-dot online' : 'status-dot'} />{online ? '服务在线' : '本地离线'}<button type="button" onClick={() => setUserId((current) => { const next = window.prompt('输入本地用户标识', current) || current; localStorage.setItem('mytest-user-id', next); return next; })}><UserRound size={16} />{userId}</button></div>
      </header>
      <section className="user-content">
        <div className="user-welcome"><p className="user-eyebrow">{tenantName} · CUSTOMER SUPPORT</p><h1>你好，今天想了解什么？</h1><p>你的消息会进入当前租户专属 Agent，会话上下文由共享 Session 后端持续保存。</p></div>
        <div className="user-chat-card">
          <div className="user-chat-head"><div><span className="user-agent-avatar"><Bot size={18} /></span><div><strong>{tenantName} Agent</strong><small>智能客服 · 随时在线</small></div></div><span className="user-secure"><CheckCircle2 size={14} />租户专属会话</span></div>
          <div className="user-messages" aria-live="polite">
            {messages.map((message, index) => <div className={`user-message-row ${message.role}`} key={`${message.time}-${index}`}><span className="user-message-avatar">{message.role === 'assistant' ? <Bot size={15} /> : <UserRound size={15} />}</span><div><span className="user-message-name">{message.role === 'assistant' ? 'Agent' : '你'} · {message.time}</span><p>{message.text}</p></div></div>)}
            {sending && <div className="user-message-row assistant"><span className="user-message-avatar"><Bot size={15} /></span><div><span className="user-message-name">Agent · 正在输入</span><p className="user-typing"><i /><i /><i /></p></div></div>}
          </div>
          <form className="user-composer" onSubmit={sendMessage}><button type="button" aria-label="添加附件" title="附件上传即将支持"><Paperclip size={18} /></button><input aria-label="输入消息" value={draft} onChange={(event) => setDraft(event.target.value)} placeholder="输入你的问题…" /><button className="user-send" type="submit" disabled={!draft.trim() || sending} aria-label="发送消息"><ArrowUp size={18} /></button></form>
          <p className="user-composer-note">按 Enter 发送 · 当前为本地验证模式 · 不会展示管理配置</p>
        </div>
        <div className="user-suggestions"><span>你可以试试</span>{['查询订单 10086', '修改收货地址', '联系客服'].map((item) => <button key={item} type="button" onClick={() => setDraft(item)}>{item}</button>)}</div>
      </section>
      <footer className="user-footer">由 tRPC-Agent 驱动 <span>·</span> 当前租户：{tenantName}</footer>
    </main>
  );
}
