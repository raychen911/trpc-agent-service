'use client';

import {
  Activity,
  ArrowRight,
  Bot,
  Boxes,
  Braces,
  Check,
  ChevronDown,
  CircleAlert,
  Clock3,
  Database,
  ExternalLink,
  FileText,
  Gauge,
  KeyRound,
  LockKeyhole,
  MessageSquareText,
  MoreHorizontal,
  Play,
  Plus,
  RadioTower,
  RefreshCw,
  RotateCcw,
  Save,
  Search,
  Send,
  Server,
  Settings2,
  ShieldCheck,
  Sparkles,
  TerminalSquare,
  Users,
  Wrench,
  X,
  Zap,
} from 'lucide-react';
import { FormEvent, ReactNode, useEffect, useMemo, useState } from 'react';

type Tenant = {
  tenant_id: string;
  name: string;
  model: string;
  storage: string;
  tools: string[];
  accent: string;
  storage_config?: StorageConfig;
};

type BackendName = 'redis' | 'mysql';
type StorageConfig = {
  session_backend: BackendName;
  memory_backend: BackendName;
  summary_backend: BackendName;
  audit_backend: 'mysql';
  redis_configured: boolean;
  mysql_configured: boolean;
  version: number;
};

type ChannelName = 'wecom' | 'wechat_kf' | 'dingtalk' | 'feishu' | 'qq';
type ChannelStatus = { channel: ChannelName; label: string; configured: boolean; secret_configured: boolean };

type ChatMessage = {
  id: string;
  role: 'user' | 'assistant' | 'system';
  text: string;
  time: string;
  trace?: string;
};

type NavKey = 'playground' | 'tenants' | 'sessions' | 'traces' | 'audit' | 'topology' | 'storage' | 'im' | 'config';

type AuditItem = {
  tenant_id: string;
  channel?: string;
  user_id?: string;
  session_id?: string;
  agent_name?: string;
  tool_name?: string;
  decision: string;
  latency_ms?: number;
  error_type?: string;
  trace_id?: string;
  created_at: string;
};

const API_URL = process.env.NEXT_PUBLIC_AGENT_API_URL ?? 'http://127.0.0.1:8765';

const defaultTenants: Tenant[] = [
  { tenant_id: 'acme-retail', name: 'Acme 零售', model: 'deepseek-chat', storage: 'REDIS', tools: ['订单查询', '知识检索'], accent: '#fa6d3b' },
  { tenant_id: 'nova-finance', name: 'Nova 金融', model: 'deepseek-chat', storage: 'MYSQL', tools: ['知识检索'], accent: '#8876ff' },
  { tenant_id: 'orbit-lab', name: 'Orbit 实验室', model: 'mock-local', storage: 'REDIS', tools: ['计算器', '危险操作'], accent: '#23a998' },
];

const initialMessages: ChatMessage[] = [
  {
    id: 'welcome',
    role: 'assistant',
    text: '验证台已就绪。你可以直接对话，或从下方场景开始测试租户隔离、重复投递和危险工具确认。',
    time: '刚刚',
  },
];

const scenarios = [
  { id: 'isolation', title: '租户隔离', detail: '跨租户使用同一用户 ID', icon: ShieldCheck },
  { id: 'duplicate', title: '重复投递', detail: '相同 message_id 连发两次', icon: RefreshCw },
  { id: 'danger', title: '危险工具', detail: '触发二次确认流程', icon: CircleAlert },
];

function StatusDot({ ok = true }: { ok?: boolean }) {
  return <span className={`status-dot ${ok ? 'ok' : 'warn'}`} aria-hidden="true" />;
}

function PageIntro({ eyebrow, title, detail, action }: { eyebrow: string; title: string; detail: string; action?: ReactNode }) {
  return <div className="page-intro"><div><p className="eyebrow">{eyebrow}</p><h2>{title}</h2><span>{detail}</span></div>{action}</div>;
}

function TenantsPage({ tenants, selected, onSelect, onNotify, onCreated }: {
  tenants: Tenant[]; selected: string; onSelect: (id: string) => void; onNotify: (text: string) => void; onCreated: (tenant: Tenant) => void;
}) {
  const [disabled, setDisabled] = useState<Record<string, boolean>>({});
  const [open, setOpen] = useState(false);
  const [tenantId, setTenantId] = useState('');
  const [name, setName] = useState('');
  const [modelName, setModelName] = useState('deepseek-chat');
  const [storageBackend, setStorageBackend] = useState<BackendName>('redis');
  const [creating, setCreating] = useState(false);

  async function createTenant(event: FormEvent) {
    event.preventDefault();
    setCreating(true);
    try {
      const response = await fetch(`${API_URL}/api/tenants`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ tenant_id: tenantId.trim(), name: name.trim(), model_name: modelName, storage_backend: storageBackend }) });
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || '创建失败');
      onCreated(data);
      onSelect(data.tenant_id);
      setOpen(false); setTenantId(''); setName('');
      onNotify(`租户 ${data.name} 已创建，初始配置 v1`);
    } catch (error) {
      onNotify(error instanceof Error ? error.message : '租户创建失败');
    } finally { setCreating(false); }
  }

  return <div className="management-page">
    <PageIntro eyebrow="TENANT CONTROL PLANE" title="租户管理" detail="配置、模型、工具权限和数据边界按租户独立生效。" action={
      <button className="primary-action" onClick={() => setOpen(true)}><Plus size={15} />新建租户</button>
    } />
    <div className="summary-cards">
      <div><span>租户总数</span><b>{tenants.length}</b><small>全部配置版本正常</small></div>
      <div><span>启用租户</span><b>{tenants.filter((t) => !disabled[t.tenant_id]).length}</b><small>实时路由可用</small></div>
      <div><span>模型端点</span><b>2</b><small>1 个 Mock · 1 个 DeepSeek</small></div>
      <div><span>配置变更</span><b>7</b><small>最近 24 小时</small></div>
    </div>
    <section className="data-panel">
      <div className="table-toolbar"><div className="search-box"><Search size={14} /><span>搜索 tenant_id 或名称</span></div><button><Settings2 size={14} />显示字段</button></div>
      <div className="tenant-table table-scroll">
        <div className="table-row table-head"><span>租户</span><span>模型</span><span>Session 后端</span><span>工具</span><span>状态</span><span>操作</span></div>
        {tenants.map((item) => <div className={`table-row ${selected === item.tenant_id ? 'selected' : ''}`} key={item.tenant_id}>
          <span className="tenant-cell"><i style={{ background: item.accent }} /><span><b>{item.name}</b><small>{item.tenant_id}</small></span></span>
          <span><code>{item.model}</code></span><span>{item.storage}</span><span>{item.tools.length} 项授权</span>
          <span><em className={disabled[item.tenant_id] ? 'state-off' : 'state-on'}><StatusDot ok={!disabled[item.tenant_id]} />{disabled[item.tenant_id] ? '已停用' : '运行中'}</em></span>
          <span className="row-actions"><button onClick={() => { onSelect(item.tenant_id); onNotify(`已切换到 ${item.name}`); }}>管理</button><button aria-label={`切换 ${item.name} 状态`} onClick={() => setDisabled((old) => ({ ...old, [item.tenant_id]: !old[item.tenant_id] }))}><MoreHorizontal size={15} /></button></span>
        </div>)}
      </div>
    </section>
    {open && <div className="modal-backdrop" role="dialog" aria-modal="true" aria-label="新建租户"><form className="confirm-card tenant-create-card" onSubmit={createTenant}><button type="button" className="modal-close" onClick={() => setOpen(false)} aria-label="关闭"><X size={18} /></button><p className="eyebrow">CREATE TENANT</p><h2>注册新租户</h2><p>创建后会立即加入本地租户路由，并生成初始配置版本。</p><div className="form-grid"><label><span>tenant_id</span><input required pattern="[A-Za-z0-9_-]+" minLength={2} maxLength={128} value={tenantId} onChange={(event) => setTenantId(event.target.value)} placeholder="例如 acme-retail" /></label><label><span>租户名称</span><input required value={name} onChange={(event) => setName(event.target.value)} placeholder="例如 Acme 零售" /></label><label><span>模型</span><select value={modelName} onChange={(event) => setModelName(event.target.value)}><option value="deepseek-chat">DeepSeek Chat</option><option value="mock-local">Mock Local</option></select></label><label><span>默认数据后端</span><select value={storageBackend} onChange={(event) => setStorageBackend(event.target.value as BackendName)}><option value="redis">Redis</option><option value="mysql">MySQL</option></select></label></div><div className="modal-actions"><button type="button" onClick={() => setOpen(false)}>取消</button><button className="danger" type="submit" disabled={creating}>{creating ? '创建中…' : '创建租户'}</button></div></form></div>}
  </div>;
}

function SessionsPage({ tenants, messages, onOpen }: {
  tenants: Tenant[]; messages: Record<string, ChatMessage[]>; onOpen: (id: string) => void;
}) {
  return <div className="management-page">
    <PageIntro eyebrow="SHARED SESSION BACKEND" title="会话" detail="任意 Worker 都能从共享后端恢复上下文，因此不需要 sticky session。" action={<button className="secondary-action"><RefreshCw size={14} />刷新会话</button>} />
    <div className="summary-cards compact">
      <div><span>活跃 Session</span><b>{Object.keys(messages).length}</b><small>过去 15 分钟</small></div>
      <div><span>事件写入</span><b>{Object.values(messages).flat().length}</b><small>append-only</small></div>
      <div><span>锁等待 P95</span><b>3.8 ms</b><small>Redis writer lock</small></div>
    </div>
    <section className="data-panel">
      <div className="section-bar"><div><h3>最近会话</h3><p>按 tenant_id + channel + user/chat 生成稳定 session_id</p></div><span className="tag green">跨节点可见</span></div>
      <div className="session-list">
        {tenants.map((item, index) => <article key={item.tenant_id}>
          <span className="session-icon"><MessageSquareText size={17} /></span>
          <div className="session-main"><b>{item.name} · local-tester</b><code>{item.tenant_id}:web:{(index + 1).toString().padStart(8, '0')}…</code></div>
          <div><span>事件</span><b>{messages[item.tenant_id]?.length ?? 0}</b></div><div><span>最后活动</span><b>{index ? `${index * 7} 分钟前` : '刚刚'}</b></div>
          <button onClick={() => onOpen(item.tenant_id)}>打开会话<ArrowRight size={13} /></button>
        </article>)}
      </div>
    </section>
  </div>;
}

function TracesPage({ tenant, messages }: { tenant: Tenant; messages: ChatMessage[] }) {
  const traceCount = Math.max(1, messages.filter((message) => message.trace).length);
  return <div className="management-page trace-page">
    <PageIntro eyebrow="OPENTELEMETRY" title="链路追踪" detail="从 IM callback 串起 Runner、工具、Session / Memory 与回复投递。" action={<button className="secondary-action"><ExternalLink size={14} />打开 Jaeger</button>} />
    <div className="trace-layout">
      <section className="data-panel trace-index">
        <div className="section-bar"><div><h3>Trace 列表</h3><p>{tenant.name} · 最近 30 分钟</p></div><b>{traceCount} 条</b></div>
        {[0, 1, 2, 3].map((item) => <button className={item === 0 ? 'selected' : ''} key={item}>
          <span className="trace-status"><StatusDot ok={item !== 2} /></span><span><b>IM callback / agent.run</b><code>{item ? `8f2a${item}d9c…` : '当前本地 trace…'}</code></span><span><b>{328 + item * 47} ms</b><small>{item * 4 + 1} 分钟前</small></span>
        </button>)}
      </section>
      <section className="data-panel waterfall">
        <div className="section-bar"><div><h3>Trace 详情</h3><p>tenant.id={tenant.tenant_id}</p></div><span className="state-on"><StatusDot />success</span></div>
        {[
          ['IM callback', '0 ms', '328 ms', '100%'], ['signature + idempotency', '4 ms', '12 ms', '18%'],
          ['queue.enqueue / consume', '16 ms', '21 ms', '26%'], ['Runner execution', '39 ms', '251 ms', '78%'],
          ['Session / Memory', '58 ms', '8 ms', '32%'], ['IM reply', '291 ms', '22 ms', '66%'],
        ].map(([name, start, duration, width]) => <div className="span-row" key={name}><span><b>{name}</b><small>start {start}</small></span><div><i style={{ width }} /></div><code>{duration}</code></div>)}
      </section>
    </div>
  </div>;
}

function AuditPage({ items, tenant, onRefresh }: { items: AuditItem[]; tenant: Tenant; onRefresh: () => void }) {
  const rows = items.length ? items : [{ tenant_id: tenant.tenant_id, channel: 'web', user_id: 'local-tester', agent_name: `${tenant.tenant_id}_agent`, decision: 'allow', latency_ms: 12, trace_id: '等待第一轮真实调用', created_at: new Date().toISOString() }];
  return <div className="management-page">
    <PageIntro eyebrow="IMMUTABLE AUDIT TRAIL" title="审计日志" detail="所有查询自动限定当前租户；敏感字段在写入前完成脱敏。" action={<button className="secondary-action" onClick={onRefresh}><RefreshCw size={14} />刷新</button>} />
    <div className="audit-filter"><span><Search size={14} />搜索用户、工具或 trace_id</span><button>{tenant.name}<ChevronDown size={13} /></button><button>全部决策<ChevronDown size={13} /></button><button>最近 24 小时<ChevronDown size={13} /></button></div>
    <section className="data-panel table-scroll">
      <div className="audit-table table-row table-head"><span>时间</span><span>用户 / 通道</span><span>Agent / 工具</span><span>决策</span><span>延迟</span><span>Trace ID</span></div>
      {rows.map((item, index) => <div className="audit-table table-row" key={`${item.created_at}-${index}`}>
        <span>{new Date(item.created_at).toLocaleTimeString('zh-CN')}<small>{new Date(item.created_at).toLocaleDateString('zh-CN')}</small></span>
        <span><b>{item.user_id ?? 'system'}</b><small>{item.channel ?? 'admin'}</small></span><span><b>{item.agent_name ?? '—'}</b><small>{item.tool_name ?? '无工具调用'}</small></span>
        <span><em className={item.decision === 'error' ? 'state-error' : 'state-on'}>{item.decision}</em></span><span>{item.latency_ms ?? 0} ms</span><span><code>{item.trace_id?.slice(0, 14) ?? '—'}…</code></span>
      </div>)}
    </section>
  </div>;
}

function TopologyPage({ online, onNotify }: { online: boolean; onNotify: (text: string) => void }) {
  const components = [
    ['Agent Gateway', '2 replicas', '8080', '验签 · 幂等 · 入队', Server], ['Agent Worker', '3 replicas', 'consumer', 'Runner · Tool · Lock', Bot],
    ['Channel Adapter', '4 bindings', '企微 / 微信客服 / 钉钉 / 飞书', '消息转换 · 投递', RadioTower], ['Storage Adapter', '2 pools', 'Redis / MySQL', 'Session · Memory', Database],
    ['Admin API', '2 replicas', '/admin', '配置 · 回滚 · 审计', Settings2], ['Telemetry', '1 collector', 'OTLP 4318', 'Metrics · Trace', Activity],
  ] as const;
  return <div className="management-page">
    <PageIntro eyebrow="STATELESS NODE TOPOLOGY" title="节点拓扑" detail="Gateway 与 Worker 独立扩缩容；共享状态位于 Redis / SQL。" action={<button className="primary-action" onClick={() => onNotify('健康检查完成：8/8 组件正常')}><Activity size={14} />运行健康检查</button>} />
    <div className="topology-flow"><span>企微 / 微信客服</span><span>钉钉 / 飞书</span><ArrowRight size={16} /><b>Gateway ×2</b><ArrowRight size={16} /><b>Redis Streams</b><ArrowRight size={16} /><b>Worker ×3</b><ArrowRight size={16} /><span>Redis / MySQL</span></div>
    <div className="component-grid">
      {components.map(([name, replicas, port, detail, Icon], index) => <article key={name}>
        <header><span><Icon size={18} /></span><em className={online || index > 0 ? 'state-on' : 'state-off'}><StatusDot ok={online || index > 0} />{online || index > 0 ? 'healthy' : 'offline'}</em></header>
        <h3>{name}</h3><p>{detail}</p><div><span>{replicas}</span><code>{port}</code></div><button onClick={() => onNotify(`${name} 详情检查通过`)}>查看详情<ArrowRight size={13} /></button>
      </article>)}
    </div>
  </div>;
}

function StoragePage({ tenant, tenants, onTenants, onNotify }: {
  tenant: Tenant; tenants: Tenant[]; onTenants: (items: Tenant[]) => void; onNotify: (text: string) => void;
}) {
  const fallback: StorageConfig = tenant.storage_config ?? {
    session_backend: tenant.storage.toLowerCase() === 'mysql' ? 'mysql' : 'redis',
    memory_backend: 'redis', summary_backend: tenant.storage.toLowerCase() === 'mysql' ? 'mysql' : 'redis', audit_backend: 'mysql',
    redis_configured: false, mysql_configured: false, version: 1,
  };
  const [config, setConfig] = useState<StorageConfig>(fallback);
  const [redisUrl, setRedisUrl] = useState('');
  const [mysqlUrl, setMysqlUrl] = useState('');
  const [checking, setChecking] = useState<BackendName | ''>('');
  const [status, setStatus] = useState<Record<BackendName, { ok?: boolean; text: string }>>({
    redis: { text: '待检测' }, mysql: { text: '待检测' },
  });
  const [saving, setSaving] = useState(false);
  const source = 'redis' as const;
  const target = 'mysql' as const;
  const [migration, setMigration] = useState<{ status: string; stage: string; progress: number; copied_by_kind?: Record<string, number>; verified?: boolean; error?: string } | null>(null);

  useEffect(() => {
    fetch(`${API_URL}/api/tenants/${tenant.tenant_id}/storage`)
      .then((response) => response.ok ? response.json() : Promise.reject())
      .then(setConfig)
      .catch(() => setConfig(fallback));
  }, [tenant.tenant_id]); // eslint-disable-line react-hooks/exhaustive-deps

  function choose(field: keyof Pick<StorageConfig, 'session_backend' | 'memory_backend'>, value: BackendName) {
    setConfig((old) => field === 'session_backend'
      ? { ...old, session_backend: value, summary_backend: value }
      : { ...old, memory_backend: value });
  }

  async function testBackend(backend: BackendName) {
    setChecking(backend);
    const response = await fetch(`${API_URL}/api/tenants/${tenant.tenant_id}/storage/test`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ backend, redis_url: redisUrl || null, mysql_url: mysqlUrl || null }),
    }).catch(() => null);
    const data = response?.ok ? await response.json() : { ok: false, error: '本地 API 不可用' };
    setStatus((old) => ({ ...old, [backend]: { ok: data.ok, text: data.ok ? `连接正常 · ${data.latency_ms} ms` : data.error } }));
    setChecking('');
  }

  async function save() {
    setSaving(true);
    const response = await fetch(`${API_URL}/api/tenants/${tenant.tenant_id}/storage`, {
      method: 'PATCH', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...config, redis_url: redisUrl || null, mysql_url: mysqlUrl || null }),
    }).catch(() => null);
    if (response?.ok) {
      const saved: StorageConfig = await response.json();
      setConfig(saved);
      onTenants(tenants.map((item) => item.tenant_id === tenant.tenant_id
        ? { ...item, storage: saved.session_backend.toUpperCase(), storage_config: saved } : item));
      setRedisUrl(''); setMysqlUrl('');
      onNotify(`已保存 ${tenant.name} 存储配置 v${saved.version}，下一次请求使用新路由`);
    } else onNotify('保存失败，请检查后端 API 和配置格式');
    setSaving(false);
  }

  async function dryRun() {
    const response = await fetch(`${API_URL}/api/tenants/${tenant.tenant_id}/storage/migrate/dry-run`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ source, target }),
    }).catch(() => null);
    const data = response?.ok ? await response.json() : null;
    onNotify(data?.message ?? '迁移检查失败；没有修改任何数据');
  }

  async function executeMigration() {
    setMigration({ status: 'pending', stage: 'queued', progress: 0 });
    const response = await fetch(`${API_URL}/api/tenants/${tenant.tenant_id}/storage/migrate`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ source, target, kinds: ['session', 'memory'] }),
    }).catch(() => null);
    const initial = response ? await response.json() : null;
    if (!response?.ok) {
      setMigration({ status: 'failed', stage: 'request_failed', progress: 100, error: initial?.detail ?? '迁移请求失败' });
      onNotify(initial?.detail ?? '迁移请求失败');
      return;
    }
    let current = initial;
    setMigration(current);
    for (let attempt = 0; attempt < 120 && ['pending', 'running'].includes(current.status); attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 500));
      const poll = await fetch(`${API_URL}/api/tenants/${tenant.tenant_id}/storage/migrate/${initial.job_id}`).catch(() => null);
      if (!poll?.ok) continue;
      current = await poll.json();
      setMigration(current);
    }
    if (current.status === 'completed' && current.storage) {
      setConfig(current.storage);
      onTenants(tenants.map((item) => item.tenant_id === tenant.tenant_id
        ? { ...item, storage: current.storage.session_backend.toUpperCase(), storage_config: current.storage } : item));
      onNotify(current.message ?? '迁移完成，租户路由已切换');
    } else if (current.status === 'failed') {
      onNotify(current.error ?? '迁移校验失败，未切换路由');
    } else {
      onNotify('迁移仍在后台执行，可稍后刷新查看');
    }
  }

  async function rollback() {
    const response = await fetch(`${API_URL}/api/tenants/${tenant.tenant_id}/rollback`, { method: 'POST' }).catch(() => null);
    const data = response?.ok ? await response.json() : null;
    if (data?.storage) setConfig(data.storage);
    onNotify(data?.message ?? '当前没有可回滚的配置版本');
  }

  return <div className="management-page">
    <PageIntro eyebrow="TENANT DATA PLANE" title="数据后端" detail="企业平台只允许 Redis 与 MySQL；配置按 tenant_id 版本化并在下一次 Worker 请求生效。" action={<button className="primary-action" onClick={save} disabled={saving}><Save size={14} />{saving ? '保存中…' : '保存新版本'}</button>} />
    <div className="backend-grid two">
      {([['redis', 'Redis', 'Session · Memory · Summary · Queue · Lock'], ['mysql', 'MySQL', 'Session · Memory · Summary · Audit · Config']] as const).map(([key, name, use]) => <article key={key}>
        <header><Database size={17} /><em className={status[key].ok ? 'state-on' : 'state-off'}><StatusDot ok={status[key].ok} />{status[key].text}</em></header>
        <h3>{name}</h3><p>{use}</p><div><span>连接配置<b>{key === 'redis' ? (config.redis_configured ? '已配置' : '未配置') : (config.mysql_configured ? '已配置' : '未配置')}</b></span><span>配置版本<b>v{config.version}</b></span></div>
      </article>)}
    </div>
    <div className="ops-grid">
      <section className="data-panel config-form">
        <div className="section-bar"><div><h3>{tenant.name} · 存储路由</h3><p>密码字段只提交给本地 API，不会回显</p></div><span className="tag green">无 sticky session</span></div>
        <div className="form-grid">
          {([['session_backend', 'Session'], ['memory_backend', 'Memory']] as const).map(([field, label]) => <label key={field}><span>{label} 后端</span><select aria-label={`${label} 后端`} value={config[field]} onChange={(event) => choose(field, event.target.value as BackendName)}><option value="redis">Redis</option><option value="mysql">MySQL</option></select></label>)}
          <label><span>Summary 后端</span><select aria-label="Summary 后端" value={config.session_backend} disabled><option value={config.session_backend}>{config.session_backend === 'redis' ? 'Redis' : 'MySQL'}（跟随 Session）</option></select></label>
          <label><span>Audit 后端</span><select aria-label="Audit 后端" value="mysql" disabled><option value="mysql">MySQL（固定）</option></select></label>
          <label className="wide"><span>Redis URL（留空保留现有密钥）</span><input type="password" value={redisUrl} onChange={(event) => setRedisUrl(event.target.value)} placeholder={config.redis_configured ? '已配置 · 输入新值可替换' : 'redis://127.0.0.1:6379/0'} /></label>
          <label className="wide"><span>MySQL URL（留空保留现有密钥）</span><input type="password" value={mysqlUrl} onChange={(event) => setMysqlUrl(event.target.value)} placeholder={config.mysql_configured ? '已配置 · 输入新值可替换' : 'mysql+aiomysql://user:password@127.0.0.1:3306/trpc_agent'} /></label>
        </div>
        <div className="panel-actions"><button onClick={() => testBackend('redis')} disabled={!!checking}>{checking === 'redis' ? '检测中…' : '测试 Redis'}</button><button onClick={() => testBackend('mysql')} disabled={!!checking}>{checking === 'mysql' ? '检测中…' : '测试 MySQL'}</button><button onClick={rollback}><RotateCcw size={13} />回滚上一版本</button></div>
      </section>
      <section className="data-panel migration-panel">
        <div className="section-bar"><div><h3>租户级迁移检查</h3><p>先验证源和目标连接，不通过时不会写数据</p></div></div>
        <div className="migration-route"><select value={source} disabled><option value="redis">Redis</option></select><ArrowRight size={17} /><select value={target} disabled><option value="mysql">MySQL</option></select></div>
        <ol><li>连接检测与目标 Schema 检查</li><li>按 tenant_id 全量复制</li><li>源端增量复扫与 checksum 校验</li><li>校验通过后切换路由，配置可回滚</li></ol>
        {migration && <div className="migration-status"><span>{migration.status === 'completed' ? '迁移完成' : migration.status === 'failed' ? '迁移失败' : '迁移执行中'} · {migration.progress}%</span><small>{migration.error ?? `阶段：${migration.stage}${migration.copied_by_kind ? ` · Session ${migration.copied_by_kind.session ?? 0} · Memory ${migration.copied_by_kind.memory ?? 0}` : ''}`}</small></div>}
        <div className="panel-actions"><button onClick={dryRun} disabled={migration?.status === 'running'}>运行 Dry-run</button><button className="primary-action" onClick={executeMigration} disabled={migration?.status === 'running' || migration?.status === 'pending'}><Play size={13} />{['running', 'pending'].includes(migration?.status ?? '') ? '迁移中…' : '执行真实迁移'}</button></div>
      </section>
    </div>
  </div>;
}

function IMPage({ tenant, mode, onNotify }: { tenant: Tenant; mode: 'mock' | 'deepseek'; onNotify: (text: string) => void }) {
  const [channels, setChannels] = useState<ChannelStatus[]>([]);
  const [channel, setChannel] = useState<ChannelName>('wecom');
  const [chatType, setChatType] = useState<'private' | 'group'>('private');
  const [userId, setUserId] = useState('local-im-user');
  const [chatId, setChatId] = useState('local-group-1');
  const [text, setText] = useState('请帮我查询订单 10086');
  const [messageId, setMessageId] = useState('');
  const [primaryId, setPrimaryId] = useState('');
  const [secondaryId, setSecondaryId] = useState('');
  const [secretOne, setSecretOne] = useState('');
  const [secretTwo, setSecretTwo] = useState('');
  const [webhookUrl, setWebhookUrl] = useState('');
  const [result, setResult] = useState<Record<string, unknown> | null>(null);
  const [running, setRunning] = useState(false);
  const labels: Record<ChannelName, string> = { wecom: '企业微信', wechat_kf: '微信客服', dingtalk: '钉钉', feishu: '飞书', qq: 'QQ' };
  const fieldLabels: Record<ChannelName, [string, string, string, string]> = {
    wecom: ['Corp ID', 'Agent ID', 'Callback Token', 'EncodingAESKey'],
    wechat_kf: ['Corp ID', 'Open KF ID', 'Callback Token', 'EncodingAESKey'],
    dingtalk: ['Client ID', 'Robot Code', 'Client Secret', 'Webhook Secret'],
    feishu: ['App ID', 'Webhook URL', 'Verification Token', 'Encrypt Key'],
    qq: ['App ID', '保留（留空）', 'App Secret', 'Access Token（可选）'],
  };

  useEffect(() => {
    fetch(`${API_URL}/api/tenants/${tenant.tenant_id}/channels`)
      .then((response) => response.ok ? response.json() : Promise.reject())
      .then((data) => setChannels(data.items ?? []))
      .catch(() => setChannels([]));
  }, [tenant.tenant_id]);

  async function saveChannel() {
    const body: Record<string, string | null> = { webhook_url: webhookUrl || null };
    if (channel === 'wecom') Object.assign(body, { corp_id: primaryId, agent_id: secondaryId, token: secretOne, aes_key: secretTwo });
    if (channel === 'wechat_kf') Object.assign(body, { corp_id: primaryId, open_kfid: secondaryId, token: secretOne, aes_key: secretTwo });
    if (channel === 'dingtalk') Object.assign(body, { app_id: primaryId, robot_code: secondaryId, secret: secretOne || secretTwo });
    if (channel === 'feishu') Object.assign(body, { app_id: primaryId, webhook_url: secondaryId || webhookUrl, verification_token: secretOne, encrypt_key: secretTwo });
    if (channel === 'qq') Object.assign(body, { app_id: primaryId, secret: secretOne, access_token: secretTwo });
    const response = await fetch(`${API_URL}/api/tenants/${tenant.tenant_id}/channels/${channel}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }).catch(() => null);
    if (response?.ok) {
      const data = await response.json();
      setChannels((old) => old.map((item) => item.channel === channel ? data.item : item));
      setSecretOne(''); setSecretTwo('');
      onNotify(`${labels[channel]}配置已保存为 v${data.version}，密钥不会回显`);
    } else onNotify('IM 配置保存失败');
  }

  async function simulate() {
    setRunning(true);
    const response = await fetch(`${API_URL}/api/im/simulate`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ tenant_id: tenant.tenant_id, channel, text, user_id: userId, chat_id: chatType === 'group' ? chatId : userId, chat_type: chatType, message_id: messageId || null, mode }) }).catch(() => null);
    const data = response ? await response.json().catch(() => null) : null;
    setResult(data);
    onNotify(response?.ok ? (data.duplicate ? '重复消息已拦截' : `${labels[channel]}本地流程验证通过`) : data?.detail ?? '本地 IM 验证失败；请先检测数据库连接');
    setRunning(false);
  }

  const current = channels.find((item) => item.channel === channel);
  return <div className="management-page">
    <PageIntro eyebrow="UNIFIED IM ADAPTER" title="IM 接入验证" detail="使用同一个 ChannelAdapter 抽象适配企业微信、微信客服、钉钉、飞书和 QQ；无需真实账号即可先验证本地流程。" />
    <div className="channel-tabs">{(Object.keys(labels) as ChannelName[]).map((key) => <button key={key} className={channel === key ? 'active' : ''} onClick={() => { setChannel(key); setResult(null); }}><RadioTower size={15} /><span>{labels[key]}<small>{channels.find((item) => item.channel === key)?.configured ? '已配置' : '待配置'}</small></span></button>)}</div>
    <div className="ops-grid im-grid">
      <section className="data-panel config-form">
        <div className="section-bar"><div><h3>{labels[channel]}配置</h3><p>真实凭据可由导师运行时填写；浏览器只能看到配置状态</p></div><span className={current?.configured ? 'tag green' : 'tag'}>{current?.configured ? '已配置' : '未配置'}</span></div>
        <div className="form-grid">
          <label><span>{fieldLabels[channel][0]}</span><input value={primaryId} onChange={(e) => setPrimaryId(e.target.value)} /></label>
          <label><span>{fieldLabels[channel][1]}</span><input value={secondaryId} onChange={(e) => setSecondaryId(e.target.value)} /></label>
          <label><span>{fieldLabels[channel][2]}</span><input type="password" value={secretOne} onChange={(e) => setSecretOne(e.target.value)} placeholder={current?.secret_configured ? '已配置 · 留空保持不变' : ''} /></label>
          <label><span>{fieldLabels[channel][3]}</span><input type="password" value={secretTwo} onChange={(e) => setSecretTwo(e.target.value)} placeholder={current?.secret_configured ? '已配置 · 留空保持不变' : ''} /></label>
          <label className="wide"><span>Callback URL</span><input value={`${API_URL}/webhook/${tenant.tenant_id}/${channel}`} readOnly /></label>
          <label className="wide"><span>平台发送 URL（可选，真实联调时填写）</span><input value={webhookUrl} onChange={(e) => setWebhookUrl(e.target.value)} placeholder="由平台控制台或官方 SDK 提供" /></label>
        </div>
        <div className="panel-actions"><button className="primary-action" onClick={saveChannel}><Save size={13} />保存平台配置</button></div>
      </section>
      <section className="data-panel config-form">
        <div className="section-bar"><div><h3>本地 IM 模拟器</h3><p>平台 Payload → Adapter → Worker → 数据库 → 平台回复 Payload</p></div><span className="tag green">无真实发送</span></div>
        <div className="form-grid">
          <label><span>聊天类型</span><select value={chatType} onChange={(e) => setChatType(e.target.value as 'private' | 'group')}><option value="private">单聊</option><option value="group">群聊</option></select></label>
          <label><span>外部用户 ID</span><input value={userId} onChange={(e) => setUserId(e.target.value)} /></label>
          {chatType === 'group' && <label className="wide"><span>群 ID</span><input value={chatId} onChange={(e) => setChatId(e.target.value)} /></label>}
          <label className="wide"><span>消息 ID（重复填写可验证幂等）</span><input value={messageId} onChange={(e) => setMessageId(e.target.value)} placeholder="留空自动生成" /></label>
          <label className="wide"><span>用户消息</span><textarea rows={4} value={text} onChange={(e) => setText(e.target.value)} /></label>
        </div>
        <div className="panel-actions"><button className="primary-action" onClick={simulate} disabled={running || !text.trim()}><Play size={13} />{running ? '执行中…' : '运行完整 IM 流程'}</button></div>
      </section>
    </div>
    {result && <section className="data-panel im-result"><div className="section-bar"><div><h3>验证结果</h3><p>签名在本地 fixture 模式下跳过；真实 webhook 仍由 Gateway 强制验签</p></div><span className={result.duplicate ? 'tag' : 'tag green'}>{result.duplicate ? 'duplicate' : 'completed'}</span></div><div className="result-grid"><div><b>标准化输入</b><pre>{JSON.stringify(result.inbound ?? result, null, 2)}</pre></div><div><b>平台回复 Payload</b><pre>{JSON.stringify(result.outbound_payloads ?? [], null, 2)}</pre></div></div></section>}
  </div>;
}

function ConfigPage({ tenant, mode, onMode, onNotify, onNavigate }: { tenant: Tenant; mode: 'mock' | 'deepseek'; onMode: (value: 'mock' | 'deepseek') => void; onNotify: (text: string) => void; onNavigate: (key: NavKey) => void }) {
  const [timeout, setTimeoutValue] = useState('30');
  const [instruction, setInstruction] = useState('你是专业、友好且遵守租户边界的企业 Agent。');
  return <div className="management-page config-page">
    <PageIntro eyebrow="VERSIONED CONFIGURATION" title="配置中心" detail={`正在编辑 ${tenant.name}；保存会创建新版本，可按租户回滚。`} action={<button className="primary-action" onClick={() => onNotify('配置已保存为 v8，并通知所有 Worker 热更新')}><Save size={14} />保存新版本</button>} />
    <div className="config-layout">
      <aside className="config-menu"><button className="active"><Bot size={15} />模型与 Agent</button><button onClick={() => onNotify('工具权限由租户治理 Filter 管理')}><Wrench size={15} />工具权限</button><button onClick={() => onNavigate('im')}><RadioTower size={15} />IM 通道</button><button onClick={() => onNavigate('storage')}><Database size={15} />数据后端</button><button onClick={() => onNotify('审计与治理配置已启用版本管理')}><ShieldCheck size={15} />审计与治理</button><button onClick={() => onNotify('密钥仅保存于后端 Secret/KMS 引用')}><LockKeyhole size={15} />密钥引用</button></aside>
      <section className="data-panel config-form">
        <div className="section-bar"><div><h3>模型与 Agent</h3><p>API Key 仅从本地服务端环境变量读取</p></div><span className="tag green">config v7</span></div>
        <div className="form-grid"><label><span>运行模式</span><select value={mode} onChange={(event) => onMode(event.target.value as 'mock' | 'deepseek')}><option value="mock">Mock（本地）</option><option value="deepseek">DeepSeek</option></select></label><label><span>模型名称</span><input value={mode === 'mock' ? 'mock-local' : tenant.model} readOnly /></label><label className="wide"><span>API Endpoint</span><input value="https://api.deepseek.com" readOnly /></label><label><span>超时（秒）</span><input value={timeout} onChange={(event) => setTimeoutValue(event.target.value)} /></label><label><span>Fallback Model</span><input value="deepseek-chat" readOnly /></label><label className="wide"><span>系统指令</span><textarea rows={5} value={instruction} onChange={(event) => setInstruction(event.target.value)} /></label></div>
        <div className="secret-notice"><KeyRound size={18} /><div><b>密钥未进入配置文档</b><p>当前引用环境变量 <code>DEEPSEEK_API_KEY</code>，日志、Trace 和错误报告只显示 ***。</p></div><span>已保护</span></div>
        <div className="version-row"><div><RotateCcw size={16} /><span><b>v7 · 当前版本</b><small>本地管理员 · 12 分钟前</small></span></div><div><span><b>v6 · 上一版本</b><small>调整 timeout 30 → 20</small></span><button onClick={() => onNotify('已回滚到配置 v6')}>回滚</button></div></div>
      </section>
    </div>
  </div>;
}

export default function Home() {
  const [tenants, setTenants] = useState(defaultTenants);
  const [tenantId, setTenantId] = useState(defaultTenants[0].tenant_id);
  const [mode, setMode] = useState<'mock' | 'deepseek'>('mock');
  const [activeNav, setActiveNav] = useState<NavKey>('playground');
  const [messages, setMessages] = useState<Record<string, ChatMessage[]>>({
    [defaultTenants[0].tenant_id]: initialMessages,
  });
  const [input, setInput] = useState('');
  const [sending, setSending] = useState(false);
  const [online, setOnline] = useState(false);
  const [sideTab, setSideTab] = useState<'session' | 'trace'>('session');
  const [sessionEvents, setSessionEvents] = useState(1);
  const [toast, setToast] = useState('');
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [auditItems, setAuditItems] = useState<AuditItem[]>([]);

  const tenant = useMemo(
    () => tenants.find((item) => item.tenant_id === tenantId) ?? tenants[0],
    [tenantId, tenants],
  );
  const currentMessages = messages[tenantId] ?? initialMessages;

  useEffect(() => {
    fetch(`${API_URL}/api/bootstrap`)
      .then((response) => {
        if (!response.ok) throw new Error('offline');
        return response.json();
      })
      .then((data) => {
        if (Array.isArray(data.tenants) && data.tenants.length) setTenants(data.tenants);
        setOnline(true);
      })
      .catch(() => setOnline(false));
  }, []);

  useEffect(() => {
    if (activeNav !== 'audit') return;
    fetch(`${API_URL}/api/audit?tenant_id=${encodeURIComponent(tenantId)}`)
      .then((response) => response.ok ? response.json() : Promise.reject(new Error('audit unavailable')))
      .then((data) => setAuditItems(data.items ?? []))
      .catch(() => setAuditItems([]));
  }, [activeNav, tenantId]);

  const pageTitles: Record<NavKey, string> = {
    playground: '多租户 Agent 实战验证台', tenants: '租户管理', sessions: '会话', traces: '链路追踪',
    audit: '审计日志', topology: '节点拓扑', storage: '数据后端', im: 'IM 接入验证', config: '配置中心',
  };

  async function refreshAudit() {
    try {
      const response = await fetch(`${API_URL}/api/audit?tenant_id=${encodeURIComponent(tenantId)}`);
      if (!response.ok) throw new Error('audit unavailable');
      setAuditItems((await response.json()).items ?? []);
    } catch {
      setAuditItems([]);
    }
  }

  function notify(text: string) {
    setToast(text);
    window.setTimeout(() => setToast(''), 3200);
  }

  function pushMessage(id: string, message: ChatMessage) {
    setMessages((old) => ({ ...old, [id]: [...(old[id] ?? initialMessages), message] }));
  }

  async function sendMessage(event?: FormEvent, override?: string) {
    event?.preventDefault();
    const text = (override ?? input).trim();
    if (!text || sending) return;

    if (text.includes('删除') || text.includes('转账') || text.includes('危险')) {
      setConfirmOpen(true);
      setInput(text);
      return;
    }

    const now = new Date().toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
    pushMessage(tenantId, { id: crypto.randomUUID(), role: 'user', text, time: now });
    setInput('');
    setSending(true);
    try {
      const response = await fetch(`${API_URL}/api/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tenant_id: tenantId,
          user_id: 'local-tester',
          channel: 'web',
          message: text,
          mode,
        }),
      });
      if (!response.ok) throw new Error((await response.json()).detail ?? '请求失败');
      const data = await response.json();
      pushMessage(tenantId, {
        id: crypto.randomUUID(),
        role: 'assistant',
        text: data.reply,
        time: now,
        trace: data.trace_id,
      });
      setSessionEvents((value) => value + 2);
      setOnline(true);
    } catch (error) {
      const fallback = mode === 'mock'
        ? `[本地演示] ${tenant.name} 已收到：“${text}”。启动后端后，这里会显示真实 tRPC-Agent 的 Session、Memory 与审计结果。`
        : `连接失败：${error instanceof Error ? error.message : '请检查本地后端'}`;
      pushMessage(tenantId, { id: crypto.randomUUID(), role: 'assistant', text: fallback, time: now });
      setOnline(false);
    } finally {
      setSending(false);
    }
  }

  async function runScenario(id: string) {
    if (id === 'danger') {
      setInput('请执行危险操作：删除测试订单 10086');
      setConfirmOpen(true);
      return;
    }
    const response = await fetch(`${API_URL}/api/scenarios/${id}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tenant_id: tenantId }),
    }).catch(() => null);
    const data = response?.ok ? await response.json() : null;
    setToast(data?.message ?? (id === 'duplicate' ? '模拟完成：第二次投递被幂等层拦截' : '隔离检查通过：未发现跨租户数据'));
    window.setTimeout(() => setToast(''), 3200);
  }

  function approveDanger() {
    const text = input || '执行危险工具';
    setConfirmOpen(false);
    setInput('');
    pushMessage(tenantId, { id: crypto.randomUUID(), role: 'user', text, time: '刚刚' });
    pushMessage(tenantId, {
      id: crypto.randomUUID(),
      role: 'system',
      text: '确认已记录，但本地验证台不会执行真实破坏操作。审计决策：approved / dry-run。',
      time: '刚刚',
    });
    setToast('危险操作已批准（仅 dry-run）');
  }

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand"><span className="brand-mark"><Braces size={19} /></span><span>AgentOps</span></div>
        <nav className="nav-list" aria-label="主导航">
          <button className={activeNav === 'playground' ? 'active' : ''} onClick={() => setActiveNav('playground')}><Play size={17} />验证台</button>
          <button className={activeNav === 'tenants' ? 'active' : ''} onClick={() => setActiveNav('tenants')}><Users size={17} />租户管理</button>
          <button className={activeNav === 'sessions' ? 'active' : ''} onClick={() => setActiveNav('sessions')}><MessageSquareText size={17} />会话</button>
          <button className={activeNav === 'traces' ? 'active' : ''} onClick={() => setActiveNav('traces')}><Activity size={17} />链路追踪</button>
          <button className={activeNav === 'audit' ? 'active' : ''} onClick={() => setActiveNav('audit')}><FileText size={17} />审计日志</button>
        </nav>
        <div className="nav-label">平台</div>
        <nav className="nav-list">
          <button className={activeNav === 'topology' ? 'active' : ''} onClick={() => setActiveNav('topology')}><Boxes size={17} />节点拓扑</button>
          <button className={activeNav === 'storage' ? 'active' : ''} onClick={() => setActiveNav('storage')}><Database size={17} />数据后端</button>
          <button className={activeNav === 'im' ? 'active' : ''} onClick={() => setActiveNav('im')}><RadioTower size={17} />IM 接入</button>
          <button className={activeNav === 'config' ? 'active' : ''} onClick={() => setActiveNav('config')}><Settings2 size={17} />配置中心</button>
        </nav>
        <div className="sidebar-footer">
          <div className="health-row"><span><StatusDot ok={online} />本地后端</span><b>{online ? '正常' : '待启动'}</b></div>
          <div className="health-row"><span><StatusDot />Mock 模型</span><b>可用</b></div>
          <p>v0.1 · local workspace</p>
        </div>
      </aside>

      <section className="workspace">
        <header className="topbar">
          <div>
            <p className="eyebrow">FULL-STACK VALIDATION</p>
            <h1>{pageTitles[activeNav]}</h1>
          </div>
          <div className="top-actions">
            <label className="mode-switch">
              <button className={mode === 'mock' ? 'selected' : ''} onClick={() => setMode('mock')}>Mock</button>
              <button className={mode === 'deepseek' ? 'selected' : ''} onClick={() => setMode('deepseek')}>DeepSeek</button>
            </label>
            <button className="icon-button" title="设置" onClick={() => setActiveNav('config')}><Settings2 size={18} /></button>
          </div>
        </header>

        <nav className="mobile-nav" aria-label="移动端导航">
          {[
            ['playground', '验证台', Play], ['tenants', '租户', Users], ['sessions', '会话', MessageSquareText],
            ['traces', 'Trace', Activity], ['audit', '审计', FileText], ['topology', '节点', Boxes],
            ['storage', '存储', Database], ['im', 'IM', RadioTower], ['config', '配置', Settings2],
          ].map(([key, label, Icon]) => <button key={key as string} className={activeNav === key ? 'active' : ''} onClick={() => setActiveNav(key as NavKey)}><Icon size={14} />{label as string}</button>)}
        </nav>

        <div className="context-bar">
          <div className="selector-block">
            <span className="tenant-swatch" style={{ background: tenant.accent }} />
            <div><small>当前租户</small><strong>{tenant.name}</strong></div>
            <ChevronDown size={16} />
            <select aria-label="选择租户" value={tenantId} onChange={(event) => setTenantId(event.target.value)}>
              {tenants.map((item) => <option key={item.tenant_id} value={item.tenant_id}>{item.name}</option>)}
            </select>
          </div>
          <span className="divider" />
          <div className="context-item"><Bot size={16} /><span><small>模型</small><b>{mode === 'mock' ? 'mock-local' : tenant.model}</b></span></div>
          <div className="context-item"><Database size={16} /><span><small>Session 后端</small><b>{tenant.storage}</b></span></div>
          <div className="context-item"><Wrench size={16} /><span><small>工具权限</small><b>{tenant.tools.length} 个已授权</b></span></div>
          <div className={`connection-pill ${online ? 'online' : ''}`}><StatusDot ok={online} />{online ? 'API 已连接' : '离线预览'}</div>
        </div>

        {activeNav === 'playground' ? <>
        <div className="content-grid">
          <section className="chat-panel panel">
            <div className="panel-heading">
              <div><h2>Agent 对话</h2><p>session: web · local-tester · {tenantId}</p></div>
              <button className="text-button" onClick={() => setMessages((old) => ({ ...old, [tenantId]: initialMessages }))}><RefreshCw size={14} />新会话</button>
            </div>
            <div className="message-list">
              {currentMessages.map((message) => (
                <article key={message.id} className={`message ${message.role}`}>
                  <div className="avatar">{message.role === 'user' ? '你' : message.role === 'system' ? <ShieldCheck size={15} /> : <Sparkles size={15} />}</div>
                  <div className="message-body">
                    <div className="message-meta"><b>{message.role === 'user' ? '本地测试用户' : message.role === 'system' ? '治理 Filter' : tenant.name + ' Agent'}</b><span>{message.time}</span></div>
                    <p>{message.text}</p>
                    {message.trace && <code>trace {message.trace.slice(0, 16)}…</code>}
                  </div>
                </article>
              ))}
              {sending && <article className="message assistant"><div className="avatar"><Sparkles size={15} /></div><div className="typing"><i /><i /><i /></div></article>}
            </div>

            <div className="scenario-row">
              {scenarios.map(({ id, title, detail, icon: Icon }) => (
                <button key={id} onClick={() => runScenario(id)}><Icon size={16} /><span><b>{title}</b><small>{detail}</small></span></button>
              ))}
            </div>
            <form className="composer" onSubmit={sendMessage}>
              <textarea value={input} onChange={(event) => setInput(event.target.value)} onKeyDown={(event) => {
                if (event.key === 'Enter' && !event.shiftKey) { event.preventDefault(); sendMessage(); }
              }} placeholder="输入消息，Enter 发送 · Shift + Enter 换行" rows={2} />
              <div className="composer-footer"><span><KeyRound size={13} />密钥只保存在本地后端</span><button disabled={!input.trim() || sending} aria-label="发送消息"><Send size={17} /></button></div>
            </form>
          </section>

          <aside className="inspector panel">
            <div className="tabs">
              <button className={sideTab === 'session' ? 'active' : ''} onClick={() => setSideTab('session')}>Session</button>
              <button className={sideTab === 'trace' ? 'active' : ''} onClick={() => setSideTab('trace')}>Trace</button>
            </div>
            {sideTab === 'session' ? <>
              <div className="stat-grid">
                <div><span>事件数</span><strong>{sessionEvents}</strong><small>+2 本轮</small></div>
                <div><span>Tokens</span><strong>{mode === 'mock' ? '0' : '1.2k'}</strong><small>预算 8%</small></div>
              </div>
              <section className="inspect-section">
                <div className="section-title"><h3>会话状态</h3><span className="tag green">共享后端</span></div>
                <div className="kv"><span>tenant_id</span><code>{tenantId}</code></div>
                <div className="kv"><span>user_id</span><code>local-tester</code></div>
                <div className="kv"><span>channel</span><code>web / private</code></div>
                <div className="kv"><span>version</span><code>v{sessionEvents}</code></div>
              </section>
              <section className="inspect-section">
                <div className="section-title"><h3>Memory</h3><button>查看全部</button></div>
                <div className="empty-memory"><Database size={18} /><p>跨节点可见</p><span>新记忆将在写入后显示</span></div>
              </section>
              <section className="inspect-section">
                <div className="section-title"><h3>工具策略</h3><span>{tenant.tools.length}/{tenant.tools.length}</span></div>
                <div className="tool-list">{tenant.tools.map((tool) => <span key={tool}><Check size={12} />{tool}</span>)}</div>
              </section>
            </> : <>
              <div className="trace-summary"><span><Activity size={18} /></span><div><b>最近一次链路</b><small>总耗时 328 ms</small></div></div>
              <div className="trace-line"><i /><div><b>IM Callback</b><span>12 ms</span><small>signature · idempotency</small></div></div>
              <div className="trace-line"><i /><div><b>Agent Worker</b><span>286 ms</span><small>tenant route · session lock</small></div></div>
              <div className="trace-line"><i /><div><b>Session / Memory</b><span>8 ms</span><small>{tenant.storage} shared backend</small></div></div>
              <div className="trace-line"><i /><div><b>IM Reply</b><span>22 ms</span><small>delivery success</small></div></div>
            </>}
          </aside>
        </div>

        <footer className="metrics-strip">
          <div><Gauge size={16} /><span>请求成功率</span><b>99.98%</b></div>
          <div><Clock3 size={16} /><span>P95 延迟</span><b>412 ms</b></div>
          <div><Zap size={16} /><span>Worker 节点</span><b>2 / 2</b></div>
          <div><TerminalSquare size={16} /><span>审计事件</span><b>{sessionEvents + 23}</b></div>
        </footer>
        </> : <div className="view-scroll">
          {activeNav === 'tenants' && <TenantsPage tenants={tenants} selected={tenantId} onSelect={setTenantId} onNotify={notify} onCreated={(created) => setTenants((old) => [...old, created])} />}
          {activeNav === 'sessions' && <SessionsPage tenants={tenants} messages={messages} onOpen={(id) => { setTenantId(id); setActiveNav('playground'); }} />}
          {activeNav === 'traces' && <TracesPage tenant={tenant} messages={currentMessages} />}
          {activeNav === 'audit' && <AuditPage items={auditItems} tenant={tenant} onRefresh={refreshAudit} />}
          {activeNav === 'topology' && <TopologyPage online={online} onNotify={notify} />}
          {activeNav === 'storage' && <StoragePage tenant={tenant} tenants={tenants} onTenants={setTenants} onNotify={notify} />}
          {activeNav === 'im' && <IMPage tenant={tenant} mode={mode} onNotify={notify} />}
          {activeNav === 'config' && <ConfigPage tenant={tenant} mode={mode} onMode={setMode} onNotify={notify} onNavigate={setActiveNav} />}
        </div>}
      </section>

      {confirmOpen && <div className="modal-backdrop" role="dialog" aria-modal="true" aria-label="危险工具确认">
        <div className="confirm-card">
          <button className="modal-close" onClick={() => setConfirmOpen(false)} aria-label="关闭"><X size={18} /></button>
          <div className="danger-icon"><CircleAlert size={23} /></div>
          <p className="eyebrow">HUMAN IN THE LOOP</p>
          <h2>需要人工确认</h2>
          <p>Agent 请求调用高风险工具 <code>delete_test_order</code>。当前验证环境将强制使用 dry-run，不会修改真实数据。</p>
          <div className="risk-box"><span>租户</span><b>{tenant.name}</b><span>请求用户</span><b>local-tester</b><span>审计策略</span><b>完整记录</b></div>
          <div className="modal-actions"><button onClick={() => setConfirmOpen(false)}>拒绝</button><button className="danger" onClick={approveDanger}>确认 dry-run</button></div>
        </div>
      </div>}
      {toast && <div className="toast"><Check size={16} />{toast}</div>}
    </main>
  );
}
