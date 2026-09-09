"use strict";

const state = {
  adminKey: sessionStorage.getItem("trpcAdminKey") || "",
  overview: null,
  selectedTenant: null,
};

const labels = {
  overview: "运行总览",
  tenants: "租户与版本",
  configuration: "配置发布",
  audit: "审计追踪",
  evidence: "验收证据",
};

const metricDefinitions = [
  ["tenant_count", "租户总数", "TENANTS", "已建立可信路由"],
  ["sessions", "会话总数", "SESSIONS", "跨节点共享状态"],
  ["inbox_open", "待处理消息", "INBOX OPEN", "含重试与对账"],
  ["runs_open", "执行中任务", "RUNS OPEN", "Agent 运行账本"],
  ["outbox_open", "待投递回复", "OUTBOX OPEN", "含 unknown 状态"],
  ["audit_records", "审计记录", "AUDIT EVENTS", "仅统计元数据"],
];

const template = {
  schema_version: 1,
  tenant_id: "tenant-demo",
  revision: 1,
  display_name: "演示租户",
  status: "active",
  apps: [
    {
      app_id: "assistant",
      revision: 1,
      name: "assistant_agent",
      prompt: "你是该租户的专业助手。只使用经过授权的知识与工具，回答简洁、准确。",
      model: {
        provider: "openai",
        model: "your-approved-model",
        api_key_ref: "secret://env/TENANT_DEMO_MODEL_API_KEY",
        timeout_seconds: 60,
        token_ceiling: 8000,
        temperature: 0.2,
        fallback_models: [],
      },
      tools: {
        allowed: ["preload_memory"],
        requires_approval: [],
        max_calls_per_turn: 12,
        max_cost_per_turn: 1.0,
      },
      governance: {
        redact_sensitive_data: true,
        max_input_chars: 16000,
        max_output_chars: 16000,
        blocked_input_terms: [],
        blocked_output_terms: [],
      },
      metadata: {},
    },
  ],
  channels: [
    {
      binding_id: "wecom-demo",
      app_id: "assistant",
      app_revision: 1,
      channel: "wecom",
      external_account_id: "replace-with-wecom-bot-id",
      callback_path: "/v1/channels/wecom/wecom-demo-public/callback",
      public_callback_id: "wecom-demo-public",
      route_rule: {},
      secret_refs: {
        token: "secret://env/TENANT_DEMO_WECOM_TOKEN",
        aes_key: "secret://env/TENANT_DEMO_WECOM_AES_KEY",
      },
      identity_policy: {
        default_action: "deny",
        allow_principals: [],
        deny_principals: [],
        allowed_scopes: ["private", "group", "group_member"],
      },
      enabled: false,
    },
    {
      binding_id: "telegram-demo",
      app_id: "assistant",
      app_revision: 1,
      channel: "telegram",
      external_account_id: "replace-with-telegram-bot-id",
      callback_path: "/v1/channels/telegram/telegram-demo-public/callback",
      public_callback_id: "telegram-demo-public",
      route_rule: {},
      secret_refs: {
        webhook_secret: "secret://env/TENANT_DEMO_TELEGRAM_WEBHOOK_SECRET",
        bot_token: "secret://env/TENANT_DEMO_TELEGRAM_BOT_TOKEN",
      },
      identity_policy: {
        default_action: "deny",
        allow_principals: [],
        deny_principals: [],
        allowed_scopes: ["private", "group", "group_member"],
      },
      enabled: false,
    },
  ],
  storage: {
    session: "postgresql",
    memory: "postgresql",
    summary: "postgresql",
    knowledge: "pgvector",
    artifact: "s3",
  },
  audit: {
    scope: "all_tools",
    retention_days: 180,
    export: "restricted",
    capture_prompt_content: false,
  },
  budget: {},
};

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));

function node(tag, options = {}, children = []) {
  const element = document.createElement(tag);
  if (options.className) element.className = options.className;
  if (options.text !== undefined) element.textContent = String(options.text);
  if (options.type) element.type = options.type;
  if (options.title) element.title = options.title;
  if (options.dataset) Object.assign(element.dataset, options.dataset);
  for (const child of children) {
    if (child !== null && child !== undefined) element.append(child);
  }
  return element;
}

function showToast(message, isError = false) {
  const toast = node("div", {
    className: `toast${isError ? " is-error" : ""}`,
    text: message,
  });
  $("#toast-region").append(toast);
  window.setTimeout(() => toast.remove(), 4200);
}

function setConnection(connected) {
  const element = $("#connection-state");
  element.classList.toggle("is-offline", !connected);
  element.lastChild.textContent = connected ? " 已连接" : " 未连接";
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  headers.set("x-admin-key", state.adminKey);
  headers.set("x-admin-actor", "admin:console");
  if (options.body) headers.set("content-type", "application/json");
  const response = await fetch(path, { ...options, headers });
  if (response.status === 401) {
    setConnection(false);
    throw new Error("管理密钥无效");
  }
  if (!response.ok) {
    let detail = `请求失败（HTTP ${response.status}）`;
    try {
      const body = await response.json();
      detail = body.detail || body.title || detail;
      if (Array.isArray(body.detail)) {
        detail = body.detail.map((item) => item.msg).join("；");
      }
    } catch {
      // Keep the status-only message when the response is intentionally empty.
    }
    throw new Error(detail);
  }
  setConnection(true);
  if (response.status === 204) return null;
  return response.json();
}

function switchView(name) {
  $$("[data-view-panel]").forEach((panel) => {
    const active = panel.dataset.viewPanel === name;
    panel.hidden = !active;
    panel.classList.toggle("is-active", active);
  });
  $$("[data-view]").forEach((button) => {
    const active = button.dataset.view === name;
    button.classList.toggle("is-active", active);
    if (active) {
      button.setAttribute("aria-current", "page");
    } else {
      button.removeAttribute("aria-current");
    }
  });
  $("#breadcrumb-current").textContent = labels[name];
  if (name === "audit") loadAudit();
}

function renderLoading() {
  const grid = $("#metric-grid");
  grid.replaceChildren(
    ...metricDefinitions.map(() =>
      node("div", { className: "metric-card" }, [
        node("div", { className: "skeleton" }),
      ]),
    ),
  );
}

function renderMetrics(overview) {
  const values = {
    tenant_count: overview.tenant_count,
    ...overview.totals,
  };
  $("#metric-grid").replaceChildren(
    ...metricDefinitions.map(([key, chinese, technical, note]) =>
      node("article", { className: "metric-card" }, [
        node("span", { className: "metric-label", text: technical }),
        node("strong", { className: "metric-value", text: values[key] ?? 0 }),
        node("span", { className: "metric-note", text: `${chinese} · ${note}` }),
      ]),
    ),
  );
}

function statusChip(status) {
  const className = status === "active" ? "chip" : "chip chip-warning";
  return node("span", { className, text: status });
}

function renderTenantOverview(tenants) {
  const host = $("#tenant-overview");
  if (!tenants.length) {
    host.replaceChildren(
      node("div", { className: "empty-state" }, [
        node("span", { className: "empty-glyph", text: "○" }),
        node("h2", { text: "还没有可见租户" }),
        node("p", { text: "发布至少包含一个通道绑定的 TenantSpec 后会显示在这里。" }),
      ]),
    );
    return;
  }

  const tbody = node("tbody");
  for (const tenant of tenants) {
    const tenantButton = node("button", { className: "tenant-table-link", type: "button" }, [
      node("strong", { text: tenant.display_name }),
      node("span", { className: "mono", text: tenant.tenant_id }),
    ]);
    tenantButton.addEventListener("click", () => selectTenant(tenant.tenant_id));
    const open =
      tenant.queue.inbox_open +
      tenant.queue.runs_open +
      tenant.queue.outbox_open +
      tenant.queue.projections_open;
    const row = node("tr", {}, [
      node("td", {}, [tenantButton]),
      node("td", {}, [statusChip(tenant.status)]),
      node("td", { className: "mono", text: `r${tenant.active_revision}` }),
      node("td", { text: tenant.channels.join(" / ") || "—" }),
      node("td", { className: "numeric", text: tenant.queue.sessions }),
      node("td", { className: "numeric", text: open }),
    ]);
    tbody.append(row);
  }
  const table = node("table", { className: "data-table" }, [
    node("thead", {}, [
      node("tr", {}, [
        node("th", { text: "租户" }),
        node("th", { text: "状态" }),
        node("th", { text: "版本" }),
        node("th", { text: "通道" }),
        node("th", { className: "numeric", text: "会话" }),
        node("th", { className: "numeric", text: "待处理" }),
      ]),
    ]),
    tbody,
  ]);
  host.replaceChildren(table);
}

function renderTenantList(tenants) {
  const host = $("#tenant-list");
  if (!tenants.length) {
    host.replaceChildren(
      node("div", { className: "callout callout-muted", text: "暂无已绑定通道的租户。" }),
    );
    return;
  }
  host.replaceChildren(
    ...tenants.map((tenant) => {
      const button = node("button", { className: "tenant-list-item", type: "button" }, [
        node("span", {}, [
          node("strong", { text: tenant.display_name }),
          node("small", { text: `${tenant.tenant_id} / revision ${tenant.active_revision}` }),
        ]),
        statusChip(tenant.status),
      ]);
      button.dataset.tenantId = tenant.tenant_id;
      button.addEventListener("click", () => selectTenant(tenant.tenant_id));
      return button;
    }),
  );
}

function populateAuditSelect(tenants) {
  const select = $("#audit-tenant");
  const previous = select.value;
  select.replaceChildren(
    ...tenants.map((tenant) => {
      const option = node("option", { text: `${tenant.display_name} · ${tenant.tenant_id}` });
      option.value = tenant.tenant_id;
      return option;
    }),
  );
  if (tenants.some((tenant) => tenant.tenant_id === previous)) select.value = previous;
}

function renderOverview(overview) {
  state.overview = overview;
  renderMetrics(overview);
  renderTenantOverview(overview.tenants);
  renderTenantList(overview.tenants);
  populateAuditSelect(overview.tenants);
  $("#environment-badge").textContent = `${overview.environment} / ${overview.database}`;
  $("#snapshot-time").textContent = new Date(overview.generated_at).toLocaleString("zh-CN");

  const open =
    overview.totals.inbox_open +
    overview.totals.runs_open +
    overview.totals.outbox_open +
    overview.totals.projections_open;
  $("#pipeline-advice").textContent = open
    ? `当前共有 ${open} 个开放状态，请优先检查 retry、unknown 与 dead letter。`
    : "当前没有开放队列项。真实可用性仍需结合 Worker 日志和外部 IM 探针。";
}

async function loadOverview({ authenticate = false } = {}) {
  renderLoading();
  try {
    const overview = await api("/v1/admin/overview");
    renderOverview(overview);
    if (authenticate) $("#auth-dialog").close();
    return true;
  } catch (error) {
    if (authenticate) {
      $("#auth-error").textContent = error.message;
    } else {
      showToast(error.message, true);
      if (error.message === "管理密钥无效") showAuth();
    }
    return false;
  }
}

async function selectTenant(tenantId) {
  state.selectedTenant = tenantId;
  switchView("tenants");
  $$(".tenant-list-item").forEach((item) => {
    item.classList.toggle("is-active", item.dataset.tenantId === tenantId);
  });
  const detail = $("#tenant-detail");
  detail.replaceChildren(node("div", { className: "loading-state", text: "正在加载租户快照…" }));
  try {
    const [spec, revisions] = await Promise.all([
      api(`/v1/admin/tenants/${encodeURIComponent(tenantId)}/active`),
      api(`/v1/admin/tenants/${encodeURIComponent(tenantId)}/revisions`),
    ]);
    renderTenantDetail(spec, revisions);
  } catch (error) {
    detail.replaceChildren(
      node("div", { className: "empty-state" }, [
        node("h2", { text: "无法加载租户" }),
        node("p", { text: error.message }),
      ]),
    );
  }
}

function renderTenantDetail(spec, revisions) {
  const tenant = state.overview.tenants.find((item) => item.tenant_id === spec.tenant_id);
  const editButton = node("button", {
    className: "button button-secondary",
    type: "button",
    text: "从当前版本创建下一版",
  });
  editButton.addEventListener("click", () => editNextRevision(spec));

  const revisionList = node("div", { className: "revision-list" });
  for (const revision of revisions) {
    let action;
    if (revision.active) {
      action = node("span", { className: "chip", text: "ACTIVE" });
    } else {
      action = node("button", { className: "text-button", type: "button", text: "回滚到此版本" });
      action.addEventListener("click", () => rollback(spec.tenant_id, revision.revision));
    }
    revisionList.append(
      node("div", { className: "revision-row" }, [
        node("strong", { className: "mono", text: `r${revision.revision}` }),
        node("span", { className: "mono", text: revision.created_by }),
        node("span", { className: "hash", text: revision.content_hash }),
        action,
      ]),
    );
  }

  const queue = tenant?.queue || {};
  $("#tenant-detail").replaceChildren(
    node("div", { className: "detail-header" }, [
      node("div", {}, [
        node("span", { className: "detail-id", text: spec.tenant_id }),
        node("h2", { text: spec.display_name }),
        statusChip(spec.status),
      ]),
      editButton,
    ]),
    node("div", { className: "detail-subgrid" }, [
      detailStat("活动版本", `r${spec.revision}`),
      detailStat("Agent 应用", spec.apps.length),
      detailStat("IM 通道", spec.channels.length),
      detailStat("会话", queue.sessions || 0),
      detailStat("Inbox 开放", queue.inbox_open || 0),
      detailStat("Outbox 开放", queue.outbox_open || 0),
    ]),
    detailSection("模型路由", spec.apps.map((app) => `${app.model.provider} / ${app.model.model}`)),
    detailSection(
      "通道绑定",
      spec.channels.map(
        (channel) =>
          `${channel.channel} · ${channel.public_callback_id} · ${channel.enabled ? "ENABLED" : "DISABLED"}`,
      ),
    ),
    detailSection(
      "存储选择",
      Object.entries(spec.storage).map(([key, value]) => `${key}: ${value}`),
    ),
    node("section", { className: "detail-section" }, [
      node("h3", { text: "不可变版本" }),
      revisionList,
    ]),
  );
}

function detailStat(label, value) {
  return node("div", { className: "detail-stat" }, [
    node("span", { text: label }),
    node("strong", { text: value }),
  ]);
}

function detailSection(title, values) {
  const chips = values.length
    ? values.map((value) => node("span", { className: "chip chip-neutral", text: value }))
    : [node("span", { className: "chip chip-warning", text: "未配置" })];
  return node("section", { className: "detail-section" }, [
    node("h3", { text: title }),
    node("div", { className: "chip-row" }, chips),
  ]);
}

function editNextRevision(spec) {
  const next = structuredClone(spec);
  next.revision += 1;
  $("#tenant-path").value = next.tenant_id;
  $("#config-editor").value = JSON.stringify(next, null, 2);
  switchView("configuration");
  $("#config-editor").focus();
}

async function rollback(tenantId, revision) {
  const accepted = window.confirm(`确认将 ${tenantId} 的活动配置回滚到 revision ${revision}？`);
  if (!accepted) return;
  try {
    await api(`/v1/admin/tenants/${encodeURIComponent(tenantId)}/rollback`, {
      method: "POST",
      body: JSON.stringify({ target_revision: revision }),
    });
    showToast(`已将 ${tenantId} 回滚到 revision ${revision}`);
    await loadOverview();
    await selectTenant(tenantId);
  } catch (error) {
    showToast(error.message, true);
  }
}

function loadTemplate({ notify = true } = {}) {
  const copy = structuredClone(template);
  $("#tenant-path").value = copy.tenant_id;
  $("#config-editor").value = JSON.stringify(copy, null, 2);
  if (notify) showToast("已载入不含真实凭据的安全模板");
}

async function publishConfig() {
  const tenantId = $("#tenant-path").value.trim();
  let spec;
  try {
    spec = JSON.parse($("#config-editor").value);
  } catch {
    showToast("JSON 格式无效，请先检查逗号、引号和括号。", true);
    return;
  }
  if (!tenantId || spec.tenant_id !== tenantId) {
    showToast("目标租户 ID 必须与 JSON 中的 tenant_id 完全一致。", true);
    return;
  }
  try {
    const result = await api(`/v1/admin/tenants/${encodeURIComponent(tenantId)}/revisions`, {
      method: "POST",
      body: JSON.stringify(spec),
    });
    showToast(
      result.idempotent
        ? `revision ${result.revision} 已存在，内容一致`
        : `revision ${result.revision} 已校验并发布`,
    );
    await loadOverview();
    await selectTenant(tenantId);
  } catch (error) {
    showToast(error.message, true);
  }
}

async function loadAudit() {
  const tenantId = $("#audit-tenant").value;
  const host = $("#audit-table");
  if (!tenantId) {
    host.replaceChildren(
      node("div", { className: "empty-state" }, [
        node("h2", { text: "暂无租户" }),
        node("p", { text: "发布租户并产生 Agent 运行记录后，审计事件会显示在这里。" }),
      ]),
    );
    return;
  }
  host.replaceChildren(node("div", { className: "loading-state", text: "正在读取审计元数据…" }));
  try {
    const entries = await api(`/v1/admin/tenants/${encodeURIComponent(tenantId)}/activity?limit=50`);
    renderAudit(entries);
  } catch (error) {
    host.replaceChildren(node("div", { className: "empty-state" }, [node("p", { text: error.message })]));
  }
}

function renderAudit(entries) {
  const host = $("#audit-table");
  if (!entries.length) {
    host.replaceChildren(
      node("div", { className: "empty-state" }, [
        node("span", { className: "empty-glyph", text: "◎" }),
        node("h2", { text: "尚无审计事件" }),
        node("p", { text: "成功完成或最终拒绝的 Agent turn 会在这里留下不可变审计记录。" }),
      ]),
    );
    return;
  }
  const tbody = node("tbody");
  for (const entry of entries) {
    tbody.append(
      node("tr", {}, [
        node("td", { className: "mono", text: new Date(entry.created_at).toLocaleString("zh-CN") }),
        node("td", {}, [node("strong", { text: entry.decision })]),
        node("td", { text: entry.action }),
        node("td", { text: entry.agent_name }),
        node("td", { className: "numeric", text: `${entry.latency_ms} ms` }),
        node("td", { text: entry.error_type || "—" }),
        node("td", {
          className: "trace-link",
          text: entry.trace_id.length > 16 ? `${entry.trace_id.slice(0, 16)}…` : entry.trace_id,
          title: entry.trace_id,
        }),
      ]),
    );
  }
  host.replaceChildren(
    node("table", { className: "data-table" }, [
      node("thead", {}, [
        node("tr", {}, [
          node("th", { text: "时间" }),
          node("th", { text: "决策" }),
          node("th", { text: "动作" }),
          node("th", { text: "Agent" }),
          node("th", { className: "numeric", text: "耗时" }),
          node("th", { text: "错误" }),
          node("th", { text: "Trace" }),
        ]),
      ]),
      tbody,
    ]),
  );
}

function showAuth() {
  const dialog = $("#auth-dialog");
  $("#auth-error").textContent = "";
  if (!dialog.open) dialog.showModal();
  window.setTimeout(() => $("#admin-key").focus(), 30);
}

function signOut() {
  sessionStorage.removeItem("trpcAdminKey");
  state.adminKey = "";
  setConnection(false);
  showAuth();
}

function bindEvents() {
  $$("[data-view]").forEach((button) => {
    button.addEventListener("click", () => switchView(button.dataset.view));
  });
  $$("[data-view-jump]").forEach((button) => {
    button.addEventListener("click", () => switchView(button.dataset.viewJump));
  });
  $$("[data-new-config]").forEach((button) => {
    button.addEventListener("click", () => {
      loadTemplate();
      switchView("configuration");
    });
  });
  $("#refresh").addEventListener("click", () => loadOverview());
  $("#sign-out").addEventListener("click", signOut);
  $("#load-template").addEventListener("click", loadTemplate);
  $("#publish-config").addEventListener("click", publishConfig);
  $("#audit-tenant").addEventListener("change", loadAudit);
  $("#auth-dialog").addEventListener("cancel", (event) => event.preventDefault());
  $("#auth-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const candidate = $("#admin-key").value;
    state.adminKey = candidate;
    const valid = await loadOverview({ authenticate: true });
    if (valid) {
      sessionStorage.setItem("trpcAdminKey", candidate);
      $("#admin-key").value = "";
    }
  });
}

async function boot() {
  bindEvents();
  renderLoading();
  loadTemplate({ notify: false });
  if (!state.adminKey) {
    showAuth();
    return;
  }
  const valid = await loadOverview();
  if (!valid) showAuth();
}

boot();
