const state = {
  token: "", actor: null, tenants: [], models: [], credentials: [], profiles: [],
  agents: [], nodes: [], workerPool: null,
};
const titles = {
  overview: "平台概览", tenants: "租户", accounts: "管理账号", models: "模型目录",
  credentials: "模型凭据", profiles: "租户模型策略", runtime: "运行节点",
  usage: "用量账本", adapters: "IM 适配器", audit: "管理审计",
};

const {
  $, $$, escapeHTML, createApi, toast, setLoginError, badge, empty,
  formatDate, compactNumber, setOptions, withLoading,
} = window.ConsoleUI;
const api = createApi(state, "interactive platform administration");

function parseJSONObject(value, label) {
  try {
    const parsed = JSON.parse(String(value || "{}"));
    if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") throw new Error();
    return parsed;
  } catch {
    throw new Error(`${label}必须是 JSON 对象`);
  }
}

function modelLabel(modelId) {
  const model = state.models.find((item) => item.model_catalog_id === modelId);
  return model ? `${model.display_name} · ${model.model_name}` : modelId;
}

function profileLabel(profileId) {
  if (!profileId) return "未分配";
  return state.profiles.find((item) => item.model_profile_id === profileId)?.name || profileId;
}

async function loadTenants() {
  const data = await api.all("/tenants");
  state.tenants = data.items;
  $("#tenant-rows").innerHTML = data.items.map((item) => `
    <tr><td><span class="row-title">${escapeHTML(item.name)}</span><span class="row-subtitle mono">${escapeHTML(item.tenant_id)}</span></td>
    <td>${badge(item.status)}</td><td>${escapeHTML(item.isolation_mode)}</td><td>${formatDate(item.updated_at)}</td>
    <td><div class="actions"><button class="button secondary small tenant-edit" data-id="${item.tenant_id}">编辑</button>
    ${item.status === "active" ? `<button class="button danger small tenant-remove" data-id="${item.tenant_id}">移除</button>` : `<button class="button secondary small tenant-enable" data-id="${item.tenant_id}">重新启用</button>`}
    </div></td></tr>`).join("") || empty(5);
  setOptions(".tenant-options, #profile-tenant", data.items.filter((item) => item.status === "active"), "tenant_id", (item) => item.name);
  $$(".tenant-edit").forEach((button) => button.addEventListener("click", () => openTenantEdit(button.dataset.id)));
  $$(".tenant-remove").forEach((button) => button.addEventListener("click", () => removeTenant(button)));
  $$(".tenant-enable").forEach((button) => button.addEventListener("click", () => enableTenant(button)));
  return data;
}

async function loadAccounts() {
  const data = await api.page("/admin/principals", "account-rows", loadAccounts);
  $("#account-rows").innerHTML = data.items.map((item) => {
    const isTenantAdmin = item.external_subject.startsWith("tenant-console:");
    const passwordAction = item.principal_type === "human" ? `<button class="button secondary small password-open" data-id="${item.management_principal_id}" data-subject="${escapeHTML(item.external_subject)}">重置密码</button>` : "";
    // A tenant has one administrator identity; password rotation is its recovery path.
    const statusAction = isTenantAdmin ? `<span class="row-subtitle">唯一账号不可停用</span>` : `<button class="button ${item.status === "active" ? "danger" : "secondary"} small principal-toggle" data-id="${item.management_principal_id}" data-status="${item.status}">${item.status === "active" ? "停用" : "启用"}</button>`;
    return `<tr><td><span class="row-title">${escapeHTML(item.display_name)}</span><span class="row-subtitle mono">${escapeHTML(item.management_principal_id)}</span></td><td>${isTenantAdmin ? "租户管理员" : escapeHTML(item.principal_type)}</td><td class="mono">${escapeHTML(item.external_subject)}</td><td>${badge(item.status)}</td><td><div class="actions">${passwordAction}${statusAction}</div></td></tr>`;
  }).join("") || empty(5);
  $$(".password-open").forEach((button) => button.addEventListener("click", () => openPasswordDialog(button)));
  $$(".principal-toggle").forEach((button) => button.addEventListener("click", () => togglePrincipal(button)));
  return data;
}

async function loadModels() {
  const data = await api("/admin/model-catalog");
  state.models = data.items;
  $("#model-rows").innerHTML = data.items.map((item) => {
    const limits = item.default_limits || {};
    const limitText = [limits.context_window_tokens ? `上下文 ${Number(limits.context_window_tokens).toLocaleString()}` : "", limits.max_output_tokens ? `输出 ${Number(limits.max_output_tokens).toLocaleString()}` : ""].filter(Boolean).join(" · ") || "未设置";
    return `<tr><td><span class="row-title">${escapeHTML(item.display_name)}</span><span class="row-subtitle mono">${escapeHTML(item.model_name)}</span></td><td>${escapeHTML(item.provider)}</td><td><span class="row-title">${escapeHTML(limitText)}</span><span class="row-subtitle">${item.platform_credential_configured ? "平台凭据已配置" : "使用独立凭据"}</span></td><td>${badge(item.status)}</td><td><div class="actions"><button class="button secondary small model-edit" data-id="${item.model_catalog_id}">编辑</button>${item.status === "active" ? `<button class="button danger small model-remove" data-id="${item.model_catalog_id}">移除</button>` : `<button class="button secondary small model-enable" data-id="${item.model_catalog_id}">重新启用</button>`}</div></td></tr>`;
  }).join("") || empty(5);
  setOptions("#profile-model", data.items.filter((item) => item.status === "active"), "model_catalog_id", (item) => `${item.provider} / ${item.model_name}`);
  $$(".model-edit").forEach((button) => button.addEventListener("click", () => openModelEdit(button.dataset.id)));
  $$(".model-remove").forEach((button) => button.addEventListener("click", () => removeModel(button)));
  $$(".model-enable").forEach((button) => button.addEventListener("click", () => enableModel(button)));
  return data;
}

async function loadCredentials() {
  const data = await api("/admin/model-credentials");
  state.credentials = data.items;
  $("#credential-rows").innerHTML = data.items.map((item) => `<tr><td><span class="row-title">${escapeHTML(item.name)}</span><span class="row-subtitle mono">${escapeHTML(item.model_credential_id)}</span></td><td>${escapeHTML(item.provider)}</td><td>${item.secret_configured ? "已引用" : "未配置"}</td><td>${badge(item.status)}</td><td><div class="actions"><button class="button secondary small credential-edit" data-id="${item.model_credential_id}">编辑</button>${item.status === "active" ? `<button class="button danger small credential-remove" data-id="${item.model_credential_id}">移除</button>` : `<button class="button secondary small credential-enable" data-id="${item.model_credential_id}">重新启用</button>`}</div></td></tr>`).join("") || empty(5);
  setOptions("#profile-credential", data.items.filter((item) => item.status === "active"), "model_credential_id", (item) => `${item.provider} / ${item.name}`);
  $$(".credential-edit").forEach((button) => button.addEventListener("click", () => openCredentialEdit(button.dataset.id)));
  $$(".credential-remove").forEach((button) => button.addEventListener("click", () => removeCredential(button)));
  $$(".credential-enable").forEach((button) => button.addEventListener("click", () => enableCredential(button)));
  return data;
}

async function loadProfiles() {
  if (!state.tenants.length) await loadTenants();
  if (!state.models.length) await loadModels();
  if (!state.credentials.length) await loadCredentials();
  const tenantId = $("#profile-tenant").value;
  if (!tenantId) {
    state.profiles = []; state.agents = [];
    $("#profile-rows").innerHTML = empty(6, "请先创建可用租户");
    $("#profile-agent-rows").innerHTML = empty(5, "请先创建可用租户");
    return { items: [], total: 0 };
  }
  const [data, agents] = await Promise.all([api(`/tenants/${tenantId}/model-profiles`), api.all(`/tenants/${tenantId}/agents`)]);
  state.profiles = data.items; state.agents = agents.items;
  $("#profile-rows").innerHTML = data.items.map((item) => `<tr><td><span class="row-title">${escapeHTML(item.name)}</span><span class="row-subtitle mono">${escapeHTML(item.model_profile_id)}</span></td><td>${escapeHTML(modelLabel(item.model_catalog_id))}</td><td class="config-cell">${escapeHTML(JSON.stringify(item.parameter_config))}</td><td class="config-cell">${escapeHTML(JSON.stringify(item.limits))}</td><td>${badge(item.status)}</td><td><div class="actions"><button class="button secondary small profile-edit" data-id="${item.model_profile_id}">编辑</button>${item.status === "active" ? `<button class="button danger small profile-remove" data-id="${item.model_profile_id}">移除</button>` : `<button class="button secondary small profile-enable" data-id="${item.model_profile_id}">重新启用</button>`}</div></td></tr>`).join("") || empty(6);
  $("#profile-agent-rows").innerHTML = agents.items.map((item) => `<tr><td><span class="row-title">${escapeHTML(item.name)}</span><span class="row-subtitle mono">${escapeHTML(item.agent_app_id)}</span></td><td>${escapeHTML(profileLabel(item.model_profile_id))}</td><td>${badge(item.status)}</td><td>v${item.stable_config_version}</td><td><button class="button secondary small agent-profile-edit" data-id="${item.agent_app_id}">调整策略</button></td></tr>`).join("") || empty(5, "该租户暂无 Agent");
  $$(".profile-edit").forEach((button) => button.addEventListener("click", () => openProfileEdit(button.dataset.id)));
  $$(".profile-remove").forEach((button) => button.addEventListener("click", () => removeProfile(button)));
  $$(".profile-enable").forEach((button) => button.addEventListener("click", () => enableProfile(button)));
  $$(".agent-profile-edit").forEach((button) => button.addEventListener("click", () => openAgentProfileEdit(button.dataset.id)));
  return data;
}

async function loadRuntime() {
  const [data, pool] = await Promise.all([api("/admin/runtime-nodes"), api("/admin/worker-pool")]);
  state.nodes = data.items; state.workerPool = pool;
  $("#runtime-rows").innerHTML = data.items.map((item) => `<tr><td><span class="row-title">${escapeHTML(item.node_id)}</span><span class="row-subtitle">启动于 ${formatDate(item.started_at)}</span></td><td>${escapeHTML(item.role)}</td><td>${item.worker_concurrency}</td><td>${formatDate(item.heartbeat_at)}</td><td>${badge(item.health)}</td></tr>`).join("") || empty(5);
  $("#pool-desired").textContent = pool.desired_nodes; $("#pool-active").textContent = pool.active_nodes;
  $("#pool-draining").textContent = pool.draining_nodes; $("#pool-stale").textContent = pool.stale_nodes;
  $("#worker-pool-desired").value = pool.desired_nodes;
  $("#worker-pool-generation").textContent = `Generation ${pool.generation} · ${pool.scaler_mode}`;
  $("#worker-pool-state").textContent = pool.reconciling ? "调整中" : "已收敛";
  $("#worker-pool-state").className = `badge ${pool.reconciling ? "warning" : ""}`;
  return data;
}

async function loadUsage() {
  const data = await api.page("/admin/usage", "usage-rows", loadUsage);
  $("#usage-summary").textContent = `共 ${data.total} 条模型调用，${Number(data.summary.total_tokens).toLocaleString()} Token，预估成本 ${data.summary.estimated_cost}`;
  $("#usage-rows").innerHTML = data.items.map((item) => `<tr><td>${formatDate(item.occurred_at)}</td><td class="mono">${escapeHTML(item.tenant_id)}</td><td>${escapeHTML(item.model_provider)} / ${escapeHTML(item.model_name)}</td><td>${Number(item.total_tokens).toLocaleString()}</td><td>${escapeHTML(item.estimated_cost)}</td><td class="mono">${escapeHTML(item.request_id)}</td></tr>`).join("") || empty(6);
  return data;
}

async function loadAdapters() {
  const data = await api("/admin/channel-adapter-types");
  $("#adapter-rows").innerHTML = data.items.map((item) => `<tr><td><span class="row-title">${escapeHTML(item.display_name)}</span><span class="row-subtitle mono">${escapeHTML(item.channel_type)}</span></td><td>${escapeHTML(item.adapter_version)}</td><td class="mono">${escapeHTML(Object.keys(item.capabilities || {}).join("、") || "—")}</td><td>${badge(item.status)}</td><td><button class="button ${item.status === "active" ? "danger" : "secondary"} small adapter-toggle" data-id="${escapeHTML(item.channel_type)}" data-status="${item.status}">${item.status === "active" ? "停用" : "启用"}</button></td></tr>`).join("") || empty(5);
  $$(".adapter-toggle").forEach((button) => button.addEventListener("click", () => toggleAdapter(button)));
  return data;
}

async function loadAudit() {
  const data = await api.page("/admin/audit", "audit-rows", loadAudit);
  $("#audit-rows").innerHTML = data.items.map((item) => `<tr><td>${formatDate(item.occurred_at)}</td><td><span class="row-title">${escapeHTML(item.action)}</span>${item.reason ? `<span class="row-subtitle">${escapeHTML(item.reason)}</span>` : ""}</td><td>${escapeHTML(item.resource_type)}<span class="row-subtitle mono">${escapeHTML(item.resource_id)}</span></td><td class="mono">${escapeHTML(item.actor_subject)}</td><td>${badge(item.decision)}</td></tr>`).join("") || empty(5);
  return data;
}

async function loadOverview() {
  const [tenants, models, nodes, usage, health, ready] = await Promise.all([
    loadTenants(), loadModels(), loadRuntime(), loadUsage(), fetch("/health").then((response) => response.json()),
    fetch("/ready").then(async (response) => ({ ok: response.ok, body: await response.json() })),
  ]);
  $("#metric-tenants").textContent = tenants.total;
  $("#metric-models").textContent = models.items.filter((item) => item.status === "active").length;
  $("#metric-workers").textContent = nodes.items.filter((item) => item.health === "active" && ["worker", "api_worker"].includes(item.role)).length;
  $("#metric-tokens").textContent = compactNumber(usage.summary.total_tokens);
  $("#health-gateway").textContent = health.status === "ok" ? "正常" : "异常";
  $("#health-database").textContent = ready.body?.checks?.database === "ok" ? "正常" : "异常";
  $("#health-worker").textContent = ready.body?.checks?.worker_nodes > 0 ? "正常" : "暂无可用节点";
  $("#health-database").className = `badge ${ready.body?.checks?.database === "ok" ? "" : "error"}`;
  $("#health-worker").className = `badge ${ready.body?.checks?.worker_nodes > 0 ? "" : "warning"}`;
}

const loaders = { overview: loadOverview, tenants: loadTenants, accounts: loadAccounts, models: loadModels, credentials: loadCredentials, profiles: loadProfiles, runtime: loadRuntime, usage: loadUsage, adapters: loadAdapters, audit: loadAudit };

async function refresh(view, { quiet = false } = {}) {
  try { await withLoading(view, loaders[view]); if (!quiet) toast("数据已刷新"); }
  catch (error) { toast(error.message, "error"); throw error; }
}

async function mutate({ button, request, success, refreshView, dialog, form }) {
  if (button) button.disabled = true;
  try { await request(); }
  catch (error) { toast(error.message, "error"); return false; }
  finally { if (button) button.disabled = false; }
  dialog?.close(); form?.reset(); toast(success, "success");
  if (refreshView) {
    try { await loaders[refreshView](); }
    catch (error) { toast(`${success}，但列表刷新失败：${error.message}`, "warning"); }
  }
  return true;
}

function showView(name) {
  $$(".nav-item").forEach((item) => item.classList.toggle("active", item.dataset.view === name));
  $$(".view").forEach((item) => item.classList.toggle("active", item.id === name));
  $$(".nav-item").forEach((item) => { if (item.dataset.view === name) item.setAttribute("aria-current", "page"); else item.removeAttribute("aria-current"); });
  $("#page-title").textContent = titles[name] || "系统管理";
  refresh(name, { quiet: true }).catch(() => {});
}

async function removeTenant(button) {
  if (!confirm("确认移除该租户？其 Agent、IM 接入将停止使用；历史配置和审计记录仍会保留。")) return;
  await mutate({ button, request: () => api(`/tenants/${button.dataset.id}`, { method: "DELETE" }), success: "租户已移除", refreshView: "tenants" });
}

async function enableTenant(button) {
  await mutate({ button, request: () => api(`/tenants/${button.dataset.id}`, { method: "PATCH", body: JSON.stringify({ status: "active" }) }), success: "租户已重新启用", refreshView: "tenants" });
}

async function togglePrincipal(button) {
  const next = button.dataset.status === "active" ? "disabled" : "active";
  if (next === "disabled" && !confirm("确认停用该管理身份？")) return;
  await mutate({ button, request: () => api(`/admin/principals/${button.dataset.id}`, { method: "PATCH", body: JSON.stringify({ status: next }) }), success: next === "active" ? "身份已启用" : "身份已停用", refreshView: "accounts" });
}

async function removeModel(button) {
  if (!confirm("确认移除该模型目录项？已被活跃 Agent 使用时，系统会拒绝本次操作。")) return;
  await mutate({ button, request: () => api(`/admin/model-catalog/${button.dataset.id}`, { method: "DELETE" }), success: "模型已从可用目录移除", refreshView: "models" });
}

async function enableModel(button) {
  await mutate({ button, request: () => api(`/admin/model-catalog/${button.dataset.id}`, { method: "PATCH", body: JSON.stringify({ status: "active" }) }), success: "模型已重新启用", refreshView: "models" });
}

async function removeCredential(button) {
  if (!confirm("确认移除该模型凭据？已被活跃 Agent 使用时，系统会拒绝本次操作。")) return;
  await mutate({ button, request: () => api(`/admin/model-credentials/${button.dataset.id}`, { method: "DELETE" }), success: "模型凭据已移除", refreshView: "credentials" });
}

async function enableCredential(button) {
  await mutate({ button, request: () => api(`/admin/model-credentials/${button.dataset.id}`, { method: "PATCH", body: JSON.stringify({ status: "active" }) }), success: "模型凭据已重新启用", refreshView: "credentials" });
}

async function removeProfile(button) {
  const tenantId = $("#profile-tenant").value;
  if (!confirm("确认移除该模型策略？请先将引用它的 Agent 调整到其他策略。")) return;
  await mutate({ button, request: () => api(`/tenants/${tenantId}/model-profiles/${button.dataset.id}`, { method: "DELETE" }), success: "模型策略已移除", refreshView: "profiles" });
}

async function enableProfile(button) {
  const tenantId = $("#profile-tenant").value;
  await mutate({ button, request: () => api(`/tenants/${tenantId}/model-profiles/${button.dataset.id}`, { method: "PATCH", body: JSON.stringify({ status: "active" }) }), success: "模型策略已重新启用", refreshView: "profiles" });
}

async function toggleAdapter(button) {
  const next = button.dataset.status === "active" ? "disabled" : "active";
  await mutate({ button, request: () => api(`/admin/channel-adapter-types/${button.dataset.id}`, { method: "PATCH", body: JSON.stringify({ status: next }) }), success: `适配器已${next === "active" ? "启用" : "停用"}`, refreshView: "adapters" });
}

function openTenantEdit(tenantId) {
  const tenant = state.tenants.find((item) => item.tenant_id === tenantId);
  if (!tenant) return;
  const form = $("#tenant-edit-form");
  form.elements.tenant_id.value = tenant.tenant_id;
  form.elements.name.value = tenant.name;
  form.elements.isolation_mode.value = tenant.isolation_mode;
  form.elements.audit_policy.value = JSON.stringify(tenant.audit_policy || {}, null, 2);
  $("#tenant-edit-dialog").showModal();
}

function prepareModelCreate() {
  const form = $("#model-form");
  form.reset(); form.dataset.mode = "create";
  form.elements.model_catalog_id.value = ""; form.elements.provider.disabled = false;
  $("#model-dialog-title").textContent = "登记模型";
  $("#model-dialog-copy").textContent = "当前运行时支持百炼原生和 OpenAI 兼容接口。";
  $("#model-secret-help").textContent = "可留空并通过独立模型凭据绑定。";
  $("button[type='submit']", form).textContent = "登记模型";
}

function openModelEdit(modelId) {
  const model = state.models.find((item) => item.model_catalog_id === modelId);
  if (!model) return;
  const form = $("#model-form");
  form.reset(); form.dataset.mode = "edit";
  form.elements.model_catalog_id.value = model.model_catalog_id;
  form.elements.provider.value = model.provider; form.elements.provider.disabled = true;
  form.elements.model_name.value = model.model_name; form.elements.display_name.value = model.display_name;
  form.elements.max_output_tokens.value = model.default_limits?.max_output_tokens || "";
  form.elements.context_window_tokens.value = model.default_limits?.context_window_tokens || "";
  form.elements.platform_secret_ref.value = "";
  $("#model-dialog-title").textContent = "编辑模型";
  $("#model-dialog-copy").textContent = "修改模型名称和默认限制；已保存的密钥不会回显。";
  $("#model-secret-help").textContent = "留空表示保留当前 SecretRef；填写新值会执行轮换。";
  $("button[type='submit']", form).textContent = "保存模型";
  $("#model-dialog").showModal();
}

function prepareCredentialCreate() {
  const form = $("#credential-form");
  form.reset(); form.dataset.mode = "create";
  form.elements.model_credential_id.value = ""; form.elements.provider.disabled = false;
  $("#credential-dialog-title").textContent = "登记模型凭据";
  $("#credential-secret-help").textContent = "SecretRef 指向环境变量或外部密钥管理器。";
  $("button[type='submit']", form).textContent = "登记凭据";
}

function openCredentialEdit(credentialId) {
  const credential = state.credentials.find((item) => item.model_credential_id === credentialId);
  if (!credential) return;
  const form = $("#credential-form");
  form.reset(); form.dataset.mode = "edit";
  form.elements.model_credential_id.value = credential.model_credential_id;
  form.elements.provider.value = credential.provider; form.elements.provider.disabled = true;
  form.elements.name.value = credential.name; form.elements.secret_ref.value = "";
  $("#credential-dialog-title").textContent = "编辑模型凭据";
  $("#credential-secret-help").textContent = "留空表示保留当前 SecretRef；填写新值会执行轮换。";
  $("button[type='submit']", form).textContent = "保存凭据";
  $("#credential-dialog").showModal();
}

function prepareProfileCreate() {
  const form = $("#profile-form");
  form.reset(); form.dataset.mode = "create"; form.dataset.tenantId = "";
  form.elements.model_profile_id.value = ""; form.elements.tenant_id.disabled = false;
  if ($("#profile-tenant").value) form.elements.tenant_id.value = $("#profile-tenant").value;
  $("#profile-dialog-title").textContent = "创建租户模型策略";
  $("button[type='submit']", form).textContent = "创建策略";
}

function openProfileEdit(profileId) {
  const profile = state.profiles.find((item) => item.model_profile_id === profileId);
  if (!profile) return;
  const form = $("#profile-form");
  const parameters = profile.parameter_config || {}; const limits = profile.limits || {};
  form.reset(); form.dataset.mode = "edit"; form.dataset.tenantId = profile.tenant_id;
  form.elements.model_profile_id.value = profile.model_profile_id;
  form.elements.tenant_id.value = profile.tenant_id; form.elements.tenant_id.disabled = true;
  form.elements.model_catalog_id.value = profile.model_catalog_id;
  form.elements.credential_id.value = profile.credential_id || "";
  form.elements.name.value = profile.name;
  form.elements.temperature.value = parameters.temperature ?? 0.2;
  form.elements.enable_thinking.value = String(parameters.enable_thinking ?? "");
  form.elements.timeout_seconds.value = parameters.timeout_seconds ?? 120;
  form.elements.max_output_tokens.value = parameters.max_output_tokens ?? 4096;
  form.elements.context_window_tokens.value = parameters.context_window_tokens ?? 32768;
  form.elements.daily_tokens.value = limits.daily_tokens || ""; form.elements.daily_calls.value = limits.daily_calls || "";
  $("#profile-dialog-title").textContent = "编辑租户模型策略";
  $("button[type='submit']", form).textContent = "保存策略";
  $("#profile-dialog").showModal();
}

function openAgentProfileEdit(agentId) {
  const agent = state.agents.find((item) => item.agent_app_id === agentId);
  if (!agent) return;
  setOptions("#agent-profile-select", state.profiles.filter((item) => item.status === "active"), "model_profile_id", (item) => `${item.name} / ${modelLabel(item.model_catalog_id)}`);
  const form = $("#agent-profile-form");
  form.elements.agent_app_id.value = agent.agent_app_id; form.elements.tenant_id.value = agent.tenant_id;
  form.elements.agent_name.value = agent.name; form.elements.model_profile_id.value = agent.model_profile_id || "";
  $("#agent-profile-dialog").showModal();
}

function openPasswordDialog(button) {
  const form = $("#password-form");
  form.elements.principal_id.value = button.dataset.id;
  form.elements.username.value = (button.dataset.subject || "").replace(/^tenant-console:/, "");
  $("#password-dialog").showModal();
}

async function enterConsole() {
  const actor = await api("/admin/me");
  if (!actor.roles.includes("platform_admin")) {
    if (!state.token) await api("/auth/logout", { method: "POST" });
    throw new Error("该账号不是系统管理员，请使用租户管理入口。");
  }
  state.actor = actor; $("#actor-name").textContent = actor.subject;
  $("#login-shell").hidden = true; $("#app-shell").hidden = false; setLoginError();
  await withLoading("overview", loadOverview);
}

async function logout() {
  if (!state.token) {
    try { await api("/auth/logout", { method: "POST" }); } catch { /* Session may already expire. */ }
  }
  state.token = ""; state.actor = null;
  $("#app-shell").hidden = true; $("#login-shell").hidden = false; $("#login-form").reset();
}

$("#login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = event.currentTarget; const button = $("button[type='submit']", form);
  button.disabled = true; setLoginError();
  try {
    const data = new FormData(form); state.token = "";
    await api("/auth/login", { method: "POST", body: JSON.stringify({ username: data.get("username"), password: data.get("password") }) });
    await enterConsole();
  } catch (error) { setLoginError(error.message); }
  finally { button.disabled = false; }
});

$("#token-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = event.currentTarget; const button = $("button[type='submit']", form);
  button.disabled = true; setLoginError();
  try { state.token = String(new FormData(form).get("token") || "").trim(); await enterConsole(); }
  catch (error) { state.token = ""; setLoginError(error.message); }
  finally { button.disabled = false; }
});

$("#logout").addEventListener("click", logout);
$$('[data-open]').forEach((button) => button.addEventListener("click", () => {
  if (button.dataset.open === "model-dialog") prepareModelCreate();
  if (button.dataset.open === "credential-dialog") prepareCredentialCreate();
  if (button.dataset.open === "profile-dialog") prepareProfileCreate();
  $(`#${button.dataset.open}`).showModal();
}));
$$('[data-close]').forEach((button) => button.addEventListener("click", () => button.closest("dialog").close()));
$$('.nav-item').forEach((button) => button.addEventListener("click", () => showView(button.dataset.view)));
$$('[data-view-jump]').forEach((button) => button.addEventListener("click", () => showView(button.dataset.viewJump)));
$$('[data-refresh]').forEach((button) => button.addEventListener("click", () => refresh(button.dataset.refresh).catch(() => {})));
$("#profile-tenant").addEventListener("change", () => refresh("profiles", { quiet: true }).catch(() => {}));

$("#worker-pool-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = event.currentTarget; const desired = Number(new FormData(form).get("desired_nodes"));
  const current = state.workerPool;
  if (!current) { toast("请先刷新 Worker Pool 状态", "error"); return; }
  if (desired < current.desired_nodes && !confirm(`确认将 Worker 从 ${current.desired_nodes} 个缩减到 ${desired} 个？多余节点会先排空当前任务。`)) return;
  await mutate({ button: $("button[type='submit']", form), request: () => api("/admin/worker-pool", { method: "PUT", body: JSON.stringify({ desired_nodes: desired, expected_generation: current.generation }) }), success: "Worker Pool 期望容量已更新", refreshView: "runtime" });
});

$("#tenant-form").addEventListener("submit", async (event) => {
  event.preventDefault(); const form = event.currentTarget; const data = new FormData(form);
  await mutate({ button: $("button[type='submit']", form), request: () => api("/tenants", { method: "POST", body: JSON.stringify({ name: data.get("name"), isolation_mode: data.get("isolation_mode") }) }), success: "租户已创建", refreshView: "tenants", dialog: form.closest("dialog"), form });
});

$("#tenant-edit-form").addEventListener("submit", async (event) => {
  event.preventDefault(); const form = event.currentTarget; const data = new FormData(form);
  let auditPolicy;
  try { auditPolicy = parseJSONObject(data.get("audit_policy"), "审计策略"); }
  catch (error) { toast(error.message, "error"); return; }
  await mutate({ button: $("button[type='submit']", form), request: () => api(`/tenants/${data.get("tenant_id")}`, { method: "PATCH", body: JSON.stringify({ name: data.get("name"), isolation_mode: data.get("isolation_mode"), audit_policy: auditPolicy }) }), success: "租户配置已更新", refreshView: "tenants", dialog: form.closest("dialog"), form });
});

$("#account-form").addEventListener("submit", async (event) => {
  event.preventDefault(); const form = event.currentTarget; const data = new FormData(form);
  await mutate({ button: $("button[type='submit']", form), request: () => api("/admin/tenant-accounts", { method: "POST", body: JSON.stringify({ tenant_id: data.get("tenant_id"), username: String(data.get("username")).trim().toLowerCase(), password: data.get("password") }) }), success: "租户管理员账号已创建", refreshView: "accounts", dialog: form.closest("dialog"), form });
});

$("#password-form").addEventListener("submit", async (event) => {
  event.preventDefault(); const form = event.currentTarget; const data = new FormData(form);
  await mutate({ button: $("button[type='submit']", form), request: () => api(`/admin/principals/${data.get("principal_id")}/password`, { method: "PUT", body: JSON.stringify({ username: String(data.get("username")).trim().toLowerCase(), password: data.get("password") }) }), success: "登录密码已更新", dialog: form.closest("dialog"), form });
});

$("#model-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = event.currentTarget; const data = new FormData(form);
  const secretRef = String(data.get("platform_secret_ref") || "").trim(); const defaultLimits = {};
  if (data.get("max_output_tokens")) defaultLimits.max_output_tokens = Number(data.get("max_output_tokens"));
  if (data.get("context_window_tokens")) defaultLimits.context_window_tokens = Number(data.get("context_window_tokens"));
  const editing = form.dataset.mode === "edit";
  const payload = { model_name: data.get("model_name"), display_name: data.get("display_name"), default_limits: defaultLimits, ...(secretRef ? { platform_secret_ref: secretRef } : {}) };
  if (!editing) Object.assign(payload, { provider: data.get("provider"), capabilities: { text: true } });
  const path = editing ? `/admin/model-catalog/${data.get("model_catalog_id")}` : "/admin/model-catalog";
  await mutate({ button: $("button[type='submit']", form), request: () => api(path, { method: editing ? "PATCH" : "POST", body: JSON.stringify(payload) }), success: editing ? "模型配置已更新" : "模型已登记", refreshView: "models", dialog: form.closest("dialog"), form });
});

$("#credential-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = event.currentTarget; const data = new FormData(form);
  const secretRef = String(data.get("secret_ref") || "").trim(); const editing = form.dataset.mode === "edit";
  if (!editing && !secretRef) { toast("新凭据必须填写 SecretRef", "error"); return; }
  const payload = { name: data.get("name"), ...(secretRef ? { secret_ref: secretRef } : {}) };
  if (!editing) payload.provider = data.get("provider");
  const path = editing ? `/admin/model-credentials/${data.get("model_credential_id")}` : "/admin/model-credentials";
  await mutate({ button: $("button[type='submit']", form), request: () => api(path, { method: editing ? "PATCH" : "POST", body: JSON.stringify(payload) }), success: editing ? "模型凭据已更新" : "模型凭据已登记", refreshView: "credentials", dialog: form.closest("dialog"), form });
});

$("#profile-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = event.currentTarget; const data = new FormData(form); const limits = {};
  if (data.get("daily_tokens")) limits.daily_tokens = Number(data.get("daily_tokens"));
  if (data.get("daily_calls")) limits.daily_calls = Number(data.get("daily_calls"));
  const payload = {
    name: data.get("name"), model_catalog_id: data.get("model_catalog_id"), credential_id: data.get("credential_id"),
    parameter_config: {
      temperature: Number(data.get("temperature")), max_output_tokens: Number(data.get("max_output_tokens")),
      context_window_tokens: Number(data.get("context_window_tokens")), timeout_seconds: Number(data.get("timeout_seconds")),
    }, limits,
  };
  if (data.get("enable_thinking") !== "") payload.parameter_config.enable_thinking = data.get("enable_thinking") === "true";
  const editing = form.dataset.mode === "edit";
  const tenantId = editing ? form.dataset.tenantId : data.get("tenant_id");
  const path = editing ? `/tenants/${tenantId}/model-profiles/${data.get("model_profile_id")}` : `/tenants/${tenantId}/model-profiles`;
  await mutate({ button: $("button[type='submit']", form), request: () => api(path, { method: editing ? "PATCH" : "POST", body: JSON.stringify(payload) }), success: editing ? "模型策略已更新" : "租户模型策略已创建", refreshView: "profiles", dialog: form.closest("dialog"), form });
});

$("#agent-profile-form").addEventListener("submit", async (event) => {
  event.preventDefault(); const form = event.currentTarget; const data = new FormData(form);
  await mutate({ button: $("button[type='submit']", form), request: () => api(`/tenants/${data.get("tenant_id")}/agents/${data.get("agent_app_id")}`, { method: "PATCH", body: JSON.stringify({ model_profile_id: data.get("model_profile_id") }) }), success: "Agent 模型策略已更新", refreshView: "profiles", dialog: form.closest("dialog"), form });
});

(async () => {
  try { await enterConsole(); }
  catch (error) { if (error.status !== 401) setLoginError(error.message); }
})();
