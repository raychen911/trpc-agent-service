const state = {
  token: "", actor: null, tenants: [], agents: [], adapters: [], bindings: [],
  mcpConnections: [], skills: [], capabilitiesLoaded: false,
};
const titles = {
  overview: "工作空间概览", agents: "Agent", channels: "IM 接入",
  mcp: "MCP 接入", knowledge: "知识库", failures: "投递恢复",
};
const {
  $, $$, escapeHTML, createApi, toast, setLoginError, badge, empty,
  formatDate, setOptions, statusLabel, withLoading,
} = window.ConsoleUI;
const api = createApi(state);

function tenantId() {
  return state.tenants[0]?.tenant_id || "";
}

function currentRoles() {
  return state.actor?.tenant_roles?.[tenantId()] || [];
}

function agentName(agentId) {
  return state.agents.find((item) => item.agent_app_id === agentId)?.name || agentId;
}

function renderCapabilityOptions() {
  $("#skill-options").innerHTML = state.skills.map((item) =>
    `<label title="${escapeHTML(item.description)}"><input name="skills" type="checkbox" value="${escapeHTML(item.name)}">${escapeHTML(item.name)}</label>`
  ).join("") || "<small>平台当前没有可授权的 Skill。</small>";
  const entries = state.mcpConnections.flatMap((connection) =>
    (connection.tool_catalog || []).map((tool) => ({ connection, tool })));
  $("#mcp-tool-options").innerHTML = entries.map(({ connection, tool }) => {
    const risk = Number(tool.risk_level) === 0 ? 0 : 2;
    const interaction = risk === 0
      ? '<span class="badge">只读直通</span>'
      : '<span class="badge warning">写操作确认</span>';
    return `<label title="${escapeHTML(tool.description || "")}"><input name="mcp_tools" type="checkbox" value="${escapeHTML(tool.name)}" data-connection="${escapeHTML(connection.connection_id)}" data-risk="${risk}">${escapeHTML(connection.name)} / ${escapeHTML(tool.remote_name || tool.name)} ${interaction}</label>`;
  }).join("") || "<small>请先新增并成功刷新 MCP 连接。</small>";
}

async function loadCapabilityCatalog({ force = false } = {}) {
  if (state.capabilitiesLoaded && !force) return;
  const [skills, connections] = await Promise.all([
    api(`/tenants/${tenantId()}/skills`),
    api.all(`/tenants/${tenantId()}/mcp-connections`),
  ]);
  state.skills = skills.items;
  state.mcpConnections = connections.items;
  state.capabilitiesLoaded = true;
  renderCapabilityOptions();
}

async function loadOwnedTenants() {
  const ids = Object.entries(state.actor?.tenant_roles || {})
    .filter(([, roles]) => roles.includes("tenant_admin"))
    .map(([id]) => id);
  if (ids.length !== 1) throw new Error("租户管理员账号必须且只能关联一个租户，请联系系统管理员。");
  state.tenants = await Promise.all(ids.map((id) => api(`/tenants/${id}`)));
  $("#tenant-header-name").textContent = state.tenants[0].name;
}

async function loadAgents() {
  const data = await api.all(`/tenants/${tenantId()}/agents`);
  state.agents = data.items;
  $("#agent-rows").innerHTML = data.items.map((item) => {
    const bases = item.knowledge_config?.knowledge_base_names || [];
    const runtimeStatus = item.model_profile_id
      ? badge(item.status)
      : '<span class="badge warning">待系统管理员配置模型</span>';
    return `<tr><td><span class="row-title">${escapeHTML(item.name)}</span><span class="row-subtitle mono">${escapeHTML(item.agent_app_id)}</span></td><td>${runtimeStatus}</td><td>${escapeHTML(bases.join("、") || "未配置")}</td><td>v${item.stable_config_version}</td><td><div class="actions"><button class="button secondary small agent-edit" data-id="${item.agent_app_id}">编辑</button><button class="button ${item.status === "active" ? "danger" : "secondary"} small agent-toggle" data-id="${item.agent_app_id}" data-status="${item.status}">${item.status === "active" ? "停用" : "启用"}</button></div></td></tr>`;
  }).join("") || empty(5);
  $$(".agent-edit").forEach((button) => button.addEventListener("click", () => editAgent(button.dataset.id)));
  $$(".agent-toggle").forEach((button) => button.addEventListener("click", () => toggleAgent(button)));
  setOptions("#knowledge-agent", data.items.filter((item) => item.status === "active" && item.model_profile_id), "agent_app_id", (item) => item.name);
  loadKnowledgeBases();
  return data;
}

function editAgent(agentId) {
  const agent = state.agents.find((item) => item.agent_app_id === agentId);
  if (!agent) return;
  const form = $("#agent-form");
  form.reset();
  form.elements.agent_app_id.value = agent.agent_app_id;
  form.elements.name.value = agent.name;
  form.elements.instruction.value = agent.application_config?.instruction || "";
  form.elements.knowledge_bases.value = (agent.knowledge_config?.knowledge_base_names || []).join(", ");
  form.elements.http_allowed_hosts.value = (agent.tool_permissions?.http_allowed_hosts || []).join(", ");
  renderCapabilityOptions();
  const grants = Array.isArray(agent.tool_permissions?.grants)
    ? agent.tool_permissions.grants : [];
  const enabled = new Set([
    ...(agent.tool_permissions?.allowlist || []),
    ...grants.filter((grant) => ["tool", "workspace"].includes(grant.kind))
      .map((grant) => grant.name),
  ]);
  $$('input[name="tools"]', form).forEach((input) => { input.checked = enabled.has(input.value); });
  const enabledSkills = new Set(grants.filter((grant) => grant.kind === "skill")
    .map((grant) => grant.name));
  $$('input[name="skills"]', form).forEach((input) => {
    input.checked = enabledSkills.has(input.value);
  });
  const enabledMcp = new Set(grants.filter((grant) => grant.kind === "mcp")
    .flatMap((grant) => (grant.resources || []).map((resource) => `${resource}|${grant.name}`)));
  $$('input[name="mcp_tools"]', form).forEach((input) => {
    input.checked = enabledMcp.has(`${input.dataset.connection}|${input.value}`);
  });
  $("#agent-dialog-title").textContent = "编辑 Agent";
  $("#agent-dialog").showModal();
}

async function toggleAgent(button) {
  const next = button.dataset.status === "active" ? "disabled" : "active";
  if (next === "disabled" && !confirm("确认停用该 Agent？已绑定的 IM 将无法继续执行请求。")) return;
  await mutate({
    button,
    request: () => api(`/tenants/${tenantId()}/agents/${button.dataset.id}`, {
      method: "PATCH", body: JSON.stringify({ status: next }),
    }),
    success: `Agent 已${next === "active" ? "启用" : "停用"}`,
    refreshView: "agents",
  });
}

function schemaInputs(schema, kind, values = {}, configured = []) {
  const required = new Set(schema.required || []);
  return Object.entries(schema.properties || {}).map(([name, spec]) => {
    const value = kind === "secret" ? "" : (values[name] ?? spec.default ?? "");
    const placeholder = kind === "secret" && configured.includes(name)
      ? "已配置，留空则不修改"
      : (spec.description || "");
    return `
    <label class="field">${escapeHTML(spec.title || name)}
      <input name="${escapeHTML(kind)}:${escapeHTML(name)}" type="${kind === "secret" ? "password" : "text"}"
        ${required.has(name) && (kind !== "secret" || !configured.includes(name)) ? "required" : ""}
        value="${escapeHTML(value)}"
        placeholder="${escapeHTML(placeholder)}"
        maxlength="${spec.maxLength || 500}" autocomplete="off">
      ${spec.description ? `<small>${escapeHTML(spec.description)}</small>` : ""}
    </label>`;
  }).join("");
}

async function loadChannels() {
  if (!state.agents.length) await loadAgents();
  const [catalog, bindings] = await Promise.all([
    api(`/tenants/${tenantId()}/channel-adapter-types`),
    api.all(`/tenants/${tenantId()}/channel-bindings`),
  ]);
  state.adapters = catalog.items;
  state.bindings = bindings.items;
  $("#channel-cards").innerHTML = catalog.items.map((adapter) => {
    const current = bindings.items.find((item) => item.channel_type === adapter.channel_type);
    // Only executable Agents can back an IM. The API repeats this validation so
    // future clients and future adapters cannot bypass the readiness boundary.
    const agentOptions = state.agents.filter((item) => item.status === "active" && item.model_profile_id).map((item) =>
      `<option value="${item.agent_app_id}" ${current?.agent_app_id === item.agent_app_id ? "selected" : ""}>${escapeHTML(item.name)}</option>`).join("");
    return `<article class="adapter-card"><div class="adapter-card-head"><div><h3>${escapeHTML(adapter.display_name)}</h3><p>${escapeHTML(adapter.channel_type)} · ${escapeHTML(adapter.adapter_version)}</p></div><span class="adapter-mark">${escapeHTML(adapter.display_name.slice(0, 1))}</span></div><form class="channel-form" data-channel="${escapeHTML(adapter.channel_type)}" data-binding="${escapeHTML(current?.binding_id || "")}"><label class="field">绑定 Agent<select name="agent_app_id" required>${agentOptions}</select></label>${schemaInputs(adapter.config_schema, "config", current?.account_config || {})}${schemaInputs(adapter.secret_schema, "secret", {}, current?.secret_fields || [])}<button class="button" type="submit">${current ? "更新接入配置" : "启用接入"}</button></form></article>`;
  }).join("") || "<p class=\"inline-notice\">平台尚未启用可用的 IM 适配器。</p>";
  $$(".channel-form").forEach((form) => form.addEventListener("submit", saveChannel));
  $("#binding-rows").innerHTML = bindings.items.map((item) => `<tr><td><span class="row-title">${escapeHTML(item.channel_type)}</span><span class="row-subtitle mono">${escapeHTML(item.binding_public_id)}</span></td><td>${escapeHTML(agentName(item.agent_app_id))}</td><td class="mono">${escapeHTML(JSON.stringify(item.account_config))}</td><td>${escapeHTML(item.secret_fields.join("、") || "—")}</td><td>${badge(item.status)}</td><td><button class="button danger small binding-disable" data-id="${item.binding_id}">停用</button></td></tr>`).join("") || empty(6);
  $$(".binding-disable").forEach((button) => button.addEventListener("click", () => disableBinding(button)));
  return bindings;
}

async function saveChannel(event) {
  event.preventDefault();
  const formElement = event.currentTarget;
  const form = new FormData(formElement);
  const accountConfig = {};
  const secretValues = {};
  for (const [key, value] of form.entries()) {
    if (key.startsWith("config:")) accountConfig[key.slice(7)] = value;
    if (key.startsWith("secret:") && value) secretValues[key.slice(7)] = value;
  }
  const bindingId = formElement.dataset.binding;
  const payload = {
    agent_app_id: form.get("agent_app_id"), account_config: accountConfig,
    ...(Object.keys(secretValues).length ? { secret_values: secretValues } : {}),
    ...(bindingId ? { status: "active" } : { channel_type: formElement.dataset.channel }),
  };
  await mutate({
    button: $("button[type='submit']", formElement),
    request: () => api(bindingId ? `/tenants/${tenantId()}/channel-bindings/${bindingId}` : `/tenants/${tenantId()}/channel-bindings`, {
      method: bindingId ? "PATCH" : "POST", body: JSON.stringify(payload),
    }),
    success: "IM 接入配置已保存",
    refreshView: "channels",
  });
}

async function disableBinding(button) {
  if (!confirm("确认停用该 IM 接入？")) return;
  await mutate({ button, request: () => api(`/tenants/${tenantId()}/channel-bindings/${button.dataset.id}`, { method: "DELETE" }), success: "IM 接入已停用", refreshView: "channels" });
}

async function loadMcpConnections() {
  await loadCapabilityCatalog({ force: true });
  $("#mcp-rows").innerHTML = state.mcpConnections.map((item) => {
    const tools = (item.tool_catalog || []).map((tool) => tool.remote_name || tool.name);
    const authorizedAgents = state.agents.filter((agent) =>
      (agent.tool_permissions?.grants || []).some((grant) =>
        grant.kind === "mcp" && (grant.resources || []).includes(item.connection_id)))
      .map((agent) => agent.name);
    const authorization = authorizedAgents.length
      ? `已授权：${authorizedAgents.join("、")}` : "尚未授权给 Agent";
    const credential = item.auth_type === "none"
      ? "无需认证" : (item.credential_configured ? "Bearer · 已配置" : "Bearer · 未配置");
    return `<tr><td><span class="row-title">${escapeHTML(item.name)}</span><span class="row-subtitle mono">${escapeHTML(item.connection_id)}</span></td><td class="mono">${escapeHTML(item.endpoint_url)}</td><td>${escapeHTML(credential)}</td><td><span class="row-title">${tools.length} 个</span><span class="row-subtitle">${escapeHTML(tools.join("、") || (item.last_error_code ? `刷新失败：${item.last_error_code}` : "尚未刷新"))}</span><span class="row-subtitle">${escapeHTML(authorization)}</span></td><td>${badge(item.status)}</td><td><div class="actions"><button class="button secondary small mcp-edit" data-id="${item.connection_id}">编辑</button><button class="button secondary small mcp-refresh" data-id="${item.connection_id}">测试并刷新</button><button class="button secondary small mcp-grant" data-id="${item.connection_id}">配置 Agent</button><button class="button danger small mcp-disable" data-id="${item.connection_id}">停用</button></div></td></tr>`;
  }).join("") || empty(6);
  $$(".mcp-edit").forEach((button) => button.addEventListener("click", () => editMcp(button.dataset.id)));
  $$(".mcp-refresh").forEach((button) => button.addEventListener("click", () => refreshMcp(button)));
  $$(".mcp-grant").forEach((button) => button.addEventListener("click", () => configureMcpGrant(button.dataset.id)));
  $$(".mcp-disable").forEach((button) => button.addEventListener("click", () => disableMcp(button)));
  return { items: state.mcpConnections, total: state.mcpConnections.length };
}

function configureMcpGrant(connectionId) {
  if (!state.agents.length) {
    showView("agents");
    toast("请先创建 Agent，再为它授权 MCP 工具", "warning");
    return;
  }
  if (state.agents.length === 1) {
    editAgent(state.agents[0].agent_app_id);
    toast("请在 MCP 工具中勾选所需能力并保存 Agent", "warning");
    return;
  }
  showView("agents");
  toast(`请选择目标 Agent 并点击“编辑”，再配置连接 ${connectionId.slice(0, 8)} 的工具`, "warning");
}

function editMcp(connectionId) {
  const connection = state.mcpConnections.find((item) => item.connection_id === connectionId);
  if (!connection) return;
  const form = $("#mcp-form");
  form.reset();
  form.elements.connection_id.value = connection.connection_id;
  form.elements.name.value = connection.name;
  form.elements.endpoint_url.value = connection.endpoint_url;
  form.elements.auth_type.value = connection.auth_type;
  form.elements.timeout_seconds.value = connection.timeout_seconds;
  $("#mcp-dialog-title").textContent = "编辑 MCP 连接";
  $("#mcp-dialog").showModal();
}

async function refreshMcp(button) {
  await mutate({
    button,
    request: () => api(`/tenants/${tenantId()}/mcp-connections/${button.dataset.id}/refresh`, { method: "POST" }),
    success: "MCP 连接已验证；请继续配置目标 Agent 的工具授权",
    refreshView: "mcp",
  });
}

async function disableMcp(button) {
  if (!confirm("确认停用该 MCP 连接？关联工具将不再提供给 Agent。")) return;
  await mutate({
    button,
    request: () => api(`/tenants/${tenantId()}/mcp-connections/${button.dataset.id}`, { method: "DELETE" }),
    success: "MCP 连接已停用",
    refreshView: "mcp",
  });
}

function loadKnowledgeBases() {
  const selected = $("#knowledge-agent").value;
  const agent = state.agents.find((item) => item.agent_app_id === selected);
  const names = agent?.knowledge_config?.knowledge_base_names || [];
  $("#knowledge-base").innerHTML = names.map((name) => `<option>${escapeHTML(name)}</option>`).join("");
}

async function loadKnowledge() {
  if (!state.agents.length) await loadAgents();
  loadKnowledgeBases();
  const agentId = $("#knowledge-agent").value;
  const base = $("#knowledge-base").value;
  if (!agentId || !base) { $("#knowledge-rows").innerHTML = empty(5, "请先为 Agent 配置知识库名称"); return { items: [], total: 0 }; }
  const data = await api(`/tenants/${tenantId()}/knowledge-bases/${encodeURIComponent(base)}/documents?agent_app_id=${agentId}`);
  $("#knowledge-rows").innerHTML = data.items.map((item) => `<tr><td><span class="row-title">${escapeHTML(item.filename)}</span><span class="row-subtitle mono">${escapeHTML(item.document_id)}</span></td><td>v${item.version}</td><td>${item.chunk_count}</td><td>${badge(item.status)}</td><td><div class="actions"><button class="button secondary small knowledge-replace" data-id="${item.document_id}">替换</button><button class="button danger small knowledge-delete" data-id="${item.document_id}">删除</button></div></td></tr>`).join("") || empty(5);
  $$(".knowledge-replace").forEach((button) => button.addEventListener("click", () => openKnowledgeDialog(button.dataset.id)));
  $$(".knowledge-delete").forEach((button) => button.addEventListener("click", () => deleteKnowledge(button)));
  return data;
}

function openKnowledgeDialog(documentId = "") {
  const form = $("#knowledge-form");
  form.reset();
  form.elements.document_id.value = documentId;
  $("#knowledge-dialog-title").textContent = documentId ? "替换知识文档" : "上传知识文档";
  $("#knowledge-dialog").showModal();
}

async function deleteKnowledge(button) {
  if (!confirm("确认删除该知识文档？旧版本将保留审计记录，但不会继续参与检索。")) return;
  const agentId = $("#knowledge-agent").value;
  const base = $("#knowledge-base").value;
  await mutate({ button, request: () => api(`/tenants/${tenantId()}/knowledge-bases/${encodeURIComponent(base)}/documents/${button.dataset.id}?agent_app_id=${agentId}`, { method: "DELETE" }), success: "知识文档已删除", refreshView: "knowledge" });
}

async function loadFailures() {
  const data = await api.page(`/tenants/${tenantId()}/delivery-failures`, "failure-rows", loadFailures);
  $("#failure-rows").innerHTML = data.items.map((item) => `<tr><td>${formatDate(item.updated_at)}</td><td class="mono">${escapeHTML(item.binding_id || "—")}</td><td><span class="row-title">${escapeHTML(item.last_error_code || "未分类")}</span><span class="row-subtitle">${escapeHTML(item.last_error_summary || "没有安全摘要")}</span></td><td>${badge(item.status)}</td><td>${["DEAD_LETTER", "UNKNOWN"].includes(item.status) ? `<button class="button secondary small failure-replay" data-id="${escapeHTML(item.outbox_id)}">重新投递</button>` : "等待自动重试"}</td></tr>`).join("") || empty(5);
  $$(".failure-replay").forEach((button) => button.addEventListener("click", () => replayFailure(button)));
  return data;
}

async function replayFailure(button) {
  const form = $("#replay-form");
  form.reset();
  form.elements.outbox_id.value = button.dataset.id;
  $("#replay-dialog").showModal();
}

async function loadOverview() {
  const tenant = await api(`/tenants/${tenantId()}`);
  // Agent options are prerequisites for both channel and knowledge views.
  const agents = await loadAgents();
  const [bindings, failures] = await Promise.all([loadChannels(), loadFailures()]);
  let documents = { total: 0 };
  try { documents = await loadKnowledge(); } catch { /* A tenant may not have a knowledge backend yet. */ }
  $("#metric-agents").textContent = agents.total;
  $("#metric-channels").textContent = bindings.items.filter((item) => item.status === "active").length;
  $("#metric-documents").textContent = documents.total;
  $("#metric-failures").textContent = failures.total;
  $("#tenant-name").textContent = tenant.name;
  $("#tenant-isolation").textContent = tenant.isolation_mode;
  $("#tenant-role").textContent = currentRoles().includes("tenant_admin") ? "租户管理员" : "—";
  const status = $("#tenant-status");
  status.textContent = statusLabel(tenant.status);
  status.className = `badge ${String(tenant.status || "unknown").toLowerCase()}`;
}

const loaders = {
  overview: loadOverview, agents: loadAgents, channels: loadChannels,
  mcp: loadMcpConnections, knowledge: loadKnowledge, failures: loadFailures,
};

async function refresh(view, { quiet = false } = {}) {
  try { await withLoading(view, loaders[view]); if (!quiet) toast("数据已刷新"); }
  catch (error) { toast(error.message, "error"); throw error; }
}

async function mutate({ button, request, success, refreshView, dialog, form }) {
  if (button) button.disabled = true;
  try { await request(); }
  catch (error) { toast(error.message, "error"); return false; }
  finally { if (button) button.disabled = false; }
  dialog?.close();
  form?.reset();
  toast(success, "success");
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
  $("#page-title").textContent = titles[name] || "租户工作台";
  refresh(name, { quiet: true }).catch(() => {});
}

async function enterConsole() {
  const actor = await api("/admin/me");
  if (!Object.keys(actor.tenant_roles || {}).length) {
    await api("/auth/logout", { method: "POST" });
    throw new Error("该账号没有租户管理权限，请使用系统管理入口。");
  }
  state.actor = actor;
  $("#actor-name").textContent = actor.subject;
  await loadOwnedTenants();
  await loadCapabilityCatalog();
  $("#login-shell").hidden = true;
  $("#app-shell").hidden = false;
  setLoginError();
  await withLoading("overview", loadOverview);
}

async function logout() {
  try { await api("/auth/logout", { method: "POST" }); } catch { /* Session may already expire. */ }
  state.actor = null;
  $("#app-shell").hidden = true;
  $("#login-shell").hidden = false;
  $("#login-form").reset();
}

$("#login-form").addEventListener("submit", async (event) => {
  event.preventDefault(); const form = event.currentTarget; const button = $("button[type='submit']", form);
  button.disabled = true; setLoginError();
  try { const data = new FormData(form); await api("/auth/login", { method: "POST", body: JSON.stringify({ username: data.get("username"), password: data.get("password") }) }); await enterConsole(); }
  catch (error) { setLoginError(error.message); }
  finally { button.disabled = false; }
});

$("#logout").addEventListener("click", logout);
$$('.nav-item').forEach((button) => button.addEventListener("click", () => showView(button.dataset.view)));
$$('[data-view-jump]').forEach((button) => button.addEventListener("click", () => showView(button.dataset.viewJump)));
$$('[data-refresh]').forEach((button) => button.addEventListener("click", () => refresh(button.dataset.refresh).catch(() => {})));
$$('[data-open]').forEach((button) => button.addEventListener("click", async () => {
  if (button.dataset.open === "agent-dialog") {
    try { await loadCapabilityCatalog(); }
    catch (error) { toast(`能力目录加载失败：${error.message}`, "error"); return; }
    $("#agent-form").reset();
    $("#agent-form").elements.agent_app_id.value = "";
    $("#agent-dialog-title").textContent = "创建 Agent";
    renderCapabilityOptions();
  }
  if (button.dataset.open === "mcp-dialog") {
    $("#mcp-form").reset();
    $("#mcp-form").elements.connection_id.value = "";
    $("#mcp-dialog-title").textContent = "新增 MCP 连接";
  }
  if (button.dataset.open === "knowledge-dialog") { openKnowledgeDialog(); return; }
  $(`#${button.dataset.open}`).showModal();
}));
$$('[data-close]').forEach((button) => button.addEventListener("click", () => button.closest("dialog").close()));
$("#knowledge-agent").addEventListener("change", () => { loadKnowledgeBases(); refresh("knowledge", { quiet: true }).catch(() => {}); });
$("#knowledge-base").addEventListener("change", () => refresh("knowledge", { quiet: true }).catch(() => {}));

$("#agent-form").addEventListener("submit", async (event) => {
  event.preventDefault(); const form = event.currentTarget; const data = new FormData(form);
  const agentId = String(data.get("agent_app_id") || "");
  const bases = String(data.get("knowledge_bases") || "").split(/[,，]/).map((item) => item.trim()).filter(Boolean);
  const tools = data.getAll("tools").map(String);
  const hosts = String(data.get("http_allowed_hosts") || "").split(/[,，]/)
    .map((item) => item.trim().toLowerCase()).filter(Boolean);
  if (tools.includes("http.get") && !hosts.length) {
    toast("启用 HTTPS 查询时必须填写允许访问的域名", "error");
    return;
  }
  const grants = tools.flatMap((name) => {
    const resources = name.startsWith("knowledge.")
      ? bases : (name === "http.get" ? hosts : []);
    if (name.startsWith("knowledge.") && !resources.length) return [];
    return [{ kind: "tool", name, actions: ["execute"], resources, risk_level: 0 }];
  });
  data.getAll("skills").forEach((name) => grants.push({
    kind: "skill", name: String(name), actions: ["load"], resources: [], risk_level: 0,
  }));
  $$("input[name=\"mcp_tools\"]:checked", form).forEach((input) => grants.push({
    kind: "mcp", name: input.value, actions: ["execute"],
    resources: [input.dataset.connection], risk_level: Number(input.dataset.risk || 2),
  }));
  const payload = {
    name: data.get("name"),
    application_config: { instruction: data.get("instruction") },
    tool_permissions: { grants, http_allowed_hosts: hosts },
    knowledge_config: { knowledge_base_names: bases, auto_retrieve: true, retrieval_limit: 5 },
  };
  // backend_config is creation-only here; omitting it on edits preserves the
  // storage profile already selected by the platform.
  if (!agentId) payload.backend_config = {};
  await mutate({ button: $("button[type='submit']", form), request: () => api(agentId ? `/tenants/${tenantId()}/agents/${agentId}` : `/tenants/${tenantId()}/agents`, { method: agentId ? "PATCH" : "POST", body: JSON.stringify(payload) }), success: agentId ? "Agent 配置已更新" : "Agent 已创建", refreshView: "agents", dialog: form.closest("dialog"), form });
});

$("#mcp-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = event.currentTarget;
  const data = new FormData(form);
  const connectionId = String(data.get("connection_id") || "");
  const authType = String(data.get("auth_type"));
  const secretValue = String(data.get("secret_value") || "").trim();
  const current = state.mcpConnections.find((item) => item.connection_id === connectionId);
  if (authType === "bearer" && !secretValue && !current?.credential_configured) {
    toast("Bearer 认证需要填写访问密钥", "error");
    return;
  }
  const payload = {
    name: data.get("name"), endpoint_url: data.get("endpoint_url"), auth_type: authType,
    timeout_seconds: Number(data.get("timeout_seconds")),
    ...(authType === "bearer" && secretValue ? { secret_value: secretValue } : {}),
  };
  const saved = await mutate({
    button: $("button[type='submit']", form),
    request: () => api(`/tenants/${tenantId()}/mcp-connections${connectionId ? `/${connectionId}` : ""}`, {
      method: connectionId ? "PATCH" : "POST", body: JSON.stringify(payload),
    }),
    success: `MCP 连接已${connectionId ? "更新" : "保存"}，请执行测试并刷新`,
    refreshView: "mcp", dialog: form.closest("dialog"), form,
  });
  if (saved) state.capabilitiesLoaded = false;
});

$("#knowledge-form").addEventListener("submit", async (event) => {
  event.preventDefault(); const form = event.currentTarget; const data = new FormData(form); const file = data.get("file"); const documentId = String(data.get("document_id") || "");
  if (!(file instanceof File) || !file.size) { toast("请选择需要上传的文件", "error"); return; }
  const action = documentId ? "替换" : "写入";
  if (!confirm(`确认${action}知识文档“${file.name}”？该操作将记录审计。`)) return;
  const base = $("#knowledge-base").value; const agentId = $("#knowledge-agent").value;
  let path = `/tenants/${tenantId()}/knowledge-bases/${encodeURIComponent(base)}/documents`;
  if (documentId) path += `/${documentId}`;
  path += `?agent_app_id=${agentId}&filename=${encodeURIComponent(file.name)}`;
  await mutate({ button: $("button[type='submit']", form), request: () => api(path, { method: documentId ? "PUT" : "POST", body: file, headers: { "Content-Type": file.type || "application/octet-stream" } }), success: `知识文档已${action}`, refreshView: "knowledge", dialog: form.closest("dialog"), form });
});

$("#replay-form").addEventListener("submit", async (event) => {
  event.preventDefault(); const form = event.currentTarget; const data = new FormData(form);
  const outboxId = encodeURIComponent(String(data.get("outbox_id") || ""));
  await mutate({ button: $("button[type='submit']", form), request: () => api(`/tenants/${tenantId()}/delivery-failures/${outboxId}/replay`, { method: "POST", body: JSON.stringify({ reason: data.get("reason") }) }), success: "投递任务已重新入队", refreshView: "failures", dialog: form.closest("dialog"), form });
});

(async () => {
  try { await enterConsole(); }
  catch (error) { if (error.status !== 401) setLoginError(error.message); }
})();
