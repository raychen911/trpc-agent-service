const $ = (id) => document.getElementById(id);
let activeRequest = "";
let activeRequests = [];
let pollTimer = null;
let polling = false;
let selectedFiles = [];

async function api(path, options = {}) {
  const response = await fetch(path, {headers:{"Content-Type":"application/json"}, ...options});
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(apiErrorMessage(body, response.status));
  return body;
}

function apiErrorMessage(body, status) {
  const detail = body.detail ?? body.message;
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) return detail.map(item => {
    if (!item || typeof item !== "object") return String(item);
    const location = Array.isArray(item.loc) ? item.loc.join(".") : "";
    return `${location ? `${location}: ` : ""}${item.msg || JSON.stringify(item)}`;
  }).join("；");
  if (detail && typeof detail === "object") return detail.message || JSON.stringify(detail);
  return `HTTP ${status}`;
}

async function bootstrap() {
  const data = await api("/api/v1/dev/im/bootstrap");
  $("channel").innerHTML = data.channels.map(x => `<option value="${x.value}">${x.label}</option>`).join("");
  $("model-mode").innerHTML = data.model_modes.map(x => `<option value="${x.value}">${x.label}</option>`).join("");
}

function setState(text, kind="running") { $("result-state").textContent=text; $("result-state").className=`pill ${kind}`; }
function pretty(value) { return JSON.stringify(value, null, 2); }
async function attachmentData(file) {
  if (file.size > 1024 * 1024) throw new Error("测试附件不能超过 1 MiB");
  const data = await new Promise((resolve,reject) => { const r=new FileReader(); r.onload=()=>resolve(r.result.split(",")[1]); r.onerror=reject; r.readAsDataURL(file); });
  return {name:file.name, mime_type:file.type || "application/octet-stream", content_base64:data};
}

function renderAttachments() {
  const total = selectedFiles.reduce((sum, file) => sum + file.size, 0);
  $("attachment-summary").textContent = selectedFiles.length ? `${selectedFiles.length} 个附件，共 ${(total / 1024).toFixed(1)} KiB` : "未选择附件";
  const rows = selectedFiles.map((file, index) => {
    const row = document.createElement("li"), label = document.createElement("span"), button = document.createElement("button");
    label.textContent = `${file.name} · ${(file.size / 1024).toFixed(1)} KiB`;
    button.type = "button"; button.className = "remove-attachment"; button.dataset.index = String(index); button.textContent = "移除";
    row.append(label, button);
    return row;
  });
  $("attachment-list").replaceChildren(...rows);
}

$("attachment").addEventListener("change", (event) => {
  const additions = Array.from(event.target.files || []);
  for (const file of additions) {
    const key = `${file.name}:${file.size}:${file.lastModified}`;
    if (!selectedFiles.some(item => `${item.name}:${item.size}:${item.lastModified}` === key)) selectedFiles.push(file);
  }
  event.target.value = "";
  renderAttachments();
});

$("attachment-list").addEventListener("click", (event) => {
  const button = event.target.closest(".remove-attachment");
  if (!button) return;
  selectedFiles.splice(Number(button.dataset.index), 1);
  renderAttachments();
});

$("clear-attachments").addEventListener("click", () => { selectedFiles = []; renderAttachments(); });

async function poll() {
  if (!activeRequests.length || !polling) return;
  try {
    const records = await Promise.all(activeRequests.map(id => api(`/api/v1/dev/im/messages/${id}`)));
    const replies = records.map((data, index) => (data.request.result || {}).text ? `#${index + 1} ${(data.request.result || {}).text}` : "").filter(Boolean);
    const outboxes = records.map(data => (data.outbox || {}).state || "尚未创建");
    const errors = records.map(data => data.request.error_code || (data.outbox || {}).last_error).filter(Boolean);
    $("reply").textContent = replies.join("\n\n") || "处理中……";
    $("config-version").textContent = [...new Set(records.map(data => data.request.config_version))].join(", ");
    $("outbox-state").textContent = outboxes.join(", ");
    $("error-code").textContent = errors.join(", ") || "—";
    $("stages").innerHTML = records.flatMap((data, index) => (data.stages || []).map(s => `<li><span>#${index + 1} ${s.name}</span><b>${s.state}</b></li>`)).join("");
    const terminal = records.every(data => ["succeeded","failed"].includes(data.request.state) && (!data.outbox || ["delivered","dead","unknown"].includes(data.outbox.state)));
    const delivered = records.every(data => data.delivered);
    const failed = records.some(data => data.request.state === "failed" || ["dead","unknown"].includes((data.outbox || {}).state));
    setState(delivered ? "全部已投递" : (failed && terminal ? "完成但有失败" : "处理中"), delivered ? "done" : (failed ? "error" : "running"));
    if (!terminal && polling) pollTimer = setTimeout(poll, 700);
    else polling = false;
  } catch (error) {
    stopPolling(false);
    setState("查询失败", "error");
    $("error-code").textContent = error.message;
  }
}

function stopPolling(showState=true) {
  polling = false;
  if (pollTimer !== null) clearTimeout(pollTimer);
  pollTimer = null;
  if (showState && activeRequest) setState("已停止查询", "idle");
}

$("composer").addEventListener("submit", async (event) => {
  event.preventDefault(); stopPolling(false); $("send").disabled=true; setState("正在接收");
  try {
    const common = {channel:$("channel").value, model_mode:$("model-mode").value, external_user_id:$("user-id").value,
      external_conversation_id:$("conversation-id").value, chat_type:$("chat-type").value,
      duplicate_count:$("duplicate").checked ? 2 : 1};
    const body = {...common, text:$("text").value, attachments:await Promise.all(selectedFiles.map(attachmentData))};
    const responses = [await api("/api/v1/dev/im/messages", {method:"POST", body:JSON.stringify(body)})];
    activeRequests=responses.map(data => data.request_id); activeRequest=activeRequests[0]; polling=true;
    $("request-id").textContent=activeRequests.join("\n");
    $("duplicate-reused").textContent=responses.every(data => data.duplicate_reused) ? "是" : "否";
    $("raw").textContent=pretty(responses.length === 1 ? responses[0].raw : responses.map(data => data.raw));
    $("normalized").textContent=pretty(responses.length === 1 ? responses[0].normalized : responses.map(data => data.normalized));
    poll();
  } catch (error) { setState("发送失败","error"); $("reply").textContent=error.message; }
  finally { $("send").disabled=false; }
});

$("stop-poll").addEventListener("click", () => stopPolling(true));

$("set-fault").addEventListener("click", async () => {
  try { await api("/api/v1/dev/im/faults", {method:"POST",body:JSON.stringify({channel:$("channel").value,fault:$("fault").value})}); setState("故障已设置"); }
  catch(error) { setState("设置失败","error"); $("reply").textContent=error.message; }
});

bootstrap().catch(error => { setState("初始化失败","error"); $("reply").textContent=error.message; });
