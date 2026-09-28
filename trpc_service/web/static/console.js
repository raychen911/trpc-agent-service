(() => {
  const $ = (selector, root = document) => root.querySelector(selector);
  const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

  function escapeHTML(value) {
    const node = document.createElement("span");
    node.textContent = String(value ?? "");
    return node.innerHTML;
  }

  function cookie(name) {
    return document.cookie.split("; ")
      .find((row) => row.startsWith(`${name}=`))?.split("=").slice(1).join("=") || "";
  }

  function errorMessage(body, status) {
    // The API exposes one stable error envelope. Keep legacy detail handling so
    // the console can also display errors from proxies and older deployments.
    if (typeof body?.error?.message === "string") return body.error.message;
    if (typeof body?.detail === "string") return body.detail;
    if (Array.isArray(body?.detail)) return body.detail.map((item) => item.msg).join("；");
    if (body?.detail) return JSON.stringify(body.detail);
    return `请求失败（HTTP ${status}）`;
  }

  function createApi(state, supportReason = "") {
    const prefix = window.ConsoleConfig?.apiPrefix || "/api/v1";
    const api = async (path, options = {}) => {
      const headers = { ...(options.headers || {}) };
      if (state.token) headers.Authorization = `Bearer ${state.token}`;
      else if (!["GET", "HEAD", "OPTIONS"].includes(options.method || "GET")) {
        headers["X-CSRF-Token"] = decodeURIComponent(cookie("trpc_management_csrf"));
      }
      if (supportReason) headers["X-Support-Reason"] = supportReason;
      if (typeof options.body === "string") headers["Content-Type"] = "application/json";
      const controller = new AbortController();
      const timeout = window.setTimeout(() => controller.abort(), options.body instanceof File ? 120000 : 30000);
      const abort = () => controller.abort();
      options.signal?.addEventListener("abort", abort, { once: true });
      if (options.signal?.aborted) controller.abort();
      try {
        const response = await fetch(`${prefix}${path}`, {
          credentials: "same-origin", ...options, headers, signal: controller.signal,
        });
        const text = await response.text();
        let body = null;
        if (text) { try { body = JSON.parse(text); } catch { body = text; } }
        if (!response.ok) {
          const error = new Error(response.status === 429 ? "操作过于频繁，请稍后重试。" : errorMessage(body, response.status));
          error.status = response.status;
          if (response.status === 401 && state.actor && path !== "/auth/logout") {
            state.actor = null; state.token = "";
            document.querySelectorAll("dialog[open]").forEach((dialog) => dialog.close());
            $("#app-shell").hidden = true; $("#login-shell").hidden = false;
            setLoginError("登录已过期，请重新登录。");
          }
          throw error;
        }
        return body;
      } catch (error) {
        if (error.name === "AbortError") throw new Error("请求已超时或取消，请刷新后重试。");
        if (error instanceof TypeError) throw new Error("无法连接服务，请检查网络后重试。");
        throw error;
      } finally {
        window.clearTimeout(timeout);
        options.signal?.removeEventListener("abort", abort);
      }
    };
    api.all = async (path) => {
      const url = new URL(path, window.location.origin);
      const items = [];
      let data;
      do {
        url.searchParams.set("offset", String(items.length)); url.searchParams.set("limit", "100");
        data = await api(url.pathname + url.search);
        if (!Array.isArray(data.items)) throw new Error("服务返回的列表格式不正确。");
        if (!data.items.length && items.length < data.total) throw new Error("列表已更新，请刷新后重试。");
        items.push(...data.items);
      } while (items.length < data.total);
      return { ...data, items };
    };
    const pages = new Map();
    api.page = async (path, rowId, reload) => {
      const key = `${rowId}:${path}`;
      const offset = pages.get(key) || 0;
      const url = new URL(path, window.location.origin);
      url.searchParams.set("offset", String(offset)); url.searchParams.set("limit", "25");
      const data = await api(url.pathname + url.search);
      if (offset && offset >= data.total) {
        pages.set(key, Math.max(0, Math.floor((data.total - 1) / 25) * 25));
        return api.page(path, rowId, reload);
      }
      const table = document.getElementById(rowId).closest(".data-surface");
      let pager = table.querySelector(".pagination");
      if (!pager) { pager = document.createElement("nav"); pager.className = "pagination"; pager.setAttribute("aria-label", "列表翻页"); table.append(pager); }
      pager.replaceChildren();
      const label = document.createElement("span");
      label.textContent = data.total ? `第 ${offset + 1}–${offset + data.items.length} 条，共 ${data.total} 条` : "暂无记录";
      pager.append(label);
      for (const [text, next, disabled] of [["上一页", offset - 25, offset === 0], ["下一页", offset + 25, offset + data.items.length >= data.total]]) {
        const button = document.createElement("button"); button.type = "button"; button.className = "button secondary small";
        button.textContent = text; button.disabled = disabled;
        button.addEventListener("click", async () => {
          const buttons = [...pager.querySelectorAll("button")];
          const previousDisabled = buttons.map((item) => item.disabled);
          buttons.forEach((item) => { item.disabled = true; });
          pages.set(key, next);
          try { await reload(); }
          catch (error) {
            pages.set(key, offset); toast(error.message, "error");
            buttons.forEach((item, index) => { item.disabled = previousDisabled[index]; });
          }
        });
        pager.append(button);
      }
      return data;
    };
    return api;
  }

  function toast(text, kind = "success") {
    const item = document.createElement("div");
    item.className = `toast ${kind}`;
    item.textContent = text;
    $("#toasts").append(item);
    window.setTimeout(() => item.remove(), 4200);
  }

  function setLoginError(text = "") {
    const error = $("#login-error");
    error.textContent = text;
    error.hidden = !text;
  }

  const statusLabels = {
    active: "已启用", disabled: "已停用", stopped: "已停止", stale: "心跳过期",
    unknown: "待确认", ready: "已就绪", failed: "失败", error: "异常",
    pending: "待处理", processing: "处理中", ingesting: "入库中", draining: "排空中",
    retryable_failed: "等待重试", dead_letter: "需人工处理", delivered: "已送达",
    cancelled: "已取消", allowed: "已允许", denied: "已拒绝", success: "成功",
  };

  function statusLabel(value) {
    return statusLabels[String(value || "unknown").toLowerCase()] || String(value);
  }

  function badge(value) {
    const normalized = String(value || "unknown").toLowerCase();
    return `<span class="badge ${escapeHTML(normalized)}">${escapeHTML(statusLabel(value))}</span>`;
  }

  const loadingViews = new Map();
  function withLoading(view, operation) {
    if (loadingViews.has(view)) return loadingViews.get(view);
    const section = document.getElementById(view);
    let notice = section.querySelector(".view-status");
    if (!notice) {
      notice = document.createElement("p"); notice.className = "view-status";
      notice.setAttribute("role", "status"); section.prepend(notice);
    }
    notice.textContent = "正在加载最新数据…"; notice.classList.remove("error"); notice.hidden = false;
    section.setAttribute("aria-busy", "true");
    // Keep the filter context stable while its table is being fetched. A
    // coalesced refresh must never render tenant A beneath tenant B's selector.
    const controls = [...$$(`[data-refresh="${view}"]`), ...$$("select", section)];
    const disabled = controls.map((control) => control.disabled);
    controls.forEach((control) => { control.disabled = true; });
    const tables = $$(".data-surface", section);
    tables.forEach((table) => { table.inert = true; });
    let loaded = false;
    const pending = Promise.resolve().then(operation).then((result) => {
      loaded = true; notice.hidden = true; return result;
    }).catch((error) => {
      notice.textContent = "数据加载失败，已暂停表格操作，请刷新重试。"; notice.classList.add("error"); throw error;
    }).finally(() => {
      section.setAttribute("aria-busy", "false");
      controls.forEach((control, index) => { control.disabled = disabled[index]; });
      tables.forEach((table) => { table.inert = !loaded; });
      loadingViews.delete(view);
    });
    loadingViews.set(view, pending);
    return pending;
  }

  function empty(columns, text = "暂无数据") {
    return `<tr><td class="empty" colspan="${columns}">${escapeHTML(text)}</td></tr>`;
  }

  function formatDate(value) {
    return value ? new Date(value).toLocaleString("zh-CN", { hour12: false }) : "—";
  }

  function compactNumber(value) {
    return new Intl.NumberFormat("zh-CN", {
      notation: "compact", maximumFractionDigits: 1,
    }).format(Number(value || 0));
  }

  function setOptions(selector, items, valueKey, label) {
    $$(selector).forEach((select) => {
      const previous = select.value;
      select.innerHTML = items.map((item) =>
        `<option value="${escapeHTML(item[valueKey])}">${escapeHTML(label(item))}</option>`).join("");
      if (items.some((item) => String(item[valueKey]) === previous)) select.value = previous;
    });
  }

  const icons = {
    overview: '<rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/>',
    tenants: '<path d="M3 21V7l9-4v18M12 9h9v12M1 21h22M6 9h3M6 13h3M6 17h3M15 13h3M15 17h3"/>',
    accounts: '<circle cx="12" cy="8" r="4"/><path d="M4 21v-3a8 8 0 0 1 16 0v3"/>',
    models: '<path d="m12 2 10 6-10 6L2 8l10-6zM2 12l10 6 10-6M2 16l10 6 10-6"/>',
    credentials: '<circle cx="8" cy="8" r="5"/><path d="m12 12 9 9M17 17l3-3M14 14l3-3"/>',
    profiles: '<path d="M4 5h16M4 12h16M4 19h16"/><circle cx="8" cy="5" r="2" fill="white"/><circle cx="16" cy="12" r="2" fill="white"/><circle cx="10" cy="19" r="2" fill="white"/>',
    adapters: '<path d="M8 3v5M16 3v5M6 8h12v3a6 6 0 0 1-12 0V8zM12 17v5"/>',
    agents: '<rect x="4" y="7" width="16" height="14" rx="3"/><path d="M12 3v4M8 12v2M16 12v2M9 17h6M1 12h3M20 12h3"/>',
    channels: '<rect x="3" y="3" width="18" height="14" rx="3"/><path d="m7 17-2 4 7-4M7 8h10M7 12h6"/>',
    knowledge: '<path d="M12 5C8 2 4 3 2 4v15c4-1 7-1 10 2 3-3 6-3 10-2V4c-4-1-7-1-10 1v16"/>',
    runtime: '<rect x="3" y="3" width="18" height="7" rx="2"/><rect x="3" y="14" width="18" height="7" rx="2"/><path d="M7 6.5h.01M7 17.5h.01M12 6.5h5M12 17.5h5"/>',
    usage: '<path d="M4 3v18h17M9 17v-5M14 17V8M19 17V4"/>',
    audit: '<path d="M8 3H5v18h14V3h-3M9 2h6v4H9zM8 10h8M8 14h8M8 18h5"/>',
    failures: '<path d="m12 3 10 18H2L12 3zM12 9v5M12 17h.01"/>',
    mcp: '<path d="m8 5-6 7 6 7M16 5l6 7-6 7M14 3l-4 18"/>',
  };
  $$(".nav-item").forEach((button) => {
    const icon = icons[button.dataset.view] || icons.runtime;
    button.insertAdjacentHTML("afterbegin", `<svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">${icon}</svg>`);
    if (button.classList.contains("active")) button.setAttribute("aria-current", "page");
  });
  $$(".dialog-close").forEach((button) => button.setAttribute("aria-label", "关闭对话框"));
  $$("input[name='password']").forEach((input) => { input.maxLength = 256; });
  $$("dialog").forEach((dialog) => {
    const title = dialog.querySelector("h2");
    if (title) { title.id ||= `${dialog.id}-heading`; dialog.setAttribute("aria-labelledby", title.id); }
  });
  $("#toasts")?.setAttribute("aria-live", "polite");
  $("#login-error")?.setAttribute("role", "alert");

  // Catalogs also populate form selectors, so fetch their complete collection
  // and paginate locally. Activity feeds use server pagination above.
  $$(".data-surface tbody").forEach((body) => {
    const surface = body.closest(".data-surface");
    let page = 0;
    let controls;
    let search;
    let pager;
    const render = () => {
      if (surface.querySelector(".pagination:not(.local-pagination)")) return;
      const rows = [...body.rows];
      if (!controls && !rows.some((row) => !row.querySelector(".empty"))) return;
      if (!controls) {
        controls = document.createElement("div"); controls.className = "table-filter";
        search = document.createElement("input"); search.type = "search";
        search.placeholder = "搜索当前列表…"; search.setAttribute("aria-label", "搜索当前列表");
        controls.append(search); surface.prepend(controls);
        pager = document.createElement("nav"); pager.className = "pagination local-pagination";
        pager.setAttribute("aria-label", "列表翻页"); surface.append(pager);
        search.addEventListener("input", () => { page = 0; render(); });
      }
      const query = search.value.trim().toLocaleLowerCase();
      const matches = rows.filter((row) => !row.querySelector(".empty") && row.textContent.toLocaleLowerCase().includes(query));
      page = Math.min(page, Math.max(0, Math.ceil(matches.length / 25) - 1));
      const visible = new Set(matches.slice(page * 25, (page + 1) * 25));
      rows.forEach((row) => { row.hidden = row.querySelector(".empty") ? matches.length > 0 : !visible.has(row); });
      pager.replaceChildren();
      const label = document.createElement("span");
      label.textContent = matches.length ? `第 ${page * 25 + 1}–${Math.min((page + 1) * 25, matches.length)} 条，共 ${matches.length} 条` : "暂无匹配记录";
      pager.append(label);
      for (const [labelText, step, disabled] of [["上一页", -1, page === 0], ["下一页", 1, (page + 1) * 25 >= matches.length]]) {
        const button = document.createElement("button"); button.type = "button";
        button.className = "button secondary small"; button.textContent = labelText; button.disabled = disabled;
        button.addEventListener("click", () => { page += step; render(); }); pager.append(button);
      }
    };
    new MutationObserver(() => { page = 0; render(); }).observe(body, { childList: true });
    render();
  });

  window.ConsoleUI = {
    $, $$, escapeHTML, createApi, toast, setLoginError, badge, empty,
    formatDate, compactNumber, setOptions, statusLabel, withLoading,
  };
})();
