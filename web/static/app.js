const $ = (sel) => document.querySelector(sel);

async function getJSON(url) {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`${url} -> ${resp.status}`);
  return resp.json();
}

function fmt(ts) {
  if (ts == null) return "—";
  return new Date(ts * 1000).toLocaleString();
}

function badge(status) {
  if (status == null) return '<span class="muted">—</span>';
  return `<span class="badge badge-${status}">${status}</span>`;
}

async function refresh() {
  const [alertsData, notifData, budgetData] = await Promise.all([
    getJSON("/api/alerts"),
    getJSON("/api/notifications"),
    getJSON("/api/budgets"),
  ]);

  const s = alertsData.silence;
  $("#silence-state").innerHTML = s.active
    ? `<span class="silence-on">🔕 静默中，剩余 ${Math.ceil(s.remaining_seconds)} 秒</span>（截止 ${fmt(s.until_ts)}，原因：${s.reason || "未填写"}）`
    : '<span class="silence-off">🔔 通知正常</span>';

  $("#alerts-table tbody").innerHTML = alertsData.alerts.map((a) => `
    <tr>
      <td><code class="fp-link" title="点击填入预算表单">${escapeHtml(a.fingerprint)}</code></td>
      <td>${a.current_seq}</td>
      <td>${badge(a.status)}</td>
      <td>${a.round}</td>
      <td>${escapeHtml(a.summary)}</td>
      <td>${badge(a.firing_notify_status)}</td>
      <td>${badge(a.recovery_notify_status)}</td>
      <td>${fmt(a.updated_at)}</td>
    </tr>`).join("") || '<tr><td colspan="8" class="muted">暂无告警</td></tr>';

  $("#budgets-table tbody").innerHTML = budgetData.budgets.map((b) => `
    <tr>
      <td><code>${escapeHtml(b.fingerprint)}</code></td>
      <td>${b.allowed_seconds}</td>
      <td>${b.downtime_seconds}</td>
      <td>${b.exempt_seconds}</td>
      <td class="${b.exhausted ? "budget-exhausted" : "budget-ok"}">${b.remaining_seconds}</td>
      <td>${b.exhausted
        ? '<span class="badge badge-failed">已耗尽</span>'
        : '<span class="badge badge-delivered">有剩余</span>'}</td>
      <td>${fmt(b.set_at)}</td>
      <td>${fmt(b.window_start)} ～ ${fmt(b.now)}</td>
    </tr>`).join("")
    || '<tr><td colspan="8" class="muted">尚未设置预算</td></tr>';

  $("#notifications-table tbody").innerHTML = notifData.notifications.map((n) => `
    <tr>
      <td><code>${n.notification_no}</code></td>
      <td>${n.fingerprint}</td>
      <td>${n.round}</td>
      <td>${n.kind}</td>
      <td>${badge(n.status)}</td>
      <td>${n.attempts}</td>
      <td>${n.last_error ? escapeHtml(n.last_error) : "—"}</td>
      <td>${fmt(n.enqueued_at)}</td>
      <td>${fmt(n.delivered_at)}</td>
    </tr>`).join("") || '<tr><td colspan="9" class="muted">暂无通知</td></tr>';
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

$("#silence-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const resp = await fetch("/api/silence", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      duration_seconds: Number($("#duration").value),
      reason: $("#reason").value,
    }),
  });
  if (!resp.ok) alert("设置失败: " + (await resp.text()));
  refresh();
});

$("#clear-silence").addEventListener("click", async () => {
  await fetch("/api/silence", { method: "DELETE" });
  refresh();
});

$("#budget-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const fp = $("#budget-fp").value.trim();
  const allowed = Number($("#budget-allowed").value);
  if (!fp) {
    alert("请输入指纹");
    return;
  }
  const resp = await fetch("/api/budgets", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fingerprint: fp, allowed_seconds: allowed }),
  });
  if (!resp.ok) {
    alert("预算设置失败: " + (await resp.text()));
    return;
  }
  const data = await resp.json();
  refresh();
  alert(`已设置：已计入停机 ${data.downtime_seconds} 秒，剩余预算 ${data.remaining_seconds} 秒`);
});

// Clicking an alert fingerprint fills the budget form for quick setup.
document.addEventListener("click", (e) => {
  if (e.target.classList.contains("fp-link")) {
    $("#budget-fp").value = e.target.textContent;
    $("#budget-allowed").focus();
  }
});

refresh();
setInterval(refresh, 2000);
