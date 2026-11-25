// If the UI is hosted somewhere else (Netlify), put the API address here.
// Empty string = same server as the API.
const API = "";

let currentAccount = null;
let filters = {};

async function call(path, options = {}) {
  const res = await fetch(API + path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    let text = data.error || "request failed";
    if (data.details) text += ": " + data.details.map(d => `${d.field} - ${d.problem}`).join(", ");
    throw new Error(text);
  }
  return data;
}

function money(x) {
  return Number(x).toFixed(2);
}

function showMsg(id, text, ok) {
  const el = document.getElementById(id);
  el.textContent = text;
  el.className = "msg " + (ok ? "ok" : "error");
}

// ---------- accounts ----------

async function loadAccounts(selectId) {
  const accounts = await call("/api/accounts");
  const sel = document.getElementById("accountSelect");
  sel.innerHTML = "";
  if (accounts.length === 0) {
    sel.innerHTML = "<option>no account yet</option>";
    currentAccount = null;
    return;
  }
  for (const a of accounts) {
    const opt = document.createElement("option");
    opt.value = a.id;
    opt.textContent = `#${a.id} ${a.owner_name} (${a.currency})`;
    sel.appendChild(opt);
  }
  sel.value = selectId || currentAccount?.id || accounts[0].id;
  await selectAccount(sel.value);
}

async function selectAccount(id) {
  currentAccount = await call(`/api/accounts/${id}`);
  document.getElementById("accountInfo").textContent =
    `Balance: ${money(currentAccount.balance)} ${currentAccount.currency}`;
  await refresh();
}

document.getElementById("accountSelect").addEventListener("change", e => selectAccount(e.target.value));

document.getElementById("accountForm").addEventListener("submit", async e => {
  e.preventDefault();
  const f = new FormData(e.target);
  try {
    const acc = await call("/api/accounts", {
      method: "POST",
      body: JSON.stringify({
        owner_name: f.get("owner_name"),
        currency: f.get("currency"),
        opening_balance: f.get("opening_balance") || 0,
      }),
    });
    e.target.reset();
    await loadAccounts(acc.id);
  } catch (err) {
    alert(err.message);
  }
});

// ---------- new transaction ----------

document.getElementById("txForm").addEventListener("submit", async e => {
  e.preventDefault();
  if (!currentAccount) return showMsg("txMsg", "create an account first", false);
  const f = new FormData(e.target);
  try {
    const tx = await call("/api/transactions", {
      method: "POST",
      body: JSON.stringify({
        account_id: currentAccount.id,
        type: f.get("type"),
        amount: f.get("amount") || 0,
        category: f.get("category"),
        description: f.get("description"),
      }),
    });
    showMsg("txMsg", `Transaction #${tx.id} sent, status ${tx.status}`, true);
    e.target.amount.value = "";
    e.target.description.value = "";
    await refresh();
  } catch (err) {
    showMsg("txMsg", err.message, false);
  }
});

// ---------- list + filters ----------

document.getElementById("filterForm").addEventListener("submit", async e => {
  e.preventDefault();
  filters = {};
  for (const [k, v] of new FormData(e.target)) if (v) filters[k] = v;
  await refresh();
});

document.getElementById("clearFilter").addEventListener("click", async () => {
  document.getElementById("filterForm").reset();
  filters = {};
  await refresh();
});

async function loadTransactions() {
  const params = new URLSearchParams({ account_id: currentAccount.id, limit: 30, ...filters });
  const tbody = document.querySelector("#txTable tbody");
  try {
    const page = await call("/api/transactions?" + params);
    document.getElementById("filterMsg").textContent = "";
    tbody.innerHTML = "";
    for (const t of page.items) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${t.id}</td>
        <td>${new Date(t.created_at).toLocaleString()}</td>
        <td>${t.type}</td>
        <td>${t.category}</td>
        <td class="num">${money(t.amount)}</td>
        <td><span class="status ${t.status}" title="${t.reject_reason || ""}">${t.status}</span></td>
        <td><button class="link" data-id="${t.id}">log</button></td>`;
      tbody.appendChild(tr);
    }
    document.getElementById("txCount").textContent = `${page.items.length} of ${page.total} shown`;
  } catch (err) {
    showMsg("filterMsg", err.message, false);
  }
}

document.querySelector("#txTable tbody").addEventListener("click", async e => {
  const id = e.target.dataset.id;
  if (!id) return;
  const logs = await call(`/api/transactions/${id}/logs`);
  document.getElementById("logTxId").textContent = "#" + id;
  document.getElementById("logList").innerHTML = logs
    .map(l => `<li><b>${l.event}</b> (${l.source}, ${new Date(l.created_at).toLocaleTimeString()}) - ${l.message}</li>`)
    .join("");
  document.getElementById("logBox").hidden = false;
});

// ---------- summary ----------

async function loadSummary() {
  const s = await call(`/api/reports/summary?account_id=${currentAccount.id}`);
  const rows = [
    ["Total deposit", money(s.total_deposit)],
    ["Total withdrawal", money(s.total_withdrawal)],
    ["Total payment", money(s.total_payment)],
    ["Net", money(s.net)],
  ];
  for (const [st, c] of Object.entries(s.count_by_status)) rows.push([st + " count", c]);
  for (const [cat, sum] of Object.entries(s.spending_by_category)) rows.push(["spent on " + cat, money(sum)]);
  document.getElementById("summary").innerHTML = rows.map(r => `<tr><td>${r[0]}</td><td>${r[1]}</td></tr>`).join("");
}

// ---------- kafka activity + health ----------

async function loadActivity() {
  try {
    const a = await call("/api/messaging/activity");
    document.getElementById("activity").innerHTML = a.messages
      .map(m => `<li class="${m.direction}">${new Date(m.time).toLocaleTimeString()} ${m.direction.padEnd(8)} [${m.topic}] ${JSON.stringify(m.value)}</li>`)
      .join("") || "<li>no messages yet</li>";
  } catch (err) { /* ignore */ }
}

async function loadHealth() {
  try {
    const h = await call("/health");
    document.getElementById("health").textContent = `API ${h.api} | DB ${h.database} | Kafka ${h.kafka}`;
  } catch {
    document.getElementById("health").textContent = "API not reachable";
  }
}

async function refresh() {
  if (!currentAccount) return;
  currentAccount = await call(`/api/accounts/${currentAccount.id}`);
  document.getElementById("accountInfo").textContent =
    `Balance: ${money(currentAccount.balance)} ${currentAccount.currency}`;
  await Promise.all([loadTransactions(), loadSummary(), loadActivity()]);
}

// pending transactions change after the consumer works, so we poll
setInterval(() => { refresh(); loadHealth(); }, 2000);
loadHealth();
loadAccounts().catch(err => alert(err.message));
