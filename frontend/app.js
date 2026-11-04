/* Bot Control Center - React (UMD) + htm, no build step needed. */
const { useState, useEffect, useRef, useCallback } = React;
const html = htm.bind(React.createElement);

async function api(path, opts = {}) {
  const r = await fetch("/api" + path, { credentials: "same-origin", headers: { "Content-Type": "application/json" }, ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined });
  if (r.status === 401 && path !== "/auth/login") { window.dispatchEvent(new Event("logged-out")); throw new Error("Login required"); }
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(typeof j.detail === "string" ? j.detail : "Request failed");
  return j;
}
const fmt = (t) => t ? new Date(t).toLocaleString() : "-";
const ago = (t) => { if (!t) return "never"; const s = Math.max(0, (Date.now() - new Date(t)) / 1000); return s < 60 ? Math.floor(s) + "s ago" : s < 3600 ? Math.floor(s / 60) + "m ago" : s < 86400 ? Math.floor(s / 3600) + "h ago" : Math.floor(s / 86400) + "d ago"; };
const num = (n) => (n ?? 0).toLocaleString();

// live websocket events -> any component can subscribe
const listeners = new Set();
function useLive(cb) { useEffect(() => { listeners.add(cb); return () => listeners.delete(cb); }, [cb]); }
function connectWS() {
  let ws, stop = false;
  const open = () => {
    ws = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws");
    ws.onmessage = (e) => { try { const m = JSON.parse(e.data); listeners.forEach((f) => f(m)); } catch {} };
    ws.onclose = () => { if (!stop) setTimeout(open, 2000); };
  };
  open();
  const ping = setInterval(() => ws && ws.readyState === 1 && ws.send("ping"), 25000);
  return () => { stop = true; clearInterval(ping); ws && ws.close(); };
}

const Stat = ({ label, value, kind }) => html`<div class="card stat ${kind || ""}"><div class="l">${label}</div><div class="v">${value}</div></div>`;
const Status = ({ on }) => html`<span><span class=${"dot " + (on ? "on" : "")}></span>${on ? "Online" : "Offline"}</span>`;

function Overview() {
  const [o, setO] = useState(null), [d, setD] = useState([]), [bots, setBots] = useState([]);
  const load = useCallback(() => { api("/overview").then(setO); api("/analytics/daily?days=14").then((r) => setD(r.days)); api("/bots").then(setBots); }, []);
  useEffect(load, []);
  useLive(useCallback((m) => { if (m.type !== "log") load(); }, [load]));
  if (!o) return html`<p class="muted">Loading...</p>`;
  return html`<div><h1>Overview</h1><div class="sub">Live status of every connected bot</div>
    <div class="grid">
      <${Stat} label="Registered bots" value=${o.total_bots} /><${Stat} label="Online" value=${o.online} kind="accent" /><${Stat} label="Offline" value=${o.offline} kind=${o.offline ? "bad" : ""} />
      <${Stat} label="Pending IDs" value=${num(o.pending)} /><${Stat} label="Processing IDs" value=${num(o.processing)} /><${Stat} label="Completed IDs" value=${num(o.completed)} kind="accent" />
      <${Stat} label="Errors" value=${num(o.errors)} kind=${o.errors ? "bad" : ""} /><${Stat} label="Processed today" value=${num(o.processed_today)} kind="accent" />
    </div>
    <h2 style=${{ margin: "22px 0 10px" }}>Each bot</h2>
    <${BotCards} bots=${bots} />
    <div class="card" style=${{ marginTop: 16 }}><h2>IDs completed per day (14 days)</h2><${Bars} days=${d} /></div></div>`;
}

function Bars({ days, key2 = "processed", err }) {
  const max = Math.max(1, ...days.map((x) => x[key2]));
  return html`<div class="bars">${days.map((x) => html`<div class=${"bar " + (err ? "e" : "")} key=${x.date} title=${x.date + ": " + x[key2]}>
    <span>${x[key2] || ""}</span><div style=${{ height: (x[key2] / max) * 100 + "%" }}></div><span>${x.date.slice(5)}</span></div>`)}</div>`;
}

function BotCards({ bots }) {
  if (!bots.length) return html`<div class="card muted">No bots yet. Go to All Bots and click Add Bot.</div>`;
  return html`<div class="botgrid">${bots.map((b) => html`<div class="card botcard" key=${b.id} onClick=${() => (location.hash = "#/bot/" + b.id)}>
    <div class="row" style=${{ marginBottom: 4 }}><b>${b.name}</b><span style=${{ flex: 1 }}></span><${Status} on=${b.online} />${!b.online && b.app_status ? html`<span class="muted">${b.app_status}</span>` : ""}</div>
    <div class="muted">${b.telegram_username ? "@" + b.telegram_username : b.id}${b.server ? " · " + b.server : ""}</div>
    <div class="bnums">
      <div><div class="l">IDs</div><div class="v">${num(b.pending)}</div></div>
      <div><div class="l">Processing</div><div class="v">${num(b.processing)}</div></div>
      <div><div class="l">Done</div><div class="v accent">${num(b.completed)}</div></div>
    </div></div>`)}</div>`;
}

function AddBot({ onClose, onDone }) {
  const [token, setToken] = useState(""), [name, setName] = useState(""), [err, setErr] = useState(""), [res, setRes] = useState(null), [busy, setBusy] = useState(false);
  const submit = async () => {
    setBusy(true); setErr("");
    try { const r = await api("/bots", { method: "POST", body: { name: name || null, telegram_token: token || null } }); setToken(""); setRes(r); onDone(); }
    catch (e) { setErr(e.message); } setBusy(false);
  };
  return html`<div class="modal" onClick=${(e) => e.target === e.currentTarget && onClose()}><div class="card">
    ${!res ? html`<h2>Add Bot</h2>
      <p class="muted">Paste a Telegram bot token to fetch name/username automatically, or just give the bot a name. The token is validated, encrypted, and never shown again.</p>
      <input style=${{ width: "100%", marginBottom: 10 }} type="password" autocomplete="off" placeholder="Telegram bot token (optional)" value=${token} onInput=${(e) => setToken(e.target.value)} />
      <input style=${{ width: "100%" }} placeholder="Bot name (required if no token)" value=${name} onInput=${(e) => setName(e.target.value)} />
      ${err && html`<div class="err">${err}</div>`}
      <div class="row" style=${{ marginTop: 14, marginBottom: 0 }}><button class="btn" disabled=${busy} onClick=${submit}>${busy ? "Registering..." : "Register bot"}</button><button class="btn ghost" onClick=${onClose}>Cancel</button></div>`
    : html`<h2>Bot registered: ${res.bot.name}</h2><p>Bot ID <b class="mono">${res.bot.id}</b></p>
      <p class="muted">Put this one-time pairing key in <span class="mono">monitor_config.json</span> on the machine that runs the bot, then run <span class="mono">python start.py</span>. It is shown only once.</p>
      <div class="note mono">${res.pairing_key}</div>
      <div class="row"><button class="btn" onClick=${() => navigator.clipboard && navigator.clipboard.writeText(res.pairing_key)}>Copy key</button><button class="btn ghost" onClick=${onClose}>Done</button></div>`}
  </div></div>`;
}

function Bots() {
  const [bots, setBots] = useState([]), [q, setQ] = useState(""), [st, setSt] = useState(""), [add, setAdd] = useState(false);
  const load = useCallback(() => api(`/bots?q=${encodeURIComponent(q)}&status=${st}`).then(setBots), [q, st]);
  useEffect(() => { load(); }, [load]);
  useLive(useCallback((m) => { if (m.type !== "log") load(); }, [load]));
  return html`<div><h1>All Bots</h1><div class="sub">${bots.length} bots</div>
    <div class="row"><input placeholder="Search name, @username, ID" value=${q} onInput=${(e) => setQ(e.target.value)} />
      <select value=${st} onChange=${(e) => setSt(e.target.value)}><option value="">All status</option><option value="online">Online</option><option value="offline">Offline</option></select>
      <span style=${{ flex: 1 }}></span><button class="btn" onClick=${() => setAdd(true)}>+ Add Bot</button></div>
    <${BotCards} bots=${bots} />${add && html`<${AddBot} onClose=${() => setAdd(false)} onDone=${load} />`}</div>`;
}

function Files({ botId }) {
  const [kind, setKind] = useState("pending"), [q, setQ] = useState(""), [page, setPage] = useState(1), [d, setD] = useState(null);
  const load = useCallback(() => api(`/bots/${botId}/files/${kind}?q=${encodeURIComponent(q)}&page=${page}&page_size=25`).then(setD), [botId, kind, q, page]);
  useEffect(() => { load(); }, [load]);
  const pages = d ? Math.max(1, Math.ceil(d.total / d.page_size)) : 1;
  const names = { pending: "accs.json", progress: "progress.json", done: "done_accs.json" };
  return html`<div class="card"><h2>JSON file viewer</h2>
    <div class="tabs">${Object.entries(names).map(([k, n]) => html`<button key=${k} class=${kind === k ? "on" : ""} onClick=${() => { setKind(k); setPage(1); }}>${n}</button>`)}</div>
    <div class="row"><input placeholder="Search ID" value=${q} onInput=${(e) => { setQ(e.target.value); setPage(1); }} />
      <a class="btn ghost" href=${`/api/bots/${botId}/files/${kind}/export?format=json`}>Export JSON</a><a class="btn ghost" href=${`/api/bots/${botId}/files/${kind}/export?format=csv`}>Export CSV</a>
      <span class="muted">${d ? `${num(d.total)} entries, updated ${ago(d.updated_at)}` : ""}</span></div>
    <pre class="mono" style=${{ background: "var(--panel2)", padding: 12, borderRadius: 8, maxHeight: 340, overflow: "auto", margin: 0 }}>${d ? JSON.stringify(d.rows.map((r) => (kind === "progress" ? { [r.uid]: r.raw } : { [r.uid]: r.password })), null, 2) : "Loading..."}</pre>
    <div class="row" style=${{ marginTop: 10, marginBottom: 0 }}><button class="btn ghost" disabled=${page <= 1} onClick=${() => setPage(page - 1)}>Prev</button><span>Page ${page} / ${pages}</span><button class="btn ghost" disabled=${page >= pages} onClick=${() => setPage(page + 1)}>Next</button></div></div>`;
}

const LogRow = ({ l }) => html`<div class="log mono"><span class="muted">${fmt(l.ts)}</span><span class=${"L-" + l.level}>${l.level}</span><span class="bn">${l.bot || ""} <span class="muted">${l.event}</span></span><span>${l.message}</span></div>`;

function BotPage({ id }) {
  const [b, setB] = useState(null), [key, setKey] = useState(null), [err, setErr] = useState("");
  const load = useCallback(() => api("/bots/" + id).then(setB).catch((e) => setErr(e.message)), [id]);
  useEffect(() => { load(); }, [load]);
  useLive(useCallback((m) => { if ((m.type === "bot_update" && m.bot.id === id) || (m.type === "log" && m.log.bot_id === id)) load(); }, [id, load]));
  if (err) return html`<div class="err">${err}</div>`;
  if (!b) return html`<p class="muted">Loading...</p>`;
  const regen = async () => { if (confirm("Generate a new pairing key? The current agent will be disconnected.")) setKey((await api(`/bots/${id}/pairing`, { method: "POST" })).pairing_key); };
  const del = async () => { if (confirm("Delete this bot and all its data?")) { await api("/bots/" + id, { method: "DELETE" }); location.hash = "#/bots"; } };
  return html`<div><a href="#/bots">&larr; All bots</a><h1 style=${{ marginTop: 8 }}>${b.name}</h1>
    <div class="sub"><${Status} on=${b.online} /> &middot; ${b.telegram_username ? "@" + b.telegram_username : "no Telegram link"} &middot; <span class="mono">${b.id}</span> &middot; server: ${b.server || "not paired yet"} &middot; tgbot.py: ${b.app_status || "unknown"}</div>
    <div class="grid"><${Stat} label="Pending" value=${num(b.pending)} /><${Stat} label="Processing" value=${num(b.processing)} /><${Stat} label="Completed" value=${num(b.completed)} kind="accent" /><${Stat} label="Errors" value=${num(b.errors)} kind=${b.errors ? "bad" : ""} /><${Stat} label="Last heartbeat" value=${ago(b.last_heartbeat)} /></div>
    <div class="row" style=${{ margin: "16px 0" }}><button class="btn ghost" onClick=${regen}>New pairing key</button><button class="btn danger" onClick=${del}>Delete bot</button></div>
    ${key && html`<div class="note mono">New pairing key (shown once): ${key}</div>`}
    <${Files} botId=${id} />
    <div class="card" style=${{ marginTop: 16, padding: 0 }}><h2 style=${{ padding: "14px 16px 0" }}>Recent logs</h2>${b.recent_logs.map((l) => html`<${LogRow} key=${l.id} l=${l} />`)}${!b.recent_logs.length && html`<div class="muted" style=${{ padding: 16 }}>No logs yet</div>`}</div>
    <div class="card" style=${{ marginTop: 16, padding: 0 }}><h2 style=${{ padding: "14px 16px 0" }}>Error history</h2>${b.error_history.map((l) => html`<${LogRow} key=${l.id} l=${l} />`)}${!b.error_history.length && html`<div class="muted" style=${{ padding: 16 }}>No errors</div>`}</div></div>`;
}

function Term({ botId, name }) {
  const [text, setText] = useState("");
  useEffect(() => { api("/bots/" + botId + "/screen").then((r) => setText(r.text || "")).catch(() => {}); }, [botId]);
  useLive(useCallback((m) => { if (m.type === "screen" && m.bot_id === botId) setText(m.text || ""); }, [botId]));
  return html`<div class="card" style=${{ padding: 0, marginBottom: 16 }}><h2 style=${{ padding: "14px 16px 0" }}>${name || "Terminal"}</h2><pre class="term">${text || "Script chalu hone ke baad terminal screen yahan aayegi."}</pre></div>`;
}

function Logs() {
  const [f, setF] = useState({ bot_id: "", level: "", event: "", date_from: "", date_to: "", q: "" });
  const [rows, setRows] = useState([]), [events, setEvents] = useState([]), [bots, setBots] = useState([]), [live, setLive] = useState(true);
  const fref = useRef(f); fref.current = f;
  const load = useCallback(() => { const p = new URLSearchParams(Object.entries(f).filter(([, v]) => v)); p.set("limit", 200); api("/logs?" + p).then((r) => { setRows(r.logs); setEvents(r.events); }); }, [f]);
  useEffect(() => { load(); api("/bots").then(setBots); }, [load]);
  useLive(useCallback((m) => {
    if (m.type !== "log" || !live) return; const g = fref.current, l = m.log;
    if ((g.bot_id && g.bot_id !== l.bot_id) || (g.level && g.level !== l.level) || (g.event && g.event !== l.event) || (g.q && !l.message.toLowerCase().includes(g.q.toLowerCase()))) return;
    setRows((r) => [l, ...r].slice(0, 300));
  }, [live]));
  const set = (k) => (e) => setF({ ...f, [k]: e.target.value });
  return html`<div><h1>Live Logs</h1><div class="sub">Streaming from all agents</div>
    <div class="row"><select value=${f.bot_id} onChange=${set("bot_id")}><option value="">All bots</option>${bots.map((b) => html`<option key=${b.id} value=${b.id}>${b.name}</option>`)}</select>
      <select value=${f.level} onChange=${set("level")}><option value="">All levels</option>${["INFO", "WARNING", "ERROR", "COMMAND", "MESSAGE", "SYSTEM"].map((x) => html`<option key=${x}>${x}</option>`)}</select>
      <select value=${f.event} onChange=${set("event")}><option value="">All events</option>${events.map((x) => html`<option key=${x}>${x}</option>`)}</select>
      <input type="date" value=${f.date_from} onChange=${set("date_from")} /><input type="date" value=${f.date_to} onChange=${set("date_to")} />
      <input placeholder="Search text" value=${f.q} onInput=${set("q")} />
      <button class=${"btn " + (live ? "" : "ghost")} onClick=${() => setLive(!live)}>${live ? "Live: ON" : "Live: paused"}</button></div>
    ${(f.bot_id ? bots.filter((b) => b.id === f.bot_id) : bots).map((b) => html`<${Term} key=${b.id} botId=${b.id} name=${b.name} />`)}
    <div class="card" style=${{ padding: 0 }}>${rows.map((l) => html`<${LogRow} key=${l.id} l=${l} />`)}${!rows.length && html`<div class="muted" style=${{ padding: 16 }}>No logs match</div>`}</div></div>`;
}

function Servers() {
  const [s, setS] = useState([]);
  const load = useCallback(() => api("/servers").then(setS), []);
  useEffect(load, []); useLive(useCallback((m) => { if (m.type === "bot_update") load(); }, [load]));
  return html`<div><h1>Servers</h1><div class="sub">Machines running a monitoring agent</div>
    <div class="tw"><table><thead><tr><th>Server</th><th>Host</th><th>OS</th><th>Bots</th><th>Online</th><th>Pending</th><th>Completed</th><th>Last seen</th></tr></thead><tbody>
    ${s.map((x) => html`<tr key=${x.id}><td><b>${x.name}</b></td><td class="mono">${x.hostname || "-"}</td><td>${x.os || "-"}</td><td>${x.bots}</td><td>${x.online_bots}/${x.bots}</td><td>${num(x.pending)}</td><td>${num(x.completed)}</td><td class="muted">${ago(x.last_seen)}</td></tr>`)}
    ${!s.length && html`<tr><td colspan="8" class="muted">No server has connected yet</td></tr>`}</tbody></table></div></div>`;
}

function Analytics() {
  const [days, setDays] = useState(14), [bot, setBot] = useState(""), [d, setD] = useState([]), [bots, setBots] = useState([]);
  useEffect(() => { api("/bots").then(setBots); }, []);
  useEffect(() => { api(`/analytics/daily?days=${days}&bot_id=${bot}`).then((r) => setD(r.days)); }, [days, bot]);
  const tot = d.reduce((a, x) => a + x.processed, 0);
  return html`<div><h1>Analytics</h1><div class="sub">Daily processing statistics</div>
    <div class="row"><select value=${days} onChange=${(e) => setDays(+e.target.value)}>${[7, 14, 30, 90].map((n) => html`<option key=${n} value=${n}>Last ${n} days</option>`)}</select>
      <select value=${bot} onChange=${(e) => setBot(e.target.value)}><option value="">All bots</option>${bots.map((b) => html`<option key=${b.id} value=${b.id}>${b.name}</option>`)}</select></div>
    <div class="grid" style=${{ marginBottom: 16 }}><${Stat} label=${"Completed in " + days + "d"} value=${num(tot)} kind="accent" /><${Stat} label="Avg / day" value=${num(Math.round(tot / days))} /></div>
    <div class="card"><h2>IDs completed per day</h2><${Bars} days=${d} /></div>
    <div class="card" style=${{ marginTop: 16 }}><h2>Errors per day</h2><${Bars} days=${d} key2="errors" err=${true} /></div></div>`;
}

function Stock() {
  const [s, setS] = useState(null), [text, setText] = useState(""), [msg, setMsg] = useState(""), [busy, setBusy] = useState(false);
  const [count, setCount] = useState("100"), [removeOn, setRemoveOn] = useState(false), [wmsg, setWmsg] = useState(""), [wbusy, setWbusy] = useState(false), [result, setResult] = useState(null), [doneSum, setDoneSum] = useState(null);
  const load = useCallback(() => api("/stock").then(setS).catch((e) => setMsg(e.message)), []);
  const loadDone = useCallback(() => api("/overview").then((o) => setDoneSum(o.completed)).catch(() => {}), []);
  useEffect(() => { load(); loadDone(); }, [load, loadDone]);
  useLive(useCallback((m) => { if (m.type !== "log") loadDone(); }, [loadDone]));
  const add = async () => {
    setBusy(true); setMsg("");
    try { const r = await api("/stock", { method: "POST", body: { text } }); setMsg("Added " + num(r.added) + ", already in stock " + num(r.skipped)); setText(""); load(); }
    catch (e) { setMsg(e.message); }
    setBusy(false);
  };
  const withdraw = async () => {
    const n = parseInt(count, 10);
    if (!n || n < 1 || n > 65000) { setWmsg("Enter a number from 1 to 65000"); return; }
    setWbusy(true); setWmsg("");
    try { setResult(await api("/withdraw", { method: "POST", body: { count: n, remove: removeOn } })); }
    catch (e) { setWmsg(e.message); }
    setWbusy(false);
  };
  const download = () => {
    if (!result) return;
    const blob = new Blob([JSON.stringify(result.accounts, null, 2)], { type: "application/json" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "withdrawal.json";
    a.click();
  };
  return html`<div><h1>Stock</h1><div class="sub">When a bot's accs.json hits 0, it takes ${s ? num(s.batch) : "5000"} accounts from here and starts. If even 1 ID is left, it does not take any.</div>
    <div class="grid" style=${{ marginBottom: 16 }}><${Stat} label="Available" value=${s ? num(s.available) : "..."} kind="accent" /><${Stat} label="Already given to bots" value=${s ? num(s.assigned) : "..."} /></div>
    <div class="card"><h2>Add accounts</h2>
      <p class="muted">Paste JSON <span class="mono">{"uid": "password"}</span> or one <span class="mono">uid:password</span> per line.</p>
      <textarea value=${text} onInput=${(e) => setText(e.target.value)} placeholder='{"8397143103": "PASSWORD"}' style=${{ width: "100%", minHeight: 220, background: "var(--panel2)", color: "var(--text)", border: "1px solid var(--line)", borderRadius: 8, padding: 12, fontFamily: "JetBrains Mono, monospace" }}></textarea>
      <div class="row" style=${{ marginTop: 12, marginBottom: 0 }}><button class="btn" disabled=${busy || !text.trim()} onClick=${add}>${busy ? "Adding..." : "Add to stock"}</button><span class="muted">${msg}</span></div></div>
    <div class="card" style=${{ marginTop: 16 }}><h2>Withdrawal</h2>
      <p class="muted">One total for every bot. 650 means 650 done IDs across all bots, not 650 from each. Remove On deletes them from done_accs.json. Remove Off only shows them.</p>
      <div class="row">
        <input type="number" min="1" max="65000" value=${count} onInput=${(e) => setCount(e.target.value)} style=${{ width: 140 }} />
        <button class=${"btn " + (removeOn ? "" : "ghost")} onClick=${() => setRemoveOn(!removeOn)}>${removeOn ? "Remove: ON" : "Remove: OFF"}</button>
        <button class="btn" disabled=${wbusy} onClick=${withdraw}>${wbusy ? "Working..." : "Withdraw"}</button>
        <div class="card" style=${{ margin: 0, minWidth: 170 }}><div class="muted" style=${{ fontSize: 11, letterSpacing: ".4px", textTransform: "uppercase" }}>Live state</div><div style=${{ fontSize: 28, fontWeight: 700, color: "var(--accent)" }}>${doneSum == null ? "..." : num(doneSum)}</div><div class="muted">done IDs, all bots</div></div>
        <span class="muted">${wmsg}</span>
      </div>
      ${result && html`<div>
        <p>Got <b>${num(result.taken)}</b> of ${num(result.requested)}. ${result.remove ? "These IDs will be removed from done_accs.json." : "done_accs.json was not changed."}</p>
        <p class="muted">${result.by_bot.map((b) => b.bot + ": " + num(b.count)).join(" · ") || "No done IDs on any bot yet."}</p>
        <button class="btn ghost" onClick=${download}>Download JSON</button>
        <pre class="mono" style=${{ background: "var(--panel2)", padding: 12, borderRadius: 8, maxHeight: 280, overflow: "auto", marginTop: 12 }}>${JSON.stringify(result.accounts, null, 2)}</pre>
      </div>`}
    </div></div>`;
}

function Settings({ user }) {
  const [cur, setCur] = useState(""), [nw, setNw] = useState(""), [msg, setMsg] = useState("");
  const save = async () => { try { await api("/auth/password", { method: "POST", body: { current: cur, new: nw } }); setMsg("Password changed"); setCur(""); setNw(""); } catch (e) { setMsg(e.message); } };
  return html`<div><h1>Settings</h1><div class="sub">Signed in as ${user}</div>
    <div class="card" style=${{ maxWidth: 420 }}><h2>Change password</h2>
      <input type="password" style=${{ width: "100%", marginBottom: 10 }} placeholder="Current password" value=${cur} onInput=${(e) => setCur(e.target.value)} />
      <input type="password" style=${{ width: "100%", marginBottom: 10 }} placeholder="New password (10+ characters)" value=${nw} onInput=${(e) => setNw(e.target.value)} />
      <button class="btn" onClick=${save}>Update</button> <span class="muted">${msg}</span></div>
    <div class="card" style=${{ maxWidth: 420, marginTop: 16 }}><h2>Connect an agent</h2><p class="muted">Add Bot &rarr; copy pairing key &rarr; put it plus this server's URL in <span class="mono">monitor_config.json</span> next to the bot's app.py &rarr; run <span class="mono">python start.py</span>.</p>
      <div class="mono">server_url: ${location.origin}</div></div></div>`;
}

function Login({ onIn }) {
  const [u, setU] = useState(""), [p, setP] = useState(""), [err, setErr] = useState("");
  const go = async (e) => { e.preventDefault(); try { const r = await api("/auth/login", { method: "POST", body: { username: u, password: p } }); onIn(r.username); } catch (x) { setErr(x.message); } };
  return html`<form class="login" onSubmit=${go}><div class="brand"><i></i>Bot Control Center</div>
    <input placeholder="Username" value=${u} onInput=${(e) => setU(e.target.value)} autofocus /><input type="password" placeholder="Password" value=${p} onInput=${(e) => setP(e.target.value)} />
    ${err && html`<div class="err">${err}</div>`}<button class="btn" style=${{ width: "100%" }}>Sign in</button></form>`;
}

function App() {
  const [user, setUser] = useState(undefined), [route, setRoute] = useState(location.hash || "#/overview");
  useEffect(() => { api("/auth/me").then((r) => setUser(r.username)).catch(() => setUser(null)); const h = () => setRoute(location.hash || "#/overview"); window.addEventListener("hashchange", h); const o = () => setUser(null); window.addEventListener("logged-out", o); return () => { window.removeEventListener("hashchange", h); window.removeEventListener("logged-out", o); }; }, []);
  useEffect(() => (user ? connectWS() : undefined), [user]);
  if (user === undefined) return null;
  if (!user) return html`<${Login} onIn=${setUser} />`;
  const nav = [["overview", "Overview"], ["bots", "All Bots"], ["logs", "Live Logs"], ["servers", "Servers"], ["analytics", "Analytics"], ["stock", "Stock"], ["settings", "Settings"]];
  const [, page, arg] = route.split("/");
  const view = page === "bot" ? html`<${BotPage} id=${arg} key=${arg} />` : page === "bots" ? html`<${Bots} />` : page === "logs" ? html`<${Logs} />` : page === "servers" ? html`<${Servers} />`
    : page === "analytics" ? html`<${Analytics} />` : page === "stock" ? html`<${Stock} />` : page === "settings" ? html`<${Settings} user=${user} />` : html`<${Overview} />`;
  return html`<div class="layout"><aside class="side"><div class="brand"><i></i>Bot Control</div><nav class="nav">
    ${nav.map(([k, n]) => html`<a key=${k} class=${page === k || (page === "bot" && k === "bots") ? "on" : ""} href=${"#/" + k}>${n}</a>`)}</nav><div class="spacer"></div>
    <a class="nav" style=${{ padding: "9px 12px", color: "var(--mute)", cursor: "pointer" }} onClick=${async () => { await api("/auth/logout", { method: "POST" }); setUser(null); }}>Sign out</a></aside><main class="main">${view}</main></div>`;
}

ReactDOM.createRoot(document.getElementById("root")).render(html`<${App} />`);
