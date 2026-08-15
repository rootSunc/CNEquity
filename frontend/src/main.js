// Lake dashboard. Two views routed on the hash: the tier overview (#/) and one
// dataset (#/dataset/<name>[/state|meta]).
import { disposeAll, heatmap, provenanceSeries, runGantt, severityTimeline } from "./charts.js";

const qs = new URLSearchParams(location.search);
const TOKEN = qs.get("token");
// Keep the default window compact enough to read on the overview.  The API
// supports longer windows through ?days=..., but 250 sessions makes sparse
// snapshot datasets look like a large unexplained blank block.
const DAYS = Number(qs.get("days") || 90);
const app = document.getElementById("app");

const NAV_ITEMS = [
  ["overview", "概览", "#/"],
  ["datasets", "数据集", "#/datasets"],
  ["runs", "跑批", "#/runs"],
  ["quality", "质量", "#/quality"],
];

function pageShell(content, active = "overview") {
  const nav = NAV_ITEMS.map(
    ([key, label, href]) =>
      `<a class="nav-link ${active === key ? "active" : ""}" href="${href}" data-nav="${key}">${label}</a>`,
  ).join("");
  return `<div class="app-shell">
    <header class="topbar">
      <a class="brand-lockup" href="#/" aria-label="返回概览">
        <span class="brand-mark">CML</span>
        <span class="brand-copy"><strong>CNMarketLake</strong><small>research lake</small></span>
      </a>
      <nav class="nav" aria-label="主导航">${nav}</nav>
      <div class="topbar-meta"><span class="console-mode">只读控制台</span>
        <button class="button button-ghost" id="refresh-page" type="button">刷新</button>
      </div>
    </header>
    <main class="page-main">${content}</main>
  </div>`;
}

function setPage(content, active = "overview") {
  app.innerHTML = pageShell(content, active);
  document.getElementById("refresh-page")?.addEventListener("click", () => location.reload());
}

async function api(path) {
  const sep = path.includes("?") ? "&" : "?";
  const url = TOKEN ? `${path}${sep}token=${encodeURIComponent(TOKEN)}` : path;
  const res = await fetch(url);
  if (!res.ok) {
    // Surface what the server said. A 422 here is usually a real contract
    // message ("requires as_of= for point-in-time queries"), and showing the
    // status code instead throws away the one useful part.
    let detail = `${path} → ${res.status}`;
    try {
      const body = await res.json();
      if (body?.detail) detail = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail);
    } catch {
      /* not JSON — keep the status line */
    }
    throw new Error(detail);
  }
  return res.json();
}

const fmt = (n) => (n ?? 0).toLocaleString();
const compact = (n) => new Intl.NumberFormat("zh-CN", { notation: "compact", maximumFractionDigits: 1 }).format(n ?? 0);
const mb = (b) => (!b ? "—" : b >= 1e9 ? `${(b / 1e9).toFixed(1)} GB` : `${(b / 1e6).toFixed(0)} MB`);
const esc = (s) =>
  String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

// --- overview ---------------------------------------------------------------

const kpi = (n, label, note = "") => `<article class="metric-card"><div class="metric-label">${label}</div>
  <div class="metric-value">${n}</div>${note ? `<div class="metric-note">${note}</div>` : ""}</article>`;

const statusPill = (status, label = status) => {
  const tone = status === "fresh" || status === "success" ? "fresh" : status === "stale" || status === "failed" ? "stale" : status === "empty" ? "empty" : "running";
  return `<span class="status-pill status-pill-${tone}"><span class="dot ${tone === "running" ? "" : tone}"></span>${esc(label)}</span>`;
};

// storage/stats.py emits exactly two of these; anything else falls through
// unchanged rather than being silently mistranslated.
function statsReason(reason) {
  if (!reason) return "原因未知";
  if (reason.includes("landed after the stats were built")) return "有新的采集批次晚于度量表";
  if (reason.includes("no stats yet")) return "尚未生成过度量表";
  return reason;
}

async function renderOverview() {
  const [h, tiers, datasets, hm] = await Promise.all([
    api("/api/health"),
    api("/api/tiers"),
    api("/api/datasets"),
    api(`/api/heatmap?days=${DAYS}`),
  ]);
  const sev = h.findings_by_severity || {};
  const notes = [];
  // The reason strings come from the ingestion layer and are English, with a
  // run UUID in them. Fine in a log, wrong in the banner of a Chinese page —
  // the id is not something the reader can act on. Translate the ones that can
  // actually appear, keep the original as a tooltip for anyone debugging.
  if (h.stats_stale) notes.push(`度量表过期（${esc(statsReason(h.stats_reason))}）——已在后台重建，稍后刷新。`);
  if (h.stale_datasets.length) {
    notes.push(
      `STALE：${h.stale_datasets.map((d) => `<code class="ds-link" data-ds="${esc(d)}">${esc(d)}</code>`).join(" ")}`,
    );
  }
  if (h.empty_required.length) {
    notes.push(`必需但为空：${h.empty_required.map((d) => `<code>${esc(d)}</code>`).join(" ")}`);
  }

  const byTier = {};
  for (const d of datasets) (byTier[d.tier] ||= []).push(d);

  const state = sev.error ? "error" : notes.length ? "attention" : "healthy";
  const stateLabel = { healthy: "运行正常", attention: "需要关注", error: "存在错误" }[state];
  const actionItems = notes.length
    ? notes.map((note) => `<li class="issue-item">${note}</li>`).join("")
    : `<li class="empty-state"><span class="empty-icon">✓</span><span><strong>没有待处理事项</strong><small>数据、度量表与审计快照均在预期状态。</small></span></li>`;
  const tierCards = tiers
    .map(
      (t) => `<details class="tier-card" ${t.stale || t.empty ? "open" : ""}>
        <summary><span class="tier-summary"><span class="tier-name"><span class="tier-tag">${esc(t.tier)}</span>${esc(t.label)}</span>
          <span class="tier-counts"><b>${t.datasets}</b> 个数据集 · <b>${fmt(t.rows)}</b> 行</span>
          <span class="tier-status ${t.stale ? "is-stale" : t.empty ? "is-empty" : "is-fresh"}">${t.stale ? `${t.stale} stale` : t.empty ? `${t.empty} empty` : "全部 fresh"}</span></span></summary>
        <div class="tier-members">${membersTable(byTier[t.tier] || [])}</div>
      </details>`,
    )
    .join("");

  const visibleHeatmapRows = hm.rows.filter((row) => /[#.]/.test(row.cells));
  const hiddenHeatmapRows = hm.rows.length - visibleHeatmapRows.length;
  const heatmapData = { ...hm, rows: visibleHeatmapRows };

  setPage(`
    <section class="page-heading">
      <div class="eyebrow">数据湖控制台 / 概览</div>
      <div class="heading-row"><div><h1>湖状态</h1>
        <p class="sub">最后交易日 ${esc(h.anchor)} · ${h.datasets} 个注册数据集 · 审计快照 ${esc(h.audit_trade_date || "无")}</p></div>
        <div class="action-row"><a class="button button-ghost" href="#/runs">查看跑批</a><a class="button button-primary" href="#/quality">查看质量</a></div>
      </div>
    </section>
    <section class="status-hero status-${state}" aria-live="polite">
      <span class="status-icon" aria-hidden="true">${state === "healthy" ? "✓" : state === "error" ? "!" : "•"}</span>
      <div><strong>${stateLabel}</strong><span>${state === "healthy" ? "核心数据集已覆盖最新交易日。" : `${notes.length} 项事项需要核查，数据仍可只读访问。`}</span></div>
      <span class="status-anchor">anchor ${esc(h.anchor || "—")}</span>
    </section>
    <section class="metric-grid" aria-label="关键指标">
      ${kpi(h.datasets, "数据集", "已注册")}
      ${kpi(h.fresh, "Fresh", "最新水位")}
      ${kpi(`<span class="${h.stale ? "err" : ""}">${h.stale}</span>`, "Stale", "超过容忍窗口")}
      ${kpi(`<span title="${fmt(h.rows)} 行">${compact(h.rows)}</span>`, "行数", "curated")}
      ${kpi(mb(h.bytes), "存储", "curated")}
      ${kpi(`<span class="${sev.error ? "err" : ""}">${sev.error || 0}</span><span class="metric-secondary"> / ${sev.warning || 0}</span>`, "审计", "error / warning")}
    </section>
    <section class="overview-grid">
      <article class="surface-panel heat-panel"><div class="panel-header"><div><div class="eyebrow">Coverage</div><h2>覆盖热力</h2></div><span class="panel-meta">${visibleHeatmapRows.length}/${hm.rows.length} 个数据集 · ${hm.days.length} 个交易日</span></div>
        <div id="heat" aria-label="覆盖热力图"></div>
        <p class="legend"><span><i class="swatch" style="background:var(--cell-covered)"></i> 有分区覆盖</span><span><i class="swatch" style="background:var(--cell-gap)"></i> 日更源缺口</span><span><i class="swatch" style="background:var(--cell-cadence)"></i> 按源节奏间隔</span><span><i class="swatch" style="background:var(--cell-outside)"></i> 区间外</span></p>
        <p class="heatmap-note">灰色表示当前窗口外，或该数据集按快照 / 月度 / 季度节奏采集，不等同于采集失败。${hiddenHeatmapRows ? ` ${hiddenHeatmapRows} 个当前没有分区的数据集已留在“数据层”中。` : ""}</p>
      </article>
      <aside class="surface-panel action-panel"><div class="panel-header"><div><div class="eyebrow">Attention</div><h2>行动项</h2></div><span class="panel-meta">${notes.length || 0} 项</span></div><ul class="issue-list">${actionItems}</ul></aside>
    </section>
    <section class="surface-panel dataset-panel" id="dataset-list"><div class="panel-header"><div><div class="eyebrow">Data catalog</div><h2>数据层</h2></div><a class="panel-link" href="#/datasets">查看全部数据集 →</a></div><div class="tier-grid">${tierCards}</div></section>
  `);
  heatmap(document.getElementById("heat"), heatmapData);
}

async function renderDatasets() {
  const datasets = await api("/api/datasets");
  const rows = (items) => items.map((d) => `<tr class="dataset-row"><td><span class="dot ${d.freshness === "fresh" ? "fresh" : d.freshness === "stale" ? "stale" : "empty"}"></span><span class="ds-link" data-ds="${esc(d.dataset)}">${esc(d.dataset)}</span></td><td>${esc(d.tier_label || d.tier)}</td><td>${esc(d.history_mode)}</td><td>${esc(d.granularity || "merge")}</td><td>${esc(d.watermark || "—")}</td><td class="n">${fmt(d.row_count)}</td><td class="n">${mb(d.bytes)}</td></tr>`).join("");
  setPage(`<section class="page-heading"><div class="eyebrow">数据湖控制台 / 数据集</div><div class="heading-row"><div><h1>数据集</h1><p class="sub">按注册契约浏览 ${datasets.length} 个数据集，点击名称查看状态、元数据与数据。</p></div></div></section>
    <section class="surface-panel catalog-panel"><div class="catalog-toolbar"><label class="search-field"><span aria-hidden="true">⌕</span><input id="dataset-search" type="search" placeholder="搜索数据集、层级或语义" autocomplete="off"></label><span class="panel-meta" id="dataset-count">${datasets.length} 个结果</span></div><div class="scroll"><table id="dataset-table"><thead><tr><th>数据集</th><th>分层</th><th>语义</th><th>粒度</th><th>水位</th><th class="n">行</th><th class="n">体积</th></tr></thead><tbody></tbody></table></div></section>`, "datasets");
  const table = document.querySelector("#dataset-table tbody");
  const count = document.getElementById("dataset-count");
  const render = (query = "") => {
    const q = query.trim().toLowerCase();
    const filtered = datasets.filter((d) => [d.dataset, d.tier, d.tier_label, d.history_mode, d.granularity].some((v) => String(v || "").toLowerCase().includes(q)));
    table.innerHTML = filtered.length ? rows(filtered) : '<tr><td colspan="7" class="empty-table">没有匹配的数据集。</td></tr>';
    count.textContent = `${filtered.length} 个结果`;
  };
  document.getElementById("dataset-search").addEventListener("input", (e) => render(e.target.value));
  render();
}

function membersTable(rows) {
  const head = `<tr><th>数据集</th><th>语义</th><th>粒度</th><th>覆盖</th>
    <th>水位</th><th class="n">行</th><th class="n">体积</th></tr>`;
  const body = rows
    .map((d) => {
      const cls = d.freshness === "fresh" ? "fresh" : d.freshness === "stale" ? "stale" : "empty";
      const cover = d.coverage_start ? `${d.coverage_start} → ${d.coverage_end}` : "—";
      const opt = d.required ? "" : " <span style='opacity:.6'>(可选)</span>";
      return `<tr><td><span class="dot ${cls}"></span><span class="ds-link" data-ds="${esc(d.dataset)}">${esc(d.dataset)}</span>${opt}</td>
      <td>${d.history_mode}</td><td>${d.granularity || "merge"}</td><td>${cover}</td>
      <td>${d.watermark || "—"}</td><td class="n">${fmt(d.row_count)}</td>
      <td class="n">${mb(d.bytes)}</td></tr>`;
    })
    .join("");
  return `<table>${head}${body}</table>`;
}

// --- dataset detail ---------------------------------------------------------

function coverageBar(d) {
  if (!d.coverage_start) return '<p class="muted">尚无分区。</p>';
  const start = new Date(d.coverage_start).getTime();
  const end = new Date(d.coverage_end).getTime();
  const span = Math.max(end - start, 1);
  let horizon = "";
  if (d.earliest_available) {
    const h = new Date(d.earliest_available).getTime();
    if (h > start && h < end) {
      const pct = ((h - start) / span) * 100;
      horizon = `<div class="horizon" style="left:${pct}%" title="源端历史天花板 ${d.earliest_available}"></div>`;
    }
  }
  return `<div class="cover"><div class="fill" style="left:0;right:0"></div>${horizon}</div>
    <p class="legend"><span>${d.coverage_start}</span><span style="margin-left:auto">${d.coverage_end}</span></p>`;
}

function gapsNote(d) {
  const g = d.gaps;
  if (!g.total) return '<p class="muted">覆盖区间内无缺口。</p>';
  const cadence =
    d.max_staleness_days > 1 ? `　该源非日更（容忍 ${d.max_staleness_days} 天），间隔属其节奏。` : "";
  return `<p class="${d.max_staleness_days > 1 ? "muted" : "err"}">${g.total} 个 ${g.unit} 无分区${cadence}</p>
    <p class="muted">${g.missing.slice(0, 12).map(esc).join("、")}${g.total > 12 ? " …" : ""}</p>`;
}

function stateTab(d, prov) {
  const provTable = prov.length
    ? `<table>
    <tr><th>source</th><th>data_version</th><th class="n">行</th><th>fetched_at 跨度</th></tr>
    ${prov
      .map(
        (p) => `<tr><td>${esc(p.source)}</td><td>${esc(p.data_version)}</td>
      <td class="n">${fmt(p.row_count)}</td>
      <td class="muted">${(p.fetched_at_min || "").slice(0, 10)} → ${(p.fetched_at_max || "").slice(0, 10)}</td></tr>`,
      )
      .join("")}
    </table>`
    : '<p class="muted">无溯源度量。</p>';

  const findings = d.findings.length
    ? `<table>
    <tr><th>severity</th><th>check</th><th>message</th></tr>
    ${d.findings
      .map(
        (f) => `<tr><td class="${f.severity === "error" ? "err" : ""}">${esc(f.severity)}</td>
      <td>${esc(f.check)}</td><td>${esc(f.message)}</td></tr>`,
      )
      .join("")}</table>`
    : '<p class="muted">上次审计没有该数据集的 findings。</p>';

  const batches = d.batches.length
    ? `<div class="scroll"><table>
    <tr><th>状态</th><th>窗口</th><th class="n">写入</th><th class="n">重试</th><th>开始</th><th>错误</th></tr>
    ${d.batches
      .map(
        (b) => `<tr><td class="${b.status === "success" ? "" : "err"}">${esc(b.status)}</td>
      <td class="muted">${esc(b.window_start || "—")} → ${esc(b.window_end || "—")}</td>
      <td class="n">${fmt(b.rows_written)}</td><td class="n">${b.retry_count || ""}</td>
      <td class="muted">${esc((b.started_at || "").slice(0, 19))}</td>
      <td class="muted">${esc((b.error_message || "").slice(0, 90))}</td></tr>`,
      )
      .join("")}</table></div>`
    : '<p class="muted">manifest 中没有该数据集的 batch。</p>';

  return `
    <h3>覆盖</h3>${coverageBar(d)}${gapsNote(d)}
    <h3>溯源分布（按时间）</h3><div id="prov"></div><p class="muted" id="provnote"></p>
    <h3>溯源合计</h3>${provTable}
    <h3>审计 findings</h3>${findings}
    <h3>最近 batch</h3>${batches}`;
}

const fact = (k, v) => `<div class="fact"><span class="k">${esc(k)}</span><span class="v">${v}</span></div>`;

/** What one row covers.
 *
 * Was keyed on `intraday`, the bar-frequency field — which `trade_ticks`
 * deliberately leaves unset so it cannot inherit bar-shaped checks. That
 * printed a dash, making intraday transaction records look like a daily
 * dataset. `row_grain` is set for all three intraday datasets.
 */
function grainText(d) {
  if (d.row_grain === "tick") return "分笔（3 秒快照聚合，非 bar）";
  if (d.row_grain) return `${esc(d.row_grain)} bar`;
  return "日";
}

/** How far back the *source* still serves, and by which mechanism.
 *
 * Two different limits reach this panel and they must not read the same. A
 * rolling per-symbol count (minute bars) moves forward every day; a fixed
 * calendar floor (trade_ticks) does not, and its horizon therefore grows.
 * Keying only on `history_horizon_days` printed "无上限" for the floor case —
 * directly contradicting the 最早可得 line right underneath it.
 */
function horizonText(d) {
  if (d.history_floor_date) return `自 ${esc(d.history_floor_date)} 起（固定底，不滚动）`;
  if (d.history_horizon_days) return `${d.history_horizon_days} 个交易日（滚动）`;
  return "无上限";
}

function metaTab(d) {
  const yn = (b) => (b ? "是" : "否");
  const contract = [
    fact("分层", `${d.tier} ${esc(d.tier_label)}`),
    fact("存储层", d.layer),
    fact("分区键", d.partition_col ? `<code>${esc(d.partition_col)}</code>` : "—（单文件 merge）"),
    fact("分区粒度", d.granularity || "—"),
    fact("查询日期列", d.date_col ? `<code>${esc(d.date_col)}</code>` : "—"),
    fact("主键", d.primary_key.map((c) => `<code>${esc(c)}</code>`).join(" ")),
  ].join("");
  const semantics = [
    fact("fetch_semantics", d.fetch_semantics),
    fact("history_mode", d.history_mode),
    fact("PIT", yn(d.pit)),
    fact("维护水位", yn(d.watermarked)),
  ].join("");
  const sources = [
    fact("回填源", d.backfill_source ? esc(d.backfill_source) : "—"),
    fact("源端历史视野", horizonText(d)),
    fact("最早可得", d.earliest_available || "不受源端限制"),
  ].join("");
  const ops = [
    fact("staleness 容忍", `${d.max_staleness_days} 天`),
    fact("required", yn(d.required)),
    fact("行粒度", grainText(d)),
    fact(
      "回填分块",
      d.backfill_chunk_days
        ? `${d.backfill_chunk_days} 天`
        : d.backfill_chunk_symbols
          ? `${d.backfill_chunk_symbols} 标的`
          : "—",
    ),
  ].join("");

  const schema = `<div class="scroll"><table>
    <tr><th>列</th><th>类型</th><th>主键</th></tr>
    ${d.schema
      .map(
        (c) => `<tr><td><code>${esc(c.column)}</code></td><td class="muted">${esc(c.dtype)}</td>
      <td>${d.primary_key.includes(c.column) ? "✓" : ""}</td></tr>`,
      )
      .join("")}</table></div>`;

  const cmds = d.commands
    .map(
      (c, i) => `<div class="cmd"><code id="cmd${i}">${esc(c.cmd)}</code>
      <button data-copy="cmd${i}">复制</button><span class="muted">${esc(c.why)}</span></div>`,
    )
    .join("");

  return `
    <h3>契约</h3><div class="facts">${contract}</div>
    <h3>语义</h3><div class="facts">${semantics}</div>
    <h3>来源</h3><div class="facts">${sources}</div>
    <h3>运维</h3><div class="facts">${ops}</div>
    <h3>Schema</h3>${schema}
    <h3>命令</h3>${cmds}
    <p class="muted">以上全部来自 <code>domain/datasets.py</code> 与 <code>domain/schemas.py</code>；面板不复制一份。</p>`;
}

// --- data tab ---------------------------------------------------------------

const KIND_LABEL = {
  trading_day: "交易日",
  event_day: "事件日",
  period: "周期",
  report_period: "报告期",
  none: "",
};

/**
 * The date control is chosen by the server's `kind`, not assumed.
 *
 * The registry spans twelve date columns in four shapes; a calendar widget over
 * `report_period` would invite a query the column cannot answer, and one over a
 * sparse event column would mostly offer days with nothing behind them. Only
 * values that exist are listed.
 */
function dataControls(d, dates) {
  const picker =
    dates.kind === "none"
      ? ""
      : `<label>${KIND_LABEL[dates.kind]}
        <select id="q-period">${dates.values
          .map((v) => `<option value="${esc(v)}">${esc(v)}</option>`)
          .join("")}</select></label>`;
  const symbol = `<label>标的 <input id="q-symbol" placeholder="600519.SH" size="12"></label>`;
  // PIT datasets have no default "current" view — load() refuses without a
  // cutoff, on purpose. Seed it with today so the tab opens on something, and
  // say what it means.
  const asOf = d.pit
    ? `<label title="PIT：只保留在该日之前已披露的事实，并取当时现行的那一版">
         as_of <input id="q-asof" type="date" value="${new Date().toISOString().slice(0, 10)}"></label>`
    : "";
  const adjust = d.adjustable
    ? `<label>复权 <select id="q-adjust">
         <option value="">不复权</option><option value="hfq">hfq</option>
         <option value="qfq">qfq</option></select></label>`
    : "";
  const note = dates.note ? `<p class="muted">${esc(dates.note)}</p>` : "";
  return `<div class="controls">${picker}${symbol}${asOf}${adjust}
    <button id="q-run">查询</button></div>${note}`;
}

function rowTable(page, primaryKey) {
  if (!page.rows.length) return '<p class="muted">没有匹配的行。</p>';
  const head = page.columns
    .map((c) => `<th${primaryKey.includes(c) ? ' class="pk"' : ""}>${esc(c)}</th>`)
    .join("");
  const body = page.rows
    .map((r) => `<tr>${r.map((v) => `<td>${v === null ? '<span class="muted">null</span>' : esc(v)}</td>`).join("")}</tr>`)
    .join("");
  const shown = `${page.offset + 1}–${page.offset + page.rows.length} / ${fmt(page.total)}`;
  return `<div class="scroll"><table class="rows"><tr>${head}</tr>${body}</table></div>
    <div class="controls">
      <button id="q-prev" ${page.offset === 0 ? "disabled" : ""}>上一页</button>
      <button id="q-next" ${page.offset + page.limit >= page.total ? "disabled" : ""}>下一页</button>
      <span class="muted">${shown}</span>
    </div>`;
}

async function dataTab(d, host) {
  const enc = encodeURIComponent(d.dataset);
  const dates = await api(`/api/datasets/${enc}/dates`);
  host.innerHTML = `${dataControls(d, dates)}<div id="q-out"></div>`;

  const state = { offset: 0 };
  const out = document.getElementById("q-out");

  async function run() {
    const params = new URLSearchParams();
    const period = document.getElementById("q-period")?.value;
    const symbol = document.getElementById("q-symbol")?.value.trim();
    const asOf = document.getElementById("q-asof")?.value;
    const adjust = document.getElementById("q-adjust")?.value;
    if (period) params.set("period", period);
    if (symbol) params.set("symbol", symbol);
    if (asOf) params.set("as_of", asOf);
    if (adjust) params.set("adjust", adjust);
    params.set("offset", String(state.offset));
    out.innerHTML = '<p class="muted">查询中…</p>';
    try {
      const page = await api(`/api/datasets/${enc}/rows?${params}`);
      out.innerHTML = rowTable(page, d.primary_key);
      const prev = document.getElementById("q-prev");
      const next = document.getElementById("q-next");
      if (prev) prev.onclick = () => { state.offset = Math.max(0, state.offset - page.limit); run(); };
      if (next) next.onclick = () => { state.offset += page.limit; run(); };
    } catch (err) {
      out.innerHTML = `<p class="err">${esc(err.message)}</p>`;
    }
  }

  document.getElementById("q-run").onclick = () => {
    state.offset = 0;
    run();
  };
  run();
}

async function renderDetail(name, tab) {
  setPage(`<div class="loading-state"><span class="spinner" aria-hidden="true"></span><span>加载 ${esc(name)}…</span></div>`, "datasets");
  const enc = encodeURIComponent(name);
  const [d, series, prov] = await Promise.all([
    api(`/api/datasets/${enc}`),
    api(`/api/datasets/${enc}/provenance/series`),
    api(`/api/datasets/${enc}/provenance`),
  ]);
  const cls = d.freshness === "fresh" ? "fresh" : d.freshness === "stale" ? "stale" : "empty";
  setPage(`
    <section class="page-heading">
      <div class="eyebrow">数据湖控制台 / 数据集 / ${esc(d.tier)}</div>
      <div class="heading-row"><div><div class="page-title-with-status"><h1>${esc(d.dataset)}</h1>${statusPill(cls)}</div>
        <p class="sub">${esc(d.tier_label)} · ${esc(d.layer)} · ${esc(d.history_mode)}${d.required ? "" : " · 可选数据集"}</p></div>
        <div class="action-row"><a class="button button-ghost" href="#/datasets">← 返回数据集</a></div>
      </div>
    </section>
    <section class="metric-grid detail-metrics" aria-label="数据集关键指标">
      ${kpi(`<span title="${fmt(d.row_count)} 行">${compact(d.row_count)}</span>`, "行数", "curated")}
      ${kpi(mb(d.bytes), "存储", "curated")}
      ${kpi(esc(d.watermark || "—"), "水位", d.watermarked ? "维护中" : "不维护水位")}
      ${kpi(esc(d.coverage_end || "—"), "覆盖至", d.granularity || "merge")}
    </section>
    <section class="surface-panel detail-workspace">
    <nav class="tabs" aria-label="数据集详情标签页">
      ${["state", "meta", "data"]
        .map(
          (t) =>
            `<a class="tab ${tab === t ? "on" : ""}" href="#/dataset/${enc}/${t}" aria-current="${tab === t ? "page" : "false"}">${
              { state: "状态", meta: "元数据", data: "数据" }[t]
            }</a>`,
        )
        .join("")}
    </nav>
    <div id="tabbody" class="tab-body">${tab === "meta" ? metaTab(d) : tab === "data" ? "" : stateTab(d, prov)}</div>
    </section>`, "datasets");

  if (tab === "state") {
    provenanceSeries(document.getElementById("prov"), series);
    document.getElementById("provnote").textContent = `每点跨度：${series.bucket}`;
  } else if (tab === "data") {
    await dataTab(d, document.getElementById("tabbody"));
  }

  app.querySelectorAll("button[data-copy]").forEach((b) => {
    b.onclick = () => navigator.clipboard?.writeText(document.getElementById(b.dataset.copy).textContent);
  });
}

// --- runs -------------------------------------------------------------------

const AGO = (iso) => {
  if (!iso) return "—";
  const secs = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (secs < 90) return `${Math.round(secs)} 秒前`;
  if (secs < 5400) return `${Math.round(secs / 60)} 分钟前`;
  if (secs < 172800) return `${Math.round(secs / 3600)} 小时前`;
  return `${Math.round(secs / 86400)} 天前`;
};

const DURATION = (a, b) => {
  if (!a) return "—";
  const secs = Math.max(0, ((b ? new Date(b) : new Date()).getTime() - new Date(a).getTime()) / 1000);
  return secs < 90 ? `${Math.round(secs)}s` : `${Math.round(secs / 60)}m`;
};

async function renderRuns() {
  const runs = await api("/api/runs?limit=40");
  const succeeded = runs.filter((r) => r.status === "success").length;
  const running = runs.filter((r) => r.status === "running").length;
  const failed = runs.filter((r) => r.status === "failed").length;
  const written = runs.reduce((sum, r) => sum + (r.rows_written || 0), 0);
  const rows = runs
    .map((r) => {
      const bad = Object.keys(r.batch_status).some((s) => s === "failed" || s === "stale");
      const tally = Object.entries(r.batch_status)
        .map(([s, n]) => `${s} ${n}`)
        .join(" · ");
      return `<tr>
        <td><a class="table-link" href="#/runs/${encodeURIComponent(r.run_id)}">${esc(r.job_name)}</a></td>
        <td>${statusPill(r.status)}</td>
        <td class="muted">${AGO(r.started_at)}</td>
        <td class="n">${DURATION(r.started_at, r.finished_at)}</td>
        <td class="n">${fmt(r.rows_written)}</td>
        <td class="muted ${bad ? "err" : ""}">${esc(tally) || "—"}</td>
        <td class="muted">${esc((r.error_message || "").slice(0, 60))}</td>
      </tr>`;
    })
    .join("");

  setPage(`
    <section class="page-heading"><div class="eyebrow">数据湖控制台 / 跑批</div><div class="heading-row"><div><h1>跑批</h1><p class="sub">最近 ${runs.length} 个 run，查看执行状态、写入规模与 batch 时间线。</p></div></div></section>
    <section class="metric-grid run-metrics" aria-label="跑批关键指标">
      ${kpi(runs.length, "最近运行", "最多 40 个")}
      ${kpi(succeeded, "成功", "success")}
      ${kpi(`<span class="${running ? "live" : ""}">${running}</span>`, "运行中", "running")}
      ${kpi(`<span class="${failed ? "err" : ""}">${failed}</span>`, "失败", "failed")}
      ${kpi(`<span title="${fmt(written)} 行">${compact(written)}</span>`, "累计写入", "当前列表")}
    </section>
    <section class="surface-panel table-panel"><div class="panel-header"><div><div class="eyebrow">Run history</div><h2>运行记录</h2></div><span class="panel-meta">点击任务名查看详情</span></div><div class="scroll"><table class="data-table">
      <tr><th>job</th><th>状态</th><th>开始</th><th class="n">耗时</th>
          <th class="n">写入</th><th>batch</th><th>错误</th></tr>
      ${rows || '<tr><td colspan="7" class="empty-table">还没有运行记录。</td></tr>'}
    </table></div></section>`, "runs");
}

let runStream = null;
let runTicker = null;
let lastRunDetail = null;

function closeRunStream() {
  if (runStream) {
    runStream.close();
    runStream = null;
  }
  if (runTicker) {
    clearInterval(runTicker);
    runTicker = null;
  }
  lastRunDetail = null;
}

/**
 * Advance the clock between stream frames.
 *
 * The stream only fires when the manifest changes, and a batch heartbeats far
 * less often than once a second — measured at one frame in 70s on an intraday
 * backfill. Without a local tick the elapsed time and the running bar's leading
 * edge sit frozen under a badge that says 实时, which is worse than not
 * claiming it. The data still comes from the stream; only "now" moves here.
 */
function startRunTicker() {
  if (runTicker) clearInterval(runTicker);
  let ticks = 0;
  runTicker = setInterval(() => {
    if (!lastRunDetail || lastRunDetail.status !== "running") return;
    const el = document.getElementById("run-status");
    if (!el) return closeRunStream();
    paintRunStatus(lastRunDetail);
    // The bar's right edge is "now" too, but redrawing a canvas every second
    // to move it a few pixels is not worth it.
    if (++ticks % 5 === 0) runGantt(document.getElementById("run-gantt"), lastRunDetail);
  }, 1000);
}

function paintRunStatus(detail) {
  const live = detail.status === "running";
  document.getElementById("run-status").innerHTML =
    `${esc(detail.status)}${live ? ' <span class="live">● 实时</span>' : ""}
     · ${DURATION(detail.started_at, detail.finished_at)}
     · ${fmt(detail.rows_written)} 行`;
}

function paintRun(detail) {
  lastRunDetail = detail;
  paintRunStatus(detail);

  const stalled = detail.batches.filter((b) => b.stalled);
  document.getElementById("run-note").innerHTML = stalled.length
    ? `<div class="banner">${stalled.length} 个 batch 仍是 running 但已静默超过
       ${Math.round(detail.stale_after_seconds / 60)} 分钟——下次 run 会把它们判为 failed：
       ${stalled.map((b) => `<code>${esc(b.dataset)}</code>`).join(" ")}</div>`
    : "";

  runGantt(document.getElementById("run-gantt"), detail);

  const failed = detail.batches.filter((b) => b.error_message);
  document.getElementById("run-errors").innerHTML = failed.length
    ? `<section class="surface-panel report-panel"><div class="panel-header"><div><div class="eyebrow">Failures</div><h2>失败的 Batch</h2></div><span class="panel-meta">${failed.length} 项</span></div><div class="scroll"><table>
        <tr><th>数据集</th><th>状态</th><th class="n">重试</th><th>错误</th></tr>
        ${failed
          .map(
            (b) => `<tr><td>${esc(b.dataset)}</td><td class="err">${esc(b.status)}</td>
            <td class="n">${b.retry_count || ""}</td>
            <td class="muted">${esc(b.error_message)}</td></tr>`,
          )
          .join("")}</table></div></section>`
    : "";
}

async function renderRunDetail(runId) {
  const detail = await api(`/api/runs/${encodeURIComponent(runId)}`);
  setPage(`
    <section class="page-heading"><div class="eyebrow">数据湖控制台 / 跑批 / 运行详情</div><div class="heading-row"><div><h1>${esc(detail.job_name)}</h1><p class="sub"><span id="run-status"></span> · <code>${esc(detail.run_id)}</code></p></div><div class="action-row"><a class="button button-ghost" href="#/runs">← 返回跑批</a></div></div></section>
    <section class="metric-grid detail-metrics" aria-label="运行关键指标">
      ${kpi(DURATION(detail.started_at, detail.finished_at), "耗时", detail.status === "running" ? "持续更新" : "已结束")}
      ${kpi(`<span title="${fmt(detail.rows_written)} 行">${compact(detail.rows_written)}</span>`, "写入", "行")}
      ${kpi(detail.batches.length, "Batch", "数据集任务")}
      ${kpi(AGO(detail.started_at), "开始", esc((detail.started_at || "").slice(0, 19)))}
    </section>
    <div id="run-note"></div>
    <section class="surface-panel report-panel"><div class="panel-header"><div><div class="eyebrow">Timeline</div><h2>Batch 时间线</h2></div><span class="panel-meta">按数据集分道</span></div><div id="run-gantt"></div>
    <p class="legend">
      <span><i class="swatch" style="background:var(--cell-covered)"></i> success</span>
      <span><i class="swatch" style="background:var(--series-1)"></i> running</span>
      <span><i class="swatch" style="background:var(--cell-gap)"></i> failed</span>
      <span><i class="swatch" style="background:var(--series-4)"></i> stale</span>
      <span>橙色描边 = 有重试；斜纹 = 已静默</span>
    </p></section>
    <div id="run-errors" class="report-stack"></div>`, "runs");
  paintRun(detail);

  // Only a running job can change. Subscribing to a finished one would hold a
  // connection open for events that can never arrive.
  if (detail.status !== "running") return;
  startRunTicker();
  const url = TOKEN
    ? `/api/stream/runs/${encodeURIComponent(runId)}?token=${encodeURIComponent(TOKEN)}`
    : `/api/stream/runs/${encodeURIComponent(runId)}`;
  runStream = new EventSource(url);
  runStream.onmessage = (e) => {
    // Ignore late frames for a run the user has already navigated away from.
    if (!location.hash.includes(runId)) return closeRunStream();
    paintRun(JSON.parse(e.data));
  };
  runStream.onerror = () => closeRunStream();
}

// --- quality ----------------------------------------------------------------

async function renderQuality() {
  const q = await api("/api/quality");
  const latestFindings = q.findings_runs[0]?.by_severity || {};
  const quarantineFiles = q.quarantine.reduce((sum, item) => sum + (item.files || 0), 0);
  const quarantineBytes = q.quarantine.reduce((sum, item) => sum + (item.bytes || 0), 0);
  const cacheEntries = q.on_demand.reduce((sum, item) => sum + (item.entries || 0), 0);

  const findingsRows = q.findings_runs
    .map((r) => {
      const sev = r.by_severity;
      return `<tr>
        <td><a class="table-link" href="#/quality/${encodeURIComponent(r.run_id)}">${esc(r.trade_date || "—")}</a></td>
        <td class="n ${sev.error ? "err" : ""}">${sev.error || ""}</td>
        <td class="n">${sev.warning || ""}</td>
        <td class="n muted">${sev.info || ""}</td>
        <td class="muted">${r.top_checks.map(([c, n]) => `${esc(c)}×${n}`).join("、")}</td>
      </tr>`;
    })
    .join("");

  const diffRows = q.diff_runs
    .map(
      (r) => `<tr>
        <td><a class="table-link" href="#/quality/${encodeURIComponent(r.run_id)}">${esc(r.trade_date || "—")}</a></td>
        <td class="n">${r.diff_count}</td>
        <td class="muted">${Object.entries(r.by_check).map(([c, n]) => `${esc(c)}×${n}`).join("、")}</td>
      </tr>`,
    )
    .join("");

  const quarantineRows = q.quarantine.length
    ? q.quarantine
        .map(
          (e) => `<tr><td><code>${esc(e.name)}</code></td><td class="n">${fmt(e.files)}</td>
            <td class="n">${mb(e.bytes)}</td><td class="muted">${esc(e.modified.slice(0, 10))}</td></tr>`,
        )
        .join("")
    : `<tr><td colspan="4" class="muted">隔离区是空的。</td></tr>`;

  const onDemandRows = q.on_demand.length
    ? q.on_demand
        .map(
          (e) => `<tr><td>${esc(e.dataset)}</td><td class="n">${fmt(e.entries)}</td>
            <td class="n">${mb(e.bytes)}</td><td class="muted">${esc((e.newest || "").slice(0, 10)) || "—"}</td></tr>`,
        )
        .join("")
    : `<tr><td colspan="4" class="muted">还没有人查过 on-demand 数据集（<code>stock_news</code> /
       <code>research_reports</code>）——这是正常状态，不是缺口。</td></tr>`;

  setPage(`
    <section class="page-heading"><div class="eyebrow">数据湖控制台 / 质量</div><div class="heading-row"><div><h1>质量</h1><p class="sub">审计、跨源比对、隔离区与按需缓存的只读证据面板。</p></div></div></section>
    <section class="metric-grid quality-metrics" aria-label="质量关键指标">
      ${kpi(q.findings_runs.length, "审计快照", q.findings_runs[0]?.trade_date || "暂无")}
      ${kpi(`<span class="${latestFindings.error ? "err" : ""}">${latestFindings.error || 0}</span>`, "最新错误", "error")}
      ${kpi(latestFindings.warning || 0, "最新警告", "warning")}
      ${kpi(quarantineFiles, "隔离文件", mb(quarantineBytes))}
      ${kpi(cacheEntries, "按需缓存", "entries")}
    </section>
    <div class="report-stack">
    <section class="surface-panel report-panel"><div class="panel-header"><div><div class="eyebrow">Audit findings</div><h2>审计趋势</h2></div><span class="panel-meta">按审计日</span></div>
    <div id="sev-chart"></div>
    <div class="scroll"><table>
      <tr><th>审计日</th><th class="n">error</th><th class="n">warning</th><th class="n">info</th><th>主要 check</th></tr>
      ${findingsRows || '<tr><td colspan="5" class="muted">还没有 findings。</td></tr>'}
    </table></div></section>

    <section class="surface-panel report-panel"><div class="panel-header"><div><div class="eyebrow">Cross-source</div><h2>跨源比对</h2></div><span class="panel-meta">主源 vs 备源</span></div>
    <p class="muted">主源与备源在同一天同一字段上的分歧。<code>no_overlap</code> 是「两边没有共同主键可比」，
      不是「一致」——那是这张表最容易被读反的一行。</p>
    <div class="scroll"><table>
      <tr><th>审计日</th><th class="n">差异</th><th>按 check</th></tr>
      ${diffRows || '<tr><td colspan="3" class="muted">还没有跨源比对产物。</td></tr>'}
    </table></div></section>

    <section class="surface-panel report-panel"><div class="panel-header"><div><div class="eyebrow">Quarantine</div><h2>隔离区</h2></div><span class="panel-meta">保留问题证据</span></div>
    <p class="muted"><strong>不是垃圾桶。</strong>这些是因为有问题而被撤出 curated 的数据，留着当证据。
      删之前先看清楚是什么。</p>
    <div class="scroll"><table>
      <tr><th>目录</th><th class="n">文件</th><th class="n">体积</th><th>最后修改</th></tr>
      ${quarantineRows}
    </table></div></section>

    <section class="surface-panel report-panel"><div class="panel-header"><div><div class="eyebrow">On-demand</div><h2>按需缓存</h2></div><span class="panel-meta">不进入 curated</span></div>
    <p class="muted">按 symbol 抓取、缓存在 <code>meta/on_demand/</code>，不进 curated——
      面板别处看不到它们。</p>
    <div class="scroll"><table>
      <tr><th>数据集</th><th class="n">条目</th><th class="n">体积</th><th>最新</th></tr>
      ${onDemandRows}
    </table></div></section></div>`, "quality");

  severityTimeline(document.getElementById("sev-chart"), q.findings_runs);
}

async function renderQualityRun(runId) {
  const d = await api(`/api/quality/runs/${encodeURIComponent(runId)}`);
  const table = (rows, cols) =>
    rows.length
      ? `<div class="scroll"><table><tr>${cols.map((c) => `<th>${esc(c)}</th>`).join("")}</tr>
          ${rows
            .map(
              (r) => `<tr>${cols
                .map((c) => {
                  const v = r[c];
                  const cls = c === "severity" && v === "error" ? ' class="err"' : "";
                  return `<td${cls}>${v === undefined || v === null ? '<span class="muted">—</span>' : esc(v)}</td>`;
                })
                .join("")}</tr>`,
            )
            .join("")}</table></div>`
      : '<p class="muted">无。</p>';

  setPage(`
    <section class="page-heading"><div class="eyebrow">数据湖控制台 / 质量 / 审计详情</div><div class="heading-row"><div><h1>${esc(d.trade_date || runId.slice(0, 8))}</h1><p class="sub"><code>${esc(d.run_id)}</code></p></div><div class="action-row"><a class="button button-ghost" href="#/quality">← 返回质量</a></div></div></section>
    <section class="metric-grid detail-metrics" aria-label="审计关键指标">${kpi(d.findings.length, "Findings", "审计发现")}${kpi(d.diffs.length, "Diffs", "跨源差异")}</section>
    <div class="report-stack"><section class="surface-panel report-panel"><div class="panel-header"><div><div class="eyebrow">Findings</div><h2>审计发现</h2></div><span class="panel-meta">${d.findings.length} 项</span></div>${table(d.findings, ["severity", "dataset", "check", "message"])}</section>
    <section class="surface-panel report-panel"><div class="panel-header"><div><div class="eyebrow">Cross-source</div><h2>跨源差异</h2></div><span class="panel-meta">${d.diffs.length} 项</span></div>${table(d.diffs, ["severity", "dataset", "check", "field", "bps", "message"])}</section></div>`, "quality");
}

// --- routing ----------------------------------------------------------------

async function route() {
  disposeAll();
  closeRunStream();
  const dataset = location.hash.match(/^#\/dataset\/([^/]+)(?:\/(state|meta|data))?/);
  const run = location.hash.match(/^#\/runs\/(.+)$/);
  const qrun = location.hash.match(/^#\/quality\/(.+)$/);
  try {
    if (dataset) await renderDetail(decodeURIComponent(dataset[1]), dataset[2] || "state");
    else if (run) await renderRunDetail(decodeURIComponent(run[1]));
    else if (location.hash.startsWith("#/runs")) await renderRuns();
    else if (qrun) await renderQualityRun(decodeURIComponent(qrun[1]));
    else if (location.hash.startsWith("#/quality")) await renderQuality();
    else if (location.hash.startsWith("#/datasets")) await renderDatasets();
    else await renderOverview();
    window.scrollTo(0, 0);
  } catch (err) {
    setPage(`<section class="error-state"><div class="eyebrow">CNMarketLake</div><h1>加载失败</h1><p class="sub err">${esc(err.message)}</p><a class="button button-primary" href="#/">返回概览</a></section>`);
  }
}

// Delegated so it survives every re-render.
document.addEventListener("click", (e) => {
  const link = e.target.closest("[data-ds]");
  if (link) {
    e.stopPropagation();
    location.hash = `#/dataset/${encodeURIComponent(link.dataset.ds)}`;
  }
});
window.addEventListener("hashchange", route);
route();
