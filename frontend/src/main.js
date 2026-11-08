// Lake dashboard. Two views routed on the hash: the tier overview (#/) and one
// dataset (#/dataset/<name>[/state|meta]).
import { renderStorage, closeStorage } from "./storage.js";
import { closeOps, renderOps, setRunBoardPainter } from "./ops.js";
import { configDriftBanner } from "./drift.js";
import { closeRunStream, renderRunDetail, renderRuns } from "./runs.js";
import { disposeAll, heatmap, partitionSeries, provenanceSeries, runGantt, severityTimeline } from "./charts.js";
import {
  ds,
  dsMarkup,
  dsSearch,
  getLang,
  grainText as partitionText,
  historyText,
  modeLabel,
  onLanguageChange,
  setLang,
  statusText,
  tierText,
  tr,
} from "./i18n.js";

setRunBoardPainter((el, detail) => runGantt(el, detail));

const qs = new URLSearchParams(location.search);
const TOKEN = qs.get("token");
// Keep the default window compact enough to read on the overview.  The API
// supports longer windows through ?days=..., but 250 sessions makes sparse
// snapshot datasets look like a large unexplained blank block.
const DAYS = Number(qs.get("days") || 90);
const app = document.getElementById("app");

function navItems() {
  return [
    ["overview", tr("概览", "Overview"), "#/"],
    ["ops", tr("操作", "Operations"), "#/ops"],
    ["datasets", tr("数据集", "Datasets"), "#/datasets"],
    ["runs", tr("运行", "Runs"), "#/runs"],
    ["quality", tr("质量", "Quality"), "#/quality"],
    ["storage", tr("存储", "Storage"), "#/storage"],
  ];
}

function pageShell(content, active = "overview") {
  const nav = navItems()
    .map(
      ([key, label, href]) =>
        `<a class="nav-link ${active === key ? "active" : ""}" href="${href}" data-nav="${key}">${label}</a>`,
    )
    .join("");
  const current = getLang();
  const zhLabel = "中文"; // i18n-ignore
  return `<div class="app-shell">
    <header class="topbar">
      <a class="brand-lockup" href="#/" aria-label="${tr("概览", "Overview")}">
        <span class="brand-mark">CNE</span>
        <span class="brand-copy"><strong>CNEquity</strong></span>
      </a>
      <nav class="nav" aria-label="${tr("主导航", "Main")}">${nav}</nav>
      <div class="topbar-meta"><span class="console-mode">…</span>
        <div class="lang-switch" role="group" aria-label="${tr("界面语言", "Language")}">
          <button class="lang-option${current === "zh" ? " on" : ""}" type="button" data-set-lang="zh" aria-pressed="${current === "zh"}">${zhLabel}</button>
          <button class="lang-option${current === "en" ? " on" : ""}" type="button" data-set-lang="en" aria-pressed="${current === "en"}">EN</button>
        </div>
        <button class="button button-ghost" id="refresh-page" type="button">${tr("刷新", "Refresh")}</button>
      </div>
    </header>
    <main class="page-main">${content}</main>
  </div>`;
}

// Each route() call owns the page until the next one starts. A slow render
// (the overview's heatmap, say) must not paint over the page the user has
// since moved to, so every render checks live() after each await.
let routeSeq = 0;
function beginRoute() {
  const seq = ++routeSeq;
  const live = () => seq === routeSeq;
  return { live, setPage: (content, active) => (live() ? setPage(content, active) : undefined) };
}

function setPage(content, active = "overview") {
  app.innerHTML = pageShell(content, active);
  document.getElementById("refresh-page")?.addEventListener("click", () => location.reload());
  paintMode();
}

let modePaint = 0;
async function paintMode() {
  const ticket = ++modePaint;
  try {
    const body = await api("/api/ops");
    if (ticket !== modePaint) return;
    const el = document.querySelector(".console-mode");
    if (el) el.textContent = modeLabel(body.mode?.label || "");
    document.getElementById("config-drift")?.remove();
    const drift = configDriftBanner(body.mode, esc);
    if (drift) {
      document.querySelector(".page-main")?.insertAdjacentHTML("afterbegin", drift);
      const copy = document.querySelector("#config-drift [data-copy-command]");
      if (copy) {
        copy.onclick = async () => {
          try {
            await navigator.clipboard.writeText(copy.dataset.copyCommand || "");
            copy.textContent = tr("已复制", "Copied");
          } catch {
            // No clipboard permission: leave the command selected for Ctrl/Cmd+C.
            const code = document.querySelector("#config-drift .drift-command code");
            if (code) window.getSelection()?.selectAllChildren(code);
            copy.textContent = tr("已选中，按 Ctrl/⌘+C 复制", "Selected; press Ctrl/⌘+C");
          }
        };
      }
    }
    const slot = body.occupancy?.slot;
    const meta = document.querySelector(".topbar-meta");
    if (meta && slot && (slot.state === "running" || slot.state === "starting") && !meta.querySelector(".ops-live")) {
      const link = document.createElement("a");
      link.className = "ops-live";
      link.href = `#/ops/jobs/${encodeURIComponent(slot.job_id)}`;
      link.textContent = tr("任务进行中", "Job running");
      meta.prepend(link);
    }
  } catch {
    /* a page that cannot read /api/ops still renders its own error */
  }
}

function opsHref(op, params) {
  const query = new URLSearchParams();
  query.set("op", op);
  for (const [key, value] of Object.entries(params || {})) {
    if (Array.isArray(value)) query.set(key, value.join(","));
    else if (value != null && value !== "") query.set(key, String(value));
  }
  return `#/ops?${query}`;
}

async function api(path, options) {
  const sep = path.includes("?") ? "&" : "?";
  const url = TOKEN ? `${path}${sep}token=${encodeURIComponent(TOKEN)}` : path;
  // Server messages (errors, notes) come back in the page's language.
  const headers = { ...(options?.headers || {}), "X-CNE-Lang": getLang() === "en" ? "en" : "zh-CN" };
  const res = await fetch(url, { ...options, headers });
  if (!res.ok) {
    // Surface what the server said. A 422 here is usually a real contract
    // message ("requires as_of= for point-in-time queries"), and showing the
    // status code instead throws away the one useful part.
    let detail = `${path} → ${res.status}`;
    try {
      const body = await res.json();
      if (body?.detail) detail = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail);
    } catch {
      /* not JSON, keep the status line */
    }
    throw new Error(detail);
  }
  return res.json();
}

const fmt = (n) => (n ?? 0).toLocaleString(getLang() === "en" ? "en" : "zh-CN");
const compact = (n) =>
  new Intl.NumberFormat(getLang() === "en" ? "en" : "zh-CN", { notation: "compact", maximumFractionDigits: 1 }).format(n ?? 0);
const mb = (b) => (!b ? "-" : b >= 1e9 ? `${(b / 1e9).toFixed(1)} GB` : `${(b / 1e6).toFixed(0)} MB`);
const esc = (s) =>
  String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

/** First line of a batch error, without the HTTP client's docs trailer. */
function failureLine(message) {
  const line = String(message || "").split("\n")[0];
  const cut = line.indexOf(" For more information check:");
  return cut === -1 ? line : line.slice(0, cut).trimEnd();
}

// --- overview ---------------------------------------------------------------

const kpi = (n, label, note = "", tone = "") =>
  `<article class="metric-card${tone ? ` metric-card-${tone}` : ""}"><div class="metric-label">${label}</div>
  <div class="metric-value">${n}</div>${note ? `<div class="metric-note">${note}</div>` : ""}</article>`;

/** One tone map for every status word the API hands the console.
 *
 * Batch statuses, dataset freshness and audit severities were being coloured
 * by three separate inline ternaries that had already drifted: `failed` was
 * orange as a pill and red as a chip, in the same table row. They are one
 * vocabulary; keep one table. An unlisted word falls through to the neutral
 * chip rather than guessing at it. */
const TONE = {
  fresh: "fresh", success: "fresh", running: "running",
  failed: "error", error: "error", stale: "stale", warning: "stale", degraded: "stale",
  empty: "empty", info: "",
};

const statusPill = (status, label = statusText(status)) => {
  const tone = TONE[status] || "empty";
  return `<span class="status-pill status-pill-${tone}"><span class="dot ${tone === "running" ? "" : tone}"></span>${esc(label)}</span>`;
};

const chip = (label, tone = "", n) =>
  `<span class="chip${tone ? ` chip-${tone}` : ""}">${esc(label)}${n === undefined ? "" : `<b>${fmt(n)}</b>`}</span>`;

const chipRow = (items, empty = "-") =>
  items.length ? `<div class="chip-row">${items.join("")}</div>` : `<span class="muted">${empty}</span>`;

/** A status tally as chips instead of a single run-on status string.
 *
 * The batch column is the densest cell on the runs table and it rendered as
 * one run-on grey string, so a failed batch read exactly like a successful one
 * until the whole cell went red. That said "something here is wrong"
 * without saying which. Per-status chips carry that on the status itself. */
const tally = (counts) => chipRow(Object.entries(counts || {}).map(([k, n]) => chip(statusText(k), TONE[k], n)));

/** A zero is a result, not an absence. */
const num = (n, cls = "") => (n ? (cls ? `<span class="${cls}">${fmt(n)}</span>` : fmt(n)) : '<span class="muted">0</span>');

/** One table, one shape.
 *
 * Every report table below was hand-rolling `<table><tr><th>` with no
 * thead/tbody and its own empty-row markup, so right-alignment and the empty
 * state drifted panel by panel. Columns are declared once: a bare string is a
 * plain header, `{h, n: true}` right-aligns the header the way the tabular
 * numbers underneath it already are. */
function dataTable(cols, rows, empty, cls = "") {
  const head = cols
    .map((c) => (typeof c === "string" ? { h: c } : c))
    .map((c) => `<th${c.n ? ' class="n"' : ""}>${esc(c.h)}</th>`)
    .join("");
  const body = rows.length
    ? rows.join("")
    : `<tr><td colspan="${cols.length}" class="empty-table"><span>${empty}</span></td></tr>`;
  return `<div class="scroll"><table${cls ? ` class="${cls}"` : ""}><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

/** Load-bearing prose, not a footnote.
 *
 * The quality panels carry the two sentences most likely to be read backwards
 * `no_overlap` is "nothing comparable", not "agrees", and quarantine is
 * evidence, not a bin. They were set as small grey text under the header and
 * read as boilerplate. */
const panelNote = (html) => `<p class="panel-note">${html}</p>`;

// storage/stats.py emits exactly two of these; anything else falls through
// unchanged rather than being silently mistranslated.
function statsReason(reason) {
  if (!reason) return tr("原因未知", "Unknown reason");
  if (reason.includes("landed after the stats were built")) return tr("有新的采集批次晚于度量表", "A newer batch landed after the stats snapshot");
  if (reason.includes("no stats yet")) return tr("尚未生成过度量表", "No stats snapshot yet");
  return reason;
}

function dsLink(id) {
  return `<span class="ds-link" data-ds="${esc(id)}">${dsMarkup(id)}</span>`;
}

async function renderOverview(view) {
  const [h, tiers, datasets, hm] = await Promise.all([
    api("/api/health"),
    api("/api/tiers"),
    api("/api/datasets"),
    api(`/api/heatmap?days=${DAYS}`),
  ]);
  if (!view.live()) return;
  const sev = h.findings_by_severity || {};
  const notes = [];
  // The reason strings come from the ingestion layer and are English, with a
  // run UUID in them. Fine in a log, wrong in the banner of a Chinese page.
  // the id is not something the reader can act on. Translate the ones that can
  // actually appear, keep the original as a tooltip for anyone debugging.
  if (h.stats_stale) {
    notes.push({
      tone: "info",
      title: tr("度量表过期", "Stats are stale"),
      detail: tr(
        `已在后台重建（${esc(statsReason(h.stats_reason))}）。稍后刷新。`,
        `A rebuild is already running (${esc(statsReason(h.stats_reason))}). Refresh shortly.`,
      ),
    });
  }
  if (h.stale_datasets.length) {
    notes.push({
      tone: "stale",
      title: tr(`${h.stale_datasets.length} 个数据集落后`, `${h.stale_datasets.length} stale ${h.stale_datasets.length === 1 ? "dataset" : "datasets"}`),
      chips: h.stale_datasets.map((d) => dsLink(d)).join(""),
      action: { href: "#/ops?op=daily.stale", label: tr("补抓落后数据集", "Catch up stale datasets") },
    });
  }
  let primaryHref = "#/quality";
  let primaryLabel = tr("查看质量", "Open quality");
  try {
    const ops = await api("/api/ops");
    if (!view.live()) return;
    const pending = ops.occupancy?.incomplete_init;
    if (pending?.running) {
      // An empty lake during its first init is expected; offering to start
      // another init sent people into a form that could only refuse.
      notes.push({
        tone: "info",
        title: tr("初始化正在进行", "Initialization is running"),
        detail: tr("数据会随批次陆续出现，进度在运行记录里。", "Data appears batch by batch; the run shows its progress."),
        action: { href: "#/runs", label: tr("查看进度", "View progress") },
      });
      primaryHref = "#/runs";
      primaryLabel = tr("查看进度", "View progress");
    } else if (pending) {
      const resume = `#/ops?op=init.resume&run_id=${encodeURIComponent(pending.run_id)}`;
      if (ops.occupancy?.lake_empty) {
        notes.push({
          tone: "stale",
          title: tr("湖里还没有 curated 数据", "The lake has no curated data yet"),
          detail: tr("初始化会下载全市场行情主干。", "Initialization downloads the full-market price spine."),
          action: { href: "#/ops?op=init.start", label: tr("初始化数据湖", "Initialize the lake") },
        });
      }
      notes.push({
        tone: "stale",
        title: tr("初始化没跑完", "Initialization did not finish"),
        detail: tr("已成功的批次会保留。", "Batches that succeeded are kept."),
        action: { href: resume, label: tr("继续初始化", "Resume initialization") },
      });
      primaryHref = resume;
      primaryLabel = tr("继续初始化", "Resume initialization");
    } else if (ops.occupancy?.lake_empty) {
      notes.push({
        tone: "stale",
        title: tr("湖里还没有 curated 数据", "The lake has no curated data yet"),
        detail: tr("初始化会下载全市场行情主干。", "Initialization downloads the full-market price spine."),
        action: { href: "#/ops?op=init.start", label: tr("初始化数据湖", "Initialize the lake") },
      });
      primaryHref = "#/ops?op=init.start";
      primaryLabel = tr("初始化数据湖", "Initialize the lake");
    } else if (h.stale_datasets.length) {
      primaryHref = "#/ops?op=daily.stale";
      primaryLabel = tr("补抓落后数据集", "Catch up stale datasets");
    }
  } catch {
    /* overview still renders without the operations summary */
  }
  if (h.empty_required.length) {
    notes.push({
      tone: "error",
      title: tr("必需数据集为空", "Required datasets are empty"),
      chips: h.empty_required.map((d) => dsLink(d)).join(""),
    });
  }

  const byTier = {};
  for (const d of datasets) (byTier[d.tier] ||= []).push(d);
  const totalBytes = tiers.reduce((sum, tier) => sum + (tier.bytes || 0), 0);
  const tierMeters = (tier) => {
    const count = tier.datasets || 0;
    const width = (n) => (count ? ((n || 0) / count) * 100 : 0);
    const share = totalBytes ? ((tier.bytes || 0) / totalBytes) * 100 : 0;
    const title = tr(
      `${tier.fresh} 最新，${tier.stale} 落后，${tier.empty} 为空`,
      `${tier.fresh} fresh, ${tier.stale} stale, ${tier.empty} empty`,
    );
    return `<span class="tier-meters"><span class="tier-health" title="${esc(title)}"><i class="fresh" style="width:${width(tier.fresh)}%"></i><i class="stale" style="width:${width(tier.stale)}%"></i><i class="empty" style="width:${width(tier.empty)}%"></i></span><span class="tier-share" title="${esc(mb(tier.bytes))}"><i style="width:${share}%"></i></span></span>`;
  };

  const state = sev.error ? "error" : notes.length ? "attention" : "healthy";
  const stateLabel = {
    healthy: tr("正常", "Healthy"),
    attention: tr("待核查", "Needs review"),
    error: tr("有错误", "Errors"),
  }[state];
  const attentionRow = (note) => {
    const body = note.chips
      ? `<div class="attention-chips">${note.chips}</div>`
      : `<p class="attention-detail">${note.detail || ""}</p>`;
    const action = note.action
      ? `<a class="button ${note.tone === "info" ? "button-ghost" : "button-primary"}" href="${note.action.href}">${esc(note.action.label)}</a>`
      : "";
    return `<li class="attention-row tone-${note.tone}"><div class="attention-copy"><strong>${esc(note.title)}</strong>${body}</div>${action}</li>`;
  };
  const attention = notes.length ? `<ul class="attention-list">${notes.map(attentionRow).join("")}</ul>` : "";
  // The status block already carries each action; the heading only repeats one
  // when no row offers it.
  const headerAction = notes.some((note) => note.action?.href === primaryHref)
    ? ""
    : `<a class="button button-primary" href="${primaryHref}">${esc(primaryLabel)}</a>`;
  const tierCards = tiers
    .map((tier) => {
      const status = tier.stale
        ? tr(`${tier.stale} 个落后`, `${tier.stale} stale`)
        : tier.empty
          ? tr(`${tier.empty} 个为空`, `${tier.empty} empty`)
          : tier.fresh === tier.datasets
            ? tr("全部最新", "All fresh")
            : tr("无落后", "Nothing stale");
      return `<details class="tier-card" ${tier.stale || tier.empty ? "open" : ""}>
        <summary><span class="tier-summary"><span class="tier-name"><span class="tier-tag">${esc(tier.tier)}</span>${esc(tierText(tier.tier, tier.label))}</span>
          <span class="tier-counts"><span><b>${tier.datasets}</b> ${tr("个数据集", "datasets")}</span><span><b>${fmt(tier.rows)}</b> ${tr("行", "rows")}</span></span>
          <span class="tier-status-wrap">${tierMeters(tier)}<span class="tier-status ${tier.stale ? "is-stale" : tier.empty ? "is-empty" : "is-fresh"}">${status}</span></span></span></summary>
        <div class="tier-members">${membersTable(byTier[tier.tier] || [])}</div>
      </details>`;
    })
    .join("");

  const visibleHeatmapRows = hm.rows.filter((row) => /[#.]/.test(row.cells));
  const heatmapData = { ...hm, rows: visibleHeatmapRows };

  const stateDetail = state === "healthy"
    ? `${tr("基准日", "Anchor")} ${esc(h.anchor || "-")}`
    : tr(`${notes.length} 项待核查`, `${notes.length} to review`);
  setPage(`
    <section class="page-heading">
      <div class="heading-row"><h1>${tr("概览", "Overview")}</h1>
        <div class="action-row"><a class="button button-ghost" href="#/runs">${tr("运行记录", "Runs")}</a>${headerAction}</div>
      </div>
    </section>
    <section class="status-hero status-${state}" aria-live="polite">
      <span class="dot ${state === "healthy" ? "fresh" : state === "error" ? "error" : "stale"}" aria-hidden="true"></span>
      <div><strong>${stateLabel}</strong><span>${stateDetail}</span></div>
      ${attention}
    </section>
    <section class="metric-grid" aria-label="${tr("关键指标", "Key metrics")}">
      ${kpi(num(h.datasets), tr("数据集", "Datasets"))}
      ${kpi(num(h.fresh), tr("最新", "Fresh"))}
      ${kpi(num(h.stale, "err"), tr("落后", "Stale"), tr("超过容忍窗口", "Outside tolerance"), h.stale ? "alert" : "")}
      ${kpi(`<span title="${fmt(h.rows)} ${tr("行", "rows")}">${compact(h.rows)}</span>`, tr("行数", "Rows"), "curated")}
      ${kpi(mb(h.bytes), tr("存储", "Storage"), "curated")}
      ${kpi(`${num(sev.error, "err")}<span class="metric-secondary"> / ${fmt(sev.warning)}</span>`, tr("审计", "Audit"), "", sev.error ? "alert" : "")}
    </section>
    <article class="surface-panel heat-panel"><div class="panel-header"><h2>${tr("覆盖", "Coverage")}</h2><span class="panel-meta">${tr(`${visibleHeatmapRows.length}/${hm.rows.length} 个数据集，${hm.days.length} 个交易日`, `${visibleHeatmapRows.length}/${hm.rows.length} datasets, ${hm.days.length} sessions`)}</span></div>
      ${visibleHeatmapRows.length ? `<div id="heat" aria-label="${tr("覆盖", "Coverage")}"></div>` : `<p class="heat-empty">${tr("当前窗口没有分区。", "No partitions in this window.")}</p>`}
      ${visibleHeatmapRows.length ? `<p class="legend"><span><i class="swatch" style="background:var(--cell-covered)"></i> ${tr("有分区", "Present")}</span><span><i class="swatch" style="background:var(--cell-gap)"></i> ${tr("日更缺口", "Daily gap")}</span><span><i class="swatch" style="background:var(--cell-cadence)"></i> ${tr("采集间隔", "Cadence")}</span><span><i class="swatch" style="background:var(--cell-outside)"></i> ${tr("窗口外", "Outside")}</span></p>` : ""}
    </article>
    <section class="surface-panel dataset-panel" id="dataset-list"><div class="panel-header"><h2>${tr("数据层", "Layers")}</h2><a class="panel-link" href="#/datasets">${tr("全部数据集", "All datasets")}</a></div><div class="tier-grid">${tierCards}</div></section>
  `);
  if (visibleHeatmapRows.length) heatmap(document.getElementById("heat"), heatmapData);
}

function datasetOpLabel(op, why) {
  const labels = {
    "backfill.run": tr("回填这个数据集", "Backfill this dataset"),
    "daily.stale": tr("补抓落后数据", "Catch up stale data"),
    "derive.run": tr("重算派生", "Recompute derived data"),
  };
  return labels[op] || why;
}

function datasetActionButtons(dataset) {
  const runnable = (dataset.commands || []).filter((command) => command.op).slice(0, 2);
  const buttons = [`<a class="button button-ghost" href="#/datasets">${tr("返回数据集", "Datasets")}</a>`];
  for (const [index, command] of runnable.entries()) {
    const tone = index === 0 ? "button button-primary" : "button button-ghost";
    buttons.push(`<a class="${tone}" href="${opsHref(command.op, command.params)}">${esc(datasetOpLabel(command.op, command.why))}</a>`);
  }
  return buttons.join("");
}

function datasetQuick(d) {
  if (d.history_mode === "derived") return `<a class="table-link" href="${opsHref("derive.run", { name: d.dataset })}">${tr("重算", "Recompute")}</a>`;
  if (d.freshness === "stale") return `<a class="table-link" href="${opsHref("daily.stale")}">${tr("补抓", "Catch up")}</a>`;
  if (!d.has_data && d.history_mode !== "snapshot_only") return `<a class="table-link" href="${opsHref("backfill.run", { dataset: d.dataset })}">${tr("回填", "Backfill")}</a>`;
  return `<a class="table-link" href="#/dataset/${encodeURIComponent(d.dataset)}/data">${tr("数据", "Data")}</a>`;
}

async function renderDatasets(view) {
  const datasets = await api("/api/datasets");
  if (!view.live()) return;
  const stale = datasets.filter((d) => d.freshness === "stale").length;
  const removed = datasets.some((d) => d.freshness === "removed");
  const filters = [
    ["all", tr("全部", "All")],
    ["fresh", tr("最新", "Fresh")],
    ["stale", tr("落后", "Stale")],
    ["empty", tr("为空", "Empty")],
  ];
  if (removed) filters.push(["removed", tr("已移出", "Removed")]);
  const tiers = [...new Set(datasets.map((d) => d.tier).filter(Boolean))].sort();
  const sortHead = (key, label, numeric = false) =>
    `<th${numeric ? ' class="n"' : ""}><button type="button" class="sort" data-sort="${key}">${label}</button></th>`;
  const rows = (items) =>
    items
      .map(
        (d) => `<tr class="dataset-row"><td><span class="dot ${d.freshness === "fresh" ? "fresh" : d.freshness === "stale" ? "stale" : "empty"}"></span>${dsLink(d.dataset)}${removedNote(d)}</td><td>${esc(tierText(d.tier, d.tier_label))}</td><td>${esc(historyText(d.history_mode))}</td><td>${esc(partitionText(d.granularity))}</td><td>${esc(d.watermark || "-")}</td><td class="n">${fmt(d.row_count)}</td><td class="n">${mb(d.bytes)}</td><td>${datasetQuick(d)}</td></tr>`,
      )
      .join("");
  setPage(`<section class="page-heading"><div class="heading-row"><h1>${tr("数据集", "Datasets")}</h1>
    ${stale ? `<div class="action-row"><a class="button button-primary" href="#/ops?op=daily.stale">${tr(`补抓 ${stale} 个落后数据集`, `Catch up ${stale} stale ${stale === 1 ? "dataset" : "datasets"}`)}</a></div>` : ""}</div></section>
    <section class="surface-panel catalog-panel"><div class="catalog-toolbar"><label class="search-field"><span aria-hidden="true">⌕</span><input id="dataset-search" type="search" placeholder="${tr("搜索数据集、层级或语义", "Search name, layer, or semantics")}" autocomplete="off"></label><span class="panel-meta" id="dataset-count">${datasets.length} ${tr("个结果", "results")}</span></div>
    <div class="catalog-filters" id="dataset-filters">${filters.map(([id, label]) => `<button type="button" class="button ${id === "all" ? "button-primary" : "button-ghost"}" data-freshness="${id}">${label}</button>`).join("")}</div>
    <div class="catalog-filters" id="dataset-tiers"><button type="button" class="button button-primary" data-tier="all">${tr("全部层级", "All layers")}</button>${tiers.map((code) => `<button type="button" class="button button-ghost" data-tier="${esc(code)}">${esc(tierText(code))}</button>`).join("")}</div>
    <div class="scroll"><table id="dataset-table"><thead><tr>${sortHead("name", tr("数据集", "Dataset"))}${sortHead("tier", tr("分层", "Layer"))}<th>${tr("语义", "Semantics")}</th><th>${tr("粒度", "Grain")}</th>${sortHead("watermark", tr("水位", "Watermark"))}${sortHead("rows", tr("行", "Rows"), true)}${sortHead("bytes", tr("体积", "Size"), true)}<th>${tr("操作", "Action")}</th></tr></thead><tbody></tbody></table></div></section>`, "datasets");
  const table = document.querySelector("#dataset-table tbody");
  const count = document.getElementById("dataset-count");
  const state = { freshness: "all", tier: "all", sort: "name", dir: 1, query: "" };
  const sortValue = (d) => {
    if (state.sort === "tier") return tierText(d.tier, d.tier_label);
    if (state.sort === "watermark") return d.watermark || "";
    if (state.sort === "rows") return d.row_count || 0;
    if (state.sort === "bytes") return d.bytes || 0;
    return ds(d.dataset);
  };
  const render = () => {
    const q = state.query.trim().toLowerCase();
    const filtered = datasets.filter((d) => {
      if (state.freshness !== "all" && d.freshness !== state.freshness) return false;
      if (state.tier !== "all" && d.tier !== state.tier) return false;
      return [dsSearch(d.dataset), d.tier, tierText(d.tier, d.tier_label), historyText(d.history_mode), d.history_mode, partitionText(d.granularity), d.granularity].some((v) =>
        String(v || "").toLowerCase().includes(q),
      );
    });
    const collator = getLang() === "en" ? "en" : "zh-CN";
    const sorted = filtered.slice().sort((a, b) => {
      const av = sortValue(a);
      const bv = sortValue(b);
      const order = typeof av === "number" && typeof bv === "number" ? av - bv : String(av).localeCompare(String(bv), collator);
      return order * state.dir;
    });
    table.innerHTML = sorted.length ? rows(sorted) : `<tr><td colspan="8" class="empty-table"><span>${tr("没有匹配的数据集。", "No matching dataset.")}</span></td></tr>`;
    count.textContent = `${sorted.length} ${tr("个结果", "results")}`;
    document.querySelectorAll("#dataset-filters button").forEach((button) => {
      button.className = `button ${button.dataset.freshness === state.freshness ? "button-primary" : "button-ghost"}`;
    });
    document.querySelectorAll("#dataset-tiers button").forEach((button) => {
      button.className = `button ${button.dataset.tier === state.tier ? "button-primary" : "button-ghost"}`;
    });
    document.querySelectorAll("#dataset-table button.sort").forEach((button) => {
      const on = button.dataset.sort === state.sort;
      button.className = `sort${on ? " on" : ""}`;
      button.dataset.dir = on ? String(state.dir) : "";
    });
  };
  document.getElementById("dataset-search").addEventListener("input", (event) => {
    state.query = event.target.value;
    render();
  });
  document.getElementById("dataset-filters").addEventListener("click", (event) => {
    const button = event.target.closest("button");
    if (!button) return;
    state.freshness = button.dataset.freshness || "all";
    render();
  });
  document.getElementById("dataset-tiers").addEventListener("click", (event) => {
    const button = event.target.closest("button");
    if (!button) return;
    state.tier = button.dataset.tier || "all";
    render();
  });
  document.querySelectorAll("#dataset-table button.sort").forEach((button) => {
    button.onclick = () => {
      const key = button.dataset.sort;
      if (state.sort === key) state.dir *= -1;
      else {
        state.sort = key;
        state.dir = key === "name" ? 1 : -1;
      }
      render();
    };
  });
  render();
}

// A dataset the config took out of its schedule on purpose: real last date, not owed.
const removedNote = (d) => (d.freshness === "removed" ? ` <span class="muted" title="${tr("配置用 steps_remove / enabled = false 移出了调度", "Removed from the schedule by steps_remove / enabled = false")}">${tr("已移出调度", "removed in config")}</span>` : "");

function membersTable(rows) {
  const body = rows.map((d) => {
    const cls = d.freshness === "fresh" ? "fresh" : d.freshness === "stale" ? "stale" : "empty";
    const cover = d.coverage_start ? `${d.coverage_start} → ${d.coverage_end}` : "-";
    const opt = d.required ? "" : ` <span class="muted">${tr("可选", "optional")}</span>`;
    return `<tr class="data-row"><td><span class="dot ${cls}"></span>${dsLink(d.dataset)}${opt}${removedNote(d)}</td>
      <td>${esc(historyText(d.history_mode))}</td><td>${esc(partitionText(d.granularity))}</td><td class="cell-time">${cover}</td>
      <td class="cell-time">${d.watermark || "-"}</td><td class="n">${fmt(d.row_count)}</td>
      <td class="n">${mb(d.bytes)}</td></tr>`;
  });
  return dataTable(
    [tr("数据集", "Dataset"), tr("语义", "Semantics"), tr("粒度", "Grain"), tr("覆盖", "Coverage"), tr("水位", "Watermark"), { h: tr("行", "Rows"), n: true }, { h: tr("体积", "Size"), n: true }],
    body,
    tr("这一层还没有注册数据集。", "This layer has no registered datasets."),
  );
}

// --- dataset detail ---------------------------------------------------------

function periodMs(value) {
  const text = String(value || "");
  const day = /^(\d{4})-(\d{2})-(\d{2})/.exec(text);
  if (day) return Date.UTC(Number(day[1]), Number(day[2]) - 1, Number(day[3]));
  const month = /^(\d{4})-(\d{2})$/.exec(text);
  if (month) return Date.UTC(Number(month[1]), Number(month[2]) - 1, 1);
  const year = /^(\d{4})$/.exec(text);
  if (year) return Date.UTC(Number(year[1]), 0, 1);
  return null;
}

function repairHref(d, start, end) {
  if (d.history_mode === "snapshot_only" || d.max_staleness_days > 1) return "";
  if (d.history_mode === "derived") return opsHref("derive.run", { name: d.dataset });
  return opsHref("backfill.run", { dataset: d.dataset, start, end });
}

function gapMarks(d, start, right, pct) {
  const points = [];
  for (const value of d.gaps?.missing || []) {
    const ms = periodMs(value);
    if (ms == null || ms < start || ms > right) continue;
    points.push({ ms, value: String(value) });
  }
  points.sort((a, b) => a.ms - b.ms);
  const marks = points
    .map((point) => {
      const title = tr(`回填 ${point.value}`, `Backfill ${point.value}`);
      const href = repairHref(d, point.value, point.value);
      if (!href) return `<i class="gap-mark" style="left:${pct(point.ms)}%" title="${esc(title)}"></i>`;
      return `<a class="gap-mark" href="${esc(href)}" style="left:${pct(point.ms)}%" title="${esc(title)}"></a>`;
    })
    .join("");
  const action = points.length && repairHref(d, points[0].value, points[points.length - 1].value)
    ? `<a href="${esc(repairHref(d, points[0].value, points[points.length - 1].value))}">${tr(`回填这 ${points.length} 处缺口`, points.length === 1 ? "Backfill this gap" : `Backfill these ${points.length} gaps`)}</a>`
    : "";
  return { marks, action };
}

function coverageBar(d, anchor) {
  if (!d.coverage_start) return `<p class="muted">${tr("尚无分区。", "No partitions yet.")}</p>`;
  const start = periodMs(d.coverage_start);
  const end = periodMs(d.coverage_end);
  if (start == null || end == null) {
    return `<div class="cover"><div class="fill" style="left:0;width:100%"></div></div>
      <p class="legend"><span>${esc(d.coverage_start)}</span><span style="margin-left:auto">${esc(d.coverage_end || "")}</span></p>`;
  }
  const anchorMs = periodMs(anchor);
  const right = d.freshness === "stale" && anchorMs != null && anchorMs > end ? anchorMs : end;
  const span = Math.max(right - start, 1);
  const pct = (ms) => (((ms - start) / span) * 100).toFixed(2);
  const watermark = d.watermark ? periodMs(d.watermark) : null;
  const dataMs = watermark != null ? Math.min(Math.max(watermark, start), right) : end;
  const covered = Math.max(0, Math.min(100, Number(pct(dataMs))));
  const lag = Math.max(0, 100 - covered);
  let horizon = "";
  if (d.earliest_available) {
    const floor = periodMs(d.earliest_available);
    if (floor != null && floor > start && floor < right) {
      horizon = `<div class="horizon" style="left:${pct(floor)}%" title="${esc(tr(`源端历史天花板 ${d.earliest_available}`, `Source history ceiling ${d.earliest_available}`))}"></div>`;
    }
  }
  const gaps = gapMarks(d, start, right, pct);
  const behind = d.freshness === "stale" && lag === 0;
  const rightLabel = right > end ? anchor : d.coverage_end;
  const title = lag > 0
    ? tr(`水位 ${d.watermark || d.coverage_end} 到 ${rightLabel} 还没跟上`, `Watermark ${d.watermark || d.coverage_end} lags through ${rightLabel}`)
    : behind
      ? tr(`水位停在 ${d.watermark || d.coverage_end}，尚未跟上`, `Watermark stopped at ${d.watermark || d.coverage_end} and is behind`)
      : tr(`${d.coverage_start} → ${d.coverage_end}`, `${d.coverage_start} → ${d.coverage_end}`);
  let lagHtml = "";
  if (lag > 0) {
    const from = d.watermark || d.coverage_end;
    const lagTitle = tr(`从 ${from} 补到 ${rightLabel}`, `Fill from ${from} through ${rightLabel}`);
    const href = repairHref(d, from, rightLabel);
    const style = `left:${covered}%;width:${lag}%`;
    lagHtml = href
      ? `<a class="fill lag" href="${esc(href)}" style="${style}" title="${esc(lagTitle)}"></a>`
      : `<div class="fill lag" style="${style}" title="${esc(lagTitle)}"></div>`;
  }
  const action = gaps.action ? `<p class="legend">${gaps.action}</p>` : "";
  return `<div class="cover${behind ? " is-stale" : ""}" title="${esc(title)}"><div class="fill" style="left:0;width:${covered}%"></div>${lagHtml}${gaps.marks}${horizon}</div>
    <p class="legend"><span>${esc(d.coverage_start)}</span><span style="margin-left:auto">${esc(rightLabel || "")}</span></p>${action}`;
}

function gapsNote(d) {
  const g = d.gaps;
  if (!g.total) return d.coverage_start ? `<p class="muted">${tr("覆盖区间内无缺口。", "No gaps inside the covered range.")}</p>` : "";
  const cadence =
    d.max_staleness_days > 1
      ? tr(
          `　该源非日更（容忍 ${d.max_staleness_days} 天），间隔属其节奏。`,
          ` This source is not daily (tolerance ${d.max_staleness_days} days); the spacing is its cadence.`,
        )
      : "";
  return `<p class="${d.max_staleness_days > 1 ? "muted" : "err"}">${tr(`${g.total} 个 ${g.unit} 无分区`, `${g.total} ${g.unit} without a partition`)}${cadence}</p>
    <p class="muted">${g.missing.slice(0, 12).map(esc).join("、")}${g.total > 12 ? " …" : ""}</p>`;
}

// Stored 15m/30m/60m resamples carry no watermark, so this is the only place
// the page can say they trail the 1m/5m they were built from.
function resampleNote(d) {
  const r = d.resample;
  if (!r) return "";
  const run = `<a href="${opsHref("derive.run", { name: d.dataset })}">${tr("重算派生", "Recompute")}</a>`;
  if (!r.derived) {
    return panelNote(r.input_sessions
      ? tr(`尚未派生。湖里的 1m / 5m 共有 ${fmt(r.input_sessions)} 个交易日可以计算，${run}后入湖。`, `Not derived yet. The lake's 1m / 5m cover ${fmt(r.input_sessions)} sessions; ${run} to store them.`)
      : tr("尚未派生。湖里还没有 1m 或 5m 分钟线。", "Not derived yet. The lake has no 1m or 5m bars."));
  }
  return panelNote(r.pending
    ? tr(`输入有 ${fmt(r.pending)} 个交易日尚未重算。1m / 5m 更新后不会自动重算，${run}后才会更新。`, `${fmt(r.pending)} input sessions have not been recomputed. Nothing reruns this when 1m / 5m change; ${run} to update.`)
    : tr("已与 1m / 5m 一致，没有待重算的交易日。", "In step with 1m / 5m; no session waits to be recomputed."));
}

function stateTab(d, prov, anchor) {
  const provTable = dataTable(
    ["source", "data_version", { h: tr("行", "Rows"), n: true }, tr("fetched_at 跨度", "fetched_at span")],
    prov.map(
      (p) => `<tr class="data-row"><td>${esc(p.source)}</td><td>${esc(p.data_version)}</td>
      <td class="n">${fmt(p.row_count)}</td>
      <td class="muted cell-time">${esc((p.fetched_at_min || "").slice(0, 10))} → ${esc((p.fetched_at_max || "").slice(0, 10))}</td></tr>`,
    ),
    tr("无溯源度量。", "No provenance stats."),
  );

  const findings = dataTable(
    ["severity", "check", "message"],
    d.findings.map(
      (f) => `<tr class="data-row"><td>${chip(f.severity, TONE[f.severity])}</td>
      <td>${esc(f.check)}</td><td>${esc(f.message)}</td></tr>`,
    ),
    tr("上次审计没有该数据集的 findings。", "The last audit has no findings for this dataset."),
  );

  const batches = dataTable(
    [tr("状态", "Status"), tr("窗口", "Window"), { h: tr("写入", "Written"), n: true }, { h: tr("重试", "Retries"), n: true }, tr("开始", "Started"), tr("错误", "Error")],
    d.batches.map(
      (b) => `<tr class="data-row"><td>${statusPill(b.status)}</td>
      <td class="muted cell-time">${esc(b.window_start || "-")} → ${esc(b.window_end || "-")}</td>
      <td class="n">${fmt(b.rows_written)}</td><td class="n">${num(b.retry_count)}</td>
      <td class="muted cell-time">${esc((b.started_at || "").slice(0, 19))}</td>
      <td class="cell-failures">${b.error_message ? esc(failureLine(b.error_message)) : '<span class="muted">-</span>'}</td></tr>`,
    ),
    tr("manifest 中没有该数据集的 batch。", "The manifest has no batch for this dataset."),
  );

  return `
    <h3>${tr("覆盖", "Coverage")}</h3>${coverageBar(d, anchor)}${resampleNote(d)}${gapsNote(d)}
    <h3>${tr("分区体量", "Partition size")}</h3>
    <div class="controls" id="parts-mode">
      <button type="button" class="button button-primary" data-part-mode="both">${tr("同时", "Both")}</button>
      <button type="button" class="button button-ghost" data-part-mode="rows">${tr("行数", "Rows")}</button>
      <button type="button" class="button button-ghost" data-part-mode="bytes">${tr("体积", "Size")}</button>
    </div>
    <div id="parts"></div>
    <h3>${tr("溯源分布（按时间）", "Provenance over time")}</h3><div id="prov"></div><p class="muted" id="provnote"></p>
    <h3>${tr("溯源合计", "Provenance totals")}</h3>${provTable}
    <h3>${tr("审计 findings", "Audit findings")}</h3>${findings}
    <h3>${tr("最近 batch", "Recent batches")}</h3>${batches}`;
}

const fact = (k, v) => `<div class="fact"><span class="k">${esc(k)}</span><span class="v">${v}</span></div>`;
const bucketText = (b) => ({ day: tr("日", "day"), month: tr("月", "month"), year: tr("年", "year") })[b] || b;
const dayCount = (n) => `${n} ${Number(n) === 1 ? "day" : "days"}`;

/** What one row covers.
 *
 * Was keyed on `intraday`, the bar-frequency field, which `trade_ticks`
 * deliberately leaves unset so it cannot inherit bar-shaped checks. That
 * printed a dash, making intraday transaction records look like a daily
 * dataset. `row_grain` is set on every intraday dataset, including the stored
 * 15m/30m/60m resamples.
 */
function rowGrainText(d) {
  if (d.row_grain === "tick") return tr("分笔（3 秒快照聚合，非 bar）", "Ticks (3-second snapshots, not bars)");
  if (d.row_grain) return `${esc(d.row_grain)} bar`;
  return tr("日", "Day");
}

/** How far back the *source* still serves, and by which mechanism.
 *
 * Two different limits reach this panel and they must not read the same. A
 * rolling per-symbol count (minute bars) moves forward every day; a fixed
 * calendar floor (trade_ticks) does not, and its horizon therefore grows.
 * Keying only on `history_horizon_days` printed "无上限" for the floor case.
 * directly contradicting the 最早可得 line right underneath it.
 */
function horizonText(d) {
  if (d.history_floor_date) return tr(`自 ${esc(d.history_floor_date)} 起（固定底，不滚动）`, `From ${esc(d.history_floor_date)} (fixed floor)`);
  if (d.history_horizon_days) return tr(`${d.history_horizon_days} 个交易日（滚动）`, `${d.history_horizon_days} sessions (rolling)`);
  return tr("无上限", "No declared limit");
}

function metaTab(d) {
  // Flags read as state, not as prose: "是" beside "否" in the same grey
  // column made the reader parse four identical rows to find the set one.
  const yn = (b) => chip(b ? tr("是", "Yes") : tr("否", "No"), b ? "fresh" : "");
  const contract = [
    fact(tr("分层", "Layer"), `${d.tier} ${esc(tierText(d.tier, d.tier_label))}`),
    fact(tr("存储层", "Storage"), d.layer),
    fact(tr("分区键", "Partition key"), d.partition_col ? `<code>${esc(d.partition_col)}</code>` : tr("单文件 merge", "Single-file merge")),
    fact(tr("分区粒度", "Partition grain"), partitionText(d.granularity)),
    fact(tr("查询日期列", "Query date column"), d.date_col ? `<code>${esc(d.date_col)}</code>` : "-"),
    fact(tr("主键", "Primary key"), d.primary_key.map((c) => `<code>${esc(c)}</code>`).join(" ")),
  ].join("");
  const semantics = [
    fact("fetch_semantics", d.fetch_semantics),
    fact("history_mode", d.history_mode),
    fact("PIT", yn(d.pit)),
    fact(tr("维护水位", "Maintains watermark"), yn(d.watermarked)),
  ].join("");
  const sources = [
    fact(tr("回填源", "Backfill source"), d.backfill_source ? esc(d.backfill_source) : "-"),
    fact(tr("源端历史视野", "Source horizon"), horizonText(d)),
    fact(tr("最早可得", "Earliest available"), d.earliest_available || tr("不受源端限制", "Not limited by the source")),
  ].join("");
  const ops = [
    fact(tr("落后容忍", "Staleness tolerance"), tr(`${d.max_staleness_days} 天`, dayCount(d.max_staleness_days))),
    fact(tr("必需", "Required"), yn(d.required)),
    fact(tr("行粒度", "Row grain"), rowGrainText(d)),
    fact(
      tr("回填分块", "Backfill chunks"),
      d.backfill_chunk_days
        ? tr(`${d.backfill_chunk_days} 天`, dayCount(d.backfill_chunk_days))
        : d.backfill_chunk_symbols
          ? tr(`${d.backfill_chunk_symbols} 标的`, `${d.backfill_chunk_symbols} symbols`)
          : `<span class="muted">${tr("不分块", "Not chunked")}</span>`,
    ),
  ].join("");

  const schema = `<div class="scroll"><table>
    <tr><th>${tr("列", "Column")}</th><th>${tr("类型", "Type")}</th><th>${tr("主键", "Key")}</th></tr>
    ${d.schema
      .map(
        (c) => `<tr><td><code>${esc(c.column)}</code></td><td class="muted">${esc(c.dtype)}</td>
      <td>${d.primary_key.includes(c.column) ? "✓" : ""}</td></tr>`,
      )
      .join("")}</table></div>`;

  const cmds = d.commands
    .map((c, i) => {
      const href = c.op ? opsHref(c.op, c.params) : "";
      return `<div class="cmd"><code id="cmd${i}">${esc(c.cmd)}</code>
      <button data-copy="cmd${i}">${tr("复制", "Copy")}</button>${href ? `<a href="${href}">${tr("在操作页运行", "Run on the operations page")}</a>` : ""}<span class="muted">${esc(c.why)}</span></div>`;
    })
    .join("");

  return `
    <h3>${tr("契约", "Contract")}</h3><div class="facts">${contract}</div>
    <h3>${tr("语义", "Semantics")}</h3><div class="facts">${semantics}</div>
    <h3>${tr("来源", "Sources")}</h3><div class="facts">${sources}</div>
    <h3>${tr("运维", "Operations")}</h3><div class="facts">${ops}</div>
    <h3>Schema</h3>${schema}
    <h3>${tr("命令", "Commands")}</h3>${cmds}`;
}

// --- data tab ---------------------------------------------------------------

function kindLabel(kind) {
  return {
    trading_day: tr("交易日", "Session"),
    event_day: tr("事件日", "Event day"),
    period: tr("周期", "Period"),
    report_period: tr("报告期", "Report period"),
    none: "",
  }[kind] || "";
}

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
      : `<div class="period-step"><button type="button" id="q-older">${tr("更早", "Older")}</button><label>${kindLabel(dates.kind)}
        <select id="q-period">${dates.values
          .map((v) => `<option value="${esc(v)}">${esc(v)}</option>`)
          .join("")}</select></label><button type="button" id="q-newer">${tr("更新", "Newer")}</button></div>`;
  const symbol = `<label>${tr("标的", "Symbol")} <input id="q-symbol" placeholder="600519.SH" size="12"></label>`;
  // PIT datasets have no default "current" view. load() refuses without a
  // cutoff, on purpose. Seed it with today so the tab opens on something, and
  // say what it means.
  const asOf = d.pit
    ? `<label title="${tr("PIT：只保留在该日之前已披露的事实，并取当时现行的那一版", "PIT: keep facts announced on or before this date, at the version current then")}">
         as_of <input id="q-asof" type="date" value="${new Date().toISOString().slice(0, 10)}"></label>`
    : "";
  const adjust = d.adjustable
    ? `<label>${tr("复权", "Adjust")} <select id="q-adjust">
         <option value="">${tr("不复权", "None")}</option><option value="hfq">hfq</option>
         <option value="qfq">qfq</option></select></label>`
    : "";
  const note = dates.note ? `<p class="muted">${esc(dates.note)}</p>` : "";
  return `<div class="controls">${picker}${symbol}${asOf}${adjust}
    <button id="q-run">${tr("查询", "Query")}</button></div>${note}`;
}

const PROVENANCE_COLUMNS = ["source", "data_version", "fetched_at"];

function numericDtype(dtype) {
  const text = String(dtype || "").toLowerCase();
  return text.startsWith("int") || text.startsWith("uint") || text.startsWith("float") || text.startsWith("decimal");
}

function formatCell(column, value, dtype) {
  if (value === null || value === undefined) return '<span class="muted">null</span>';
  if (numericDtype(dtype)) {
    const number = typeof value === "number" ? value : Number(value);
    if (Number.isFinite(number)) return `<span title="${esc(value)}">${esc(number.toLocaleString(getLang() === "en" ? "en" : "zh-CN"))}</span>`;
  }
  if (column === "symbol") return `<button type="button" class="cell-symbol" data-symbol="${esc(value)}">${esc(value)}</button>`;
  return esc(value);
}

function downloadRows(page, ext, columns) {
  const sep = ext === "tsv" ? "\t" : ",";
  const cols = columns || page.columns;
  const indexOf = new Map(page.columns.map((column, index) => [column, index]));
  const cell = (value) => {
    const text = value == null ? "" : String(value);
    if (ext === "tsv") return text.split("\t").join(" ").split("\n").join(" ").split("\r").join(" ");
    const needsQuote = text.includes('"') || text.includes(",") || text.includes("\n") || text.includes("\r");
    return needsQuote ? `"${text.replaceAll('"', '""')}"` : text;
  };
  const lines = [cols.map(cell).join(sep), ...page.rows.map((row) => cols.map((column) => cell(row[indexOf.get(column)])).join(sep))];
  const blob = new Blob([`\uFEFF${lines.join("\n")}`], { type: "text/plain;charset=utf-8" });
  const link = document.createElement("a");
  const url = URL.createObjectURL(blob);
  link.href = url;
  link.download = ext === "tsv" ? "rows.tsv" : "rows.csv";
  link.click();
  URL.revokeObjectURL(url);
}

function columnOrder(columns) {
  const body = [];
  const prov = [];
  for (const column of columns) {
    if (PROVENANCE_COLUMNS.includes(column)) prov.push(column);
    else body.push(column);
  }
  return body.concat(PROVENANCE_COLUMNS.filter((column) => prov.includes(column)));
}

function rowTable(page, primaryKey, schema, showProv = true) {
  if (!page.rows.length) return `<p class="muted">${tr("没有匹配的行。", "No matching rows.")}</p>`;
  const keys = primaryKey || [];
  const dtypes = new Map((schema || []).map((col) => [col.column, col.dtype]));
  const order = columnOrder(page.columns);
  const indexOf = new Map(page.columns.map((column, index) => [column, index]));
  const provStart = order.find((column) => PROVENANCE_COLUMNS.includes(column));
  const cellClass = (column, sticky) =>
    [sticky ? "pk sticky" : keys.includes(column) ? "pk" : "", numericDtype(dtypes.get(column)) ? "n" : "", column === provStart ? "prov-start" : "", PROVENANCE_COLUMNS.includes(column) ? "prov" : ""].filter(Boolean).join(" ");
  let stuck = false;
  const head = order
    .map((column) => {
      const sticky = keys.includes(column) && !stuck;
      if (sticky) stuck = true;
      const cls = cellClass(column, sticky).split(" ").filter((name) => name !== "n").join(" ");
      return `<th${cls ? ` class="${cls}"` : ""}>${esc(column)}</th>`;
    })
    .join("");
  const body = page.rows
    .map((row) => {
      stuck = false;
      const cells = order.map((column) => {
        const sticky = keys.includes(column) && !stuck;
        if (sticky) stuck = true;
        const cls = cellClass(column, sticky);
        return `<td${cls ? ` class="${cls}"` : ""}>${formatCell(column, row[indexOf.get(column)], dtypes.get(column))}</td>`;
      });
      return `<tr>${cells.join("")}</tr>`;
    })
    .join("");
  const shown = `${page.offset + 1}-${page.offset + page.rows.length} / ${fmt(page.total)}`;
  const quality = page.adjustment;
  const factorNote = quality ? `<p class="meta-row"><span>${tr("复权来源", "Factor sources")} ${esc(Object.keys(quality.factor_sources || {}).join(", ") || "—")}</span><span>${quality.diagnostics_assessed ? tr(`待核实事件 ${quality.related_event_count || 0} 个`, `${quality.related_event_count || 0} unresolved events`) : tr("历史事件未核验", "Historical events not assessed")}</span></p>` : "";
  const provButton = order.some((column) => PROVENANCE_COLUMNS.includes(column))
    ? `<button id="q-prov" type="button">${showProv ? tr("隐藏溯源列", "Hide provenance") : tr("显示溯源列", "Show provenance")}</button>`
    : "";
  return `${factorNote}<div class="scroll"><table class="rows${showProv ? " show-prov" : ""}"><tr>${head}</tr>${body}</table></div>
    <div class="controls">
      <button id="q-prev" type="button" ${page.offset === 0 ? "disabled" : ""}>${tr("上一页", "Previous")}</button>
      <button id="q-next" type="button" ${page.offset + page.limit >= page.total ? "disabled" : ""}>${tr("下一页", "Next")}</button>
      <span class="muted">${shown}</span>
      <button id="q-csv" type="button" title="${tr("导出当前页", "Export this page")}">CSV</button>
      <button id="q-tsv" type="button" title="${tr("导出当前页", "Export this page")}">TSV</button>
      ${provButton}
    </div>`;
}

async function dataTab(d, host, view) {
  const enc = encodeURIComponent(d.dataset);
  const dates = await api(`/api/datasets/${enc}/dates`);
  if (!view.live()) return;
  host.innerHTML = `${dataControls(d, dates)}<div id="q-out"></div>`;

  const state = { offset: 0, provenance: true };
  const out = document.getElementById("q-out");
  let shown = null;

  function bindPage(page) {
    const prev = document.getElementById("q-prev");
    const next = document.getElementById("q-next");
    if (prev) prev.onclick = () => { state.offset = Math.max(0, state.offset - page.limit); run(); };
    if (next) next.onclick = () => { state.offset += page.limit; run(); };
    const csv = document.getElementById("q-csv");
    const tsv = document.getElementById("q-tsv");
    const columns = columnOrder(page.columns).filter((column) => state.provenance || !PROVENANCE_COLUMNS.includes(column));
    if (csv) csv.onclick = () => downloadRows(page, "csv", columns);
    if (tsv) tsv.onclick = () => downloadRows(page, "tsv", columns);
    const prov = document.getElementById("q-prov");
    if (prov) prov.onclick = () => {
      state.provenance = !state.provenance;
      if (shown) paint(shown);
    };
    if (typeof out.querySelectorAll === "function") {
      out.querySelectorAll("[data-symbol]").forEach((button) => {
        button.onclick = () => {
          const input = document.getElementById("q-symbol");
          if (!input) return;
          input.value = button.dataset.symbol || "";
          state.offset = 0;
          run();
        };
      });
    }
  }

  function paint(page) {
    shown = page;
    out.innerHTML = rowTable(page, d.primary_key, d.schema, state.provenance);
    bindPage(page);
  }

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
    out.innerHTML = `<p class="muted">${tr("查询中…", "Querying…")}</p>`;
    try {
      const page = await api(`/api/datasets/${enc}/rows?${params}`);
      paint(page);
    } catch (err) {
      out.innerHTML = `<p class="err">${esc(err.message)}</p>`;
    }
  }

  document.getElementById("q-run").onclick = () => {
    state.offset = 0;
    run();
  };
  const stepPeriod = (delta) => {
    const select = document.getElementById("q-period");
    if (!select) return;
    const next = select.selectedIndex + delta;
    if (next < 0 || next >= select.options.length) return;
    select.selectedIndex = next;
    state.offset = 0;
    syncPeriod();
    run();
  };
  const syncPeriod = () => {
    const select = document.getElementById("q-period");
    const older = document.getElementById("q-older");
    const newer = document.getElementById("q-newer");
    if (!select || !older || !newer) return;
    older.disabled = select.selectedIndex >= select.options.length - 1;
    newer.disabled = select.selectedIndex <= 0;
  };
  const older = document.getElementById("q-older");
  const newer = document.getElementById("q-newer");
  const period = document.getElementById("q-period");
  if (older) older.onclick = () => stepPeriod(1);
  if (newer) newer.onclick = () => stepPeriod(-1);
  if (period) period.onchange = () => { state.offset = 0; syncPeriod(); run(); };
  syncPeriod();
  host.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    const id = event.target?.id;
    if (id !== "q-symbol" && id !== "q-asof" && id !== "q-period") return;
    event.preventDefault();
    state.offset = 0;
    run();
  });
  run();
}

async function renderDetail(name, tab, view) {
  view.setPage(`<div class="loading-state"><span>${tr(`加载 ${esc(ds(name))}…`, `Loading ${esc(ds(name))}…`)}</span></div>`, "datasets");
  const enc = encodeURIComponent(name);
  const [d, series, prov, parts, health] = await Promise.all([
    api(`/api/datasets/${enc}`),
    api(`/api/datasets/${enc}/provenance/series`),
    api(`/api/datasets/${enc}/provenance`),
    api(`/api/datasets/${enc}/partitions`).catch(() => []),
    api("/api/health").catch(() => null),
  ]);
  if (!view.live()) return;
  const cls = d.freshness === "fresh" ? "fresh" : d.freshness === "stale" ? "stale" : "empty";
  setPage(`
    <section class="page-heading">
      <div class="heading-row"><div><div class="page-title-with-status"><h1>${dsMarkup(d.dataset)}</h1>${statusPill(cls)}${removedNote(d)}</div>
        <p class="meta-row"><span>${esc(tierText(d.tier, d.tier_label))}</span><span>${esc(d.layer)}</span><span>${esc(historyText(d.history_mode))}</span>${d.required ? "" : `<span>${tr("可选", "Optional")}</span>`}</p></div>
        <div class="action-row">${datasetActionButtons(d)}</div>
      </div>
    </section>
    <section class="metric-grid detail-metrics" aria-label="${tr("数据集关键指标", "Dataset metrics")}">
      ${kpi(`<span title="${fmt(d.row_count)} ${tr("行", "rows")}">${compact(d.row_count)}</span>`, tr("行数", "Rows"), "curated")}
      ${kpi(mb(d.bytes), tr("存储", "Storage"), "curated")}
      ${kpi(esc(d.watermark || "-"), tr("水位", "Watermark"), d.watermarked ? tr("维护中", "Maintained") : tr("不维护水位", "No watermark"))}
      ${kpi(esc(d.coverage_end || "-"), tr("覆盖至", "Covered through"), partitionText(d.granularity))}
    </section>
    <section class="surface-panel detail-workspace">
    <nav class="tabs" aria-label="${tr("数据集详情标签页", "Dataset tabs")}">
      ${["state", "meta", "data"]
        .map(
          (name) =>
            `<a class="tab ${tab === name ? "on" : ""}" href="#/dataset/${enc}/${name}" aria-current="${tab === name ? "page" : "false"}">${
              { state: tr("状态", "Status"), meta: tr("元数据", "Metadata"), data: tr("数据", "Data") }[name]
            }</a>`,
        )
        .join("")}
    </nav>
    <div id="tabbody" class="tab-body">${tab === "meta" ? metaTab(d) : tab === "data" ? "" : stateTab(d, prov, health?.anchor || "")}</div>
    </section>`, "datasets");

  if (tab === "state") {
    provenanceSeries(document.getElementById("prov"), series);
    const paintParts = (mode) => {
      partitionSeries(document.getElementById("parts"), parts, mode);
      document.querySelectorAll("#parts-mode button").forEach((button) => {
        button.className = `button ${button.dataset.partMode === mode ? "button-primary" : "button-ghost"}`;
      });
    };
    paintParts("both");
    document.getElementById("parts-mode")?.addEventListener("click", (event) => {
      const button = event.target.closest("[data-part-mode]");
      if (button) paintParts(button.dataset.partMode);
    });
    document.getElementById("provnote").textContent = tr(`每点跨度：${bucketText(series.bucket)}`, `Each point spans a ${bucketText(series.bucket)}`);
  } else if (tab === "data") {
    await dataTab(d, document.getElementById("tabbody"), view);
    if (!view.live()) return;
  }

  app.querySelectorAll("button[data-copy]").forEach((b) => {
    b.onclick = () => navigator.clipboard?.writeText(document.getElementById(b.dataset.copy).textContent);
  });
}


// --- quality ----------------------------------------------------------------

async function renderQuality(view) {
  const q = await api("/api/quality");
  if (!view.live()) return;
  const latestFindings = q.findings_runs[0]?.by_severity || {};
  const quarantineFiles = q.quarantine.reduce((sum, item) => sum + (item.files || 0), 0);
  const quarantineBytes = q.quarantine.reduce((sum, item) => sum + (item.bytes || 0), 0);
  const cacheEntries = q.on_demand.reduce((sum, item) => sum + (item.entries || 0), 0);

  // check×n joined by "、" was a wall of grey the eye slid off. Same counts,
  // one chip each, so the frequent check is findable without reading the row.
  const checks = (pairs) => chipRow(pairs.map(([c, n]) => chip(c, "", n)));

  const findingsRows = q.findings_runs.map((r) => {
    const sev = r.by_severity;
    return `<tr class="data-row">
      <td class="cell-time"><a class="table-link" href="#/quality/${encodeURIComponent(r.run_id)}">${esc(r.trade_date || "-")}</a></td>
      <td class="n">${num(sev.error, "err")}</td>
      <td class="n">${num(sev.warning)}</td>
      <td class="n">${num(sev.info)}</td>
      <td>${checks(r.top_checks)}</td>
    </tr>`;
  });

  const diffRows = q.diff_runs.map(
    (r) => `<tr class="data-row">
      <td class="cell-time"><a class="table-link" href="#/quality/${encodeURIComponent(r.run_id)}">${esc(r.trade_date || "-")}</a></td>
      <td class="n">${num(r.diff_count)}</td>
      <td>${checks(Object.entries(r.by_check))}</td>
    </tr>`,
  );

  const quarantineRows = q.quarantine.map(
    (e) => `<tr class="data-row"><td><code>${esc(e.name)}</code></td><td class="n">${fmt(e.files)}</td>
      <td class="n">${mb(e.bytes)}</td><td class="muted cell-time">${esc(e.modified.slice(0, 10))}</td></tr>`,
  );

  const onDemandRows = q.on_demand.map(
    (e) => `<tr class="data-row"><td>${dsLink(e.dataset)}</td><td class="n">${fmt(e.entries)}</td>
      <td class="n">${mb(e.bytes)}</td><td class="muted cell-time">${esc((e.newest || "").slice(0, 10)) || "-"}</td></tr>`,
  );

  setPage(`
    <section class="page-heading"><div class="heading-row"><h1>${tr("质量", "Quality")}</h1>
      <div class="action-row"><a class="${latestFindings.error ? "button button-primary" : "button button-ghost"}" href="#/ops?op=check.audit">${tr("全湖审计", "Full-lake audit")}</a></div></div></section>
    <section class="metric-grid quality-metrics" aria-label="${tr("质量指标", "Quality metrics")}">
      ${kpi(num(q.findings_runs.length), tr("审计快照", "Audit snapshots"), q.findings_runs[0]?.trade_date || tr("暂无", "None yet"))}
      ${kpi(num(latestFindings.error, "err"), tr("最新错误", "Latest errors"), "", latestFindings.error ? "alert" : "")}
      ${kpi(num(latestFindings.warning), tr("最新警告", "Latest warnings"))}
      ${kpi(num(quarantineFiles), tr("隔离文件", "Quarantine files"), mb(quarantineBytes))}
      ${kpi(num(cacheEntries), tr("按需缓存", "On-demand cache"))}
    </section>
    <div class="report-stack">
    <section class="surface-panel report-panel"><div class="panel-header"><h2>${tr("审计趋势", "Audit trend")}</h2></div>
    <div id="sev-chart"></div>
    ${dataTable(
      [tr("审计日", "Audit date"), { h: statusText("error"), n: true }, { h: statusText("warning"), n: true }, { h: statusText("info"), n: true }, tr("主要检查", "Top checks")],
      findingsRows,
      tr("尚无审计发现。", "No findings yet."),
    )}</section>

    <section class="surface-panel report-panel"><div class="panel-header"><h2>${tr("跨源比对", "Cross-source")}</h2></div>
    ${panelNote(tr(
      "<code>no_overlap</code> 表示没有共同主键，不是两边一致。",
      "<code>no_overlap</code> means there is no shared key, not that the two sides agree.",
    ))}
    ${dataTable(
      [tr("审计日", "Audit date"), { h: tr("差异", "Diffs"), n: true }, tr("检查项", "Checks")],
      diffRows,
      tr("尚无跨源比对。", "No cross-source output yet."),
    )}</section>

    <section class="surface-panel report-panel"><div class="panel-header"><h2>${tr("隔离区", "Quarantine")}</h2></div>
    ${dataTable(
      [tr("目录", "Directory"), { h: tr("文件", "Files"), n: true }, { h: tr("体积", "Size"), n: true }, tr("修改日期", "Modified")],
      quarantineRows,
      tr("隔离区为空。", "Quarantine is empty."),
    )}</section>

    <section class="surface-panel report-panel"><div class="panel-header"><h2>${tr("按需缓存", "On-demand cache")}</h2><span class="panel-meta">meta/on_demand</span></div>
    ${dataTable(
      [tr("数据集", "Dataset"), { h: tr("条目", "Entries"), n: true }, { h: tr("体积", "Size"), n: true }, tr("最新", "Newest")],
      onDemandRows,
      tr("尚无记录。", "No entries yet."),
    )}</section></div>`, "quality");

  severityTimeline(document.getElementById("sev-chart"), q.findings_runs);
}

async function renderQualityRun(runId, view) {
  const d = await api(`/api/quality/runs/${encodeURIComponent(runId)}`);
  if (!view.live()) return;
  // severity was coloured only when it read exactly "error", so a warning row
  // looked identical to an info row. It is the column the reader sorts by eye.
  const table = (rows, cols) =>
    dataTable(
      cols,
      rows.map(
        (r) => `<tr class="data-row">${cols
          .map((c) => {
            const v = r[c];
            if (v === undefined || v === null || v === "") return '<td><span class="muted">-</span></td>';
            if (c === "severity") return `<td>${chip(statusText(v), TONE[v])}</td>`;
            if (c === "dataset") return `<td>${dsLink(v)}</td>`;
            return `<td>${esc(v)}</td>`;
          })
          .join("")}</tr>`,
      ),
      tr("无。", "None."),
    );

  setPage(`
    <section class="page-heading"><div class="heading-row"><div><h1>${esc(d.trade_date || runId.slice(0, 8))}</h1><p class="meta-row"><code>${esc(d.run_id)}</code></p></div><div class="action-row"><a class="button button-ghost" href="#/quality">${tr("返回质量", "Quality")}</a></div></div></section>
    <section class="metric-grid metrics-2" aria-label="${tr("审计指标", "Audit metrics")}">${kpi(num(d.findings.length), tr("审计发现", "Findings"))}${kpi(num(d.diffs.length), tr("跨源差异", "Diffs"))}</section>
    <div class="report-stack"><section class="surface-panel report-panel"><div class="panel-header"><h2>${tr("审计发现", "Findings")}</h2><span class="panel-meta">${d.findings.length}</span></div>${table(d.findings, ["severity", "dataset", "check", "message"])}</section>
    <section class="surface-panel report-panel"><div class="panel-header"><h2>${tr("跨源差异", "Cross-source diffs")}</h2><span class="panel-meta">${d.diffs.length}</span></div>${table(d.diffs, ["severity", "dataset", "check", "field", "bps", "message"])}</section></div>`, "quality");
}

function runCtx(view) {
  return {
    api,
    setPage: view.setPage,
    esc,
    dataTable,
    kpi,
    statusPill,
    tally,
    dsLink,
    fmt,
    num,
    compact,
    failureLine,
    token: TOKEN,
  };
}

// --- routing ----------------------------------------------------------------

async function route() {
  disposeAll();
  closeRunStream();
  closeStorage();
  closeOps();
  const view = beginRoute();
  const ctx = { api, setPage: view.setPage, esc, dataTable };
  const dataset = location.hash.match(/^#\/dataset\/([^/]+)(?:\/(state|meta|data))?/);
  const run = location.hash.match(/^#\/runs\/([^/?]+)/);
  const qrun = location.hash.match(/^#\/quality\/(.+)$/);
  const opsJob = location.hash.match(/^#\/ops\/jobs\/([^/?]+)/);
  const opsPage = location.hash === "#/ops" || location.hash.startsWith("#/ops?");
  try {
    if (opsJob) await renderOps({ ...ctx, jobId: decodeURIComponent(opsJob[1]) });
    else if (opsPage) await renderOps(ctx);
    else if (location.hash === "#/storage") await renderStorage(ctx);
    else if (dataset) await renderDetail(decodeURIComponent(dataset[1]), dataset[2] || "state", view);
    else if (run) await renderRunDetail(decodeURIComponent(run[1]), runCtx(view));
    else if (location.hash.startsWith("#/runs")) await renderRuns(runCtx(view));
    else if (qrun) await renderQualityRun(decodeURIComponent(qrun[1]), view);
    else if (location.hash.startsWith("#/quality")) await renderQuality(view);
    else if (location.hash.startsWith("#/datasets")) await renderDatasets(view);
    else await renderOverview(view);
    if (view.live() && !location.hash.includes("?")) window.scrollTo(0, 0);
  } catch (err) {
    if (!view.live()) return;
    if (String(err.message || "").includes("尚未配置")) { // i18n-ignore
      await renderOps(ctx);
      return;
    }
    view.setPage(`<section class="error-state"><h1>${tr("加载失败", "Failed to load")}</h1><p class="sub err">${esc(err.message)}</p><a class="button button-primary" href="#/">${tr("返回概览", "Back to overview")}</a></section>`);
  }
}

// Delegated so it survives every re-render.
document.addEventListener("click", (e) => {
  const langButton = e.target.closest("[data-set-lang]");
  if (langButton) {
    setLang(langButton.dataset.setLang);
    return;
  }
  const link = e.target.closest("[data-ds]");
  if (link) {
    e.stopPropagation();
    location.hash = `#/dataset/${encodeURIComponent(link.dataset.ds)}`;
  }
});
onLanguageChange(() => route());
window.addEventListener("hashchange", route);
route();
