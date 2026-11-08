// The operations page's "today" pictures: a Beijing-time timeline of today's
// jobs, a digest of recent jobs that folds the frequent event streams, the
// daily-group matrix, and the stale-dataset list. Pure functions over API
// payloads, so the tests can drive them without a page.
import { ds, locale, statusText, tr } from "./i18n.js";

const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const BEIJING_MS = 8 * 3600 * 1000;
const DAY_MINUTES = 1440;

// --- jobs --------------------------------------------------------------------

/** One colour family per outcome; the same words the job page uses. */
export function jobTone(state) {
  if (state === "succeeded" || state === "complete") return "ok";
  if (state === "partial" || state === "findings") return "warn";
  if (state === "running" || state === "starting") return "live";
  if (["failed", "lost", "cancelled", "error", "interrupted", "rejected"].includes(state)) return "bad";
  return "muted";
}

export function laneOf(op = "") {
  if (op === "events.run") return "events";
  if (op === "daily.full" || op === "daily.group" || op.startsWith("init.")) return "daily";
  if (op === "daily.stale" || op === "backfill.run" || op === "derive.run" || op.startsWith("run.")) return "fill";
  if (op.startsWith("snapshot.")) return "backup";
  return "checks";
}

function lanes() {
  return [
    { id: "daily", label: tr("日更", "Daily") },
    { id: "fill", label: tr("补数", "Fill") },
    { id: "events", label: tr("事件流", "Events") },
    { id: "checks", label: tr("巡检", "Checks") },
    { id: "backup", label: tr("备份", "Backup") },
  ];
}

const FILTERS = ["all", "problem", "daily", "fill", "events", "checks"];

export function filterLabel(id) {
  return {
    all: tr("全部", "All"),
    problem: tr("有问题", "Needs a look"),
    daily: tr("日更", "Daily"),
    fill: tr("补数", "Fill"),
    events: tr("事件流", "Events"),
    checks: tr("巡检", "Checks"),
  }[id];
}

export function jobFilters() {
  return FILTERS;
}

const REASONS = {
  source_transient: () => tr("数据源暂时不可用", "source temporarily unavailable"),
  execution_error: () => tr("执行出错", "execution error"),
  no_data: () => tr("源没有返回数据", "no data from the source"),
  source_disabled: () => tr("数据源已关闭", "source switched off"),
  coverage_gap: () => tr("覆盖有缺口", "coverage gap"),
  breaker_open: () => tr("熔断中", "circuit breaker open"),
  coverage_limited: () => tr("覆盖受限", "coverage limited"),
  source_payload_invalid: () => tr("源返回的数据无效", "invalid data from the source"),
};

export function reasonLabel(code) {
  return REASONS[code] ? REASONS[code]() : code;
}

/**
 * Datasets a partial or failed job left short, with the reason codes. A run
 * result carries ``fallback`` itself; a daily result carries it per group and
 * for its event stream.
 */
export function jobReasons(job) {
  const summary = job?.result?.summary;
  if (!summary || typeof summary !== "object") return [];
  const parts = [summary, ...(Array.isArray(summary.groups) ? summary.groups : []), summary.events];
  const found = [];
  for (const part of parts) {
    for (const item of Array.isArray(part?.fallback) ? part.fallback : []) {
      if (item?.dataset) found.push({ dataset: item.dataset, codes: Array.isArray(item.reason_codes) ? item.reason_codes : [] });
    }
  }
  return found;
}

/** Failed batches the server read from the job's runs; covers results with no fallback. */
export function failedBatches(job) {
  return (Array.isArray(job?.failed_datasets) ? job.failed_datasets : [])
    .filter((item) => item?.dataset)
    .map((item) => ({ dataset: item.dataset, codes: Array.isArray(item.reason_codes) ? item.reason_codes : [] }));
}

function rowsText(n) {
  const number = Number(n) || 0;
  const shown = number >= 10000
    ? new Intl.NumberFormat(locale(), { notation: "compact", maximumFractionDigits: 1 }).format(number)
    : number.toLocaleString(locale());
  return tr(`写入 ${shown} 行`, `${shown} rows written`);
}

function runLinks(job) {
  const ids = Array.isArray(job?.result?.run_ids) ? job.result.run_ids.filter(Boolean) : [];
  if (!ids.length) return "";
  if (ids.length === 1) return `<a href="#/runs/${encodeURIComponent(ids[0])}">${tr("运行", "Run")}</a>`;
  return `<a href="#/ops/jobs/${encodeURIComponent(job.job_id)}">${tr(`${ids.length} 个运行`, `${ids.length} runs`)}</a>`;
}

/** "Failed: capital, research · Degraded: core" for a daily job's groups. */
export function groupText(job) {
  const groups = job?.result?.summary?.groups;
  if (!Array.isArray(groups) || !groups.length) return "";
  const pick = (statuses) => groups.filter((group) => statuses.includes(group.status)).map((group) => group.group);
  const failed = pick(["failed", "interrupted", "blocked"]);
  const degraded = pick(["degraded", "warning", "partial"]);
  const parts = [];
  if (failed.length) parts.push(tr(`失败：${failed.join("、")}`, `Failed: ${failed.join(", ")}`));
  if (degraded.length) parts.push(tr(`降级：${degraded.join("、")}`, `Degraded: ${degraded.join(", ")}`));
  return parts.join(" · ");
}

/** "Flash news, News headlines: source temporarily unavailable" — grouped by reason. */
export function reasonText(reasons) {
  if (!reasons.length) return "";
  const byCode = new Map();
  for (const { dataset, codes } of reasons) {
    for (const code of codes.length ? codes : [""]) {
      if (!byCode.has(code)) byCode.set(code, []);
      if (!byCode.get(code).includes(dataset)) byCode.get(code).push(dataset);
    }
  }
  return [...byCode.entries()]
    .map(([code, names]) => {
      const shown = names.slice(0, 3).map((name) => ds(name)).join(tr("、", ", "));
      const more = names.length > 3 ? tr(` 等 ${names.length} 个`, ` and ${names.length - 3} more`) : "";
      return code ? `${shown}${more}${tr("：", ": ")}${reasonLabel(code)}` : `${shown}${more}`;
    })
    .join(tr("；", "; "));
}

function parsed(raw) {
  if (!raw) return null;
  const value = Date.parse(raw);
  return Number.isFinite(value) ? value : null;
}

export function durationText(job, now = Date.now()) {
  const start = parsed(job.started_at);
  if (start === null) return "";
  const end = parsed(job.finished_at) ?? now;
  const seconds = Math.max(0, Math.round((end - start) / 1000));
  if (seconds < 60) return tr(`${seconds} 秒`, `${seconds}s`);
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return tr(`${minutes} 分钟`, `${minutes} min`);
  const hours = Math.floor(minutes / 60);
  return tr(`${hours} 小时 ${minutes % 60} 分`, `${hours}h ${minutes % 60}m`);
}

/** The Beijing calendar date of an instant, as YYYY-MM-DD. */
export function beijingDate(ms) {
  return new Date(ms + BEIJING_MS).toISOString().slice(0, 10);
}

/** The UTC instant of 00:00 Beijing time on the Beijing date of ``now``. */
export function beijingMidnight(now = Date.now()) {
  return Date.parse(`${beijingDate(now)}T00:00:00+08:00`);
}

function clock(ms) {
  return new Date(ms + BEIJING_MS).toISOString().slice(11, 16);
}

function stamp(job) {
  const at = parsed(job.finished_at) ?? parsed(job.created_at);
  if (at === null) return "";
  return new Intl.DateTimeFormat(locale(), {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
    timeZone: "Asia/Shanghai",
  }).format(new Date(at));
}

const title = (job) => (tr(job.title, job.title_en || job.title) || job.op || "");
const stateLabel = (job) => tr(job.label || job.state, job.label_en || job.label || job.state);

/**
 * Rows for the recent-jobs list, newest first. Event streams run every hour or
 * so and would bury everything else, so each Beijing day's event jobs fold
 * into one row with a tally and the datasets they keep missing.
 */
export function digestJobs(jobs) {
  const seen = new Set();
  const rows = [];
  const events = new Map();
  for (const job of jobs || []) {
    if (!job || seen.has(job.job_id)) continue;
    seen.add(job.job_id);
    const at = parsed(job.created_at) ?? 0;
    if (job.op === "events.run") {
      const day = beijingDate(at);
      if (!events.has(day)) {
        const row = { kind: "events", day, at, jobs: [], tally: {}, missing: new Map() };
        events.set(day, row);
        rows.push(row);
      }
      const row = events.get(day);
      row.jobs.push(job);
      row.at = Math.max(row.at, at);
      const tone = jobTone(job.state);
      row.tally[tone] = (row.tally[tone] || 0) + 1;
      for (const reason of jobReasons(job)) {
        const entry = row.missing.get(reason.dataset) || { dataset: reason.dataset, codes: new Set(), count: 0 };
        entry.count += 1;
        for (const code of reason.codes) entry.codes.add(code);
        row.missing.set(reason.dataset, entry);
      }
    } else {
      rows.push({ kind: "job", at, job });
    }
  }
  return rows.sort((a, b) => b.at - a.at);
}

export function rowMatches(row, filter) {
  if (filter === "all") return true;
  if (filter === "problem") {
    if (row.kind === "events") return Boolean(row.tally.warn || row.tally.bad);
    const tone = jobTone(row.job.state);
    return tone === "warn" || tone === "bad";
  }
  if (row.kind === "events") return filter === "events";
  const lane = laneOf(row.job.op);
  return filter === lane || (filter === "checks" && lane === "backup");
}

function toneChip(tone, text, n) {
  const cls = { ok: "chip-fresh", warn: "chip-stale", bad: "chip-error", live: "chip-running" }[tone] || "";
  return `<span class="chip ${cls}">${esc(text)}${n != null ? ` <b>${n}</b>` : ""}</span>`;
}

function toneWord(tone) {
  return {
    ok: tr("成功", "Succeeded"),
    warn: tr("部分交付", "Partly delivered"),
    bad: tr("失败", "Failed"),
    live: tr("执行中", "Running"),
    muted: tr("跳过", "Skipped"),
  }[tone];
}

function jobRow(job) {
  const tone = jobTone(job.state);
  const failed = failedBatches(job);
  const reasons = [groupText(job) || reasonText(jobReasons(job)), failed.length && !groupText(job) && !jobReasons(job).length ? tr("失败：", "Failed: ") + reasonText(failed) : ""]
    .filter(Boolean)
    .join(" · ");
  const meta = [
    job.scheduled ? `<span>${tr("定时", "Scheduled")}</span>` : "",
    stamp(job) ? `<span>${esc(stamp(job))}</span>` : "",
    durationText(job) ? `<span>${esc(durationText(job))}</span>` : "",
    job.rows_written != null ? `<span>${esc(rowsText(job.rows_written))}</span>` : "",
    runLinks(job) ? `<span>${runLinks(job)}</span>` : "",
  ].join("");
  return `<li class="ops-job-row" data-tone="${tone}">
    <i class="ops-dot tone-${tone}" aria-hidden="true"></i>
    <div class="ops-job-main"><a href="#/ops/jobs/${encodeURIComponent(job.job_id)}">${esc(title(job))}</a>
      ${reasons ? `<div class="ops-job-why">${esc(reasons)}</div>` : ""}</div>
    <span class="meta-row">${toneChip(tone, stateLabel(job))}${meta}</span>
  </li>`;
}

function eventsRow(row) {
  const total = row.jobs.length;
  const worst = row.tally.bad ? "bad" : row.tally.warn ? "warn" : row.tally.live ? "live" : "ok";
  const tallies = ["bad", "warn", "ok", "live", "muted"]
    .filter((tone) => row.tally[tone])
    .map((tone) => toneChip(tone, toneWord(tone), row.tally[tone]))
    .join("");
  const missing = [...row.missing.values()].sort((a, b) => b.count - a.count);
  const why = missing.length
    ? tr("常缺：", "Often short: ") + missing.slice(0, 4).map((item) => `${ds(item.dataset)} ×${item.count}${item.codes.size ? tr(`（${[...item.codes].map(reasonLabel).join("、")}）`, ` (${[...item.codes].map(reasonLabel).join(", ")})`) : ""}`).join(tr("；", "; "))
    : "";
  const list = row.jobs.map(jobRow).join("");
  return `<li class="ops-job-row ops-job-events" data-tone="${worst}">
    <i class="ops-dot tone-${worst}" aria-hidden="true"></i>
    <details class="ops-job-main"><summary>${tr(`事件流 · ${row.day} · ${total} 次`, `Event streams · ${row.day} · ${total} runs`)}</summary>
      <ul class="ops-jobs ops-jobs-nested">${list}</ul></details>
    <span class="meta-row">${tallies}</span>
    ${why ? `<div class="ops-job-why">${esc(why)}</div>` : ""}
  </li>`;
}

const SHOWN_ROWS = 12;

export function recentHtml(rows, filter = "all", expanded = false) {
  const matching = rows.filter((row) => rowMatches(row, filter));
  const shown = expanded ? matching : matching.slice(0, SHOWN_ROWS);
  const chips = FILTERS.map((id) => {
    const n = rows.filter((row) => rowMatches(row, id)).length;
    return `<button type="button" class="ops-filter${id === filter ? " on" : ""}" data-job-filter="${id}" aria-pressed="${id === filter}">${esc(filterLabel(id))} <b>${n}</b></button>`;
  }).join("");
  const more = matching.length > shown.length
    ? `<button type="button" class="button button-ghost" data-job-more>${tr(`显示全部 ${matching.length} 条`, `Show all ${matching.length}`)}</button>`
    : "";
  const body = shown.length
    ? `<ul class="ops-jobs">${shown.map((row) => (row.kind === "events" ? eventsRow(row) : jobRow(row.job))).join("")}</ul>${more}`
    : `<p class="muted">${tr("这个分类下没有任务。", "No jobs in this view.")}</p>`;
  return `<section class="surface-panel report-panel ops-recent">
    <div class="panel-header"><h2>${tr("最近的任务", "Recent jobs")}</h2></div>
    <div class="ops-filter-row" role="group" aria-label="${tr("按类型筛选", "Filter by kind")}">${chips}</div>
    ${body}
  </section>`;
}

// --- timeline ----------------------------------------------------------------

function minutesAt(text) {
  const match = /^(\d{1,2}):(\d{2})$/.exec(String(text || ""));
  return match ? Number(match[1]) * 60 + Number(match[2]) : null;
}

/**
 * Lanes of job blocks plus the scheduled marks, in minutes since Beijing
 * midnight. ``daysBack`` 1 draws yesterday, which is where the evening's daily
 * update sits when the page is opened the next morning.
 */
export function timelineModel(jobs, schedule = {}, now = Date.now(), daysBack = 0) {
  const start = beijingMidnight(now) - daysBack * 86400000;
  const end = start + DAY_MINUTES * 60000;
  const nowMin = (now - start) / 60000;
  const byLane = new Map(lanes().map((lane) => [lane.id, { ...lane, blocks: [] }]));
  const seen = new Set();
  for (const job of jobs || []) {
    if (!job || seen.has(job.job_id)) continue;
    seen.add(job.job_id);
    const from = parsed(job.started_at) ?? parsed(job.created_at);
    if (from === null) continue;
    const tone = jobTone(job.state);
    const to = parsed(job.finished_at) ?? (tone === "live" ? now : from);
    if (to < start || from >= end) continue;
    const x0 = Math.max(0, (from - start) / 60000);
    const x1 = Math.min(DAY_MINUTES, Math.max(x0, (to - start) / 60000));
    byLane.get(laneOf(job.op)).blocks.push({ x0, x1, tone, job });
  }
  const marks = [];
  const add = (at, label, on = true) => {
    const minute = minutesAt(at);
    if (on && minute !== null) marks.push({ x: minute, label, at });
  };
  add(schedule.daily_run_at, tr("日更", "Daily"), schedule.daily !== false);
  add(schedule.stale_run_at, tr("收尾补抓", "Catch-up"), schedule.stale !== false && schedule.stale_run_at);
  add(schedule.backup_run_at, tr("备份", "Backup"), Boolean(schedule.backup));
  const used = [...byLane.values()].filter((lane) => lane.blocks.length || lane.id === "daily" || lane.id === "events");
  return { day: beijingDate(start), now: nowMin, lanes: used, marks };
}

export function timelineSvg(model) {
  const left = 64;
  const width = 1000;
  const laneH = 26;
  const top = 26;
  const plot = width - left - 8;
  const height = top + model.lanes.length * laneH + 22;
  const x = (minute) => left + (minute / DAY_MINUTES) * plot;
  const hours = [0, 3, 6, 9, 12, 15, 18, 21, 24]
    .map((h) => `<line class="tl-grid" x1="${x(h * 60)}" x2="${x(h * 60)}" y1="${top - 4}" y2="${height - 18}"/><text class="tl-hour" x="${x(h * 60)}" y="${height - 4}" text-anchor="${h === 24 ? "end" : h === 0 ? "start" : "middle"}">${String(h).padStart(2, "0")}:00</text>`)
    .join("");
  const rows = model.lanes
    .map((lane, index) => {
      const y = top + index * laneH;
      const blocks = lane.blocks
        .map((block) => {
          const w = Math.max(3, x(block.x1) - x(block.x0));
          const tip = `${title(block.job)} · ${stateLabel(block.job)} · ${clock(parsed(block.job.started_at) ?? parsed(block.job.created_at))}${durationText(block.job) ? ` · ${durationText(block.job)}` : ""}`;
          return `<a href="#/ops/jobs/${encodeURIComponent(block.job.job_id)}"><rect class="tl-job tone-${block.tone}" x="${x(block.x0).toFixed(1)}" y="${y + 5}" width="${w.toFixed(1)}" height="${laneH - 10}" rx="2"><title>${esc(tip)}</title></rect></a>`;
        })
        .join("");
      return `<text class="tl-lane" x="0" y="${y + laneH / 2 + 4}">${esc(lane.label)}</text><line class="tl-base" x1="${left}" x2="${width - 8}" y1="${y + laneH - 1}" y2="${y + laneH - 1}"/>${blocks}`;
    })
    .join("");
  const marks = model.marks
    .map((mark) => `<g class="tl-mark"><line x1="${x(mark.x)}" x2="${x(mark.x)}" y1="${top - 4}" y2="${height - 18}"/><text x="${x(mark.x)}" y="${top - 10}" text-anchor="middle">${esc(mark.label)} ${esc(mark.at)}</text></g>`)
    .join("");
  const now = model.now >= 0 && model.now <= DAY_MINUTES
    ? `<g class="tl-now"><line x1="${x(model.now)}" x2="${x(model.now)}" y1="${top - 4}" y2="${height - 18}"/><circle cx="${x(model.now)}" cy="${top - 4}" r="3"/><title>${esc(tr("现在", "Now"))}</title></g>`
    : "";
  return `<svg class="ops-timeline-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${esc(tr(`${model.day} 北京时间的任务时间线`, `Jobs on ${model.day}, Beijing time`))}">${hours}${rows}${marks}${now}</svg>`;
}

export function timelineLegend(eventsInterval) {
  const items = [["ok", tr("成功", "Succeeded")], ["warn", tr("部分交付", "Partly delivered")], ["bad", tr("失败", "Failed")], ["live", tr("执行中", "Running")]]
    .map(([tone, label]) => `<span><i class="ops-dot tone-${tone}"></i>${esc(label)}</span>`)
    .join("");
  const every = eventsInterval ? `<span>${tr(`事件流每 ${eventsInterval} 分钟一次`, `Event streams every ${eventsInterval} min`)}</span>` : "";
  return `<div class="ops-legend">${items}<span><i class="tl-mark-key"></i>${tr("定时时间", "Scheduled time")}</span><span><i class="tl-now-key"></i>${tr("现在", "Now")}</span>${every}</div>`;
}

// --- daily matrix ------------------------------------------------------------

function cellTone(status) {
  if (status === "success") return "ok";
  if (status === "degraded" || status === "warning" || status === "partial") return "warn";
  if (status === "running") return "live";
  if (status === "failed" || status === "interrupted" || status === "blocked") return "bad";
  return "muted";
}

export function matrixHtml(matrix) {
  const sessions = matrix?.sessions || [];
  const groups = matrix?.groups || [];
  if (!sessions.length || !groups.length) {
    return `<p class="muted">${tr("还没有带交易日的日更记录。", "No dated daily runs yet.")}</p>`;
  }
  const cells = new Map((matrix.cells || []).map((cell) => [`${cell.group}|${cell.trade_date}`, cell]));
  const head = sessions.map((day) => `<th scope="col" title="${esc(day)}">${esc(day.slice(5))}</th>`).join("");
  const body = groups
    .map((group) => {
      const tds = sessions
        .map((day) => {
          const cell = cells.get(`${group}|${day}`);
          if (!cell) return `<td><span class="ops-cell tone-none" title="${esc(tr(`${group} · ${day} · 没有运行`, `${group} · ${day} · did not run`))}"></span></td>`;
          const tone = cellTone(cell.status);
          const retried = cell.attempts > 1 ? tr(` · ${cell.attempts} 次尝试`, ` · ${cell.attempts} attempts`) : "";
          const tip = `${group} · ${day} · ${statusText(cell.status)}${retried}`;
          return `<td><a class="ops-cell tone-${tone}${cell.attempts > 1 ? " retried" : ""}" href="#/runs/${encodeURIComponent(cell.run_id)}" title="${esc(tip)}" aria-label="${esc(tip)}"></a></td>`;
        })
        .join("");
      return `<tr><th scope="row" title="${esc(group)}"><span>${esc(group)}</span></th>${tds}</tr>`;
    })
    .join("");
  return `<div class="ops-matrix-wrap"><table class="ops-matrix"><thead><tr><th></th>${head}</tr></thead><tbody>${body}</tbody></table></div>
    <div class="ops-legend"><span><i class="ops-cell tone-ok"></i>${tr("成功", "Success")}</span><span><i class="ops-cell tone-warn"></i>${tr("降级", "Degraded")}</span><span><i class="ops-cell tone-bad"></i>${tr("失败", "Failed")}</span><span><i class="ops-cell tone-none"></i>${tr("没有运行", "Did not run")}</span><span><i class="ops-cell tone-ok retried"></i>${tr("重试过", "Retried")}</span></div>`;
}

// --- stale datasets ----------------------------------------------------------

function daysBetween(from, to) {
  const a = Date.parse(`${from}T00:00:00Z`);
  const b = Date.parse(`${to}T00:00:00Z`);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
  return Math.round((b - a) / 86400000);
}

function nextDay(text) {
  const at = Date.parse(`${text}T00:00:00Z`);
  return Number.isFinite(at) ? new Date(at + 86400000).toISOString().slice(0, 10) : "";
}

/** Stale datasets with how far behind they are and the action that can fix them. */
export function staleRows(health, datasets) {
  const byName = new Map((datasets || []).map((item) => [item.dataset, item]));
  const anchor = health?.anchor || "";
  return (health?.stale_datasets || []).map((name) => {
    const item = byName.get(name) || {};
    const watermark = item.watermark || item.coverage_end || "";
    const lag = watermark && anchor ? daysBetween(watermark, anchor) : null;
    const snapshot = item.history_mode === "snapshot_only";
    const action = snapshot
      ? { op: "daily.stale", href: "#/ops?op=daily.stale", label: tr("补抓落后", "Catch up") }
      : {
          op: "backfill.run",
          href: `#/ops?op=backfill.run&dataset=${encodeURIComponent(name)}${watermark ? `&start=${nextDay(watermark)}` : ""}${anchor ? `&end=${anchor}` : ""}`,
          label: tr("回填", "Backfill"),
        };
    return { dataset: name, watermark, lag, snapshot, action };
  }).sort((a, b) => (b.lag ?? -1) - (a.lag ?? -1));
}

export function staleHtml(rows, anchor = "", disabled = false) {
  if (!rows.length) {
    return `<p class="ops-ok-line"><i class="ops-dot tone-ok"></i>${tr("没有落后的数据集。", "No dataset is stale.")}</p>`;
  }
  const most = Math.max(1, ...rows.map((row) => row.lag || 0));
  const items = rows
    .map((row) => {
      const width = row.lag ? Math.max(6, Math.round((Math.min(row.lag, most) / most) * 100)) : 0;
      const lag = row.lag != null ? tr(`落后 ${row.lag} 天`, `${row.lag} ${row.lag === 1 ? "day" : "days"} behind`) : tr("没有水位", "No watermark");
      const note = row.snapshot ? tr("快照：漏掉的日子补不回来", "Snapshot: missed days cannot be refetched") : "";
      const action = disabled
        ? ""
        : `<a class="button button-ghost" href="${esc(row.action.href)}">${esc(row.action.label)}</a>`;
      return `<li class="ops-stale-row">
        <div class="ops-stale-name"><a class="ds-link" href="#/dataset/${encodeURIComponent(row.dataset)}">${esc(ds(row.dataset))}</a>
          <span class="muted">${esc(row.watermark || "—")}${note ? ` · ${esc(note)}` : ""}</span></div>
        <div class="ops-stale-bar" title="${esc(lag)}"><i style="width:${width}%"></i><span>${esc(lag)}</span></div>
        <div class="ops-actions">${action}</div>
      </li>`;
    })
    .join("");
  const head = anchor ? `<p class="ops-hint">${tr(`以最近交易日 ${anchor} 为准。`, `Measured against the last session, ${anchor}.`)}</p>` : "";
  return `${head}<ul class="ops-stale">${items}</ul>`;
}
