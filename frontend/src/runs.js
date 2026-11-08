// Run history, plus the commands that write a manifest run.
// The form is the operations page form: preview, then confirm, one job at a time.
import { runGantt } from "./charts.js";
import { pick, statusText, tr } from "./i18n.js";
import { beginOps, mountOpForm, openJobStreams, opsIsCurrent, runLauncherModel } from "./ops.js";
import {
  NEEDS_ACTION,
  failurePreview,
  jobTitle,
  openBatchFailures,
  parseRunsHash,
  presetsEqual,
  runsHref,
  runsListPath,
} from "./runs-model.js";

const RUN_PAGE = 100;
const GENERIC_RUN_ERROR = "one or more core steps failed";

let runStream = null;
let runTicker = null;
let lastRunDetail = null;

export function closeRunStream() {
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

const ago = (iso) => {
  if (!iso) return "-";
  const secs = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (secs < 90) return tr(`${Math.round(secs)} 秒前`, `${Math.round(secs)}s ago`);
  if (secs < 5400) return tr(`${Math.round(secs / 60)} 分钟前`, `${Math.round(secs / 60)}m ago`);
  if (secs < 172800) return tr(`${Math.round(secs / 3600)} 小时前`, `${Math.round(secs / 3600)}h ago`);
  return tr(`${Math.round(secs / 86400)} 天前`, `${Math.round(secs / 86400)}d ago`);
};

const duration = (start, end) => {
  if (!start) return "-";
  const secs = Math.max(0, ((end ? new Date(end) : new Date()).getTime() - new Date(start).getTime()) / 1000);
  return secs < 90 ? `${Math.round(secs)}s` : `${Math.round(secs / 60)}m`;
};

function pageTotal(body) {
  if (!body || Array.isArray(body) || typeof body.total !== "number") return null;
  return body.total;
}

function runOpHref(state, op, params) {
  return runsHref({ op, preset: params || {} }, state);
}

function detailHref(runId, params) {
  const query = new URLSearchParams(params || {});
  const text = query.toString();
  const base = `#/runs/${encodeURIComponent(runId)}`;
  return text ? `${base}?${text}` : base;
}

function runActions(run, hrefFor, includeFailedGroups) {
  const links = [];
  const resumeInit = run.job_name === "init" && !["success", "running"].includes(run.status);
  if (resumeInit) {
    links.push({ href: hrefFor("init.resume", { run_id: run.run_id }), label: tr("继续初始化", "Resume initialization"), primary: true });
  } else if (NEEDS_ACTION.has(run.status)) {
    links.push({ href: hrefFor("run.retry", { run_id: run.run_id }), label: tr("重试此 run", "Retry this run"), primary: true });
  }
  if (includeFailedGroups && run.status === "failed" && !resumeInit) {
    links.push({ href: hrefFor("run.retry_failed_groups", {}), label: tr("重试失败的日更组", "Retry failed daily groups"), primary: false });
  }
  if (run.has_staging && run.status !== "running") {
    links.push({ href: hrefFor("run.compact", { run_id: run.run_id }), label: tr("发布 staging", "Publish staging"), primary: false });
  }
  return links;
}

function actionHtml(links, esc, asButtons) {
  if (!links.length) return '<span class="muted">-</span>';
  const body = links
    .map((link) => {
      const cls = asButtons ? `button ${link.primary ? "button-primary" : "button-ghost"}` : "";
      return `<a class="${cls}" href="${esc(link.href)}">${esc(link.label)}</a>`;
    })
    .join("");
  return asButtons ? body : `<div class="cell-actions">${body}</div>`;
}

function failureHtml(run, ctx) {
  const { shown, extra } = failurePreview(run, ctx.failureLine);
  if (!shown.length) return '<span class="muted">-</span>';
  const lines = shown
    .map((line) => {
      const who = line.dataset ? `${ctx.dsLink(line.dataset)} ` : "";
      return `<div class="failure-line">${who}<span class="cell-truncate" title="${ctx.esc(line.text)}"><span>${ctx.esc(line.text)}</span></span></div>`;
    })
    .join("");
  const more = extra
    ? `<div class="muted">${tr(`还有 ${extra} 条，打开详情查看。`, `${extra} more on the detail page.`)}</div>`
    : "";
  return lines + more;
}

function jobCell(run, ctx) {
  const title = jobTitle(run.job_name);
  const raw = String(run.job_name || "");
  const code = title === raw ? "" : `<small>${ctx.esc(raw)}</small>`;
  const panel = run.launch_id
    ? `<a class="muted" href="#/ops/jobs/${encodeURIComponent(run.launch_id)}">${tr("面板任务", "Panel job")}</a>`
    : "";
  return `<div class="run-job"><a class="table-link" href="#/runs/${encodeURIComponent(run.run_id)}">${ctx.esc(title)}</a>${code}${panel}</div>`;
}

function launcherButton(item, state, esc) {
  const active = state.op === item.id && presetsEqual(state.preset, item.preset);
  const tone = item.id === "daily.full" && !active ? "button-primary" : "button-ghost";
  const title = item.reason || item.summary || "";
  return `<button type="button" class="button ${tone}${active ? " on" : ""}" data-run-op="${esc(item.id)}" data-run-preset="${esc(JSON.stringify(item.preset))}" ${item.disabled ? "disabled" : ""} title="${esc(title)}">${esc(item.label)}</button>`;
}

function bindLauncher(state) {
  if (typeof document.querySelectorAll !== "function") return;
  document.querySelectorAll("[data-run-op]").forEach((button) => {
    button.onclick = () => {
      if (button.disabled) return;
      let preset = {};
      try {
        preset = JSON.parse(button.dataset.runPreset || "{}");
      } catch {
        preset = {};
      }
      const active = state.op === button.dataset.runOp && presetsEqual(state.preset, preset);
      location.hash = runsHref(
        active ? { op: "", preset: new URLSearchParams() } : { op: button.dataset.runOp, preset },
        state,
      );
    };
  });
}

function openRunForm(ctx, home, current, state, closeHref) {
  const host = document.getElementById("run-form");
  if (!host || !state.op || !opsIsCurrent(current)) return;
  if (!home) {
    host.innerHTML = `<p class="panel-note">${ctx.esc(tr("手动运行暂不可用。记录仍可查看。", "Manual runs are unavailable. History is still listed."))}</p>`;
    return;
  }
  const card = home.operations?.find((item) => item.id === state.op);
  if (!card || !card.available || !home.mode?.ops_enabled) {
    const reason = !home.mode?.ops_enabled
      ? tr("当前模式不能从面板启动命令。", "This mode cannot start commands from the panel.")
      : pick(card, "unavailable_reason") || tr("当前没有这项操作。", "This operation is not available.");
    host.innerHTML = `<p class="panel-note">${ctx.esc(reason)}</p>`;
    return;
  }
  mountOpForm(ctx, home, state.op, state.preset, host, current);
  const header = host.querySelector?.(".panel-header");
  if (header && closeHref) {
    header.insertAdjacentHTML(
      "beforeend",
      `<a class="button button-ghost" href="${ctx.esc(closeHref)}">${tr("关闭", "Close")}</a>`,
    );
  }
}

function countText(value, num) {
  return value == null ? '<span class="muted">—</span>' : num(value);
}

function metricLink(value, label, note, href, extra = "") {
  return `<a class="metric-card${extra}" href="${href}"><div class="metric-label">${label}</div><div class="metric-value">${value}</div>${note ? `<div class="metric-note">${note}</div>` : ""}</a>`;
}

export async function renderRuns(ctx) {
  const current = beginOps();
  const state = parseRunsHash(location.hash);
  const jobs = [
    ctx.api(runsListPath(state.view, state.offset, RUN_PAGE)),
    ctx.api(runsListPath("running", 0, 1)).catch(() => null),
    ctx.api(runsListPath("attention", 0, 1)).catch(() => null),
    ctx.api("/api/ops").catch(() => null),
  ];
  if (state.view !== "all") jobs.push(ctx.api("/api/runs?limit=1").catch(() => null));
  const [page, runningMeta, attentionMeta, home, allMeta] = await Promise.all(jobs);
  if (!opsIsCurrent(current)) return;
  const legacy = Array.isArray(page);
  const runs = legacy ? page : page.runs || [];
  const total = legacy ? runs.length : page.total;
  const start = runs.length ? state.offset + 1 : 0;
  const end = state.offset + runs.length;
  const allTotal = state.view === "all" ? total : pageTotal(allMeta);
  const runningTotal = pageTotal(runningMeta);
  const attentionTotal = pageTotal(attentionMeta);
  const launcher = runLauncherModel(home);
  const hrefFor = (op, params) => runOpHref(state, op, params);
  const rows = runs.map((run) => {
    const links = runActions(run, hrefFor, false);
    return `<tr class="data-row${run.status === "running" ? " data-row-live" : ""}">
      <td>${jobCell(run, ctx)}</td>
      <td>${ctx.statusPill(run.status)}</td>
      <td class="muted cell-time">${ago(run.started_at)}</td>
      <td class="n">${duration(run.started_at, run.finished_at)}</td>
      <td class="n">${ctx.fmt(run.rows_written)}</td>
      <td class="cell-tally">${ctx.tally(run.batch_status)}</td>
      <td class="cell-failures">${failureHtml(run, ctx)}</td>
      <td>${actionHtml(links, ctx.esc, false)}</td>
    </tr>`;
  });
  const alerts = [];
  const slot = home?.occupancy?.slot;
  if (slot && (slot.state === "running" || slot.state === "starting")) {
    alerts.push(`<li class="attention-row tone-info"><div class="attention-copy"><strong>${tr("正在执行", "Running")}</strong><p class="attention-detail">${ctx.esc(pick(slot, "title") || slot.op || "")}</p></div><a class="button button-primary" href="#/ops/jobs/${encodeURIComponent(slot.job_id)}">${tr("查看任务", "Open job")}</a></li>`);
  }
  const pending = home?.occupancy?.incomplete_init;
  if (pending && !pending.running) {
    alerts.push(`<li class="attention-row"><div class="attention-copy"><strong>${tr("初始化没跑完", "Initialization did not finish")}</strong><p class="attention-detail">${tr("已成功的批次会保留。", "Batches that succeeded are kept.")}</p></div><a class="button button-primary" href="${ctx.esc(hrefFor("init.resume", { run_id: pending.run_id }))}">${tr("继续初始化", "Resume initialization")}</a></li>`);
  }
  const attention = alerts.length
    ? `<section class="surface-panel attention-band" aria-label="${tr("需要先处理的事项", "Needs attention")}"><ul class="attention-list">${alerts.join("")}</ul></section>`
    : "";
  const notes = [];
  if (launcher.status) notes.push(`<p class="ops-hint">${ctx.esc(launcher.status)}</p>`);
  if (launcher.closedNote) notes.push(`<p class="ops-hint">${ctx.esc(launcher.closedNote)}</p>`);
  if (home?.occupancy?.lake_empty) {
    notes.push(`<p class="ops-hint">${tr("湖里还没有 curated 数据。日更和补抓不会补历史。", "The lake has no curated data yet. A daily update or catch-up does not fill history.")} <a href="#/ops?op=init.start">${tr("去初始化", "Initialize")}</a></p>`);
  }
  if (home?.mode?.setup) {
    notes.push(`<p class="ops-hint">${tr("首次配置还没完成。", "First-time setup is not finished.")} <a href="#/ops">${tr("去操作页", "Operations")}</a></p>`);
  } else if (home && !home.mode?.ops_enabled) {
    notes.push(`<p class="ops-hint">${ctx.esc(tr("当前模式不能从面板启动命令。", "This mode cannot start commands from the panel."))}</p>`);
  }
  const retryGroups = attentionMeta?.failed_daily_groups
    ? `<a class="button button-ghost${state.op === "run.retry_failed_groups" ? " on" : ""}" href="${ctx.esc(runsHref({ op: "run.retry_failed_groups", preset: new URLSearchParams() }, state))}">${tr("重试失败的日更组", "Retry failed daily groups")}</a>`
    : "";
  const successHref = ctx.esc(runsHref({ view: state.view === "success" ? "all" : "success", offset: 0 }, state));
  const empty = state.offset
    ? tr("这一页没有运行记录。", "This page has no runs.")
    : state.view === "attention"
      ? tr("没有需要处理的运行。", "Nothing needs attention.")
      : state.view === "running"
        ? tr("没有正在运行的任务。", "Nothing is running.")
        : state.view === "success"
          ? tr("没有成功的运行。", "No successful runs.")
          : tr("尚无运行记录。", "No runs yet.");
  const range = !total ? tr("共 0 条", "0 runs") : tr(
    `第 ${ctx.fmt(start)}–${ctx.fmt(end)} 条，${state.view === "all" ? "共" : "筛选后"} ${ctx.fmt(total)} 条`,
    `${ctx.fmt(start)}–${ctx.fmt(end)} / ${ctx.fmt(total)}`,
  );
  ctx.setPage(`
    <section class="page-heading">
      <div class="heading-row"><div><h1>${tr("运行", "Runs")}</h1>
        <p class="meta-row"><span>${range}</span>${legacy ? `<span>${tr("当前服务是旧接口，重启后才能筛选和翻页。", "This serve process has the old API. Restart it to filter and page.")}</span>` : ""}</p></div></div></section>
    ${attention}
    <section class="surface-panel report-panel run-launch">
      <div class="panel-header"><h2>${tr("启动", "Start")}</h2></div>
      ${notes.join("")}
      <div class="run-launch-grid">${launcher.items.map((item) => launcherButton(item, state, ctx.esc)).join("")}${retryGroups}</div>
      <div id="run-form" class="run-form-host"></div>
    </section>
    <section class="metric-grid run-metrics" aria-label="${tr("运行指标", "Run metrics")}">
      ${metricLink(countText(allTotal, ctx.num), tr("全部", "All"), "", ctx.esc(runsHref({ view: "all", offset: 0 }, state)), state.view === "all" ? " on" : "")}
      ${metricLink(countText(runningTotal, ctx.num), tr("运行中", "Running"), "", ctx.esc(runsHref({ view: "running", offset: 0 }, state)), `${state.view === "running" ? " on" : ""}${runningTotal ? " metric-card-live" : ""}`)}
      ${metricLink(countText(attentionTotal, ctx.num), tr("待处理", "Open"), "", ctx.esc(runsHref({ view: "attention", offset: 0 }, state)), `${state.view === "attention" ? " on" : ""}${attentionTotal ? " metric-card-alert" : ""}`)}
    </section>
    <section class="surface-panel table-panel"><div class="panel-header"><h2>${tr("记录", "History")}</h2>
      <div class="run-views"><a class="${state.view === "success" ? "on" : ""}" href="${successHref}">${tr("只看成功", "Success only")}</a></div></div>
    ${ctx.dataTable(
      [tr("任务", "Job"), tr("状态", "Status"), tr("开始", "Started"), { h: tr("耗时", "Duration"), n: true }, { h: tr("写入", "Written"), n: true }, tr("批次", "Batches"), tr("原因", "Reason"), tr("操作", "Action")],
      rows,
      empty,
      "data-table",
    )}
    <div class="action-row run-pager">
      ${state.offset > 0 ? `<a class="button button-ghost" href="${ctx.esc(runsHref({ offset: Math.max(0, state.offset - RUN_PAGE) }, state))}">${tr("上一页", "Previous")}</a>` : `<button class="button button-ghost" type="button" disabled>${tr("上一页", "Previous")}</button>`}
      <span class="muted">${ctx.fmt(start)}–${ctx.fmt(end)} / ${ctx.fmt(total)}</span>
      ${end < total ? `<a class="button button-ghost" href="${ctx.esc(runsHref({ offset: end }, state))}">${tr("下一页", "Next")}</a>` : `<button class="button button-ghost" type="button" disabled>${tr("下一页", "Next")}</button>`}
    </div></section>`, "runs");
  bindLauncher(state);
  openRunForm(ctx, home, current, state, runsHref({ op: "", preset: new URLSearchParams() }, state));
}

function startRunTicker(ctx) {
  if (runTicker) clearInterval(runTicker);
  let ticks = 0;
  runTicker = setInterval(() => {
    if (!lastRunDetail || lastRunDetail.status !== "running") return;
    const el = document.getElementById("run-status");
    if (!el) return closeRunStream();
    paintRunStatus(lastRunDetail, ctx);
    if (++ticks % 5 === 0) runGantt(document.getElementById("run-gantt"), lastRunDetail);
  }, 1000);
}

function paintRunStatus(detail, ctx) {
  const live = detail.status === "running";
  document.getElementById("run-status").innerHTML = [
    `<span>${ctx.esc(statusText(detail.status))}${live ? ` <span class="live">${tr("实时", "Live")}</span>` : ""}</span>`,
    `<span>${duration(detail.started_at, detail.finished_at)}</span>`,
    `<span>${ctx.fmt(detail.rows_written)} ${tr("行", "rows")}</span>`,
  ].join("");
}

function paintRun(detail, ctx) {
  lastRunDetail = detail;
  paintRunStatus(detail, ctx);
  const stalled = detail.batches.filter((batch) => batch.stalled);
  document.getElementById("run-note").innerHTML = stalled.length
    ? `<div class="banner">${tr(
        `${stalled.length} 个 batch 仍是 running 但已静默超过 ${Math.round(detail.stale_after_seconds / 60)} 分钟。下次 run 会把它们判为 failed：`,
        `${stalled.length} batches are still running but silent for more than ${Math.round(detail.stale_after_seconds / 60)} minutes. The next run will mark them failed:`,
      )}
       ${stalled.map((batch) => ctx.dsLink(batch.dataset)).join(" ")}</div>`
    : "";
  runGantt(document.getElementById("run-gantt"), detail);
  const failed = openBatchFailures(detail.batches);
  const summaryText = detail.error_message && detail.error_message !== GENERIC_RUN_ERROR ? detail.error_message : "";
  const summary = summaryText ? `<p class="failure-summary">${ctx.esc(summaryText)}</p>` : "";
  const failureTable = failed.length
    ? ctx.dataTable(
        [tr("数据集", "Dataset"), tr("状态", "Status"), { h: tr("重试", "Retries"), n: true }, tr("原因", "Reason")],
        failed.map(
          (batch) => `<tr><td>${ctx.dsLink(batch.dataset)}</td><td>${ctx.statusPill(batch.status)}</td>
            <td class="n">${ctx.num(batch.retry_count)}</td>
            <td class="cell-failures">${ctx.esc(ctx.failureLine(batch.error_message))}</td></tr>`,
        ),
        "",
      )
    : "";
  document.getElementById("run-errors").innerHTML = failed.length || summary
    ? `<section class="surface-panel report-panel"><div class="panel-header"><h2>${tr("失败原因", "Failures")}</h2><span class="panel-meta">${failed.length || 1}</span></div>
       ${summary}
       ${failureTable}</section>`
    : "";
}

export async function renderRunDetail(runId, ctx) {
  const current = beginOps();
  const state = parseRunsHash(location.hash);
  const detail = await ctx.api(`/api/runs/${encodeURIComponent(runId)}`);
  if (!opsIsCurrent(current)) return;
  let home = null;
  if (state.op) home = await ctx.api("/api/ops").catch(() => null);
  if (!opsIsCurrent(current)) return;
  let panelJob = null;
  if (detail.launch_id) {
    panelJob = await ctx.api(`/api/ops/jobs/${encodeURIComponent(detail.launch_id)}`).catch(() => null);
    if (!opsIsCurrent(current)) return;
  }
  const jobLive = Boolean(panelJob && (panelJob.state === "running" || panelJob.state === "starting"));
  let csrf = home?.csrf_token || "";
  if (jobLive && !csrf) {
    const opsHome = await ctx.api("/api/ops").catch(() => null);
    if (!opsIsCurrent(current)) return;
    csrf = opsHome?.csrf_token || "";
  }
  const hrefFor = (op, params) => detailHref(runId, { op, ...params });
  const links = runActions(detail, hrefFor, true);
  const actions = [`<a class="button button-ghost" href="#/runs">${tr("返回运行", "Runs")}</a>`];
  if (links.length) actions.push(actionHtml(links, ctx.esc, true));
  if (detail.launch_id) {
    actions.push(`<a class="button button-ghost" href="#/ops/jobs/${encodeURIComponent(detail.launch_id)}">${tr("面板任务", "Panel job")}</a>`);
  }
  if (jobLive) actions.push(`<button class="button button-danger" id="run-cancel" type="button">${tr("取消", "Cancel")}</button>`);
  const title = jobTitle(detail.job_name);
  const raw = String(detail.job_name || "");
  ctx.setPage(`
    <section class="page-heading">
      <div class="heading-row"><div><h1>${ctx.esc(title)}</h1><p class="meta-row" id="run-status"></p><p class="meta-row"><code>${ctx.esc(detail.run_id)}</code>${title === raw ? "" : `<code>${ctx.esc(raw)}</code>`}</p></div>
      <div class="action-row">${actions.join("")}</div></div></section>
    ${state.op ? `<div id="run-form" class="run-form-host"></div>` : ""}
    <section class="metric-grid detail-metrics" aria-label="${tr("运行指标", "Run metrics")}">
      ${ctx.kpi(duration(detail.started_at, detail.finished_at), tr("耗时", "Duration"))}
      ${ctx.kpi(`<span title="${ctx.fmt(detail.rows_written)} ${tr("行", "rows")}">${ctx.compact(detail.rows_written)}</span>`, tr("写入", "Written"), tr("行", "rows"))}
      ${ctx.kpi(ctx.num(detail.batches.length), tr("批次", "Batches"))}
      ${ctx.kpi(ago(detail.started_at), tr("开始", "Started"), ctx.esc((detail.started_at || "").slice(0, 19)))}
    </section>
    <div id="run-note"></div>
    <div id="run-errors" class="report-stack"></div>
    ${jobLive ? `<details class="surface-panel report-panel" id="run-job-logs"><summary>${tr("任务日志", "Job logs")}</summary><pre class="ops-log" id="run-job-log"></pre><pre class="ops-log" id="run-job-out"></pre></details>` : ""}
    <section class="surface-panel report-panel"><div class="panel-header"><h2>${tr("时间线", "Timeline")}</h2></div><div id="run-gantt"></div>
    <p class="legend">
      <span><i class="swatch" style="background:var(--cell-covered)"></i> ${tr("成功", "success")}</span>
      <span><i class="swatch" style="background:var(--series-1)"></i> ${tr("运行中", "running")}</span>
      <span><i class="swatch" style="background:var(--cell-gap)"></i> ${tr("失败", "failed")}</span>
      <span><i class="swatch" style="background:var(--series-4)"></i> ${tr("落后", "stale")}</span>
      <span>${tr("橙色描边 = 有重试；斜纹 = 已静默", "Orange outline = retried; hatch = silent")}</span>
    </p></section>`, "runs");
  paintRun(detail, ctx);
  if (jobLive) {
    const cancel = document.getElementById("run-cancel");
    if (cancel) {
      cancel.onclick = async () => {
        cancel.disabled = true;
        try {
          await ctx.api(`/api/ops/jobs/${encodeURIComponent(detail.launch_id)}/cancel`, {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-CNE-CSRF": csrf },
            body: "{}",
          });
        } catch (err) {
          const note = document.getElementById("run-note");
          if (note) note.insertAdjacentHTML("beforeend", `<p class="err">${ctx.esc(err.message)}</p>`);
        }
      };
    }
    openJobStreams(detail.launch_id, current, {
      log: document.getElementById("run-job-log"),
      out: document.getElementById("run-job-out"),
    });
  }
  if (state.op) openRunForm(ctx, home, current, state, detailHref(runId));
  if (detail.status !== "running") return;
  startRunTicker(ctx);
  const url = ctx.token
    ? `/api/stream/runs/${encodeURIComponent(runId)}?token=${encodeURIComponent(ctx.token)}`
    : `/api/stream/runs/${encodeURIComponent(runId)}`;
  runStream = new EventSource(url);
  runStream.onmessage = (event) => {
    const here = decodeURIComponent(location.hash.split("?")[0]);
    if (here !== `#/runs/${runId}`) return closeRunStream();
    paintRun(JSON.parse(event.data), ctx);
  };
  runStream.onerror = () => closeRunStream();
}
