// Whitelisted commands. A preview is not a start; the launch token is single use.
import { renderSchedule } from "./schedule.js";
import { renderBackups } from "./backups.js";
import { renderSettings } from "./settings.js";
import { choiceLabel, ds, locale, modeLabel, pick, tr } from "./i18n.js";
let generation = 0;
let streams = [];
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

// Readiness rows carry the pack's Chinese title; the id names it in English.
const PACK_TITLES_EN = { market: "Market data", fundamentals: "Fundamentals", universe: "Universe" };
const packTitle = (row) => tr(row.title, PACK_TITLES_EN[row.id] || row.title);

export function closeOps() {
  generation += 1;
  for (const source of streams) source.close();
  streams = [];
}

export function beginOps() {
  return ++generation;
}

export function opsIsCurrent(ticket) {
  return ticket === generation;
}

function dailyScheduleText(schedule) {
  if (schedule.daily_pending) {
    return tr(
      `定时日更 ${schedule.daily_pending} ${schedule.daily_run_at} 已到点，这次会话还没跑。`,
      `Scheduled daily update ${schedule.daily_pending} ${schedule.daily_run_at} is due and has not run in this session.`,
    );
  }
  if (schedule.daily_done) {
    const session = schedule.daily_due ? `${schedule.daily_due}` : "";
    return session
      ? tr(
          `定时日更 ${session}（${schedule.daily_run_at}）这次会话已经跑过。`,
          `Scheduled daily update ${session} (${schedule.daily_run_at}) already ran in this session.`,
        )
      : tr(
          `定时日更（${schedule.daily_run_at}）这次会话已经跑过。`,
          `The scheduled daily update (${schedule.daily_run_at}) already ran in this session.`,
        );
  }
  if (schedule.daily_run_at) {
    return tr(`定时日更时间 ${schedule.daily_run_at}（北京时间）。`, `Scheduled daily update at ${schedule.daily_run_at} (Beijing time).`);
  }
  return "";
}

const DAILY_OPS = new Set(["daily.full", "daily.group", "daily.stale"]);
const dailyRangeNote = () => tr(
  `要补一段日线，用<a href="#/ops?op=backfill.run&dataset=daily_bars">回填 ${ds("daily_bars")}</a>，填写起点和终点。日更只有一个交易日。日线按标的请求，即使只补一天也会扫该范围的市场。`,
  `To fill a range of daily bars, <a href="#/ops?op=backfill.run&dataset=daily_bars">backfill ${ds("daily_bars")}</a> and set the start and end. A daily update is one session. Daily bars are requested per symbol, so one day still scans that market range.`,
);

function dailySessionText(schedule, opId, params) {
  if (!DAILY_OPS.has(opId) || !schedule?.daily_run_at) return "";
  const today = schedule.today || "";
  const pending = schedule.daily_pending || "";
  const runAt = schedule.daily_run_at;
  const parts = [];
  if (today) {
    parts.push(schedule.today_is_session
      ? tr(`今天 ${today} 是交易日。`, `Today ${today} is a trading session.`)
      : tr(`今天 ${today} 不是交易日。`, `Today ${today} is not a trading session.`));
  }
  if (pending) parts.push(tr(`待跑的定时日更会话是 ${pending} ${runAt}。`, `The due scheduled session is ${pending} ${runAt}.`));
  else if (schedule.daily_done && schedule.daily_due) {
    parts.push(tr(`定时日更 ${schedule.daily_due}（${runAt}）这次会话已经跑过。`, `Scheduled daily update ${schedule.daily_due} (${runAt}) already ran in this session.`));
  } else parts.push(tr(`定时日更时间 ${runAt}（北京时间）。`, `Scheduled daily update at ${runAt} (Beijing time).`));
  if (opId === "daily.full") {
    const date = String(params.trade_date || "").trim();
    const counts = !params.backfill && pending && ((date === "" && today === pending) || date === pending);
    parts.push(counts
      ? tr(`这次手跑会计入定时日更 ${pending}。`, `This manual run counts as the scheduled update ${pending}.`)
      : tr("这次手跑不会记成那次定时日更。", "This manual run is not recorded as that scheduled update."));
    if (!date && schedule.today_is_session === false) {
      parts.push(tr(
        "不填日期会按今天提交。今天不是交易日，调度组会跳过，只有事件流会跑。要跑某个交易日，请填写日期。",
        "An empty date submits today. Today is not a trading session, so schedule groups are skipped and only the event stream runs. Enter a session date to run one.",
      ));
    } else if (!date && schedule.today_is_session && schedule.before_daily_run_at) {
      parts.push(tr(`还没到今天的日更时间 ${runAt}，当天数据可能还没发布。`, `Today's update time ${runAt} has not arrived, so today's data may not be published yet.`));
    }
  } else parts.push(tr("这次不会写“定时日更已跑过”的标记。", "This run will not mark the scheduled daily update as done."));
  return parts.join(" ");
}

// Opt-in resamples (`cne derive minute_bars_15m` …): the page is one of their
// entry points, so the form spells out what running one does.
const RESAMPLED_MINUTE = new Set(["minute_bars_15m", "minute_bars_30m", "minute_bars_60m"]);

function deriveTargetText(opId, params) {
  if (opId !== "derive.run" || !RESAMPLED_MINUTE.has(params.name)) return "";
  const name = ds(params.name);
  return tr(
    `${name}默认不计算，在这里运行后才写进数据湖，之后可用 SQL、HTTP 接口和 MCP 读取。某只股票某天有 1m 就用 1m，否则用 5m，resampled_from 列标明来源；上午从 09:30、下午从 13:00 对齐，开高低收只计入有成交的 K 线。停牌（整天无成交）和盘中停复牌的股票当天跳过并单独计数；有成交而 K 线中间断档的也跳过，但这次运行会记为降级。起点终点都不填：只算还没算过、或分钟线已更新的交易日；勾选全量重写：重算全部。分钟线更新后不会自动重算，需要再运行一次。完整规则见文档「15 / 30 / 60 分钟线」。`,
    `${name} are not computed by default; running this writes them into the lake, where SQL, the HTTP API and MCP can read them. Each stock-day is built from 1m when the lake has it and from 5m otherwise, and the resampled_from column records which. Bars align to 09:30 and 13:00 and OHLC counts only traded bars. Halted stock-days and intraday suspensions are skipped and counted on their own; a traded day with a hole between bars is skipped too and marks the run degraded. With no start or end, only sessions not yet computed or whose minute bars changed are rebuilt; Full rewrite rebuilds all. Nothing reruns this when minute bars update, so run it again. See the "15 / 30 / 60-minute bars" docs page for the full rules.`,
  );
}

function formNoteText(schedule, opId, params) {
  return dailySessionText(schedule, opId, params) || deriveTargetText(opId, params);
}

function jobStamp(job) {
  const raw = job.finished_at || job.created_at;
  if (!raw) return "";
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return "";
  return new Intl.DateTimeFormat(locale(), {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  }).format(parsed);
}

function jobNextStep(job) {
  const state = job.state;
  if (state === "running" || state === "starting") return "";
  const op = job.op || "";
  const runId = (job.runs || []).find((run) => run.run_id)?.run_id || "";
  const link = (opId, label) => {
    const query = new URLSearchParams({ op: opId });
    if (runId) query.set("run_id", runId);
    return `<a href="#/ops?${query.toString()}">${label}</a>`;
  };
  const overview = new Set(["init.start", "init.resume", "daily.full", "daily.group", "daily.stale", "events.run"]);
  if (state === "complete" || state === "succeeded" || state === "skipped") {
    return overview.has(op) ? `<a href="#/">${tr("查看总览", "Open overview")}</a>` : "";
  }
  if (op === "init.start" || op === "init.resume") return link("init.resume", tr("继续初始化", "Resume initialization"));
  if (op.startsWith("daily.") || op === "events.run") {
    return runId ? link("run.retry", tr("重试这次 run", "Retry this run")) : `<a href="#/ops?op=run.retry_failed_groups">${tr("重试失败的日更组", "Retry failed daily groups")}</a>`;
  }
  return "";
}

function hashQuery() {
  const raw = location.hash.replace(/^#/, "");
  const query = raw.split("?")[1] || "";
  return new URLSearchParams(query);
}

function fieldValue(spec, source) {
  const preset = source.get(spec.name);
  if (spec.kind === "bool") return preset === "1" || preset === "true";
  if (spec.kind === "multi") return preset ? preset.split(",").filter(Boolean) : [];
  return preset || "";
}

function readForm(form, specs) {
  const params = {};
  for (const spec of specs) {
    const input = form.querySelector(`[name="${CSS.escape(spec.name)}"]`);
    if (!input) continue;
    if (spec.kind === "bool") params[spec.name] = input.checked;
    else if (spec.kind === "multi") {
      params[spec.name] = [...input.selectedOptions].map((option) => option.value);
    } else if (input.value) params[spec.name] = input.value.trim();
  }
  return params;
}

function control(spec, source) {
  const value = fieldValue(spec, source);
  const help = spec.help ? `<small class="muted">${pick(spec, "help")}</small>` : "";
  if (spec.kind === "bool") {
    return `<label class="ops-check"><input type="checkbox" name="${spec.name}" ${value ? "checked" : ""}> ${pick(spec, "label")}</label>${help}`;
  }
  if (spec.kind === "choice") {
    const options = (spec.choices || [])
      .map((choice) => `<option value="${esc(choice)}" ${choice === value ? "selected" : ""}>${esc(choiceLabel(spec.name, choice))}</option>`)
      .join("");
    return `<label>${pick(spec, "label")}<select name="${spec.name}"><option value="">${spec.required ? tr("请选择", "Select") : tr("不限", "Any")}</option>${options}</select></label>${help}`;
  }
  if (spec.kind === "multi") {
    const options = (spec.choices || [])
      .map((choice) => `<option value="${esc(choice)}" ${value.includes(choice) ? "selected" : ""}>${esc(choiceLabel(spec.name, choice))}</option>`)
      .join("");
    return `<label>${pick(spec, "label")}<select name="${spec.name}" multiple size="${Math.min(6, (spec.choices || []).length || 1)}">${options}</select></label>${help}`;
  }
  const type = spec.kind === "date" ? "date" : "text";
  return `<label>${esc(pick(spec, "label"))}<input name="${esc(spec.name)}" type="${type}" value="${esc(value)}" ${spec.required ? "required" : ""}></label>${help}`;
}

function scopes() {
  return [
    { id: "packs", label: tr("研究包（一个交易日）", "Research pack (one session)"), op: "daily.full", note: tr("不选研究包就跑全部日更组和事件流。日更只有一个交易日。", "With no pack selected, every daily group and the event stream run. A daily update is one session.") },
    { id: "group", label: tr("一个调度组（一个交易日）", "One schedule group (one session)"), op: "daily.group", note: tr("只跑这一组，不会把那天记成定时日更已完成。", "Runs only this group, and does not mark that session's scheduled update done.") },
    { id: "events", label: tr("事件流（一个自然日）", "Event stream (one calendar day)"), op: "events.run", note: tr("周末和节假日也可以。公告和资讯不在研究包日更里。", "Weekends and holidays are included. Announcements and news are not part of a research-pack daily update.") },
    { id: "dataset", label: tr("一个数据集（起止日期）", "One dataset (start and end)"), op: "backfill.run", note: tr("一段历史走回填。一次只能回填一个数据集。", "A history range is a backfill. One dataset at a time.") },
    { id: "derive", label: tr("一个派生（起止日期）", "One derived dataset (start and end)"), op: "derive.run", note: tr("从已发布的数据重算。全量会重写分区。15 / 30 / 60 分钟线默认不计算，也在这里入湖。", "Recompute from published data. A full range rewrites partitions. 15/30/60-minute bars are not computed by default; store them here too.") },
    { id: "stale", label: tr("只补仍落后的数据", "Only what is still stale"), op: "daily.stale", note: tr("没有日期。只重抓仍然落后的数据集。快照漏掉的当天只在当天再试。", "No date. Refetch only datasets that are still stale. A missed snapshot day can only be retried that day.") },
  ];
}

function manualLimits() {
  return [
    tr(`日更只有一个交易日。要补一段日线，把范围改成一个数据集并选择 ${ds("daily_bars")}。`, `A daily update is one session. To fill a range of daily bars, choose one dataset and select ${ds("daily_bars")}.`),
    tr("交易状态这类快照漏掉当天，改一个旧日期补不回来；收尾补抓只在当天再试。", "A missed snapshot day, such as trading status, cannot be recovered by picking an old date. The catch-up only retries that day."),
    tr("手跑一个调度组或一次回填，不会把那天记成定时日更已完成。", "Running one group or a backfill by hand does not mark that day's scheduled update done."),
  ];
}

function commandGroups() {
  return [
    [tr("初始化", "Initialize"), ["diag.doctor", "init.start", "init.resume"]],
    [tr("日更", "Daily"), ["daily.full", "daily.group", "daily.stale", "events.run"]],
    [tr("补数", "Backfill"), ["backfill.run", "derive.run"]],
    [tr("跑批", "Runs"), ["run.retry", "run.retry_failed_groups", "run.compact"]],
    [tr("巡检", "Checks"), ["check.status", "check.verify", "check.audit", "check.sources_probe", "maint.stats_rebuild", "config.upgrade_preview"]],
  ];
}

function cliOnly() {
  return [
    ["cne repair", tr("修复已验证、且有诚实来源的历史缺口。页面不启动这条命令。", "Repairs a verified historical gap that has an honest source. The page does not start this command.")],
    ["cne query", tr("查询已发布数据。页面用数据集页浏览样例。", "Query published data. The page browses a sample on the dataset page.")],
    ["cne config create / validate / diff / upgrade", tr("配置写入留在终端。页面只预览 cne config upgrade --dry-run。", "Config writes stay in the terminal. The page only previews cne config upgrade --dry-run.")],
    ["python scripts/delisted_ops.py", tr("退市名录是仓库里的一次性工程脚本，不能从页面启动。", "The delisting list is a one-off repo script and cannot be started from the page.")],
  ];
}

function workflowOf(opId) {
  if (opId === "diag.doctor" || opId.startsWith("init.")) return "init";
  if (opId.startsWith("daily.") || opId === "events.run") return "daily";
  if (opId === "backfill.run" || opId === "derive.run") return "manual";
  if (opId.startsWith("snapshot.")) return "backup";
  return "commands";
}

function scopeById(id) {
  return scopes().find((item) => item.id === id) || scopes()[0];
}

function dailyHeader(schedule) {
  const parts = [];
  if (schedule.today) {
    parts.push(schedule.today_is_session
      ? tr(`今天 ${schedule.today} 是交易日。`, `Today ${schedule.today} is a trading session.`)
      : tr(`今天 ${schedule.today} 不是交易日。`, `Today ${schedule.today} is not a trading session.`));
  }
  const text = dailyScheduleText(schedule);
  if (text) parts.push(text);
  return parts.join(" ");
}

/** Commands that write a manifest run. The runs page opens the same form. */
export function runLauncherModel(home) {
  const schedule = home?.occupancy?.schedule || {};
  const enabled = Boolean(home?.mode?.ops_enabled);
  const blocked = !home
    ? ""
    : enabled
      ? ""
      : tr("当前模式不能从面板启动命令。", "This mode cannot start commands from the panel.");
  const item = (id, label, preset = {}) => {
    const card = home?.operations?.find((entry) => entry.id === id);
    const reason = !home
      ? tr("读不到操作状态。", "Operations status is unavailable.")
      : blocked || (card?.available ? "" : pick(card, "unavailable_reason") || tr("当前没有这项操作。", "This operation is not available."));
    return { id, label, preset, summary: pick(card, "summary") || "", disabled: !home || !enabled || !card?.available, reason };
  };
  const items = [];
  if (!home || schedule.today_is_session !== false) {
    items.push(item("daily.full", tr("跑今天", "Run today")));
  } else if (schedule.daily_pending) {
    items.push(item(
      "daily.full",
      tr(`跑 ${schedule.daily_pending}`, `Run ${schedule.daily_pending}`),
      { trade_date: schedule.daily_pending },
    ));
  }
  items.push(
    item("daily.group", tr("一个调度组", "One group")),
    item("daily.stale", tr("补落后", "Catch up stale")),
    item("events.run", tr("事件流", "Events")),
    item("backfill.run", tr("回填", "Backfill")),
    item("derive.run", tr("重算派生", "Recompute")),
  );
  const closedNote = schedule.today_is_session === false && !schedule.daily_pending
    ? tr(
        "今天不是交易日，所以没有“跑今天”。要重跑某一个交易日，打开「一个调度组」并填写日期。",
        "Today is not a trading session, so there is no run-today action. To rerun one session, open One group and enter the date.",
      )
    : "";
  return { items, closedNote, status: home ? dailyHeader(schedule) : "" };
}

function opButton(home, id, label, className) {
  const card = home.operations.find((item) => item.id === id);
  if (!card) return "";
  const disabled = !card.available || !home.mode.ops_enabled;
  return `<button type="button" class="${className}" data-op="${esc(id)}" ${disabled ? "disabled" : ""}>${esc(label)}</button>`;
}

function initBlock(home, escapeHtml) {
  const empty = Boolean(home.occupancy.lake_empty);
  const pending = home.occupancy.incomplete_init;
  const doctor = opButton(home, "diag.doctor", tr("环境体检", "Environment check"), "button button-ghost");
  if (!empty && !pending) {
    return `<div class="ops-quiet" id="ops-init" data-workflow="init"><span>${tr("初始化已完成", "Initialization is complete")}</span>${doctor}</div>`;
  }
  const actions = [];
  if (pending && !pending.running) {
    const resume = `#/ops?op=init.resume&run_id=${encodeURIComponent(pending.run_id)}`;
    actions.push(`<a class="button button-primary" href="${escapeHtml(resume)}">${tr("继续初始化", "Resume initialization")}</a>`);
  }
  if (!pending?.running) {
    actions.push(opButton(home, "init.start", tr("初始化数据湖", "Initialize the lake"), pending ? "button button-ghost" : "button button-primary"));
  }
  if (doctor) actions.push(doctor);
  const note = pending?.running
    ? tr("初始化正在进行。", "Initialization is running.")
    : pending
      ? tr("有一次初始化没跑完。已成功的批次会保留。", "An initialization did not finish. Batches that succeeded are kept.")
      : tr("湖里还没有 curated 数据。初始化下载全市场行情主干。", "The lake has no curated data yet. Initialization downloads the full-market price spine.");
  const buttons = actions.filter(Boolean).join("");
  return `<section class="surface-panel report-panel ops-workflow ops-card-primary" id="ops-init" data-workflow="init">
    <div class="panel-header"><h2>${tr("初始化", "Initialize")}</h2></div>
    <p class="ops-lead">${escapeHtml(note)}</p>
    ${buttons ? `<div class="ops-actions">${buttons}</div>` : ""}
  </section>`;
}

function dailyPrimaryAction(home, schedule, escapeHtml) {
  if (schedule.today_is_session !== false) {
    return opButton(home, "daily.full", tr("现在跑今天", "Run today now"), "button button-primary");
  }
  if (!schedule.daily_pending) return "";
  const date = schedule.daily_pending;
  const label = tr(`现在跑 ${date}`, `Run ${date} now`);
  const card = home.operations.find((item) => item.id === "daily.full");
  if (!card || !card.available || !home.mode.ops_enabled) {
    return `<button type="button" class="button button-primary" disabled>${escapeHtml(label)}</button>`;
  }
  return `<a class="button button-primary" href="#/ops?op=daily.full&trade_date=${encodeURIComponent(date)}">${escapeHtml(label)}</a>`;
}

function dailyBlock(home, escapeHtml) {
  const schedule = home.occupancy.schedule || {};
  const header = dailyHeader(schedule);
  const actions = [
    dailyPrimaryAction(home, schedule, escapeHtml),
    opButton(home, "daily.stale", tr("收尾补抓", "Catch up stale data"), "button button-ghost"),
    opButton(home, "events.run", tr("跑事件流", "Run the event stream"), "button button-ghost"),
  ].filter(Boolean).join("");
  const closedToday = schedule.today_is_session === false && !schedule.daily_pending
    ? `<p class="ops-hint">${tr("今天不是交易日，所以没有“现在跑今天”。要重跑某一个交易日，用手动更新并填写日期。", "Today is not a trading session, so there is no run-today action. To rerun one session, use a manual update and enter the date.")}</p>`
    : "";
  const when = schedule.daily_run_at ? ` · ${escapeHtml(schedule.daily_run_at)}` : "";
  const pending = home.occupancy.incomplete_init;
  const initLeads = Boolean(home.occupancy.lake_empty) || Boolean(pending);
  return `<section class="surface-panel report-panel ops-workflow${initLeads ? "" : " ops-card-primary"}" id="ops-daily" data-workflow="daily">
    <div class="panel-header"><h2>${tr("日更", "Daily")}</h2></div>
    <p class="ops-lead">${header ? escapeHtml(header) : tr("还没有定时日更时间。", "No scheduled daily time yet.")}</p>
    ${actions ? `<div class="ops-actions">${actions}</div>` : ""}
    ${closedToday}
    <p class="ops-hint">${tr("要补一段日线，用", "To fill a range of daily bars, use")} <a href="#/ops?op=backfill.run&dataset=daily_bars">${tr("回填", "backfill")} ${ds("daily_bars")}</a>.</p>
    <details class="ops-fold ops-nested"><summary>${tr("定时设置", "Schedule")}${when}</summary><section class="surface-panel report-panel" id="ops-schedule">${tr("正在读取定时任务状态…", "Reading schedule status…")}</section></details>
  </section>`;
}

function manualFields(home, scope, preset) {
  const card = home.operations.find((item) => item.id === scope.op);
  if (!card) return `<p class="panel-note">${tr("当前没有这项操作。", "This operation is not available.")}</p>`;
  const disabled = !card.available || !home.mode.ops_enabled;
  return `${card.params.map((spec) => control(spec, preset)).join("")}
    <div class="action-row"><button class="button button-primary" type="submit" ${disabled ? "disabled" : ""}>${tr("预览", "Preview")}</button></div>`;
}

function manualBlock(home, escapeHtml, preset) {
  const scope = scopeById("packs");
  const options = scopes().map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.label)}</option>`).join("");
  const session = formNoteText(home.occupancy.schedule || {}, scope.op, {});
  return `<section class="ops-workflow" id="ops-manual" data-workflow="manual">
    <p class="ops-hint">${tr("先选要更新的数据，再选时间。一次只启动一条命令。", "Choose the data, then the time. One command at a time.")}</p>
    <ul class="ops-limits">${manualLimits().map((line) => `<li>${escapeHtml(line)}</li>`).join("")}</ul>
    <form id="ops-manual-form" class="ops-fields">
      <label>${tr("更新范围", "What to update")}<select id="ops-scope">${options}</select></label>
      <p class="ops-hint" id="ops-manual-note">${escapeHtml(scope.note)}</p>
      <p class="ops-hint" id="ops-manual-session" ${session ? "" : "hidden"}>${escapeHtml(session)}</p>
      <div id="ops-manual-fields">${manualFields(home, scope, preset)}</div>
    </form>
    <div id="ops-manual-preview"></div>
  </section>`;
}

function commandBlock(home, escapeHtml) {
  const byId = new Map(home.operations.map((card) => [card.id, card]));
  const groups = commandGroups().map(([title, ids]) => {
    const rows = ids
      .map((id) => {
        const card = byId.get(id);
        if (!card) return "";
        const disabled = !card.available || !home.mode.ops_enabled;
        const copy = card.command
          ? `<button type="button" class="button button-ghost" data-copy-command="${escapeHtml(card.command)}">${tr("复制", "Copy")}</button>`
          : "";
        return `<div class="ops-command"><div><strong>${escapeHtml(pick(card, "title"))}</strong><div class="muted">${escapeHtml(pick(card, "summary"))}</div><code>${escapeHtml(card.command || "")}</code></div>
          <div class="ops-actions">${copy}<button type="button" class="button button-ghost" data-op="${escapeHtml(card.id)}" ${disabled ? "disabled" : ""}>${tr("用这个", "Use this")}</button></div></div>`;
      })
      .join("");
    if (!rows) return "";
    return `<section class="ops-command-group"><h3>${escapeHtml(title)}</h3>${rows}</section>`;
  }).join("");
  const cli = cliOnly().map(
    ([command, why]) => `<div class="ops-command ops-cli-only"><div><code>${escapeHtml(command)}</code><div class="muted">${escapeHtml(why)}</div></div></div>`,
  ).join("");
  return `<section class="ops-workflow" id="ops-commands" data-workflow="commands">
    <p class="ops-hint">${tr("和页面启动的是同一套白名单。点「用这个」打开表单，先预览再启动。", "This is the same allowlist the page can start. Use this opens the form; preview before starting.")}</p>
    ${groups}
    <section class="ops-command-group"><h3>${tr("仅命令行", "Terminal only")}</h3>${cli}</section>
  </section>`;
}

export async function renderOps(ctx) {
  const current = beginOps();
  const alive = () => current === generation;
  const { api, setPage, esc } = ctx;
  if (ctx.jobId) {
    await renderJob(ctx, current);
    return;
  }
  let home;
  try {
    home = await api("/api/ops");
  } catch (err) {
    if (!alive()) return;
    setPage(`<section class="error-state"><h1>${tr("操作页打不开", "Operations page failed to open")}</h1><p class="sub err">${esc(err.message)}</p></section>`, "ops");
    return;
  }
  if (!alive()) return;
  if (home.mode.setup) {
    renderWizard(ctx, home, current);
    return;
  }
  const preset = hashQuery();
  const selected = preset.get("op");
  const slot = home.occupancy.slot;
  const emptyLake = Boolean(home.occupancy.lake_empty);
  const backupOpen = (selected || "").startsWith("snapshot.") || preset.get("snapshot_root") ? " open" : "";
  const manualOpen = selected && workflowOf(selected) === "manual" ? " open" : "";
  const commandOpen = selected && workflowOf(selected) === "commands" ? " open" : "";
  const alerts = (home.occupancy.hints || []).map((hint) => `<li>${esc(hint)}</li>`);
  if (slot) {
    alerts.push(`<li>${tr("正在执行", "Running")} <a href="#/ops/jobs/${encodeURIComponent(slot.job_id)}">${esc(pick(slot, "title") || slot.op)}</a></li>`);
  }
  if (!home.mode.ops_enabled) alerts.push(`<li>${tr("当前模式不能从面板启动命令。", "This mode cannot start commands from the panel.")}</li>`);
  const attention = alerts.length
    ? `<section class="surface-panel report-panel ops-attention"><ul class="ops-status">${alerts.join("")}</ul></section>`
    : "";
  const deferredOpen = emptyLake
    ? `<details class="ops-secondary"><summary>${tr("空湖上的日更和补抓不会补历史。初始化完成后再用。", "A daily update or catch-up on an empty lake does not fill history. Use them after initialization.")}</summary>`
    : "";
  const deferredClose = emptyLake ? "</details>" : "";
  setPage(
    `<section class="page-heading"><div class="eyebrow">${tr("数据湖控制台 / 操作", "Lake console / Operations")}</div>
      <div class="heading-row"><div><h1>${tr("操作", "Operations")}</h1><p class="sub">${tr("先预览，再启动。同一时间只跑一个。", "Preview, then start. Only one job runs at a time.")}</p></div>
      <span class="status-pill">${esc(modeLabel(home.mode.label))}</span></div></section>
    <section class="surface-panel report-panel"><p class="panel-note">${tr("当前数据目录", "Active data directory")}: <code id="active-data-root">${esc(home.mode.data_root || "—")}</code></p>
      <p class="panel-note">${tr("配置文件", "Config file")}: <code>${esc(home.mode.config_path || "—")}</code></p>
      <p class="panel-note">${tr("修改配置文件后，刷新页面即可安全同步；取数或存储维护期间会等待任务结束。", "After editing the config file, refresh to safely reload it. Active fetching or storage maintenance must finish first.")}</p></section>
    ${attention}
    <div id="ops-recent" hidden></div>
    <div class="ops-work" id="ops-work">${initBlock(home, esc)}
      ${deferredOpen}${dailyBlock(home, esc)}
      <details class="ops-fold" id="ops-manual-fold"${manualOpen}><summary>${tr("手动更新", "Manual update")}</summary>${manualBlock(home, esc, preset)}</details>
      ${deferredClose}
      <details class="ops-fold" id="ops-commands-fold"${commandOpen}><summary>${tr("常用命令", "Commands")}</summary>${commandBlock(home, esc)}</details>
      <div id="ops-form"></div></div>
    <details class="ops-fold"><summary>${tr("取数设置", "Fetch settings")}</summary><section class="surface-panel report-panel" id="ops-settings">${tr("正在读取取数设置…", "Reading fetch settings…")}</section></details>
    <details class="ops-fold" data-workflow="backup"${backupOpen}><summary>${tr("数据备份", "Backups")}</summary><section class="surface-panel report-panel" id="ops-backups">${tr("正在读取数据备份…", "Reading backups…")}</section></details>`,
    "ops",
  );
  bindOps(ctx, home, current);
  bindManual(ctx, home, current);
  void renderSettings(ctx, home, alive);
  void renderSchedule(ctx, home, alive);
  void renderBackups(ctx, home, alive, preset.get("snapshot_root") || "");
  const jobs = await api("/api/ops/jobs?limit=8");
  if (!alive()) return;
  const recent = document.getElementById("ops-recent");
  if (recent) {
    if (!jobs.length) {
      recent.hidden = true;
      recent.innerHTML = "";
    } else {
      recent.hidden = false;
      recent.innerHTML = `<section class="surface-panel report-panel ops-recent"><div class="panel-header"><h2>${tr("最近的任务", "Recent jobs")}</h2></div><ul class="ops-jobs">${jobs
        .map((job) => {
          const when = jobStamp(job);
          return `<li><a href="#/ops/jobs/${encodeURIComponent(job.job_id)}">${esc(pick(job, "title") || job.op)}</a><span class="muted">${esc(pick(job, "label") || job.state)}${job.scheduled ? tr(" · 定时执行", " · scheduled") : ""}${when ? ` · ${esc(when)}` : ""}</span></li>`;
        })
        .join("")}</ul></section>`;
    }
  }
  if (selected && home.operations.some((card) => card.id === selected && card.available)) {
    showForm(ctx, home, selected, preset, current);
  }
}

function bindOps(ctx, home, current) {
  const open = (opId) => {
    if (typeof document.querySelectorAll === "function") {
      document.querySelectorAll("[data-op]").forEach((item) => item.classList.toggle("on", item.dataset.op === opId));
    }
    try {
      const next = `#/ops?op=${encodeURIComponent(opId)}`;
      if (!location.hash.includes(`op=${encodeURIComponent(opId)}`)) history.replaceState(null, "", next);
    } catch {
      /* the form still opens when the address cannot be updated */
    }
    showForm(ctx, home, opId, hashQuery(), current);
  };
  if (typeof document.querySelectorAll !== "function") return;
  document.querySelectorAll("[data-op]").forEach((button) => {
    button.onclick = () => open(button.dataset.op);
  });
  document.querySelectorAll("[data-copy-command]").forEach((button) => {
    button.onclick = () => navigator.clipboard?.writeText(button.dataset.copyCommand || "");
  });
}

function bindManual(ctx, home, current) {
  const select = document.getElementById("ops-scope");
  const form = document.getElementById("ops-manual-form");
  if (!select || !form) return;
  const paint = () => {
    const scope = scopeById(select.value || "packs");
    const note = document.getElementById("ops-manual-note");
    if (note) note.textContent = scope.note;
    const fields = document.getElementById("ops-manual-fields");
    if (fields) fields.innerHTML = manualFields(home, scope, hashQuery());
    const session = document.getElementById("ops-manual-session");
    if (!session) return;
    const card = home.operations.find((item) => item.id === scope.op);
    const params = card && typeof form.querySelector === "function" ? readForm(form, card.params) : {};
    const text = formNoteText(home.occupancy.schedule || {}, scope.op, params);
    session.hidden = !text;
    session.textContent = text;
  };
  select.onchange = paint;
  form.onsubmit = async (event) => {
    event.preventDefault();
    const scope = scopeById(select.value || "packs");
    const card = home.operations.find((item) => item.id === scope.op);
    if (!card || typeof form.querySelector !== "function") return;
    const params = readForm(form, card.params);
    const session = document.getElementById("ops-manual-session");
    if (session) {
      const text = formNoteText(home.occupancy.schedule || {}, card.id, params);
      session.hidden = !text;
      session.textContent = text;
    }
    await previewCard(ctx, home, card, params, current, document.getElementById("ops-manual-preview"));
  };
  if (typeof form.addEventListener === "function") {
    form.addEventListener("input", () => {
      const scope = scopeById(select.value || "packs");
      const card = home.operations.find((item) => item.id === scope.op);
      const session = document.getElementById("ops-manual-session");
      if (!card || !session || typeof form.querySelector !== "function") return;
      const text = formNoteText(home.occupancy.schedule || {}, card.id, readForm(form, card.params));
      session.hidden = !text;
      session.textContent = text;
    });
  }
  paint();
}

export function mountOpForm(ctx, home, opId, preset, host, current) {
  const card = home.operations.find((item) => item.id === opId);
  if (!card || !host || current !== generation) return null;
  const initial = {};
  for (const spec of card.params) {
    const value = fieldValue(spec, preset);
    if (spec.kind === "bool") initial[spec.name] = value;
    else if (value) initial[spec.name] = value;
  }
  const sessionText = formNoteText(home.occupancy.schedule || {}, card.id, initial);
  const rangeNote = card.id === "daily.full" || card.id === "daily.group" ? `<p class="panel-note">${dailyRangeNote()}</p>` : "";
  host.innerHTML = `<section class="surface-panel report-panel ops-form">
    <div class="panel-header"><div><h2>${ctx.esc(pick(card, "title"))}</h2><p class="sub">${ctx.esc(pick(card, "summary"))}</p></div></div>
    ${rangeNote}
    ${sessionText ? `<p class="panel-note" id="ops-session">${ctx.esc(sessionText)}</p>` : `<p class="panel-note" id="ops-session" hidden></p>`}
    <form id="ops-fields" class="ops-fields">${card.params.map((spec) => control(spec, preset)).join("")}
      <div class="action-row"><button class="button button-primary" type="submit" ${home.mode.ops_enabled ? "" : "disabled"}>${tr("预览", "Preview")}</button></div>
    </form>
    <div id="ops-preview"></div>
  </section>`;
  host.scrollIntoView?.({ behavior: "smooth", block: "start" });
  const form = document.getElementById("ops-fields");
  if (form && typeof form.querySelector === "function") {
    const paintSession = () => {
      const text = formNoteText(home.occupancy.schedule || {}, card.id, readForm(form, card.params));
      const session = document.getElementById("ops-session");
      if (!session) return;
      session.hidden = !text;
      session.textContent = text;
    };
    form.addEventListener("input", paintSession);
  }
  if (form) {
    form.onsubmit = async (event) => {
      event.preventDefault();
      await preview(ctx, home, card, current);
    };
  }
  return card;
}

function showForm(ctx, home, opId, preset, current) {
  const card = home.operations.find((item) => item.id === opId);
  const host = document.getElementById("ops-form");
  if (!card || !host || current !== generation) return;
  if (typeof document.querySelector === "function" && typeof CSS !== "undefined") {
    const section = document.querySelector(`[data-workflow="${CSS.escape(workflowOf(card.id))}"]`);
    if (section?.after) {
      section.after(host);
      const fold = section.closest?.("details");
      if (fold) fold.open = true;
    }
  }
  mountOpForm(ctx, home, opId, preset, host, current);
}

async function preview(ctx, home, card, current) {
  const params = readForm(document.getElementById("ops-fields"), card.params);
  await previewCard(ctx, home, card, params, current, document.getElementById("ops-preview"));
}

async function previewCard(ctx, home, card, params, current, target) {
  const { api, esc } = ctx;
  if (!target) return;
  target.innerHTML = `<p class="panel-note">${tr("正在预览…", "Previewing…")}</p>`;
  let body;
  try {
    body = await api("/api/ops/preview", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CNE-CSRF": home.csrf_token,
      },
      body: JSON.stringify({ op: card.id, params }),
    });
  } catch (err) {
    if (current === generation) target.innerHTML = `<p class="panel-note err">${esc(err.message)}</p>`;
    return;
  }
  if (current !== generation) return;
  const blockers = (body.blockers || []).map((item) => `<li>${esc(item)}</li>`).join("");
  const hints = (body.hints || []).map((item) => `<li>${esc(item)}</li>`).join("");
  const checks = (body.acknowledgements || [])
    .map(
      (item) =>
        `<label class="ops-check"><input type="checkbox" data-ack="${esc(item.id)}"> ${esc(pick(item, "text"))}</label>`,
    )
    .join("");
  target.innerHTML = `<p class="panel-note">${tr("将执行", "Will run")} <code>${esc(body.command)}</code></p>
    ${body.plan ? `<pre class="ops-log">${esc(body.plan)}</pre>` : ""}
    ${blockers ? `<ul class="ops-list err">${blockers}</ul>` : ""}
    ${hints ? `<ul class="ops-list">${hints}</ul>` : ""}
    ${checks}
    <div class="action-row"><button class="button button-primary" data-ops-start type="button" ${body.launch_token ? "" : "disabled"}>${tr("启动", "Start")}</button></div>`;
  const button = typeof target.querySelector === "function" ? target.querySelector("[data-ops-start]") : document.getElementById("ops-start");
  if (!button) return;
  const refresh = () => {
    const inputs = [...target.querySelectorAll("[data-ack]")];
    button.disabled = !body.launch_token || inputs.some((input) => !input.checked);
  };
  target.querySelectorAll("[data-ack]").forEach((input) => input.addEventListener("change", refresh));
  refresh();
  button.onclick = async () => {
    button.disabled = true;
    const acknowledged = [...target.querySelectorAll("[data-ack]:checked")].map((input) => input.dataset.ack);
    try {
      const job = await api("/api/ops/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CNE-CSRF": home.csrf_token },
        body: JSON.stringify({
          preview_id: body.preview_id,
          launch_token: body.launch_token,
          acknowledged,
        }),
      });
      location.hash = `#/ops/jobs/${encodeURIComponent(job.job_id)}`;
    } catch (err) {
      if (current === generation) target.insertAdjacentHTML("beforeend", `<p class="panel-note err">${esc(err.message)}</p>`);
    }
  };
}

async function renderPackFinish(job, api, csrf) {
  const host = document.getElementById("ops-readiness");
  if (!host) return;
  const initJob = job.op === "init.start" || job.op === "init.resume";
  const finished = job.state === "succeeded" || job.state === "partial";
  if (!initJob || !finished) {
    delete host.dataset.shown;
    host.innerHTML = "";
    return;
  }
  if (host.dataset.shown === job.job_id) return;
  host.dataset.shown = job.job_id;
  let body;
  try {
    body = await api("/api/ops/readiness");
  } catch (error) {
    host.textContent = error.message;
    return;
  }
  const packs = body.packs || [];
  host.innerHTML = `<section class="surface-panel report-panel ops-form">
    <h2>${tr("研究包", "Research packs")}</h2>
    ${packs.map((row) => `<p>${esc(packTitle(row))}${tr("：", ": ")}${esc(row.detail)}${row.next_command ? tr(`，下一步 <code>${esc(row.next_command)}</code>`, `; next: <code>${esc(row.next_command)}</code>`) : ""}</p>`).join("")}
    <p class="panel-note">${esc(body.note || "")}</p>
    <label class="ops-check"><input id="pack-schedule-ack" type="checkbox"> ${tr("我知道定时日更只跑这些研究包，快照漏一天无法按日期补回，公告和资讯不在其中。", "I understand the scheduled daily update runs only these packs, a missed snapshot day cannot be backfilled by date, and announcements and news are not included.")}</label>
    <div class="action-row"><button class="button button-primary" id="pack-schedule" type="button">${tr("安装定时日更", "Install scheduled daily update")}</button></div>
    <p class="panel-note" id="pack-schedule-note"></p>
  </section>`;
  document.getElementById("pack-schedule").addEventListener("click", async () => {
    const note = document.getElementById("pack-schedule-note");
    if (!document.getElementById("pack-schedule-ack").checked) {
      note.textContent = tr("请先确认执行范围。", "Confirm the scope first.");
      return;
    }
    try {
      const headers = { "Content-Type": "application/json", "X-CNE-CSRF": csrf };
      const schedule = await api("/api/ops/schedule");
      const preview = await api("/api/ops/schedule/preview", {
        method: "POST",
        headers,
        body: JSON.stringify({
          daily: true,
          stale: true,
          daily_run_at: schedule.daily_run_at,
          stale_run_at: schedule.stale_run_at,
          daily_packs: packs.map((row) => row.id),
        }),
      });
      await api("/api/ops/schedule/apply", {
        method: "POST",
        headers,
        body: JSON.stringify({ token: preview.token, acknowledged: true }),
      });
      note.textContent = tr("已安装。", "Installed.");
    } catch (error) {
      note.textContent = error.message;
    }
  });
}

async function renderJob(ctx, current) {
  const { api, setPage, esc, jobId } = ctx;
  let job;
  try {
    job = await api(`/api/ops/jobs/${encodeURIComponent(jobId)}`);
  } catch (err) {
    if (current === generation) {
      setPage(`<section class="error-state"><h1>${tr("没有这个任务", "Job not found")}</h1><p class="sub err">${esc(err.message)}</p></section>`, "ops");
    }
    return;
  }
  if (current !== generation) return;
  const home = await api("/api/ops");
  if (current !== generation) return;
  const runs = (job.runs || [])
    .map(
      (run) =>
        `<li><a href="#/runs/${encodeURIComponent(run.run_id)}">${esc(run.job_name)}</a> ${esc(run.status)}</li>`,
    )
    .join("");
  setPage(
    `<section class="page-heading"><div class="eyebrow">${tr("数据湖控制台 / 操作 / 任务", "Lake console / Operations / Job")}</div>
      <div class="heading-row"><div><h1>${esc(pick(job, "title") || job.op)}</h1><p class="sub"><span id="ops-job-state">${esc(pick(job, "label") || job.state)}</span> · <code>${esc(job.command || "")}</code></p></div>
      <div class="action-row"><a class="button button-ghost" href="#/ops">${tr("← 返回操作", "← Back to operations")}</a>
        <button class="button button-danger" id="ops-cancel" type="button" ${job.state === "running" || job.state === "starting" ? "" : "disabled"}>${tr("取消", "Cancel")}</button></div></div></section>
    <p class="panel-note" id="ops-job-note">${esc(job.outcome?.message || "")}</p>
    <p class="panel-note" id="ops-next" ${jobNextStep(job) ? "" : "hidden"}>${jobNextStep(job)}</p>
    <div id="ops-readiness"></div>
    ${runs ? `<section class="surface-panel report-panel"><h2>${tr("关联 run", "Related runs")}</h2><ul class="ops-jobs">${runs}</ul></section>` : `<div id="ops-runs"></div>`}
    <section class="surface-panel report-panel"><div class="panel-header"><h2>${tr("进度", "Progress")}</h2><label class="ops-check"><input type="checkbox" id="ops-follow" checked> ${tr("跟随", "Follow")}</label></div>
      <pre class="ops-log" id="ops-log"></pre></section>
    <section class="surface-panel report-panel"><h2>${tr("命令输出", "Command output")}</h2><pre class="ops-log" id="ops-out"></pre></section>`,
    "ops",
  );
  const log = document.getElementById("ops-log");
  const out = document.getElementById("ops-out");
  const follow = () => document.getElementById("ops-follow")?.checked;
  const token = new URLSearchParams(location.search).get("token");
  const open = (which, node) => {
    const url = `/api/stream/ops/jobs/${encodeURIComponent(jobId)}?stream=${which}${token ? `&token=${encodeURIComponent(token)}` : ""}`;
    const source = new EventSource(url);
    streams.push(source);
    source.onmessage = (event) => {
      if (current !== generation) return source.close();
      const frame = JSON.parse(event.data);
      if (frame.text) {
        node.textContent += frame.text;
        if (follow()) node.scrollTop = node.scrollHeight;
      }
      if (frame.done) {
        source.close();
        refreshJob();
      }
    };
    return source;
  };
  open("err", log);
  open("out", out);
  async function refreshJob() {
    if (current !== generation) return;
    const latest = await api(`/api/ops/jobs/${encodeURIComponent(jobId)}`);
    const state = document.getElementById("ops-job-state");
    if (state) state.textContent = pick(latest, "label") || latest.state;
    const note = document.getElementById("ops-job-note");
    if (note) note.textContent = latest.outcome?.message || "";
    const next = document.getElementById("ops-next");
    if (next) {
      const html = jobNextStep(latest);
      next.hidden = !html;
      next.innerHTML = html;
    }
    const cancel = document.getElementById("ops-cancel");
    if (cancel) cancel.disabled = !(latest.state === "running" || latest.state === "starting");
    await renderPackFinish(latest, api, home.csrf_token);
    const host = document.getElementById("ops-runs");
    if (host && latest.runs?.length) {
      host.innerHTML = `<section class="surface-panel report-panel"><h2>${tr("关联 run", "Related runs")}</h2><ul class="ops-jobs">${latest.runs
        .map((run) => `<li><a href="#/runs/${encodeURIComponent(run.run_id)}">${esc(run.job_name)}</a> ${esc(run.status)}</li>`)
        .join("")}</ul></section>`;
    }
  }
  document.getElementById("ops-cancel").onclick = async () => {
    const button = document.getElementById("ops-cancel");
    button.disabled = true;
    try {
      await api(`/api/ops/jobs/${encodeURIComponent(jobId)}/cancel`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CNE-CSRF": home.csrf_token },
        body: "{}",
      });
    } catch (err) {
      const note = document.getElementById("ops-job-note");
      if (note) note.textContent = err.message;
    }
  };
}

function renderWizard(ctx, home, current) {
  const { api, setPage, esc } = ctx;
  setPage(
    `<section class="page-heading"><div class="eyebrow">${tr("首次配置", "First-time setup")}</div><div class="heading-row"><div><h1>${tr("建立数据湖", "Create the lake")}</h1><p class="sub">${tr("选定数据目录、历史起点和研究包。初始化仍下载固定的全市场行情主干。这一步只能在本机完成。", "Choose a data directory, history start and research packs. Initialization always downloads the fixed full-market market data core. This step can only be done on this machine.")}</p></div></div></section>
    <section class="surface-panel report-panel ops-form">
      <label>${tr("数据目录", "Data directory")}<input id="setup-root" type="text" value="${esc(home.mode.suggested_data_root || "")}"></label>
      <p class="panel-note">${tr("必须是绝对路径。缺少的目录会自动建立，已有上级目录必须可写。", "Must be an absolute path. Missing directories are created; existing parent directories must be writable.")}</p>
      <div class="action-row"><button class="button button-ghost" id="setup-doctor" type="button">${tr("环境体检", "Environment check")}</button></div>
      <div id="setup-doctor-out"></div>
      <label>${tr("历史深度", "History depth")}<select id="setup-profile"><option value="quick">${tr("近 3 年", "Last 3 years")}</option><option value="full">${tr("从 2016-01-01 起的日线", "Daily bars since 2016-01-01")}</option></select></label>
      <label>${tr("历史起点（可选，覆盖深度）", "History start (optional, overrides depth)")}<input id="setup-since" type="date"></label>
      <fieldset class="ops-fields">
        <legend>${tr("研究包", "Research packs")}</legend>
        <label class="ops-check"><input id="setup-pack-market" type="checkbox" checked disabled> ${tr("行情（初始化固定下载，日更更新这一组）", "Market data (always downloaded at initialization; the daily update refreshes this group)")}</label>
        <label class="ops-check"><input id="setup-pack-fundamentals" type="checkbox"> ${tr("基本面（日更再加财报和股本，不在这次初始化里下载）", "Fundamentals (the daily update adds financial statements and share capital; not downloaded during this initialization)")}</label>
        <label class="ops-check"><input id="setup-pack-universe" type="checkbox"> ${tr("股票池（结束后补历史 ST，不额外占用日更额度）", "Universe (backfills historical ST afterwards; uses no extra daily update quota)")}</label>
      </fieldset>
      <p class="panel-note">${tr("定时日更只跑所选研究包。交易状态等快照漏掉当天无法按日期补回。公告和资讯要另外运行。", "The scheduled daily update runs only the selected packs. Snapshots such as trading status cannot be backfilled by date if a day is missed. Announcements and news run separately.")}</p>
      <label class="ops-check"><input id="setup-ack" type="checkbox"> ${tr("我知道初始化会按所选深度请求数据源，可能持续数小时。", "I understand initialization queries the data sources for the selected depth and may take several hours.")}</label>
      <div class="action-row"><button class="button button-primary" id="setup-go" type="button" disabled>${tr("生成配置并开始初始化", "Create config and start initialization")}</button></div>
      <p class="panel-note" id="setup-hint" hidden></p>
      <p class="panel-note" id="setup-command" hidden></p>
      <p class="panel-note" id="setup-note"></p>
    </section>`,
    "ops",
  );
  const note = document.getElementById("setup-note");
  const go = document.getElementById("setup-go");
  const doctor = document.getElementById("setup-doctor");
  let armed = null;
  const wizardParams = () => {
    const params = {
      profile: document.getElementById("setup-profile").value,
      pack: ["market"],
    };
    if (document.getElementById("setup-pack-fundamentals").checked) params.pack.push("fundamentals");
    if (document.getElementById("setup-pack-universe").checked) params.pack.push("universe");
    const since = document.getElementById("setup-since").value;
    if (since) params.since = since;
    return params;
  };
  const allowGo = () => {
    go.disabled = go.dataset.doctor === "1" || go.dataset.blocked === "1" || !document.getElementById("setup-ack").checked;
  };
  const clearCommand = () => {
    armed = null;
    const command = document.getElementById("setup-command");
    command.hidden = true;
    command.textContent = "";
    if (go.textContent === tr("开始初始化", "Start initialization")) go.textContent = tr("预览初始化", "Preview initialization");
  };
  document.getElementById("setup-ack").onchange = allowGo;
  for (const id of ["setup-profile", "setup-since", "setup-pack-fundamentals", "setup-pack-universe"]) {
    document.getElementById(id).onchange = () => {
      if (!armed) return;
      clearCommand();
      note.textContent = tr("参数已改，请再预览一次命令。", "Parameters changed. Preview the command again.");
      allowGo();
    };
  }
  doctor.onclick = async () => {
    doctor.disabled = true;
    go.dataset.doctor = "1";
    allowGo();
    note.textContent = tr("正在体检…", "Checking the environment…");
    try {
      const preview = await api("/api/ops/preview", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CNE-CSRF": home.csrf_token },
        body: JSON.stringify({ op: "diag.doctor", params: {} }),
      });
      if (!preview.launch_token) {
        note.textContent = (preview.blockers || [tr("现在不能体检。", "The environment check cannot run now.")]).join(" ");
        return;
      }
      const job = await api("/api/ops/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CNE-CSRF": home.csrf_token },
        body: JSON.stringify({ preview_id: preview.preview_id, launch_token: preview.launch_token, acknowledged: [] }),
      });
      let latest = job;
      while (latest.state === "starting" || latest.state === "running") {
        note.textContent = tr("体检进行中，结束后才能初始化。", "The environment check is running. Initialization waits until it finishes.");
        await new Promise((resolve) => setTimeout(resolve, 400));
        if (current !== generation) return;
        latest = await api(`/api/ops/jobs/${encodeURIComponent(job.job_id)}`);
      }
      if (current !== generation) return;
      const findings = latest.result?.summary?.findings || [];
      const bad = findings.filter((item) => item.severity === "error");
      document.getElementById("setup-doctor-out").innerHTML = findings.length
        ? `<ul class="ops-list">${findings.map((item) => `<li>${esc(item.severity)} ${esc(item.title)}</li>`).join("")}</ul>`
        : `<p class="panel-note">${esc(latest.outcome?.message || latest.label || "")}</p>`;
      const blocked = bad.length > 0 || latest.state === "failed" || latest.state === "findings" || latest.state === "error";
      go.dataset.blocked = blocked ? "1" : "";
      note.textContent = blocked
        ? bad.length
          ? tr("体检有 error，先处理再初始化。", "The environment check has an error. Fix it before initializing.")
          : latest.outcome?.message || tr("体检没有通过。", "The environment check did not pass.")
        : tr("体检没有 error。", "The environment check has no error.");
    } catch (err) {
      note.textContent = err.message;
    } finally {
      doctor.disabled = false;
      go.dataset.doctor = "";
      if (current === generation) allowGo();
    }
  };
  go.onclick = async () => {
    if (go.dataset.blocked === "1") {
      note.textContent = tr("体检有 error，先处理再初始化。", "The environment check has an error. Fix it before initializing.");
      return;
    }
    if (go.dataset.doctor === "1") {
      note.textContent = tr("体检还在进行，结束后才能初始化。", "The environment check is still running. Initialization waits until it finishes.");
      return;
    }
    const params = wizardParams();
    const signature = JSON.stringify(params);
    if (armed && armed.signature === signature) {
      go.disabled = true;
      try {
        const job = await api("/api/ops/jobs", {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-CNE-CSRF": armed.csrf },
          body: JSON.stringify({
            preview_id: armed.preview_id,
            launch_token: armed.launch_token,
            acknowledged: ["confirm", "heavy"],
          }),
        });
        location.hash = `#/ops/jobs/${encodeURIComponent(job.job_id)}`;
      } catch (err) {
        armed = null;
        note.textContent = err.message;
        go.textContent = tr("预览初始化", "Preview initialization");
        allowGo();
      }
      return;
    }
    go.disabled = true;
    note.textContent = tr("正在检查配置…", "Checking configuration…");
    try {
      let fresh = await api("/api/ops");
      if (current !== generation) return;
      if (fresh.mode.setup) {
        note.textContent = tr("正在生成配置…", "Creating configuration…");
        const created = await api("/api/setup/config", {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-CNE-CSRF": fresh.csrf_token },
          body: JSON.stringify({ data_root: document.getElementById("setup-root").value.trim() }),
        });
        if (current !== generation) return;
        fresh = await api("/api/ops");
        if (current !== generation) return;
        if (created.hint) {
          const hint = document.getElementById("setup-hint");
          hint.hidden = false;
          hint.textContent = created.hint;
          document.getElementById("setup-root").disabled = true;
          go.textContent = tr("预览初始化", "Preview initialization");
          note.textContent = tr("配置已生成。请先阅读上面的说明，再预览初始化命令。", "Configuration created. Read the notes above, then preview the initialization command.");
          allowGo();
          return;
        }
      }
      document.getElementById("setup-root").disabled = true;
      go.textContent = tr("预览初始化", "Preview initialization");
      note.textContent = tr("正在预览命令…", "Previewing command…");
      const preview = await api("/api/ops/preview", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CNE-CSRF": fresh.csrf_token },
        body: JSON.stringify({ op: "init.start", params }),
      });
      if (!preview.launch_token) {
        armed = null;
        note.textContent = (preview.blockers || []).join(" ");
        allowGo();
        return;
      }
      armed = {
        signature,
        preview_id: preview.preview_id,
        launch_token: preview.launch_token,
        csrf: fresh.csrf_token,
      };
      const command = document.getElementById("setup-command");
      command.hidden = false;
      command.textContent = tr(`将执行 ${preview.command}`, `Will run ${preview.command}`);
      go.textContent = tr("开始初始化", "Start initialization");
      note.textContent = tr("请核对上面的命令，再开始初始化。", "Check the command above, then start initialization.");
      allowGo();
    } catch (err) {
      armed = null;
      note.textContent = err.message;
      allowGo();
    }
  };
}
