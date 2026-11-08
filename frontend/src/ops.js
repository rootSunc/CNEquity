// Whitelisted commands. A preview is not a start; the launch token is single use.
import { renderSchedule } from "./schedule.js";
import { renderBackups } from "./backups.js";
import { renderSettings } from "./settings.js";
import { choiceLabel, ds, locale, modeLabel, pick, tr } from "./i18n.js";
import {
  beijingDate,
  beijingMidnight,
  digestJobs,
  jobTone,
  matrixHtml,
  recentHtml,
  staleHtml,
  staleRows,
  timelineLegend,
  timelineModel,
  timelineSvg,
} from "./ops-today.js";
let generation = 0;
let streams = [];
let paintRunBoard = () => {};

export function setRunBoardPainter(paint) {
  paintRunBoard = typeof paint === "function" ? paint : () => {};
}
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
      `${schedule.daily_pending} 的定时日更（${schedule.daily_run_at}）已到点，还没跑。`,
      `The scheduled daily update for ${schedule.daily_pending} (${schedule.daily_run_at}) is due and has not run yet.`,
    );
  }
  if (schedule.daily_done) {
    const session = schedule.daily_due ? `${schedule.daily_due}` : "";
    return session
      ? tr(
          `${session} 的定时日更（${schedule.daily_run_at}）已经跑过。`,
          `The scheduled daily update for ${session} (${schedule.daily_run_at}) already ran.`,
        )
      : tr(
          `今天的定时日更（${schedule.daily_run_at}）已经跑过。`,
          `Today's scheduled daily update (${schedule.daily_run_at}) already ran.`,
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
  if (pending) parts.push(tr(`待跑的定时日更是 ${pending}（${runAt}）。`, `The scheduled daily update due is ${pending} (${runAt}).`));
  else if (schedule.daily_done && schedule.daily_due) {
    parts.push(tr(`${schedule.daily_due} 的定时日更（${runAt}）已经跑过。`, `The scheduled daily update for ${schedule.daily_due} (${runAt}) already ran.`));
  } else parts.push(tr(`定时日更时间 ${runAt}（北京时间）。`, `Scheduled daily update at ${runAt} (Beijing time).`));
  if (opId === "daily.full") {
    const date = String(params.trade_date || "").trim();
    const counts = !params.backfill && pending && ((date === "" && today === pending) || date === pending);
    parts.push(counts
      ? tr(`这次手动运行会计入定时日更 ${pending}。`, `This manual run counts as the scheduled update ${pending}.`)
      : tr("这次手动运行不会记成定时日更。", "This manual run is not recorded as that scheduled update."));
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
    `${name}默认不计算。运行后写入数据湖，resampled_from 标明由 1 分钟或 5 分钟重采样。不填起止日期时只补未计算或分钟线已更新的交易日；全量重写会重算全部。`,
    `${name} are not computed by default. Running this writes them into the lake, and resampled_from records whether each bar came from 1-minute or 5-minute data. With no start or end, only sessions not yet computed or whose minute bars changed are rebuilt. Full rewrite rebuilds all of them.`,
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

// A preview has no side effects, so it runs as soon as the required fields are
// filled and again after each edit; the reader only has to press Start.
function previewReady(params, specs) {
  return specs.every((spec) => !spec.required || (Array.isArray(params[spec.name]) ? params[spec.name].length : params[spec.name]));
}

function autoPreview(form, specs, run, blocked = () => false) {
  if (!form || typeof form.addEventListener !== "function" || typeof form.querySelector !== "function") return;
  let timer = 0;
  const fire = () => {
    const params = readForm(form, specs);
    if (previewReady(params, specs) && !blocked(params)) void run();
  };
  form.addEventListener("input", () => {
    clearTimeout(timer);
    timer = setTimeout(fire, 450);
  });
  fire();
}

function readForm(form, specs) {
  const params = {};
  for (const spec of specs) {
    const input = form.querySelector(`[name="${CSS.escape(spec.name)}"]`);
    if (!input) continue;
    if (spec.kind === "bool") params[spec.name] = input.checked;
    else if (spec.kind === "multi") {
      params[spec.name] = typeof form.querySelectorAll === "function" && input.type === "checkbox"
        ? [...form.querySelectorAll(`[name="${CSS.escape(spec.name)}"]:checked`)].map((box) => box.value)
        : [...(input.selectedOptions || [])].map((option) => option.value);
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
    const boxes = (spec.choices || [])
      .map((choice) => `<label class="ops-chip"><input type="checkbox" name="${esc(spec.name)}" value="${esc(choice)}" ${value.includes(choice) ? "checked" : ""}><span>${esc(choiceLabel(spec.name, choice))}</span></label>`)
      .join("");
    return `<fieldset class="ops-chips"><legend>${pick(spec, "label")}</legend>${boxes}</fieldset>${help}`;
  }
  const type = spec.kind === "date" ? "date" : "text";
  return `<label>${esc(pick(spec, "label"))}<input name="${esc(spec.name)}" type="${type}" value="${esc(value)}" ${spec.required ? "required" : ""}></label>${help}`;
}

// What each launchable operation can and cannot do, shown above its form.
function opNote(opId) {
  return {
    "daily.full": tr("不选研究包就跑全部日更组和事件流。日更只有一个交易日。", "With no pack selected, every daily group and the event stream run. A daily update is one session."),
    "daily.group": tr("只跑这一组，不会把那天记成定时日更已完成。", "Runs only this group, and does not mark that session's scheduled update done."),
    "events.run": tr("周末和节假日也可以。公告和资讯不在研究包日更里。", "Weekends and holidays are included. Announcements and news are not part of a research-pack daily update."),
    "backfill.run": tr("一段历史走回填。一次只能回填一个数据集。交易状态这类快照漏掉的日子，改一个旧日期也补不回来。", "A history range is a backfill, one dataset at a time. A missed snapshot day, such as trading status, cannot be recovered by picking an old date."),
    "derive.run": tr("从已发布的数据重算。全量会重写分区。15 / 30 / 60 分钟线也在这里入湖。", "Recompute from published data. A full range rewrites partitions. 15/30/60-minute bars are stored from here too."),
    "daily.stale": tr("没有日期。只重抓仍然落后的数据集。快照漏掉的当天只在当天再试。", "No date. Refetch only datasets that are still stale. A missed snapshot day can only be retried that day."),
  }[opId] || "";
}

function commandGroups() {
  return [
    [tr("初始化", "Initialize"), ["diag.doctor", "init.start", "init.resume"]],
    [tr("日更", "Daily"), ["daily.full", "daily.group", "daily.stale", "events.run"]],
    [tr("补数", "Backfill"), ["backfill.run", "derive.run"]],
    [tr("运行", "Runs"), ["run.retry", "run.retry_failed_groups", "run.compact"]],
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

function moreMenu(home) {
  const items = [
    ["daily.group", tr("一个调度组", "One schedule group")],
    ["events.run", tr("跑事件流", "Run the event stream")],
    ["derive.run", tr("重算派生", "Recompute a derived dataset")],
  ]
    .map(([id, label]) => opButton(home, id, label, "ops-menu-item"))
    .filter(Boolean)
    .join("");
  return items ? `<details class="ops-more"><summary class="button button-ghost">${tr("更多", "More")}</summary><div class="ops-menu">${items}</div></details>` : "";
}

function intentBlock(home, escapeHtml) {
  const schedule = home.occupancy.schedule || {};
  const actions = [
    dailyPrimaryAction(home, schedule, escapeHtml),
    opButton(home, "backfill.run", tr("补一段历史", "Fill a history range"), "button button-ghost"),
    opButton(home, "daily.stale", tr("补抓落后", "Catch up stale data"), "button button-ghost"),
    moreMenu(home),
  ].filter(Boolean).join("");
  const closedToday = schedule.today_is_session === false && !schedule.daily_pending
    ? `<p class="ops-hint">${tr("今天不是交易日，所以没有“现在跑今天”。要重跑某一个交易日，打开「更多 → 一个调度组」并填写日期。", "Today is not a trading session, so there is no run-today action. To rerun one session, open More → One schedule group and enter the date.")}</p>`
    : "";
  const pending = home.occupancy.incomplete_init;
  const initLeads = Boolean(home.occupancy.lake_empty) || Boolean(pending);
  return `<section class="surface-panel report-panel ops-workflow ops-intents${initLeads ? "" : " ops-card-primary"}" id="ops-daily" data-workflow="daily">
    <div class="panel-header"><h2>${tr("要做什么", "What to run")}</h2></div>
    ${actions ? `<div class="ops-actions">${actions}</div>` : ""}
    ${closedToday}
  </section>`;
}

// Checks a reader runs by name, each with what it did last time. The full
// allowlist with command lines stays below as a folded reference.
const CHECK_IDS = [
  "check.status",
  "check.verify",
  "check.audit",
  "check.sources_probe",
  "diag.doctor",
  "maint.stats_rebuild",
  "config.upgrade_preview",
  "run.retry_failed_groups",
  "run.retry",
  "run.compact",
];

function launchButton(card, disabled, escapeHtml) {
  const quick = card.confirm === "none" && !(card.params || []).some((spec) => spec.required);
  return quick
    ? `<button type="button" class="button button-primary" data-op-quick="${escapeHtml(card.id)}" ${disabled ? "disabled" : ""}>${tr("运行", "Run")}</button>`
    : `<button type="button" class="button button-ghost" data-op="${escapeHtml(card.id)}" ${disabled ? "disabled" : ""}>${tr("用这个", "Use this")}</button>`;
}

function checksBlock(home, escapeHtml) {
  const byId = new Map(home.operations.map((card) => [card.id, card]));
  const cards = CHECK_IDS.map((id) => {
    const card = byId.get(id);
    if (!card) return "";
    const disabled = !card.available || !home.mode.ops_enabled;
    return `<article class="ops-check-card" data-check="${escapeHtml(id)}">
      <div><strong>${escapeHtml(pick(card, "title"))}</strong><p class="muted">${escapeHtml(pick(card, "summary"))}</p></div>
      <p class="ops-check-last" id="ops-last-${escapeHtml(id)}">${tr("近期没有从页面运行过。", "Not run from the page lately.")}</p>
      <div class="ops-actions">${launchButton(card, disabled, escapeHtml)}</div>
    </article>`;
  }).join("");
  const groups = commandGroups().map(([title, ids]) => {
    const rows = ids
      .map((id) => {
        const card = byId.get(id);
        if (!card) return "";
        const disabled = !card.available || !home.mode.ops_enabled;
        const copy = card.command
          ? `<button type="button" class="button button-ghost" data-copy-command="${escapeHtml(card.command)}">${tr("复制", "Copy")}</button>`
          : "";
        return `<div class="ops-command"><div><strong>${escapeHtml(pick(card, "title"))}</strong><code>${escapeHtml(card.command || "")}</code></div>
          <div class="ops-actions">${copy}${launchButton(card, disabled, escapeHtml)}</div></div>`;
      })
      .join("");
    return rows ? `<section class="ops-command-group"><h3>${escapeHtml(title)}</h3>${rows}</section>` : "";
  }).join("");
  const cli = cliOnly().map(
    ([command, why]) => `<div class="ops-command ops-cli-only"><div><code>${escapeHtml(command)}</code><div class="muted">${escapeHtml(why)}</div></div></div>`,
  ).join("");
  return `<section class="ops-workflow" id="ops-commands-fold" data-workflow="commands">
    <p class="ops-hint">${tr("没有参数、也不改数据的巡检直接运行；其余点「用这个」打开表单，确认命令后启动。", "A check with no parameters and no data change runs directly. Use this opens the form for the rest; check the command, then start.")}</p>
    ${cards ? `<div class="ops-check-grid">${cards}</div>` : ""}
    <details class="ops-fold" id="ops-reference"><summary>${tr("命令参考", "Command reference")}</summary>
      <div class="ops-workflow">
        <p class="ops-hint">${tr("和页面能启动的是同一套白名单。", "The same allowlist the page can start.")}</p>
        ${groups}
        <section class="ops-command-group"><h3>${tr("仅命令行", "Terminal only")}</h3>${cli}</section>
      </div>
    </details>
  </section>`;
}

function paintLastRuns(jobs) {
  for (const id of CHECK_IDS) {
    const node = document.getElementById(`ops-last-${id}`);
    const job = (jobs || []).find((item) => item.op === id);
    if (!node || !job) continue;
    const tone = jobTone(job.state);
    const when = jobStamp(job);
    node.innerHTML = `<i class="ops-dot tone-${tone}" aria-hidden="true"></i>${tr("上次", "Last")} <a href="#/ops/jobs/${encodeURIComponent(job.job_id)}">${esc(pick(job, "label") || job.state)}</a>${when ? ` · ${esc(when)}` : ""}`;
  }
}

const OPS_TABS = ["today", "checks", "schedule"];

function tabForOp(opId) {
  if (!opId) return "today";
  if (opId.startsWith("snapshot.")) return "schedule";
  if (["check.", "maint.", "config.", "run.", "diag."].some((prefix) => opId.startsWith(prefix))) return "checks";
  return "today";
}

function showOpsTab(name) {
  for (const id of OPS_TABS) {
    const panel = document.getElementById(`ops-panel-${id}`);
    const tab = document.getElementById(`ops-tab-${id}`);
    const on = id === name;
    if (panel) panel.hidden = !on;
    if (tab) {
      tab.className = `tab${on ? " on" : ""}`;
      tab.setAttribute?.("aria-selected", String(on));
    }
  }
}

// The start form opens in a side drawer over the page, so choosing an action
// never moves the page under the reader.
function setDrawer(open) {
  const host = document.getElementById("ops-form");
  const backdrop = document.getElementById("ops-drawer-backdrop");
  if (host) host.hidden = !open;
  if (backdrop) backdrop.hidden = !open;
  if (!open && host) host.innerHTML = "";
}

function closeDrawer() {
  setDrawer(false);
  try {
    if (/[?&]op=/.test(location.hash)) history.replaceState(null, "", "#/ops");
  } catch {
    /* the drawer still closes when the address cannot be updated */
  }
}

let drawerKeys = false;
function bindDrawer() {
  const backdrop = document.getElementById("ops-drawer-backdrop");
  if (backdrop) backdrop.onclick = closeDrawer;
  if (drawerKeys || typeof document.addEventListener !== "function") return;
  drawerKeys = true;
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && document.getElementById("ops-form")?.hidden === false) closeDrawer();
  });
  // The "More" menu is a <details>; a click anywhere else folds it again.
  document.addEventListener("click", (event) => {
    document.querySelectorAll?.("details.ops-more[open]").forEach((menu) => {
      if (!menu.contains(event.target)) menu.open = false;
    });
  });
}

function sessionGlance(schedule) {
  const at = schedule.daily_run_at ? tr(`北京时间 ${schedule.daily_run_at}`, `${schedule.daily_run_at} Beijing`) : "";
  if (schedule.daily_pending) return { title: tr(`待跑 ${schedule.daily_pending}`, `Due ${schedule.daily_pending}`), detail: at };
  if (schedule.daily_done && schedule.daily_due) return { title: tr(`${schedule.daily_due} 已跑过`, `${schedule.daily_due} already ran`), detail: at };
  if (schedule.today_is_session === false) return { title: tr("非交易日", "Not a session"), detail: at || tr("今天不跑日更。", "No daily update today.") };
  if (schedule.today_is_session) return { title: tr("交易日", "Trading session"), detail: at || tr("可以跑今天。", "Today can run.") };
  return { title: tr("日更", "Daily"), detail: at || tr("还没有定时时间。", "No scheduled time yet.") };
}

function sourceGlance(sources) {
  if (!sources) return { title: tr("—", "—"), detail: tr("随操作页一起读取。", "Shown with the operations page."), alert: false };
  const count = (sources.breakers || []).length;
  if (sources.readable === false) return { title: tr("不可读", "Unreadable"), detail: tr("读不到熔断账本。", "Could not read the breaker ledger."), alert: true };
  if (sources.push2_paused && count) {
    return {
      title: tr("降级", "Degraded"),
      detail: tr(`push2 已暂停，${count} 个源已熔断。`, count === 1 ? "push2 is paused; 1 source tripped." : `push2 is paused; ${count} sources tripped.`),
      alert: true,
    };
  }
  if (sources.push2_paused) return { title: tr("push2 暂停", "push2 paused"), detail: tr("今日没有熔断。", "No breaker tripped today."), alert: true };
  if (count) {
    return {
      title: tr("已熔断", "Tripped"),
      detail: tr(`${count} 个源今日已熔断。`, count === 1 ? "1 source tripped today." : `${count} sources tripped today.`),
      alert: true,
    };
  }
  return { title: tr("正常", "Clear"), detail: tr("push2 可用，今日没有熔断。", "push2 is available; no breaker tripped today."), alert: false };
}

function statusCard(id, label, title, detail, extra = "", action = "") {
  const tone = extra.includes("alert") ? "warn" : extra.includes("live") ? "live" : "ok";
  return `<article class="ops-status-card${extra}" id="${id}"><span class="metric-label"><i class="ops-dot tone-${tone}" aria-hidden="true"></i>${label}</span><strong>${title}</strong><p>${detail}</p>${action}</article>`;
}

function statusBar(home, escapeHtml) {
  const session = sessionGlance(home.occupancy.schedule || {});
  const sources = sourceGlance(home.occupancy.sources);
  const slot = home.occupancy.slot;
  const slotLive = slot && (slot.state === "running" || slot.state === "starting");
  const slotDetail = slotLive
    ? `<a href="#/ops/jobs/${encodeURIComponent(slot.job_id)}">${escapeHtml(pick(slot, "title") || slot.op || "")}</a>`
    : escapeHtml(tr("可以启动下一条命令。", "The next command can start."));
  const tabs = [
    ["today", tr("今日", "Today")],
    ["checks", tr("巡检", "Checks")],
    ["schedule", tr("定时、数据源与备份", "Schedule, sources & backups")],
  ].map(([id, label]) => `<button type="button" class="tab" role="tab" id="ops-tab-${id}">${label}</button>`).join("");
  return `<section class="ops-status-bar" aria-label="${tr("运维状态", "Operations status")}">
    ${statusCard("ops-status-session", tr("日更", "Daily"), escapeHtml(session.title), escapeHtml(dailyHeader(home.occupancy.schedule || {}) || session.detail), home.occupancy.schedule?.daily_pending ? " ops-status-alert" : "")}
    ${statusCard("ops-status-scheduler", tr("定时任务", "Schedule"), escapeHtml(tr("正在读取…", "Reading…")), escapeHtml(tr("正在读取定时任务状态…", "Reading schedule status…")))}
    ${statusCard("ops-status-sources", tr("数据源", "Sources"), escapeHtml(sources.title), escapeHtml(sources.detail), sources.alert ? " ops-status-alert" : "")}
    ${statusCard("ops-status-slot", tr("执行槽", "Slot"), escapeHtml(slotLive ? tr("执行中", "Running") : tr("空闲", "Idle")), slotDetail, slotLive ? " ops-status-live" : "")}
  </section>
  <nav class="tabs ops-tabs" role="tablist" aria-label="${tr("操作分区", "Operations sections")}">${tabs}</nav>`;
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
  const emptyLake = Boolean(home.occupancy.lake_empty);
  const activeTab = tabForOp(selected || (preset.get("snapshot_root") ? "snapshot.create" : ""));
  const hide = (name) => (activeTab === name ? "" : " hidden");
  // The status cards already show the session, the sources and the slot; the
  // banner only carries what blocks starting anything from here.
  const alerts = (home.occupancy.hints || []).map((hint) => `<li>${esc(hint)}</li>`);
  if (!home.mode.ops_enabled) alerts.push(`<li>${tr("当前模式不能从面板启动命令。", "This mode cannot start commands from the panel.")}</li>`);
  if (home.mode.config_drift) alerts.push(`<li>${tr("配置文件和这个控制台用的不是同一个数据湖，处理好之前不能从这里启动任务，见页面顶部的提示。", "The config file and this console disagree about the lake. Jobs cannot start from here until that is resolved; see the notice at the top of the page.")}</li>`);
  const attention = alerts.length
    ? `<section class="surface-panel report-panel ops-attention"><ul class="ops-status">${alerts.join("")}</ul></section>`
    : "";
  const deferredOpen = emptyLake
    ? `<details class="ops-secondary"><summary>${tr("空湖上的日更和补抓不会补历史。初始化完成后再用。", "A daily update or catch-up on an empty lake does not fill history. Use them after initialization.")}</summary>`
    : "";
  const deferredClose = emptyLake ? "</details>" : "";
  setPage(
    `<section class="page-heading">
      <div class="heading-row"><div><h1>${tr("操作", "Operations")}</h1>
        <p class="meta-row"><span>${tr("数据目录", "Data directory")}</span><code>${esc(home.mode.data_root || "—")}</code></p></div>
      <span class="status-pill">${esc(modeLabel(home.mode.label))}</span></div></section>
    ${attention}
    <div class="ops-work" id="ops-work">${statusBar(home, esc)}
      <div id="ops-panel-today" data-ops-panel="today"${hide("today")}>${initBlock(home, esc)}
        ${deferredOpen}${intentBlock(home, esc)}${deferredClose}
        <section class="surface-panel report-panel ops-timeline" id="ops-timeline">
          <div class="panel-header"><h2>${tr("任务时间线", "Job timeline")}</h2>
            <div class="ops-day-switch" role="group" aria-label="${tr("选择日期", "Pick a day")}"><span class="panel-meta">${tr("北京时间", "Beijing time")}</span>
              <button type="button" class="ops-filter" data-tl-day="1" aria-pressed="false">${tr("昨天", "Yesterday")}</button><button type="button" class="ops-filter on" data-tl-day="0" aria-pressed="true">${tr("今天", "Today")}</button></div></div>
          <div id="ops-timeline-body"><p class="muted">${tr("正在读取今天的任务…", "Reading today's jobs…")}</p></div>
        </section>
        <div class="ops-split">
          <section class="surface-panel report-panel" id="ops-stale"><div class="panel-header"><h2>${tr("落后的数据集", "Stale datasets")}</h2></div><div id="ops-stale-body"><p class="muted">${tr("正在读取…", "Reading…")}</p></div></section>
          <section class="surface-panel report-panel" id="ops-matrix"><div class="panel-header"><h2>${tr("近 14 个交易日的日更", "Daily groups, last 14 sessions")}</h2><a class="panel-meta" href="#/runs">${tr("全部运行", "All runs")}</a></div><div id="ops-matrix-body"><p class="muted">${tr("正在读取…", "Reading…")}</p></div></section>
        </div>
        <div id="ops-recent" hidden></div>
      </div>
      <div id="ops-panel-checks" data-ops-panel="checks"${hide("checks")}>${checksBlock(home, esc)}</div>
      <div id="ops-panel-schedule" data-ops-panel="schedule"${hide("schedule")}>
        <section class="surface-panel report-panel" id="ops-schedule">${tr("正在读取定时任务状态…", "Reading schedule status…")}</section>
        <section class="surface-panel report-panel" id="ops-settings">${tr("正在读取取数设置…", "Reading fetch settings…")}</section>
        <section class="surface-panel report-panel" id="ops-backups" data-workflow="backup">${tr("正在读取数据备份…", "Reading backups…")}</section>
      </div>
    </div>
    <div class="ops-drawer-backdrop" id="ops-drawer-backdrop" hidden></div>
    <aside class="ops-drawer" id="ops-form" hidden aria-label="${tr("启动表单", "Start form")}"></aside>`,
    "ops",
  );
  showOpsTab(activeTab);
  bindOps(ctx, home, current);
  bindDrawer();
  void renderSettings(ctx, home, alive);
  void renderSchedule(ctx, home, alive);
  void renderBackups(ctx, home, alive, preset.get("snapshot_root") || "");
  void paintStale(ctx, home, current);
  void paintMatrix(ctx, alive);
  await paintJobs(ctx, home, alive);
  if (!alive()) return;
  if (selected && home.operations.some((card) => card.id === selected && card.available)) {
    showForm(ctx, home, selected, preset, current);
  }
}

let jobFilter = "all";

async function paintJobs(ctx, home, alive) {
  const since = new Date(beijingMidnight() - 86400000).toISOString();
  let today = [];
  let recent = [];
  let schedule = null;
  try {
    [today, recent, schedule] = await Promise.all([
      ctx.api(`/api/ops/jobs?limit=500&since=${encodeURIComponent(since)}`),
      ctx.api("/api/ops/jobs?limit=50&exclude_op=events.run"),
      ctx.api("/api/ops/schedule").catch(() => null),
    ]);
  } catch (err) {
    if (!alive()) return;
    const body = document.getElementById("ops-timeline-body");
    if (body) body.innerHTML = `<p class="panel-note err">${esc(err.message)}</p>`;
    return;
  }
  if (!alive()) return;
  const body = document.getElementById("ops-timeline-body");
  if (body) {
    const marks = { ...(home.occupancy.schedule || {}), ...(schedule || {}) };
    const legend = timelineLegend(schedule?.events ? schedule.events_interval_minutes : null);
    const draw = (daysBack) => {
      body.innerHTML = `<div class="ops-timeline-scroll">${timelineSvg(timelineModel(today, marks, Date.now(), daysBack))}</div>${legend}`;
      if (typeof document.querySelectorAll !== "function") return;
      document.querySelectorAll("[data-tl-day]").forEach((button) => {
        const on = Number(button.dataset.tlDay) === daysBack;
        button.className = `ops-filter${on ? " on" : ""}`;
        button.setAttribute?.("aria-pressed", String(on));
        button.onclick = () => draw(Number(button.dataset.tlDay));
      });
    };
    draw(0);
  }
  paintLastRuns(recent);
  const host = document.getElementById("ops-recent");
  if (!host) return;
  const rows = digestJobs([...(today || []), ...(recent || [])]);
  if (!rows.length) {
    host.hidden = true;
    host.innerHTML = "";
    return;
  }
  let expanded = false;
  const paint = () => {
    host.hidden = false;
    host.innerHTML = recentHtml(rows, jobFilter, expanded);
    if (typeof document.querySelectorAll !== "function") return;
    document.querySelectorAll("[data-job-filter]").forEach((button) => {
      button.onclick = () => {
        jobFilter = button.dataset.jobFilter;
        expanded = false;
        paint();
      };
    });
    document.querySelectorAll("[data-job-more]").forEach((button) => {
      button.onclick = () => {
        expanded = true;
        paint();
      };
    });
  };
  paint();
}

async function paintStale(ctx, home, current) {
  let health;
  let datasets;
  try {
    [health, datasets] = await Promise.all([ctx.api("/api/health"), ctx.api("/api/datasets")]);
  } catch {
    const host = document.getElementById("ops-stale");
    if (current === generation && host) host.hidden = true;
    return;
  }
  const host = document.getElementById("ops-stale");
  const body = document.getElementById("ops-stale-body");
  if (current !== generation || !host || !body) return;
  host.hidden = false;
  body.innerHTML = staleHtml(staleRows(health, datasets), health.anchor || "", !home.mode.ops_enabled);
  if (typeof body.querySelectorAll !== "function") return;
  body.querySelectorAll("a.button[href^='#/ops?']").forEach((link) => {
    link.onclick = (event) => {
      const query = new URLSearchParams(link.getAttribute("href").split("?")[1] || "");
      const opId = query.get("op");
      if (!home.operations.some((card) => card.id === opId && card.available)) return;
      event.preventDefault();
      try {
        history.replaceState(null, "", link.getAttribute("href"));
      } catch {
        /* the form still opens */
      }
      showForm(ctx, home, opId, query, current);
    };
  });
}

async function paintMatrix(ctx, alive) {
  let matrix;
  try {
    matrix = await ctx.api("/api/runs/daily-matrix?sessions=14");
  } catch {
    const host = document.getElementById("ops-matrix");
    if (alive() && host) host.hidden = true;
    return;
  }
  const host = document.getElementById("ops-matrix");
  const body = document.getElementById("ops-matrix-body");
  if (!alive() || !host || !body) return;
  host.hidden = false;
  body.innerHTML = matrixHtml(matrix);
}

function bindOps(ctx, home, current) {
  const open = (opId) => {
    if (typeof document.querySelectorAll === "function") {
      document.querySelectorAll("[data-op]").forEach((item) => item.classList?.toggle("on", item.dataset.op === opId));
    }
    try {
      const next = `#/ops?op=${encodeURIComponent(opId)}`;
      if (!location.hash.includes(`op=${encodeURIComponent(opId)}`)) history.replaceState(null, "", next);
    } catch {
      /* the form still opens when the address cannot be updated */
    }
    showForm(ctx, home, opId, hashQuery(), current);
  };
  const quick = async (opId) => {
    const card = home.operations.find((item) => item.id === opId);
    const needsForm = !card || card.confirm !== "none" || (card.params || []).some((spec) => spec.required) || !home.mode.ops_enabled;
    if (needsForm) {
      open(opId);
      return;
    }
    let body;
    try {
      body = await ctx.api("/api/ops/preview", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CNE-CSRF": home.csrf_token },
        body: JSON.stringify({ op: card.id, params: {} }),
      });
    } catch (err) {
      open(opId);
      const target = document.getElementById("ops-preview");
      if (target && current === generation) target.innerHTML = `<p class="panel-note err">${ctx.esc(err.message)}</p>`;
      return;
    }
    if (current !== generation) return;
    if (!body.launch_token || (body.blockers || []).length || (body.acknowledgements || []).length) {
      open(opId);
      await previewCard(ctx, home, card, {}, current, document.getElementById("ops-preview"));
      return;
    }
    try {
      const job = await ctx.api("/api/ops/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CNE-CSRF": home.csrf_token },
        body: JSON.stringify({ preview_id: body.preview_id, launch_token: body.launch_token, acknowledged: [] }),
      });
      location.hash = `#/ops/jobs/${encodeURIComponent(job.job_id)}`;
    } catch (err) {
      open(opId);
      const target = document.getElementById("ops-preview");
      if (target && current === generation) target.innerHTML = `<p class="panel-note err">${ctx.esc(err.message)}</p>`;
    }
  };
  if (typeof document.querySelectorAll !== "function") return;
  document.querySelectorAll("[data-op]").forEach((button) => {
    button.onclick = () => open(button.dataset.op);
  });
  document.querySelectorAll("[data-op-quick]").forEach((button) => {
    button.onclick = () => { void quick(button.dataset.opQuick); };
  });
  document.querySelectorAll("[data-copy-command]").forEach((button) => {
    button.onclick = () => navigator.clipboard?.writeText(button.dataset.copyCommand || "");
  });
  for (const id of OPS_TABS) {
    const tab = document.getElementById(`ops-tab-${id}`);
    if (tab) tab.onclick = () => showOpsTab(id);
  }
}

// Dataset facts the form checks against (history mode), read once per page load.
let datasetFacts = null;
async function loadDatasetFacts(ctx) {
  if (datasetFacts) return datasetFacts;
  try {
    const rows = await ctx.api("/api/datasets");
    datasetFacts = new Map((rows || []).map((row) => [row.dataset, row]));
  } catch {
    return new Map();
  }
  return datasetFacts;
}

/**
 * What is wrong with the filled fields before anything is previewed. A
 * blocking problem stops the automatic preview; the others only warn.
 */
export function fieldProblems(opId, params, { today = "", facts = new Map() } = {}) {
  const problems = [];
  const start = String(params.start || "");
  const end = String(params.end || "");
  const date = String(params.trade_date || "");
  if (start && end && start > end) {
    problems.push({ blocking: true, text: tr(`起点 ${start} 晚于终点 ${end}。`, `The start ${start} is after the end ${end}.`) });
  }
  if (date && today && date > today) {
    problems.push({ blocking: true, text: tr(`交易日 ${date} 晚于今天 ${today}。`, `The trade date ${date} is after today, ${today}.`) });
  }
  if (end && today && end > today) {
    problems.push({ blocking: false, text: tr(`终点晚于今天 ${today}，之后的日期还没有数据。`, `The end is after today, ${today}; later dates have no data yet.`) });
  }
  const fact = params.dataset ? facts.get(params.dataset) : null;
  if (opId === "backfill.run" && fact?.history_mode === "snapshot_only" && (!start || !today || start < today)) {
    problems.push({
      blocking: false,
      text: tr(
        `${ds(params.dataset)}是快照数据集：源只给当天的数据，填旧的起点补不回漏掉的日子。要补今天，用「补抓落后」。`,
        `${ds(params.dataset)} is a snapshot dataset: the source only serves today, so an old start cannot recover missed days. To fetch today, use Catch up stale data.`,
      ),
    });
  }
  return problems;
}

export function mountOpForm(ctx, home, opId, preset, host, current, options = {}) {
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
  const note = opNote(card.id);
  const close = options.drawer
    ? `<button class="button button-ghost ops-drawer-close" id="ops-drawer-close" type="button" aria-label="${tr("关闭", "Close")}">${tr("关闭", "Close")}</button>`
    : "";
  host.innerHTML = `<section class="surface-panel report-panel ops-form">
    <div class="panel-header"><div><h2>${ctx.esc(pick(card, "title"))}</h2><p class="sub">${ctx.esc(pick(card, "summary"))}</p></div>${close}</div>
    ${note ? `<p class="panel-note" id="ops-op-note">${ctx.esc(note)}</p>` : ""}
    <ul class="ops-list ops-field-problems" id="ops-field-problems" hidden></ul>
    ${rangeNote}
    ${sessionText ? `<p class="panel-note" id="ops-session">${ctx.esc(sessionText)}</p>` : `<p class="panel-note" id="ops-session" hidden></p>`}
    <form id="ops-fields" class="ops-fields">${card.params.map((spec) => control(spec, preset)).join("")}
      ${home.mode.ops_enabled ? "" : `<div class="action-row"><button class="button button-ghost" type="submit" disabled>${tr("重新预览", "Preview again")}</button></div>`}
    </form>
    <div id="ops-preview"></div>
  </section>`;
  if (options.scroll !== false) host.scrollIntoView?.({ behavior: "smooth", block: "start" });
  const form = document.getElementById("ops-fields");
  const today = home.occupancy.schedule?.today || beijingDate(Date.now());
  let facts = datasetFacts || new Map();
  const problemsOf = (params) => fieldProblems(card.id, params, { today, facts });
  const blocked = (params) => problemsOf(params).some((item) => item.blocking);
  const paintProblems = (params) => {
    const list = document.getElementById("ops-field-problems");
    if (!list) return;
    const problems = problemsOf(params);
    list.hidden = !problems.length;
    list.innerHTML = problems.map((item) => `<li class="${item.blocking ? "err" : ""}">${ctx.esc(item.text)}</li>`).join("");
  };
  if (form && typeof form.querySelector === "function") {
    const paintSession = () => {
      const params = readForm(form, card.params);
      const text = formNoteText(home.occupancy.schedule || {}, card.id, params);
      paintProblems(params);
      const session = document.getElementById("ops-session");
      if (!session) return;
      session.hidden = !text;
      session.textContent = text;
    };
    form.addEventListener("input", paintSession);
    paintProblems(readForm(form, card.params));
    if (card.params.some((spec) => spec.name === "dataset") && !datasetFacts) {
      void loadDatasetFacts(ctx).then((loaded) => {
        if (current !== generation || document.getElementById("ops-fields") !== form) return;
        facts = loaded;
        paintProblems(readForm(form, card.params));
      });
    }
  }
  if (form) {
    form.onsubmit = async (event) => {
      event.preventDefault();
      if (typeof form.querySelector === "function" && blocked(readForm(form, card.params))) return;
      await preview(ctx, home, card, current);
    };
    if (home.mode.ops_enabled) autoPreview(form, card.params, () => preview(ctx, home, card, current), blocked);
  }
  return card;
}

function showForm(ctx, home, opId, preset, current, options = {}) {
  const card = home.operations.find((item) => item.id === opId);
  const host = document.getElementById("ops-form");
  if (!card || !host || current !== generation) return;
  if (typeof document.querySelectorAll === "function") {
    document.querySelectorAll("details.ops-more[open]").forEach((menu) => { menu.open = false; });
  }
  mountOpForm(ctx, home, opId, preset, host, current, { ...options, drawer: true, scroll: false });
  setDrawer(true);
  const close = document.getElementById("ops-drawer-close");
  if (close) close.onclick = closeDrawer;
  host.querySelector?.("input, select, button")?.focus?.();
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
  const ackItems = body.acknowledgements || [];
  const startLabel = ackItems.length ? pick(ackItems[0], "text") : tr("启动", "Start");
  target.innerHTML = `<p class="panel-note">${tr("将执行", "Will run")} <code>${esc(body.command)}</code></p>
    ${body.plan ? `<details class="ops-plan"><summary>${tr("查看执行计划", "Show the plan")}</summary><pre class="ops-log">${esc(body.plan)}</pre></details>` : ""}
    ${blockers ? `<ul class="ops-list err">${blockers}</ul>` : ""}
    ${hints ? `<ul class="ops-list">${hints}</ul>` : ""}
    <div class="action-row"><button class="button button-primary ops-start" data-ops-start type="button" ${body.launch_token ? "" : "disabled"}>${esc(startLabel)}</button></div>`;
  const button = typeof target.querySelector === "function" ? target.querySelector("[data-ops-start]") : document.getElementById("ops-start");
  if (!button) return;
  button.disabled = !body.launch_token;
  button.onclick = async () => {
    button.disabled = true;
    const acknowledged = ackItems.map((item) => item.id);
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

export function openJobStreams(jobId, ticket, { log, out, follow, onDone, onText } = {}) {
  const token = new URLSearchParams(location.search).get("token");
  const open = (which, node) => {
    const url = `/api/stream/ops/jobs/${encodeURIComponent(jobId)}?stream=${which}${token ? `&token=${encodeURIComponent(token)}` : ""}`;
    const source = new EventSource(url);
    streams.push(source);
    source.onmessage = (event) => {
      if (ticket !== generation) return source.close();
      let frame;
      try {
        frame = JSON.parse(event.data);
      } catch {
        return;
      }
      if (frame.text && node) {
        node.textContent += frame.text;
        if (onText) onText(which, node.textContent);
        if (!follow || follow()) node.scrollTop = node.scrollHeight;
      }
      if (frame.done) {
        source.close();
        if (onDone) onDone();
      }
    };
  };
  open("err", log);
  open("out", out);
}

function writtenCount(rows) {
  const number = Number(rows) || 0;
  if (number >= 10000) return new Intl.NumberFormat(locale(), { notation: "compact", maximumFractionDigits: 1 }).format(number);
  return number.toLocaleString(locale());
}

function elapsedText(detail) {
  if (!detail.started_at) return "";
  const start = Date.parse(detail.started_at);
  const end = detail.finished_at ? Date.parse(detail.finished_at) : Date.now();
  if (!Number.isFinite(start) || !Number.isFinite(end)) return "";
  const seconds = Math.max(0, Math.round((end - start) / 1000));
  if (seconds < 60) return tr(`耗时 ${seconds} 秒`, `${seconds}s`);
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  return tr(`耗时 ${minutes} 分 ${rest} 秒`, `${minutes}m ${rest}s`);
}

function batchProgress(detail) {
  const batches = detail.batches || [];
  const settled = batches.filter((batch) => batch.status === "success" || batch.status === "failed" || batch.status === "stale").length;
  const active = batches.filter((batch) => batch.status === "running").map((batch) => ds(batch.dataset));
  const rows = detail.rows_written ?? batches.reduce((sum, batch) => sum + (Number(batch.rows_written) || 0), 0);
  const parts = [
    tr(`${settled}/${batches.length} 个批次`, `${settled}/${batches.length} batches`),
    tr(`写入 ${writtenCount(rows)} 行`, `${writtenCount(rows)} rows written`),
  ];
  const elapsed = elapsedText(detail);
  if (elapsed) parts.push(elapsed);
  if (active.length) parts.push(tr(`进行中：${active.slice(0, 3).join("、")}`, `In progress: ${active.slice(0, 3).join(", ")}`));
  return parts.join(tr(" · ", " · "));
}

function stdoutSummary(text) {
  const raw = String(text || "");
  let payload = null;
  let cursor = raw.length;
  while (cursor > 0) {
    const at = raw.lastIndexOf("{", cursor - 1);
    if (at < 0) break;
    try {
      payload = JSON.parse(raw.slice(at));
      break;
    } catch {
      cursor = at;
    }
  }
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return "";
  const preferred = ["status", "rows", "row_count", "rows_written", "message", "datasets", "errors"];
  const keys = preferred.filter((key) => key in payload).concat(Object.keys(payload).filter((key) => !preferred.includes(key))).slice(0, 6);
  if (!keys.length) return "";
  const items = keys
    .map((key) => {
      const value = payload[key];
      const shown = value != null && typeof value === "object" ? JSON.stringify(value) : String(value ?? "");
      return `<li><span>${esc(key)}</span> ${esc(shown.slice(0, 160))}</li>`;
    })
    .join("");
  return `<ul class="ops-changes">${items}</ul>`;
}

function paintJobBoard(detail) {
  if (!detail || !Array.isArray(detail.batches)) return;
  const progress = document.getElementById("ops-progress");
  if (progress) progress.textContent = batchProgress(detail);
  const fill = document.getElementById("ops-meter-fill");
  if (fill && fill.style) {
    const batches = detail.batches || [];
    const settled = batches.filter((batch) => batch.status === "success" || batch.status === "failed" || batch.status === "stale").length;
    fill.style.width = `${batches.length ? Math.round((settled / batches.length) * 100) : 0}%`;
  }
  const root = document.getElementById("ops-run-board");
  if (!root) return;
  let board = root;
  if (detail.run_id && typeof root.appendChild === "function") {
    const id = `ops-run-${String(detail.run_id).replace(/[^\w-]/g, "_")}`;
    board = document.getElementById(id);
    if (!board) {
      board = document.createElement("div");
      board.id = id;
      root.appendChild(board);
    }
  }
  if (board && board.style) paintRunBoard(board, detail);
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
  const runItems = (runs) =>
    (runs || [])
      .map((run) => `<li><a href="#/runs/${encodeURIComponent(run.run_id)}">${esc(run.job_name)}</a> ${esc(run.status)}</li>`)
      .join("");
  setPage(
    `<section class="page-heading">
      <div class="heading-row"><div><h1>${esc(pick(job, "title") || job.op)}</h1><p class="meta-row"><span id="ops-job-state">${esc(pick(job, "label") || job.state)}</span><code>${esc(job.command || "")}</code></p></div>
      <div class="action-row"><a class="button button-ghost" href="#/ops">${tr("返回操作", "Operations")}</a>
        <button class="button button-danger" id="ops-cancel" type="button" ${job.state === "running" || job.state === "starting" ? "" : "disabled"}>${tr("取消", "Cancel")}</button></div></div></section>
    <p class="panel-note" id="ops-job-note">${esc(job.outcome?.message || "")}</p>
    <p class="panel-note" id="ops-next" ${jobNextStep(job) ? "" : "hidden"}>${jobNextStep(job)}</p>
    <div id="ops-readiness"></div>
    <section class="surface-panel report-panel" id="ops-runs">
      <div class="panel-header"><h2>${tr("关联运行", "Related runs")}</h2></div>
      <div class="ops-meter" id="ops-meter"><i id="ops-meter-fill"></i></div>
      <p class="panel-meta" id="ops-progress"></p>
      ${job.runs?.length ? "" : `<p class="muted" id="ops-runs-empty">${tr("还没有关联运行。", "No related runs yet.")}</p>`}
      <ul class="ops-jobs" id="ops-runs-list">${runItems(job.runs)}</ul>
      <div id="ops-run-board"></div>
    </section>
    <details class="surface-panel report-panel" id="ops-logs" ${job.state === "running" || job.state === "starting" ? "open" : ""}>
      <summary>${tr("终端日志", "Terminal logs")}</summary>
      <div class="ops-log-grid">
        <section>
          <div class="panel-header"><h2>${tr("进度", "Progress")}</h2><label class="ops-check"><input type="checkbox" id="ops-follow" checked> ${tr("跟随", "Follow")}</label></div>
          <pre class="ops-log" id="ops-log"></pre>
        </section>
        <section>
          <h2>${tr("命令输出", "Command output")}</h2>
          <div id="ops-out-summary"></div>
          <pre class="ops-log" id="ops-out"></pre>
        </section>
      </div>
    </details>`,
    "ops",
  );
  const seenRuns = new Set();
  async function watchRuns(runs) {
    for (const run of runs || []) {
      if (!run?.run_id || seenRuns.has(run.run_id)) continue;
      seenRuns.add(run.run_id);
      let detail = null;
      try {
        detail = await api(`/api/runs/${encodeURIComponent(run.run_id)}`);
      } catch {
        detail = null;
      }
      if (current !== generation) return;
      if (detail) paintJobBoard(detail);
      if (detail?.status && detail.status !== "running") continue;
      const source = new EventSource(`/api/stream/runs/${encodeURIComponent(run.run_id)}`);
      streams.push(source);
      source.onmessage = (event) => {
        if (current !== generation) return source.close();
        let frame;
        try {
          frame = JSON.parse(event.data);
        } catch {
          return;
        }
        paintJobBoard(frame);
        if (frame.status && frame.status !== "running") source.close();
      };
    }
  }
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
    const list = document.getElementById("ops-runs-list");
    if (list && latest.runs?.length) {
      list.innerHTML = runItems(latest.runs);
      const empty = document.getElementById("ops-runs-empty");
      if (empty) empty.hidden = true;
    }
    await watchRuns(latest.runs);
  }
  openJobStreams(jobId, current, {
    log: document.getElementById("ops-log"),
    out: document.getElementById("ops-out"),
    follow: () => document.getElementById("ops-follow")?.checked,
    onText: (which, text) => {
      if (which !== "out") return;
      const host = document.getElementById("ops-out-summary");
      const pre = document.getElementById("ops-out");
      if (!host) return;
      const summary = stdoutSummary(text);
      host.innerHTML = summary;
      if (pre) pre.hidden = Boolean(summary);
    },
    onDone: () => {
      void refreshJob();
    },
  });
  await watchRuns(job.runs);
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
    `<section class="page-heading"><div class="heading-row"><div><h1>${tr("建立数据湖", "Create the lake")}</h1><p class="meta-row"><span>${tr("只能在本机完成。初始化包含全市场行情主干。", "This machine only. Initialization includes the full-market price spine.")}</span></p></div></div></section>
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
