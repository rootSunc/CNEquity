import assert from "node:assert/strict";
import test from "node:test";
import { closeOps, renderOps } from "../src/ops.js";

function page(t) {
  const saved = new Map(["document", "location", "EventSource"].map((key) => [key, Object.getOwnPropertyDescriptor(globalThis, key)]));
  const nodes = new Map();
  const node = (id) => {
    if (!nodes.has(id)) nodes.set(id, { value: "", checked: false, disabled: false, dataset: {}, textContent: "", innerHTML: "" });
    return nodes.get(id);
  };
  globalThis.document = { getElementById: node, querySelectorAll: () => [] };
  globalThis.location = { search: "", hash: "#/ops" };
  const sources = [];
  globalThis.EventSource = class {
    constructor(url) { this.url = url; this.closed = false; sources.push(this); }
    close() { this.closed = true; }
  };
  t.after(() => {
    closeOps();
    for (const [key, descriptor] of saved) {
      if (descriptor) Object.defineProperty(globalThis, key, descriptor);
      else delete globalThis[key];
    }
  });
  return { node, sources, setPage() {}, esc: (value) => String(value ?? "") };
}

test("a completed job replays both log streams before closing them", async (t) => {
  const ctx = page(t);
  ctx.jobId = "finished-job";
  ctx.api = async (path) => path === "/api/ops"
    ? { csrf_token: "csrf" }
    : { state: "complete", title: "新鲜度", runs: [] };
  await renderOps(ctx);
  assert.equal(ctx.sources.length, 2);
  for (const [index, source] of ctx.sources.entries()) {
    assert.equal(source.closed, false, "history must not be closed before its first event");
    const text = index ? "historical output" : "historical progress";
    source.onmessage({ data: JSON.stringify({ text, offset: text.length }) });
    assert.equal(ctx.node(index ? "ops-out" : "ops-log").textContent, text);
    source.onmessage({ data: JSON.stringify({ text: "", done: true, state: "complete" }) });
    assert.equal(source.closed, true);
  }
});

for (const failure of ["invalid preview", "busy preview", "lost setup response"]) {
  test(`setup resumes after ${failure} without creating the config again`, async (t) => {
    const ctx = page(t);
    let configured = false;
    let creates = 0;
    let previews = 0;
    let launched;
    ctx.api = async (path, options) => {
      if (path === "/api/ops") return { mode: { setup: !configured }, csrf_token: "csrf" };
      if (path === "/api/setup/config") {
        creates++;
        assert.equal(configured, false, "config must be created only once");
        configured = true;
        if (failure === "lost setup response") throw new Error("connection lost");
        return {};
      }
      if (path === "/api/ops/preview") {
        previews++;
        const body = JSON.parse(options.body);
        assert.equal(body.op, "init.start");
        if (previews === 1 && failure === "invalid preview") throw new Error("历史起点不能晚于今天");
        if (previews === 1 && failure === "busy preview") return { launch_token: null, blockers: ["busy"] };
        assert.equal(body.params.since, "2024-06-01");
        return { preview_id: "preview", launch_token: "launch", command: "cne init --profile quick --since 2024-06-01" };
      }
      if (path === "/api/ops/jobs") {
        launched = JSON.parse(options.body);
        return { job_id: "init-job" };
      }
      throw new Error(`Unexpected API: ${path}`);
    };
    await renderOps(ctx);
    ctx.node("setup-root").value = "/isolated/data/cnequity";
    ctx.node("setup-profile").value = "quick";
    ctx.node("setup-ack").checked = true;
    ctx.node("setup-since").value = "2099-01-01";
    await ctx.node("setup-go").onclick();
    assert.equal(configured, true);
    assert.equal(launched, undefined);
    assert.equal(ctx.node("setup-go").disabled, false);
    ctx.node("setup-since").value = "2024-06-01";
    await ctx.node("setup-go").onclick();
    assert.equal(creates, 1);
    assert.equal(launched, undefined);
    assert.match(ctx.node("setup-command").textContent, /cne init --profile quick --since 2024-06-01/);
    assert.equal(ctx.node("setup-go").textContent, "开始初始化");
    await ctx.node("setup-go").onclick();
    assert.equal(creates, 1);
    assert.equal(ctx.node("setup-root").disabled, true);
    assert.deepEqual(launched.acknowledged, ["confirm", "heavy"]);
    assert.equal(globalThis.location.hash, "#/ops/jobs/init-job");
  });
}

test("an existing lake pauses on the takeover note before showing the command", async (t) => {
  const ctx = page(t);
  let creates = 0;
  let previews = 0;
  let launched;
  const hint = "这个目录里已经有数据湖，生成配置后会接管它，不会清空。";
  ctx.api = async (path, options) => {
    if (path === "/api/ops") return { mode: { setup: creates === 0 }, csrf_token: "csrf" };
    if (path === "/api/setup/config") {
      creates += 1;
      return { hint };
    }
    if (path === "/api/ops/preview") {
      previews += 1;
      return { preview_id: "preview", launch_token: "launch", command: "cne init --profile quick" };
    }
    if (path === "/api/ops/jobs") {
      launched = JSON.parse(options.body);
      return { job_id: "init-job" };
    }
    throw new Error(path);
  };
  await renderOps(ctx);
  ctx.node("setup-ack").checked = true;
  await ctx.node("setup-go").onclick();
  assert.equal(creates, 1);
  assert.equal(previews, 0);
  assert.equal(launched, undefined);
  assert.equal(ctx.node("setup-hint").textContent, hint);
  assert.equal(ctx.node("setup-hint").hidden, false);
  assert.equal(ctx.node("setup-root").disabled, true);
  assert.equal(ctx.node("setup-go").textContent, "预览初始化");
  await ctx.node("setup-go").onclick();
  assert.equal(creates, 1);
  assert.equal(previews, 1);
  assert.equal(launched, undefined);
  assert.match(ctx.node("setup-command").textContent, /cne init --profile quick/);
  assert.equal(ctx.node("setup-hint").textContent, hint);
  await ctx.node("setup-go").onclick();
  assert.equal(creates, 1);
  assert.deepEqual(launched.acknowledged, ["confirm", "heavy"]);
});

test("changing init parameters requires a new command preview", async (t) => {
  const ctx = page(t);
  let configured = false;
  const previews = [];
  let launched;
  ctx.api = async (path, options) => {
    if (path === "/api/ops") return { mode: { setup: !configured }, csrf_token: "csrf" };
    if (path === "/api/setup/config") {
      configured = true;
      return {};
    }
    if (path === "/api/ops/preview") {
      previews.push(JSON.parse(options.body).params);
      return { preview_id: "preview", launch_token: "launch", command: `cne init --since ${JSON.parse(options.body).params.since}` };
    }
    if (path === "/api/ops/jobs") {
      launched = JSON.parse(options.body);
      return { job_id: "init-job" };
    }
    throw new Error(path);
  };
  await renderOps(ctx);
  ctx.node("setup-ack").checked = true;
  ctx.node("setup-since").value = "2024-06-01";
  await ctx.node("setup-go").onclick();
  assert.equal(launched, undefined);
  ctx.node("setup-since").value = "2024-07-01";
  ctx.node("setup-since").onchange();
  assert.equal(ctx.node("setup-command").hidden, true);
  await ctx.node("setup-go").onclick();
  assert.equal(launched, undefined);
  assert.equal(previews.at(-1).since, "2024-07-01");
  await ctx.node("setup-go").onclick();
  assert.equal(launched.preview_id, "preview");
});

test("environment check holds initialization until it finishes", async (t) => {
  const ctx = page(t);
  let polls = 0;
  ctx.api = async (path) => {
    if (path === "/api/ops") return { mode: { setup: true }, csrf_token: "csrf" };
    if (path === "/api/ops/preview") return { preview_id: "preview", launch_token: "launch" };
    if (path === "/api/ops/jobs") return { job_id: "doc", state: "running" };
    if (path === "/api/ops/jobs/doc") {
      polls += 1;
      assert.equal(ctx.node("setup-go").dataset.doctor, "1");
      assert.equal(ctx.node("setup-go").disabled, true);
      return polls === 1
        ? { job_id: "doc", state: "running" }
        : { job_id: "doc", state: "complete", outcome: { message: "检查通过。" }, result: { summary: { findings: [] } } };
    }
    throw new Error(path);
  };
  await renderOps(ctx);
  ctx.node("setup-ack").checked = true;
  await ctx.node("setup-doctor").onclick();
  assert.equal(polls, 2);
  assert.equal(ctx.node("setup-go").dataset.blocked, "");
  assert.equal(ctx.node("setup-go").dataset.doctor, "");
  assert.equal(ctx.node("setup-go").disabled, false);
  assert.match(ctx.node("setup-note").textContent, /没有 error/);
});

test("environment findings keep initialization blocked", async (t) => {
  const ctx = page(t);
  ctx.api = async (path) => {
    if (path === "/api/ops") return { mode: { setup: true }, csrf_token: "csrf" };
    if (path === "/api/ops/preview") return { preview_id: "preview", launch_token: "launch" };
    if (path === "/api/ops/jobs") {
      return {
        job_id: "doc",
        state: "findings",
        outcome: { message: "检查未通过。" },
        result: { summary: { findings: [{ severity: "error", title: "缺依赖" }] } },
      };
    }
    throw new Error(path);
  };
  await renderOps(ctx);
  ctx.node("setup-ack").checked = true;
  await ctx.node("setup-doctor").onclick();
  assert.equal(ctx.node("setup-go").dataset.blocked, "1");
  assert.equal(ctx.node("setup-go").disabled, true);
  assert.match(ctx.node("setup-doctor-out").innerHTML, /缺依赖/);
  await ctx.node("setup-go").onclick();
  assert.match(ctx.node("setup-note").textContent, /先处理再初始化/);
});

// The scheduler uses the same minimal browser fixtures as the operation page.
import { renderSchedule } from "../src/schedule.js";

function scheduleState() {
  return { backend: "isolated", native: { available: true }, daily: false, stale: false,
    daily_run_at: "17:30", stale_run_at: "21:00", backup: false, backup_run_at: "23:00",
    backup_root: "/isolated/backups", backup_choices: ["daily_bars"], backup_datasets: [],
    events: false, events_groups: ["news"], events_interval_minutes: 60, note: "checks every minute" };
}

test("schedule only applies after preview and explicit confirmation", async (t) => {
  const ctx = page(t);
  const calls = [];
  ctx.api = async (path, options) => {
    calls.push([path, options && JSON.parse(options.body)]);
    if (path === "/api/ops/schedule") return scheduleState();
    if (path.endsWith("/preview")) return { token: "once", action: "启用", backend: "isolated", confirmation: "automatic fetch", artifact: "timer" };
    if (path.endsWith("/apply")) return {};
    throw new Error(path);
  };
  await renderSchedule(ctx, { mode: { ops_enabled: true }, csrf_token: "csrf" }, () => true);
  ctx.node("schedule-daily").checked = true;
  ctx.node("schedule-daily-at").value = "18:00";
  ctx.node("schedule-stale-at").value = "21:00";
  ctx.node("schedule-backup").checked = true;
  ctx.node("schedule-backup-at").value = "22:45";
  ctx.node("schedule-backup-root").value = "/external/backups";
  ctx.node("schedule-backup-datasets").selectedOptions = [{ value: "daily_bars" }];
  ctx.node("schedule-events").checked = true;
  ctx.node("schedule-events-group").value = "news";
  ctx.node("schedule-events-interval").value = "5";
  await ctx.node("schedule-fields").onsubmit({ preventDefault() {} });
  assert.equal(calls.some(([path]) => path.endsWith("/apply")), false);
  assert.deepEqual(calls.find(([path]) => path.endsWith("/preview"))[1], {
    daily: true, stale: false, daily_run_at: "18:00", stale_run_at: "21:00",
    backup: true, backup_run_at: "22:45", backup_root: "/external/backups", backup_datasets: ["daily_bars"],
    events: true, events_group: "news", events_interval_minutes: 5, daily_packs: [],
  });
  await ctx.node("schedule-apply").onclick();
  assert.deepEqual(calls.find(([path]) => path.endsWith("/apply"))[1], { token: "once", acknowledged: true });
  assert.equal(calls.filter(([path]) => path === "/api/ops/schedule").length, 2);
  assert.match(ctx.node("ops-schedule").innerHTML, /定时任务/);
});

test("editing schedule inputs invalidates an older confirmation", async (t) => {
  const ctx = page(t);
  let applied = false;
  ctx.api = async (path) => {
    if (path === "/api/ops/schedule") return scheduleState();
    if (path.endsWith("/preview")) return { token: "old", action: "启用" };
    applied = true;
  };
  await renderSchedule(ctx, { mode: { ops_enabled: true } }, () => true);
  await ctx.node("schedule-fields").onsubmit({ preventDefault() {} });
  const oldClick = ctx.node("schedule-apply").onclick;
  ctx.node("schedule-fields").oninput();
  await oldClick();
  assert.equal(applied, false);
  assert.equal(ctx.node("schedule-preview").innerHTML, "");
});

test("read-only schedule renders disabled controls", async (t) => {
  const ctx = page(t);
  ctx.api = async () => scheduleState();
  await renderSchedule(ctx, { mode: { ops_enabled: false } }, () => true);
  assert.match(ctx.node("ops-schedule").innerHTML, /type="submit" disabled/);
  assert.match(ctx.node("ops-schedule").innerHTML, /id="schedule-events-interval"[^>]*disabled/);
});

import { renderBackups } from "../src/backups.js";

function capture(ctx) {
  let html = "";
  ctx.setPage = (markup) => {
    html = markup;
  };
  return () => html;
}

function consoleApi(home, jobs = []) {
  return async (path) => {
    if (path === "/api/ops") return home;
    if (path.startsWith("/api/ops/jobs?") || path === "/api/ops/jobs") return jobs;
    if (path === "/api/ops/schedule") return scheduleState();
    if (path === "/api/ops/settings") return { note: "保存设置不会启动取数。", sections: [], push2_env_paused: false, push2_env_note: null };
    if (path.startsWith("/api/ops/backups")) return { root: "/isolated/backups", snapshots: [] };
    throw new Error(path);
  };
}

const dailyCard = (id, title) => ({
  id, title, summary: title, group: id.startsWith("daily") || id === "events.run" ? "日更" : "补抓与恢复",
  available: true,
  params: [
    { name: "trade_date", kind: "date", label: "交易日" },
    { name: "backfill", kind: "bool", label: "按补跑语义" },
  ],
});

test("ops status names the pending session and links an unfinished init", async (t) => {
  const ctx = page(t);
  const html = capture(ctx);
  ctx.api = consoleApi({
    csrf_token: "csrf",
    mode: { ops_enabled: true, label: "可发起取数", setup: false },
    operations: [
      { id: "init.start", title: "初始化数据湖", summary: "全市场", group: "初始化", available: true, params: [] },
      dailyCard("daily.full", "跑一天的更新"),
      { id: "daily.stale", title: "补抓落后的数据集", summary: "补抓", group: "补抓与恢复", available: true, params: [] },
    ],
    occupancy: {
      hints: [],
      lake_empty: true,
      incomplete_init: { run_id: "run/1", running: false },
      schedule: { daily_run_at: "17:30", daily_pending: "2026-09-30", daily_due: "2026-09-30", daily_done: false },
    },
  });
  await renderOps(ctx);
  const pageHtml = html();
  assert.match(pageHtml, /2026-09-30 的定时日更（17:30）已到点，还没跑。/);
  assert.match(pageHtml, /op=init.resume&run_id=run%2F1/);
  assert.match(pageHtml, /初始化数据湖/);
  const initAt = pageHtml.indexOf('data-op="init.start"');
  const detailsAt = pageHtml.indexOf("<details");
  const dailyAt = pageHtml.indexOf('data-op="daily.full"');
  assert.ok(initAt !== -1 && detailsAt !== -1 && dailyAt !== -1);
  assert.ok(initAt < detailsAt && detailsAt < dailyAt);
  assert.match(pageHtml, /ops-card-primary/);
  assert.match(pageHtml, /空湖上的日更和补抓不会补历史/);
});

test("a finished scheduled session names that trade date", async (t) => {
  const ctx = page(t);
  const html = capture(ctx);
  ctx.api = consoleApi({
    csrf_token: "csrf",
    mode: { ops_enabled: true, label: "可发起取数" },
    operations: [],
    occupancy: {
      hints: [],
      lake_empty: false,
      schedule: { daily_run_at: "17:30", daily_due: "2026-09-30", daily_done: true },
    },
  });
  await renderOps(ctx);
  assert.match(html(), /2026-09-30 的定时日更（17:30）已经跑过。/);
  assert.doesNotMatch(html(), /ops-secondary/);
  assert.doesNotMatch(html(), /没有需要先处理的占用/);
  assert.doesNotMatch(html(), /ops-side/);
  assert.match(html(), /id="ops-recent" hidden/);
  assert.equal(ctx.node("ops-recent").hidden, true);
  assert.equal(ctx.node("ops-recent").innerHTML, "");
  assert.match(html(), /id="ops-daily"[\s\S]*id="ops-timeline"[\s\S]*id="ops-schedule"/);
  assert.match(html(), /id="ops-status-session"/);
  assert.match(html(), /id="ops-tab-today"/);
  assert.match(html(), /id="ops-form" hidden/);
  assert.match(html(), /id="ops-commands-fold"/);
  assert.match(html(), /初始化已完成/);
});

test("a closed session does not offer run today, and a pending one names that date", async (t) => {
  const ctx = page(t);
  const html = capture(ctx);
  const home = {
    csrf_token: "csrf",
    mode: { ops_enabled: true, label: "可发起取数" },
    operations: [
      dailyCard("daily.full", "跑一天的更新"),
      { id: "daily.stale", title: "补抓落后的数据集", summary: "补抓", group: "补抓与恢复", available: true, params: [] },
      { id: "events.run", title: "跑事件流", summary: "事件", group: "日更", available: true, params: [] },
    ],
    occupancy: {
      hints: [],
      lake_empty: false,
      schedule: {
        today: "2026-10-05",
        today_is_session: false,
        daily_run_at: "17:30",
        daily_due: "2026-09-30",
        daily_done: true,
      },
    },
  };
  ctx.api = consoleApi(home);
  const daily = () => {
    const pageHtml = html();
    return pageHtml.slice(pageHtml.indexOf('id="ops-daily"'), pageHtml.indexOf('id="ops-timeline"'));
  };
  await renderOps(ctx);
  // The session sentence lives on the status card only, not again above the buttons.
  const card = () => html().slice(html().indexOf('id="ops-status-session"'), html().indexOf('id="ops-status-scheduler"'));
  assert.match(card(), /今天 2026-10-05 不是交易日。/);
  assert.match(card(), /2026-09-30 的定时日更（17:30）已经跑过。/);
  assert.doesNotMatch(daily(), /已经跑过/);
  assert.doesNotMatch(daily(), /data-op="daily.full"/);
  assert.match(daily(), /今天不是交易日，所以没有“现在跑今天”。/);
  assert.match(daily(), /data-op="daily.stale"/);
  assert.match(daily(), /data-op="events.run"/);
  home.occupancy.schedule.daily_pending = "2026-09-30";
  home.occupancy.schedule.daily_done = false;
  await renderOps(ctx);
  assert.match(daily(), /href="#\/ops\?op=daily.full&trade_date=2026-09-30"/);
  assert.match(daily(), /现在跑 2026-09-30/);
  assert.doesNotMatch(daily(), /data-op="daily.full"/);
  home.occupancy.schedule.today_is_session = true;
  delete home.occupancy.schedule.daily_pending;
  await renderOps(ctx);
  assert.match(daily(), /data-op="daily.full"/);
  assert.match(daily(), />现在跑今天</);
});

test("recent jobs stay in the main column and disappear when there are none", async (t) => {
  const ctx = page(t);
  ctx.api = consoleApi({
    csrf_token: "csrf",
    mode: { ops_enabled: true, label: "可发起取数" },
    operations: [],
    occupancy: { hints: [], lake_empty: false, schedule: {} },
  }, [
    { job_id: "job-1", title: "跑一天的更新", state: "succeeded", label: "成功", created_at: "2026-09-30T09:30:00Z" },
  ]);
  await renderOps(ctx);
  const recent = ctx.node("ops-recent");
  assert.equal(recent.hidden, false);
  assert.match(recent.innerHTML, /跑一天的更新/);
  assert.match(recent.innerHTML, /成功/);
  assert.doesNotMatch(recent.innerHTML, /ops-side/);
});

test("the daily form says whether this run counts as the scheduled session", async (t) => {
  const ctx = page(t);
  globalThis.location.hash = "#/ops?op=daily.full";
  ctx.api = consoleApi({
    csrf_token: "csrf",
    mode: { ops_enabled: true, label: "可发起取数" },
    operations: [dailyCard("daily.full", "跑一天的更新"), { ...dailyCard("daily.group", "跑一个调度组"), group: "日更" }],
    occupancy: {
      hints: [],
      lake_empty: false,
      schedule: {
        daily_run_at: "17:30",
        today: "2026-10-04",
        today_is_session: false,
        daily_pending: "2026-09-30",
        daily_due: "2026-09-30",
        daily_done: false,
        before_daily_run_at: true,
      },
    },
  });
  await renderOps(ctx);
  assert.match(ctx.node("ops-form").innerHTML, /今天 2026-10-04 不是交易日。/);
  assert.match(ctx.node("ops-form").innerHTML, /待跑的定时日更是 2026-09-30（17:30）。/);
  assert.match(ctx.node("ops-form").innerHTML, /这次手动运行不会记成定时日更。/);
  assert.match(ctx.node("ops-form").innerHTML, /不填日期会按今天提交。/);
  globalThis.location.hash = "#/ops?op=daily.full&trade_date=2026-09-30";
  await renderOps(ctx);
  assert.match(ctx.node("ops-form").innerHTML, /这次手动运行会计入定时日更 2026-09-30。/);
  globalThis.location.hash = "#/ops?op=daily.group";
  await renderOps(ctx);
  assert.match(ctx.node("ops-form").innerHTML, /这次不会写“定时日更已跑过”的标记。/);
});

test("intents open one start form in the drawer, and a range goes to one dataset", async (t) => {
  const ctx = page(t);
  const html = capture(ctx);
  ctx.api = consoleApi({
    csrf_token: "csrf",
    mode: { ops_enabled: true, label: "可发起取数" },
    operations: [
      { ...dailyCard("daily.full", "跑一天的更新"), command: "cne run daily [--trade-date TRADE_DATE]" },
      {
        id: "backfill.run",
        title: "回填一个数据集",
        summary: "回填",
        group: "定向补数",
        available: true,
        command: "cne backfill DATASET [--start START] [--end END]",
        params: [
          { name: "dataset", kind: "choice", label: "数据集", required: true, choices: ["daily_bars"] },
          { name: "start", kind: "date", label: "起点" },
          { name: "end", kind: "date", label: "终点" },
        ],
      },
    ],
    occupancy: { hints: [], lake_empty: false, schedule: {} },
  });
  const buttons = new Map();
  globalThis.document.querySelectorAll = (selector) => {
    if (selector !== "[data-op]") return [];
    const found = [...html().matchAll(/data-op="([^"]+)"/g)].map((match) => match[1]);
    return found.map((op) => {
      if (!buttons.has(op)) buttons.set(op, { dataset: { op } });
      return buttons.get(op);
    });
  };
  await renderOps(ctx);
  const pageHtml = html();
  assert.match(pageHtml, /补一段历史/);
  assert.match(pageHtml, /cne repair/);
  assert.match(pageHtml, /命令参考/);
  assert.equal(ctx.node("ops-form").innerHTML, "", "nothing opens until the reader picks an action");
  buttons.get("backfill.run").onclick();
  const fields = ctx.node("ops-form").innerHTML;
  assert.equal(ctx.node("ops-form").hidden, false);
  assert.match(fields, /起点/);
  assert.match(fields, /终点/);
  assert.match(fields, /daily_bars/);
  assert.match(fields, /一次只能回填一个数据集/);
  assert.match(fields, /id="ops-drawer-close"/);
  ctx.node("ops-drawer-close").onclick();
  assert.equal(ctx.node("ops-form").hidden, true);
  assert.equal(ctx.node("ops-form").innerHTML, "");
});

test("a live job shows batch progress from the run stream", async (t) => {
  const ctx = page(t);
  ctx.jobId = "job-live";
  const job = {
    state: "running",
    op: "daily.full",
    title: "跑一天的更新",
    command: "cne run daily",
    runs: [{ run_id: "run-1", job_name: "daily", status: "running" }],
  };
  const detail = {
    run_id: "run-1",
    status: "running",
    rows_written: 1200,
    batches: [
      { dataset: "daily_bars", status: "success", rows_written: 1000 },
      { dataset: "adj_factors", status: "running", rows_written: 200 },
    ],
  };
  ctx.api = async (path) => {
    if (path === "/api/ops") return { csrf_token: "csrf" };
    if (path === "/api/runs/run-1") return detail;
    if (path === "/api/ops/jobs/job-live") return job;
    throw new Error(path);
  };
  await renderOps(ctx);
  assert.match(ctx.node("ops-progress").textContent, /1\/2 个批次/);
  assert.match(ctx.node("ops-progress").textContent, /1,200/);
  const runSource = ctx.sources.find((source) => source.url.includes("/api/stream/runs/run-1"));
  assert.ok(runSource);
  detail.batches[1].status = "success";
  detail.rows_written = 1500;
  detail.status = "succeeded";
  runSource.onmessage({ data: JSON.stringify(detail) });
  assert.match(ctx.node("ops-progress").textContent, /2\/2 个批次/);
  assert.equal(runSource.closed, true);
});

test("a finished job offers the next step for that command", async (t) => {
  const ctx = page(t);
  const html = capture(ctx);
  const job = {
    state: "failed",
    op: "daily.full",
    title: "跑一天的更新",
    command: "cne run daily",
    outcome: { message: "执行失败。" },
    runs: [{ run_id: "run-9", job_name: "daily", status: "failed" }],
  };
  ctx.jobId = "job-1";
  ctx.api = async (path) => (path === "/api/ops" ? { csrf_token: "csrf" } : job);
  await renderOps(ctx);
  assert.match(html(), /重试这次 run/);
  assert.match(html(), /run_id=run-9/);
  job.state = "succeeded";
  job.op = "init.start";
  await renderOps(ctx);
  assert.match(html(), /href="#\/"/);
  assert.match(html(), /查看总览/);
  job.state = "partial";
  job.runs = [];
  job.op = "daily.stale";
  await renderOps(ctx);
  assert.match(html(), /重试失败的日更组/);
});

import { renderSettings } from "../src/settings.js";

function settingsState() {
  return {
    note: "保存设置不会启动取数。",
    push2_env_paused: true,
    push2_env_note: "本机环境变量开着，关掉配置无效。",
    sections: [
      {
        id: "sources",
        title: "数据源",
        settings: [
          { id: "push2_paused", kind: "bool", label: "暂停 push2", help: "停掉 push2", value: false },
          { id: "tdx", kind: "bool", label: "通达信", help: "日线主源", value: true },
        ],
      },
      {
        id: "universe",
        title: "日更覆盖",
        settings: [
          {
            id: "ingest",
            kind: "choice",
            label: "日更覆盖的证券",
            value: "all_a",
            choices: [
              { value: "all_a", label: "全部 A 股" },
              { value: "all_a_sh_sz", label: "不含北交所" },
            ],
          },
        ],
      },
    ],
  };
}

test("settings apply only after preview and confirmation", async (t) => {
  const ctx = page(t);
  const calls = [];
  ctx.api = async (path, options) => {
    calls.push([path, options && JSON.parse(options.body)]);
    if (path === "/api/ops/settings") return settingsState();
    if (path.endsWith("/preview")) {
      return {
        token: "once",
        note: "这次不会启动取数。",
        acknowledgement: "我确认保存这些设置。定时日更、收尾补抓和之后的命令都会按新值执行。",
        changes: [{ label: "暂停 push2", before_label: "关", after_label: "开" }],
      };
    }
    if (path.endsWith("/apply")) return { backup_name: "cnequity.toml.bak-1" };
    throw new Error(path);
  };
  await renderSettings(ctx, { mode: { ops_enabled: true }, csrf_token: "csrf" }, () => true);
  assert.match(ctx.node("ops-settings").innerHTML, /本机环境变量开着/);
  ctx.node("setting-push2_paused").checked = true;
  ctx.node("setting-tdx").checked = true;
  ctx.node("setting-ingest").value = "all_a_sh_sz";
  await ctx.node("settings-fields").onsubmit({ preventDefault() {} });
  assert.equal(calls.some(([path]) => path.endsWith("/apply")), false);
  assert.deepEqual(calls.find(([path]) => path.endsWith("/preview"))[1], {
    values: { push2_paused: true, tdx: true, ingest: "all_a_sh_sz" },
  });
  assert.match(ctx.node("settings-preview").innerHTML, /暂停 push2：关 → 开/);
  assert.match(ctx.node("settings-preview").innerHTML, /定时日更、收尾补抓和之后的命令都会按新值执行/);
  assert.match(ctx.node("settings-preview").innerHTML, /保存这 1 项改动/);
  assert.equal(ctx.node("settings-apply").disabled, false);
  await ctx.node("settings-apply").onclick();
  assert.deepEqual(calls.find(([path]) => path.endsWith("/apply"))[1], { token: "once", acknowledged: true });
  assert.match(ctx.node("ops-settings").innerHTML, /这次没有启动取数/);
  assert.match(ctx.node("ops-settings").innerHTML, /cnequity.toml.bak-1/);
});

test("editing settings inputs invalidates an older confirmation", async (t) => {
  const ctx = page(t);
  let applied = false;
  ctx.api = async (path) => {
    if (path === "/api/ops/settings") return settingsState();
    if (path.endsWith("/preview")) return { token: "old", acknowledgement: "确认", changes: [{ label: "通达信", before_label: "开", after_label: "关" }] };
    applied = true;
    return {};
  };
  await renderSettings(ctx, { mode: { ops_enabled: true }, csrf_token: "csrf" }, () => true);
  await ctx.node("settings-fields").onsubmit({ preventDefault() {} });
  const oldClick = ctx.node("settings-apply").onclick;
  ctx.node("settings-fields").oninput();
  await oldClick();
  assert.equal(applied, false);
  assert.equal(ctx.node("settings-preview").innerHTML, "");
});

test("read-only settings render disabled controls", async (t) => {
  const ctx = page(t);
  ctx.api = async () => settingsState();
  await renderSettings(ctx, { mode: { ops_enabled: false } }, () => true);
  assert.match(ctx.node("ops-settings").innerHTML, /type="submit" disabled/);
  assert.match(ctx.node("ops-settings").innerHTML, /id="setting-push2_paused"[^>]*disabled/);
  assert.match(ctx.node("ops-settings").innerHTML, /当前模式不能修改这些设置/);
});

test("each source switch shows today's state, requests and failed batches", async (t) => {
  const ctx = page(t);
  ctx.api = async () => settingsState();
  const sources = {
    push2_paused: true,
    breakers: [],
    requests: { push2: { requests: 3, budget: 150 } },
    keys: {},
    failures: { tdx_protocol: { batches: 2, datasets: ["daily_bars"] } },
  };
  await renderSettings(ctx, { mode: { ops_enabled: true }, occupancy: { sources } }, () => true);
  const html = ctx.node("ops-settings").innerHTML;
  assert.match(html, /tone-ok[^<]*<\/i>可用/);
  assert.match(html, /push2 今日请求 3\/150/);
  assert.match(html, /今日失败 2 批：/);
});

test("daily runs point at a dated daily_bars backfill and keep a single trade date", async (t) => {
  const ctx = page(t);
  const html = capture(ctx);
  globalThis.location.hash = "#/ops?op=daily.full";
  ctx.api = consoleApi({
    csrf_token: "csrf",
    mode: { ops_enabled: true, label: "可发起取数" },
    operations: [
      dailyCard("daily.full", "跑一天的更新"),
      {
        id: "backfill.run",
        title: "回填一个数据集",
        summary: "回填",
        group: "定向补数",
        available: true,
        params: [
          { name: "dataset", kind: "choice", label: "数据集", choices: ["daily_bars", "index_bars"], required: true },
          { name: "start", kind: "date", label: "起点" },
          { name: "end", kind: "date", label: "终点" },
        ],
      },
    ],
    occupancy: { hints: [], lake_empty: false, schedule: {} },
  });
  await renderOps(ctx);
  assert.match(ctx.node("ops-form").innerHTML, /#\/ops\?op=backfill.run&dataset=daily_bars/);
  assert.doesNotMatch(ctx.node("ops-form").innerHTML, /name="start"/);
  globalThis.location.hash = "#/ops?op=backfill.run&dataset=daily_bars";
  await renderOps(ctx);
  assert.match(ctx.node("ops-form").innerHTML, /value="daily_bars" selected/);
  assert.match(ctx.node("ops-form").innerHTML, /name="start"/);
  assert.match(ctx.node("ops-form").innerHTML, /name="end"/);
});

test("backup inventory only reads and links to job previews", async (t) => {
  const ctx = page(t);
  const calls = [];
  ctx.api = async (path, options) => {
    assert.equal(options, undefined);
    calls.push(path);
    return { root: "/external/backups", snapshots: [{ name: "baseline", datasets: ["daily_bars"], files: 2, bytes: 1024 }] };
  };
  await renderBackups(ctx, { mode: { ops_enabled: true } }, () => true);
  assert.match(ctx.node("ops-backups").innerHTML, /op=snapshot.restore/);
  assert.match(ctx.node("ops-backups").innerHTML, /待校验/);
  ctx.node("backup-root").value = "/another root";
  await ctx.node("backup-directory").onsubmit({ preventDefault() {} });
  assert.equal(calls.at(-1), "/api/ops/backups?root=%2Fanother%20root");
  await renderBackups(ctx, { mode: { ops_enabled: false } }, () => true);
  assert.doesNotMatch(ctx.node("ops-backups").innerHTML, /op=snapshot.restore/);
});

test("the today panel draws stale datasets, the daily matrix and today's timeline", async (t) => {
  const ctx = page(t);
  const base = consoleApi({
    csrf_token: "csrf",
    mode: { ops_enabled: true, label: "可发起取数" },
    operations: [{ id: "backfill.run", title: "回填一个数据集", summary: "回填", available: true, params: [] }],
    occupancy: { hints: [], lake_empty: false, schedule: { daily_run_at: "17:30" }, sources: { push2_paused: true, breakers: [] } },
  });
  const asked = [];
  ctx.api = async (path) => {
    asked.push(path);
    if (path === "/api/health") return { anchor: "2026-11-06", stale_datasets: ["valuation_metrics"] };
    if (path === "/api/datasets") return [{ dataset: "valuation_metrics", history_mode: "by_date", watermark: "2026-11-03" }];
    if (path.startsWith("/api/runs/daily-matrix")) {
      return { sessions: ["2026-11-06"], groups: ["core"], cells: [{ group: "core", trade_date: "2026-11-06", status: "failed", run_id: "r1", attempts: 1 }] };
    }
    return base(path);
  };
  await renderOps(ctx);
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.ok(asked.some((path) => /^\/api\/ops\/jobs\?limit=500&since=/.test(path)), "today's jobs are asked by time, not by count");
  assert.ok(asked.includes("/api/ops/jobs?limit=50&exclude_op=events.run"));
  assert.match(ctx.node("ops-stale-body").innerHTML, /落后 3 天/);
  assert.match(ctx.node("ops-stale-body").innerHTML, /op=backfill.run&amp;dataset=valuation_metrics|op=backfill.run&dataset=valuation_metrics/);
  assert.match(ctx.node("ops-matrix-body").innerHTML, /href="#\/runs\/r1"/);
  assert.match(ctx.node("ops-timeline-body").innerHTML, /<svg class="ops-timeline-svg"/);
});

