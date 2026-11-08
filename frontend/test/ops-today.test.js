import assert from "node:assert/strict";
import test from "node:test";
import {
  beijingMidnight,
  digestJobs,
  jobReasons,
  matrixHtml,
  reasonText,
  recentHtml,
  rowMatches,
  staleHtml,
  staleRows,
  timelineModel,
  timelineSvg,
} from "../src/ops-today.js";

const eventJob = (id, created, state = "partial", datasets = ["news_headlines"]) => ({
  job_id: id,
  op: "events.run",
  title: "跑事件流",
  state,
  label: state === "partial" ? "部分交付" : "成功",
  created_at: created,
  started_at: created,
  finished_at: created,
  result: { summary: { fallback: datasets.map((dataset) => ({ dataset, reason_codes: ["source_transient"] })) } },
});

test("event streams fold into one row per Beijing day and keep other jobs visible", () => {
  const rows = digestJobs([
    eventJob("e1", "2026-11-08T21:44:00+00:00"),
    eventJob("e2", "2026-11-08T20:44:00+00:00", "succeeded", []),
    eventJob("e3", "2026-11-08T15:00:00+00:00"),
    { job_id: "d1", op: "daily.full", title: "跑一天的更新", state: "failed", label: "失败", created_at: "2026-11-08T09:30:00+00:00" },
    eventJob("e1", "2026-11-08T21:44:00+00:00"),
  ]);
  // 21:44Z and 20:44Z are 2026-11-09 in Beijing; 15:00Z is still 11-08.
  assert.deepEqual(rows.map((row) => row.kind === "events" ? `events ${row.day} ${row.jobs.length}` : row.job.job_id), [
    "events 2026-11-09 2",
    "events 2026-11-08 1",
    "d1",
  ]);
  assert.deepEqual(rows[0].tally, { warn: 1, ok: 1 });
  assert.equal(rows[0].missing.get("news_headlines").count, 1);
  assert.equal(rowMatches(rows[0], "problem"), true);
  assert.equal(rowMatches(rows[2], "daily"), true);
  assert.equal(rowMatches(rows[2], "events"), false);
  const html = recentHtml(rows, "daily");
  assert.match(html, /跑一天的更新/);
  assert.doesNotMatch(html, /事件流 · 2026-11-09/);
  assert.match(recentHtml(rows), /事件流 · 2026-11-09 · 2 次/);
  assert.match(recentHtml(rows), /常缺：.+×1/);
});

test("a partial job names the datasets it left short and why", () => {
  const job = eventJob("e1", "2026-11-08T21:44:00+00:00", "partial", ["flash_news_wire", "news_headlines"]);
  assert.deepEqual(jobReasons(job).map((item) => item.dataset), ["flash_news_wire", "news_headlines"]);
  assert.match(reasonText(jobReasons(job)), /数据源暂时不可用/);
  assert.equal(reasonText(jobReasons({ result: null })), "");
});

test("the timeline places today's jobs by lane and marks the scheduled times", () => {
  const now = Date.parse("2026-11-09T10:00:00+08:00");
  assert.equal(new Date(beijingMidnight(now)).toISOString(), "2026-11-08T16:00:00.000Z");
  const model = timelineModel(
    [
      { job_id: "y", op: "daily.full", state: "succeeded", created_at: "2026-11-07T09:30:00+00:00", started_at: "2026-11-07T09:30:00+00:00", finished_at: "2026-11-07T10:30:00+00:00" },
      { job_id: "a", op: "daily.full", state: "running", created_at: "2026-11-09T09:00:00+08:00", started_at: "2026-11-09T09:00:00+08:00" },
      eventJob("b", "2026-11-09T08:00:00+08:00"),
    ],
    { daily_run_at: "17:30", stale_run_at: "21:00", stale: true, backup: false, backup_run_at: "23:00" },
    now,
  );
  const daily = model.lanes.find((lane) => lane.id === "daily");
  assert.equal(daily.blocks.length, 1, "yesterday's job is not on today's line");
  assert.equal(daily.blocks[0].x0, 540);
  assert.equal(daily.blocks[0].x1, 600, "a running job reaches now");
  assert.equal(model.lanes.find((lane) => lane.id === "events").blocks[0].tone, "warn");
  assert.deepEqual(model.marks.map((mark) => mark.at), ["17:30", "21:00"]);
  assert.equal(model.now, 600);
  const svg = timelineSvg(model);
  assert.match(svg, /href="#\/ops\/jobs\/a"/);
  assert.match(svg, /日更 17:30/);
  assert.doesNotMatch(svg, /23:00<\/text>.*备份/);
});

test("stale datasets sort by lag and offer the action that can fix them", () => {
  const rows = staleRows(
    { anchor: "2026-11-06", stale_datasets: ["news_headlines", "valuation_metrics"] },
    [
      { dataset: "news_headlines", history_mode: "snapshot_only", watermark: "2026-11-04" },
      { dataset: "valuation_metrics", history_mode: "by_date", watermark: "2026-11-03" },
    ],
  );
  assert.deepEqual(rows.map((row) => [row.dataset, row.lag]), [["valuation_metrics", 3], ["news_headlines", 2]]);
  assert.equal(rows[0].action.href, "#/ops?op=backfill.run&dataset=valuation_metrics&start=2026-11-04&end=2026-11-06");
  assert.equal(rows[1].action.op, "daily.stale");
  const html = staleHtml(rows, "2026-11-06");
  assert.match(html, /落后 3 天/);
  assert.match(html, /快照：漏掉的日子补不回来/);
  assert.doesNotMatch(staleHtml(rows, "", true), /class="button/);
  assert.match(staleHtml([]), /没有落后的数据集/);
});

test("the daily matrix colours each group's latest attempt and links its run", () => {
  const html = matrixHtml({
    sessions: ["2026-11-05", "2026-11-06"],
    groups: ["core", "capital"],
    cells: [
      { group: "core", trade_date: "2026-11-05", status: "success", run_id: "r1", attempts: 1 },
      { group: "core", trade_date: "2026-11-06", status: "failed", run_id: "r2", attempts: 2 },
      { group: "capital", trade_date: "2026-11-06", status: "degraded", run_id: "r3", attempts: 1 },
    ],
  });
  assert.match(html, />11-05</);
  assert.match(html, /tone-bad retried" href="#\/runs\/r2"/);
  assert.match(html, /tone-warn" href="#\/runs\/r3"/);
  assert.match(html, /capital · 2026-11-05 · 没有运行/);
  assert.match(matrixHtml({ sessions: [], groups: [], cells: [] }), /还没有带交易日的日更记录/);
});

test("yesterday's timeline holds the evening's daily update and no now line", () => {
  const now = Date.parse("2026-11-09T07:00:00+08:00");
  const evening = { job_id: "y", op: "daily.full", state: "partial", created_at: "2026-11-08T17:30:00+08:00", started_at: "2026-11-08T17:30:00+08:00", finished_at: "2026-11-08T19:30:00+08:00" };
  assert.equal(timelineModel([evening], {}, now).lanes.find((lane) => lane.id === "daily").blocks.length, 0);
  const model = timelineModel([evening], { daily_run_at: "17:30" }, now, 1);
  assert.equal(model.day, "2026-11-08");
  assert.deepEqual(model.lanes.find((lane) => lane.id === "daily").blocks.map((block) => [block.x0, block.x1]), [[1050, 1170]]);
  assert.doesNotMatch(timelineSvg(model), /tl-now/);
});

test("a job row shows rows written, the datasets its runs failed, and links its run", () => {
  const html = recentHtml(digestJobs([{
    job_id: "s1",
    op: "daily.stale",
    title: "补抓落后的数据集",
    state: "failed",
    label: "失败",
    created_at: "2026-11-08T06:19:00+00:00",
    rows_written: 14936675,
    failed_datasets: [{ dataset: "hot_rank", reason_codes: ["source_transient"] }],
    result: { run_ids: ["r1"] },
  }]));
  assert.match(html, /写入 .+ 行/);
  assert.match(html, /失败：.+：数据源暂时不可用/);
  assert.match(html, /href="#\/runs\/r1"/);
});
