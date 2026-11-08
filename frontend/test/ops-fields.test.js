import assert from "node:assert/strict";
import test from "node:test";
import { fieldProblems } from "../src/ops.js";

test("a start after the end or a future trade date blocks the preview", () => {
  const range = fieldProblems("backfill.run", { dataset: "daily_bars", start: "2026-11-06", end: "2026-11-01" }, { today: "2026-11-09" });
  assert.equal(range.length, 1);
  assert.equal(range[0].blocking, true);
  assert.match(range[0].text, /晚于终点/);
  const future = fieldProblems("daily.full", { trade_date: "2026-11-12" }, { today: "2026-11-09" });
  assert.equal(future[0].blocking, true);
  assert.deepEqual(fieldProblems("daily.full", { trade_date: "2026-11-06" }, { today: "2026-11-09" }), []);
});

test("an old start on a snapshot dataset warns without blocking", () => {
  const facts = new Map([["hot_rank", { history_mode: "snapshot_only" }], ["daily_bars", { history_mode: "by_date" }]]);
  const snapshot = fieldProblems("backfill.run", { dataset: "hot_rank", start: "2026-10-01" }, { today: "2026-11-09", facts });
  assert.equal(snapshot.length, 1);
  assert.equal(snapshot[0].blocking, false);
  assert.match(snapshot[0].text, /快照数据集/);
  assert.deepEqual(fieldProblems("backfill.run", { dataset: "daily_bars", start: "2026-10-01" }, { today: "2026-11-09", facts }), []);
  const later = fieldProblems("backfill.run", { dataset: "daily_bars", end: "2026-12-01" }, { today: "2026-11-09", facts });
  assert.equal(later[0].blocking, false);
});
