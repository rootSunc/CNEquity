// ECharts, registered piece by piece rather than pulled in whole: the prebuilt
// bundle is 1.1MB and half of it is chart types this dashboard never draws.
import * as echarts from "echarts/core";
import { BarChart, CustomChart, HeatmapChart } from "echarts/charts";
import {
  DataZoomComponent,
  GridComponent,
  LegendComponent,
  TooltipComponent,
  VisualMapComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

echarts.use([
  BarChart,
  CustomChart,
  HeatmapChart,
  DataZoomComponent,
  GridComponent,
  LegendComponent,
  TooltipComponent,
  VisualMapComponent,
  CanvasRenderer,
]);

const css = (name) =>
  getComputedStyle(document.documentElement).getPropertyValue(name).trim();

/** Chart-wide text and axis tokens, read from the page so both themes follow. */
function tokens() {
  return {
    ink: css("--fg"),
    muted: css("--muted"),
    line: css("--line"),
    surface: css("--card"),
    covered: css("--cell-covered"),
    gap: css("--cell-gap"),
    cadence: css("--cell-cadence"),
    outside: css("--cell-outside"),
    series: [1, 2, 3, 4, 5].map((i) => css(`--series-${i}`)),
    other: css("--series-other"),
  };
}

const charts = new Map();

/** The 640px breakpoint from styles.css, asked once instead of spelled out at
 * every chart that needs it: below it the rail collapses and the panels lose
 * their padding, so the axis gutter has to come down with them. */
const COMPACT = window.matchMedia("(max-width: 640px)");
const compact = () => COMPACT.matches;

/** Mount (or replace) a chart on *el*, keeping one instance per element.
 *
 * `build` is kept, not just the option it returns. The gutter is a function of
 * the breakpoint, and `chart.resize()` only re-lays out the option the chart
 * already holds — so a chart drawn wide keeps a 200px label gutter on a
 * phone-sized window until something else redraws it. */
function mount(el, build) {
  const existing = charts.get(el);
  if (existing) existing.chart.dispose();
  const chart = echarts.init(el, null, { renderer: "canvas" });
  chart.setOption(build());
  charts.set(el, { chart, build });
  return chart;
}

export function disposeAll() {
  for (const { chart } of charts.values()) chart.dispose();
  charts.clear();
}

window.addEventListener("resize", () => {
  for (const { chart } of charts.values()) chart.resize();
});

// Only on the transition, not on every resize frame: rebuilding is not free,
// and it has to be a replace rather than a merge — a merge would leave the
// compact axisLabel's `width` behind on the way back up — which resets the
// heatmap's dataZoom. Crossing the breakpoint is rare enough to pay that.
COMPACT.addEventListener("change", () => {
  for (const { chart, build } of charts.values()) chart.setOption(build(), true);
});

// --- coverage heatmap --------------------------------------------------------

// The cell alphabet the API sends, mapped to the codes visualMap pieces on.
const CELL_CODE = { " ": 0, "#": 1, ".": 2, "-": 4 };

/**
 * Dataset x trading-day coverage.
 *
 * `.` splits into two codes on the row's `gap_meaning`. The server decides
 * which. Whether a hole is a fault or the dataset's shape is a question about
 * fetch semantics and cadence, and that lives in the registry, not here.
 */
export function heatmap(el, data) {
  const t = tokens();
  const days = data.days;
  const rows = data.rows;
  const points = [];
  rows.forEach((row, y) => {
    [...row.cells].forEach((ch, x) => {
      let code = CELL_CODE[ch] ?? 0;
      if (code === 2 && row.gap_meaning === "cadence") code = 3;
      points.push([x, y, code]);
    });
  });

  const labels = {
    0: "覆盖区间外",
    1: "有分区覆盖",
    2: "缺口（日更 by_date 源真的少了一天）",
    3: "属其形态的间隔（非日更，或 snapshot 无法诚实补全）",
    4: "无分区（单文件 merge）",
  };

  // Show ~90 days at a time however long the window is, so the cells keep a
  // legible size instead of collapsing into a smear.
  const windowEnd = 100;
  const windowStart = Math.max(0, 100 - (90 / Math.max(days.length, 1)) * 100);

  el.style.height = `${Math.max(220, rows.length * 15 + 90)}px`;
  return mount(el, () => ({
    animation: false,
    grid: { left: compact() ? 118 : 200, right: compact() ? 8 : 20, top: 10, bottom: 62, containLabel: false },
    tooltip: {
      backgroundColor: t.surface,
      borderColor: t.line,
      textStyle: { color: t.ink, fontSize: 12 },
      formatter: (p) => {
        const row = rows[p.value[1]];
        const cadence = row.cadence_days > 1 ? `<br>容忍 ${row.cadence_days} 天` : "";
        return `<b>${row.dataset}</b><br>${days[p.value[0]]}<br>${labels[p.value[2]]}`
          + `<br>粒度 ${row.granularity || "merge"}${cadence}`;
      },
    },
    xAxis: {
      type: "category",
      data: days,
      axisLine: { lineStyle: { color: t.line } },
      axisTick: { show: false },
      axisLabel: { color: t.muted, fontSize: 10, hideOverlap: true },
      splitArea: { show: false },
    },
    yAxis: {
      type: "category",
      data: rows.map((r) => r.dataset),
      inverse: true,
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: compact()
        ? { color: t.muted, fontSize: 9, width: 105, overflow: "truncate" }
        : { color: t.muted, fontSize: 11 },
    },
    visualMap: {
      type: "piecewise",
      show: false,
      pieces: [
        { value: 0, color: t.outside },
        { value: 1, color: t.covered },
        { value: 2, color: t.gap },
        { value: 3, color: t.cadence },
        { value: 4, color: t.outside },
      ],
    },
    dataZoom: [
      { type: "inside", xAxisIndex: 0, start: windowStart, end: windowEnd },
      {
        type: "slider",
        xAxisIndex: 0,
        start: windowStart,
        end: windowEnd,
        height: 18,
        bottom: 14,
        borderColor: t.line,
        fillerColor: "transparent",
        handleStyle: { color: t.muted },
        textStyle: { color: t.muted, fontSize: 10 },
      },
    ],
    series: [
      {
        type: "heatmap",
        data: points,
        // 1px of surface between cells so adjacent states never bleed together.
        itemStyle: { borderColor: t.surface, borderWidth: 1 },
        progressive: 0,
      },
    ],
  }));
}

// --- provenance over time ----------------------------------------------------

/**
 * Source mix as it moved, stacked.
 *
 * Sources take colour slots by name, not by row count: a source that happens to
 * grow must not repaint the chart. Past five they fold into one neutral
 * "other" rather than cycling hues onto a sixth generated colour.
 */
export function provenanceSeries(el, data) {
  const t = tokens();
  const points = data.points;
  if (!points.length) {
    el.innerHTML = '<p class="muted">没有溯源度量。先跑 <code>cne stats rebuild</code>。</p>';
    return null;
  }

  const periods = [...new Set(points.map((p) => p.period_start))].sort();
  const named = [...new Set(points.map((p) => p.source))].sort();
  const shown = named.slice(0, t.series.length);
  const folded = named.slice(t.series.length);
  const keys = folded.length ? [...shown, "其他"] : shown;

  const bucket = new Map();
  for (const p of points) {
    const key = shown.includes(p.source) ? p.source : "其他";
    bucket.set(`${p.period_start}|${key}`, (bucket.get(`${p.period_start}|${key}`) || 0) + p.row_count);
  }

  el.style.height = "230px";
  return mount(el, () => ({
    animation: false,
    grid: { left: 62, right: 16, top: 12, bottom: 48 },
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      backgroundColor: t.surface,
      borderColor: t.line,
      textStyle: { color: t.ink, fontSize: 12 },
      valueFormatter: (v) => (v ? v.toLocaleString() + " 行" : "-"),
    },
    legend: {
      data: keys,
      bottom: 0,
      textStyle: { color: t.muted, fontSize: 11 },
      itemWidth: 10,
      itemHeight: 10,
      icon: "roundRect",
    },
    xAxis: {
      type: "category",
      data: periods,
      axisLine: { lineStyle: { color: t.line } },
      axisTick: { show: false },
      axisLabel: { color: t.muted, fontSize: 10, hideOverlap: true },
    },
    yAxis: {
      type: "value",
      splitLine: { lineStyle: { color: t.line } },
      axisLabel: {
        color: t.muted,
        fontSize: 10,
        formatter: (v) => (v >= 1e6 ? `${v / 1e6}M` : v >= 1e3 ? `${v / 1e3}k` : v),
      },
    },
    series: keys.map((key, i) => ({
      name: key,
      type: "bar",
      stack: "rows",
      color: key === "其他" ? t.other : t.series[i],
      // 2px of surface between stacked segments so adjacent hues never touch.
      itemStyle: { borderColor: t.surface, borderWidth: 1 },
      barMaxWidth: 40,
      data: periods.map((d) => bucket.get(`${d}|${key}`) || 0),
    })),
  }));
}

// --- run gantt ---------------------------------------------------------------

const BATCH_COLORS = {
  success: "--cell-covered",
  running: "--series-1",
  failed: "--cell-gap",
  stale: "--series-4",
};

/**
 * One run's batches on a time axis, grouped into a lane per dataset.
 *
 * A batch that is still running has no `finished_at`, so its bar is drawn to
 * "now", which is what makes the chart readable while a job is in flight, and
 * why it is redrawn on every stream frame rather than only at the end.
 */
export function runGantt(el, detail) {
  const t = tokens();
  const batches = detail.batches.filter((b) => b.started_at);
  if (!batches.length) {
    el.innerHTML = '<p class="muted">这个 run 还没有 batch 记录。</p>';
    return null;
  }

  const lanes = [...new Set(batches.map((b) => b.dataset))].sort();
  const laneIndex = new Map(lanes.map((d, i) => [d, i]));
  const now = Date.now();
  const ms = (s) => (s ? new Date(s).getTime() : null);

  const data = batches.map((b) => {
    const start = ms(b.started_at);
    const end = ms(b.finished_at) ?? now;
    return {
      value: [laneIndex.get(b.dataset), start, end, b],
      itemStyle: {
        color: css(BATCH_COLORS[b.status] ?? "--empty"),
        // A retried batch is not the same event as a clean one; outline it
        // rather than hiding the retry in a tooltip nobody opens.
        borderColor: b.retry_count ? css("--stale") : "transparent",
        borderWidth: b.retry_count ? 2 : 0,
        // A stalled batch is still 'running' in the manifest but nothing is
        // moving; hatch it so it does not read as healthy progress.
        decal: b.stalled ? { symbol: "line", rotation: 0.8, color: css("--bg") } : undefined,
      },
    };
  });

  el.style.height = `${Math.max(180, lanes.length * 26 + 80)}px`;
  return mount(el, () => ({
    animation: false,
    grid: { left: compact() ? 108 : 170, right: compact() ? 8 : 24, top: 12, bottom: 40 },
    tooltip: {
      backgroundColor: t.surface,
      borderColor: t.line,
      textStyle: { color: t.ink, fontSize: 12 },
      formatter: (p) => {
        const b = p.value[3];
        const secs = Math.round((( ms(b.finished_at) ?? now) - ms(b.started_at)) / 1000);
        const lines = [
          `<b>${b.dataset}</b> · ${b.status}${b.stalled ? " · 静默" : ""}`,
          `${secs}s${b.finished_at ? "" : "（进行中）"}`,
          `写入 ${(b.rows_written ?? 0).toLocaleString()} 行`,
        ];
        if (b.retry_count) lines.push(`重试 ${b.retry_count} 次`);
        if (b.window_start) lines.push(`窗口 ${b.window_start} → ${b.window_end}`);
        if (b.error_message) lines.push(`<span style="opacity:.8">${b.error_message.slice(0, 120)}</span>`);
        return lines.join("<br>");
      },
    },
    xAxis: {
      type: "time",
      axisLine: { lineStyle: { color: t.line } },
      axisLabel: { color: t.muted, fontSize: 10 },
      splitLine: { lineStyle: { color: t.line } },
    },
    yAxis: {
      type: "category",
      data: lanes,
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: compact()
        ? { color: t.muted, fontSize: 9, width: 96, overflow: "truncate" }
        : { color: t.muted, fontSize: 11 },
    },
    series: [
      {
        type: "custom",
        renderItem: (params, api) => {
          const lane = api.value(0);
          const start = api.coord([api.value(1), lane]);
          const end = api.coord([api.value(2), lane]);
          const height = Math.min(api.size([0, 1])[1] * 0.55, 18);
          const rect = echarts.graphic.clipRectByRect(
            { x: start[0], y: start[1] - height / 2, width: Math.max(end[0] - start[0], 2), height },
            {
              x: params.coordSys.x,
              y: params.coordSys.y,
              width: params.coordSys.width,
              height: params.coordSys.height,
            },
          );
          return rect && { type: "rect", shape: { ...rect, r: 2 }, style: api.style() };
        },
        encode: { x: [1, 2], y: 0 },
        data,
      },
    ],
  }));
}

// --- quality timeline --------------------------------------------------------

// Status colours are reserved: they mean severity here and are never reused as
// "series 4". Each ships with its name in the legend, so severity is never
// carried by colour alone.
const SEVERITY = [
  ["error", "--cell-gap"],
  ["warning", "--series-4"],
  ["info", "--empty"],
];

/** Findings per audited trade date, stacked by severity. */
export function severityTimeline(el, runs) {
  const t = tokens();
  if (!runs.length) {
    el.innerHTML = '<p class="muted">还没有审计产物。请跑一次 <code>cne audit</code>。</p>';
    return null;
  }
  // Oldest first: a timeline that reads right-to-left is a timeline nobody reads.
  const ordered = [...runs].reverse();
  const labels = ordered.map((r) => r.trade_date || r.run_id.slice(0, 8));

  el.style.height = "220px";
  return mount(el, () => ({
    animation: false,
    grid: { left: 52, right: 16, top: 12, bottom: 48 },
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" },
      backgroundColor: t.surface, borderColor: t.line,
      textStyle: { color: t.ink, fontSize: 12 } },
    legend: { data: SEVERITY.map(([name]) => name), bottom: 0, icon: "roundRect",
      itemWidth: 10, itemHeight: 10, textStyle: { color: t.muted, fontSize: 11 } },
    xAxis: { type: "category", data: labels,
      axisLine: { lineStyle: { color: t.line } }, axisTick: { show: false },
      axisLabel: { color: t.muted, fontSize: 10, hideOverlap: true } },
    yAxis: { type: "value", splitLine: { lineStyle: { color: t.line } },
      axisLabel: { color: t.muted, fontSize: 10 } },
    series: SEVERITY.map(([name, token]) => ({
      name,
      type: "bar",
      stack: "sev",
      color: css(token),
      itemStyle: { borderColor: t.surface, borderWidth: 1 },
      barMaxWidth: 44,
      data: ordered.map((r) => r.by_severity[name] || 0),
    })),
  }));
}
