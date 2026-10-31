// Page language for the serve console. Dataset ids stay the command and API
// names; the page shows a localized title beside that id.

const KEY = "cne-lang";

// No choice stored yet: follow the browser, as the CLI follows the system
// locale. Simplified Chinese reads Chinese; anything else reads English.
export function browserLang(languages = globalThis.navigator?.languages ?? [globalThis.navigator?.language]) {
  for (const tag of languages || []) {
    if (!tag) continue;
    const value = String(tag).toLowerCase().replace("_", "-");
    if (value === "zh" || value.startsWith("zh-cn") || value.startsWith("zh-sg") || value.startsWith("zh-hans")) return "zh";
    return "en";
  }
  return "en";
}

function storedLang() {
  try {
    const value = globalThis.localStorage?.getItem(KEY);
    return value === "en" || value === "zh" ? value : browserLang();
  } catch {
    return browserLang();
  }
}

let lang = storedLang();
const listeners = new Set();

function applyDocumentLang() {
  const root = globalThis.document?.documentElement;
  if (root) root.lang = lang === "en" ? "en" : "zh-CN";
}

applyDocumentLang();

export function getLang() {
  return lang;
}

export function locale() {
  return lang === "en" ? "en" : "zh-CN";
}

export function tr(zh, en) {
  return lang === "en" ? en : zh;
}

// Server payloads carry their display text in Chinese under `key` and, where a
// translation exists, in English under `key_en`. Falls back to the Chinese.
export function pick(obj, key) {
  if (!obj) return "";
  return lang === "en" ? (obj[`${key}_en`] ?? obj[key]) : obj[key];
}

export function setLang(next) {
  const value = next === "en" ? "en" : "zh";
  if (value === lang) return;
  lang = value;
  try {
    globalThis.localStorage?.setItem(KEY, lang);
  } catch {
    /* private browsing still switches the current page */
  }
  applyDocumentLang();
  for (const listener of listeners) listener(lang);
}

export function onLanguageChange(listener) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

const DATASETS = {
  instruments: ["证券列表", "Instruments"],
  etf_profiles: ["ETF 档案", "ETF profiles"],
  trading_calendar: ["交易日历", "Trading calendar"],
  trading_status: ["交易状态", "Trading status"],
  daily_bars: ["日线", "Daily bars"],
  index_bars: ["指数行情", "Index bars"],
  minute_bars: ["1 分钟线", "1-minute bars"],
  minute_bars_5m: ["5 分钟线", "5-minute bars"],
  minute_bars_15m: ["15 分钟线", "15-minute bars"],
  minute_bars_30m: ["30 分钟线", "30-minute bars"],
  minute_bars_60m: ["60 分钟线", "60-minute bars"],
  trade_ticks: ["分笔", "Trade ticks"],
  adj_factors: ["复权因子", "Adjustment factors"],
  delisting_events: ["退市事件", "Delisting events"],
  corporate_actions: ["公司行为", "Corporate actions"],
  announcement_index: ["公告索引", "Announcements"],
  earnings_disclosure_schedule: ["预约披露", "Earnings schedule"],
  financial_statement_items: ["财务报表", "Financial statements"],
  valuation_metrics: ["估值指标", "Valuation"],
  analyst_consensus: ["一致预期", "Analyst consensus"],
  share_structure: ["股本结构", "Share structure"],
  shareholder_counts: ["股东户数", "Shareholder counts"],
  top_holders: ["十大股东", "Top holders"],
  fund_flow: ["资金流向", "Fund flow"],
  fund_flow_ths: ["同花顺资金流", "THS fund flow"],
  margin_trading: ["融资融券", "Margin trading"],
  northbound_holdings: ["北向持股", "Northbound holdings"],
  northbound_flows: ["北向资金", "Northbound flows"],
  dragon_tiger: ["龙虎榜", "Dragon-tiger list"],
  block_trades: ["大宗交易", "Block trades"],
  institutional_holdings: ["机构持仓", "Institutional holdings"],
  sector_members: ["板块成分", "Sector members"],
  index_constituents: ["指数成分", "Index constituents"],
  industry_members: ["行业分类", "Industry members"],
  industry_index: ["行业指数", "Industry index"],
  macro_indicators: ["宏观指标", "Macro indicators"],
  market_breadth: ["市场宽度", "Market breadth"],
  sentiment_scores: ["情绪得分", "Sentiment"],
  hot_rank: ["人气榜", "Hot rank"],
  sector_bars: ["板块行情", "Sector bars"],
  sector_fund_flow: ["板块资金流", "Sector fund flow"],
  sector_fund_flow_ths: ["同花顺板块资金", "THS sector flow"],
  news_headlines: ["新闻标题", "News headlines"],
  flash_news_wire: ["快讯", "Flash news"],
  economic_calendar: ["财经日历", "Economic calendar"],
  economic_events: ["财经事件", "Economic events"],
  market_holidays: ["交易所休市", "Market holidays"],
  share_unlock_schedule: ["限售解禁", "Share unlocks"],
  regulatory_events: ["监管事件", "Regulatory events"],
  commodity_bars: ["商品主连", "Commodity bars"],
  futures_contracts: ["期货合约", "Futures contracts"],
  option_contracts: ["期权合约", "Option contracts"],
  futures_bars: ["期货日线", "Futures bars"],
  option_bars: ["期权日线", "Option bars"],
  futures_continuous: ["期货连续", "Continuous futures"],
  option_greeks: ["期权希腊值", "Option greeks"],
  futures_minute_bars: ["期货分钟线", "Futures minutes"],
  stock_news: ["个股新闻", "Stock news"],
  research_reports: ["研究报告", "Research reports"],
};

const TIERS = {
  L0: ["基础参考", "Reference"],
  L1: ["行情", "Prices"],
  L2: ["公司事件", "Corporate events"],
  L3: ["基本面", "Fundamentals"],
  L4: ["资金面", "Capital flows"],
  L5: ["结构行业", "Industry structure"],
  L6: ["宏观", "Macro"],
  L7: ["舆情 / 轮动", "Sentiment"],
  L8: ["风险合规", "Risk and compliance"],
  L9: ["衍生品", "Derivatives"],
};

const HISTORY = {
  by_date: ["按日可补", "By date"],
  snapshot_with_backfill: ["快照，可回填", "Snapshot with backfill"],
  snapshot_only: ["仅快照", "Snapshot only"],
  derived: ["派生", "Derived"],
};

const GRAIN = {
  day: ["日", "Day"],
  month: ["月", "Month"],
  quarter: ["季", "Quarter"],
  year: ["年", "Year"],
  merge: ["单文件", "Single file"],
};

const STATUS = {
  fresh: ["最新", "Fresh"],
  stale: ["落后", "Stale"],
  empty: ["空", "Empty"],
  success: ["成功", "Success"],
  running: ["运行中", "Running"],
  failed: ["失败", "Failed"],
  error: ["错误", "Error"],
  warning: ["警告", "Warning"],
  degraded: ["降级", "Degraded"],
  interrupted: ["中断", "Interrupted"],
  info: ["信息", "Info"],
};

const MODES = {
  只读: ["只读", "Read only"],
  首次配置: ["首次配置", "First-time setup"],
  远程浏览: ["远程浏览", "Remote view"],
  可发起取数: ["可发起取数", "Can start jobs"],
};

function pair(table, key) {
  const row = table[key];
  return row ? tr(row[0], row[1]) : "";
}

export function datasetIds() {
  return Object.keys(DATASETS);
}

export function ds(id) {
  const name = pair(DATASETS, id);
  return name || String(id ?? "");
}

export function dsSearch(id) {
  const row = DATASETS[id];
  return row ? `${id} ${row[0]} ${row[1]}` : String(id ?? "");
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"]/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[ch]));
}

export function dsMarkup(id) {
  const code = String(id ?? "");
  const name = ds(code);
  if (!code || name === code) return `<span class="ds-label"><span class="ds-title">${escapeHtml(name)}</span></span>`;
  return `<span class="ds-label"><span class="ds-title">${escapeHtml(name)}</span><span class="ds-id">${escapeHtml(code)}</span></span>`;
}

export function dsWithCode(id) {
  const name = ds(id);
  if (!name || name === id) return String(id ?? "");
  return lang === "en" ? `${name} (${id})` : `${name}（${id}）`;
}

export function tierText(code, fallback = "") {
  return pair(TIERS, code) || fallback || String(code ?? "");
}

export function historyText(mode) {
  return pair(HISTORY, mode) || String(mode ?? "");
}

export function grainText(value) {
  if (!value) return pair(GRAIN, "merge");
  return pair(GRAIN, value) || String(value);
}

export function statusText(status) {
  return pair(STATUS, status) || String(status ?? "");
}

export function modeLabel(label) {
  return pair(MODES, label) || String(label ?? "");
}

export function choiceLabel(field, choice) {
  // `name` is the derive target, which is a dataset for everything but the
  // mapping tables; dsWithCode leaves those as their bare code.
  if (field === "dataset" || field === "datasets" || field === "name") return dsWithCode(choice);
  if (field === "pack") {
    const packs = { market: tr("行情", "Market"), fundamentals: tr("基本面", "Fundamentals"), universe: tr("股票池", "Universe") };
    if (packs[choice]) return packs[choice];
  }
  return String(choice ?? "");
}
