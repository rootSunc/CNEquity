## Why

`announcement_index` 在 [`src/cnequity/steps/common.py`](file:///home/bladestone/devspace/gitcode/stockagent/CNEquity/src/cnequity/steps/common.py#L68) 中被声明为日历日数据集（`CALENDAR_DATE_DATASETS = frozenset({"announcement_index", "regulatory_events"})`）。增量抓取 `fetch_incremental_daily` 会按自然日逐日拉取从 `watermark + 1` 到 `trade_date` 之间的所有日期。此设计的初衷非常明确：A 股上市公司在周末及节假日（尤其是周六）经常集中发布披露公告（例如 2026-08-01 周六全市场发布了 1,371 条公告），若仅按交易日拉取会导致周末披露静默遗失。

然而，在周日（例如 2026-08-02、2026-07-26）以及法定节假日中段，全市场物理上经常**完全没有上市公司发布公告**（CNINFO 真实全集查询 `totalAnnouncement: 0`）。而 `step_announcement_index` 委托给 `run_incremental_fetched` 时未开启 `allow_empty=True`，导致 `fetch_incremental_daily` 遇到空结果时直接触发硬性非空断言：
`RuntimeError: announcement_index: no rows returned for 2026-08-02`
从而导致 `capital` 组直接失败，并阻塞后续依赖它的步骤（如 `sentiment_scores`）。

实测探测同时证明：在**正常交易日**，A 股 5,300+ 家上市公司每天必然有大量披露（最冷清交易日也有 477~540 条，定期报告披露期单日达 6,500~22,000+ 条），交易日返回 0 行 100% 意味着上游数据源故障或反爬封禁，必须坚守 Fail-Loud 报警。因此，不能简单采用全局 `allow_empty=True`，而必须实施**交易日感知的容空策略**。

## What Changes

- **A. 日历日增量抓取容空（Trading-Day Sensitive Empty Tolerance）**：
  在 `fetch_incremental_daily` 的逐日校验逻辑中，对于属于 `CALENDAR_DATE_DATASETS` 的数据集，增加非交易日感知判断：
  - 若待抓取日期 `d` 为**非交易日（周末/法定节假日，`not is_trading_day(config, d)`）**且返回 0 行：视为合法的自然空日，跳过该日并记录日志，不抛出异常；
  - 若 `d` 为**正常交易日（`is_trading_day(config, d)`）**且返回 0 行：严格保留现有保护机制，抛出 `RuntimeError: {dataset}: no rows returned for trading day {d}`，守住上游故障拦截底线。
- **B. 保留非交易日有效数据入库与水印推进**：
  非交易日若有公告（如周六的 1,371 条），照常参与 diagonal relaxed 合并并入库；若全为 0 行，则当批次保持为空，直到后续有数据的日期一同合并落库并由 `finalize` 将 watermark 正确推向最新有数据的日期。
- **非目标**：
  - 不修改 CNINFO 底层 adapter 的分桶与 POST 查询机制；
  - 不修改 `regulatory_events`（其已显式传递 `allow_empty=True`，两者在日历日语义上完全自洽）；
  - 不引入新的配置项或破坏现有交易日数据集（行情、资金流等）的严格校验契约。

## Capabilities

### New Capabilities

- `announcement-calendar-empty-tolerance`: 规定日历日级别的信息披露数据集（`announcement_index` 等）在增量回溯过程中的空结果容忍契约：非交易日合法允许 0 行跳过，正常交易日严格禁止 0 行并大声失败（Fail Loud）。

### Modified Capabilities

<!-- 无既有已归档 spec 受破坏性影响 -->

## Impact

- **修改代码**：
  - `src/cnequity/steps/common.py`：在 `fetch_incremental_daily` 的空结果判断中引入 `CALENDAR_DATE_DATASETS` + `not is_trading_day(config, d)` 放行逻辑。
- **测试覆盖**：
  - `tests/unit/test_cninfo_announcements.py` 或 `tests/unit/test_incremental_daily.py`：
    - 验证周六有数据 + 周日无数据（0行）+ 周一有数据时，连续增量抓取能够平滑成功合并入库，不再触发 RuntimeError；
    - 验证正常交易日若返回 0 行，依然准确抛出 `RuntimeError: announcement_index: no rows returned for ...`。
- **无影响面**：
  - 不改变存储结构、DuckDB 视图、Parquet 字段与元数据定义；
  - 维持现有增量 watermark 推进语义与幂等重跑逻辑。
