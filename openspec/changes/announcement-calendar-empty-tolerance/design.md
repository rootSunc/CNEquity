## Context

`announcement_index` 是一个记录全市场 A 股上市公司披露公告索引的关键数据集。
在 [`src/cnequity/steps/common.py:68`](file:///home/bladestone/devspace/gitcode/stockagent/CNEquity/src/cnequity/steps/common.py#L68) 中：
```python
CALENDAR_DATE_DATASETS = frozenset({"announcement_index", "regulatory_events"})
```
系统通过 `incremental_trade_dates` 展开自然日历序列（包括周末），以确保抓取到周六及假期的上市公司公告。

但在 [`src/cnequity/steps/common.py:482-484`](file:///home/bladestone/devspace/gitcode/stockagent/CNEquity/src/cnequity/steps/common.py#L482) 的 `fetch_incremental_daily` 中：
```python
for d in fetch_dates:
    part = fetch_fn(d)
    if part.is_empty():
        if not allow_empty:
            raise RuntimeError(f"{dataset}: no rows returned for {d.isoformat()}")
```
当增量窗口跨越周日（如 2026-08-02）时，CNINFO 官方当天客观上全集为 0 条公告。由于 `step_announcement_index` 默认 `allow_empty=False`，导致抛错 `RuntimeError: announcement_index: no rows returned for 2026-08-02`。

同时，实测排查表明：**正常交易日**全天公告量在 400 ~ 22,000+ 条之间，绝对不会为 0。如果正常交易日返回 0 行，必须作为源故障大声报错（Fail Loud）。

## Goals / Non-Goals

**Goals:**
- 让 `fetch_incremental_daily` 感知日历属性：对于 `CALENDAR_DATE_DATASETS`，非交易日（周末/法定节假日）无数据时合法跳过，不再抛出 `RuntimeError`。
- 保留交易日严格校验：若正常交易日（`is_trading_day == True`）返回 0 行，依然抛出 `RuntimeError` 阻断流水线。
- 确保非交易日有数据（如周六披露）能够正常合并入库，Parquet 落地与 watermark 语义完全正确。

**Non-Goals:**
- 不将 `allow_empty=True` 粗暴地应用于全天候（避免工作日源宕机静默通过）。
- 不改动非自然日数据集（如行情 `daily_bars`、资金流 `fund_flow` 等交易日数据集依旧只遍历交易日）。
- 不改动底层 CNINFO HTTP 请求和 category 桶分页逻辑。

## Decisions

### D1. 决策点：在 `fetch_incremental_daily` 核心循环中引入日历感知放行

在 `fetch_incremental_daily` 中，针对 `part.is_empty()` 且 `not allow_empty` 的分支增加判定：
```python
if part.is_empty():
    if not allow_empty:
        if dataset in CALENDAR_DATE_DATASETS and not is_trading_day(config, d):
            logger.info(
                "%s: 0 rows returned for non-trading calendar date %s (tolerated)",
                dataset,
                d.isoformat(),
            )
            continue
        raise RuntimeError(f"{dataset}: no rows returned for {d.isoformat()}")
```

- **为什么选在 `fetch_incremental_daily` 而不是由 step 传递 `allow_empty=True`？**
  - 如果在 step 中传递 `allow_empty=True`，则 `fetch_incremental_daily` 对整个增量批次都会放行，万一当前批次中某个**周二或周三交易日**因为 CNINFO 故障返回了 0 行，就会被静默跳过而失去告警防护。
  - 在 `fetch_incremental_daily` 内部逐日循环时判断 `is_trading_day(config, d)`，能够做到**天级别的精准区分**：周日跳过，周一严格报错。
  - [`is_trading_day`](file:///home/bladestone/devspace/gitcode/stockagent/CNEquity/src/cnequity/steps/common.py#L314) 本身就定义在 `common.py`，无跨模块引入开销，且优先使用湖中同步的 `trading_calendar`，备用种子文件，判断极其可靠。

### D2. 水印与空洞语义一致性保证

- **跨多日增量场景（如周五到周一）**：
  - `fetch_dates` = [周五, 周六, 周日, 周一]
  - 周五：有数据（如 1300 条），加入 `frames`
  - 周六：有数据（如 1371 条），加入 `frames`
  - 周日：0 行，非交易日跳过，不加入 `frames`
  - 周一：有数据（如 477 条），加入 `frames`
  - `combined = pl.concat(frames)` 包含周五、周六、周一的数据，正常写入 staging 并 compact。
  - `finalize` 中 `_watermark_date_for` 提取湖中最大日期（周一），watermark 推进至周一。周日没有任何未解空洞。
- **单日非交易日运行场景（如周日当天执行流水线）**：
  - `fetch_dates` = [周日]
  - 周日返回 0 行，`frames` 为空，`fetch_incremental_daily` 返回 `(empty_df, [])`。
  - `run_incremental_fetched` 接收到空 DataFrame，返回 `rows_written: 0`。
  - 湖中没有新数据，watermark 保持在周六（或上一交易日）。
  - 下周一运行流水线时，自动从周日开始追赶并与周一数据合并，保持幂等与自愈。

## Risks / Trade-offs

- **风险：交易日历判断不准确？**
  - `is_trading_day(config, d)` 先查 weekend (`weekday >= 5`) 和已知 `CLOSED_DATES`，再查 `trading_calendar` 真实表。对于周末（如周日 2026-08-02），第一行 `trade_date.weekday() >= 5` 立即判定为 False，零开销且 100% 准确。
  - 调休为工作日的周末（如国庆调休周六），`weekday >= 5` 会继续查 `trading_calendar.is_trading`，若为交易日则依然严格要求非空。

## Migration Plan

1. 编辑 `src/cnequity/steps/common.py`：在 `fetch_incremental_daily` 中加入 `dataset in CALENDAR_DATE_DATASETS and not is_trading_day(config, d)` 的放行逻辑。
2. 编写单元测试：
   - 模拟自然日增量拉取包含非交易日 0 行时的通过行为；
   - 模拟正常交易日 0 行时的抛错行为；
   - 模拟非交易日有数据时的正常入库行为。
3. 运行 `ruff` 检查与相关单元测试，确认全绿。
