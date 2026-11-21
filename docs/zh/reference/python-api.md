# Python API

模块：`cnequity.query`。口径说明先看[查询与研究](../datasets/query-guide.md)，本页查参数、返回值和版本边界。所有函数都接受 `config=` 或 `data_root=` 指定湖；都不给时按 `CNE_CONFIG` → 当前目录的 `configs/cnequity.toml` → 用户级配置查找，与 CLI 相同。

```python
from cnequity.query import load, scan, read_scope, list_datasets, dataset_state
```

## load() {#load}

```python
load(
    dataset, *, start=None, end=None,
    adjust=None,            # "qfq" | "hfq" | "total_return"
    profile=None,           # 版本化股票池，别名 universe_profile
    universe=None,          # 兼容参数："all_a_sh_sz"
    as_of=None, pit_mode=None, all_vintages=False,
    items=None, symbols=None,
    strict_adj=False, strict_universe=False, include_otc=False,
    revision=None, revision_map=None,
    config=None, data_root=None,
) -> polars.DataFrame
```

| 参数 | 说明 |
|---|---|
| `start`、`end` | 含边界的日期窗口，作用于数据集的主日期列 |
| `adjust` | `qfq` 前复权、`hfq` 后复权，用于日线、分钟线等价量数据。`total_return` 只用于 `daily_bars`：基金现金分配再投资，股票同 `hfq`。结果附加 `adj_open`、`adj_high`、`adj_low`、`adj_close` 和 `adj_is_exact` |
| `strict_adj` | 缺复权因子时报错，而不是返回 `adj_is_exact=False`。它只检查因子覆盖，不能证明历史因子正确 |
| `profile` | 版本化股票池，如 `cn_a_sh_sz_research_v1`。用 `cne profile list` 或 `list_universe_profiles()` 查看；研究画像自动启用严格证据校验 |
| `universe` | 旧参数，只接受 `all_a_sh_sz`（沪深子集）。`universe="all_a"` 已在 0.25.0 移除，传入会报错；改用 `profile="legacy_all_a"`，返回同样的行 |
| `strict_universe` | 股票池缺少证券主数据、逐日交易状态或历史 ST 证据时报错 |
| `as_of`、`pit_mode` | PIT 截止日。`best_effort`（默认）保留事后回填但在截止日前已披露的值，标 `pit_is_exact=False`；`strict` 只返回湖在截止日已观察到的版本 |
| `all_vintages` | 返回截止日前的全部版本，用于研究财报修订；截面选股不要开，会重复计同一事实 |
| `items` | 财报科目代码 |
| `include_otc` | 默认排除北交所证券进入交易所市场之前的场外报价 |
| `revision`、`revision_map` | 固定修订，见下文 |

PIT 数据集另返回 `available_at`、`source_published_at`、`observed_at`、`revision_id`、`pit_is_exact`、`pit_quality`。未知数据集、无数据或严格校验失败时抛 `ReaderError`（`ValueError` 的子类）。

## 固定修订 {#revisions}

一次复权或股票池查询可能依赖 `daily_bars`、`adj_factors`、`instruments`、`trading_status`、`trading_calendar` 等多个数据集。`revision` 只固定主数据集；`revision_map={"daily_bars": 12, "adj_factors": 7}` 固定各依赖，各数据集用自己的修订号或修订 id。指定的修订已被保留策略清理时抛 `RevisionConsistencyError`，不会退回最新修订。没写到的依赖读当前修订。

### load_with_receipt() {#load-with-receipt}

```python
from cnequity.query import load_with_receipt

result = load_with_receipt(
    "daily_bars", start="2024-01-01", end="2024-12-31",
    symbols=["600519.SH"], adjust="hfq", require_replayable=True,
)
bars, receipt = result.frame, result.receipt
```

参数与 `load()` 相同。它先取得各依赖的已提交修订，再按这组修订读取，凭证记录选中的修订与契约指纹、返回行的来源和采集时间、PIT 模式、请求窗口与实际日期边界。`receipt_id` 是这些内容的稳定摘要。`coverage.completeness` 为 `not_assessed`，表示起止日期不能证明逐日完整。`require_replayable=True` 时，有依赖无法固定修订就在读取前拒绝。凭证只固定数据，不冻结质量报告、配置或软件版本。

本地 `cne serve` 也提供 `GET /api/read/{dataset}`，返回行和同一次读取的凭证；窗口最长 366 天、超过 1000 行拒绝，批量研究用 Python。

## scan() 与 read_scope() {#scan}

```python
import polars as pl
from cnequity.query import read_scope, scan

with read_scope():
    lf = scan("daily_bars", start="2020-01-01", symbols=["600519.SH"])
    df = lf.filter(pl.col("close") > 0).collect()
```

`scan()` 返回原始的 `polars.LazyFrame`，只做日期分区与代码过滤，不执行复权、股票池或 PIT。参数为 `start`、`end`、`symbols`、`revision`、`revision_map`、`config`、`data_root`。

延迟查询在 `collect()` 时才读文件。在 `read_scope()` 里构建并收集，保留清理就不会在这期间删除它要读的文件；在作用域外收集可能读到已被清理的文件而失败。

## load_profile() {#load-profile}

一次取多只证券、多个截止日、多个字段在当时能知道的值，返回长表，每行是 `(symbol, as_of, field)`。

```python
from datetime import date
from cnequity.query import load_profile

p = load_profile(
    ["listed", "status.st", "shares.total", "fsi:net_profit", "industry.sw.l1"],
    symbols=["600519.SH"], as_of_dates=[date(2024, 4, 30)],
    pit_mode="best_effort",    # 或 "strict"、"published"
)
```

| 字段 | 来源 |
|---|---|
| `listed` | `instruments`：截止日在上市期内为 1 |
| `status.trading`、`status.st` | `trading_status`：截止日及之前最近一行；ST 为空表示未知 |
| `shares.total`、`shares.float`、`shares.free_float` | `share_structure`：变动日与公告日都已过 |
| `holders.count` | `shareholder_counts`：公告后可知 |
| `fsi:<科目代码>` | `financial_statement_items`：截止日可见的最新报告期 |
| `industry.<sw\|eastmoney>.l<1\|2\|3>` | `industry_members`：截止日及之前最近的快照 |

返回列有 `value_num`、`value_str`、`label`、`field_date`、`visible_from`、`source`、`pit_quality`、`age_days`、`missing_reason`。`missing_reason` 区分上市前、退市后、覆盖之外、尚未披露、值未知、缺少 PIT 证据和无记录。`pit_mode="published"` 只认记录了发布时间的值，`cutoff=time(15)` 要求发布时间在截止日北京时间 15:00 之前；`canonical=True` 先把代码换成当日实际生效的代码。

## 湖的状态 {#lake-state}

| 函数 | 返回 |
|---|---|
| `list_datasets()` | 每个数据集一行：层级、主日期列、历史模式、回填来源、PIT 声明、是否有数据、覆盖起止、水位、当前修订和契约指纹。日期边界不证明中间没有缺口 |
| `dataset_state(dataset)` | `DatasetState`：当前修订号、修订 id、提交时间、运行 id、schema 版本、契约指纹、内容摘要、变化的分区。下游缓存用它判断数据是否变化，不要只看最大日期 |
| `dataset_revisions(dataset, after=修订id)` | 该修订之后的已提交修订，从旧到新；之后的历史不完整时抛 `ValueError`，调用方应按「已变化」处理 |
| `dataset_attempt(dataset)` | 最近一次采集中该数据集最严重的步骤结果（阶段、状态、原因、运行 id）。采集失败不会删除上一份可读数据，展示时应同时给出数据日期和失败原因 |
| `dataset_schema(dataset)` | 登记的列和 Polars 类型 |

## 分钟重采样 {#resample}

`resample_trade_bars(frame, "5m")` 把完整的 1m 成交数据重采样为 5m、15m、30m、60m，也可以从 5m 重采样为 15m、30m、60m；按上午、下午分别对齐，组成分钟不完整时报错。`resample_minute_history(minute_1m, minute_5m, "15m")` 按股票和交易日优先用 1m、否则用 5m，`resampled_from` 列标明来源。它们不写湖；要存进湖用 `cne derive minute_bars_15m`。

## 证券代码延续 {#identifiers}

`cnequity.domain.security_identifiers` 是公开的读取契约，提供 `canonical_symbol`、`canonicalize_dated_identifiers`、`identifier_aliases`、`identifier_changes`、`transitions_on`。登记表只收录生效日和换股比例都有公告佐证的代码变更。`canonicalize_dated_identifiers(frame, date_col=..., key_columns=...)` 把带日期的行换成当日有效的代码。删除或改名这些函数须先经过弃用周期。

## SQL {#sql}

`cne query --sql` 和 DuckDB 视图库读的是已发布数据，每个数据集一个同名视图。`daily_bars_adj` 带复权列，其前复权以每个标的在湖内的最新一根为锚；有明确窗口的前复权聚合用 `daily_bars_qfq(DATE '2020-01-01', DATE '2020-12-31')`。SQL 不会替普通表执行 `load()` 的严格 PIT、股票池或缺因子校验。
