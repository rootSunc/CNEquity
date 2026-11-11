# Python API

模块：`cnequity.query`。先看[查询指南](../datasets/query-guide.md)理解口径；本页用于查参数、返回值和版本边界。

```python
from cnequity.query import load, scan, list_datasets, dataset_schema
```

## load()

```python
def load(
    dataset: str,
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    adjust: Literal["qfq", "hfq", "total_return"] | None = None,
    universe: Literal["all_a", "all_a_sh_sz"] | None = None,
    profile: UniverseProfileLike | None = None,
    universe_profile: UniverseProfileLike | None = None,
    as_of: str | date | None = None,
    items: list[str] | None = None,
    symbols: list[str] | None = None,
    strict_adj: bool = False,
    strict_universe: bool = False,
    all_vintages: bool = False,
    pit_mode: Literal["strict", "best_effort"] | None = None,
    include_otc: bool = False,
    config: Config | None = None,
    data_root: str | Path | None = None,
    revision: RevisionSelection | None = None,
    revision_map: Mapping[str, int | str] | None = None,
) -> pl.DataFrame
```

### 参数

| 参数 | 说明 |
|------|------|
| `dataset` | 注册数据集名 |
| `start`, `end` | 含边界日期窗口（数据集主日期列） |
| `adjust` | `hfq` / `qfq`；适用于 `daily_bars`、`minute_bars`、`minute_bars_5m` 等价量数据集。`total_return` 仅用于 `daily_bars`：基金现金分配再投资，股票同 `hfq`，见[查询指南](../datasets/query-guide.md#总收益复权total_return) |
| `universe` | 兼容参数：`"all_a"` 沪深北全 A（已弃用，发出警告，0.25.0 移除；`profile="legacy_all_a"` 返回同样的行）；`"all_a_sh_sz"` 沪深子集。新研究应选版本化 `profile` |
| `as_of` | PIT 截止日；可见性规则由 `pit_mode` 决定。严格模式核验公告、可用/发布时间及观察时间，随后按事实键选有效版本 |
| `items` | 财报科目 code 列表 |
| `symbols` | symbol 白名单 |
| `strict_adj` | True 时缺复权因子抛 `ReaderError` |
| `strict_universe` | True 时支持的 universe 缺少 instruments、逐日 trading_status 覆盖或版本化历史 ST 证据收据会抛错；适合研究读取 |
| `all_vintages` | True 时返回 `as_of` 前的**全部**版本（研究财报修订用）；截面选股勿开，会重复计同一事实 |
| `pit_mode` | PIT 证据模式：省略或传 `None` 默认为 `best_effort`，保留在 `as_of` 之后采集的历史回填，并返回 `pit_is_exact=False`；公告及已知可用/发布时间仍受截止日约束。`strict` 排除 reconstructed 回填，只返回湖在截止日已有的证据 |
| `include_otc` | 默认 False：`daily_bars`、`adj_factors` 排除北交所证券进入交易所市场之前的场外报价；设为 True 保留这些报价。交易所起点取上市日和 2020-07-27 中的较晚者 |
| `config` / `data_root` | 湖位置；省略时按 `CNE_CONFIG` → 当前目录的 `configs/cnequity.toml` → 用户级配置查找 |
| `profile` / `universe_profile` | 版本化股票池策略，二者为别名；官方研究画像自动启用严格证据校验 |
| `revision` | revision 数字或 ID 固定主数据集；也支持按数据集映射。单个数字不固定因子或股票池依赖 |
| `revision_map` | 固定各依赖数据集的 retained revision；显式缺失版本抛 `RevisionConsistencyError`，不退回 latest |

初始化尚未结束时，`load("daily_bars", symbols=[...])` 可以读到已经封存的未复权日线。不写 `symbols` 的查询、`scan()` 和不带标的的 SQL 仍只看已发布数据。复权因子在 `adj_factors` 发布之后才可用。

### 固定组合读取的数据版本

复权股票池查询可能依赖 `daily_bars`、`adj_factors`、`instruments`、`trading_status`、
`trading_calendar`。`revision_map` 使用各数据集自身的 revision，数字不必相同；只填
主表并不能固定整个结果。一次 `load()` 为所用依赖各选择一次 generation，后续股票池
和覆盖数据读取复用该选择。未指定的依赖选当前版本，不提供全湖原子事务。

这只固定数据。长期研究使用 `SnapshotStore.create(name, datasets, research=True)`
或 CLI `snapshot create --research`，将完整依赖、ST/退市证据、目录、日历种子和非敏感读取配置一起封装。快照不包含 API token，恢复后可以离线解释原来的来源能力。
读取时检查冻结的 canonical 策略与所请求内置 profile，版本不一致报错；快照不打包可执行软件。
长期研究还应保存依赖版本映射、查询参数、profile、配置和软件版本；旧 generation
受保留期限制。不要以水位或 `fetched_at` 代替完整依赖的 revision 身份。

### 带读取凭证的查询

```python
from cnequity.query import load_with_receipt

result = load_with_receipt(
    "daily_bars",
    start="2024-01-01",
    end="2024-12-31",
    symbols=["600519.SH"],
    adjust="hfq",
    require_replayable=True,
    data_root="/path/to/lake",
)
bars, receipt = result.frame, result.receipt
```

`load_with_receipt()` 使用与 `load()` 相同的筛选参数，先捕获各依赖数据集的已提交
修订，再以该映射读取。凭证记录实际选中的修订与合同指纹、返回行的来源和采集时间、
PIT 模式/质量、请求窗口与实际行的日期边界。`receipt_id` 是这些内容的稳定摘要。
`coverage.completeness="not_assessed"` 明确表示行的起止日期**不能**证明全市场或逐日完整。
旧湖缺少修订时 `replayable=false` 并列出 `unpinned_datasets`；设置
`require_replayable=True` 会在扫描前拒绝。此凭证固定数据版本，但不会冻结质量报告、
操作配置或软件版本；长期复现仍需研究快照及研究产物清单。

本地 HTTP 服务也提供 `GET /api/read/{dataset}`：传入 `start`、`end` 和通常所需的
`symbol`，返回 `rows` 与同一次读取的 `receipt`。PIT 数据还须传 `as_of`，默认使用
严格模式。HTTP 窗口最长 366 天、多日查询必须给出证券代码，超过 1000 行会拒绝；
批量研究仍使用 Python API。该接口要求所有读取依赖有可重放修订，不会默默降级。

### 返回

- 未复权数据集：原始列
- PIT `load()` 还会返回可选双时态列 `available_at`、`source_published_at`、
  `observed_at`、`revision_id`，以及 `pit_is_exact` / `pit_quality` 质量标记；
  旧 Parquet 缺列时读侧补齐
- `adjust` 非空：附加 `adj_open`, `adj_high`, `adj_low`, `adj_close`, `adj_is_exact`

### 异常

`strict_adj` 和 `adj_is_exact` 仅描述因子覆盖，不能证明历史因子正确。`load_with_receipt()` 的 `adjustment` 摘要提供所读版本的来源与非阻断历史诊断；无诊断时标为未评估。

`ReaderError`（`ValueError` 子类）：未知数据集、无数据、strict_adj 失败等。

## load_profile()

一次取多只证券、多个截至日、多个字段在当时能知道的值，返回长表，每行是 `(symbol, as_of, field)`。
每个源数据集只读一次，按证券做截至日连接，所以多加截至日只多一次连接。

```python
from datetime import date
from cnequity.query import load_profile

p = load_profile(
    ["listed", "status.trading", "status.st", "shares.total", "holders.count",
     "fsi:net_profit", "industry.sw.l1", "industry.eastmoney.l3"],
    symbols=["600519.SH"],
    as_of_dates=[date(2024, 4, 30)],
    pit_mode="best_effort",   # 或 "strict"
)
```

| 字段 | 来源 | 说明 |
|---|---|---|
| `listed` | instruments | 截至日在上市期内为 1 |
| `status.trading` / `status.st` | trading_status | 截至日及之前最近一行；ST 为空表示未知，不是“非 ST” |
| `shares.total` / `shares.float` / `shares.free_float` | share_structure | 变动日和公告日都过了才可知 |
| `holders.count` | shareholder_counts | 公告后可知 |
| `fsi:<item_code>` | financial_statement_items | 截至日可见的最新报告期；`value_str` 为报告期，`field_date` 为期末日 |
| `industry.<sw\|eastmoney>.l<1\|2\|3>` | industry_members | 截至日及之前最近快照中该层级的行业代码，`label` 为名称 |

返回列：`value_num`、`value_str`、`label`、`field_date`（值描述的日期）、`visible_from`（最早可知日）、`source`、
`pit_quality`（`strict` 一手来源且本湖在截至日前已观察到；`reconstructed` 按公告日可知但事后补录；`snapshot_only` 只有快照日）、
`age_days`、`missing_reason`。

`missing_reason`：`before_listing`、`after_delisting`、`outside_coverage`（截至日早于该数据集最早记录）、`not_yet_published`、
`value_unknown`（有记录但值未知，如派生状态行的 ST）、`no_pit_evidence`（只在 `strict` 下）、`no_record`；
值比该字段的时效上限更旧时照常返回值并标 `stale`。上市期外不返回任何值。

`pit_mode="published"` 只认记录了发布时间（`available_at`）的值，公告日和入湖日都不算；同时不早于值生效的日期
（如股本变动日）。`cutoff=time(15)` 表示发布时间须在截至日北京时间 15:00 之前，否则从下一天起才可见；不给则整天都算。
行业快照没有发布时间，在该模式下为 `no_pit_evidence`；上市与交易状态是当日事实，各模式相同。
`canonical=True` 先把每行换到当日实际生效的代码（同 `canonicalize_dated_identifiers`）；`data_root`、`revision_map` 传给每次读取。

## scan()

返回原始 `pl.LazyFrame`，适合大窗口的自定义 lazy 管道。它只做日期分区和
symbol 过滤，不执行 `load()` 的复权、universe、PIT 或严格覆盖语义；需要这些
语义时应使用 `load()`。当前参数如下：

```python
def scan(
    dataset: str,
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    symbols: list[str] | None = None,
    config: Config | None = None,
    data_root: str | Path | None = None,
    revision: RevisionSelection | None = None,
    revision_map: Mapping[str, int | str] | None = None,
) -> pl.LazyFrame
```

```python
import polars as pl

lf = scan("daily_bars", start="2020-01-01", symbols=["600519.SH"])
df = lf.filter(pl.col("close") > 0).collect()
```

## list_datasets()

```python
def list_datasets(
    *,
    config: Config | None = None,
    data_root: str | Path | None = None,
) -> pl.DataFrame
```

主要列：`dataset`、`layer`、`date_col`、`fetch_semantics`、`history_mode`、`backfill_source`、`history_horizon_days`、`pit`、`pit_quality`、`pit_storage_columns`、`has_data`、`coverage_start`、`coverage_end`、`watermarked`、`watermark`。另返回 `snapshot_date`、`revision`、`revision_id`、`schema_version`、`contract_fingerprint`。

日期边界用于发现数据，不等于窗口完整性证明；版本字段来自 state，读取提交版本仍以 revision pointer 为准。

`history_mode` ∈ `by_date` / `snapshot_with_backfill` / `snapshot_only`；与 `coverage_*` 一起构成可用起点合同。

## historical_universe_validity()

历史研究门禁位于 `cnequity.quality.historical_validity`，默认验证沪深北全 A：

```python
from cnequity.quality.historical_validity import historical_universe_validity
from datetime import date

report = historical_universe_validity(
    cfg,
    start=date(2020, 1, 1),
    end=date(2024, 12, 31),
    universe="all_a_sh_sz",
)
assert report["universe_ready"]
```

`universe="all_a_sh_sz"` 会让日线区间、历史 ST 证据和退市覆盖都只按沪深
子集核验；它不会把全 A (`all_a`) 的 BJ 历史证据缺口隐藏掉。`universe_ready`
为真才表示该明确口径可以进入历史研究。

## dataset_state() 与分钟重采样

`dataset_state(dataset, config=..., data_root=...)` 返回 `DatasetState`，用于取得下游缓存身份。字段见 `query/state.py`，可用标准库 `dataclasses.asdict()` 转为字典；不要只用最大日期判断数据是否变化。

`dataset_attempt(dataset, config=..., data_root=...)` 只读最近一次采集运行中该数据集最严重的步骤收据（阶段、状态、时间、错误原因和 run ID）。它与已发布修订是两个维度：采集失败不会删除上一份可读快照，消费端应同时显示快照日期与失败原因。旧湖没有采集收据时返回 `None`。

`resample_trade_bars(frame, "5m")` 从完整 1m 成交数据重采样，支持 5m / 15m / 30m / 60m；也可以从 5m 重采样出 15m / 30m / 60m，用于 1m 覆盖不到的历史。按上午和下午分别对齐。`resample_minute_history(minute_1m, minute_5m, "15m")` 按股票和交易日优先用 1m、否则用 5m，输出 15m / 30m / 60m，并用 `resampled_from` 列标明来源。它不覆写原始湖，缺组成分钟会报错，详见[查询指南](../datasets/query-guide.md#成交口径的分钟重采样)。

## dataset_schema()

```python
def dataset_schema(dataset: str) -> dict[str, pl.DataType]
```

返回 `domain/schemas.py` 中注册的 Polars 类型映射。

## 证券代码延续

模块：`cnequity.domain.security_identifiers`。这是公开的读取契约，下游（如 EquityLab 的回测账本和研究样本）直接导入；删除或改名须先经弃用周期。

```python
from cnequity.domain.security_identifiers import (
    canonical_symbol,
    canonicalize_dated_identifiers,
    identifier_aliases,
    identifier_changes,
    transitions_on,
)
```

注册表只收录生效日和换股比例都有存档公告佐证的代码变更；不能按相近的价格或名称推断变更。`canonicalize_dated_identifiers(frame, date_col=..., key_columns=...)` 把带日期的行改回当日有效的代码，同一业务键同时有原生行和厂商回溯行时保留原生行。下游可以对模块源文件取哈希，作为冻结样本的身份，因此修改注册表会让这些样本失效。

## 配置解析

```python
from cnequity.query.reader import resolve_config

cfg = resolve_config(config=my_cfg)
cfg = resolve_config(data_root="/path/to/lake")
```

优先级：`config` > `data_root` > 默认 toml 路径。

## 示例

### 后复权沪深研究范围

```python
bars = load(
    "daily_bars",
    start="2024-01-01",
    end="2024-12-31",
    adjust="hfq",
    profile="cn_a_sh_sz_research_v1",
    strict_adj=True,
)
```

### PIT 财报

```python
roe = load(
    "financial_statement_items",
    items=["roe"],
    as_of="2024-04-30",
    pit_mode="strict",
)
```

### 指数行情

```python
idx = load("index_bars", start="2024-01-01", symbols=["000300.SH"])
```

### 显式 data_root（无需 toml）

```python
bars = load("daily_bars", start="2024-06-01", data_root="/data/cnequity")
```

## DuckDB 入口

视图由 `query/views.py` 维护，复权列见 `daily_bars_adj`。SQL 不会自动替普通表查询附加 `load()` 的 strict PIT、股票池或缺因子拒绝逻辑；需要自行表达并验证，或优先用 `load()`。示例见 [DuckDB 与 Polars](../recipes/duckdb-polars.md)。

## 相关文档

- [查询指南](../datasets/query-guide.md)
- [产品边界](../architecture/overview.md)
