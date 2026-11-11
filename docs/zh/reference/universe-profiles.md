# 股票池画像

CNEquity 通过公开的 `cnequity.domain.universe_profiles` 注册表提供带版本的股票池契约。画像是一份机器可读的范围定义，既不是实时的数据源适配器，也不是缓存下来的股票名单。在研究产物里要同时记下这三个值：

```text
name       = cn_a_sh_sz_research_v1
version    = 1
scope_hash = <注册表返回的完整 SHA-256>
```

正式画像有两个：

| 画像 | 交易所 / 板块范围 | CDR / ETF | ST / 停牌 | 证据要求 |
| --- | --- | --- | --- | --- |
| `cn_a_sh_sz_research_v1` | 沪深主板、科创板和创业板 | 排除 | 排除 | 严格：证券主数据、逐日 `trading_status`、带版本的历史 ST 与退市收据；PIT 数据集需要发布或观测证据 |
| `cn_a_all_experimental_v1` | 沪深加北交所 | 排除 | 排除 | 选用时同样严格，但标为实验性；在全交易所证据齐备之前不作为研究认可的范围 |

两个正式画像都按上市、退市日期做时点判断。对严格画像来说，缺少 `instruments`、缺少某个代码某天的 `trading_status` 行，或缺少完整的历史 ST 收据，都会抛出 `UniverseCoverageError`。这样，一条缺失的观测不会被当成一行正常、可交易的数据。PIT 财务数据仍然受读取接口显式的 `pit_mode` 和 `as_of` 约束。

旧的 `universe="all_a"` 参数为了兼容仍然可用，保留原来宽松的语义，但会发出 `DeprecationWarning`，并将在 0.25.0 移除；`profile="legacy_all_a"` 返回同样的行且不告警。它不会悄悄变成任何一个正式画像。新的研究代码请显式指定画像：

```python
from cnequity.query import load

bars = load(
    "daily_bars",
    start="2020-01-01",
    end="2024-12-31",
    profile="cn_a_sh_sz_research_v1",
)
```

不读 Parquet 也能拿到注册表和哈希：

```python
from cnequity.query import list_universe_profiles, profile_scope_hash

profiles = list_universe_profiles(include_compatibility=False)
contract_hash = profile_scope_hash("cn_a_sh_sz_research_v1")
concrete_hash = profile_scope_hash(
    "cn_a_sh_sz_research_v1", ["600000.SH", "000001.SZ"]
)
```

对应的机器可读命令是 `cne profile list` 和 `cne profile show cn_a_sh_sz_research_v1`（加 `--symbol` 绑定一组具体代码，并输出它的 `concrete_scope_hash`）。
