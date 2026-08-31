# adapters 模块

路径：`src/cnequity/adapters/`

**薄 I/O 层**：封装外部协议，返回 Polars DataFrame（含溯源列），不含 compact/水位/编排逻辑。

```
adapters/
├── throttle.py           跨源限速调度
├── tdx_protocol/         通达信协议（内置客户端）
├── eastmoney/            东方财富 HTTP API
├── sina/                 新浪（复权因子 / BJ / 外盘窄集）
├── cninfo/               巨潮资讯
├── baostock/             证券宝（估值 / ST / 退市回填）
├── pboc/                 中国人民银行（社融）
├── nbs/                  国家统计局（PMI 对照）
├── exchange/             上交所 / 深交所（ST 名单对照）
├── ths/                  同花顺（板块 K 线等回填）
├── sw/                   申万行业分类历史
├── cni/                  国证指数成分历史
├── macro/                宏观指标
└── calendar/             交易日历种子
```

---

## 设计约束

1. **不做业务编排**：分页、重试由 step 或本 adapter 内部完成，但不写 staging
2. **统一溯源**：经 `domain.schemas.with_provenance()` 或等价列
3. **fail-loud**：异常向上抛，由 step 记 batch failed
4. **限速**：HTTP 源经 `cfg.rate_limit(source).wait()`；TDX 用 `min_interval_ms`

---

## 子模块文档

| 源 | 文档 | 主要数据集 |
|----|------|------------|
| tdx_protocol | [tdx-protocol.md](tdx-protocol.md) | daily_bars, index_bars, instruments, corporate_actions |
| eastmoney | [eastmoney.md](eastmoney.md) | 资金面、估值、结构、新闻等 |
| sina | [sina.md](sina.md) | adj_factors；BJ / 部分外盘 |
| baostock | [baostock.md](baostock.md) | valuation 回填, ST 历史, 退市股 |
| cninfo | [cninfo.md](cninfo.md) | announcement_index |
| pboc | [pboc.md](pboc.md) | macro_indicators（社会融资规模增量） |
| nbs | [../quality.md](../quality.md) | 制造业 PMI 发布稿，供 `macro_pmi_vs_nbs` 对照 |
| exchange | [../quality.md](../quality.md) | 交易所上市列表，供 `st_labels_vs_exchange` 对照 |
| macro | [macro.md](macro.md) | macro_indicators |
| calendar | [calendar.md](calendar.md) | trading_calendar 种子 |
| ths / sw / cni | （无独立页）见 [逐源限制](../../datasets/sources.md) | sector_bars / industry_members / index_constituents 回填 |

---

## throttle.py

`SourceRateLimiters`：按配置为每个 source 构造 `domain.rate_limit.RateLimiter`。

---

## 启用与配置

`configs/cnequity.toml`：

```toml
[sources.eastmoney]
enabled = true
min_interval_seconds = 1.0
```

TDX 独立段 `[tdx_protocol]`，非 `sources.*`。

---

## 相关文档

- [steps 模块](../steps.md)
- [逐源限制](../../datasets/sources.md)
