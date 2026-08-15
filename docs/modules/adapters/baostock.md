# baostock 适配器

路径：`src/cn_market_lake/adapters/baostock/`

[Baostock](http://baostock.com) 开源证券数据接口。用于 **历史回填** 场景，非日更主源。

**依赖**：随 `pip install cn-market-lake` / 可编辑安装一并提供，无需额外 extras。

---

## 文件

| 文件 | 职责 |
|------|------|
| `_session.py` | 登录/重登、逐 symbol 拉取驱动 |
| `valuation.py` | 历史 PE/PB/PS 等 → valuation_metrics |
| `st_history.py` | K 线 `isST` 标记 → trading_status ST 历史 |
| `__init__.py` | 导出 |

---

## valuation.py

- `valuation_metrics` 的 `DatasetSpec.backfill_source = "baostock"`
- `cml backfill valuation_metrics` 走此路径
- 日更仍用东财快照

---

## st_history.py

- 从 baostock 日 K 的 `isST` 字段推断历史 ST
- 由 `reference` / trading_status 相关 step 在 backfill 模式调用
- 补充 EastMoney 无法提供的历史 ST

---

## 会话管理

baostock 需匿名 `bs.login()`；`_session.py` 处理：

- 单进程内复用 session；周期性 relogin；socket/watchdog 防挂死
- 断线重登 + 逐 symbol 重试；失败 symbol 返回给调用方（fail-loud / 可 resume）

### 防黑名单限速（必开）

官方免费 API 限制（超限进入黑名单，`error_code=10001011`）：

- **每日 API 请求不能超过 5 万次**
- **不能并发连接访问**（单连接串行）

全市场历史回填极易触发「黑名单用户」。配置见 `[sources.baostock]`：

| 键 | 默认 | 说明 |
|----|------|------|
| `min_interval_seconds` | 1.0 | 每 symbol 前跨进程限速（`config.rate_limit("baostock")`） |
| `batch_size` | 20 | 每完成 N 个 symbol 额外休息 |
| `batch_rest_seconds` | 120 | 批次间冷却秒数 |

时间可以慢：~5000 票 ×（1s + 批次休息）是**有意**的，换 IP 解封后务必用该配置 resume，**勿开多进程/多连接并行扫 baostock**。

---

## 相关文档

- [datasets — valuation_metrics](../../datasets/catalog.md)
- [查询指南 — 历史 ST](../../datasets/query-guide.md)
- [故障排查 — 数据源封禁](../../operations/troubleshooting.md)
