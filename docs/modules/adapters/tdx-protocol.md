# tdx_protocol 适配器

路径：`src/cn_market_lake/adapters/tdx_protocol/`

通过**内置的通达信协议客户端**连接行情服务器（无需本地通达信客户端）。A 股日线、指数、证券列表、除权等的核心主源。

**依赖**：无。协议实现内置于 `_wire/`，仅用标准库。

---

## 文件

| 文件 | 职责 |
|------|------|
| `client.py` | 连接管理、服务器探测、`fetch_*` 入口 |
| `quotes.py` | `_wire` 之上的门面：市场推导、翻页、`vol`→`volume` 别名 |
| `hosts.py` | 内置兜底行情主机列表 |
| `_wire/` | 内置的 TDX 线协议实现（源自 tdxpy，MIT，见 `LICENSE.tdxpy`） |
| `bars.py` | 分页日线/指数 K 线 |
| `minute_bars.py` | 日内 K 线（从 tip 往回翻页） |
| `trade_ticks.py` | 分笔（按交易日整段组装） |
| `corporate_actions.py` | 每股 xdxr → corporate_actions 行 |
| `__init__.py` | 导出 |

---

## 连接与服务器选择

`[tdx_protocol]`：

- `servers = "auto"`：先并行探测 `[tdx_protocol.hosts].standard`，再 fallback `hosts.py` 内置列表
- `servers = "host:port"`：固定单服
- `allow_mock = false`（生产）：连接失败抛异常，不造假数据

`cml servers test` 验证连通性。

---

## 主要 API

| 函数 | 数据 |
|------|------|
| `fetch_instruments(cfg)` | 全市场证券列表 |
| `fetch_trading_calendar(cfg, start, end)` | 交易日（辅助） |
| `fetch_daily_bars(cfg, symbol, start, end)` | 未复权日线 |
| `fetch_index_bars(cfg, symbol, start, end, frequency)` | 指数 K 线 |
| `fetch_corporate_actions(cfg, symbol)` | 除权除息 |
| `fetch_trading_status(cfg)` | 停牌列表（辅助） |
| `fetch_minute_bars(...)` | 日内 K 线批量 |
| `fetch_trade_ticks_batch(symbols, sessions, ...)` | 分笔批量（失败单位是 symbol-**day**） |

---

## 分页与限制

三种数据、三套分页参数，别混：

| 调用 | 单页上限 | `start` 的含义 | 终止条件 |
|------|---------|---------------|---------|
| `get_security_bars` / `get_index_bars` | **800**（`MAX_PAGE`） | 从最新往回的偏移 | 短页 / 走到窗口起点 |
| `get_history_transaction_data`（`0x0fb5`） | **2000**（`MAX_TICK_PAGE`） | 从当日**最后一条**往回的偏移 | 短页 |
| `get_transaction_data`（`0x0fc5`，当日） | **1800** | 同上 | 短页 |

分笔两条命令的上限**不是同一个数**：历史 2000，当日请求 2000 也只返回 1800。两者都能走完一整天——
实测 `600519.SH` 2026-07-31 历史路径 `2000+2000+308`、当日路径 `1800+1800+708`，都等于 4,308 条。

- `bars.py` 循环分页；失败即暴露，不静默截断
- 日更增量：检测水位后早停，避免每日翻全历史
- `trade_ticks.py` 更严：一个交易日**要么完整、要么整段作废**。因为 `tick_seq` 是落盘后才编的位置序号，
  半天数据会让空洞之后每一行的身份错位

---

## 分笔的三个坑（都已实测踩过）

移植自上游 tdxpy 时修掉的问题，以及一个上游没有的：

1. **价格标度不是常数。** 上游硬编码 ÷100（个股系数）。基金是 0.001：实测 `510300.SH` 用 ÷100 会让成交额对账变成 **10.004**，
   `159915.SZ` 的 3.368 元读成 33.68。`price_divisor()` 按 `SECURITY_COEFFICIENT` 取，未知前缀**报错而不回落**。
2. **除数而非乘数。** `0.01` 没有精确的 double，`135060 * 0.01 = 1350.6000000000001`；`135060 / 100` 与字面量同值。
   改用除法后，连续竞价最后一笔价格与 `daily_bars.close` **浮点精确相等**。
3. **`_decode.decoded_quantity` 不适用于分笔。** 那个 2⁻¹²⁷ 非规格化零的坑在 `helper.get_volume`（K 线专用浮点解码）；
   分笔的 `vol` / `direction` 走 `helper.get_price`，是普通变长**整数**解码。用了反而会把整数悄悄变成浮点。

另有两处上游缺陷已在移植时修正：`GetTransactionData` 用同一个 `num` 同时装记录总数与本帧笔数（变量名遮蔽），
`GetHistoryTransactionData` 的 `date` 类型判断写成了 `type(date) is (type(date) is str)`——恒为 False。

---

## 为什么内置协议实现

原先依赖 `mootdx`（它又依赖 `tdxpy`）。两者同属一个作者，均于 2024 年后停止发布；`mootdx` 还锁死 `httpx<0.26`，并引入编译型 V8 绑定 `py-mini-racer`。上游没有可修复的版本。

`_wire/` 从 tdxpy 裁剪出本项目实际用到的 5 个标准市场调用（1618 行 / 原 4929 行），砍掉本地文件 reader、财务爬虫、扩展市场与 pandas 依赖。TDX 线协议是冻结的传统二进制格式，内容是定长 `struct.unpack`，不随上游变动。

迁移时以真实服务器逐字节对拍验证过：解析结果与上游 tdxpy 完全一致，门面输出与 mootdx 完全一致（含 51478 行全量证券列表零差异）。`tests/unit/test_tdx_decoupling.py` 持续守卫，防止依赖回流。

---

## 主备角色（Failover）

| 数据集 | 角色 |
|--------|------|
| daily_bars | **主源** |
| corporate_actions | 回填主源；日更时东财为主、TDX 写 snapshot |
| minute_bars / minute_bars_5m | **唯一源**（无备源） |
| trade_ticks | **唯一源**，且[有意不设备源](../../datasets/sources.md#trade_ticks) |

---

## 多进程注意

TDX 连接**不可**跨 fork 共享（socket + 心跳线程）。`worker_pool` 在每个子进程内新建连接。

---

## 相关文档

- [配置 — tdx_protocol](../../getting-started/configuration.md#tdx_protocol)
- [bars step](../steps.md)
