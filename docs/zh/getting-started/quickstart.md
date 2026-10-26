---
description: 用 pip 安装 CNEquity，一条命令建立 A 股本地数据湖，并读出第一条股票行情查询结果。
---

# 快速开始：从安装到第一条查询

一条命令建立全市场数据湖，再读出第一条结果。下列命令无需克隆仓库；Python 和系统要求见[安装](installation.md)。

## 1. 初始化

在准备长期存放数据的目录执行：

```bash
pip install cnequity
cne init
```

第一次运行会生成用户级配置 `~/.config/cnequity/config.toml`，数据写入当前目录的 `data/cnequity/`（配置里记绝对路径）；已有配置直接沿用，不需要先跑 `cne config create`。随后建立沪深京全市场最近 3 年的主干：证券、日历、公司行为、个股与指数日线、复权因子和行业指数；自动恢复窗口内已知退市股票的日线，获取当前交易状态并派生历史停牌，审计后发布。

全市场初始化可能需要数小时，没有固定完成时间；终端实时打印批次进度和 ETA。某一批日线封存后，不必等全市场结束：`load("daily_bars", symbols=["600519.SH"])` 可以读到这只股票的未复权日线。不指定 `symbols` 的查询仍只看已经发布的数据；复权因子在初始化收尾才发布。

**中断了，或结果里有 `warning`？重跑同一条 `cne init`。** 它会续跑没完成的初始化，保留已成功的批次。`warning` / `degraded` 表示数据已发布、部分来源暂时覆盖不足，命令返回 0，缺口留待重跑或日更补齐；只有程序、存储或完整性错误才失败。

需要更深历史时，一开始就用 `cne init --profile full`（日线从 2016-01-01 起）。范围、磁盘和续跑细节见[初始化指南](initialization.md)。

## 2. 读到第一条结果

```python
from cnequity.query import load

bars = load("daily_bars", symbols=["600519.SH"])
print(bars.select("symbol", "trade_date", "close", "volume", "source").tail(10))
```

```bash
cne query --sql "SELECT symbol, trade_date, close, source FROM daily_bars ORDER BY trade_date DESC LIMIT 10"
cne check
```

`cne check` 一条命令给出新鲜度与覆盖、数据质量和规模的结论。`load` 和 `cne` 命令在任何目录都会找到这份配置（当前目录有 `configs/cnequity.toml` 时优先用它），`cne config path` 显示实际用的是哪份。`fresh` 只表示已落盘的日期够新，不等于历史完整。

<a id="browser"></a>

## 3. 在浏览器检查

```bash
cne serve
```

打开 <http://127.0.0.1:8787> 或 <http://localhost:8787>，查看数据集、水位和样例行。操作页可以发起初始化、日更、补抓和巡检；关掉页面或这个进程不会停掉已经启动的任务。按 `Ctrl-C` 停止服务。只想浏览时用 `cne serve --read-only`。

## 4. 每日更新

```bash
cne run daily
```

每天（含周末）运行一次：交易日按配置顺序跑全部日更组，然后跑公告、监管事件和资讯的事件流；非交易日只跑事件流。用系统调度器（cron、launchd、Windows 任务计划程序）每天调用这一条即可。

| 后续操作 | 作用 |
|---|---|
| `cne check` | 验收：新鲜度与覆盖、数据质量、规模 |
| `cne run retry` | 重试每个日更分组最新的失败 run |
| `pip install -U cnequity && cne config upgrade` | 升级版本，并把新版本的调度 step 补进配置 |

分钟线、分笔与逐合约衍生品默认关闭。历史 ST 和退市覆盖见[初始化指南](initialization.md)。

<a id="init-scope"></a>

!!! tip "范围、磁盘与续跑"
    quick 与 full 的区别、退市与历史 ST 补数、磁盘成本、Ctrl-C 后恢复和验收，见[初始化、范围与续跑](initialization.md#init-scope)。

## 遇到问题

| 现象 | 下一步 |
|---|---|
| TDX 无法连接 | `cne doctor` 离线体检；再执行一次 `cne sources probe --only tdx_protocol` |
| `no parquet data` | 检查使用的配置、绝对 `data.root` 与该数据集是否已 compact |
| 初始化中断或有 warning | 保留数据，重跑同一条 `cne init`；详见[续跑](initialization.md) |
| 历史 PIT 没有行 | 刚回填的数据未必是过去已观察到的证据，见[PIT 示例](../recipes/pit-rebalance.md) |

## 只想先试几只股票

不建全市场湖时，`--profile demo` 用真实数据源把 5 只股票、最近约 30 个交易日写入独立的 `data/cnequity-demo/`，配置为 `configs/cnequity.demo.toml`；`--profile sample` 离线生成同样形状的合成数据（`source=mock`），写入 `data/cnequity-sample/`，配置为 `configs/cnequity.sample.toml`，只用于验证安装。两者都不影响正式湖，也不能代替全市场初始化。

```bash
cne init --profile demo
cne init --profile sample
```

demo 结束时会打印一条带配置绝对路径的 `claude mcp add ...` 命令，复制即可把这个小湖接给 AI agent，其他客户端见 [MCP 接入](../reference/mcp.md)。用 `cne serve --config configs/cnequity.demo.toml` 可以在浏览器里查看它；面板只评估 demo 湖实际持有的数据集。

接下来：[研究示例](../recipes/README.md) · [配置](configuration.md) · [运行手册](../operations/runbook.md) · [排障](../operations/troubleshooting.md)。

执行结束不表示所有来源证据齐全；有效部分结果可发布，缺口保留。验收请用 `cne check`。参见[结果契约与升级](../reference/cli.md#命令结果与退出码)。
