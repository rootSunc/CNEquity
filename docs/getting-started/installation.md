# 安装

## 系统要求

| 项 | 要求 |
|----|------|
| Python | ≥ 3.10 |
| 操作系统 | macOS / Linux / **Windows 10+（64-bit）** |
| 磁盘 | 全量 init（2016 起）约需数十 GB，视数据集范围而定 |
| 网络 | 采集需访问 TDX 行情服务器与各 HTTP 数据源 |

Windows 说明：

- 支持原生 Win10/11 + PowerShell / cmd；CI 有 `windows-latest` 单元测试。
- 范围是 **64-bit x86-64**；32-bit 与 ARM64 Windows 未验证。
- WSL 可作为过渡，但不是必需——原生 Windows 已可用。
- 依赖（duckdb / polars / pyarrow 等）均有 `win_amd64` 轮子；若某包退化成从源码编，`cml doctor` 会报出。

## 从 PyPI 安装（推荐）

```bash
pip install cn-market-lake
cml demo    # 一分钟真数样例，不需要先 clone 仓库
```

**没有 extras**。一条命令装齐所有数据源——通达信协议（内置客户端）、东方财富、新浪、巨潮、中国人民银行、Baostock、SnowNLP，以及申万/国证成分表所需的 XLS 解析。

旧文档里的 `pip install "cn-market-lake[tdx]"` 之类仍然可用，装出来的结果完全一致——pip 会提示一句 `does not provide the extra 'tdx'` 然后照常安装，uv 则不作声。

全量 `cml init` 前先写出配置（不必 clone 仓库）：

```bash
cml config init                   # → configs/cn-market-lake.toml；data.root 写为绝对路径；macOS / Windows 自动 workers=1
cml config init --data-root /path/to/lake   # 可选：直接指定 data.root（同样会 resolve 为绝对路径）
cml config validate
```

### Windows（PowerShell / cmd）

路径用正斜杠、反斜杠或盘符均可；`cml config init --data-root` 会把反斜杠正确转义进 TOML：

```powershell
pip install cn-market-lake
cml doctor
cml config init --data-root D:/cn-market-lake
# 或：cml config init --data-root "D:\cn-market-lake"
cml demo
cml query --config configs/cn-market-lake.demo.toml --sql "SELECT count(*) FROM daily_bars"
```

> PowerShell 5.1 不支持 `&&`。请分行执行，或用 PowerShell 7+ / cmd。

## 从源码安装（开发）

```bash
git clone https://github.com/rootSunc/cn-market-lake.git
cd cn-market-lake
python3 -m venv .venv && source .venv/bin/activate
python -m pip install --upgrade pip   # PEP 735 --group 需要 pip >= 25.1
pip install -e . --group dev
# 或：uv sync
```

Windows（PowerShell）：

```powershell
git clone https://github.com/rootSunc/cn-market-lake.git
cd cn-market-lake
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e . --group dev
```

## 依赖构成

所有运行时依赖都是硬依赖，装完即可跑通日更与回填全流程：

| 包 | 用途 |
|----|------|
| polars、pyarrow、duckdb | 湖存储与查询 |
| httpx、curl_cffi | HTTP 源（东财 / 新浪 / 巨潮） |
| click | CLI |
| baostock | 估值 / ST / 退市行情的历史回填 |
| snownlp | on-demand `stock_news` 情绪（`[sentiment] use_snownlp`） |
| pandas、openpyxl、xlrd | 申万 / 国证成分历史的 XLS·XLSX 解析 |

通达信协议客户端内置于 `adapters/tdx_protocol/_wire`，只用标准库，不引入任何包。

> 曾经的 `tdx` / `macro` / `nlp` / `valuation` / `structure` / `all` extras 已全部移除。带上它们的旧命令不会失败，安装器只会忽略未知 extra（pip 附带一句警告）。

装完建议跑一次体检——它会报出配置与环境不一致（如某个源的包导入失败、`data.root` 写成相对路径）这类静默问题：

```bash
cml doctor
```

选型犹豫（本项目 vs AkShare / Tushare）见 [comparison.md](../comparison.md)。
运行前请阅读 [legal-and-data-sources.md](../legal-and-data-sources.md)。

## 配置初始化

```bash
cml config init
# 等价于从包内模板写出 configs/cn-market-lake.toml
# 仓库开发也可：cp configs/cn-market-lake.example.toml configs/cn-market-lake.toml
# 编辑 data.root — 生产环境建议使用绝对路径
```

`configs/cn-market-lake.toml`、`data/`、根目录 `logs/` 均已 gitignore，请勿强制加入版本库。

## 验证安装

```bash
cml --help
cml demo
# 全量配置就绪后：
cml config validate --config configs/cn-market-lake.toml
cml servers test --config configs/cn-market-lake.toml   # 探测 TDX 行情主机
pytest tests/unit -q                               # 需源码 + --group dev，离线可跑
```

## 依赖版本注意事项

### httpx 不再有上限

早期 `[tdx]` extra 依赖的 `mootdx` 要求 `httpx<0.26`，把整个环境压在 0.25.x。TDX 客户端内置后这个约束消失了：

| 安装方式 | httpx |
|----------|-------|
| `pip install cn-market-lake` | 0.28.x |

`pyproject.toml` 里 `httpx>=0.25` 的下界现在只标记「我们用到的 `Client()` 选项最早出现在哪个版本」，不再是为了迁就别人。

### 从 0.3.x 升级到 0.4

完整说明见 [CHANGELOG 0.4.0 — Upgrading](../../CHANGELOG.md#upgrading-from-03x)。要点：

1. **`daily_bars.volume` → 一律股（`data_version = v2`）**。已有湖需一次性改写，否则换手 / 流动性因子会错 100×：

   ```bash
   scripts/migrate_daily_bars_volume_v2.py --config configs/cn-market-lake.toml --dry-run
   scripts/migrate_daily_bars_volume_v2.py --config configs/cn-market-lake.toml --apply
   ```

2. **配置**：删掉手写配置里的 `[sources.akshare]`；加上 `[sources.pboc]`（社融）。可选 `[sources.nbs]` / `[sources.exchange]` 打开发布方交叉核验。或直接 `cml config init --force` 后把 `data.root` 改回原路径。

3. **孤儿包**：AkShare 已移除（[issue #3](https://github.com/rootSunc/cn-market-lake/issues/3)），pip / uv 不会卸掉不再依赖的包：

   ```bash
   pip uninstall akshare mini-racer py-mini-racer
   ```

   `cml doctor --fix` 已删除（只修 mini-racer 冲突）。

4. **宏观自愈**：下次 `macro_indicators` 会重写错误的 `m2_yoy`、并从央行回填 `social_financing`，无需单独迁移脚本。

5. **日内可选**：`[minute_bars].enabled` 默认 `false`；需要时再开，见 [configuration — minute_bars](configuration.md#minute_bars)。

## 下一步

- [快速开始](quickstart.md) — 首次 init 与日更
- [配置参考](configuration.md) — 调优 workers、TDX 服务器、调度组
