# 安装与升级

普通用户从 PyPI 安装即可；修改代码、使用仓库运维脚本或构建文档时再克隆源码。

## 系统要求

| 项目 | 要求 |
|---|---|
| Python | 3.10 或更高；CI 覆盖版本见[升级与反馈](#升级与兼容性) |
| 系统 | macOS、Linux、Windows；Windows 原生 x86-64 路径和文件锁有 CI，32 位及 ARM64 Windows 未验证 |
| 网络 | 真实采集需连接 TDX 与相应 HTTP 来源；安装后可用 sample 离线验证 |
| 磁盘 | 随证券范围、历史深度、频率、原始归档和保留版本增长；另留 staging / 快照空间 |

## 从 PyPI 安装

推荐在独立 Python 环境执行：

```bash
python -m pip install cnequity
cne --version
cne doctor
```

一次安装包含全部运行时 Python 依赖，**无需 extras**。可选来源仍需配置开关、适用的凭证或外部运行环境；依赖装好不表示源可达或拥有访问权限。

下一步直接进入[快速开始](quickstart.md)，运行 `cne init`。

### Windows 路径

PowerShell / cmd 下也使用 `cne`。例如正式湖放到 D 盘：

```powershell
cne config create --data-root D:/cnequity
cne init
```

生成器会处理 TOML 路径转义。PowerShell 5.1 不支持 `&&`，请逐行运行命令。

## 从源码安装

仓库主分支可能领先于稳定 PyPI 版；若要使用 main 上尚未发布的功能，使用源码安装，并记录 commit。

```bash
git clone https://github.com/rootSunc/CNEquity.git
cd CNEquity
python -m venv .venv
```

激活环境（选当前系统的一条）：

```bash
# macOS / Linux
source .venv/bin/activate
```

```powershell
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
```

然后安装：

```bash
python -m pip install --upgrade pip
pip install -e . --group dev
```

`--group` 需要 pip 25.1+。也可用 `uv sync` 安装仓库锁定环境，再通过 `uv run cne ...` 执行。

同花顺公共页面的 hexin-v token 路径还需要 Deno 可执行程序；未安装时该路径会明确报错。它不是基础 TDX demo 的前提，细节见 `adapters/ths/hexin.py`。

只有修改控制台源码才需要 Node/npm；发布包内已带静态资源，见 [frontend 说明](https://github.com/rootSunc/CNEquity/blob/main/frontend/README.md)。文档工具另装 `pip install -r docs/requirements.txt`。

## 生成正式配置

`cne init` 在默认配置不存在时会自动生成，多数情况下不需要单独这一步。想在初始化前修改数据目录等设置时，先运行：

```bash
cne config create
```

默认输出用户级配置 `~/.config/cnequity/config.toml`（当前目录已有 `configs/cnequity.toml` 时写那里），`data.root` 转为绝对路径；文件只写 `extends = "builtin"` 和数据目录，其余设置继承内置默认值（macOS / Windows 默认保守的 `workers=1`）。也可首次创建时加 `--data-root /abs/path/to/lake`。

直接复制模板不会执行路径解析和平台调整，因此优先使用生成命令。继承默认值的配置升级后自动获得新的调度 step；旧的完整配置用 `cne config upgrade` 补上，或用 `cne config upgrade --minimize` 一次性改成继承默认值。个人配置、凭证与数据目录不应提交到仓库。

## 升级已有安装

```bash
pip install --upgrade cnequity
cne config upgrade
cne migrate            # 预览还欠的数据迁移；确认后 cne migrate --apply
```

继承内置默认值（`extends = "builtin"`）的配置不需要改动，这条命令只会报告无事可做。旧的完整配置由 `cne config upgrade` 把新版本加入的调度 step 和调度组补进去，先备份原文件，写完后校验；`--minimize` 则把它改写成继承默认值的形式。先阅读目标版本的[更新日志](../changelog.md)和[升级与反馈](#升级与兼容性)，确认是否需要数据迁移。不要用 `config create --force` 代替配置升级。

旧版本的 `tdx` / `macro` / `nlp` 等 extras 已移除。历史版本的单位或 schema 迁移应按对应发布说明执行，不能把针对旧湖的脚本重复应用到新湖。当前 `httpx` 约束为 `>=0.25`；实际安装版本由环境解析，不能从安装命令推定一个固定版本。

## 依赖与诊断

| 依赖 | 用途 |
|---|---|
| Polars / PyArrow / DuckDB | 数据处理、Parquet、SQL |
| httpx / curl_cffi | HTTP 来源访问 |
| Baostock | 估值、历史 ST、退市行情回填 |
| pandas / openpyxl / xlrd | XLS/XLSX 来源解析 |
| NumPy / pypdf / SnowNLP | 期权计算、发行人公告文本、可选情绪计算 |
| FastAPI / Uvicorn / Click | 控制台服务与 CLI |

TDX 线协议客户端随包内置，无需安装通达信桌面软件。依赖约束以 `pyproject.toml` 为准。`cne doctor` 是离线诊断；真实源连通性请用限定源的 probe，见[源健康度](../operations/source-health.md)。

下一步：[快速开始](quickstart.md) → [初始化](initialization.md) → [配置参考](configuration.md)。

## 升级与兼容性
升级前记录 `cne --version`，阅读[更新日志](../changelog.md)，备份配置与需要长期保留的数据。安装方法见上文。

### 软件与数据版本

软件版本、数据集 schema 版本与数据 revision 含义不同。软件升级不代表历史数据已迁移；数据修正也不一定改变 schema。

0.x 的次版本可能包含有说明的兼容性变化。字段、单位、主键或历史语义变化应以对应版本的迁移说明为准，不能只依据列名相同继续拼接。

### 升级步骤

1. 核对新版本的范围、已知限制与迁移要求。
2. 更新软件后执行 `cne config upgrade`：把新版本加入的调度 step 和调度组补进本地配置，原文件自动备份，凭证、路径与其他设置不动。想先看改动可加 `--dry-run`；完整差异用 `cne config diff` 查看。
3. 运行 `cne migrate` 预览还欠的数据迁移，确认后 `cne migrate --apply`，再核对状态与质量报告。
4. 使用原有查询验证关键数据口径，再恢复定时任务。

迁移说明见仓库的 [contracts/migrations](https://github.com/rootSunc/CNEquity/tree/main/contracts/migrations)，常用工具见[运行脚本](../operations/scripts.md)。回退软件不一定能回退已迁移的数据，恢复时同时核对配置、软件与数据版本。

### 存储清理

`cne run clean` 先预览 staging、来源快照、日志与历史版本。交互终端接着分两次确认：历史版本，以及已合并且不在使用中的 staging。正在使用的一代和仍在使用的 staging 不会自动删除。`--dry-run`、管道和定时脚本不提问，所以现有定时任务仍然不释放空间。预览里的实际删除字段和 `bytes_freed` 为空或 0，候选大小见 `logical_bytes_selected`。`--force` 只扩大预览范围，显式 `--reconcile-runs` 仍会修改运行状态。

升级后，`cne backfill` 和 `cne run daily` 结束时每个数据集只留最近 5 代（[`[storage.retention].keep_generations`](configuration.md#storage-retention)），更早且没有保留的代目录会被删除，不等待 7 天观察期，也不经过网页确认。要留住更老的代，先 `cne storage hold`，或把该值设为 `0`。交互终端里的 `cne run clean` 确认后也会按它自己的 `--keep-revision-generations` 删除。

物理删除在 `cne serve` 的 `#/storage`。页面先列出可以删除的历史版本，按数据集汇总。可以勾选观察期内尚未到期的历史版本，以及最近 5 代里不是当前指针的版本。当前版本、仍被引用、人工保留、仍在使用的试验，以及缺少收据或归档的项目不能选。确认单要勾选不可撤销和外部任务已停止，再点永久删除。完成后列出删掉的版本、账面大小，以及删除前后的磁盘可用空间；账面大小不是实际腾出的空间。

也可以先“检查待标记项目”，确认后开始至少 7 天观察期。标记不释放空间，满 7 天只产生到期提示，不会自动删除。CLI 的 `storage apply --phase purge` 及试验 purge 已禁用，`--maintenance-window` 不能绕过网页确认。操作者先停止外部查询、其他服务和采集调度。面板会暂停自身读取，但不会替你停止外部进程。页面也列出 staging 运行；已记录成功 compact 且无未完成批次的终态运行可以勾选删除。确认时重新检查引用保留、运行锁、状态和文件指纹，只删除审核清单；中断后可重新审核原计划并恢复。失败或未完成批次、尚未 compact 的运行和 orphan 暂存继续保留，不能凭年龄强制淘汰。来源快照和日志仍仅报告，未提供网页删除。

升级后重启 serve 才会加载新的路由和保护机制。详细约束见[版本生命周期命令](../reference/cli.md#cne-storage)。

归档原目录也有独立观察期。完整快照携带保留依据和外部依赖声明，恢复后重新绑定引用；涉及生命周期登记的增量包暂时拒绝，应改用完整快照。归档和快照均不会自动解除既有保护。

### 反馈问题

在 [Issues](https://github.com/rootSunc/CNEquity/issues) 提供版本、脱敏配置片段、最小复现和错误信息。不要上传凭证、个人湖、完整运行日志或私人实验记录。安全问题见 [SECURITY.md](https://github.com/rootSunc/CNEquity/blob/main/SECURITY.md)。
