# cli 模块

路径：`src/cnequity/cli/`

Click 命令组 `cne` 的实现（`pyproject.toml` `[project.scripts]` 与 `__main__.py` 都指向
`cnequity.cli.main:cli`）。

**完整命令与参数**见 [CLI 参考](../reference/cli.md)；入门流程见 [快速开始](../getting-started/quickstart.md)。

---

## 源码地图

命令按**做什么**分文件，不按写作顺序。`main.py` 只负责把它们 import 进来完成注册——
所以它是唯一知道整个命令面的地方，也是唯一需要改的注册点。

| 文件 | 内容 |
|------|------|
| `main.py` | 入口：import 各 `*_cmds` 模块完成注册 |
| `_root.py` | `cli` 与 `run` 两个命令组本身、`--help` 分区、命令迁移提示（放在 main 会形成循环 import） |
| `_shared.py` | `--config` 装饰器、配置解析、进度日志、退出码映射 |
| `setup_cmds.py` | `init` `config` `doctor`——有湖之前会碰到的 |
| `run_cmds.py` | `run daily` `run events` `run retry` |
| `backfill_cmds.py` | `backfill` 及其分片、限定域、staging 恢复 |
| `maintain_cmds.py` | `run compact` `run clean` `derive` `stats` |
| `quality_cmds.py` | `audit` `verify`（含 `--bars` / `--runs`） `status` `sources` |
| `govern_cmds.py` | `contract` `profile` `snapshot`——可复现性那一面 |
| `consume_cmds.py` | `query` `serve` `mcp` |
| `delisted_cmds.py` | `delisted status` / `backfill`（重建目录在 `scripts/delisted_ops.py`） |
| `demo.py` | demo 编排 |

`--config` 由 `_shared.config_option` 统一提供：原先它被手写了 34 次，每一处都可以各自漂移。

`run` 组和 `cli` 一样定义在 `_root.py`：`run_cmds` 和 `maintain_cmds` 都要往上挂命令，
挂在任何一个里都会让另一个反向 import。

## 顶层分区

`_root.SECTIONS` 决定 `cne --help` 的分节与顺序——按一个湖被使用的顺序，而不是命令被写出来的顺序。
没有列进去的命令会掉进 "Other" 分区并让测试失败，所以新增命令必须同时决定它属于哪一节。

`_root.MOVED` 记录改过拼写的命令：输入旧名时直接给出新写法，而不是 Click 默认的
"No such command"。它比隐藏别名更诚实——`cne servers` 声明 0.9.0 删除，到 0.10 还在。
子命令组用 `_root.moved_hints()` 拿到同样的行为（`cne ths-official snapshot` → `capture`）。

测试打桩要指向**实际绑定该名字的模块**（`cnequity.cli.quality_cmds.JobEngine`，
不是 `cnequity.cli.main.JobEngine`）。`main.py` 刻意不再 re-export 这些内部名，
所以打错模块会直接 `AttributeError`，而不是默默给一个没人查的名字打桩。

| 关注点 | 位置 |
|--------|------|
| 配置路径解析 | `_shared.resolve_config_path`（缺省时引导 `cne config create`） |
| step 注册 | 启动时 `main.py` 里 `import cnequity.steps` |

### 退出码（供 cron / Task Scheduler）

| 场景 | 退出码 |
|------|--------|
| 成功 / `skipped_non_trading_day` | 0 |
| run / audit / init 失败 | 1 |
| `status --datasets` 有 STALE | 1 |
| `audit --full` UNHEALTHY | 1 |
