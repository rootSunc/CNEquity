# 模块索引

源码根目录：`src/cn_market_lake/`。包按**数据层与职责**划分，遵循「adapter 薄、step 编排、domain 契约」原则。

```
cn_market_lake/
├── __init__.py
├── __main__.py          → cli()
├── file_lock.py         跨平台文件锁（POSIX flock / Windows msvcrt）
├── config/              配置加载
├── domain/              Schema、数据集注册、符号、限速
├── adapters/            外部数据源 I/O
├── orchestrator/        引擎、manifest、worker pool
├── steps/               注册采集步骤（按数据层分文件）
├── storage/             Parquet 湖读写、布局、水位
├── derive/              派生计算
├── quality/             审计与 failover
├── query/               消费层 API
├── serve/               只读湖面板（cml serve）
└── cli/                 Click 命令
```

---

## 依赖方向

```
cli → orchestrator → steps → adapters
                    ↓
              storage / derive / quality
                    ↓
                  query（只读 curated/derived）
```

`query` 不依赖 `orchestrator` 执行路径，仅读湖内文件与 `Config` 路径。

`serve` 只读，且只读**已落盘的产物**（注册表、目录布局、`meta/stats`、`meta/quality`、manifest）——不扫 curated，不写湖。

---

## 模块文档

| 模块 | 文档 | 核心文件 |
|------|------|----------|
| config | [config.md](config.md) | `loader.py` |
| domain | [domain.md](domain.md) | `schemas.py`, `datasets.py`, `symbols.py` |
| adapters | [adapters/README.md](adapters/README.md) | 各源子包 |
| orchestrator | [orchestrator.md](orchestrator.md) | `engine.py`, `manifest.py` |
| steps | [steps.md](steps.md) | `reference.py` … `finalize.py` |
| storage | [storage.md](storage.md) | `parquet.py`, `layout.py`, `state.py` |
| derive | [derive.md](derive.md) | `adj_factors.py`, `trading_status_history.py` |
| quality | [quality.md](quality.md) | `audit.py`, `failover.py` |
| query | [query.md](query.md) | `reader.py`, `views.py` |
| serve | [serve.md](serve.md) | `app.py`, `lake.py` |
| cli | [cli.md](cli.md) | `main.py` |

---

## 扩展点

| 要做什么 | 改哪里 |
|----------|--------|
| 新数据集 | `domain/schemas.py` + `datasets.py` + `steps/<layer>.py` + adapter |
| 新数据源 | `adapters/<source>/` + `configs` sources 段 |
| 新 CLI 命令 | `cli/main.py` |
| 新质量检查 | `quality/dataset_checks.py` 或 `cross_checks.py` |
| 调度变更 | `configs/cn-market-lake.toml` waves/groups |

---

## 导入副作用

`import cn_market_lake.steps`（CLI 与 engine 启动时）会执行各 step 模块的 `@register_step`，填充 `STEP_REGISTRY`。新增 step 必须在 `steps/__init__.py` 中 import。

---

## 相关文档

- [架构总览](../architecture/overview.md)
- [新增数据集](../development/adding-dataset.md)
