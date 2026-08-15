# 贡献指南

**只用数据的用户**：`pip install cn-market-lake` → 见 [快速开始](docs/getting-started/quickstart.md)。  
本文面向向仓库提交代码的贡献者。

安全问题请走 [SECURITY.md](SECURITY.md)，不要开公开 issue。

提较大功能前，请先看 [定位与差异](docs/comparison.md)（本仓库只做数据层）和
[许可与数据合规](docs/legal-and-data-sources.md)。

## 适合第一次贡献的方向

不需要先理解整个编排引擎。以下工作可以独立完成，而且对用户可见：

- 为现有数据集补一条离线 recipe、查询示例或字段说明
- 给已有 adapter 增加 schema 边界测试和清晰的源限制说明
- 修复中英文文档、CLI help 与实际默认值之间的漂移
- 改进 `cml demo`、`cml status` 或 dashboard 的错误提示

数据源接入需要同时提交来源、保留期、限流和合规说明；请不要把真实数据文件提交到仓库。
较大的方向可以先在 issue / discussion 中确认范围。

## 环境

```bash
python -m venv .venv && source .venv/bin/activate
python -m pip install --upgrade pip   # PEP 735 --group 需要 pip >= 25.1
pip install -e . --group dev
# 无 extras：运行时依赖全部随包安装
# 见 docs/getting-started/installation.md
```

请勿提交 `configs/cn-market-lake.toml`、`data/`、`logs/`。

```bash
ruff format .
ruff check .
pytest                 # 全部
pytest tests/unit      # 快速
pytest tests/integration
```

## 约定

- 代码在 `src/cn_market_lake/`，按职责拆分（`domain`、`adapters`、`orchestrator`、
  `steps`、`storage`、`derive`、`quality`、`query`、`config`、`cli`）。
- Step 按 L0–L8 分层放在 `steps/`；新模块需在 `steps/__init__.py` 中 import 以注册。
- 新数据集：在 `domain/schemas.py` 声明 schema + 主键、分区键，以及溯源列
  （`source`、`data_version`、`fetched_at`）。
- Adapter 保持薄（I/O 与源侧 quirks）；归一化放在 `steps/` / `domain/`。
- 单测默认离线；需要联网的测试须明确标记。
- 非平凡架构取舍写入 `docs/adr/`（复制 `0000-template.md`；ADR 正文保持英文）。

## 新增数据集清单

1. Schema + 主键 + 分区键
2. `@register_step`，填好 `depends_on` / `group` / `requires_workers`
3. 写时 schema 校验通过
4. 归一化单测 + 至少一个边界用例
5. 更新 [`docs/datasets/catalog.md`](docs/datasets/catalog.md) 与
   [`docs/datasets/sources.md`](docs/datasets/sources.md)
