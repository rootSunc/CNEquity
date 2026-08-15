# 测试

路径：`tests/`

---

## 目录结构

```
tests/
├── conftest.py           # 共享 fixture（tmp_path 配置、mock TDX）
├── unit/                 # ~120 个单元测试文件，离线可跑
└── integration/          # 全链路测试
    ├── test_engine.py
    └── test_engine_validation.py
```

---

## 运行

```bash
pip install -e . --group dev

pytest                      # 全量离线（默认 -m 'not network'）
pytest tests/unit -q        # 快速反馈
pytest tests/integration    # 较慢
pytest -m network           # 仅外网探针（东财 datacenter 列契约等）
```

配置（`pyproject.toml`）：

- `timeout = 120`
- `timeout_method = thread`
- markers: `integration`, `network`
- 默认 `addopts` 排除 `network`；东财 schema 直播探针：

  `pytest -m network tests/unit/test_datacenter_live_contracts.py`

---

## 覆盖领域

| 领域 | 代表文件 |
|------|----------|
| 配置校验 | `test_config_validation.py` |
| 引擎 / manifest | `test_engine.py`, `test_batch_lifecycle.py`, `test_worker_manifest.py` |
| compact / 水位 | `test_compact_gate.py`, `test_state.py`, `test_instruments_compact.py` |
| Steps / adapters | `test_m3_steps.py`, `test_m3_adapters.py`, `test_bars_pagination.py` |
| Schema / 注册表 | `test_dataset_registry.py`, `test_schema_validation.py` |
| EM datacenter 列契约 | `test_datacenter_contracts.py`, `test_datacenter_live_contracts.py` (`-m network`) |
| 质量 | `test_audit_datasets.py`, `test_cross_checks.py`, `test_adj_factor_reconciliation.py` |
| 消费层 | `test_reader.py` |
| CLI | `test_cli_init.py` |
| Baostock 回填 | `test_baostock_valuation.py`, `test_baostock_st_history.py` |
| trading_status | `test_trading_status_history.py`, `test_trading_status_st_backfill.py` |

---

## conftest 约定

- 使用 `tmp_path` 构建迷你数据湖
- TDX 默认 mock 或 `allow_mock`，避免 CI 依赖外网
- 共享 `Config` fixture 指向临时 `data_root`

---

## 编写新测试

1. 单元测试放 `tests/unit/test_<topic>.py`
2. 文件名与测试函数以 `test_` 开头
3. 不断网：patch `httpx`、TDX quotes 门面或 adapter 入口
4. 覆盖至少：正常路径 + 一个边界/失败 case

---

## 覆盖率

```bash
pytest --cov=cn_market_lake --cov-report=term-missing
```

`pyproject.toml` 已配置 `coverage.run.source`。

---

## 相关文档

- [开发约定](conventions.md)
- [新增数据集](adding-dataset.md)
