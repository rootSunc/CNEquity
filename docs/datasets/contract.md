# 数据契约

`DatasetSpec`、`DATASET_SCHEMAS` 与 `PRIMARY_KEYS` 是 CNEquity 的单一事实
来源。它们可以导出为稳定 JSON，供 revision、下游作业和 CI 保存及比较：

```bash
cne contract show daily_bars
cne contract show --out meta/dataset-contract.json
cne contract validate meta/dataset-contract.json
cne contract diff meta/old-contract.json meta/dataset-contract.json
```

从 0.8.0 起，每个发布版本的完整契约保存在仓库根目录的 `contracts/`
目录中（例如 `contracts/v0.8.0.json`），作为下一版本发布审阅的稳定基线。
0.7.3 及更早版本没有导出机器可读契约，因此 0.8.0 是首个可用于跨版本
`contract diff` 的基线。

省略 `show` 的数据集参数会输出 42 个数据集的完整契约。`export` 输出包含
顶层 `fingerprint`（SHA-256）；同一 registry 在不同进程中导出的 fingerprint
一致。`contract validate` 无参数时校验当前 registry；给出文件时校验便携式契约，
加 `--against-registry` 才要求它与当前 registry 完全一致。

每个数据集记录包含：

- `schema_version`：列形状的版本。添加向后兼容列不必提升它；移除列或改变
  类型必须提升并在发布说明中标明。
- `contract_level` 与 `compatibility`：当前注册数据集默认是 `stable` / `additive`。
- `pit_grade` 是 0.x 兼容别名（`none` / `strict` / `partial`）；新的
  `pit_quality` 使用 `strict` / `reconstructed` / `snapshot_only`。当前
  `financial_statement_items` 及历史股东回填为 `reconstructed`/`partial`，
  `announcement_index` 才是严格 PIT。`availability_col` 默认是
  `announce_date`。
- `pit_modes` 固定为 `strict` / `best_effort`。strict 只允许在截止日已知的
  vintage；best-effort 可保留回填现值，但返回 `pit_is_exact=False`。
- `pit_storage_columns` 是可选双时态列：`available_at`、
  `source_published_at`、`observed_at`、`revision_id`。旧文件缺列由读侧补齐，
  不因此变成不可读；`scripts/migrate_pit_vintages.py` 可用 dry-run/apply
  幂等补列。
- `unit_contract`：价格、股数、金额、比例等数值的规范单位；没有特殊数值
  口径的表使用 `canonical`。
- `schema` / `columns`、`primary_key` / `primary_keys`：同一事实的兼容别名。

`diff` 将新列和新数据集标为 compatible；删列、改类型、改主键、单位变化、
PIT/可用性变化、历史获取语义变化（包括 `snapshot`、回填源、源端历史底）
均标为 breaking。发现 breaking 变化时命令默认退出码为 1；检查报告但允许
继续时使用 `--allow-breaking`。

Python 调用：

```python
from cnequity.domain.contracts import (
    contract_fingerprint,
    dataset_contract,
    diff_contracts,
    export_contract,
    validate_contract,
)

row = dataset_contract("daily_bars")
fingerprint = contract_fingerprint("daily_bars")
document = export_contract()  # dict; export_contract(path) also writes JSON
assert validate_contract(document) == []
changes = diff_contracts(document, document)
assert not changes["is_breaking"]
```

这些元数据只描述读取/演进契约，不会重写已有 Parquet 数据行。`data_version`
仍专门用于记录数值语义重解释（例如日线成交量单位），与
`schema_version` 分开。
