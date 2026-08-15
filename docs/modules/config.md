# config 模块

路径：`src/cn_market_lake/config/`

将 `cn-market-lake.toml` 解析为类型化 `Config`，并在启动前校验引用完整性。

**TOML 键与语义**见 [配置参考](../getting-started/configuration.md)。

---

## 源码地图

| 文件 | 职责 |
|------|------|
| `loader.py` | `load_config()`, `validate_config()`, `Config` |
| `bootstrap.py` | `cml config init`：从包内模板写出用户 toml |
| `templates/cn-market-lake.example.toml` | 随包示例（与仓库 `configs/` 副本同步） |
| `__init__.py` | 导出 `Config`, `load_config`, `validate_config`, `write_user_config` |

派生路径：`cfg.staging_root` / `curated_root` / `derived_root` / `meta_root` / `manifest_path`。  
限速：`cfg.rate_limit(source)` → `adapters/throttle.py`。
