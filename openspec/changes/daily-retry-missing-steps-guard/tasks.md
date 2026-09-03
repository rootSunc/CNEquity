# daily-retry-missing-steps-guard: Tasks

## 1. 任务启动施工图纸持久化 (Metadata Snapshot)

- [x] 1.1 在 `src/cnequity/orchestrator/engine.py` 的 `run_job` 中，在调用 `manifest.start_run` 前将 `all_steps` 写入 `metadata["expected_steps"]`。
- [x] 1.2 确保通过 `run_cmds.py`（无论是 `--group`、`daily` 还是动态 `steps`）启动的任务均能正确持久化该字段。

## 2. 通用预期与缺失步骤解析实现 (Resolution & Fallback)

- [x] 2.1 在 `engine.py` 中实现 `_resolve_expected_steps(run_id)`：
  - 优先读取 `metadata["expected_steps"]`；
  - 兜底回退：若字段缺失，根据 `job_name` 分流解析当前 TOML 中的 `schedule_groups`、`daily_waves` 或 `init_phases`；
  - 针对未知/已删除 group 增加 `None` 保护与 warning 日志，安全返回空列表。
- [x] 2.2 在 `engine.py` 中实现通用的 `_missing_run_steps(run_id)`，比对 `expected_steps` 与该 Run 中已存在的批次，找出缺失步骤。

## 3. 重试执行链自动补跑与终极门禁 (Execution & Gate Hardening)

- [x] 3.1 修改 `engine.py` 中的 `_retry_run_locked`：将原先仅针对 init 的 `missing_init` 替换为通用的 `missing_steps = self._missing_run_steps(run_id)`。
- [x] 3.2 在执行完已有失败批次后，遍历 `missing_steps` 并调用 `_run_step` 进行断点自动补跑。
- [x] 3.3 加固结尾的状态评定与门禁：检查 `incomplete_batch_count == 0 and not self._missing_run_steps(run_id)`，存在未完成缺失步骤时坚决置为 `failed`。

## 4. 自动化测试与场景验证 (Testing & Verification)

- [x] 4.1 在 `tests/unit/test_missing_steps_guard.py` 中编写单元测试套件：
  - 测试 1：新建 Run 正确持久化 `expected_steps` 到 metadata；
  - 测试 2：中途崩溃断点重试时，自动识别并补跑下游从未执行的步骤；
  - 测试 3：历史无元数据旧 Run 能优雅回退至当前 TOML 配置补齐步骤；
  - 测试 4：历史 Run 遭遇未知/已废弃 group 时安全降级，不抛出异常；
  - 测试 5：缺失步骤如果补跑依然失败，严格阻断 success 状态。
- [x] 4.2 执行现有 retry 测试套件（如 `test_retry_hardening.py`）确保零回归。
