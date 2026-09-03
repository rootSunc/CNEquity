# daily-retry-missing-steps-guard: Proposal

## Why

在当前 `CNEquity` 引擎的重试逻辑中，存在一个极其危险的**隐形步骤截断漏洞（Missing Steps 盲区）**：

1. **缺陷现状**：
   - 查阅 `engine.py:883-887`：
     ```python
     missing_init = (
         self._missing_init_steps(run_id)
         if retry_batch_ids is None and self._is_init_run(run_id)
         else []
     )
     ```
   - 引擎中只有 `_is_init_run(run_id)`（即 `job_name == "init"`）才会计算缺失步骤。对于所有的日常任务（全量 `daily` 以及各业务调度组 `daily:core`, `daily:capital`, `daily:fundamentals` 等），**完全没有缺失步骤检查**！

2. **事故场景**：
   - 假设某个日常组（如 `daily:core`）包含 `[instruments, trading_calendar, trading_status, daily_bars, index_bars, corporate_actions, trading_status_derive]` 等 7 个步骤；
   - 若在执行第 4 步 `daily_bars` 时，进程发生严重异常（OOM 被杀、OS SIGKILL、未捕获异常退出等），后续的 `index_bars`、`corporate_actions` 等步骤**根本未来得及在 Manifest 数据库中生成批次记录**；
   - 随后通过 `cne retry --run-id <id>` 重试该任务：引擎仅能捞出已记录的 `daily_bars` 批次进行重试；
   - 当 `daily_bars` 修复成功后，引擎查询未完成批次数返回 0，由于缺失步骤列表为空，引擎便**轻率地将该 Run 置为 `success`**；
   - **后果**：后续原本属于该 DAG 的步骤被永久截断漏跑，数据湖产生永久性暗坑，且系统再也不会重试或发出告警。

3. **根因**：
   - `cne retry` 最初是针对单个批次微创与 `init` 续跑设计的，日常任务在启动时**未将预期的完整施工图纸（`expected_steps`）固化进元数据**；
   - 导致重试引擎“只知有 Batch，不知有 DAG”，只要已有坏砖修好就误以为整栋大楼已竣工。

本变更旨在为 `daily` 系列任务引入**全链路步骤防截断自愈机制**，确保任何因崩溃中断的 DAG 在重试时都能完整补齐缺失步骤，守住数据湖的完整性底线。

---

## What Changes

### 1. 启动时施工图纸快照固化（Record Expected Steps）
- 在 `JobEngine.run_job` 启动 Run 时，将本次任务预期的完整步骤清单（`all_steps`）持久化写入 `metadata_json["expected_steps"]`；
- 该快照使每一次 Run 成为自解释、不可变的独立实体，彻底杜绝未来 TOML 配置文件升级或修改时产生的“配置漂移”污染历史。

### 2. 通用缺失步骤解析与优雅降级（Resolve Expected Steps with TOML Fallback）
- 在 `JobEngine` 中新增通用解析方法 `_resolve_expected_steps(run_id)` 与 `_missing_run_steps(run_id)`，替换原先狭隘的 `_missing_init_steps`：
  - **黄金通道（优先）**：优先读取 `metadata["expected_steps"]` 固化的精确快照（新 Run）；
  - **兜底通道（优雅降级）**：若 SQLite 元数据中缺失该字段（历史存量旧 Run），自动根据 `job_name` 回退至当前 `cnequity.toml` 中对应的 `schedule_groups` 或 `daily_waves` 动态推导 steps；
  - **安全熔断**：若 TOML 中也找不到该 group（如已废弃/重命名的历史 group），记录 warning 并安全跳过缺失检查，绝不抛出 KeyError 导致崩溃。

### 3. 重试执行链自动补跑与终极门禁校验（Auto Execution & Completion Gate）
- 扩充 `_retry_run_locked` 的执行链路：
  - 第一阶段：重试 Manifest 中记录在案的异常批次；
  - 第二阶段：自动按依赖顺序拉起从未生成过批次的下游 `missing_steps` 并补跑；
- **终极竣工门禁**：
  - 重试结束后，若仍有未跑通的缺失步骤，**绝对禁止将 Run 标记为 `success`**，统一置为 `failed`（并在 payload 中明确指出 `missing_steps`），彻底消除静默截断漏跑。

---

## Capabilities

### New Capabilities
- `daily-retry-missing-steps-guard`: 为 `daily` 系列任务引入预期步骤元数据快照、缺失步骤自动检测、下游断点补跑以及严格的竣工门禁防御。

---

## Impact

- **修改模块**：
  - `src/cnequity/orchestrator/engine.py`:
    - `run_job`: 写入 `metadata["expected_steps"]`；
    - `_resolve_expected_steps` / `_missing_run_steps`: 通用步骤解析与 TOML 兜底；
    - `_retry_run_locked`: 纳入 missing steps 补跑与竣工门禁拦截。
- **数据库与配置**：
  - 零 DDL 修改：直接使用已有的 `ingestion_runs.metadata_json`；
  - 零配置文件变更：天然读取现有的 `cnequity.toml`。
- **兼容性**：
  - 100% 向下兼容历史旧 Run（通过 TOML 兜底通道无缝过渡）。
- **测试套件**：
  - 新增 `tests/unit/test_missing_steps_guard.py`，完整覆盖进程崩溃后步骤截断、自动补跑、TOML 优雅降级与门禁拦截场景。
