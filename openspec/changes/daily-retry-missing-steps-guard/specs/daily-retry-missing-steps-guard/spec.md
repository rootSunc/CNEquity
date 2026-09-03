# daily-retry-missing-steps-guard: Specification

## Requirements

### Requirement: Persist Expected Steps Snapshot on Run Initiation
当 `JobEngine.run_job` 启动任意非重试任务时，系统必须将本次 Run 解析出的全部步骤列表持久化记录在 Run 的元数据中，形成不可变的施工图纸快照。

#### Scenario: New daily group run records expected steps
- **Given** 调度组 `daily:core` 包含步骤 `[instruments, trading_calendar, daily_bars]`
- **When** `engine.run_job("daily:core", ...)` 启动
- **Then** `manifest.get_run_metadata(run_id)` 中包含 `expected_steps = ["instruments", "trading_calendar", "daily_bars"]`

---

### Requirement: Missing Steps Resolution with TOML Graceful Degradation
在执行重试时，系统必须能够准确比对出该 Run 从未在 Manifest 中生成过批次的步骤；对于未持久化快照的历史旧 Run，系统必须能够从当前 TOML 配置中安全推导，遇到未知 Group 时安全跳过。

#### Scenario: Missing steps identified from persisted snapshot
- **Given** 某个 Run 的元数据中固化了 `expected_steps = [A, B, C]`
- **And** 该 Run 在执行中途崩溃，Manifest 中仅记录了步骤 `A` 的批次
- **When** 引擎调用 `_missing_run_steps(run_id)`
- **Then** 返回缺失步骤列表 `[B, C]`

#### Scenario: Legacy run gracefully falls back to TOML configuration
- **Given** 某个历史旧 Run `job_name = "daily:capital"`，其元数据中无 `expected_steps` 字段
- **When** 引擎调用 `_missing_run_steps(run_id)`
- **Then** 引擎自动根据 `cnequity.toml` 的 `schedule_groups["capital"].steps` 计算缺失步骤

#### Scenario: Unknown/deprecated group in legacy run does not raise exception
- **Given** 某个历史旧 Run `job_name = "daily:deprecated_group"`，且该组在当前 TOML 中已不存在
- **When** 引擎解析预期步骤
- **Then** 引擎输出 warning 日志并安全返回空列表，不抛出异常

---

### Requirement: Automatic Execution of Missing Steps During Retry
在执行 `cne retry` 时，系统除重试已有失败批次外，必须自动按顺序执行所有未曾启动的下游缺失步骤。

#### Scenario: Broken DAG automatically healed during retry
- **Given** 某个 Run 在步骤 1 崩溃，步骤 2 未能生成批次
- **When** 对该 Run 执行 `cne retry --run-id <run_id>`
- **Then** 步骤 1 被重试修复，紧接着步骤 2 被自动调用 `_run_step` 补齐入湖

---

### Requirement: Strict Completion Gate Blocking False Success
若在重试执行后，DAG 中仍有未跑通的缺失步骤，系统必须绝对禁止将该 Run 标记为 `success`。

#### Scenario: Persistent missing step leaves run in failed status
- **Given** 某个 Run 存在缺失步骤 2，且在补跑步骤 2 时再次抛出异常
- **When** 重试循环结束进行状态评定
- **Then** 该 Run 的状态被明确标记为 `failed`，绝不因为前序步骤成功而判定为 `success`
