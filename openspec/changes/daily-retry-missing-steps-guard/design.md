# daily-retry-missing-steps-guard: Design

## Architecture Overview

本变更旨在解决 `daily` 系列任务在面临意外进程级崩溃时，`cne retry` 容易发生的步骤截断（Missing Steps）盲区，构建具备施工图纸比对与自愈能力的重试状态机：

```
                    崩溃现场与重试自愈全景对比
                               │
┌──────────────────────────────┴──────────────────────────────┐
│ 【传统缺陷流程 (现状)】                                      │
│                                                             │
│  Step 1 ──▶ Step 2 ──▶ 💥 Step 3 (崩溃)                      │
│                          └── 未生成: Step 4, Step 5         │
│                                                             │
│  cne retry --run-id 启动                                    │
│  仅重试 Step 3 ──▶ 成功 ──▶ 发现无其他 failed 批次          │
│  ❌ 误将 Run 置为 success (Step 4, Step 5 被永久截断漏跑)    │
└──────────────────────────────┬──────────────────────────────┘
                               │
┌──────────────────────────────┴──────────────────────────────┐
│ 【本次加固流程 (Design)】                                    │
│                                                             │
│  Run 启动时: 固化 metadata["expected_steps"] = [1, 2, 3, 4, 5]│
│                                                             │
│  cne retry --run-id 启动                                    │
│  1. 状态比对: expected_steps - manifest_present = [4, 5]    │
│  2. 优先重试: 重试 Step 3 (成功)                            │
│  3. 自动补跑: 顺次执行 Step 4, Step 5                       │
│  4. 终极门禁: 检查 missing_steps == 0                       │
│  ✅ 所有步骤完整入湖，才合法置为 success                     │
└─────────────────────────────────────────────────────────────┘
```

---

## Key Decisions

### Decision 1: `metadata_json` 实例级快照优先，杜绝配置漂移（Snapshot First）
- **原理**：外部 `cnequity.toml` 属于全局动态配置文件，随着业务迭代随时可能增加或删除 steps。若历史 Run 重试时动态读取当前的 TOML，会导致新步骤错误回放至历史旧任务中，或导致已废弃步骤漏跑。
- **设计**：在 `run_job` 启动时，将本次任务经解析后的确切步骤列表直接以字符串数组写入 `metadata["expected_steps"]`。该字段作为此 Run 终身绑定的施工图纸，即使数周后重试也严格忠实于启动瞬间的意图。

### Decision 2: 双轨制向后兼容与优雅降级（Graceful Degradation to TOML）
- **原理**：存量数据库中的历史 Run 均未持久化 `expected_steps` 字段。若强制要求必须存在该字段，会导致存量数据无法享受自愈保护或直接报错。
- **设计**：
  ```
                          步骤图纸解析链路
                                 │
             ┌───────────────────┴───────────────────┐
             ▼                                       ▼
    【主通道：SQLite 元数据】                【备用通道：当前 TOML 配置】
    meta.get("expected_steps")               (兼容历史存量旧 Run)
             │                                       │
             ├─ 命中 ──▶ 采用图纸                     ├─ daily:<group> ──▶ group.steps
             │                                       ├─ daily ──────────▶ daily_waves.steps
             └─ 缺失 ───────────────────────────────▶├─ init ───────────▶ init_phases
                                                     └─ 查无此组 ────────▶ 安全跳过缺失检查
  ```

### Decision 3: 未知/废弃 Group 的安全熔断保护（Fail-Safe on Unknown Groups）
- **原理**：历史 Run 的 `job_name`（如 `daily:old_group`）在当前 `cnequity.toml` 中可能已被完全剔除。若直接按字典索引会导致 `KeyError` 炸死整个调度批次。
- **设计**：使用 `self.config.schedule_groups.get(group_name)`。若为 `None`，记录 warning 日志并返回 `[]`，使旧任务安全退化为仅重试 Manifest 中记录在案的批次，绝不中断进程。

### Decision 4: 终极竣工门禁拦截（Strict Completion Gate）
- **原理**：无论批次状态如何，只要 DAG 存在未曾生成过批次的步骤，该 Run 的生命周期就未完整闭环。
- **设计**：在 `_retry_run_locked` 结尾处：
  ```python
  remaining_missing = self._missing_run_steps(run_id)
  if remaining_missing:
      status = "failed"
  ```
  彻底杜绝在缺失步骤未跑通时被错误标记为 `success`。

---

## Detailed Data Flow & Execution Sequence

```
_retry_run_locked(run_id, trade_date)
  │
  ├─ 1. 解析施工图纸: expected_steps = self._resolve_expected_steps(run_id)
  │
  ├─ 2. 识别缺失步骤: missing_steps = self._missing_run_steps(run_id)
  │
  ├─ 3. 提取失败批次: failed = self._retryable_batches_with_worker_budget(run_id)
  │
  ├─ 4. 执行微创重试: 重试 failed 中的 worker 批次与 non-worker 步骤
  │
  ├─ 5. 补跑缺失步骤:
  │      for step in missing_steps:
  │          self._run_step(step, trade_date, run_id, context)
  │
  ├─ 6. 重新审计状态:
  │      incomplete_count = manifest.incomplete_batch_count(run_id)
  │      remaining_missing = self._missing_run_steps(run_id)
  │
  └─ 7. 终极门禁评定:
         if incomplete_count == 0 and not remaining_missing:
             finish_run(run_id, "success")
         else:
             finish_run(run_id, "failed")
```
