# orchestrator 模块

路径：`src/cn_market_lake/orchestrator/`

采集编排核心：Job 执行、Wave DAG、批级 manifest、多进程 worker、init 分阶段、compact 门禁与 run 锁。

---

## 文件一览

| 文件 | 职责 |
|------|------|
| `engine.py` | `JobEngine`：`run_job()`, `run_init_phases()`, `resume_init()`, retry |
| `registry.py` | `STEP_REGISTRY`, `@register_step`, `StepEntry` |
| `deps.py` | Step 拓扑分层；finalize 始终最后 |
| `manifest.py` | SQLite：`ingestion_runs`, `ingestion_batches` |
| `worker_pool.py` | `daily_bars` 等 symbol-batch 并行；`BrokenProcessPool` 串行恢复且不降级已 success 的 batch |
| `init_phases.py` | Init 阶段 → step 列表、backfill 标志 |
| `compact_gate.py` | 有 incomplete batch 时跳过数据集 compact |
| `run_lock.py` | 跨平台文件锁，防并发 run/retry |
| `__init__.py` | 导出 `JobEngine` |

---

## JobEngine

### run_job(job_name, steps=..., backfill=..., retry_failed_only=..., run_id=...)

主入口。流程：

1. **非 retry 入口先 `reconcile_orphaned_runs`**：按 batch heartbeat / run start 关闭
   超时仍 `running` 的僵尸 run（跳过仍持有 `meta/locks/{run_id}.lock` 的进程）
2. 非 retry：检查交易日（daily）、`start_run`
3. 解析 steps（显式传入或 config waves）
4. `deps.topological_levels()` 分层执行
5. 每层内 parallel step 用线程池；`requires_workers` 走 `worker_pool`
6. finalize steps：`compact` → `derive_adj_factors` → `audit`
7. `finish_run(success|failed)`；`try/finally` 保证进程中断时仍以 `failed` /
   `interrupted` 收口，避免永远停在 `running`
8. **retry 全绿路径**：若失败 batch 为空且 incomplete=0，直接 `finish_run(success)`
   （修复「最后一步成功后崩溃、run 仍 running」的僵尸）

### run_init_phases(trade_date, resume, resume_run_id, keep_going)

按 `cfg.init_phases` 顺序执行；每 phase 可 backfill。失败时默认停止（`keep_going` 继续）。

### resume_init(run_id)

init 专用 retry：补跑失败 batch + 缺失 phase。

---

## registry.py

```python
@register_step(
    "daily_bars",
    depends_on=["instruments", "trading_calendar"],
    group="bars",
    requires_workers=True,
)
def step_daily_bars(cfg, trade_date, run_id, ctx) -> dict: ...
```

`StepEntry` 字段：

| 字段 | 说明 |
|------|------|
| `fn` | `(Config, date, run_id, ctx) -> dict` |
| `depends_on` | 同 wave 内前置 step |
| `group` | 调度组分类；`finalize` 组最后执行 |
| `requires_workers` | 是否 ProcessPool |
| `parallelizable` | 是否可与同层其他 step 并行 |

---

## manifest.py

SQLite WAL 模式。

### ingestion_runs

`run_id`, `job_name`, `status`, `started_at`, `finished_at`, `meta` JSON

### ingestion_batches

`batch_id`, `run_id`, `dataset`, `status`, `symbol_start`, `symbol_end`, `rows`, `error`, `heartbeat_at`

Batch 状态：`pending` → `running` → `success` | `failed` | `stale`

- `advance_stale_batches()` / `advance_batch_timeouts()`：超时 running → stale → failed（retry 前调用）
- `reconcile_orphaned_runs(stale_after_seconds=batch_stale_seconds)`：关闭无活动的
  `running` run；活动时钟 = `max(run.started_at, batch heartbeat/started)`；
  更新幂等（`WHERE status='running'`）
- `count_stale_running_runs()`：只读信号，供 `cml status` 报告孤儿数
- `run_summary(run_id)`：供 `cml status` 输出

运维也可显式：`cml clean --reconcile-runs`（默认窗口同 `batch_stale_seconds`）。

---

## worker_pool.py

`daily_bars` backfill/init：

1. 从 instruments 取 symbol 列表，按 `batch_size` 切分
2. 每 batch 在子进程执行 adapter 拉取 → staging
3. 子进程独立 TDX 连接（主进程不可 fork 共享连接）
4. manifest 记录每 batch 状态，支持 `cml retry` 粒度

分页：突破 TDX 单次 800 条限制，增量模式早停于水位之后。

---

## init_phases.py

```python
INIT_PHASE_STEPS = {
    "phase1_reference": ["instruments", "trading_calendar"],
    "phase2a_corporate_actions": ["corporate_actions"],
    "phase2c_daily_bars_backfill": ["daily_bars"],
    "phase3_index_and_status": ["index_bars", "trading_status"],
    "phase4_finalize": ["compact", "derive_adj_factors", "audit"],
}
```

`INIT_BACKFILL_PHASES` 决定哪些 phase 对 step 设 `backfill=True`。

---

## compact_gate.py

compact 前检查：若本 run 某数据集存在 `running`/`failed`/`stale` batch，**整个数据集**本 run 不合并，不推水位。

---

## run_lock.py

`RunLockError`：同一 `run_id` 或全局 compact 锁冲突时抛出。底层用包根 `file_lock`（POSIX `flock` / Windows `msvcrt`）。

---

## 相关文档

- [数据流](../architecture/data-flow.md)
- [steps 模块](steps.md)
- [manifest 与 retry](../operations/troubleshooting.md)
