## Context

参见 proposal.md（Why）以了解动机。当前事实基线（2026-09-01 实测于 datalake 湖）：

- `trading_status` 是 `snapshot` 语义数据集：daily 步骤经 `fetch_incremental_daily` 只请求 `trade_date`（common.py:436-439），历史缺日仅记录为 `coverage_gap` finding，从不回补 ── 因此"每日抓当日停牌快照"无法覆盖历史停牌。
- `cne backfill trading_status`（baostock `st_history.py:76 if tradestatus != "1": continue`；tushare 仅在有成交日期产行）**故意跳过停牌日**：只产出 `is_trading=True` 的 normal/ST 行，是 ST 标签回填，不是停牌回填。
- 能反推停牌的 `derive_suspension_history`（`derive/trading_status_history.py`）现用 `CuratedWriter.write_partition` **直写可变 curated 目录**，全函数无 `revisions.commit()`。产物：(1) committed 读者（`load_curated_trading_status` → `collect_parquet_root(committed=True)` 经 `_resolved_root` 解析）不可见；(2) 下一次 compact 从 committed 快照 + 本次 staging 重建分区（`parquet.py:148,217`），会把可变目录里的手工/derive 行冲掉。
- 证据分级已存在：`status_evidence_rank`（trading_status_history.py:61-90）rank 0 = baostock / 已收盘 EastMoney 当日快照（fetched 日期==trade_date 且 >=15:00）/ 退市；rank 1 = `derived_bar_gap`；rank 2 = 滞后 EastMoney 当前态盖旧日期。但该分级**只用于 derive 自己的合并**，compact 的通用去重（`domain/canonical.py:28-55`）按 `fetched_at` 升序 + source rank(base=0/primary=2) 排序、`keep="last"` → 恰好更新的普通快照会盖掉 derived 停牌行。
- 每日 bar 路径已有未收盘防护：`_reject_unfinished_daily_bar_window`（bars.py:51）拒绝 15:00 前抓当日。derive 路径**没有**该防护。

**关于数据集边界与提交粒度的澄清（避免对"是否破坏原有设计"的误读）：**

- `trading_status` 与 `daily_bars` 是**两个独立 dataset，各自走独立的 `staging → compact → commit` 流程、各自生成 revision 提交**——在 `_compact_locked`（finalize.py:345-473）中按 dataset 逐个 `compact_dataset` + `revisions.commit(ds)`。现有代码（derive 缺陷修复前）从不存在"is_trading=false 与 daily bar 同批统一提交"的路径。
- 二者在 daily 管线中的耦合只在**门禁/校验层**，而非提交合并层：daily_bars 的 interior-gap 校验（`_staged_daily_bar_missing_keys`，bars.py:1029）与所有权分类（`classify_daily_bar_ownership` 的 `trading_status_non_trading` 分支，common.py:819）要求 **已提交的 `is_trading=false` 记录**作为缺失会话的显式豁免证据，daily_bars 批次满足该条件后才允许 checkpoint。
- 本变更**不改动 dataset 边界、不合并提交、也不做任何"分拆提交"**：只把 derive 的写路径从直写可变目录改为走 trading_status 自身的 staging→compact→commit 通道，使反推停牌行成为 daily_bars 门禁所称的"已提交豁免证据"。daily_bars 的批次门禁语义不变（见 Non-Goal：interior-gap "缺失 ≠ 停牌" 保持）。

## Goals / Non-Goals

**Goals:**
- 让 `derive_suspension_history` 的反推行通过 `staging → compact → commit` 正式发布，对 committed 读者可见且不被后续 compact 冲掉。
- 为 derive 补上"当日未收盘拒绝"防护（与 daily_bars 同款），杜绝盘中把"今天没 bar"误判为停牌。
- 让 compact 合并 trading_status 时**先按 `status_evidence_rank` 再按 `fetched_at`**，保住 derived 停牌行不被"更新的普通快照"覆盖，同时保留权威源修正 derived 的能力。
- 将反推固化为每日自动环节（挂 daily core group），并保持手动入口（`cne derive trading_status`）行为一致。

**Non-Goals:**
- 不改 daily_bars 的 interior-gap 校验语义（"缺失 ≠ 停牌"，common.py:816 的保守设计保持不变）。
- 不把 `derived_bar_gap` 提升为权威证据（保持 rank 1，可被权威源修正）。
- 不做 EastMoney 历史名单回查（方案 D）——依赖"EastMoney 是否保留很久以前当日名单"这一未验证前提，留作独立后续评估。

## Decisions

### 决策 1：derive 写入改为 staging→compact→commit（方案 A）

`derive_suspension_history` 不再用 `CuratedWriter` 直写可变目录，改为：构造符合 trading_status schema 的 row → `StagingWriter.write_batch(dataset="trading_status", run_id, batch_id, df)` → 由 compact 步骤统一合并发布。

- **理由**：与既有 `step_trading_status`/backfill 的写入路径一致；compact 负责 `dedupe_by_primary_key`、生成修订、原子翻转 `current.json`，保证 committed 读者可见与持久性，天然解决"草稿层不可见 + 被冲掉"两个缺陷。
- **替代方案（拒绝）**：
  - "保留直写并在直写后手动调用 `revisions.commit()`"——绕过 compact 的合并/审计门（source_diff gate 等），且与现有发布管线分叉，风险大于收益。拒绝。
  - "把 `current.json` 手动指向新代"——脆弱、易与并发 compact 竞争。拒绝。
- **实现形态**：由于 `trading_status` 是月分区，derive 应沿用 `_STATUS_SPEC.partition_for(td)` 生成分区值，写入 staging 后由 compact 按既有月分区合并（`compact_dataset` 已支持 `partition_col="trade_date"` + 月粒度）。staging batch_id 建议 `derived-<迭代序号>`，可复用 backfill 的 `_finish_backfill_run` 路径（若经 CLI）或注册成独立 step 后在 daily run 中被 compact 覆盖。
- **不影响既有语义**：本决策只改 derive 的写通道，不合并/拆分 trading_status 与 daily_bars 的 dataset 边界或提交粒度（详见 Context 的澄清）：trading_status 仍独立提交，daily_bars 仍按批次在满足门禁后提交。

### 决策 2：compact 合并保留 status_evidence_rank

在 `storage/parquet.py`/`domain/canonical.py` 的 trading_status 合并排序中，把 `status_evidence_rank` 作为主排序键（升序，rank 0 排最后 → `keep="last"` 保留），`fetched_at/source/data_version` 降为次级键。

- **理由**：证据分级是 trading_status 独有且已存在语义；通用 `fetched_at` 去重会令"更新的普通快照"（rank 2）盖掉 derived（rank 1），直接摧毁本次反推的成果。
- **实现注意**：`status_evidence_rank` 定义在 `derive/trading_status_history.py`（仅写端）；compact 读端不能 import derive 层。需把该函数抽到共享位置（如 `domain/trading_status.py` 或 `query/canonical.py` 专用 hook），供 `compact_dataset` / `dedupe_by_primary_key` 在 `dataset=="trading_status"` 时启用。可用"per-dataset source-rank 注入"机制（类似现有 `_source_rank_expr`），不破坏其他数据集去重。
- **替代方案（拒绝）**："compact 前在 staging 内部先做 rank 合并"——无法阻止 compact 合并既有 committed 行与 staging 时的跨批次竞争。拒绝。
- **边界**：`classify_daily_bar_ownership`/`_staged_daily_bar_missing_keys` 的读端消费语义不变（显式 `is_trading=false` 才算豁免）。

### 决策 3：未收盘/当日防护复用 daily_bars 逻辑

derive 对"今天"且 `shanghai_now().time() < _DAILY_BAR_FINAL_AT`（15:00）且 `is_trading_day(config, today)` 的窗口拒绝执行（或排除今天），复用/提取 `_reject_unfinished_daily_bar_window` 的判定函数到共享模块。

- **理由**：与 daily_bars 行为一致，杜绝盘中 `_suspended_pairs` 把"今天还没出 bar"反推成停牌。
- **替代方案（拒绝）**："只排除 start==end 的当天"——不足以覆盖以今天为 end 的窗口。拒绝。
- **实现注意**：`_suspended_pairs` 用 `curated_root/"daily_bars"` 反推，已提交 bar 只会在收盘后被发布，因此该防护主要防御"以今天为 end 的手动/定时触发"；历史 end 天然安全。

### 决策 4：注册 derive 步骤并挂 daily core group

新增/注册步骤（如 `register_step("trading_status_derive")`）在 daily run 中于 daily_bars 之后运行（`fetch_incremental_daily` 对 trading_status 是 snapshot 语义，derive 补充的是历史反推，两者互补）；或在 init 的 phase3/daylier compaction 前插入。挂入 `job.daily.groups.core` steps（config 模板），保证 compact 在同批覆盖其 staging。

- **理由**：历史停牌缺口只能靠反推弥补，且每日自动执行才能让"部署前/抓取失败/漏标"窗口的自愈成为常态。
- **替代方案（拒绝）**："仅在 `cne init` 一次性跑 derive"——仍无法覆盖日常新增缺口。拒绝。
- **实现注意**：init 的 `DEFAULT_INIT_PHASES`/`INIT_PHASE_STEPS`（orchestrator/init_phases.py）需把 derive 纳入 phase（如 phase3 或新增子阶段），确保初始化时历史停牌也被补齐。

### 决策 5：手动入口保持一致

`cne derive trading_status`（cli/maintain_cmds.py:98）改为走与自动调度相同的 staging→compact→commit 输出（可复用 `_backfill_once` 的 compact 收尾逻辑，或改为触发一个微型 run）。保留 `--start/--end` 只约束反推窗口，不改变防护语义。

## Risks / Trade-offs

- [compact 合并排序改动影响其他数据集] → 仅对 `dataset=="trading_status"` 注入 rank 排序 hook；其余数据集保持原 `fetched_at` 语义，单测覆盖回归。
- [derive 结果与每日 EastMoney 快照语义竞争（rank 1 vs rank 2）] → 决策 2 保证 rank 1 胜出；若日后权威源（baostock/已收盘快照 rank 0）对同日给出 contradictory 行，仍按设计正确修正 derive。需在 findings/audit 中记录"derived 被权威修正"以便观察。
- [staging→compact 改动增加一次 compact 覆盖面] → compact 本就以 stated datasets 为输入，新增 derive staging 只是多一个批次；发布频率与 trading_status 每日 compact 一致，无额外修订放大。
- [EastMoney 历史名单留存未知（方案 D 前提）] → 本次明确不做 D；若未来评估 D，建议先小范围验证历史日期 `fd=` 回查可用性，再决定是否作为权威补充通道。
- [已漏历史（如 600984 8/11–8/24）需上线后手动触发一次补齐] → 上线后执行一次 `cne derive trading_status --full`（或等效 backfill）补齐存量缺口，此后由 daily 自动维护。

## Migration Plan

1. 抽取 `status_evidence_rank` → 共享模块；在 `compact_dataset`/`dedupe_by_primary_key` 对 trading_status 启用 rank 排序（决策 2）。
2. 改造 `derive_suspension_history`：写入 staging；提取未收盘防护（决策 1、3）。
3. 注册 derive 步骤并挂 daily core steps + init 阶段（决策 4）；`cne derive trading_status` 走新通道（决策 5）。
4. 上线后执行一次存量补齐（`cne derive trading_status --full`），随后 `cne backfill daily_bars --symbols 600984.SH --start 2026-08-11 --end 2026-08-26` 验证 interior-gap 通过并可 checkpoint。
5. 回滚：还原 storage/parquet.py 排序 hook 与步骤注册即可；既有 committed 数据不受回滚影响（derive 产物已提交）。

## Open Questions

- 是否需要观察/告警指标标记"derived 被权威源修正"（区分正常修正 vs 反推错误率）——可滞后决定，不影响 specs/方案/任务拆分。
- 未来是否评估方案 D（EastMoney 历史名单回查）作为权威补充——独立立项，不阻塞本次 A。