## 1. 提取共享证据等级与防护工具

- [x] 1.1 将 `status_evidence_rank`（现位于 `derive/trading_status_history.py:61-90`）抽取到共享模块（如 `domain/trading_status.py`），保持 rank 语义不变（baostock/已收盘快照/退市=0, derived_bar_gap=1, 滞后快照=2）；验证 `derive/trading_status_history.py` 改 import 后既有单测通过
- [x] 1.2 将 `_reject_unfinished_daily_bar_window` 的当日判定逻辑（15:00 最新，`is_trading_day` 判定）提取到共享函数（如 `steps/common.py` 或 `domain/calendar.py`），导出 `_DAILY_BAR_FINAL_AT` 常量；验证 daily_bars 调用方仍通过原有测试

## 2. compact 合并保留证据等级

- [x] 2.1 在 `domain/canonical.py`（`_source_rank_expr`/`_sort_for_canonical`）为 `dataset=="trading_status"` 注入 `status_evidence_rank` 主排序键（rank 0 排最后, `keep="last"` 保留胜利行）, `fetched_at/source/data_version` 降为次级键; 验证新增单测"更新的普通 EastMoney 快照(rank 2) 不覆盖 derived_bar_gap(rank 1)"通过
- [x] 2.2 验证权威行(rank 0: baostock/已收盘快照)在 compact 合并中胜出并修正 derived 停牌行, 新增单测"权威源修正 derived"通过
- [x] 2.3 验证非 trading_status 数据集去重语义不变（按 fetched_at + source rank）, 既有 compact 单测全部通过（回归）

## 3. derive 写入接入正式提交通道

- [x] 3.1 改造 `derive_suspension_history`: 不再直写可变目录, 改用 `StagingWriter.write_batch(dataset="trading_status", run_id, batch_id=<derived-序号>, df)` 写入 staging（月分区 via `_STATUS_SPEC.partition_for`）; 验证单测"反推行进入 staging 且 schema 正确"
- [x] 3.2 确保 derive 的 staging 能被既有 `compact_dataset` 按月分区合并（dedupe by PK + 决策 2 的 rank 排序）并发布为 committed 修订; 验证"derive 后 compact → `load_curated_trading_status`(committed=True) 可见 `is_trading=false` 行" 集成测试通过
- [x] 3.3 验证"后续再次 compact 不冲掉 derived 停牌行": 连续两次 compact 后 derived 行仍在最新 committed 快照（单测/集成测试）
- [x] 3.4 为 derive 接入未收盘防护（决策 3）: 上海时间 <15:00 且 `end==today` 且为交易日时拒绝执行并报错; 新增单测"盘中止于当日 derive"与"收盘后/历史日期正常"通过

## 4. 注册步骤、调度挂载与手动入口

- [x] 4.1 注册 derive 步骤（如 `register_step("trading_status_derive")` 于 `steps/reference.py` 或独立模块）, 复用 `derive_suspension_history` 并以窗口参数运行; 验证 `validate_steps_registered` 通过
- [x] 4.2 将新步骤挂入 daily core group（`config/templates/cnequity.example.toml` 的 `[job.daily.groups.core]` steps, 置于 daily_bars 之后/compact 之前）; 验证配置模板可被 `cne config init`/校验加载
- [x] 4.3 将 derive 纳入 init 阶段（`orchestrator/init_phases.py` 的 `INIT_PHASE_STEPS`, 如 phase3 或新子阶段）, 保证初始化补齐历史停牌; 验证 `expected_steps`/`pending_phases` 相关单测通过
- [x] 4.4 `cli/maintain_cmds.py` 的 `cne derive trading_status` 改走新通道（staging→compact→commit, 可复用 `_backfill_once` 收尾）; 验证 CLI 集成测试或手动运行产出 committed 可见行

## 5. 存量数据补齐与端到端验证

- [x] 5.1 变更上线后执行一次存量补齐（`cne derive trading_status --full` 或等效）, 验证 600984.SH 8/11–8/24 生成 `is_trading=false` 行且经提交通道可见
- [x] 5.2 重跑 `cne backfill daily_bars --symbols 600984.SH --start 2026-08-11 --end 2026-08-26`, 验证不再报 "10 interior symbol×session key(s) remain absent" 且可正常 checkpoint
- [x] 5.3 运行 `cne audit trading_status` / `cne audit daily_bars` 相关校验, 确认无新的覆盖/缺口 findings