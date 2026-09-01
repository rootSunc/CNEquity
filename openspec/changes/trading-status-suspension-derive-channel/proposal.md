## Why

`trading_status` 的历史停牌记录存在一个结构性缺口:它既不能被 daily EastMoney 当日快照覆盖(该数据集为 `snapshot` 语义,只抓当天、从不回溯历史),也不能被 `cne backfill trading_status` 覆盖(baostock/tushare 只在 `tradestatus="1"` 的交易日产行,停牌日被 `st_history.py:76 if tradestatus != "1": continue` 显式跳过)。能反推停牌的 `derive_suspension_history` 现在直接写可变 curated 目录而**不调用 `revisions.commit()`**,导致产物永远停留在用户不可见的"草稿层";即使该 derive 命令被手动执行,其输出也既不会被 committed 读者看到,又会在下一次 compact 时被从旧快照重建而冲掉。

实际故障(2026-09-01 观测):`cne backfill daily_bars --symbols 600984.SH --start 2026-08-11 --end 2026-08-26` 报
`RuntimeError: 10 interior symbol×session key(s) remain absent; refusing to checkpoint`。daily_bars 数据本身是正确的(8/11–8/24 该股停牌、本就没有 bar,8/25、8/26 有成交且已正确入库),但 interior-gap 校验器(`_staged_daily_bar_missing_keys`)要求 `is_trading=false` 的**显式豁免证据**,而已发布的 committed 快照里没有这 10 天的停牌行。本次变更把"从 bar 缺口反推停牌"的机制接到正式提交通道上,并固化为每日自动维护环节。

## What Changes

- **A1. derive 写入接入正式提交通道**:`derive/trading_status_history.py` 的 `derive_suspension_history` 从 `CuratedWriter` 直写可变目录,改为 **staging → compact → commit** 通道(或等效地:通过已注册步骤 + compact 发布新修订),使反推出的停牌行对 committed 读者可见且持久。
- **A2. 未收盘/当日防护**:为 derive 补上与 daily_bars 相同的"未收盘(15:00 Asia/Shanghai 前)拒绝当日"防护(`_reject_unfinished_daily_bar_window` 同款逻辑),避免盘中把"今天还没出 bar"误判为"今天停牌"。
- **A3. compact 合并保留证据等级**:compact 阶段对 `trading_status` 的合并排序从"先 `fetched_at` 后 source rank"改为 **先 `status_evidence_rank`(baostock/已收盘快照=0, derived_bar_gap=1, 滞后快照=2)再 `fetched_at`**,避免"更新的普通 EastMoney 快照"意外盖掉 derived 停牌行,同时保留权威源(rank 0)对 derived 的修正能力。
- **A4. 每日自动调度**:将新的 derive 步骤挂进 `job.daily.groups.core` 的环保 steps(与 daily_bars 同批),实现每日自动补历史停牌缺口;保留手动 `cne derive trading_status` 入口但使其同样走提交通道。
- **A5. 非目标**:不改 daily_bars 的 interior-gap 校验语义("缺失 ≠ 停牌");不把 derive 反推当作权威事实(`derived_bar_gap` 维持 rank 1,可被权威源修正);不做 EastMoney 历史名单回查(方案 D,需先验证 EastMoney 历史留存,留作后续评估)。

## Capabilities

### New Capabilities

- `trading_status/suspension-history`:定义 `trading_status` 历史停牌(suspension)记录的维护契约——从 daily_bars 交易缺口反推停牌、反推行必须经正式提交通道发布并对 committed 读者可见、未收盘/当日排除防护、证据等级(权威 > derived)在 compact 合并中被保留、每日自动执行补缺口。覆盖:derive 通道改造、compact 合并等级语义、调度挂载、手动入口一致性。

### Modified Capabilities

<!-- 无既有已归档 spec 受影响(openspec/specs 当前为空)。 -->

## Impact

- 修改:
  - `src/cnequity/derive/trading_status_history.py`(写入通道、证据等级接入、未收盘防护)
  - `src/cnequity/storage/parquet.py`(`compact_dataset` 对 trading_status 的合并排序保留 `status_evidence_rank`)
  - `src/cnequity/steps/reference.py` 或 orchestrator/init_phases(注册 derive 步骤 / 挂 daily core wave)
  - `src/cnequity/cli/maintain_cmds.py`(`cne derive trading_status` 走新通道)
  - 配置模板(`configs/cnequity.example.toml` daily core steps 增加 derive 步骤)
  - 单测:derive/compact 合并、未收盘拒绝、提交可见性
- 不涉及:daily_bars 校验逻辑、manifest schema、duckdb 视图、EastMoney adapter(除非方案 D 评估)。
- 副作用:每日新增一次 derive 计算(基于已提交 daily_bars 与 trading_calendar 反推),资源开销与现有 `cne derive trading_status` 一致;发布新修订频率与 trading_status 每日 compact 一致。
- 部署注意:editable 安装即时生效;已漏的历史停牌需在变更上线后跑一次 `cne derive trading_status --full`(或对应 backfill)补齐,此后由 daily 自动维护。