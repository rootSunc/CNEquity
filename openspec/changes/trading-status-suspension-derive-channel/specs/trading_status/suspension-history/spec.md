## Purpose

定义了 trading_status 历史停牌(suspension)记录的维护契约:系统从已提交的 daily_bars 交易缺口反推停牌日,反推结果必须通过正式提交通道(staging→compact→commit)发布并对 committed 读者可见,同时保证未收盘当日不被误判、且权威来源(baostock/已收盘 EastMoney)能够修正低等级的 derive 推断。该能力确保历史停牌证据在 daily 当日快照与 ST 回填均无法覆盖的场景下仍可被补齐并持久化。

## ADDED Requirements

### Requirement: 停牌反推必须经正式提交通道发布

系统 SHALL 将 `derive_suspension_history` 产出的 `is_trading=false`(status=suspended)行写入 staging,并经 compact 发布为新的 committed 修订;反推结果 MUST 对 `load_curated_trading_status`(committed 读者)可见,且 MUST 不得被下一次 compact 从旧快照重建所冲掉。

#### Scenario: derive 反推行对 committed 读者可见
- **WHEN** 执行 `cne derive trading_status --full`(或等效自动调度)反推出某 symbol×date 的停牌行
- **THEN** 该行经 staging→compact→commit 后,t出现在 committed 修订快照中
- **AND** `load_curated_trading_status(start=…, end=…, symbols=[…])` 能读到该行的 `is_trading=false`

#### Scenario: 后续 compact 不冲掉反推停牌行
- **WHEN** derive 发布后,后续任一 trading_status compact 再次运行
- **THEN** 已提交的 derived 停牌行仍保留在最新 committed 快照中
- **AND** 不因"可变目录被从旧快照重建"而丢失

### Requirement: 当日未收盘不得被反推为停牌

derive 反推 SHALL 对"今天"且当前上海时间早于 15:00:00 的会话拒绝生成 `is_trading=false` 行(复用 daily_bars 的 `_reject_unfinished_daily_bar_window` 语义),避免把"当日 bar 尚未产生"误判为"当日停牌"。历史日期与非交易日不受此限制。

#### Scenario: 盘中止于当日 derive
- **WHEN** 在上海时间 15:00 前对以今天为 `end` 的窗口执行 derive
- **THEN** 今天不被写入任何派生停牌行
- **AND** 该次 derive 以错误/拒绝结束,提示会话未收盘

#### Scenario: 收盘后或历史日期 derive 正常
- **WHEN** 上海时间 >= 15:00 后,或 `end` 早于今天的窗口执行 derive
- **THEN** 正常反推并发布停牌行,不受当日防护拦截

### Requirement: compact 合并保留停牌证据等级

compact 合并 trading_status 时 SHALL 先按数据来源证据等级排序,再按 `fetched_at` 排序;等级越高者优先保留(baostock / 已收盘的 EastMoney 当日快照 / 退市 = rank 0,`derived_bar_gap` 推断 = rank 1,滞后的 EastMoney 当前态快照盖到旧日期 = rank 2)。此规则 MUST 防止"恰好更新的普通 EastMoney 快照"覆盖 rank 1 的 derived 停牌行,同时保留权威源对 derived 推断的修正能力。

#### Scenario: 更新的普通快照不得覆盖 derived 停牌
- **WHEN** 同一 symbol×date 同时存在 `derived_bar_gap`(is_trading=false) 与一张 `fetched_at` 更新的普通 EastMoney 快照行
- **THEN** compact 合并后保留 `derived_bar_gap`(rank 1 > rank 2)的 `is_trading=false`
- **AND** 普通快照不因 `fetched_at` 更晚而胜出

#### Scenario: 权威源可修正 derived 停牌
- **WHEN** 同一 symbol×date 同时存在 `derived_bar_gap`(rank 1) 与 baostock 或已收盘的 EastMoney 当日快照(rank 0)
- **THEN** compact 合并后保留 rank 0 权威行的状态
- **AND** 若权威行证明该日交易(rank 0, is_trading=true),derived 的 is_trading=false 被修正为正常

### Requirement: 每日自动补齐历史停牌缺口

系统 SHALL 提供一个注册步骤,将停牌反推纳入每日作业;该步骤 SHALL 与 daily_bars 同批(daily core group)执行,自动对已提交的 daily_bars / trading_calendar 反推并发布停牌行,使历史停牌缺口(daily 快照漏标、抓取失败、部署前窗口)得以每日自动补齐。手动 `cne derive trading_status` 入口 MUST 保持可用且同样走提交通道。

#### Scenario: 每日作业自动补齐缺口
- **WHEN** 每日 trading_status 维护(daily core)运行
- **THEN** 系统基于已提交 daily_bars 反推缺失的停牌日
- **AND** 结果经提交通道发布,无需人工介入

#### Scenario: 手动 derive 与自动调度行为一致
- **WHEN** 操作者手动运行 `cne derive trading_status --start/--end`
- **THEN** 产出的停牌行路径与自动调度相同(staging→compact→commit)
- **AND** 均受未收盘当日防护约束

### Requirement: 反推证据等级维持为推断而非权威

derive 产出的停牌行 SHALL 保持 `source=derived_bar_gap` 与 rank 1 语义,不得被提升为权威证据;系统 MUST 允许未来出现的权威证据(baostock 历史 / 已收盘当日快照 / 退市)按等级修正或覆盖这些推断行,以实现"列为疑似、等待权威确认"的设计意图。

#### Scenario: derived 行不覆盖权威证据
- **WHEN** derive 反推行与权威行在 compact 中竞争同一 symbol×date
- **THEN** 权威行(rank 0)胜出,derived 行(rank 1)被覆盖或修正
- **AND** derived 行本身从不作为最高权威因 `is_trading=false` 豁免而越过权威源

### Requirement: 反推缺口窗口边界

derive 反推 SHALL 将 symbol×date 的期望范围限定为该 symbol 的上市/退市活动区间(list_date..delist_date 与 bar 区间交集)内,且仅与 `is_trading=true` 的交易日历会话交叉;超出活动区间的日期 MUST NOT 被推定为停牌。

#### Scenario: 上市前/退市后日期不被反推
- **WHEN** derive 反推包含某 symbol 的 list_date 之前或 delist_date 之后的日期
- **THEN** 这些日期不出现在输出的停牌行中
- **AND** 只有活动区间内的无 bar 交易日被反推