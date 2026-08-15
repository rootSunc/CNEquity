# 与同类项目的差异

cn-market-lake 是本地部署的 A 股数据层：多源采集 + 编排 + 契约化 Parquet 湖 + 可审计查询。不是行情 SDK 合集，也不是回测 / 交易框架。

## 快速对照

用「你在意什么」对照，突出本项目相对同类的优势位：

| 你在意什么 | **cn-market-lake** | AkShare / efinance 等 | Tushare Pro | Baostock | Qlib / vn.py 等 |
|--|--|--|--|--|--|
| 本地可续跑的数据底座 | **湖 + 日更编排**（水位 / 重试 / audit） | 只拉到内存，编排自管 | 云端积分，非自建湖 | 会话拉数，无湖 | 绑在平台数据子系统里 |
| 数据从哪来、能否复查 | **行级溯源** + 写前 schema 校验 | 通常无统一契约 | 平台字段 | 无湖契约 | 视模块 |
| 多源交叉核验 | **主源 curated + 备源 snapshot**，可 diff，不静默顶替 | 单次单源调用 | 单平台 | 单源 | 视配置 |
| 研究口径是否稳定 | **`load()` 契约**：复权组合 / universe / PIT `as_of` | 自己拼 | 自己拼 | 自己拼 | 平台口径 |
| 历史怎么回填 | 分页、checkpoint、按数据集 backfill | 脚本循环 | 积分与权限 | 按接口能力 | 视方案 |
| 源挂了会怎样 | **fail batch**，暴露问题，可按批 retry | 看调用方 | 看平台 | 看调用方 | 视模块 |
| 质量门禁 | audit、跨源 diff、mock 强制标记 | 无 | 平台侧 | 有限 | 视模块 |
| 能否单独当研究数据底座 | **能**（湖 + 日更 + `load()`） | 否，还需自建落盘/编排 | 云端表，非自建湖 | 否，会话拉数 | 能，但绑平台 |
| 部署与锁定 | 本地（或自有机器），代码 Apache-2.0 | 本地调 HTTP | 依赖云账号 / 积分 | 本地调官方 | 本地/集群 |
| 许可边界 | 代码 Apache-2.0；数据条款见 [legal](legal-and-data-sources.md) | 各库 + 上游 | 商业/积分协议 | 官方协议 | 各项目许可 |

一句话：**别人帮你取数；这边帮你把数管成可复现的研究底座。**

## 实际差在哪

同类库解决「怎么把网页 / API 变成 DataFrame」。这边多管几件事：全市场怎么幂等落盘（staging → compact → curated）、日更怎么只抓水位之后、失败怎么按 batch 续跑、下游怎么稳定依赖列名与主键。

口径上几条硬约束：源失败就让 batch 失败（mock 仅测试门控且强制标记）；curated 行带 provenance；财报支持 `announce_date` + `load(..., as_of=)`；备源可审计比对，不静默覆盖主源（见 [ADR-0003](adr/0003-canonical-curated-with-source-snapshots.md)）。覆盖面可以后补，会污染下游结论的口径问题优先修。

编排也算一等公民：`cml init` / `run` / `retry` / `audit` / `status`、分组调度、限速、manifest WAL、验收脚本——纯 adapter 库里通常没有，本地湖要跑过两周却离不开。

明确不做：回测、信号、下单；托管云行情或出售数据文件；自动把备源写成 canonical；保证上游 ToS 下的商用再分发（见 [legal](legal-and-data-sources.md)）。

## 相关文档

分层数据集与字段：[catalog](datasets/catalog.md)、[schema](datasets/schema.md)。安装与日更：[getting-started](getting-started/installation.md)。架构与决策：[overview](architecture/overview.md)、[adr](adr/)。合规：[legal](legal-and-data-sources.md)。
