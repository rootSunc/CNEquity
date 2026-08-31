## Why

CNINFO `hisAnnouncement/query` 服务端对单次查询硬性只返回前 100 页（pageSize 固定 30 → 最多 3000 条），pageNum>100 时回绕重复 page 1 内容，而 `totalpages` 仍按真实总数虚报（实测 2026-08-28 szse=6538 → `totalpages=217`）。现有 `fetch_announcement_index`（announcements.py:228-234）的重复页签名护栏把这个服务端截断误判为"源坏了"，导致整步 `RuntimeError`、当日 0 行入库。akshare 裸翻页则静默重复拼接数据。该日子公告通常 ≤3000 条不触发，但中报/年报密集日必然踩雷。

同时实测 `column` 参数对 A 股 fulltext 不做市场切分（szse/sse/bj 返回完全相同的 6538 条），现有 `for column in ("szse","sse")` 循环是重复抓全量＋一条假路径；而 `category` 参数按公告类别**真实切分**，26 个类别单桶最大 92 页，全部低于 100 页红线。

## What Changes

- **A. 分桶分页（category 桶化）**：把 `fetch_announcement_index`（及共享相同模式的 `fetch_regulatory_events`）的列循环改为 CNINFO 公告类别（26 个 `category_*_szsh` 桶）循环，桶内按 `totalpages` 走页、复用现有签名护栏；跨桶用 `announcement_id` 去重（`keep="last"`）合并。单桶天然低于 100 页红线，正常日不再触发回绕。
- **B. 重复页护栏语义降级**：把"重复页签名 → raise"改为"重复页签名 → 视为到达服务端 100 页截断 → 停止该桶收集并记录 `audit_finding`（如 `cninfo_truncation_at_100_pages`）"，不再整步失败。极端日（单桶 >3000 条）降级为"写入可达部分 + 显式缺口告警"，与现有 `session_dense_empty_days` 的容忍语义一致。
- 残余缺口显式化：26 类别并集实测覆盖 6439/6538（98.5%），不在任何类别桶中的公告作为已知残余缺口在 findings 中报告，不做静默吞掉。
- **非目标**：不改 `hisAnnouncement/query` 调用形态（仍为 POST+seDate 当日过滤）；不做 per-stock 枚举（C 方案，仅留作救济路径）；不新增配置项。

## Capabilities

### New Capabilities

- `cninfo-announcement-pagination`: 定义 CNINFO announcement/regulatory 声明类数据的抓取契约——按公告类别分桶分页、跨桶按 `announcement_id`/`event_id` 去重合并、服务端 100 页截断时的降级语义（写可达部分 + `audit_finding`，而非整步失败）。

### Modified Capabilities

<!-- 无既有已归档 spec 受影响（现有 specs/ 仅 trading-status-failover、eastmoney-suspension-fetch）。 -->

## Impact

- 修改：`src/cnequity/adapters/cninfo/announcements.py`（`fetch_announcement_index` 分桶逻辑 + 护栏降级）、`src/cnequity/adapters/cninfo/regulatory.py`（共享分页模式同改，避免其先踩雷）、`tests/unit/test_cninfo_announcements.py`（夹具改 category 桶、新增截断降级/跨桶去重用例）。
- 不涉及：manifest schema、CLI、config、staging/compact/gate 语义、duckdb 视图。
- 副作用：单日请求量从「szse+sse 两趟全量」变为「26 桶合计约 totalAnn/30 页」，与当前量级相当（8-28 实测约 230 页）；跨桶去重不会放大入湖行数。
- 部署注意：editable 安装即时生效（datalake venv 为 editable）；现有 liveness/watermark 逻辑不变。