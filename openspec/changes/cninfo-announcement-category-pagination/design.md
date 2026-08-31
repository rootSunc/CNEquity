## Context

现有 CNINFO 抓取（`announcements.py` / `regulatory.py`）共享同一分页骨架：`for column in ("szse","sse")` 内按 `totalpages` 翻页，页签名去重护栏在重复页时 `raise`（announcements.py:228-234、regulatory.py:90-96）。

实际探测（2026-08-28，完整 akshare 参数 + `"rc"` 签名）：
- **100 页硬顶**：page 100 有独立数据，page101+ 全部回绕到 page 1 内容；`start/offset/pageIndex` 均被忽略；`pageSize=50/100` 被服务端钳回 30。CNINFO 前端 `search_new.js` 自己就写 `fullSearchTotal = totalRecordNum > 3000 ? 3000 : totalRecordNum`。
- **`column` 不切分 A 股**：`szse`/`sse`/`bj` 返回完全相同全集（6538），仅对 hke/fund/bond/regulator 有意义 → 现有 szse+sse 双趟 = 同一数据集抓两遍。
- **`category` 真实切分**：26 个 `category_*_szsh` 桶并集覆盖 6439/6538（98.5%），单桶最大 92 页（日常经营），全部低于 100 页红线；残余 ~99 条为无类别归属公告。
- `searchkey` 前缀分片不可靠（模糊匹配，`000`→0 命、`000001`→31），否决。
- akshare 封装仍在裸翻页，>3000 条的日子会静默重复——本 change 不做 follow。

## Goals / Non-Goals

**Goals:**
- announcement 与 regulatory 两个 fetch 改用 category 桶分页，正常日单桶低于 100 页红线、不再触发回绕。
- 重复页签名从"整步 raise"降级为"停桶 + 写可达部分 + `audit_finding`"，消灭"忙碌日 0 行落库"。
- 跨桶按 `announcement_id` / `event_id` 去重（`keep="last"`），行数不放大。

**Non-Goals:**
- 不改 POST 调用形态（仍当日 `seDate` 过滤）、不新增配置项、不做 per-stock 枚举、不忧愁 26 桶之外的类别残余（以 finding 明示为准）。
- 不追修 akshare 的同源缺陷（另一仓库）。

## Decisions

### D1. 桶集合：复用 akshare 的 26 个 `category_*_szsh` 代码，内联为模块常量

在 `announcements.py` 定义 `_CNINFO_CATEGORIES: tuple[str, ...]`，内容与 akshare `__get_category_dict()` 的 value 集合一致（26 项，含 ndbg/bndbg/yjdbg/sjdbg/yjygjxz/qyfpxzcs/dshgg/jshgg/gddh/rcjy/gszl/zj/sf/zf/gqjl/pg/jj/gszq/kzzq/qtrz/gqbd/bcgz/cqdq/fxts/tbclts/tszlq）。

- **备选（否决）**：运行时调类目接口动态拉取。增量收益为零且引入网络/解析失败面；前端不提供该清单（`search_new.js` 无 category 码），akshare 也是硬编码。
- **备选（否决）**：仍以 `column` 循环加 100 页截断护栏。`column` 本就不切分 A 股，护栏只是掩盖"丢尾巴"，桶化才能真正拿回数据。

### D2. 页签名护栏：重复页 → 停桶 + finding，而非 raise

把两个 fetch 的 `if page_signature in seen_page_signatures: raise` 改为：记录 `truncated_bucket.append({"category": bucket, "page": page})` → `break` 该桶。桶间互不影响，一个桶截断只降级那一个桶。

- **如何区分"真重复页""空页回绕"**：保持不变——重复页出现前该桶必已收集若干有签名页，签名命中即服务端封顶；空页提前（`totalpages` 未到就空）仍走现有 raise（源真的坏了）。
- **finding 形态**：新增 check 名 `cninfo_truncation_at_100_pages`，带 `bucket` 与 `page` 字段，severity=warning，结构与 `_dense_empty_day_finding`（common.py:297-309）对齐。
- **传递通道**：`fetch_incremental_daily` 目前只自行生成 `coverage_gap`/`session_dense_empty_days` 两类 findings，没有适配器回传通道。最小改动：给两个 fetch 增加可选参数 `findings: list[dict] | None = None`，截断时 append；step 的 λ 用迭代闭包收集，调用 `run_incremental_fetched` 后把它合并进 `context_updates.audit_findings` 并置 `status="warning"`（与既有 findings 合并写法一致，`http_common.run_incremental_fetched` 不动）。
- **与现有 `session_dense_empty_days` 的区别**：那是"响应缺失"，这是"响应被服务端截断"，check 名独立，后续 lake_health/告警可分别处理。

### D3. 共享分页骨架 → 参数化桶遍历，避免两份实现漂移

`fetch_announcement_index` 与 `fetch_regulatory_events` 当前各自复制粘贴同一翻页循环。本 change 把桶的**遍历层**收敛到 `announcements.py` 的一个内部 helper（保持对外函数签名与返回语义不变），regulatory 复用；桶内逐页逻辑两处保留各自的行处理（title 过滤等），仅抽取翻页/护栏决策。

- 范围克制：**只收敛翻页驱动**（桶迭代、重复页判定、截断 finding 组装），不重构行解析/符号映射/去重——把本次改动面压到最小。
- **回滚**：helper 抽得足够浅，撤销一两个函数即可回到现状。

### D4. 桶为空 / 桶无分页元数据 维持现状语义

`_pagination_total_pages`/`_pagination_has_more` 的规范化、空页前提前 end 的 raise、`totalpages` 权威停页、`None`+无元数据满页续翻——全部保留（现有单测覆盖）。桶遍历仅改变"外层走哪些过滤器"，内层行为零变化。

## Risks / Trade-offs

- **[26 桶并未覆盖全部公告（实测 98.5%，缺 ~1.5%）]** → 作为残余缺口在 findings 里显式报告（一个聚合 finding 统计缺失类别外公告量），不做静默吞掉；如后续需要全量可另立 order 走 per-stock（C 方案）。
- **[单桶极端日 >3000 条可能再触发截断]** → D2 降级已兜底：写可达部分 + `cninfo_truncation_at_100_pages` finding，比现状"整步 0 行"好一个数量级；且在把"日常经营"等都实测压在 ≤92 页后，该触发概率很低。
- **[请求量微增]** → 从"szse+sse 两趟全量"变为"26 桶合计 ≈ ceil(totalAnn/30)"，8-28 实测约 230 页，与现状同量级；rate_limit("cninfo") 计数随页面数不变。
- **[护栏放宽掩盖真 bug（源返回重复页）]** → 记录 `failed_symbols` 式停桶 + finding，可观测性不降反升；真传输失败（retry 耗尽）仍 raise，见 D2。

## Migration Plan

1. `announcements.py`：加 `_CNINFO_CATEGORIES` 常量 + 桶遍历 helper + 护栏降级；`fetch_announcement_index` 改走桶。
2. `regulatory.py`：改用共享 helper（行为一致、行处理保留）。
3. 测试 `tests/unit/test_cninfo_announcements.py`：
   - 新增：跨桶去重、空桶跳过、重复页→停桶+finding、传输失败仍 raise、桶并集覆盖样例。
   - 适配：`_FakeClient` 分页 key 由 `column` 改为 `category`；`test_cninfo_rejects_a_repeated_page` 从 expect-raise 改为 expect 截断 finding。
4. 冒烟：`cne retry --run-id ad1e00b4-9aed-4331-bea6-b7c7b50de9d1` 重跑 announcement_index，确认 8-28 不再 100 页崩、落库行数合理。
5. `ruff` + `pytest tests/unit` 全绿；CHANGELOG 记录。

Rollback：撤销 D1/D2 改动（恢复 `for column in ("szse","sse")` + 重复页 raise），需同步回退对应测试用例（保留于 git 历史）。

## Open Questions

- 26 桶外残余（~1.5%）是否需要后续 per-stock 补充——本期以 finding 明示为准，逻辑无需现在定。