---
title: 可日更、可溯源的 A 股研究数据湖
description: 将多源 A 股数据持续落到本地 Parquet，用统一查询契约处理 PIT、历史 Universe、复权与行级溯源。
hide:
  - navigation
  - toc
---

<div class="cne-home">

<section class="cne-hero">
  <div class="cne-hero__copy">
    <p class="cne-eyebrow"><span></span> OPEN · SELF-HOSTED · POINT-IN-TIME</p>
    <h1>A 股研究数据，<br><em>从获取走向可信</em></h1>
    <p class="cne-lead">CNEquity 把行情、基本面、公司事件、资金与宏观数据持续写入本地 Parquet，并统一处理复权、历史股票池与 PIT。采集可以续跑，结果可以回查，研究口径不会藏在脚本里。</p>
    <div class="cne-actions">
      <a class="cne-button cne-button--primary" href="getting-started/quickstart/">跑通第一个 Demo <span aria-hidden="true">→</span></a>
      <a class="cne-button cne-button--secondary" href="datasets/catalog/">查看数据集目录</a>
    </div>
    <ul class="cne-proof" aria-label="核心特性">
      <li><strong>零注册</strong><span>无需申请 API Token</span></li>
      <li><strong>开放存储</strong><span>Parquet 留在本地</span></li>
      <li><strong>研究友好</strong><span>Python · DuckDB · Polars · MCP</span></li>
    </ul>
  </div>

  <div class="cne-console" aria-label="CNEquity 命令行快速演示">
    <div class="cne-console__top">
      <span class="cne-console__lights" aria-hidden="true"><i></i><i></i><i></i></span>
      <span>quickstart.sh</span>
      <span class="cne-console__live"><i></i> local</span>
    </div>
    <div class="cne-console__body">
      <p class="cne-console__comment"># 安装并验证完整链路</p>
      <p><b>$</b> pip install cnequity</p>
      <p><b>$</b> cne demo</p>
      <div class="cne-console__result">
        <div><span>instruments</span><small>curated</small><strong>5 rows</strong></div>
        <div><span>daily_bars</span><small>curated</small><strong>150 rows</strong></div>
        <div><span>quality checks</span><small>audit</small><strong class="is-pass">PASS</strong></div>
      </div>
      <p class="cne-console__done"><span>✓</span> Local lake ready · 5 symbols · 30 sessions</p>
    </div>
  </div>
</section>

<section class="cne-metrics" aria-label="项目数据概览">
  <div><strong>42</strong><span>注册数据集</span></div>
  <div><strong>39 + 3</strong><span>Curated + Derived</span></div>
  <div><strong>L0—L8</strong><span>九类研究数据</span></div>
  <a href="datasets/catalog/">完整覆盖范围 <span aria-hidden="true">↗</span></a>
</section>

<section class="cne-section cne-principles">
  <div class="cne-section__heading">
    <p class="cne-kicker">WHY CNEQUITY</p>
    <h2>不是多包一层接口，<br>而是把研究口径放进数据层</h2>
    <p>真正昂贵的不是发出请求，而是多年之后仍然知道：当时能看到什么、这行数据来自哪里、故障后该从哪继续。</p>
  </div>
  <div class="cne-principle-list">
    <article>
      <span>01</span>
      <div>
        <h3>历史股票池，不用今天解释过去</h3>
        <p>退市股、历史成分与交易状态进入统一 Universe 查询，降低幸存者偏差。</p>
        <a href="recipes/pit-rebalance/">PIT 截面示例 →</a>
      </div>
    </article>
    <article>
      <span>02</span>
      <div>
        <h3>行级来源，不靠猜测解释差异</h3>
        <p>Curated 数据保留 source、data_version 与 fetched_at，结果可以追到采集批次。</p>
        <a href="datasets/contract/">数据契约 →</a>
      </div>
    </article>
    <article>
      <span>03</span>
      <div>
        <h3>可续跑任务，不因一次波动重来</h3>
        <p>按批落盘、水位记录、质量审计与主备源路由，为长期日更而设计。</p>
        <a href="operations/runbook/">生产运行手册 →</a>
      </div>
    </article>
  </div>
</section>

<section class="cne-section cne-evidence">
  <div class="cne-evidence__copy">
    <p class="cne-kicker">A SMALL CHOICE, A BIG GAP</p>
    <h2>股票池选错，<br>收益可以差一倍</h2>
    <p>用“今天仍然上市”的股票回看 2016—2021 年，同一等权买入持有策略会把累计收益从 <strong>5.9%</strong> 推高到 <strong>12.0%</strong>。退市股票不是收益为零，而是根本没有进入计算。</p>
    <dl class="cne-evidence__numbers">
      <div><dt>5.9%</dt><dd>历史完整股票池</dd></div>
      <div><dt>12.0%</dt><dd>当前股票名单回看</dd></div>
    </dl>
    <a class="cne-text-link" href="recipes/research-baseline/">复现实验与口径说明 →</a>
  </div>
  <figure class="cne-evidence__chart">
    <img src="assets/survivorship-gap.zh.svg" alt="历史完整股票池与当前股票名单回测的累计收益差异">
    <figcaption>相同区间、相同策略，唯一差别是股票池是否保留历史退市标的。</figcaption>
  </figure>
</section>

<section class="cne-section cne-workflow">
  <div class="cne-section__heading cne-section__heading--row">
    <div>
      <p class="cne-kicker">START WITH ONE WORKING QUERY</p>
      <h2>先跑通，再扩展</h2>
    </div>
    <p>从 5 只股票的可验证 Demo，到全市场日更数据湖，再到研究脚本或 AI Agent，共用同一份开放数据。</p>
  </div>
  <ol class="cne-steps">
    <li>
      <span>01 · TRY</span>
      <h3>验证链路</h3>
      <p>拉取小样本真数据；网络受限时可改用确定性离线样例验证链路。</p>
      <pre><code>pip install cnequity
cne demo</code></pre>
      <a href="getting-started/quickstart/">打开快速开始 →</a>
    </li>
    <li>
      <span>02 · BUILD</span>
      <h3>建立数据湖</h3>
      <p>初始化全市场数据，随后按水位增量日更，中断后从失败批次续跑。</p>
      <pre><code>cne config init
cne init
cne run daily</code></pre>
      <a href="operations/runbook/">查看运行方式 →</a>
    </li>
    <li>
      <span>03 · QUERY</span>
      <h3>进入研究</h3>
      <p>通过 Python、DuckDB、Polars 或只读 MCP 消费同一份本地数据。</p>
      <pre><code>from cnequity.query import load
bars = load("daily_bars")</code></pre>
      <a href="datasets/query-guide/">选择查询方式 →</a>
    </li>
  </ol>
</section>

<section class="cne-section cne-coverage">
  <div class="cne-section__heading">
    <p class="cne-kicker">DATA COVERAGE</p>
    <h2>覆盖一条研究链路，<br>而不是堆积孤立接口</h2>
    <p>42 个数据集按研究用途分为 L0—L8 九类。主键、分区、历史模式与源端限制都在目录中明确记录。</p>
    <a class="cne-text-link" href="datasets/catalog/">浏览完整数据集目录 →</a>
  </div>
  <div class="cne-coverage__grid">
    <a href="datasets/catalog/#l0"><span>L0—L1</span><strong>市场与行情</strong><small>证券主数据 · 日线 · 分钟线 · 分笔 · 复权</small></a>
    <a href="datasets/catalog/#l2"><span>L2—L3</span><strong>公司与基本面</strong><small>公告 · 公司行为 · 财报 · 估值 · 股东</small></a>
    <a href="datasets/catalog/#l4"><span>L4—L5</span><strong>资金与市场结构</strong><small>北向 · 两融 · 龙虎榜 · 指数与行业成分</small></a>
    <a href="datasets/catalog/#l6"><span>L6—L8</span><strong>宏观、舆情与风险</strong><small>宏观指标 · 市场宽度 · 新闻 · 情绪 · 监管</small></a>
  </div>
</section>

<section class="cne-cta">
  <div>
    <p class="cne-kicker">YOUR DATA · YOUR HISTORY</p>
    <h2>让下一次研究，<br>从可复查的数据底座开始</h2>
  </div>
  <div class="cne-actions">
    <a class="cne-button cne-button--primary" href="getting-started/installation/">开始安装 <span aria-hidden="true">→</span></a>
    <a class="cne-button cne-button--ghost" href="https://github.com/rootSunc/cnequity">查看 GitHub</a>
  </div>
</section>

</div>
