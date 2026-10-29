---
title: CNEquity 文档
description: 开源 A 股数据湖 CNEquity 文档：从安装、第一条查询到日更、量化回测研究、MCP 接入和排障。
hide:
  - navigation
  - toc
---

<div class="cne-home">
  <section class="cne-hero">
    <div>
      <p class="cne-eyebrow"><span></span>本地研究数据湖</p>
      <h1>可日更、可溯源<br><em>研究数据</em></h1>
      <p class="cne-lead">行情、财报、公司事件和资金流写入自己的 Parquet 文件。Python、DuckDB、Polars 和只读 MCP 读同一份数据；本机面板可以查看、日更和备份。</p>
      <div class="cne-actions">
        <a class="cne-button cne-button--primary" href="getting-started/quickstart/">快速开始</a>
        <a class="cne-button cne-button--secondary" href="datasets/catalog/">数据集目录</a>
      </div>
      <ul class="cne-proof">
        <li><strong>可续跑</strong><span>中断后重跑同一条命令</span></li>
        <li><strong>可溯源</strong><span>行上有来源、版本和抓取时间</span></li>
        <li><strong>留在本机</strong><span>开放文件，面板和只读 MCP</span></li>
      </ul>
    </div>
    <div class="cne-console">
      <div class="cne-console__top">
        <span class="cne-console__lights" aria-hidden="true"><i></i><i></i><i></i></span>
        <span>终端</span>
        <span class="cne-console__live"><i></i>第一条命令</span>
      </div>
      <div class="cne-console__body">
        <p class="cne-console__comment"># 在准备长期存放数据的目录里</p>
        <p><b>$</b>pip install cnequity</p>
        <p><b>$</b>cne init</p>
        <p><b>$</b>cne check</p>
        <p class="cne-console__comment"># 读一只股票的日线</p>
        <p><b>&gt;&gt;&gt;</b>from cnequity.query import load</p>
        <p><b>&gt;&gt;&gt;</b>load("daily_bars", symbols=["600519.SH"])</p>
        <p class="cne-console__done"><span>✓</span>中断后重跑 cne init，已成功的批次会保留</p>
      </div>
    </div>
  </section>

  <section class="cne-metrics" aria-label="覆盖范围">
    <div><strong>55</strong><span>登记数据集，47 个采集 + 8 个派生</span></div>
    <div><strong>沪深京</strong><span>股票、指数、ETF / LOF，可选商品期货与期权</span></div>
    <div><strong>Parquet</strong><span>开放文件；Python、DuckDB、Polars 和只读 MCP 读同一份</span></div>
    <a href="datasets/catalog/">看完整目录 →</a>
  </section>

  <section class="cne-section">
    <div class="cne-coverage">
      <div class="cne-section__heading">
        <p class="cne-kicker">从这里进入</p>
        <h2>按你要做的事</h2>
        <p>每条路径都落到一份文档。安装包只含程序，数据在初始化时写到本机。</p>
      </div>
      <div class="cne-coverage__grid">
        <a href="getting-started/quickstart/">
          <span>01 · 开始</span>
          <strong>建立可日更的湖</strong>
          <small>安装、初始化、续跑。全市场第一次可能要数小时。</small>
        </a>
        <a href="getting-started/quickstart/#browser">
          <span>02 · 面板</span>
          <strong>在浏览器里查看和操作</strong>
          <small>覆盖、日更、定时任务和备份。只浏览时用 --read-only。</small>
        </a>
        <a href="datasets/catalog/">
          <span>03 · 数据</span>
          <strong>查有哪些数据、能补多远</strong>
          <small>目录、来源限制、字段和单位。注册不等于已经采集。</small>
        </a>
        <a href="datasets/query-guide/">
          <span>04 · 研究</span>
          <strong>复权、历史股票池、财报</strong>
          <small>查询口径、研究示例和 Python API。严格 PIT 要显式打开。</small>
        </a>
        <a href="reference/cli/">
          <span>05 · 接口</span>
          <strong>命令、Python 和 MCP</strong>
          <small>按任务查 CLI。MCP 只读，不触发采集或清理。</small>
        </a>
        <a href="operations/troubleshooting/">
          <span>06 · 排障</span>
          <strong>失败、限流、数据缺口</strong>
          <small>先看 run 和日志，再按受影响的范围续跑。</small>
        </a>
      </div>
    </div>
  </section>

  <section class="cne-section">
    <div class="cne-section__heading cne-section__heading--row">
      <h2>从安装到每天更新</h2>
      <p>在准备长期存放数据的目录里执行。已有配置会直接沿用。</p>
    </div>
    <ol class="cne-steps">
      <li>
        <span>01</span>
        <h3>初始化</h3>
        <p>生成配置，下载沪深京全市场近 3 年主干，审计后发布。</p>
        <pre><code>pip install cnequity
cne init</code></pre>
        <a href="getting-started/initialization/">范围、磁盘和续跑</a>
      </li>
      <li>
        <span>02</span>
        <h3>读出结果</h3>
        <p>同一份已发布数据，可以用 Python 或 SQL 读取。</p>
        <pre><code>from cnequity.query import load
load("daily_bars", symbols=["600519.SH"])</code></pre>
        <a href="datasets/query-guide/">复权、股票池和 PIT</a>
      </li>
      <li>
        <span>03</span>
        <h3>保持更新</h3>
        <p>每天跑一次。交易日更新行情，周末仍更新公告和资讯。</p>
        <pre><code>cne run daily
cne check</code></pre>
        <a href="operations/runbook/">调度、面板和失败后续</a>
      </li>
    </ol>
  </section>

  <section class="cne-section">
    <div class="cne-principles">
      <div class="cne-section__heading">
        <p class="cne-kicker">使用前</p>
        <h2>先分清四件事</h2>
        <p>这四条决定查询结果能不能直接拿去研究。</p>
      </div>
      <div class="cne-principle-list">
        <article>
          <span>01</span>
          <div>
            <h3>注册不等于已采集</h3>
            <p>目前登记 57 个数据集（49 个 curated + 8 个 derived），其中有可选和占位入口。安装不会附带行情。</p>
            <a href="datasets/catalog/">看数据集目录</a>
          </div>
        </article>
        <article>
          <span>02</span>
          <div>
            <h3>新鲜不等于完整</h3>
            <p>fresh 只说明已落盘的日期够新。历史缺口、ST、退市证据和 PIT 质量要分开核验。</p>
            <a href="operations/runbook/">用 cne check 验收</a>
          </div>
        </article>
        <article>
          <span>03</span>
          <div>
            <h3>交易日不等于自然日</h3>
            <p>日更组在交易日运行。公告和资讯每天都会跑，包括周末。每天调度一次即可。</p>
            <a href="operations/runbook/#web-schedule">定时任务</a>
          </div>
        </article>
        <article>
          <span>04</span>
          <div>
            <h3>回填不等于严格 PIT</h3>
            <p>今天补进来的财报，不能证明过去那个调仓日已经看过这一版。研究时显式使用 pit_mode="strict"。</p>
            <a href="recipes/pit-rebalance/">PIT 示例</a>
          </div>
        </article>
      </div>
    </div>
  </section>

  <section class="cne-cta">
    <div>
      <p class="cne-kicker">下一步</p>
      <h2>从快速开始拿到第一条结果</h2>
    </div>
    <div class="cne-actions">
      <a class="cne-button cne-button--primary" href="getting-started/quickstart/">快速开始</a>
      <a class="cne-button cne-button--ghost" href="changelog/">更新日志</a>
    </div>
  </section>
  <p class="cne-fine">文档随 main 分支更新；还没发布到 PyPI 的改动列在<a href="changelog/">更新日志</a>顶部标为「未发布」的版本下。用 <code>cne --version</code> 核对本机版本。代码 Apache-2.0，数据受各上游条款约束，见<a href="legal-and-data-sources/">许可与来源</a>。方向和边界见<a href="architecture/overview/">产品设计</a>。常见问题：<a href="guides/free-a-share-data/">免费获取 A 股历史数据</a>、<a href="guides/avoid-look-ahead-bias/">回测避免未来函数</a>、<a href="guides/ai-query-a-shares/">用 AI 查 A 股</a>、<a href="architecture/comparison/">与 AKShare、Tushare、Baostock 对比</a>。问题请到 <a href="https://github.com/rootSunc/CNEquity/issues">Issues</a> 反馈。</p>
</div>
