---
title: CNEquity documentation
description: Documentation for CNEquity, an open-source China A-share data lake - install, first query, daily updates, quant backtesting research, MCP and troubleshooting.
hide:
  - navigation
  - toc
---

<div class="cne-home">
  <section class="cne-hero">
    <div>
      <p class="cne-eyebrow"><span></span>Local research data lake</p>
      <h1>Daily-updated, traceable<br><em>research data</em></h1>
      <p class="cne-lead">Market data, financial statements, corporate events and fund flow are written to Parquet files you own. Python, DuckDB, Polars and a read-only MCP server read the same data; a local dashboard lets you browse, run daily updates and back up.</p>
      <div class="cne-actions">
        <a class="cne-button cne-button--primary" href="getting-started/quickstart/">Quickstart</a>
        <a class="cne-button cne-button--secondary" href="datasets/catalog/">Dataset catalog</a>
      </div>
      <ul class="cne-proof">
        <li><strong>Resumable</strong><span>After an interruption, rerun the same command</span></li>
        <li><strong>Traceable</strong><span>Rows carry source, version and fetch time</span></li>
        <li><strong>Stays on your machine</strong><span>Open files, a dashboard and read-only MCP</span></li>
      </ul>
    </div>
    <div class="cne-console">
      <div class="cne-console__top">
        <span class="cne-console__lights" aria-hidden="true"><i></i><i></i><i></i></span>
        <span>Terminal</span>
        <span class="cne-console__live"><i></i>First commands</span>
      </div>
      <div class="cne-console__body">
        <p class="cne-console__comment"># In the directory where the data will live long term</p>
        <p><b>$</b>pip install cnequity</p>
        <p><b>$</b>cne init</p>
        <p><b>$</b>cne check</p>
        <p class="cne-console__comment"># Read daily bars for one stock</p>
        <p><b>&gt;&gt;&gt;</b>from cnequity.query import load</p>
        <p><b>&gt;&gt;&gt;</b>load("daily_bars", symbols=["600519.SH"])</p>
        <p class="cne-console__done"><span>✓</span>Rerun cne init after an interruption; batches that already succeeded are kept</p>
      </div>
    </div>
  </section>

  <section class="cne-metrics" aria-label="Coverage">
    <div><strong>55</strong><span>registered datasets: 47 collected + 8 derived</span></div>
    <div><strong>SH · SZ · BJ</strong><span>stocks, indices, ETFs / LOFs; optional commodity futures and options</span></div>
    <div><strong>Parquet</strong><span>open files; Python, DuckDB, Polars and read-only MCP read the same copy</span></div>
    <a href="datasets/catalog/">Full catalog →</a>
  </section>

  <section class="cne-section">
    <div class="cne-coverage">
      <div class="cne-section__heading">
        <p class="cne-kicker">Start here</p>
        <h2>By what you want to do</h2>
        <p>Each path leads to one document. The installed package contains only the program; data is written to your machine during initialization.</p>
      </div>
      <div class="cne-coverage__grid">
        <a href="getting-started/quickstart/">
          <span>01 · Start</span>
          <strong>Build a lake you can update daily</strong>
          <small>Install, initialize, resume. The first full-market run can take hours.</small>
        </a>
        <a href="getting-started/quickstart/#browser">
          <span>02 · Dashboard</span>
          <strong>Browse and operate in the browser</strong>
          <small>Coverage, daily updates, scheduled jobs and backups. Use --read-only for browsing only.</small>
        </a>
        <a href="datasets/catalog/">
          <span>03 · Data</span>
          <strong>See what data exists and how far back it goes</strong>
          <small>Catalog, source limits, columns and units. Registered does not mean collected.</small>
        </a>
        <a href="datasets/query-guide/">
          <span>04 · Research</span>
          <strong>Price adjustment, historical universes, financial statements</strong>
          <small>Query semantics, research recipes and the Python API. Strict PIT must be turned on explicitly.</small>
        </a>
        <a href="reference/cli/">
          <span>05 · Interfaces</span>
          <strong>Commands, Python and MCP</strong>
          <small>Find CLI commands by task. MCP is read-only and never triggers collection or cleanup.</small>
        </a>
        <a href="operations/troubleshooting/">
          <span>06 · Troubleshooting</span>
          <strong>Failures, rate limiting, data gaps</strong>
          <small>Check the run and logs first, then resume over the affected scope.</small>
        </a>
      </div>
    </div>
  </section>

  <section class="cne-section">
    <div class="cne-section__heading cne-section__heading--row">
      <h2>From install to daily updates</h2>
      <p>Run these in the directory where the data will live long term. An existing configuration is reused as is.</p>
    </div>
    <ol class="cne-steps">
      <li>
        <span>01</span>
        <h3>Initialize</h3>
        <p>Generates a configuration, downloads the last 3 years of core data for the whole Shanghai, Shenzhen and Beijing market, audits it and publishes.</p>
        <pre><code>pip install cnequity
cne init</code></pre>
        <a href="getting-started/initialization/">Scope, disk and resuming</a>
      </li>
      <li>
        <span>02</span>
        <h3>Read the results</h3>
        <p>The same published data can be read with Python or SQL.</p>
        <pre><code>from cnequity.query import load
load("daily_bars", symbols=["600519.SH"])</code></pre>
        <a href="datasets/query-guide/">Price adjustment, universes and PIT</a>
      </li>
      <li>
        <span>03</span>
        <h3>Keep it updated</h3>
        <p>Run once a day. Market data updates on trading days; announcements and news still update on weekends.</p>
        <pre><code>cne run daily
cne check</code></pre>
        <a href="operations/runbook/">Scheduling, the dashboard and what to do after a failure</a>
      </li>
    </ol>
  </section>

  <section class="cne-section">
    <div class="cne-principles">
      <div class="cne-section__heading">
        <p class="cne-kicker">Before you use it</p>
        <h2>Four distinctions to keep straight</h2>
        <p>These four determine whether query results can go straight into research.</p>
      </div>
      <div class="cne-principle-list">
        <article>
          <span>01</span>
          <div>
            <h3>Registered is not collected</h3>
            <p>57 datasets are registered (49 curated + 8 derived), including optional and placeholder entries. Installing does not ship any market data.</p>
            <a href="datasets/catalog/">See the dataset catalog</a>
          </div>
        </article>
        <article>
          <span>02</span>
          <div>
            <h3>Fresh is not complete</h3>
            <p>fresh only means the dates on disk are recent enough. Historical gaps, ST, delisting evidence and PIT quality must be verified separately.</p>
            <a href="operations/runbook/">Verify with cne check</a>
          </div>
        </article>
        <article>
          <span>03</span>
          <div>
            <h3>Trading days are not calendar days</h3>
            <p>The daily update groups run on trading days. Announcements and news run every day, including weekends. Scheduling once a day is enough.</p>
            <a href="operations/runbook/#web-schedule">Scheduled jobs</a>
          </div>
        </article>
        <article>
          <span>04</span>
          <div>
            <h3>Backfill is not strict PIT</h3>
            <p>Financial statements backfilled today cannot prove that a past rebalance date had already seen that version. Use pit_mode="strict" explicitly in research.</p>
            <a href="recipes/pit-rebalance/">PIT recipe</a>
          </div>
        </article>
      </div>
    </div>
  </section>

  <section class="cne-cta">
    <div>
      <p class="cne-kicker">Next step</p>
      <h2>Get your first result with the quickstart</h2>
    </div>
    <div class="cne-actions">
      <a class="cne-button cne-button--primary" href="getting-started/quickstart/">Quickstart</a>
      <a class="cne-button cne-button--ghost" href="changelog/">Changelog</a>
    </div>
  </section>
  <p class="cne-fine">These docs follow the main branch; changes not yet on PyPI sit at the top of the <a href="changelog/">changelog</a> under a version marked unreleased (未发布). Run <code>cne --version</code> to check your installed version. Code is Apache-2.0; data is subject to each upstream source's terms, see <a href="legal-and-data-sources/">License and sources</a>. For direction and boundaries, see <a href="architecture/overview/">Product design</a>. FAQ: <a href="guides/free-a-share-data/">free A-share historical data</a>, <a href="guides/avoid-look-ahead-bias/">avoiding look-ahead bias</a>, <a href="guides/ai-query-a-shares/">asking AI about A-shares</a>, <a href="architecture/comparison/">AKShare, Tushare or Baostock</a>. Report problems in <a href="https://github.com/rootSunc/CNEquity/issues">Issues</a>.</p>
</div>
