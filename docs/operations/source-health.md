# 数据源健康度：这张表是怎么来的

这些源不是本项目专属的——AkShare、各类取数 skill、你自己写的爬虫，走的是同一批端点。其中某一个变了，通常没有地方可查，你得先花半天怀疑自己的代码。这个湖本来就每个交易日跑全市场，顺手多发一个请求就知道了。

---

## 怎么用

```bash
asl sources --vantage cn     # 探测一遍，报告写进 meta/source_health/cn.json
asl serve                    # → http://127.0.0.1:8787/source-health
```

**探测在 CLI，展示在 serve。** 面板只读——它不会替你去请求十几个第三方主机，和它不触发采集是同一个理由：一个无鉴权的本地服务，不该能被一个走神的浏览器标签页指向别人的接口。

只测某几个源：

```bash
asl sources --only eastmoney_push2,sina,tdx_protocol
```

`--vantage` 是**必须认真填**的：它记录这次探测从哪个出口发出去。见下面「为什么视角决定结论」。每个 vantage 一个文件，页面把它们并排渲染。

---

## 五个状态

| 状态 | 含义 |
|------|------|
| **可用** | 返回了真实数据 |
| **空响应** | 连上了、HTTP 也正常，但没有数据 |
| **被拒** | 到达了但被拒绝（403 / 风控页 / challenge） |
| **不可达** | 连不上或超时 |
| **未探测** | 配置里关了，或被 `--only` 排除 |

### 为什么「空响应」单独一档

**HTTP 200 不等于可用。** 东财会用 200 返回风控页，新浪会用 200 返回空数组，同花顺限流时同样是 200 加空响应。只看状态行的探测会把这三种都判成健康。

所以每个探测都断言**响应体**：clist 要有 `total`、kline 要有 `klines`、深交所导出要以 `PK` 开头（xlsx 是 zip；风控页是 HTML）、申万下载要以 OLE2 magic 开头。

「空响应」被单独拎出来，是因为它看起来比失败更健康，实际更危险——回填会静默截断，而且从外面看不出来。

### 为什么「被拒」不算「挂了」

「拒绝了你」和「它不在那儿」指向完全不同的修法。前者换个网络就好，后者换网络也没用。

---

## 为什么视角决定结论

好几个源在 WAF 层拒绝非大陆出口。**同一个主机、同一秒，大陆探测可以是绿的、海外探测是红的，两个都是真的。**

所以每份报告带 `vantage` 标签，页面把不同视角**并排**放，不合并成一个结论——合并等于凭空造一个哪次探测都没测到的「事实」。

在两个网络里各跑一次，页面就有两列：

```bash
asl sources --vantage cn          # 大陆出口
asl sources --vantage overseas    # 挂代理 / 海外机器上再跑一次
```

文件名不决定列名，JSON 里的 `vantage` 字段才决定。同名会覆盖，所以同一个出口重复跑就是刷新那一列。

## 一次探测不是 SLA

每个源每次只发**一个**请求。绿色说明那一个请求成功了，不代表接下来一千个也会成功——对 `q.10jqka.com.cn`（实测 1 req/s 到第 23 个就 401）和 baostock（一个会话约 43 次查询后进黑名单）这种源来说，那是完全不同的问题。

探测是**串行**的。这些正是日更流水线依赖的主机，十几个请求一起打出去，是健康检查自己制造它本该观测的故障。

---

## 探测走的是适配器自己的代码

URL 常量、东财的鉴权头、上交所需要的 Chrome TLS 伪装、同花顺的限速、TDX 的二进制协议——用的都是流水线在用的那套。适配器改了，探测跟着改；探测绿而流水线红这种情况，不会因为两边各写一份 URL 而发生。

反过来也成立：探测所需的日期用的是**三天前的最近工作日**，不是今天。好几个端点在收盘前没有当日数据，每天早上飘红的表会被训练成没人看。

---

## 报告存在哪

`{data_root}/meta/source_health/<vantage>.json`，和湖的其它元数据放在一起。`asl serve` 启动时不读，访问 `/source-health` 时才读——所以先跑探测再刷新页面即可，不用重启。

**没有内置的定时发布。** 想每天自动跑就挂进你现有的调度里（见 [runbook](runbook.md)）：

```bash
asl sources --vantage cn >> logs/source-health.log 2>&1
```

**探测失败不会让命令失败。** 源变红是这条命令的**输出**而不是它的错误；非零退出会让调度在最该记录的那天报错并跳过。

## 加一个源

`src/ashare_lake/diagnostics/source_health.py` 里加一个 `SourceProbe`：

```python
SourceProbe(
    key="my_source",
    label="人读的名字",
    host="api.example.com",
    powers=("dataset_a", "dataset_b"),   # 它挂了会影响什么
    run=_probe_my_source,                # 一个请求，断言响应体
    note="已知的坑",
    blast_radius="example",              # 同一 WAF 的填同一个值
    config_key="my_source",              # [sources.x] 关掉时跳过
)
```

`run` 返回一段人读的证据字符串（"1119 条"、"9 根日线"），或者抛 `ProbeEmpty` / `ProbeBlocked`。其它异常都归入「不可达」。

**同一个 `blast_radius` 的探测必须在 `PROBES` 里连续排列**——页面按 radius 变化插分组标题，不连续会给一个 WAF 打出两个标题。有测试守着这条。

---

## 相关文档

- [CLI](../reference/cli.md#asl-sources) · [serve 面板](../modules/serve.md) · [逐源限制](../datasets/sources.md) · [故障排查](troubleshooting.md)
