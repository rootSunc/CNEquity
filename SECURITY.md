# 安全策略

## 支持的版本

安全修复合入 `main` 的最新提交。1.0 之前的 `0.x` 不维护长期补丁分支。

## 如何报告漏洞

请 **不要** 为安全问题开公开 GitHub issue。

优先任选其一：

1. [GitHub Security Advisories](https://github.com/rootSunc/cn-market-lake/security/advisories/new)
   （私密报告），或
2. 通过仓库所有者（GitHub：`rootSunc`）建立私密联系渠道。

报告时请尽量包含：

- 受影响版本 / commit
- 影响面（数据完整性、凭证泄露、远程代码执行等）
- 最小复现步骤
- 是否已有修复或变通办法

我们目标在 7 日内确认收到，并在修复可用后协商披露节奏。

## 范围说明

- 本项目从第三方 HTTP/TCP 接口拉取行情等数据。仅涉及上游可用性、限速或服务条款争议的问题，
  **不算** 安全漏洞——见 [许可与数据合规](docs/legal-and-data-sources.md)。
- 本地配置（`configs/cn-market-lake.toml`）、`data/` 下的湖数据与运行日志不得提交。
  若在 git 历史中发现密钥，请私下报告，以便在更广披露前清理历史。
