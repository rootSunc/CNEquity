# 维护者发布清单

本仓库由维护者按需发布。推送正式版本标签会触发 [Release 工作流](https://github.com/rootSunc/CNEquity/blob/main/.github/workflows/release.yml)，并在构建通过后向 PyPI 发布，随后据 `CHANGELOG.md` 对应小节创建（或更新）同名 GitHub Release；普通分支提交和手动触发工作流只构建、检查，不发布。标签必须是 `vMAJOR.MINOR.PATCH`，版本须与包一致，且标签所指提交已在 `main`。

## 发布前

1. 收口变更并审阅差异。确认 `CHANGELOG.md` 写明用户可见变化、来源限制和迁移事项；私有湖测量与运行日志留在 `private/`，不要放入包或公开文档。
2. 同步 `pyproject.toml`、`src/cnequity/__init__.py`、`CITATION.cff` 和 `server.json`（顶层与 `packages[0]` 两处）的正式版本。用 `cne contract show --out contracts/v<版本>.json` 生成契约；把 `pyproject.toml` 里 `share/cnequity/contracts` 的唯一一项改成这份契约（wheel 只带当前版本的契约，历史契约留在仓库和 sdist）。如有不兼容变化，在 `contracts/migrations/<版本>/` 写明变更、迁移和回退，并更新相应测试。完成版本变更前不要创建正式标签。
3. 在隔离的临时湖验证 `cne init --profile sample`、`cne query --sql "SELECT 1"`、`cne status --datasets` 等常用命令。真实来源可达性和本地湖覆盖单独记录；某个来源限流或拒绝访问，不应以重试风暴掩盖，也不能据此宣称全市场数据已齐。
4. 本地运行 CI 的质量、离线测试、前端和文档检查，以及 Release 工作流中的契约比较、恢复演练、源码包/轮子检查和干净环境轮子冒烟。网络依赖安全审计由工作流执行；检查结果应针对同一待发布提交。
5. 将待发布提交合入 `main`，确认 CI 与安全工作流通过。核对 GitHub `pypi` environment 只允许正式版本标签部署；若希望人工复核，配置 required reviewer。PyPI Trusted Publisher 应只信任本仓库的 Release 工作流与该 environment。

## 发布

在已核验的 `main` 提交上创建并推送正式标签。确认 Release 工作流的构建、契约和发布 job 均通过；`github-release` job 随后创建 GitHub Release（说明由 `scripts/dev/release_notes.py` 从 CHANGELOG 生成，无需手动 `gh release create`）；核对 PyPI 页面中的版本、说明、Releases 页面和安装后的 `cne --version`。标签与 PyPI 文件一经发布不可当作可重写草稿；若发现问题，发修复版本并说明影响。

PyPI 上线后，把同一版本登记到 [MCP Registry](https://registry.modelcontextprotocol.io)：安装 `mcp-publisher`，在仓库根目录运行 `mcp-publisher login github`（以 rootSunc 账号授权），再运行 `mcp-publisher publish`。Registry 通过 PyPI 说明里的 `mcp-name: io.github.rootSunc/cnequity` 注释核验包归属，这行注释在 `README.md` 末尾，由同步脚本带进 `README.pypi.md`，不要删。

平时只需维护一个稳定发布入口。多 Python 版本与平台由 CI 矩阵覆盖，Release 工作流重复执行一次关键离线回归，是为了让实际发布的标签与构建产物有独立证据；无需为每个版本手动重复整套矩阵。`pypi` environment 的审批与标签保护是 GitHub 仓库设置，不在此工作流文件中，维护者须到仓库设置中核验。
