"""Build redirects for retired documentation URLs and check the rendered site."""

from __future__ import annotations

import argparse
import json
import posixpath
from html import escape
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit

import tomllib

RELEASES = "https://github.com/rootSunc/CNEquity/releases"
QUICKSTART = "getting-started/quickstart/"
INSTALL = "getting-started/installation/"
CATALOG = "datasets/catalog/"
QUERY = "datasets/query-guide/"
RUNBOOK = "operations/runbook/"
CLI = "reference/cli-surface/"
DESIGN = "architecture/overview/"
PYTHON_API = "reference/python-api/"
MCP = "reference/mcp/"
TROUBLESHOOTING = "operations/troubleshooting/"

# Use the same routes under / and /en/. Targets with fragments use common
# explicit anchors so old links can keep working in either language.
REDIRECTS = {
    "architecture/comparison/": DESIGN + "#scope",
    "architecture/data-flow/": DESIGN + "#write-path",
    "architecture/data-quality-contract/": DESIGN + "#quality",
    "architecture/design-principles/": DESIGN + "#principles",
    "architecture/lake-layout/": DESIGN + "#lake-layout",
    "changelog/": RELEASES,
    "datasets/": CATALOG,
    "datasets/contract/": CATALOG + "#contract",
    "datasets/sources/": CATALOG + "#coverage",
    "getting-started/initialization/": QUICKSTART + "#init-scope",
    "guides/ai-query-a-shares/": MCP,
    "guides/avoid-look-ahead-bias/": QUERY + "#pit",
    "guides/free-a-share-data/": QUICKSTART + "#init-scope",
    "operations/fetch-policy/": TROUBLESHOOTING + "#sources",
    "operations/releasing/": RELEASES,
    "operations/scripts/": RUNBOOK,
    "operations/source-health/": TROUBLESHOOTING + "#sources",
    "recipes/": QUERY,
    "recipes/derivatives/": QUERY + "#intraday-and-derivatives",
    "recipes/duckdb-polars/": PYTHON_API + "#sql",
    "recipes/minute-bars-15-30-60/": QUERY + "#intraday-and-derivatives",
    "recipes/pit-rebalance/": QUERY + "#pit",
    "recipes/research-baseline/": QUERY + "#prices-and-universe",
    "reference/cli-options/": CLI,
    "reference/cli/": CLI,
    "reference/universe-profiles/": QUERY + "#prices-and-universe",
}
CLI_ANCHORS = {
    "cne-upgrade": INSTALL + "#cne-upgrade",
    "cne-init": QUICKSTART + "#init-scope",
    "cne-backfill": RUNBOOK + "#backfill",
    "cne-serve": QUICKSTART + "#browser",
    "cne-mcp": MCP,
    "cne-snapshot": RUNBOOK + "#backup-and-restore",
    "command-results-and-exit-codes": RUNBOOK + "#results",
    "命令结果与退出码": RUNBOOK + "#results",
}


def on_config(config):
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    config.extra["doc_version"] = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"][
        "version"
    ]
    return config


def _relative(source: str, target: str) -> str:
    path, fragment, anchor = target.partition("#")
    if urlsplit(path).scheme:
        return target
    return posixpath.relpath(path or ".", source) + "/" + fragment + anchor


def on_post_build(config):
    for source, destination in REDIRECTS.items():
        target = _relative(source, destination)
        file = Path(config.site_dir) / source / "index.html"
        file.parent.mkdir(parents=True, exist_ok=True)
        link = escape(target)
        refresh = f'<meta http-equiv="refresh" content="0; url={link}">'
        if source == "reference/cli/":
            anchors = json.dumps(
                {key: _relative(source, dest) for key, dest in CLI_ANCHORS.items()}
            )
            refresh = (
                f"<noscript>{refresh}</noscript>"
                f"<script>const links = new Map(Object.entries({anchors}));"
                f"const key = decodeURIComponent(location.hash.slice(1));"
                f"location.replace(links.get(key) || {json.dumps(target)});</script>"
            )
        label = (
            "前往当前文档" if config.theme["language"] == "zh" else "Continue to the current docs"
        )
        file.write_text(
            '<!doctype html><html><head><meta charset="utf-8">'
            '<meta name="robots" content="noindex">'
            f'<link rel="canonical" href="{link}">{refresh}<title>{label}</title>'
            f'</head><body><a href="{link}">{label}</a></body></html>\n',
            encoding="utf-8",
        )


class _Page(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids: set[str] = set()
        self.urls: set[str] = set()

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if fragment := attributes.get("id"):
            self.ids.add(fragment)
        if tag in {"a", "img", "link", "script"}:
            if url := attributes.get("href") or attributes.get("src"):
                self.urls.add(url)


def check_links(site: Path, site_url: str) -> list[str]:
    base = urlsplit(site_url)
    pages = {}
    for path in sorted(site.rglob("*.html")):
        page = _Page()
        page.feed(path.read_text(encoding="utf-8"))
        pages[path] = page

    broken = []
    for source, page in pages.items():
        relative = source.relative_to(site).as_posix()
        page_url = urljoin(site_url, relative.removesuffix("index.html"))
        for url in sorted(page.urls):
            target = urlsplit(urljoin(page_url, url))
            if target.netloc != base.netloc or not target.path.startswith(base.path):
                continue
            path = site / unquote(target.path.removeprefix(base.path))
            if path.is_dir():
                path /= "index.html"
            fragment = unquote(target.fragment)
            if not path.is_file() or (
                fragment and path in pages and fragment not in pages[path].ids
            ):
                broken.append(f"{relative}: {url}")
    return broken


def main() -> int:
    parser = argparse.ArgumentParser(description="Check links in both built documentation trees.")
    parser.add_argument("site", type=Path)
    args = parser.parse_args()
    if not (args.site / "en" / "index.html").is_file() or not (args.site / "index.html").is_file():
        parser.error("build both language sites before checking links")
    from mkdocs.config import load_config

    site_url = load_config(config_file="mkdocs.yml").site_url
    broken = check_links(args.site, site_url)
    if broken:
        print("\n".join(broken))
        return 1
    print("Rendered documentation links passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
