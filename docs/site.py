"""Build redirects for retired documentation URLs and check the rendered site."""

from __future__ import annotations

import argparse
import json
import posixpath
import re
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
    "cne-upgrade": "getting-started/upgrade-0.25/",
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


def on_files(files, config):
    from mkdocs.structure.files import File

    assets = Path(__file__).resolve().parent / "assets"
    shared_assets = [
        *assets.glob("architecture-diagram.*.svg"),
        *assets.glob("cne-serve-sample.*.png"),
    ]
    for path in sorted(shared_assets):
        files.append(
            File(
                f"assets/{path.name}",
                src_dir=str(assets.parent),
                dest_dir=config.site_dir,
                use_directory_urls=config.use_directory_urls,
            )
        )
    return files


_H4_RE = re.compile(
    r'<h4 id="([^"]+)">(.*?)<a class="headerlink" href="#[^"]+" title="[^"]+">[^<]*</a></h4>',
    re.S,
)


def _dataset_support(docs_dir: str) -> dict[str, str]:
    catalog = Path(docs_dir) / "datasets" / "catalog.md"
    support: dict[str, str] = {}
    if catalog.is_file():
        for line in catalog.read_text(encoding="utf-8").splitlines():
            if match := re.match(
                r"^\|\s*([a-z][a-z0-9_]*)\s*\|\s*(core|extended|best_effort|optional)\s*\|",
                line,
            ):
                support[match.group(1)] = match.group(2)
    return support


def on_page_content(html, page, config, files):
    if page.file.src_uri != "datasets/schema.md":
        return html
    support_map = _dataset_support(config.docs_dir)
    matches = list(_H4_RE.finditer(html))
    if not matches:
        return html

    chunks: list[str] = [html[: matches[0].start()]]
    for idx, match in enumerate(matches):
        anchor, title = match.group(1), match.group(2).strip()
        next_start = matches[idx + 1].start() if idx + 1 < len(matches) else len(html)
        body = html[match.end() : next_start]
        names = [w for w in re.findall(r"[a-z][a-z0-9_]*", title) if w in support_map]
        if not names:
            tier = "on_demand"
        elif any(support_map[n] == "core" for n in names):
            tier = "core"
        else:
            tier = support_map[names[0]]

        if tier == "core":
            heading = match.group(0).replace(
                '<a class="headerlink"',
                ' <code class="schema-tier-badge schema-tier-core">core</code><a class="headerlink"',
                1,
            )
            chunks.append(heading + body)
        else:
            chunks.append(
                f'<details class="note schema-details">'
                f'<summary id="{escape(anchor)}">'
                f'<span class="schema-summary-title">{title}</span> '
                f'<code class="schema-tier-badge">{escape(tier)}</code>'
                f"</summary>"
                f"{body}"
                f"</details>\n"
            )
    return "".join(chunks)


def _relative(source: str, target: str) -> str:
    path, fragment, anchor = target.partition("#")
    if urlsplit(path).scheme:
        return target
    return posixpath.relpath(path or ".", source) + "/" + fragment + anchor


def _version_entry(doc_version: str) -> dict[str, object]:
    parts = doc_version.split(".")
    short = ".".join(parts[:2]) if len(parts) >= 2 else doc_version
    return {"version": short, "title": doc_version, "aliases": ["latest"]}


def on_post_build(config):
    site_dir = Path(config.site_dir)
    if config.theme["language"] == "zh":
        versions_file = site_dir / "versions.json"
        if not versions_file.exists():
            doc_version = config.extra.get("doc_version", "0.25.0")
            versions_file.write_text(
                json.dumps([_version_entry(doc_version)], indent=2) + "\n",
                encoding="utf-8",
            )
    for source, destination in REDIRECTS.items():
        target = _relative(source, destination)
        file = site_dir / source / "index.html"
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


def bundle_mike_version(site: Path, doc_version: str, preserve_from: Path | None = None) -> None:
    """Populate mike versioned directories (<major.minor>/ and latest/) alongside root."""
    import shutil

    entry = _version_entry(doc_version)
    short = str(entry["version"])
    prior_entries: list[dict[str, object]] = []
    for candidate in filter(None, [preserve_from, site]):
        vf = candidate / "versions.json"
        if vf.is_file():
            try:
                loaded = json.loads(vf.read_text(encoding="utf-8"))
                if isinstance(loaded, list):
                    prior_entries = [e for e in loaded if isinstance(e, dict)]
                    break
            except ValueError:
                pass

    merged_entries: list[dict[str, object]] = [entry]
    preserved_dirs: set[str] = {short, "latest", "versions.json"}
    for old in prior_entries:
        old_ver = str(old.get("version", ""))
        if not old_ver or old_ver == short:
            continue
        aliases = [a for a in old.get("aliases", []) if a != "latest"]
        merged_entries.append(
            {"version": old_ver, "title": old.get("title", old_ver), "aliases": aliases}
        )
        preserved_dirs.add(old_ver)
        if preserve_from is not None and (preserve_from / old_ver).is_dir():
            dest_old = site / old_ver
            if not dest_old.exists():
                shutil.copytree(preserve_from / old_ver, dest_old)

    snapshot_items = [p for p in site.iterdir() if p.name not in preserved_dirs]
    for dest_name in (short, "latest"):
        dest_root = site / dest_name
        if dest_root.exists():
            shutil.rmtree(dest_root)
        dest_root.mkdir(parents=True, exist_ok=True)
        for item in snapshot_items:
            target = dest_root / item.name
            if item.is_dir():
                shutil.copytree(item, target)
            else:
                shutil.copy2(item, target)
        # Allow /CNEquity/<ver>/en/ to resolve ../versions.json within its own version dir.
        en_entries = [
            {
                "version": f"../{e['version']}/en",
                "title": e["title"],
                "aliases": e["aliases"],
            }
            for e in merged_entries
        ]
        (dest_root / "versions.json").write_text(
            json.dumps(en_entries, indent=2) + "\n",
            encoding="utf-8",
        )
    (site / "versions.json").write_text(
        json.dumps(merged_entries, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Check links in both built documentation trees.")
    parser.add_argument("site", type=Path)
    parser.add_argument(
        "--bundle-mike",
        action="store_true",
        help="also populate mike versioned directories (<major.minor>/ and latest/)",
    )
    parser.add_argument(
        "--preserve-from",
        type=Path,
        default=None,
        help="optional directory containing previously deployed mike versions to retain",
    )
    args = parser.parse_args()
    if not (args.site / "en" / "index.html").is_file() or not (args.site / "index.html").is_file():
        parser.error("build both language sites before checking links")
    from mkdocs.config import load_config

    cfg = load_config(config_file="mkdocs.yml")
    broken = check_links(args.site, cfg.site_url)
    if broken:
        print("\n".join(broken))
        return 1
    if args.bundle_mike:
        pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
        doc_version = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]["version"]
        bundle_mike_version(args.site, doc_version, args.preserve_from)
    print("Rendered documentation links passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
