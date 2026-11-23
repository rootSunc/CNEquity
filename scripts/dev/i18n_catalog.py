"""Keep the translation catalog in step with the source.

Messages are English in the code, marked with `_()`, `N_()`, `ngettext()` or
`pgettext()` from `cnequity.i18n`; their Chinese lives in
``src/cnequity/locale/zh_CN/LC_MESSAGES/cnequity.po``. The compiled ``.mo`` is
committed beside it, like the dashboard bundle, so installing the package needs
no build step:

    python scripts/dev/i18n_catalog.py update    # add new messages, drop removed ones
    python scripts/dev/i18n_catalog.py compile   # rebuild the .mo from the .po
    python scripts/dev/i18n_catalog.py           # check; exits 1 on any problem

The check fails when a marked message has no catalog entry or no translation,
an entry is fuzzy or no longer in the code, a translation's placeholders differ
from its source, or the committed ``.mo`` is not what the ``.po`` compiles to.
``tests/unit/test_i18n_catalog.py`` runs it. Babel is a contributor tool only
(the dev dependency group); the installed package reads the ``.mo`` with the
standard library.
"""

from __future__ import annotations

import argparse
import ast
import io
import re
import sys
from collections import Counter
from pathlib import Path

from babel.messages.catalog import Catalog
from babel.messages.mofile import write_mo
from babel.messages.pofile import read_po, write_po

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE = REPO_ROOT / "src" / "cnequity"
sys.path.insert(0, str(PACKAGE.parent))

from cnequity.i18n import DOMAIN, LOCALE_DIR, SUPPORTED_LANGUAGES, catalog_key  # noqa: E402

KEYWORDS = {
    "_": None,
    "gettext": None,
    "N_": None,
    "ngettext": (1, 2),
    "pgettext": ((1, "c"), 2),
    "NP_": ((1, "c"), 2),
    "word": ((1, "c"), 2),
}
TRANSLATED = [lang for lang in SUPPORTED_LANGUAGES if lang != "en"]

# printf conversions (`%s`, `%(name)d`, `%.1f`) and str.format fields
# (`{}`, `{name}`, `{0:>5}`). `%%` and `{{`/`}}` are literal and skipped.
_PRINTF = re.compile(r"%(?:\((?P<name>[^)]+)\))?[#0 +-]*\d*(?:\.\d+)?(?P<conv>[diouxXeEfFgGcrsa%])")
_BRACE = re.compile(r"(?<!\{)\{(?P<field>[^{}]*)\}(?!\})")


def po_path(language: str) -> Path:
    return LOCALE_DIR / language / "LC_MESSAGES" / f"{DOMAIN}.po"


def mo_path(language: str) -> Path:
    return po_path(language).with_suffix(".mo")


def extract() -> Catalog:
    """Every marked message in the package, keyed as `cnequity.i18n` looks it up."""
    template = Catalog(domain=DOMAIN, project="CNEquity")
    for filename, message, context in marked_calls():
        template.add(message, locations=[(f"cnequity/{filename}", None)], context=context)
    for filename, message in command_docstrings():
        template.add(message, locations=[(f"cnequity/{filename}", None)])
    return template


def _call_name(func: ast.expr) -> str | None:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def marked_calls():
    """`(file, message, context)` for each keyword call with literal arguments.

    Parsed with `ast` rather than Babel's tokenizer: before Python 3.12 an
    f-string is one token, so a tokenizer misses `_()` calls inside it and the
    catalog would depend on the interpreter. Calls whose message arguments are
    not all string literals are runtime lookups and are skipped.
    """
    for path in sorted(PACKAGE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = _call_name(node.func)
            if name not in KEYWORDS:
                continue
            spec = KEYWORDS[name] or (1,)
            literals = [
                arg.value if isinstance(arg, ast.Constant) and isinstance(arg.value, str) else None
                for arg in node.args
            ]
            context = None
            parts: list[str] = []
            for index in spec:
                is_context = isinstance(index, tuple)
                position = index[0] if is_context else index
                value = literals[position - 1] if position <= len(literals) else None
                if value is None:
                    break
                if is_context:
                    context = value
                else:
                    parts.append(value)
            else:
                if parts and parts[0]:
                    message = parts[0] if len(parts) == 1 else tuple(parts)
                    yield path.relative_to(PACKAGE).as_posix(), message, context


def _is_command_decorator(decorator: ast.expr) -> bool:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    if isinstance(target, ast.Attribute):
        return target.attr in {"command", "group"}
    return isinstance(target, ast.Name) and target.id in {"command", "group"}


def command_docstrings():
    """The docstrings Click shows as command help.

    A `@cli.command()` function's docstring is its help text, so it is a
    message like any `N_()` string; extractors only know call keywords, so
    these are collected here. Ordinary docstrings are code documentation and
    are not.
    """
    for path in sorted(PACKAGE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not any(_is_command_decorator(d) for d in node.decorator_list):
                continue
            first = node.body[0] if node.body else None
            if (
                isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)
            ):
                yield path.relative_to(PACKAGE).as_posix(), catalog_key(first.value.value)


def read(language: str) -> Catalog:
    path = po_path(language)
    if not path.exists():
        return Catalog(locale=language, domain=DOMAIN, project="CNEquity")
    with path.open("rb") as handle:
        return read_po(handle, locale=language, domain=DOMAIN)


def render_po(catalog: Catalog) -> bytes:
    buffer = io.BytesIO()
    write_po(buffer, catalog, width=0, sort_output=True, include_lineno=False, ignore_obsolete=True)
    return buffer.getvalue()


def render_mo(catalog: Catalog) -> bytes:
    buffer = io.BytesIO()
    write_mo(buffer, catalog, use_fuzzy=False)
    return buffer.getvalue()


def update() -> None:
    template = extract()
    for language in TRANSLATED:
        catalog = read(language)
        # Keep the header dates the file already has, so an update that adds
        # nothing leaves the file byte-identical.
        template.creation_date = catalog.creation_date
        catalog.update(template, no_fuzzy_matching=True, update_header_comment=False)
        catalog.revision_date = template.creation_date
        # Babel fills these with placeholders (`FULL NAME <EMAIL@ADDRESS>`)
        # whenever it writes; say something true instead.
        catalog.project, catalog.version = "CNEquity", "1"
        catalog.last_translator = catalog.language_team = "CNEquity contributors"
        catalog.msgid_bugs_address = "https://github.com/rootSunc/CNEquity/issues"
        catalog.fuzzy = False
        path = po_path(language)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(render_po(catalog))
        print(f"updated {path.relative_to(REPO_ROOT)}")


def compile_all() -> None:
    for language in TRANSLATED:
        path = mo_path(language)
        path.write_bytes(render_mo(read(language)))
        print(f"compiled {path.relative_to(REPO_ROOT)}")


def placeholders(text: str) -> tuple[list[str], Counter[str]]:
    """Positional printf conversions in order, and every named one as a count."""
    positional: list[str] = []
    named: Counter[str] = Counter()
    for match in _PRINTF.finditer(text):
        if match["conv"] == "%":
            continue
        if match["name"] is None:
            positional.append(match["conv"])
        else:
            named[f"%({match['name']}){match['conv']}"] += 1
    for match in _BRACE.finditer(text):
        named["{" + match["field"] + "}"] += 1
    return positional, named


def problems() -> list[str]:
    found: list[str] = []
    wanted = {(message.id, message.context) for message in extract() if message.id}
    for language in TRANSLATED:
        rel = po_path(language).relative_to(REPO_ROOT)
        if not po_path(language).exists():
            found.append(f"{rel}: missing; run `python scripts/dev/i18n_catalog.py update`")
            continue
        catalog = read(language)
        have = {(message.id, message.context) for message in catalog if message.id}
        for msgid, _context in sorted(wanted - have, key=str):
            found.append(f"{rel}: no entry for {msgid!r}; run `update`")
        for msgid, _context in sorted(have - wanted, key=str):
            found.append(f"{rel}: {msgid!r} is no longer in the code; run `update`")
        for message in catalog:
            if not message.id:
                continue
            ids = message.id if isinstance(message.id, tuple) else (message.id,)
            strings = message.string if isinstance(message.string, tuple) else (message.string,)
            if message.fuzzy:
                found.append(f"{rel}: {ids[0]!r} is marked fuzzy")
            if not all(strings):
                found.append(f"{rel}: {ids[0]!r} has no translation")
                continue
            source = placeholders(ids[-1])
            for string in strings:
                if placeholders(string) != source:
                    found.append(f"{rel}: placeholders differ for {ids[0]!r}: {string!r}")
                # gettext's own rule (msgfmt -c): a leading or trailing newline
                # is layout, and the translation has to keep it.
                if (string[:1] == "\n") != (ids[0][:1] == "\n") or (string[-1:] == "\n") != (
                    ids[0][-1:] == "\n"
                ):
                    found.append(f"{rel}: leading/trailing newline differs for {ids[0]!r}")
        mo = mo_path(language)
        if not mo.exists() or mo.read_bytes() != render_mo(catalog):
            found.append(
                f"{mo.relative_to(REPO_ROOT)}: stale; run `python scripts/dev/i18n_catalog.py compile`"
            )
    return found


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "action", nargs="?", choices=("check", "update", "compile"), default="check"
    )
    args = parser.parse_args(argv)
    if args.action == "update":
        update()
        return 0
    if args.action == "compile":
        compile_all()
        return 0
    found = problems()
    for line in found:
        print(line)
    return 1 if found else 0


if __name__ == "__main__":
    raise SystemExit(main())
