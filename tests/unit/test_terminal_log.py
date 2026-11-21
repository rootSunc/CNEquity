"""The terminal's short, translated view of the log, beside the English log file."""

from __future__ import annotations

import io
import logging
import os

import pytest
from babel.messages.catalog import Catalog
from babel.messages.mofile import write_mo

from cnequity import i18n
from cnequity.cli._shared import LOG_FORMAT, terminal_formatter
from cnequity.i18n import Message
from cnequity.terminal_log import RedactedFormatter, TerminalFormatter


def _record(msg, *args, level=logging.INFO, name="cnequity.orchestrator.engine", exc_info=None):
    return logging.LogRecord(name, level, __file__, 1, msg, args, exc_info)


def test_one_record_reads_english_in_the_file_and_chinese_on_screen():
    record = _record(
        "Step %s %s in %.1fs (%s rows)",
        "daily_bars",
        Message.word("step status", "success"),
        3.24,
        52341,
    )
    msg, args = record.msg, record.args
    file_line = logging.Formatter(LOG_FORMAT).format(record)
    screen = TerminalFormatter("zh_CN").format(record)

    assert file_line.endswith(
        f"INFO cnequity.orchestrator.engine [pid={os.getpid()}]: Step daily_bars success in 3.2s (52341 rows)"
    )
    assert screen.endswith(" 步骤 daily_bars 成功，用时 3.2s（52341 行）")
    # The formatter changed nothing a later handler would see.
    assert record.msg is msg and record.args is args
    assert record.getMessage() == "Step daily_bars success in 3.2s (52341 rows)"


@pytest.mark.parametrize("formatter", [RedactedFormatter(), TerminalFormatter("zh_CN")])
def test_log_formats_mask_credentials_without_changing_the_record(formatter):
    record = _record(
        "GET https://user:password-value@example.test/?token=query-value --api-key flag-value "
        "Authorization: Bearer header-value configured-value"
    )
    formatter.secrets = ("configured-value",)
    shown = formatter.format(record)
    for secret in (
        "password-value",
        "query-value",
        "flag-value",
        "header-value",
        "configured-value",
    ):
        assert secret not in shown
        assert secret in record.getMessage()


def test_english_screen_is_short_and_tags_problems():
    english = TerminalFormatter("en")
    assert english.format(_record("Step %s starting", "daily_bars"))[8:] == (
        " Step daily_bars starting"
    )
    warning = english.format(_record("Step %s skipped: %s", "x", "why", level=logging.WARNING))
    assert warning[8:] == " Warning: Step x skipped: why"
    chinese = TerminalFormatter("zh_CN").format(
        _record("Step %s skipped: %s", "x", "why", level=logging.ERROR)
    )
    assert chinese[8:] == " 错误：步骤 x 已跳过：why"


def test_fragments_inside_arguments_translate_with_their_line():
    record = _record(
        "%s %d/%d %s · %s elapsed%s",
        "adj_factors",
        100,
        5000,
        Message.word("progress unit", "symbols"),
        "1m02s",
        Message(" · ~%s left", "48m"),
    )
    assert record.getMessage() == "adj_factors 100/5000 symbols · 1m02s elapsed · ~48m left"
    assert TerminalFormatter("zh_CN").format(record)[8:] == (
        " adj_factors 100/5000 只证券 · 已用 1m02s · 约剩 48m"
    )


def test_untranslated_and_foreign_lines_show_as_written():
    zh = TerminalFormatter("zh_CN")
    assert zh.format(_record("no catalog entry for %s", "this"))[8:] == " no catalog entry for this"
    foreign = zh.format(
        _record("Connection pool is full", level=logging.WARNING, name="urllib3.connectionpool")
    )
    assert foreign[8:] == " 警告：[urllib3.connectionpool] Connection pool is full"


def test_a_bad_translation_falls_back_to_the_source(tmp_path, monkeypatch):
    target = tmp_path / "zh_CN" / "LC_MESSAGES"
    target.mkdir(parents=True)
    catalog = Catalog(locale="zh_CN", domain=i18n.DOMAIN, fuzzy=False)
    catalog.add("Step %s starting", "开始执行步骤 %s %s")  # one placeholder too many
    with (target / f"{i18n.DOMAIN}.mo").open("wb") as handle:
        write_mo(handle, catalog)
    monkeypatch.setattr(i18n, "LOCALE_DIR", tmp_path)
    i18n.translator.cache_clear()
    try:
        line = TerminalFormatter("zh_CN").format(_record("Step %s starting", "daily_bars"))
    finally:
        i18n.translator.cache_clear()
    assert line[8:] == " Step daily_bars starting"


def test_tracebacks_still_reach_the_terminal():
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        import sys

        record = _record(
            "Step %s failed after %.1fs", "x", 1.0, level=logging.ERROR, exc_info=sys.exc_info()
        )
    text = TerminalFormatter("zh_CN").format(record)
    assert "错误：步骤 x 失败，用时 1.0s" in text
    assert "RuntimeError: boom" in text


def test_message_is_its_english_text():
    word = Message.word("step status", "success")
    assert word == "success" and isinstance(word, str)
    # %-formatting on purpose: it is how `logging` renders arguments.
    rendered = "%s/%s" % (word, Message(" · ~%s left", "3m"))  # noqa: UP031
    assert rendered == "success/ · ~3m left"
    assert word.render("zh_CN") == "成功"


class _Stream(io.StringIO):
    def __init__(self, tty: bool) -> None:
        super().__init__()
        self._tty = tty

    def isatty(self) -> bool:
        return self._tty


@pytest.mark.parametrize(
    ("tty", "expected"), [(True, TerminalFormatter), (False, RedactedFormatter)]
)
def test_only_an_interactive_terminal_gets_the_short_view(tty, expected):
    formatter = terminal_formatter(_Stream(tty))
    assert type(formatter) is expected
    if not tty:
        assert formatter._fmt == LOG_FORMAT
