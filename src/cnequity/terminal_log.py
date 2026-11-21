"""The terminal view of the log: short, and in the reader's language.

The log file is for grep and bug reports: full timestamps, level, logger name,
English. The terminal of a long `cne init` is a progress screen someone is
watching, and `2026-10-16 10:00:01,123 INFO cnequity.orchestrator.engine:` in
front of every line is noise there — more so once the message after it is
Chinese. So an interactive terminal gets its own formatter:

    10:00:01 Step daily_bars starting
    10:04:15 Warning: still working: capital 3m02s (no output for 1m03s)

and the same lines in Chinese when the language is zh_CN. Only the formatting
differs: the `LogRecord` is never modified, so the file handler, the heartbeat
filter and any handler a host application adds see the original record.

A template is translated when it has a catalog entry — the pipeline marks the
ones people watch with `N_()` — and arguments that are `i18n.Message`s are
rendered in the same language. Anything else, a third-party warning included,
is shown as it was written.
"""

from __future__ import annotations

import logging
import os
import re
import time

from cnequity.i18n import DEFAULT_LANGUAGE, N_, Message, current_language, translate_to

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s [pid=%(process)d]: %(message)s"

_TAGS = {
    logging.WARNING: N_("Warning: "),
    logging.ERROR: N_("Error: "),
    logging.CRITICAL: N_("Error: "),
}

_URL_USERINFO = re.compile(r"(\b[a-z][a-z0-9+.-]*://)[^\s/@]+@", re.IGNORECASE)
_SECRET_KEY = r"(?:tushare[-_]?token|hithink[-_]finance[-_]api[-_]key|access[-_]?token|api[-_]?key|token|password|secret)"
_SECRET_VALUE = re.compile(
    rf"""(?i)((?:--{_SECRET_KEY}(?:\s+|=)|\b{_SECRET_KEY}['"]?\s*[:=]\s*))"""
    r"""(?:"[^"]*"|'[^']*'|[^\s&#,;]+)"""
)
_BEARER = re.compile(r"(\b(?:Bearer|Basic)\s+)\S+", re.IGNORECASE)


def redact_log_text(text: str, secrets: tuple[str, ...] = ()) -> str:
    for secret in secrets:
        text = text.replace(secret, "<redacted>")
    text = _URL_USERINFO.sub(r"\1<redacted>@", text)
    text = _SECRET_VALUE.sub(r"\1<redacted>", text)
    return _BEARER.sub(r"\1<redacted>", text)


def configured_log_secrets(config=None) -> tuple[str, ...]:
    return tuple(
        value
        for value in (
            getattr(config, "tushare_token", None),
            getattr(config, "ths_official_api_key", None),
            os.environ.get("TUSHARE_TOKEN"),
            os.environ.get("HITHINK_FINANCE_API_KEY"),
        )
        if value
    )


class RedactedFormatter(logging.Formatter):
    """Mask credential values after rendering the record and its traceback."""

    secrets: tuple[str, ...] = ()

    def format(self, record: logging.LogRecord) -> str:
        return redact_log_text(super().format(record), self.secrets)


def diagnostic_formatter(config=None) -> RedactedFormatter:
    formatter = RedactedFormatter(LOG_FORMAT, datefmt="%Y-%m-%dT%H:%M:%SZ")
    formatter.converter = time.gmtime
    formatter.secrets = configured_log_secrets(config)
    return formatter


def render_message(record: logging.LogRecord, language: str) -> str:
    """`record.getMessage()`, in `language` where the catalog has it."""
    if language == DEFAULT_LANGUAGE or not isinstance(record.msg, str):
        return record.getMessage()
    template = translate_to(language, record.msg)
    args = record.args
    if isinstance(args, tuple):
        args = tuple(arg.render(language) if isinstance(arg, Message) else arg for arg in args)
    if not args:
        return template
    try:
        return template % args
    except (TypeError, ValueError, KeyError):
        # A catalog mistake must never cost the line: say it the way the
        # source does.
        return record.getMessage()


class TerminalFormatter(RedactedFormatter):
    """`HH:MM:SS message`, warnings and errors tagged, in the process language."""

    def __init__(self, language: str | None = None) -> None:
        super().__init__()
        self.language = language

    def format(self, record: logging.LogRecord) -> str:
        language = self.language or current_language()
        clock = time.strftime("%H:%M:%S", self.converter(record.created))
        tag = _TAGS.get(record.levelno)
        if tag is None and record.levelno >= logging.ERROR:
            tag = _TAGS[logging.ERROR]
        prefix = translate_to(language, tag) if tag else ""
        # Lines from outside the package carry their origin: a bare warning
        # from urllib3 is otherwise unreadable.
        if not record.name.startswith("cnequity"):
            prefix += f"[{record.name}] "
        text = f"{clock} {prefix}{render_message(record, language)}"
        if record.exc_info:
            record.exc_text = record.exc_text or self.formatException(record.exc_info)
        if record.exc_text:
            text = f"{text}\n{record.exc_text}"
        if record.stack_info:
            text = f"{text}\n{self.formatStack(record.stack_info)}"
        return redact_log_text(text, self.secrets)
