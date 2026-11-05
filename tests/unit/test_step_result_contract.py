"""The keys that make a step's result partial are a listed contract."""

from __future__ import annotations

import logging
import random
import re
from pathlib import Path

import pytest

import cnequity
from cnequity.orchestrator.engine import _has_partial_failures
from cnequity.orchestrator.outcomes import COMPLETION_FLAGS, PARTIAL_FAILURE_KEYS


def _prefix_rule(result: dict) -> bool:
    """The engine's rule before the keys were listed, kept as the oracle."""
    for key, value in result.items():
        if key in {"complete", "coverage_complete"}:
            if value is False:
                return True
            continue
        if not (key.startswith("failed") or key in {"aborted", "empty_days", "days_empty"}):
            continue
        if isinstance(value, bool):
            if value:
                return True
        elif isinstance(value, (int, float)):
            if value > 0:
                return True
        elif value:
            return True
    return False


_KEYS = sorted(PARTIAL_FAILURE_KEYS | COMPLETION_FLAGS) + [
    "rows_written",
    "status",
    "failed_unlisted_key",
    "completed",
    "metrics",
]
_VALUES = [None, True, False, 0, 1, 0.0, 2.5, -1, "", "x", [], ["a"], {}, {"a": 1}]


def test_listed_keys_decide_exactly_as_the_prefix_rule_did():
    rng = random.Random(20261104)
    for _ in range(5000):
        result = {key: rng.choice(_VALUES) for key in rng.sample(_KEYS, rng.randint(0, 6))}
        assert _has_partial_failures(result) == _prefix_rule(result), result


def test_an_unlisted_failed_key_is_still_partial_and_is_logged(caplog):
    with caplog.at_level(logging.WARNING, logger="cnequity.orchestrator.engine"):
        assert _has_partial_failures({"failed_widgets": 3}) is True
    assert "failed_widgets" in caplog.text


@pytest.mark.parametrize("flag", sorted(COMPLETION_FLAGS))
def test_only_an_explicit_false_completion_flag_counts(flag):
    assert _has_partial_failures({flag: False}) is True
    assert _has_partial_failures({flag: None}) is False
    assert _has_partial_failures({flag: True}) is False


# Literal ``"failed…":`` dict keys and ``["failed…"] =`` assignments.
_KEY_LITERAL = re.compile(r"""["'](failed[a-z_]*)["']\s*:|\[["'](failed[a-z_]*)["']\]\s*=""")
# ``failed_at`` stamps an adapter's own retry record; it never reaches a step result.
_NOT_RESULT_KEYS = {"failed_at"}


def test_every_failed_key_in_the_code_is_listed():
    package = Path(cnequity.__file__).parent
    found: dict[str, str] = {}
    for path in sorted(package.rglob("*.py")):
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            for match in _KEY_LITERAL.finditer(line):
                key = match.group(1) or match.group(2)
                found.setdefault(key, f"{path.relative_to(package)}:{number}")
    unlisted = {
        key: where
        for key, where in found.items()
        if key not in PARTIAL_FAILURE_KEYS and key not in _NOT_RESULT_KEYS
    }
    assert unlisted == {}, f"add to outcomes.PARTIAL_FAILURE_KEYS or _NOT_RESULT_KEYS: {unlisted}"
