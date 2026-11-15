"""What the core asks a market: the questions only a market can answer.

The core cuts time into cells and compares them with what is stored; which
days trade and which entities exist on a day are the market's to say. These
protocols are all the core knows of either.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Protocol

import polars as pl


class Calendar(Protocol):
    def sessions(self, start: date, end: date) -> Sequence[date]:
        """The market's sessions from *start* to *end*, both included, earliest first."""
        ...


class Universe(Protocol):
    def members(self, dataset: str, start: date, end: date) -> pl.DataFrame:
        """The entities *dataset* is owed for somewhere in *start*..*end*.

        A frame with ``entity`` and the days it is owed from and to, ``first``
        and ``last``; a null ``first`` or ``last`` leaves that side open.
        """
        ...
