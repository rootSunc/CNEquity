"""The raw archive: every response a source returned, kept as it arrived.

A parser is a pure function of a response body, so keeping the bodies makes
every parse repeatable: a parser fix is replayed over what was fetched,
offline, instead of asking the source again (which may no longer say the same).

Layout: ``raw/<source>/<UTC day fetched>/<sha256>.gz`` holds one body, and
``index.jsonl`` beside it one line per response: what was asked (the request's
key, its URL and parameters, never its headers, which carry credentials), the
status, the size and the digest that names the body file.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from collections.abc import Iterator, Mapping
from datetime import datetime, timezone
from pathlib import Path

INDEX = "index.jsonl"


class RawArchive:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def put(
        self,
        source: str,
        key: str,
        url: str,
        params: Mapping[str, str],
        status: int,
        body: bytes,
        *,
        fetched_at: datetime | None = None,
    ) -> Path:
        """Keep *body*; returns the file that holds it."""
        at = fetched_at or datetime.now(timezone.utc)
        directory = self.root / source / f"{at:%Y-%m-%d}"
        directory.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(body).hexdigest()
        target = directory / f"{digest}.gz"
        if not target.exists():
            partial = target.with_suffix(".part")
            with gzip.open(partial, "wb", compresslevel=6) as handle:
                handle.write(body)
            partial.replace(target)
        entry = {
            "key": key,
            "url": url,
            "params": dict(params),
            "status": status,
            "bytes": len(body),
            "sha256": digest,
            "fetched_at": at.isoformat(timespec="seconds"),
        }
        with open(directory / INDEX, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, sort_keys=True) + "\n")
        return target

    def entries(self, source: str) -> Iterator[dict]:
        """Every response kept for *source*, oldest day first, in the order fetched."""
        home = self.root / source
        if not home.is_dir():
            return
        for directory in sorted(path for path in home.iterdir() if path.is_dir()):
            index = directory / INDEX
            if not index.is_file():
                continue
            for line in index.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    yield {**json.loads(line), "day": directory.name}

    def body(self, source: str, entry: Mapping[str, object]) -> bytes:
        """The body an index *entry* refers to."""
        path = self.root / source / str(entry["day"]) / f"{entry['sha256']}.gz"
        with gzip.open(path, "rb") as handle:
            return handle.read()

    def latest(self, source: str, key: str) -> bytes | None:
        """The body of the most recent successful response for request *key*, if kept."""
        found = None
        for entry in self.entries(source):
            if entry["key"] == key and entry["status"] == 200:
                found = entry
        return None if found is None else self.body(source, found)
