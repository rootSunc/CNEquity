#!/usr/bin/env python3
"""Fail closed unless a release carries current, passing production evidence."""

from __future__ import annotations

import argparse
from pathlib import Path

from cnequity.diagnostics.release_evidence import validate_release_evidence


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path, help="directory containing production JSON reports")
    args = parser.parse_args()
    errors = validate_release_evidence(args.root)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print(f"Release evidence OK: {args.root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
