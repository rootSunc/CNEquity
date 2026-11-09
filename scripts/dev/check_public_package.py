"""Reject local/private materials from built wheel and source archive."""

from __future__ import annotations

import argparse
import tarfile
import zipfile
from pathlib import PurePosixPath

RETIRED = {
    "china_egress_backfill.sh",
    "probe_ths_official_limits.py",
    "retry_init_finalize.py",
    "run_init_2016.py",
}
LOCAL_CONFIGS = {"cnequity.toml", "cnequity.demo.toml"}
TEMPLATE = "cnequity/config/templates/cnequity.example.toml"
REQUIRED_FILES = (TEMPLATE, "cnequity/domain/seeds/bse_code_mapping.json")


def check_members(names: list[str]) -> list[str]:
    bad = []
    for name in names:
        path = PurePosixPath(name)
        if (
            "private" in path.parts
            or "evidence" in path.parts
            or path.name == "AGENTS.md"
            or path.name.startswith(".env")
            or path.name.endswith(".local.toml")
            or path.name in RETIRED | LOCAL_CONFIGS
        ):
            bad.append(name)
    for required in REQUIRED_FILES:
        if not any(name.endswith(required) for name in names):
            bad.append(f"missing {required}")
    return bad


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("archives", nargs="+")
    args = parser.parse_args()
    for archive in args.archives:
        if archive.endswith(".whl"):
            with zipfile.ZipFile(archive) as handle:
                names = handle.namelist()
        elif archive.endswith(".tar.gz"):
            with tarfile.open(archive) as handle:
                names = handle.getnames()
        else:
            parser.error(f"unsupported archive: {archive}")
        if bad := check_members(names):
            print(f"{archive}: unexpected or missing package members: {bad}")
            return 1
        print(f"{archive}: {len(names)} members checked")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
