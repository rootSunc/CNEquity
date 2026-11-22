#!/usr/bin/env bash
# Pre-push gate: unit tests on this checkout's interpreter, and on Python 3.11
# when that is not already the interpreter under test.
#
# CI runs the suite on 3.11 and 3.14. A clone usually has one interpreter, so
# the supported floor is the run most likely to fail only after the push. The
# floor run uses uv and a cached `.venv-py311`; without uv it is skipped.
# Bypass once with `git push --no-verify`.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1
logs="$(mktemp -d)"
trap 'rm -rf "$logs"' EXIT

py="${CNE_PYTHON:-.venv/bin/python}"
if [ ! -x "$py" ]; then
  py="$(command -v python3)"
fi

floor="3.11"
local_mm="$("$py" -c 'import sys; print("%d.%d" % sys.version_info[:2])')" || exit 1

# Both runs use pytest-xdist; when the floor run happens too, each gets half the cores.
cores="$("$py" -c 'import os; print(os.cpu_count() or 2)')" || cores=2
run_floor=""
if [ "$local_mm" != "$floor" ] && command -v uv >/dev/null 2>&1; then
  run_floor=1
elif [ "$local_mm" != "$floor" ]; then
  echo "pre-push: uv not found, skipping the Python ${floor} run" >&2
fi
workers="auto"
if [ -n "$run_floor" ]; then
  workers=$(( cores / 2 > 1 ? cores / 2 : 1 ))
fi

"$py" -m pytest tests/unit -q -x -n "$workers" -p no:cacheprovider >"$logs/main.txt" 2>&1 &
main_pid=$!

floor_pid=""
if [ -n "$run_floor" ]; then
  UV_PROJECT_ENVIRONMENT=.venv-py311 uv run --frozen --quiet --python "$floor" --group dev \
    python -m pytest tests/unit -q -x -n "$workers" -p no:cacheprovider >"$logs/floor.txt" 2>&1 &
  floor_pid=$!
fi

status=0
if ! wait "$main_pid"; then
  echo "pre-push: unit tests failed on $("$py" -V 2>&1)" >&2
  tail -n 40 "$logs/main.txt" >&2
  status=1
fi
if [ -n "$floor_pid" ] && ! wait "$floor_pid"; then
  echo "pre-push: unit tests failed on Python ${floor}" >&2
  tail -n 40 "$logs/floor.txt" >&2
  status=1
fi
exit "$status"
