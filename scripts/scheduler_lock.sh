#!/usr/bin/env bash
# Small launchd/cron-compatible lock for the shell-level scheduled jobs.
#
# macOS does not ship `flock`, and the Python run lock is scoped to one `cne`
# invocation.  The daily pipeline invokes several commands, so it needs one
# lock around the whole script; otherwise the late stale pass could start in
# the gap between two daily groups.  `mkdir` is atomic on the local filesystems
# supported by macOS and gives us a lock that works with the system Bash 3.2.
#
# Source this file from a script and call:
#
#   scheduler_lock_acquire "$REPO_ROOT" daily || ...
#   scheduler_lock_install_traps
#   ...
#   scheduler_lock_release       # optional; EXIT trap also releases it
#
# CNE_SCHEDULER_LOCK_DIR (or the older CNE_LOCK_DIR alias) can point at a
# writable directory for tests or for a repo whose data directory is elsewhere.

scheduler_lock_acquire() {
  local repo_root="$1"
  local lock_name="${2:-daily}"
  local lock_root lock_dir lock_pid owner attempt

  lock_root="${CNE_SCHEDULER_LOCK_DIR:-${CNE_LOCK_DIR:-$repo_root/data/cnequity/locks}}"
  lock_dir="$lock_root/$lock_name.lock"
  lock_pid="$lock_dir/pid"

  SCHEDULER_LOCK_DIR="$lock_dir"
  SCHEDULER_LOCK_PID_FILE="$lock_pid"
  SCHEDULER_LOCK_HELD=0

  if ! mkdir -p "$lock_root" 2>/dev/null; then
    return 2
  fi

  # A second attempt is enough when the previous owner exited between the
  # `mkdir` and the first process writing its pid file.
  attempt=0
  while [[ "$attempt" -lt 2 ]]; do
    if mkdir "$lock_dir" 2>/dev/null; then
      if ! printf '%s\n' "$$" >"$lock_pid"; then
        rmdir "$lock_dir" 2>/dev/null || true
        return 2
      fi
      SCHEDULER_LOCK_HELD=1
      return 0
    fi

    # Never remove a lock with no owner marker: it may be an active process
    # that has not reached the marker write yet.  Such a process will release
    # it on EXIT; a hard-killed process leaves an operator-visible lock that can
    # be removed after checking the process (see the message from callers).
    owner=""
    if [[ -f "$lock_pid" ]]; then
      owner="$(cat "$lock_pid" 2>/dev/null || true)"
    fi
    if [[ "$owner" =~ ^[0-9]+$ ]] && kill -0 "$owner" 2>/dev/null; then
      return 1
    fi
    if [[ -n "$owner" ]]; then
      # The recorded process is gone.  Remove only the marker and empty lock
      # directory; never recurse into a user directory.
      rm -f "$lock_pid" 2>/dev/null || return 2
      rmdir "$lock_dir" 2>/dev/null || return 1
      attempt=$((attempt + 1))
      continue
    fi
    return 1
  done

  return 1
}

scheduler_lock_install_traps() {
  # Signal handlers exit and let the EXIT trap do the ownership-checked
  # release.  Clearing the signal traps avoids recursively handling a signal
  # received while the shell is unwinding.
  trap 'scheduler_lock_release' EXIT
  trap 'trap - HUP INT TERM; exit 143' HUP INT TERM
}

scheduler_lock_release() {
  local owner=""
  if [[ "${SCHEDULER_LOCK_HELD:-0}" != "1" ]]; then
    return 0
  fi

  if [[ -f "${SCHEDULER_LOCK_PID_FILE:-}" ]]; then
    owner="$(cat "$SCHEDULER_LOCK_PID_FILE" 2>/dev/null || true)"
  fi
  if [[ "$owner" == "$$" ]]; then
    rm -f "$SCHEDULER_LOCK_PID_FILE" 2>/dev/null || true
    rmdir "$SCHEDULER_LOCK_DIR" 2>/dev/null || true
  fi
  SCHEDULER_LOCK_HELD=0
}
