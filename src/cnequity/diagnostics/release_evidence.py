"""Validation for production evidence attached to a release candidate.

Release evidence is copied from a production lake, so this module deliberately
does not trust the two top-level ``passed`` flags.  It validates the native
report shape and recomputes the facts that make those flags meaningful.
"""

from __future__ import annotations

import datetime as dt
import json
import math
from pathlib import Path
from typing import Any

_STABILITY_FIELDS = frozenset(
    {
        "generated_at",
        "job_name",
        "required_days",
        "calendar_days_available",
        "consecutive_passed",
        "passed",
        "days",
    }
)
_STABILITY_DAY_FIELDS = frozenset(
    {"trade_date", "run_id", "status", "dataset_results", "passed", "reason"}
)
_SOURCE_SLO_FIELDS = frozenset(
    {"generated_at", "window_days", "minimum_observations", "passed", "results", "incidents"}
)
_PROBE_SLO_FIELDS = frozenset(
    {
        "key",
        "vantage",
        "critical",
        "observations",
        "successes",
        "availability",
        "target",
        "latest_status",
        "latest_generated_at",
        "fresh",
        "passed",
    }
)
_INCIDENT_FIELDS = frozenset({"format", "version", "threshold", "open_incidents", "open_count"})
_RUN_STATUSES = frozenset(
    {"missing", "success", "warning", "failed", "skipped", "blocked", "degraded"}
)
_SOURCE_SLO_MAX_AGE = dt.timedelta(days=2)
_VANTAGE_SENTINELS = frozenset(
    {
        "",
        "?",
        "-",
        "--",
        "na",
        "n/a",
        "n.a.",
        "nil",
        "none",
        "null",
        "sentinel",
        "unknown",
        "unknown-vantage",
        "unknown_vantage",
        "unavailable",
        "undefined",
        "unset",
        "unspecified",
    }
)


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain one JSON object")
    return value


def _fresh_generated_at(payload: dict[str, Any], path: Path, now: dt.datetime) -> None:
    raw = payload.get("generated_at")
    if not isinstance(raw, str):
        raise ValueError(f"{path}: generated_at is required")
    try:
        generated = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{path}: invalid generated_at {raw!r}") from exc
    if generated.tzinfo is None:
        raise ValueError(f"{path}: generated_at must include a timezone")
    age = now.astimezone(dt.timezone.utc) - generated.astimezone(dt.timezone.utc)
    if age < dt.timedelta(minutes=-5):
        raise ValueError(f"{path}: generated_at is in the future")
    if age > dt.timedelta(days=7):
        raise ValueError(f"{path}: evidence is older than 7 days")


def _is_int(value: object) -> bool:
    """JSON booleans are ints in Python, but never valid count fields here."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _require_fields(
    payload: dict[str, Any], required: frozenset[str], path: Path, errors: list[str]
) -> None:
    missing = sorted(required - payload.keys())
    if missing:
        errors.append(f"{path}: missing native fields: {', '.join(missing)}")


def _parse_date(raw: object) -> dt.date | None:
    if not isinstance(raw, str):
        return None
    try:
        return dt.date.fromisoformat(raw)
    except ValueError:
        return None


def _parse_timestamp(raw: object) -> dt.datetime | None:
    if not isinstance(raw, str):
        return None
    try:
        value = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return value if value.tzinfo is not None else None


def _valid_vantage(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value.strip())
        and value.strip().casefold() not in _VANTAGE_SENTINELS
    )


def _calendar_trading_dates(start: dt.date, end: dt.date) -> list[dt.date] | None:
    """Return the bundled exchange calendar where it can classify the range.

    A release validator may run against evidence newer than the bundled
    holiday seed.  In that case the seed cannot prove holiday continuity, so
    return ``None`` and let the weekday check below handle the range without
    manufacturing a future holiday schedule.
    """
    try:
        from cnequity.adapters.calendar.exchange_calendar import build_trading_calendar

        calendar = build_trading_calendar(start, end)
    except Exception:  # noqa: BLE001 - an unavailable/stale seed is not evidence
        return None
    return [
        value
        for value, is_trading in zip(
            calendar["trade_date"].to_list(), calendar["is_trading"].to_list(), strict=True
        )
        if is_trading
    ]


def _validate_stability_at(
    payload: dict[str, Any], path: Path, errors: list[str], now: dt.datetime
) -> None:
    _require_fields(payload, _STABILITY_FIELDS, path, errors)
    try:
        _fresh_generated_at(payload, path, now)
    except ValueError as exc:
        errors.append(str(exc))
    generated = _parse_timestamp(payload.get("generated_at"))
    generated_date = generated.astimezone(dt.timezone.utc).date() if generated is not None else None
    now_date = now.astimezone(dt.timezone.utc).date()

    if payload.get("job_name") != "daily:core":
        errors.append(f"{path}: job_name must be 'daily:core'")

    required = payload.get("required_days")
    if not _is_int(required) or required < 20:
        errors.append(f"{path}: required_days must be at least 20")

    calendar_available = payload.get("calendar_days_available")
    if not _is_int(calendar_available) or calendar_available < 0:
        errors.append(f"{path}: calendar_days_available must be a non-negative integer")

    consecutive = payload.get("consecutive_passed")
    if not _is_int(consecutive) or consecutive < 0:
        errors.append(f"{path}: consecutive_passed must be a non-negative integer")

    if payload.get("passed") is not True:
        errors.append(f"{path}: passed must be true")

    days = payload.get("days")
    if not isinstance(days, list):
        errors.append(f"{path}: days must be a list of native stability-day records")
        return
    if _is_int(required) and len(days) < required:
        errors.append(f"{path}: days must contain at least required_days records")
    if _is_int(calendar_available) and calendar_available < len(days):
        errors.append(f"{path}: calendar_days_available must cover every days record")

    valid_rows: list[dict[str, Any]] = []
    dates: list[dt.date] = []
    for index, row in enumerate(days):
        row_path = f"{path}: days[{index}]"
        if not isinstance(row, dict):
            errors.append(f"{row_path} must be an object")
            continue
        _require_fields(row, _STABILITY_DAY_FIELDS, Path(row_path), errors)
        trade_date = _parse_date(row.get("trade_date"))
        if trade_date is None:
            errors.append(f"{row_path}: trade_date must be YYYY-MM-DD")
        else:
            dates.append(trade_date)
            if trade_date > now_date:
                errors.append(f"{row_path}: trade_date must not be later than now")
            if generated_date is not None and trade_date > generated_date:
                errors.append(f"{row_path}: trade_date must not be later than generated_at")

        run_id = row.get("run_id")
        status = row.get("status")
        dataset_results = row.get("dataset_results")
        passed = row.get("passed")
        reason = row.get("reason")
        if status not in _RUN_STATUSES:
            errors.append(f"{row_path}: status is not a native run status")
        if not _is_int(dataset_results) or dataset_results < 0:
            errors.append(f"{row_path}: dataset_results must be a non-negative integer")
        if passed is not True and passed is not False:
            errors.append(f"{row_path}: passed must be boolean")
        if not isinstance(reason, str) or not reason.strip():
            errors.append(f"{row_path}: reason must be a non-empty string")
        if status == "missing":
            if run_id is not None:
                errors.append(f"{row_path}: missing day must not have run_id")
            if dataset_results != 0:
                errors.append(f"{row_path}: missing day must have dataset_results=0")
            if passed is not False:
                errors.append(f"{row_path}: missing day must be failed")
        elif not isinstance(run_id, str) or not run_id.strip():
            errors.append(f"{row_path}: completed day requires a non-empty run_id")
        if passed is True:
            if status not in {"success", "degraded", "warning"}:
                errors.append(f"{row_path}: a passed day has an invalid run status")
            if status in {"degraded", "warning"} and (
                not _is_int(dataset_results) or dataset_results < 1
            ):
                errors.append(f"{row_path}: degraded/warning day needs dataset receipts")
        if trade_date is not None:
            valid_rows.append(row)

    if dates:
        if dates != sorted(dates):
            errors.append(f"{path}: days must be ordered by ascending trade_date")
        if len(set(dates)) != len(dates):
            errors.append(f"{path}: days must not contain duplicate trade_date values")

        expected = _calendar_trading_dates(min(dates), max(dates))
        if expected is None:
            expected = [
                min(dates) + dt.timedelta(days=offset)
                for offset in range((max(dates) - min(dates)).days + 1)
                if (min(dates) + dt.timedelta(days=offset)).weekday() < 5
            ]
        if dates != expected:
            errors.append(
                f"{path}: days must cover one continuous trading-day sequence "
                f"({min(dates).isoformat()}..{max(dates).isoformat()})"
            )
        if generated_date is not None:
            tail_age = (generated_date - max(dates)).days
            if tail_age > 7:
                errors.append(
                    f"{path}: latest trade_date {max(dates).isoformat()} is more than 7 days "
                    "before generated_at"
                )

    if valid_rows and len(valid_rows) == len(days):
        recomputed = 0
        for row in reversed(valid_rows):
            if row.get("passed") is not True:
                break
            recomputed += 1
        if _is_int(consecutive) and consecutive != recomputed:
            errors.append(
                f"{path}: consecutive_passed={consecutive} does not match tail count {recomputed}"
            )
        if _is_int(required) and payload.get("passed") is True and recomputed < required:
            errors.append(f"{path}: passed requires at least required_days trailing passes")
        if all(row.get("passed") is True for row in valid_rows):
            expected_passed = _is_int(required) and len(valid_rows) >= required
            if payload.get("passed") is not expected_passed:
                errors.append(f"{path}: passed does not match the supplied day evidence")


def _validate_slo_number(
    value: object, *, label: str, path: str, errors: list[str], minimum: float = 0.0
) -> bool:
    if not _is_number(value) or not math.isfinite(float(value)) or float(value) < minimum:
        errors.append(f"{path}: {label} must be a finite number >= {minimum:g}")
        return False
    return True


def _validate_incidents(incidents: object, path: Path, errors: list[str]) -> None:
    if not isinstance(incidents, dict):
        errors.append(f"{path}: incidents must be the native incident object")
        return
    _require_fields(incidents, _INCIDENT_FIELDS, path, errors)
    if incidents.get("format") != "cnequity.source-incidents":
        errors.append(f"{path}: incidents.format is invalid")
    version = incidents.get("version")
    if not _is_int(version) or version < 1:
        errors.append(f"{path}: incidents.version must be a positive integer")
    threshold = incidents.get("threshold")
    if not _is_int(threshold) or threshold < 1:
        errors.append(f"{path}: incidents.threshold must be a positive integer")
    open_incidents = incidents.get("open_incidents")
    if not isinstance(open_incidents, list):
        errors.append(f"{path}: incidents.open_incidents must be a list")
    elif open_incidents:
        errors.append(f"{path}: open source incidents must be zero")
    open_count = incidents.get("open_count")
    if not _is_int(open_count) or open_count < 0:
        errors.append(f"{path}: incidents.open_count must be a non-negative integer")
    elif open_count != 0:
        errors.append(f"{path}: open source incidents must be zero")
    if (
        isinstance(open_incidents, list)
        and _is_int(open_count)
        and open_count != len(open_incidents)
    ):
        errors.append(f"{path}: incidents.open_count must equal len(open_incidents)")


def _validate_slo_at(
    payload: dict[str, Any], path: Path, errors: list[str], now: dt.datetime
) -> None:
    _require_fields(payload, _SOURCE_SLO_FIELDS, path, errors)
    try:
        _fresh_generated_at(payload, path, now)
    except ValueError as exc:
        errors.append(str(exc))

    window_days = payload.get("window_days")
    if not _is_int(window_days) or window_days != 30:
        errors.append(f"{path}: window_days must equal 30")
    minimum = payload.get("minimum_observations")
    if not _is_int(minimum) or minimum < 10:
        errors.append(f"{path}: minimum_observations must be at least 10")
    if payload.get("passed") is not True:
        errors.append(f"{path}: passed must be true")

    results = payload.get("results")
    if not isinstance(results, list) or not results:
        errors.append(f"{path}: results must be a non-empty list of native probe SLO records")
        _validate_incidents(payload.get("incidents"), path, errors)
        return

    from cnequity.diagnostics import source_health
    from cnequity.diagnostics.source_slo import CORE_TARGET, critical_probe_keys

    registry = {probe.key: probe for probe in source_health.PROBES}
    critical_keys = critical_probe_keys()
    if not critical_keys:
        errors.append(f"{path}: source-health registry has no critical probes")
    seen_keys: set[str] = set()
    keys_by_vantage: dict[str, set[str]] = {}
    seen_rows: set[tuple[str, str]] = set()
    critical_passed: list[bool] = []
    for index, row in enumerate(results):
        row_path = f"{path}: results[{index}]"
        if not isinstance(row, dict):
            errors.append(f"{row_path} must be an object")
            continue
        _require_fields(row, _PROBE_SLO_FIELDS, Path(row_path), errors)
        key = row.get("key")
        vantage = row.get("vantage")
        critical = row.get("critical")
        observations = row.get("observations")
        successes = row.get("successes")
        availability = row.get("availability")
        target = row.get("target")
        latest_status = row.get("latest_status")
        latest_generated_at = row.get("latest_generated_at")
        fresh = row.get("fresh")
        passed = row.get("passed")

        if not isinstance(key, str) or not key.strip():
            errors.append(f"{row_path}: key must be a non-empty string")
        if not isinstance(vantage, str) or not vantage.strip():
            errors.append(f"{row_path}: vantage must be a non-empty string")
        elif not _valid_vantage(vantage):
            errors.append(f"{row_path}: vantage must identify a real probe origin")
        if isinstance(key, str) and key not in registry:
            errors.append(f"{row_path}: key {key!r} is not in the source-health probe registry")
        if isinstance(key, str):
            seen_keys.add(key)
        if isinstance(key, str) and isinstance(vantage, str):
            keys_by_vantage.setdefault(vantage, set()).add(key)
            identity = (key, vantage)
            if identity in seen_rows:
                errors.append(f"{row_path}: duplicate key/vantage result")
            seen_rows.add(identity)
        expected_critical = isinstance(key, str) and key in critical_keys
        if critical is not expected_critical:
            errors.append(f"{row_path}: critical does not match the source-health registry")
        if critical is True:
            critical_passed.append(passed is True)

        if not _is_int(observations) or observations < 0:
            errors.append(f"{row_path}: observations must be a non-negative integer")
        if not _is_int(successes) or successes < 0:
            errors.append(f"{row_path}: successes must be a non-negative integer")
        if _is_int(window_days) and _is_int(observations) and observations > window_days + 1:
            errors.append(f"{row_path}: observations cannot exceed window_days + 1")
        if _is_int(observations) and _is_int(successes) and successes > observations:
            errors.append(f"{row_path}: successes cannot exceed observations")
        target_ok = _validate_slo_number(target, label="target", path=row_path, errors=errors)
        if target_ok and float(target) > 1:
            errors.append(f"{row_path}: target must be <= 1")
        if (
            critical is True
            and target_ok
            and not math.isclose(float(target), CORE_TARGET, rel_tol=0.0, abs_tol=1e-12)
        ):
            errors.append(
                f"{row_path}: critical target must equal native core target {CORE_TARGET:g}"
            )
        availability_ok = availability is None or _validate_slo_number(
            availability, label="availability", path=row_path, errors=errors
        )
        if availability is not None and availability_ok and float(availability) > 1:
            errors.append(f"{row_path}: availability must be <= 1")

        if _is_int(observations) and _is_int(successes):
            if observations == 0:
                if availability is not None:
                    errors.append(f"{row_path}: zero observations must have availability=null")
            else:
                expected_availability = successes / observations
                if not availability_ok or availability is None:
                    pass
                elif not math.isclose(
                    float(availability), expected_availability, rel_tol=1e-9, abs_tol=1e-12
                ):
                    errors.append(f"{row_path}: availability does not match successes/observations")

        allowed_statuses = {"ok", "empty", "blocked", "down", "skipped"}
        if latest_status is not None and latest_status not in allowed_statuses:
            errors.append(f"{row_path}: latest_status is not a native probe status")
        latest_time = _parse_timestamp(latest_generated_at)
        if _is_int(observations) and observations > 0:
            if latest_time is None:
                errors.append(
                    f"{row_path}: observed result requires timezone-aware latest_generated_at"
                )
            if latest_status == "skipped":
                errors.append(f"{row_path}: skipped cannot be an observed SLO sample")
        elif observations == 0:
            if latest_status is not None or latest_generated_at is not None:
                errors.append(
                    f"{row_path}: missing result must have null latest observation fields"
                )
        if fresh is not True and fresh is not False:
            errors.append(f"{row_path}: fresh must be boolean")
        if passed is not True and passed is not False:
            errors.append(f"{row_path}: passed must be boolean")

        expected_fresh: bool | None = None
        if latest_time is not None:
            age = now.astimezone(dt.timezone.utc) - latest_time.astimezone(dt.timezone.utc)
            if age < dt.timedelta(minutes=-5):
                errors.append(f"{row_path}: latest_generated_at is too far in the future")
            expected_fresh = age <= _SOURCE_SLO_MAX_AGE
        if (fresh is True or fresh is False) and expected_fresh is not None:
            if fresh is not expected_fresh:
                errors.append(f"{row_path}: fresh does not match latest_generated_at")
        elif observations == 0 and fresh is True:
            errors.append(f"{row_path}: a result with no observations cannot be fresh")

        expected_passed = (
            _is_int(observations)
            and observations >= (minimum if _is_int(minimum) else 10**18)
            and availability is not None
            and _is_number(availability)
            and _is_number(target)
            and float(availability) >= float(target)
            and fresh is True
        )
        if (passed is True or passed is False) and passed is not expected_passed:
            errors.append(f"{row_path}: passed does not match the native SLO calculation")

        if critical is True:
            if not _is_int(minimum) or not _is_int(observations) or observations < minimum:
                errors.append(f"{row_path}: critical observations must meet minimum_observations")
            if fresh is not True:
                errors.append(f"{row_path}: every critical result must be fresh")
            if passed is not True:
                errors.append(f"{row_path}: critical source must pass")
            if availability is None or not _is_number(availability) or not _is_number(target):
                errors.append(f"{row_path}: critical result needs availability and target")
            elif float(availability) < float(target):
                errors.append(f"{row_path}: critical availability must meet target")

    missing = sorted(critical_keys - seen_keys)
    if missing:
        errors.append(f"{path}: results missing critical probes: {', '.join(missing)}")
    for vantage, keys in sorted(keys_by_vantage.items()):
        missing_for_vantage = sorted(critical_keys - keys)
        if missing_for_vantage:
            errors.append(
                f"{path}: results for vantage {vantage!r} missing critical probes: "
                f"{', '.join(missing_for_vantage)}"
            )
    expected_report_passed = bool(critical_passed) and all(critical_passed)
    if payload.get("passed") is not expected_report_passed:
        errors.append(f"{path}: passed does not match all critical probe results")
    _validate_incidents(payload.get("incidents"), path, errors)


def validate_release_evidence(root: Path, *, now: dt.datetime | None = None) -> list[str]:
    """Return every release-evidence violation found below ``root``."""

    current = now or dt.datetime.now(dt.timezone.utc)
    errors: list[str] = []
    stability_path = root / "stability-20d.json"
    slo_path = root / "source-slo-30d.json"
    try:
        stability = _read_object(stability_path)
        _validate_stability_at(stability, stability_path, errors, current)
    except ValueError as exc:
        errors.append(str(exc))

    try:
        slo = _read_object(slo_path)
        _validate_slo_at(slo, slo_path, errors, current)
    except ValueError as exc:
        errors.append(str(exc))
    return errors
