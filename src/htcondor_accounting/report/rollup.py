from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from htcondor_accounting.report.daily import write_json
from htcondor_accounting.report.periods import iso_week_parts, month_period, parse_day, week_period, year_period
from htcondor_accounting.store.layout import (
    derived_all_time_summary_path,
    derived_monthly_summary_path,
    derived_weekly_summary_path,
    derived_yearly_summary_path,
)
from htcondor_accounting.version import __version__


ROLLUP_NUMERIC_FIELDS = [
    "input_files",
    "input_records",
    "unique_records",
    "duplicate_records",
    "wall_seconds",
    "cpu_user_seconds",
    "cpu_sys_seconds",
    "cpu_total_seconds",
    "scaled_wall_seconds",
    "scaled_cpu_seconds",
]


@dataclass(frozen=True)
class DailySummaryRecord:
    day: str
    path: Path
    payload: dict[str, Any]


@dataclass(frozen=True)
class RollupResult:
    period_type: str
    period: str
    days_included: int
    output_path: Path
    summary: dict[str, Any]


def enumerate_daily_summary_files(root: Path) -> list[Path]:
    return sorted((root / "derived" / "daily").rglob("summary.json"))


def load_daily_summary(path: Path) -> DailySummaryRecord:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return DailySummaryRecord(
        day=str(payload["day"]),
        path=path,
        payload=payload,
    )


def load_all_daily_summaries(root: Path) -> list[DailySummaryRecord]:
    return [load_daily_summary(path) for path in enumerate_daily_summary_files(root)]


def aggregate_rollup_summary(
    period_type: str,
    period: str,
    records: Iterable[DailySummaryRecord],
) -> dict[str, Any]:
    items = sorted(records, key=lambda record: (record.day, str(record.path)))
    source_paths = [str(record.path) for record in items]
    days = [record.day for record in items]

    summary: dict[str, Any] = {
        "schema_version": 1,
        "record_type": "rollup_summary",
        "period_type": period_type,
        "period": period,
        "days_included": len(items),
        "source_daily_summaries": source_paths,
        "user_days_total": sum(int(record.payload.get("users") or 0) for record in items),
        "vo_days_total": sum(int(record.payload.get("vos") or 0) for record in items),
        "counts_note": "user_days_total and vo_days_total are sums of daily counts, not globally distinct counts.",
        "tool_version": __version__,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "first_day": days[0] if days else None,
        "last_day": days[-1] if days else None,
    }

    for field in ROLLUP_NUMERIC_FIELDS:
        summary[field] = sum(record.payload.get(field, 0) for record in items)

    return summary


def group_daily_summaries_weekly(records: Iterable[DailySummaryRecord]) -> dict[tuple[int, int], list[DailySummaryRecord]]:
    groups: dict[tuple[int, int], list[DailySummaryRecord]] = {}
    for record in sorted(records, key=lambda item: (item.day, str(item.path))):
        key = iso_week_parts(parse_day(record.day))
        groups.setdefault(key, []).append(record)
    return groups


def group_daily_summaries_monthly(records: Iterable[DailySummaryRecord]) -> dict[tuple[int, int], list[DailySummaryRecord]]:
    groups: dict[tuple[int, int], list[DailySummaryRecord]] = {}
    for record in sorted(records, key=lambda item: (item.day, str(item.path))):
        day = parse_day(record.day)
        key = (day.year, day.month)
        groups.setdefault(key, []).append(record)
    return groups


def group_daily_summaries_yearly(records: Iterable[DailySummaryRecord]) -> dict[int, list[DailySummaryRecord]]:
    groups: dict[int, list[DailySummaryRecord]] = {}
    for record in sorted(records, key=lambda item: (item.day, str(item.path))):
        key = parse_day(record.day).year
        groups.setdefault(key, []).append(record)
    return groups


def write_weekly_rollup(root: Path, year: int, week: int, records: Iterable[DailySummaryRecord]) -> RollupResult:
    record_list = list(records)
    summary = aggregate_rollup_summary("weekly", f"{year:04d}-W{week:02d}", record_list)
    output_path = derived_weekly_summary_path(root, year, week)
    write_json(output_path, summary)
    return RollupResult("weekly", summary["period"], len(record_list), output_path, summary)


def write_monthly_rollup(root: Path, year: int, month: int, records: Iterable[DailySummaryRecord]) -> RollupResult:
    record_list = list(records)
    summary = aggregate_rollup_summary("monthly", f"{year:04d}-{month:02d}", record_list)
    output_path = derived_monthly_summary_path(root, year, month)
    write_json(output_path, summary)
    return RollupResult("monthly", summary["period"], len(record_list), output_path, summary)


def write_yearly_rollup(root: Path, year: int, records: Iterable[DailySummaryRecord]) -> RollupResult:
    record_list = list(records)
    summary = aggregate_rollup_summary("yearly", f"{year:04d}", record_list)
    output_path = derived_yearly_summary_path(root, year)
    write_json(output_path, summary)
    return RollupResult("yearly", summary["period"], len(record_list), output_path, summary)


def write_all_time_rollup(root: Path, records: Iterable[DailySummaryRecord]) -> RollupResult:
    record_list = list(records)
    summary = aggregate_rollup_summary("all-time", "all-time", record_list)
    output_path = derived_all_time_summary_path(root)
    write_json(output_path, summary)
    return RollupResult("all-time", summary["period"], len(record_list), output_path, summary)


def derive_weekly(root: Path, *, year: int, week: int) -> RollupResult:
    groups = group_daily_summaries_weekly(load_all_daily_summaries(root))
    return write_weekly_rollup(root, year, week, groups.get((year, week), []))


def derive_monthly(root: Path, *, year: int, month: int) -> RollupResult:
    groups = group_daily_summaries_monthly(load_all_daily_summaries(root))
    return write_monthly_rollup(root, year, month, groups.get((year, month), []))


def derive_yearly(root: Path, *, year: int) -> RollupResult:
    groups = group_daily_summaries_yearly(load_all_daily_summaries(root))
    return write_yearly_rollup(root, year, groups.get(year, []))


def derive_all_time(root: Path) -> RollupResult:
    return write_all_time_rollup(root, load_all_daily_summaries(root))


def derive_all_rollups(root: Path) -> list[RollupResult]:
    records = load_all_daily_summaries(root)
    results: list[RollupResult] = []

    for (year, week), items in group_daily_summaries_weekly(records).items():
        results.append(write_weekly_rollup(root, year, week, items))
    for (year, month), items in group_daily_summaries_monthly(records).items():
        results.append(write_monthly_rollup(root, year, month, items))
    for year, items in group_daily_summaries_yearly(records).items():
        results.append(write_yearly_rollup(root, year, items))

    results.append(write_all_time_rollup(root, records))
    return results
