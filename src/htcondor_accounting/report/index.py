from __future__ import annotations

import calendar
import csv
import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from htcondor_accounting.render.html import format_hours, render_template
from htcondor_accounting.store.layout import ensure_parent_dir


@dataclass(frozen=True)
class DiscoveredReport:
    period_type: str
    label: str
    sort_date: date
    directory: Path
    index_path: Path
    summary_path: Path
    users_csv_path: Path
    vos_csv_path: Path
    accounting_groups_csv_path: Path
    summary: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReportDiscovery:
    reports_root: Path
    daily_reports: list[DiscoveredReport]
    monthly_reports: list[DiscoveredReport]

    @property
    def all_reports(self) -> list[DiscoveredReport]:
        return [*self.daily_reports, *self.monthly_reports]


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _discover_daily_reports(reports_root: Path) -> list[DiscoveredReport]:
    daily_root = reports_root / "daily"
    reports: list[DiscoveredReport] = []
    if not daily_root.exists():
        return reports

    for index_path in sorted(daily_root.glob("*/*/*/index.html")):
        directory = index_path.parent
        try:
            year = int(directory.parent.parent.name)
            month = int(directory.parent.name)
            day = int(directory.name)
            report_date = date(year, month, day)
        except ValueError:
            continue
        summary_path = directory / "summary.json"
        reports.append(
            DiscoveredReport(
                period_type="daily",
                label=report_date.isoformat(),
                sort_date=report_date,
                directory=directory,
                index_path=index_path,
                summary_path=summary_path,
                users_csv_path=directory / "users.csv",
                vos_csv_path=directory / "vos.csv",
                accounting_groups_csv_path=directory / "accounting_groups.csv",
                summary=_load_json(summary_path),
            )
        )
    return sorted(reports, key=lambda report: report.sort_date)


def _discover_monthly_reports(reports_root: Path) -> list[DiscoveredReport]:
    monthly_root = reports_root / "monthly"
    reports: list[DiscoveredReport] = []
    if not monthly_root.exists():
        return reports

    for index_path in sorted(monthly_root.glob("*/*/index.html")):
        directory = index_path.parent
        try:
            year = int(directory.parent.name)
            month = int(directory.name)
            report_date = date(year, month, 1)
        except ValueError:
            continue
        summary_path = directory / "summary.json"
        reports.append(
            DiscoveredReport(
                period_type="monthly",
                label=f"{year:04d}-{month:02d}",
                sort_date=report_date,
                directory=directory,
                index_path=index_path,
                summary_path=summary_path,
                users_csv_path=directory / "users.csv",
                vos_csv_path=directory / "vos.csv",
                accounting_groups_csv_path=directory / "accounting_groups.csv",
                summary=_load_json(summary_path),
            )
        )
    return sorted(reports, key=lambda report: report.sort_date)


def discover_reports(reports_root: Path) -> ReportDiscovery:
    return ReportDiscovery(
        reports_root=reports_root,
        daily_reports=_discover_daily_reports(reports_root),
        monthly_reports=_discover_monthly_reports(reports_root),
    )


def _csv_values(path: Path, column: str) -> set[str]:
    if not path.exists():
        return set()
    values: set[str] = set()
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                value = (row.get(column) or "").strip()
                if value:
                    values.add(value)
    except OSError:
        return set()
    return values


def _resolved_values(values: set[str]) -> list[str]:
    resolved = {value for value in values if value != "-"}
    return sorted(resolved or values)


def _reports_for_totals(discovery: ReportDiscovery) -> list[DiscoveredReport]:
    return discovery.daily_reports or discovery.monthly_reports


def _root_summary(discovery: ReportDiscovery) -> dict[str, Any]:
    dated_reports = _reports_for_totals(discovery)
    earliest = min((report.sort_date for report in dated_reports), default=None)
    latest = max((report.sort_date for report in dated_reports), default=None)
    total_reports = dated_reports

    jobs = sum(int(report.summary.get("jobs_total") or 0) for report in total_reports)
    wall_seconds = sum(float(report.summary.get("wall_seconds") or 0) for report in total_reports)
    cpu_seconds = sum(float(report.summary.get("cpu_total_seconds") or 0) for report in total_reports)
    users = set()
    groups = set()
    vos = set()
    for report in discovery.all_reports:
        users.update(_csv_values(report.users_csv_path, "user"))
        groups.update(_csv_values(report.accounting_groups_csv_path, "accounting_group"))
        vos.update(_csv_values(report.vos_csv_path, "vo"))

    return {
        "earliest": earliest.isoformat() if earliest else "-",
        "latest": latest.isoformat() if latest else "-",
        "jobs": jobs,
        "wall_seconds": wall_seconds,
        "cpu_seconds": cpu_seconds,
        "users": len(_resolved_values(users)),
        "accounting_groups": _resolved_values(groups),
        "vos": _resolved_values(vos),
    }


def _daily_month_context(discovery: ReportDiscovery) -> list[dict[str, Any]]:
    by_month: dict[tuple[int, int], set[int]] = {}
    for report in discovery.daily_reports:
        key = (report.sort_date.year, report.sort_date.month)
        by_month.setdefault(key, set()).add(report.sort_date.day)

    months: list[dict[str, Any]] = []
    for year, month in sorted(by_month, reverse=True):
        weeks = []
        for week in calendar.Calendar(firstweekday=0).monthdayscalendar(year, month):
            week_cells = []
            for day_number in week:
                if day_number == 0:
                    week_cells.append({"day": None, "href": None})
                elif day_number in by_month[(year, month)]:
                    week_cells.append(
                        {
                            "day": day_number,
                            "href": f"{year:04d}/{month:02d}/{day_number:02d}/index.html",
                        }
                    )
                else:
                    week_cells.append({"day": day_number, "href": None})
            weeks.append(week_cells)
        months.append({"label": f"{year:04d}-{month:02d}", "weeks": weeks})
    return months


def _monthly_year_context(discovery: ReportDiscovery) -> list[dict[str, Any]]:
    by_year: dict[int, list[DiscoveredReport]] = {}
    for report in discovery.monthly_reports:
        by_year.setdefault(report.sort_date.year, []).append(report)

    years: list[dict[str, Any]] = []
    for year in sorted(by_year, reverse=True):
        months = [
            {"label": report.label, "href": f"{report.sort_date.year:04d}/{report.sort_date.month:02d}/index.html"}
            for report in sorted(by_year[year], key=lambda item: item.sort_date, reverse=True)
        ]
        years.append({"year": str(year), "months": months})
    return years


def _write_html(path: Path, template_name: str, context: dict[str, Any]) -> None:
    ensure_parent_dir(path)
    path.write_text(render_template(template_name, context), encoding="utf-8")


def generate_report_indexes(reports_root: Path) -> ReportDiscovery:
    reports_root.mkdir(parents=True, exist_ok=True)
    discovery = discover_reports(reports_root)
    summary = _root_summary(discovery)
    latest_daily = discovery.daily_reports[-1] if discovery.daily_reports else None
    latest_monthly = discovery.monthly_reports[-1] if discovery.monthly_reports else None

    _write_html(
        reports_root / "index.html",
        "reports/index.html",
        {
            "title": "HTCondor Accounting Reports",
            "summary_items": [
                {"label": "Earliest report", "value": summary["earliest"]},
                {"label": "Latest report", "value": summary["latest"]},
                {"label": "Total jobs", "value": str(summary["jobs"])},
                {"label": "Total wall hours", "value": format_hours(summary["wall_seconds"])},
                {"label": "Total CPU hours", "value": format_hours(summary["cpu_seconds"])},
                {"label": "Total users", "value": str(summary["users"])},
            ],
            "accounting_groups": summary["accounting_groups"],
            "vos": summary["vos"],
            "latest_daily": (
                {"label": latest_daily.label, "href": f"daily/{latest_daily.sort_date:%Y/%m/%d}/index.html"}
                if latest_daily
                else None
            ),
            "latest_monthly": (
                {"label": latest_monthly.label, "href": f"monthly/{latest_monthly.sort_date:%Y/%m}/index.html"}
                if latest_monthly
                else None
            ),
        },
    )
    _write_html(
        reports_root / "daily" / "index.html",
        "reports/daily_index.html",
        {
            "title": "Daily Reports",
            "weekdays": ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"],
            "months": _daily_month_context(discovery),
        },
    )
    _write_html(
        reports_root / "monthly" / "index.html",
        "reports/monthly_index.html",
        {"title": "Monthly Reports", "years": _monthly_year_context(discovery)},
    )
    _write_html(
        reports_root / "yearly" / "index.html",
        "reports/yearly_index.html",
        {"title": "Yearly Reports"},
    )
    return discovery
