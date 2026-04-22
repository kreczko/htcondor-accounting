from __future__ import annotations

from functools import lru_cache
from typing import Any

from jinja2 import Environment, PackageLoader, select_autoescape

from htcondor_accounting.models.reporting import MonthlyReportSummary, UsageGroupRow


def format_hours(seconds: int | float | None) -> str:
    if seconds is None:
        return "-"
    return f"{float(seconds) / 3600.0:.1f}"


def format_gb(kb: int | float | None) -> str:
    if kb is None:
        return "-"
    return f"{float(kb) / 1024.0 / 1024.0:.1f}"


def format_number(value: int | float | None, *, decimals: int = 1) -> str:
    if value is None:
        return "-"
    if isinstance(value, int):
        return str(value)
    return f"{float(value):.{decimals}f}"


def format_scaled_pair(raw_seconds: int | float | None, scaled_seconds: int | float | None) -> str:
    raw = format_hours(raw_seconds)
    scaled = format_hours(scaled_seconds)
    if raw == "-" and scaled == "-":
        return "-"
    if scaled == "-" or raw == scaled:
        return raw
    return f"{raw} ({scaled})"


def _build_rows(rows: list[UsageGroupRow], kind: str) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in rows:
        base = {
            "jobs": str(row.jobs),
            "wall_hours_display": format_scaled_pair(row.wall_seconds, row.scaled_wall_seconds),
            "cpu_hours_display": format_scaled_pair(row.cpu_total_seconds, row.scaled_cpu_seconds),
            "avg_processors_display": format_number(row.avg_processors),
            "max_processors_display": str(row.max_processors),
            "max_memory_gb_display": format_gb(row.memory_real_kb_max),
        }
        if kind == "users":
            items.append(
                {
                    "user": row.group_key,
                    "vo": row.vo or "-",
                    **base,
                }
            )
        elif kind == "vos":
            items.append(
                {
                    "vo": row.group_key,
                    "users": str(row.users or 0),
                    **base,
                }
            )
        else:
            items.append(
                {
                    "accounting_group": row.group_key,
                    "vo": row.vo or "-",
                    "users": str(row.users or 0),
                    **base,
                }
            )
    return items


def build_monthly_report_context(
    summary: MonthlyReportSummary,
    user_rows: list[UsageGroupRow],
    vo_rows: list[UsageGroupRow],
    accounting_group_rows: list[UsageGroupRow],
    *,
    benchmark_type: str,
    benchmark_baseline: float,
) -> dict[str, Any]:
    scaling_note = (
        f"Scaled values are adjusted relative to the configured {benchmark_type} baseline of "
        f"{benchmark_baseline:g}. The machine benchmark of the node each job ran on is used to "
        "normalize usage across nodes with different performance."
    )

    return {
        "title": f"HTCondor Accounting Monthly Report {summary.period}",
        "month_label": summary.period,
        "summary_items": [
            {"label": "Month", "value": summary.period},
            {"label": "Days Included", "value": str(summary.days_included)},
            {"label": "Total Jobs", "value": str(summary.jobs_total)},
            {"label": "Total Wall Hours", "value": format_hours(summary.wall_seconds)},
            {"label": "Total CPU Hours", "value": format_hours(summary.cpu_total_seconds)},
        ],
        "scaling_note": scaling_note,
        "sections": [
            {
                "title": "Users",
                "csv_href": "users.csv",
                "headers": ["User", "VO", "Jobs", "Wall h (scaled)", "CPU h (scaled)", "Avg Proc", "Max Proc", "Max Mem GB"],
                "row_fields": ["user", "vo", "jobs", "wall_hours_display", "cpu_hours_display", "avg_processors_display", "max_processors_display", "max_memory_gb_display"],
                "rows": _build_rows(user_rows, "users"),
            },
            {
                "title": "VOs",
                "csv_href": "vos.csv",
                "headers": ["VO", "Users", "Jobs", "Wall h (scaled)", "CPU h (scaled)", "Avg Proc", "Max Proc", "Max Mem GB"],
                "row_fields": ["vo", "users", "jobs", "wall_hours_display", "cpu_hours_display", "avg_processors_display", "max_processors_display", "max_memory_gb_display"],
                "rows": _build_rows(vo_rows, "vos"),
            },
            {
                "title": "Accounting Groups",
                "csv_href": "accounting_groups.csv",
                "headers": ["Accounting Group", "VO", "Users", "Jobs", "Wall h (scaled)", "CPU h (scaled)", "Avg Proc", "Max Proc", "Max Mem GB"],
                "row_fields": ["accounting_group", "vo", "users", "jobs", "wall_hours_display", "cpu_hours_display", "avg_processors_display", "max_processors_display", "max_memory_gb_display"],
                "rows": _build_rows(accounting_group_rows, "accounting_groups"),
            },
        ],
    }


@lru_cache(maxsize=1)
def _jinja_environment() -> Environment:
    return Environment(
        loader=PackageLoader("htcondor_accounting", "templates"),
        autoescape=select_autoescape(["html", "xml"]),
    )


def render_monthly_report_html(context: dict[str, Any]) -> str:
    template = _jinja_environment().get_template("monthly_report.html")
    return template.render(**context)
