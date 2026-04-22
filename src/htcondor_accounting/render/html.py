from __future__ import annotations

from html import escape
from typing import Iterable

from htcondor_accounting.models.reporting import MonthlyReportSummary, UsageGroupRow


def _format_value(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _render_table(title: str, rows: Iterable[UsageGroupRow]) -> str:
    items = list(rows)
    headers = [
        "group_key",
        "users",
        "vo",
        "jobs",
        "wall_seconds",
        "cpu_user_seconds",
        "cpu_sys_seconds",
        "cpu_total_seconds",
        "scaled_wall_seconds",
        "scaled_cpu_seconds",
        "avg_processors",
        "max_processors",
        "memory_real_kb_max",
        "memory_virtual_kb_max",
    ]
    header_html = "".join(f"<th>{escape(header)}</th>" for header in headers)
    body_html = "".join(
        "<tr>"
        + "".join(f"<td>{escape(_format_value(getattr(row, header)))}</td>" for header in headers)
        + "</tr>"
        for row in items
    )
    return (
        f"<section><h2>{escape(title)}</h2>"
        f"<table><thead><tr>{header_html}</tr></thead><tbody>{body_html}</tbody></table></section>"
    )


def render_monthly_report_html(
    summary: MonthlyReportSummary,
    user_rows: list[UsageGroupRow],
    vo_rows: list[UsageGroupRow],
    schedd_rows: list[UsageGroupRow] | None = None,
) -> str:
    extra_section = _render_table("Schedds", schedd_rows or []) if schedd_rows is not None else ""
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>HTCondor Accounting Monthly Report {escape(summary.period)}</title>
  <style>
    body {{ font-family: sans-serif; margin: 2rem; color: #1f2937; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 2rem; }}
    th, td {{ border: 1px solid #d1d5db; padding: 0.5rem; text-align: left; }}
    th {{ background: #f3f4f6; }}
    .summary {{ margin-bottom: 2rem; }}
    .summary dt {{ font-weight: 700; }}
    .summary dd {{ margin: 0 0 0.5rem 0; }}
  </style>
</head>
<body>
  <h1>HTCondor Accounting Monthly Report {escape(summary.period)}</h1>
  <dl class="summary">
    <dt>Year/Month</dt><dd>{escape(summary.period)}</dd>
    <dt>Days Included</dt><dd>{summary.days_included}</dd>
    <dt>Jobs Total</dt><dd>{summary.jobs_total}</dd>
    <dt>Wall Seconds</dt><dd>{_format_value(summary.wall_seconds)}</dd>
    <dt>CPU Total Seconds</dt><dd>{_format_value(summary.cpu_total_seconds)}</dd>
    <dt>Scaled CPU Seconds</dt><dd>{_format_value(summary.scaled_cpu_seconds)}</dd>
  </dl>
  {_render_table("Users", user_rows)}
  {_render_table("VOs", vo_rows)}
  {extra_section}
</body>
</html>
"""
