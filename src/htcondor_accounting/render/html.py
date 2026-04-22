from __future__ import annotations

from html import escape

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


def _summary_item(label: str, value: str) -> str:
    return f"<div class='summary-item'><span class='summary-label'>{escape(label)}</span><span class='summary-value'>{escape(value)}</span></div>"


def _cell(value: str) -> str:
    return f"<td>{escape(value)}</td>"


def _render_table_section(title: str, csv_name: str, headers: list[str], rows: list[list[str]]) -> str:
    header_html = "".join(f"<th>{escape(header)}</th>" for header in headers)
    body_html = "".join("<tr>" + "".join(_cell(value) for value in row) + "</tr>" for row in rows)
    return (
        "<section class='report-section'>"
        f"<div class='section-header'><h2>{escape(title)}</h2><a href='{escape(csv_name)}'>Download CSV</a></div>"
        f"<table><thead><tr>{header_html}</tr></thead><tbody>{body_html}</tbody></table>"
        "</section>"
    )


def _user_rows(rows: list[UsageGroupRow]) -> list[list[str]]:
    return [
        [
            row.group_key,
            row.vo or "-",
            str(row.jobs),
            format_scaled_pair(row.wall_seconds, row.scaled_wall_seconds),
            format_scaled_pair(row.cpu_total_seconds, row.scaled_cpu_seconds),
            format_number(row.avg_processors),
            str(row.max_processors),
            format_gb(row.memory_real_kb_max),
        ]
        for row in rows
    ]


def _vo_rows(rows: list[UsageGroupRow]) -> list[list[str]]:
    return [
        [
            row.group_key,
            str(row.users or 0),
            str(row.jobs),
            format_scaled_pair(row.wall_seconds, row.scaled_wall_seconds),
            format_scaled_pair(row.cpu_total_seconds, row.scaled_cpu_seconds),
            format_number(row.avg_processors),
            str(row.max_processors),
            format_gb(row.memory_real_kb_max),
        ]
        for row in rows
    ]


def _accounting_group_rows(rows: list[UsageGroupRow]) -> list[list[str]]:
    return [
        [
            row.group_key,
            row.vo or "-",
            str(row.users or 0),
            str(row.jobs),
            format_scaled_pair(row.wall_seconds, row.scaled_wall_seconds),
            format_scaled_pair(row.cpu_total_seconds, row.scaled_cpu_seconds),
            format_number(row.avg_processors),
            str(row.max_processors),
            format_gb(row.memory_real_kb_max),
        ]
        for row in rows
    ]


def render_monthly_report_html(
    summary: MonthlyReportSummary,
    user_rows: list[UsageGroupRow],
    vo_rows: list[UsageGroupRow],
    accounting_group_rows: list[UsageGroupRow],
    *,
    benchmark_type: str,
    benchmark_baseline: float,
) -> str:
    summary_html = "".join(
        [
            _summary_item("Month", summary.period),
            _summary_item("Days Included", str(summary.days_included)),
            _summary_item("Total Jobs", str(summary.jobs_total)),
            _summary_item("Total Wall Hours", format_hours(summary.wall_seconds)),
            _summary_item("Total CPU Hours", format_hours(summary.cpu_total_seconds)),
        ]
    )

    scaling_note = (
        f"Scaled values are adjusted relative to the configured {benchmark_type} baseline of "
        f"{benchmark_baseline:g}. The machine benchmark of the node each job ran on is used to "
        "normalize usage across nodes with different performance."
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>HTCondor Accounting Monthly Report {escape(summary.period)}</title>
  <style>
    body {{ font-family: sans-serif; margin: 1.5rem; color: #1f2937; }}
    h1 {{ margin-bottom: 0.5rem; }}
    .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 0.5rem; margin-bottom: 1rem; }}
    .summary-item {{ border: 1px solid #d1d5db; padding: 0.5rem 0.6rem; border-radius: 4px; background: #f9fafb; }}
    .summary-label {{ display: block; font-size: 0.75rem; color: #4b5563; }}
    .summary-value {{ display: block; font-weight: 700; }}
    .note {{ margin: 0 0 1rem 0; padding: 0.6rem 0.75rem; background: #eff6ff; border-left: 3px solid #60a5fa; font-size: 0.9rem; }}
    .report-section {{ margin-bottom: 1.25rem; }}
    .section-header {{ display: flex; justify-content: space-between; align-items: baseline; gap: 1rem; margin-bottom: 0.35rem; }}
    .section-header h2 {{ margin: 0; font-size: 1rem; }}
    .section-header a {{ font-size: 0.85rem; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 0.87rem; }}
    th, td {{ border: 1px solid #d1d5db; padding: 0.28rem 0.4rem; text-align: left; white-space: nowrap; }}
    th {{ background: #f3f4f6; }}
    tbody tr:nth-child(even) {{ background: #fafafa; }}
  </style>
</head>
<body>
  <h1>HTCondor Accounting Monthly Report {escape(summary.period)}</h1>
  <div class="summary-grid">{summary_html}</div>
  <p class="note">{escape(scaling_note)}</p>
  {_render_table_section(
      "Users",
      "users.csv",
      ["User", "VO", "Jobs", "Wall h (scaled)", "CPU h (scaled)", "Avg Proc", "Max Proc", "Max Mem GB"],
      _user_rows(user_rows),
  )}
  {_render_table_section(
      "VOs",
      "vos.csv",
      ["VO", "Users", "Jobs", "Wall h (scaled)", "CPU h (scaled)", "Avg Proc", "Max Proc", "Max Mem GB"],
      _vo_rows(vo_rows),
  )}
  {_render_table_section(
      "Accounting Groups",
      "accounting_groups.csv",
      ["Accounting Group", "VO", "Users", "Jobs", "Wall h (scaled)", "CPU h (scaled)", "Avg Proc", "Max Proc", "Max Mem GB"],
      _accounting_group_rows(accounting_group_rows),
  )}
</body>
</html>
"""
