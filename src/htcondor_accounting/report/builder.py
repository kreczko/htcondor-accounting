from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from htcondor_accounting.export.csv import write_csv_rows
from htcondor_accounting.models.reporting import DailyReportSummary, MonthlyReportSummary
from htcondor_accounting.render.html import build_report_context, render_report_html
from htcondor_accounting.render.plots import write_wall_hours_by_accounting_group_plot
from htcondor_accounting.report.jobs import group_jobs_by_accounting_group, group_jobs_by_user, group_jobs_by_vo
from htcondor_accounting.report.summary import build_daily_report_summary, build_monthly_report_summary, summary_json_payload
from htcondor_accounting.report.daily import write_json
from htcondor_accounting.store.layout import ensure_parent_dir


ReportPeriodType = Literal["daily", "monthly"]


def _write_usage_csvs(
    *,
    user_rows: list[Any],
    vo_rows: list[Any],
    accounting_group_rows: list[Any],
    users_csv_path: Path,
    vos_csv_path: Path,
    accounting_groups_csv_path: Path,
) -> None:
    write_csv_rows(
        users_csv_path,
        [{**row.model_dump(mode="json"), "user": row.group_key} for row in user_rows],
        [
            "user",
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
        ],
    )
    write_csv_rows(
        vos_csv_path,
        [{**row.model_dump(mode="json"), "vo": row.group_key} for row in vo_rows],
        [
            "vo",
            "users",
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
        ],
    )
    write_csv_rows(
        accounting_groups_csv_path,
        [{**row.model_dump(mode="json"), "accounting_group": row.group_key} for row in accounting_group_rows],
        [
            "accounting_group",
            "vo",
            "users",
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
        ],
    )


def build_report_summary(
    *,
    period_type: ReportPeriodType,
    period_label: str,
    jobs: list[dict[str, Any]],
    year: int | None = None,
    month: int | None = None,
    schedd_name: str | None = None,
) -> DailyReportSummary | MonthlyReportSummary:
    if period_type == "daily":
        return build_daily_report_summary(period_label, jobs)
    if year is None or month is None:
        raise ValueError("monthly report summary requires year and month")
    return build_monthly_report_summary(year, month, jobs, schedd=schedd_name)


def write_report_set(
    *,
    period_type: ReportPeriodType,
    period_label: str,
    jobs: list[dict[str, Any]],
    output_dir: Path,
    benchmark_type: str,
    benchmark_baseline: float,
    users_csv_path: Path,
    vos_csv_path: Path,
    accounting_groups_csv_path: Path,
    summary_path: Path,
    index_path: Path,
    plot_path: Path | None = None,
    year: int | None = None,
    month: int | None = None,
    schedd_name: str | None = None,
    parent_index_link: str | None = None,
    schedd_links: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    user_rows = group_jobs_by_user(jobs)
    vo_rows = group_jobs_by_vo(jobs)
    accounting_group_rows = group_jobs_by_accounting_group(jobs)
    summary = build_report_summary(
        period_type=period_type,
        period_label=period_label,
        jobs=jobs,
        year=year,
        month=month,
        schedd_name=schedd_name,
    )

    _write_usage_csvs(
        user_rows=user_rows,
        vo_rows=vo_rows,
        accounting_group_rows=accounting_group_rows,
        users_csv_path=users_csv_path,
        vos_csv_path=vos_csv_path,
        accounting_groups_csv_path=accounting_groups_csv_path,
    )
    write_json(summary_path, summary_json_payload(summary))

    plot_href: str | None = None
    if plot_path is not None:
        write_wall_hours_by_accounting_group_plot(
            jobs,
            plot_path,
            period_type=period_type,
            title=f"Wall hours by accounting group - {period_label}",
        )
        plot_href = plot_path.relative_to(output_dir).as_posix()

    ensure_parent_dir(index_path)
    report_context = build_report_context(
        period_type=period_type,
        summary=summary,
        user_rows=user_rows,
        vo_rows=vo_rows,
        accounting_group_rows=accounting_group_rows,
        benchmark_type=benchmark_type,
        benchmark_baseline=benchmark_baseline,
        schedd_name=schedd_name,
        parent_index_link=parent_index_link,
        schedd_links=schedd_links,
        plot_href=plot_href,
    )
    index_path.write_text(render_report_html(report_context), encoding="utf-8")

    return {"summary": summary, "index_path": index_path, "plot_path": plot_path}
