from __future__ import annotations

from typing import Any

from htcondor_accounting.models.reporting import MonthlyReportSummary
from htcondor_accounting.util.dates import month_label


def build_monthly_report_summary(year: int, month: int, jobs: list[dict[str, Any]]) -> MonthlyReportSummary:
    days_included = len({str(job.get("day")) for job in jobs if job.get("day")})
    wall_seconds = sum(int(job.get("wall_seconds") or 0) for job in jobs)
    cpu_user_seconds = sum(int(job.get("cpu_user_seconds") or 0) for job in jobs)
    cpu_sys_seconds = sum(int(job.get("cpu_sys_seconds") or 0) for job in jobs)
    cpu_total_seconds = sum(int(job.get("cpu_total_seconds") or 0) for job in jobs)
    scaled_wall_seconds = 0.0
    scaled_cpu_seconds = 0.0
    processor_values = [int(job.get("processors") or 1) for job in jobs]
    memory_real_values = [int(job["memory_real_kb"]) for job in jobs if job.get("memory_real_kb") is not None]
    memory_virtual_values = [int(job["memory_virtual_kb"]) for job in jobs if job.get("memory_virtual_kb") is not None]

    for job in jobs:
        scale_factor = float(job["scale_factor"]) if job.get("scale_factor") is not None else 1.0
        scaled_wall_seconds += int(job.get("wall_seconds") or 0) * scale_factor
        scaled_cpu_seconds += int(job.get("cpu_total_seconds") or 0) * scale_factor

    return MonthlyReportSummary(
        year=year,
        month=month,
        period=month_label(year, month),
        days_included=days_included,
        jobs_total=len(jobs),
        wall_seconds=wall_seconds,
        cpu_user_seconds=cpu_user_seconds,
        cpu_sys_seconds=cpu_sys_seconds,
        cpu_total_seconds=cpu_total_seconds,
        scaled_wall_seconds=scaled_wall_seconds,
        scaled_cpu_seconds=scaled_cpu_seconds,
        avg_processors=(sum(processor_values) / len(processor_values)) if processor_values else None,
        max_processors=max(processor_values) if processor_values else None,
        memory_real_kb_max=max(memory_real_values) if memory_real_values else None,
        memory_virtual_kb_max=max(memory_virtual_values) if memory_virtual_values else None,
    )


def summary_json_payload(summary: MonthlyReportSummary) -> dict[str, Any]:
    return summary.model_dump(mode="json")
