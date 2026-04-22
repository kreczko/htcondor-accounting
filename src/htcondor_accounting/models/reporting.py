from __future__ import annotations

from pydantic import BaseModel


class UsageGroupRow(BaseModel):
    group_type: str
    group_key: str
    jobs: int
    wall_seconds: int
    cpu_user_seconds: int
    cpu_sys_seconds: int
    cpu_total_seconds: int
    scaled_wall_seconds: float
    scaled_cpu_seconds: float
    processors_total: int
    memory_real_kb_max: int | None = None
    memory_virtual_kb_max: int | None = None
    memory_real_kb_avg: float | None = None
    memory_virtual_kb_avg: float | None = None


class MonthlyReportSummary(BaseModel):
    schema_version: int = 1
    record_type: str = "monthly_report_summary"
    year: int
    month: int
    period: str
    days_included: int
    jobs_total: int
    wall_seconds: int
    cpu_user_seconds: int
    cpu_sys_seconds: int
    cpu_total_seconds: int
    scaled_wall_seconds: float
    scaled_cpu_seconds: float
    processors_total: int
    memory_real_kb_max: int | None = None
    memory_virtual_kb_max: int | None = None
