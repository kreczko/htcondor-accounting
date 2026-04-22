from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from htcondor_accounting.models.reporting import UsageGroupRow
from htcondor_accounting.store.jsonl import read_jsonl_zst
from htcondor_accounting.store.layout import derived_daily_jobs_file
from htcondor_accounting.util.dates import iter_days_in_month


def iter_monthly_job_paths(output_root: Path, year: int, month: int) -> list[Path]:
    paths: list[Path] = []
    for day in iter_days_in_month(year, month):
        path = derived_daily_jobs_file(output_root, day)
        if path.exists():
            paths.append(path)
    return paths


def iter_monthly_jobs(output_root: Path, year: int, month: int) -> Iterable[dict[str, Any]]:
    for path in iter_monthly_job_paths(output_root, year, month):
        yield from read_jsonl_zst(path)


def load_monthly_jobs(output_root: Path, year: int, month: int) -> list[dict[str, Any]]:
    return list(iter_monthly_jobs(output_root, year, month))


def _safe_int(value: Any) -> int:
    if value is None:
        return 0
    return int(value)


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    return float(value)


def _group_value(job: dict[str, Any], key: str) -> str:
    value = job.get(key)
    if value in (None, ""):
        return "-"
    return str(value)


@dataclass
class _Accumulator:
    jobs: int = 0
    wall_seconds: int = 0
    cpu_user_seconds: int = 0
    cpu_sys_seconds: int = 0
    cpu_total_seconds: int = 0
    scaled_wall_seconds: float = 0.0
    scaled_cpu_seconds: float = 0.0
    processors_total: int = 0
    memory_real_kb_max: int | None = None
    memory_virtual_kb_max: int | None = None
    memory_real_kb_sum: int = 0
    memory_virtual_kb_sum: int = 0
    memory_real_kb_count: int = 0
    memory_virtual_kb_count: int = 0


def _update_accumulator(acc: _Accumulator, job: dict[str, Any]) -> None:
    acc.jobs += 1
    wall_seconds = _safe_int(job.get("wall_seconds"))
    cpu_user_seconds = _safe_int(job.get("cpu_user_seconds"))
    cpu_sys_seconds = _safe_int(job.get("cpu_sys_seconds"))
    cpu_total_seconds = _safe_int(job.get("cpu_total_seconds")) or (cpu_user_seconds + cpu_sys_seconds)
    scale_factor = job.get("scale_factor")
    scale = _safe_float(scale_factor) if scale_factor is not None else 1.0
    processors = _safe_int(job.get("processors")) or 1

    acc.wall_seconds += wall_seconds
    acc.cpu_user_seconds += cpu_user_seconds
    acc.cpu_sys_seconds += cpu_sys_seconds
    acc.cpu_total_seconds += cpu_total_seconds
    acc.scaled_wall_seconds += wall_seconds * scale
    acc.scaled_cpu_seconds += cpu_total_seconds * scale
    acc.processors_total += processors

    memory_real = job.get("memory_real_kb")
    if memory_real is not None:
        memory_real_int = _safe_int(memory_real)
        acc.memory_real_kb_max = memory_real_int if acc.memory_real_kb_max is None else max(acc.memory_real_kb_max, memory_real_int)
        acc.memory_real_kb_sum += memory_real_int
        acc.memory_real_kb_count += 1

    memory_virtual = job.get("memory_virtual_kb")
    if memory_virtual is not None:
        memory_virtual_int = _safe_int(memory_virtual)
        acc.memory_virtual_kb_max = (
            memory_virtual_int if acc.memory_virtual_kb_max is None else max(acc.memory_virtual_kb_max, memory_virtual_int)
        )
        acc.memory_virtual_kb_sum += memory_virtual_int
        acc.memory_virtual_kb_count += 1


def _group_rows(
    jobs: Iterable[dict[str, Any]],
    *,
    group_type: str,
    group_key_fn: Callable[[dict[str, Any]], str],
) -> list[UsageGroupRow]:
    grouped: dict[str, _Accumulator] = defaultdict(_Accumulator)
    for job in jobs:
        key = group_key_fn(job)
        _update_accumulator(grouped[key], job)

    rows: list[UsageGroupRow] = []
    for key in sorted(grouped):
        acc = grouped[key]
        rows.append(
            UsageGroupRow(
                group_type=group_type,
                group_key=key,
                jobs=acc.jobs,
                wall_seconds=acc.wall_seconds,
                cpu_user_seconds=acc.cpu_user_seconds,
                cpu_sys_seconds=acc.cpu_sys_seconds,
                cpu_total_seconds=acc.cpu_total_seconds,
                scaled_wall_seconds=acc.scaled_wall_seconds,
                scaled_cpu_seconds=acc.scaled_cpu_seconds,
                processors_total=acc.processors_total,
                memory_real_kb_max=acc.memory_real_kb_max,
                memory_virtual_kb_max=acc.memory_virtual_kb_max,
                memory_real_kb_avg=(
                    acc.memory_real_kb_sum / acc.memory_real_kb_count if acc.memory_real_kb_count else None
                ),
                memory_virtual_kb_avg=(
                    acc.memory_virtual_kb_sum / acc.memory_virtual_kb_count if acc.memory_virtual_kb_count else None
                ),
            )
        )
    return rows


def group_jobs_by_user(jobs: Iterable[dict[str, Any]]) -> list[UsageGroupRow]:
    return _group_rows(jobs, group_type="user", group_key_fn=lambda job: _group_value(job, "local_user"))


def group_jobs_by_vo(jobs: Iterable[dict[str, Any]]) -> list[UsageGroupRow]:
    return _group_rows(jobs, group_type="vo", group_key_fn=lambda job: _group_value(job, "vo"))


def group_jobs_by_schedd(jobs: Iterable[dict[str, Any]]) -> list[UsageGroupRow]:
    return _group_rows(jobs, group_type="schedd", group_key_fn=lambda job: _group_value(job, "source_schedd"))
