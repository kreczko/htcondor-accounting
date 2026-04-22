from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
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


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    return int(value)


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    return float(value)


def _display_value(value: Any) -> str:
    if value in (None, ""):
        return "-"
    return str(value)


def _distinct_marker(values: set[str]) -> str:
    cleaned = {value for value in values if value and value != "-"}
    if not cleaned:
        return "-"
    if len(cleaned) == 1:
        return next(iter(cleaned))
    return "MULTIPLE"


def _accounting_group_key(job: dict[str, Any]) -> str:
    for key in ("acct_group", "accounting_group", "acct_group_user", "route_name"):
        value = _display_value(job.get(key))
        if value != "-":
            return value
    return "-"


@dataclass
class _Accumulator:
    jobs: int = 0
    wall_seconds: int = 0
    cpu_user_seconds: int = 0
    cpu_sys_seconds: int = 0
    cpu_total_seconds: int = 0
    scaled_wall_seconds: float = 0.0
    scaled_cpu_seconds: float = 0.0
    processors_sum: int = 0
    max_processors: int = 0
    memory_real_kb_max: int | None = None
    memory_virtual_kb_max: int | None = None
    users: set[str] = field(default_factory=set)
    vos: set[str] = field(default_factory=set)


def _update_accumulator(acc: _Accumulator, job: dict[str, Any]) -> None:
    acc.jobs += 1
    wall_seconds = _safe_int(job.get("wall_seconds"))
    cpu_user_seconds = _safe_int(job.get("cpu_user_seconds"))
    cpu_sys_seconds = _safe_int(job.get("cpu_sys_seconds"))
    cpu_total_seconds = _safe_int(job.get("cpu_total_seconds")) or (cpu_user_seconds + cpu_sys_seconds)
    scale_factor = _safe_float(job.get("scale_factor"), default=1.0) if job.get("scale_factor") is not None else 1.0
    processors = _safe_int(job.get("processors"), default=1) or 1

    acc.wall_seconds += wall_seconds
    acc.cpu_user_seconds += cpu_user_seconds
    acc.cpu_sys_seconds += cpu_sys_seconds
    acc.cpu_total_seconds += cpu_total_seconds
    acc.scaled_wall_seconds += wall_seconds * scale_factor
    acc.scaled_cpu_seconds += cpu_total_seconds * scale_factor
    acc.processors_sum += processors
    acc.max_processors = max(acc.max_processors, processors)

    user = _display_value(job.get("local_user"))
    if user != "-":
        acc.users.add(user)
    vo = _display_value(job.get("vo"))
    if vo != "-":
        acc.vos.add(vo)

    memory_real = job.get("memory_real_kb")
    if memory_real is not None:
        memory_real_int = _safe_int(memory_real)
        acc.memory_real_kb_max = memory_real_int if acc.memory_real_kb_max is None else max(acc.memory_real_kb_max, memory_real_int)

    memory_virtual = job.get("memory_virtual_kb")
    if memory_virtual is not None:
        memory_virtual_int = _safe_int(memory_virtual)
        acc.memory_virtual_kb_max = (
            memory_virtual_int if acc.memory_virtual_kb_max is None else max(acc.memory_virtual_kb_max, memory_virtual_int)
        )


def _make_row(
    group_type: str,
    group_key: str,
    acc: _Accumulator,
    *,
    include_users: bool,
    include_vo: bool,
) -> UsageGroupRow:
    return UsageGroupRow(
        group_type=group_type,
        group_key=group_key,
        jobs=acc.jobs,
        users=len(acc.users) if include_users else None,
        vo=_distinct_marker(acc.vos) if include_vo else None,
        wall_seconds=acc.wall_seconds,
        cpu_user_seconds=acc.cpu_user_seconds,
        cpu_sys_seconds=acc.cpu_sys_seconds,
        cpu_total_seconds=acc.cpu_total_seconds,
        scaled_wall_seconds=acc.scaled_wall_seconds,
        scaled_cpu_seconds=acc.scaled_cpu_seconds,
        avg_processors=(acc.processors_sum / acc.jobs) if acc.jobs else 0.0,
        max_processors=acc.max_processors,
        memory_real_kb_max=acc.memory_real_kb_max,
        memory_virtual_kb_max=acc.memory_virtual_kb_max,
    )


def _group_rows(
    jobs: Iterable[dict[str, Any]],
    *,
    group_type: str,
    group_key_fn: Callable[[dict[str, Any]], str],
    include_users: bool = False,
    include_vo: bool = False,
) -> list[UsageGroupRow]:
    grouped: dict[str, _Accumulator] = defaultdict(_Accumulator)
    for job in jobs:
        key = group_key_fn(job)
        _update_accumulator(grouped[key], job)

    return [
        _make_row(group_type, key, grouped[key], include_users=include_users, include_vo=include_vo)
        for key in sorted(grouped)
    ]


def group_jobs_by_user(jobs: Iterable[dict[str, Any]]) -> list[UsageGroupRow]:
    return _group_rows(
        jobs,
        group_type="user",
        group_key_fn=lambda job: _display_value(job.get("local_user")),
        include_vo=True,
    )


def group_jobs_by_vo(jobs: Iterable[dict[str, Any]]) -> list[UsageGroupRow]:
    return _group_rows(
        jobs,
        group_type="vo",
        group_key_fn=lambda job: _display_value(job.get("vo")),
        include_users=True,
    )


def group_jobs_by_accounting_group(jobs: Iterable[dict[str, Any]]) -> list[UsageGroupRow]:
    return _group_rows(
        jobs,
        group_type="accounting_group",
        group_key_fn=_accounting_group_key,
        include_users=True,
        include_vo=True,
    )


def group_jobs_by_schedd(jobs: Iterable[dict[str, Any]]) -> list[UsageGroupRow]:
    return _group_rows(jobs, group_type="schedd", group_key_fn=lambda job: _display_value(job.get("source_schedd")))
