import json
from pathlib import Path

from htcondor_accounting.export.csv import write_csv_rows
from htcondor_accounting.models.reporting import UsageGroupRow
from htcondor_accounting.render.html import render_monthly_report_html
from htcondor_accounting.report.jobs import (
    group_jobs_by_user,
    group_jobs_by_vo,
    iter_monthly_job_paths,
    load_monthly_jobs,
)
from htcondor_accounting.report.summary import build_monthly_report_summary
from htcondor_accounting.store.jsonl import write_jsonl_zst


def _job(
    global_job_id: str,
    *,
    day: str,
    user: str,
    vo: str,
    wall_seconds: int,
    cpu_user_seconds: int,
    cpu_sys_seconds: int,
    processors: int,
    memory_real_kb: int,
    memory_virtual_kb: int,
    scale_factor: float = 1.0,
    schedd: str = "schedd-a.example",
) -> dict:
    return {
        "schema_version": 1,
        "record_type": "report_job",
        "site_name": "TEST-SITE",
        "global_job_id": global_job_id,
        "owner": user,
        "local_user": user,
        "vo": vo,
        "vo_group": f"/{vo}",
        "vo_role": None,
        "auth_method": "scitoken",
        "start_time": 1,
        "end_time": 2,
        "wall_seconds": wall_seconds,
        "cpu_user_seconds": cpu_user_seconds,
        "cpu_sys_seconds": cpu_sys_seconds,
        "cpu_total_seconds": cpu_user_seconds + cpu_sys_seconds,
        "processors": processors,
        "memory_real_kb": memory_real_kb,
        "memory_virtual_kb": memory_virtual_kb,
        "scale_factor": scale_factor,
        "benchmark_type": "hepscore23",
        "source_schedd": schedd,
        "day": day,
    }


def _write_daily_jobs(root: Path, day: str, jobs: list[dict]) -> None:
    year, month, day_number = day.split("-")
    path = root / "derived" / "daily" / year / month / day_number / "jobs.jsonl.zst"
    write_jsonl_zst(path, jobs)


def test_monthly_grouping_by_user_and_vo(tmp_path: Path) -> None:
    _write_daily_jobs(
        tmp_path,
        "2026-04-17",
        [
            _job("job-1", day="2026-04-17", user="alice", vo="atlas", wall_seconds=10, cpu_user_seconds=4, cpu_sys_seconds=1, processors=1, memory_real_kb=100, memory_virtual_kb=300),
            _job("job-2", day="2026-04-17", user="alice", vo="atlas", wall_seconds=20, cpu_user_seconds=6, cpu_sys_seconds=2, processors=2, memory_real_kb=200, memory_virtual_kb=400),
            _job("job-3", day="2026-04-17", user="bob", vo="cms", wall_seconds=5, cpu_user_seconds=2, cpu_sys_seconds=1, processors=1, memory_real_kb=150, memory_virtual_kb=250),
        ],
    )
    _write_daily_jobs(
        tmp_path,
        "2026-04-18",
        [
            _job("job-4", day="2026-04-18", user="alice", vo="atlas", wall_seconds=7, cpu_user_seconds=3, cpu_sys_seconds=1, processors=1, memory_real_kb=120, memory_virtual_kb=320, scale_factor=2.0),
        ],
    )

    jobs = load_monthly_jobs(tmp_path, 2026, 4)
    user_rows = group_jobs_by_user(jobs)
    vo_rows = group_jobs_by_vo(jobs)

    assert len(iter_monthly_job_paths(tmp_path, 2026, 4)) == 2
    assert len(user_rows) == 2
    assert len(vo_rows) == 2

    alice = next(row for row in user_rows if row.group_key == "alice")
    assert alice.jobs == 3
    assert alice.wall_seconds == 37
    assert alice.cpu_total_seconds == 17
    assert alice.scaled_wall_seconds == 44.0
    assert alice.scaled_cpu_seconds == 21.0
    assert alice.processors_total == 4
    assert alice.memory_real_kb_max == 200
    assert alice.memory_virtual_kb_max == 400

    atlas = next(row for row in vo_rows if row.group_key == "atlas")
    assert atlas.jobs == 3
    assert atlas.wall_seconds == 37


def test_monthly_summary_aggregates_totals_and_max_memory(tmp_path: Path) -> None:
    _write_daily_jobs(
        tmp_path,
        "2026-04-17",
        [
            _job("job-1", day="2026-04-17", user="alice", vo="atlas", wall_seconds=10, cpu_user_seconds=4, cpu_sys_seconds=1, processors=1, memory_real_kb=100, memory_virtual_kb=300),
            _job("job-2", day="2026-04-17", user="bob", vo="cms", wall_seconds=20, cpu_user_seconds=6, cpu_sys_seconds=2, processors=2, memory_real_kb=250, memory_virtual_kb=450, scale_factor=2.0),
        ],
    )
    jobs = load_monthly_jobs(tmp_path, 2026, 4)
    summary = build_monthly_report_summary(2026, 4, jobs)

    assert summary.jobs_total == 2
    assert summary.days_included == 1
    assert summary.wall_seconds == 30
    assert summary.cpu_total_seconds == 13
    assert summary.scaled_wall_seconds == 50.0
    assert summary.scaled_cpu_seconds == 21.0
    assert summary.memory_real_kb_max == 250
    assert summary.memory_virtual_kb_max == 450


def test_write_csv_rows_uses_stable_headers(tmp_path: Path) -> None:
    path = tmp_path / "users.csv"
    write_csv_rows(
        path,
        [
            UsageGroupRow(
                group_type="user",
                group_key="alice",
                jobs=1,
                wall_seconds=10,
                cpu_user_seconds=4,
                cpu_sys_seconds=1,
                cpu_total_seconds=5,
                scaled_wall_seconds=10.0,
                scaled_cpu_seconds=5.0,
                processors_total=1,
                memory_real_kb_max=100,
                memory_virtual_kb_max=200,
            )
        ],
        ["group_type", "group_key", "jobs"],
    )

    assert path.read_text(encoding="utf-8") == "group_type,group_key,jobs\nuser,alice,1\n"


def test_html_render_includes_summary_and_tables(tmp_path: Path) -> None:
    summary = build_monthly_report_summary(
        2026,
        4,
        [
            _job("job-1", day="2026-04-17", user="alice", vo="atlas", wall_seconds=10, cpu_user_seconds=4, cpu_sys_seconds=1, processors=1, memory_real_kb=100, memory_virtual_kb=300),
        ],
    )
    user_rows = [
        UsageGroupRow(
            group_type="user",
            group_key="alice",
            jobs=1,
            wall_seconds=10,
            cpu_user_seconds=4,
            cpu_sys_seconds=1,
            cpu_total_seconds=5,
            scaled_wall_seconds=10.0,
            scaled_cpu_seconds=5.0,
            processors_total=1,
            memory_real_kb_max=100,
            memory_virtual_kb_max=300,
        )
    ]
    vo_rows = [
        UsageGroupRow(
            group_type="vo",
            group_key="atlas",
            jobs=1,
            wall_seconds=10,
            cpu_user_seconds=4,
            cpu_sys_seconds=1,
            cpu_total_seconds=5,
            scaled_wall_seconds=10.0,
            scaled_cpu_seconds=5.0,
            processors_total=1,
            memory_real_kb_max=100,
            memory_virtual_kb_max=300,
        )
    ]

    html = render_monthly_report_html(summary, user_rows, vo_rows)

    assert "HTCondor Accounting Monthly Report 2026-04" in html
    assert "Days Included" in html
    assert "Users" in html
    assert "VOs" in html
    assert "alice" in html
    assert "atlas" in html
