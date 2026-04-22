import json
from pathlib import Path

from htcondor_accounting.export.csv import write_csv_rows
from htcondor_accounting.models.reporting import UsageGroupRow
from htcondor_accounting.render.html import render_monthly_report_html
from htcondor_accounting.report.jobs import (
    group_jobs_by_accounting_group,
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
    acct_group: str | None = None,
    acct_group_user: str | None = None,
    accounting_group: str | None = None,
    route_name: str | None = None,
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
        "acct_group": acct_group,
        "acct_group_user": acct_group_user,
        "accounting_group": accounting_group,
        "route_name": route_name,
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
            _job("job-3", day="2026-04-17", user="bob", vo="cms", wall_seconds=5, cpu_user_seconds=2, cpu_sys_seconds=1, processors=4, memory_real_kb=150, memory_virtual_kb=250),
        ],
    )
    _write_daily_jobs(
        tmp_path,
        "2026-04-18",
        [
            _job("job-4", day="2026-04-18", user="alice", vo="atlas", wall_seconds=7, cpu_user_seconds=3, cpu_sys_seconds=1, processors=1, memory_real_kb=120, memory_virtual_kb=320, scale_factor=2.0),
            _job("job-5", day="2026-04-18", user="charlie", vo="cms", wall_seconds=8, cpu_user_seconds=3, cpu_sys_seconds=1, processors=2, memory_real_kb=180, memory_virtual_kb=260),
        ],
    )

    jobs = load_monthly_jobs(tmp_path, 2026, 4)
    user_rows = group_jobs_by_user(jobs)
    vo_rows = group_jobs_by_vo(jobs)

    assert len(iter_monthly_job_paths(tmp_path, 2026, 4)) == 2
    assert len(user_rows) == 3
    assert len(vo_rows) == 2

    alice = next(row for row in user_rows if row.group_key == "alice")
    assert alice.jobs == 3
    assert alice.vo == "atlas"
    assert alice.wall_seconds == 37
    assert alice.cpu_total_seconds == 17
    assert alice.scaled_wall_seconds == 44.0
    assert alice.scaled_cpu_seconds == 21.0
    assert alice.avg_processors == 4 / 3
    assert alice.max_processors == 2
    assert alice.memory_real_kb_max == 200
    assert alice.memory_virtual_kb_max == 400

    atlas = next(row for row in vo_rows if row.group_key == "atlas")
    assert atlas.jobs == 3
    assert atlas.wall_seconds == 37
    assert atlas.users == 1

    cms = next(row for row in vo_rows if row.group_key == "cms")
    assert cms.users == 2


def test_user_projection_vo_marker_handles_multiple_and_missing_vo(tmp_path: Path) -> None:
    _write_daily_jobs(
        tmp_path,
        "2026-04-17",
        [
            _job("job-1", day="2026-04-17", user="alice", vo="atlas", wall_seconds=10, cpu_user_seconds=4, cpu_sys_seconds=1, processors=1, memory_real_kb=100, memory_virtual_kb=300),
            _job("job-2", day="2026-04-17", user="alice", vo="cms", wall_seconds=10, cpu_user_seconds=4, cpu_sys_seconds=1, processors=1, memory_real_kb=100, memory_virtual_kb=300),
            _job("job-3", day="2026-04-17", user="bob", vo="-", wall_seconds=10, cpu_user_seconds=4, cpu_sys_seconds=1, processors=1, memory_real_kb=100, memory_virtual_kb=300),
        ],
    )
    rows = group_jobs_by_user(load_monthly_jobs(tmp_path, 2026, 4))
    alice = next(row for row in rows if row.group_key == "alice")
    bob = next(row for row in rows if row.group_key == "bob")
    assert alice.vo == "MULTIPLE"
    assert bob.vo == "-"


def test_accounting_group_projection_is_deterministic(tmp_path: Path) -> None:
    _write_daily_jobs(
        tmp_path,
        "2026-04-17",
        [
            _job("job-1", day="2026-04-17", user="alice", vo="atlas", wall_seconds=10, cpu_user_seconds=4, cpu_sys_seconds=1, processors=1, memory_real_kb=100, memory_virtual_kb=300, acct_group="group-a"),
            _job("job-2", day="2026-04-17", user="bob", vo="atlas", wall_seconds=20, cpu_user_seconds=6, cpu_sys_seconds=2, processors=2, memory_real_kb=200, memory_virtual_kb=400, accounting_group="group-b"),
            _job("job-3", day="2026-04-17", user="charlie", vo="cms", wall_seconds=5, cpu_user_seconds=2, cpu_sys_seconds=1, processors=4, memory_real_kb=150, memory_virtual_kb=250, acct_group_user="group-c"),
            _job("job-4", day="2026-04-17", user="dana", vo="cms", wall_seconds=7, cpu_user_seconds=3, cpu_sys_seconds=1, processors=8, memory_real_kb=500, memory_virtual_kb=700, route_name="route-d"),
        ],
    )
    rows = group_jobs_by_accounting_group(load_monthly_jobs(tmp_path, 2026, 4))
    assert [row.group_key for row in rows] == ["group-a", "group-b", "group-c", "route-d"]
    group_d = next(row for row in rows if row.group_key == "route-d")
    assert group_d.vo == "cms"
    assert group_d.users == 1
    assert group_d.avg_processors == 8.0
    assert group_d.max_processors == 8
    assert group_d.memory_real_kb_max == 500


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
    assert summary.avg_processors == 1.5
    assert summary.max_processors == 2
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
                users=None,
                vo="atlas",
                wall_seconds=10,
                cpu_user_seconds=4,
                cpu_sys_seconds=1,
                cpu_total_seconds=5,
                scaled_wall_seconds=10.0,
                scaled_cpu_seconds=5.0,
                avg_processors=1.0,
                max_processors=1,
                memory_real_kb_max=100,
                memory_virtual_kb_max=200,
            )
        ],
        ["group_type", "group_key", "vo", "jobs", "avg_processors", "max_processors"],
    )

    assert path.read_text(encoding="utf-8") == "group_type,group_key,vo,jobs,avg_processors,max_processors\nuser,alice,atlas,1,1.0,1\n"


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
            users=None,
            vo="atlas",
            wall_seconds=10,
            cpu_user_seconds=4,
            cpu_sys_seconds=1,
            cpu_total_seconds=5,
            scaled_wall_seconds=10.0,
            scaled_cpu_seconds=5.0,
            avg_processors=1.0,
            max_processors=1,
            memory_real_kb_max=100,
            memory_virtual_kb_max=300,
        )
    ]
    vo_rows = [
        UsageGroupRow(
            group_type="vo",
            group_key="atlas",
            jobs=1,
            users=1,
            vo=None,
            wall_seconds=10,
            cpu_user_seconds=4,
            cpu_sys_seconds=1,
            cpu_total_seconds=5,
            scaled_wall_seconds=10.0,
            scaled_cpu_seconds=5.0,
            avg_processors=1.0,
            max_processors=1,
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
