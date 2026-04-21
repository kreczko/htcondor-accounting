import json
from datetime import datetime, timezone
from pathlib import Path

from htcondor_accounting.report.dedup import deduplicate_canonical_records
from htcondor_accounting.report.daily import derive_daily
from htcondor_accounting.store.jsonl import read_jsonl_zst, write_jsonl_zst
from htcondor_accounting.store.layout import canonical_day_dir


def _canonical_record(
    global_job_id: str,
    *,
    owner: str = "alice",
    local_user: str = "alice",
    vo: str | None = "atlas",
    vo_group: str | None = "/atlas",
    vo_role: str | None = None,
    auth_method: str | None = "scitoken",
    start_time: int = 1776386139,
    end_time: int = 1776428989,
    wall_seconds: int = 100,
    cpu_user_seconds: int = 50,
    cpu_sys_seconds: int = 10,
    processors: int = 1,
    memory_real_kb: int | None = 1000,
    memory_virtual_kb: int | None = 2000,
    scale_factor: float | None = 2.0,
    benchmark_type: str | None = "hepscore23",
    schedd: str = "lcgce02.phy.bris.ac.uk",
) -> dict:
    return {
        "schema_version": 1,
        "record_type": "job",
        "site_name": "UKI-SOUTHGRID-BRIS-HEP",
        "source": {
            "system": "htcondor",
            "schedd": schedd,
            "collector_host": None,
            "collected_at": "2026-04-17T12:00:00Z",
        },
        "job": {
            "global_job_id": global_job_id,
            "routed_from_job_id": None,
            "owner": owner,
            "local_user": local_user,
        },
        "usage": {
            "wall_seconds": wall_seconds,
            "cpu_user_seconds": cpu_user_seconds,
            "cpu_sys_seconds": cpu_sys_seconds,
            "processors": processors,
            "memory_real_kb": memory_real_kb,
            "memory_virtual_kb": memory_virtual_kb,
        },
        "timing": {
            "queue_time": None,
            "start_time": start_time,
            "end_time": end_time,
            "status_time": end_time,
        },
        "identity": {
            "dn": "/C=UK/O=eScience/CN=alice",
            "fqan": "/atlas",
            "vo": vo,
            "vo_group": vo_group,
            "vo_role": vo_role,
            "auth_method": auth_method,
            "token_issuer": "https://issuer.example",
            "token_subject": "subject",
            "token_groups": ["/atlas"],
        },
        "benchmark": {
            "benchmark_type": benchmark_type,
            "site_baseline_per_core": None,
            "node_per_core": None,
            "scale_factor": scale_factor,
        },
        "execution": {
            "ce_host": schedd,
            "ce_id": None,
            "execute_node": "slot1@node",
            "slot_name": "slot1@node",
        },
    }


def test_deduplicate_canonical_records_by_global_job_id() -> None:
    records = [
        _canonical_record("job-001", owner="alice"),
        _canonical_record("job-001", owner="bob"),
        _canonical_record("job-002", owner="carol"),
    ]

    result = deduplicate_canonical_records(records)

    assert result.input_records == 3
    assert result.duplicate_records == 1
    assert len(result.unique_records) == 2
    assert result.unique_records[0]["job"]["owner"] == "alice"
    assert result.duplicate_sample == ["job-001"]


def test_derive_daily_writes_summary_and_deduplicated_jobs(tmp_path: Path) -> None:
    when = datetime(2026, 4, 17, tzinfo=timezone.utc)
    day_dir = canonical_day_dir(tmp_path, when)
    write_jsonl_zst(
        day_dir / "ce01.jsonl.zst",
        [
            _canonical_record("job-001", owner="alice", local_user="alice", vo="atlas", wall_seconds=100, cpu_user_seconds=50, cpu_sys_seconds=10, scale_factor=2.0),
            _canonical_record("job-001", owner="alice-dup", local_user="alice", vo="atlas", wall_seconds=999, cpu_user_seconds=1, cpu_sys_seconds=1, scale_factor=9.0),
        ],
    )
    write_jsonl_zst(
        day_dir / "ce02.jsonl.zst",
        [
            _canonical_record("job-002", owner="bob", local_user="bob", vo="cms", wall_seconds=200, cpu_user_seconds=70, cpu_sys_seconds=30, scale_factor=None),
        ],
    )

    result = derive_daily(tmp_path, when)

    assert len(result.input_paths) == 2
    assert result.input_records == 3
    assert result.unique_records == 2
    assert result.duplicate_records == 1
    assert result.jobs_path.exists()
    assert result.summary_path.exists()
    assert result.duplicates_path.exists()

    jobs = list(read_jsonl_zst(result.jobs_path))
    assert len(jobs) == 2
    assert jobs[0]["record_type"] == "report_job"
    assert jobs[0]["cpu_total_seconds"] == 60
    assert jobs[0]["day"] == "2026-04-17"
    assert "dn" not in jobs[0]
    assert "token_subject" not in jobs[0]
    assert jobs[0]["global_job_id"] == "job-001"

    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert summary["day"] == "2026-04-17"
    assert summary["input_files"] == 2
    assert summary["input_records"] == 3
    assert summary["unique_records"] == 2
    assert summary["duplicate_records"] == 1
    assert summary["users"] == 2
    assert summary["vos"] == 2
    assert summary["wall_seconds"] == 300
    assert summary["cpu_user_seconds"] == 120
    assert summary["cpu_sys_seconds"] == 40
    assert summary["cpu_total_seconds"] == 160
    assert summary["scaled_wall_seconds"] == 400.0
    assert summary["scaled_cpu_seconds"] == 220.0

    duplicates = json.loads(result.duplicates_path.read_text(encoding="utf-8"))
    assert duplicates["day"] == "2026-04-17"
    assert duplicates["input_files"] == 2
    assert duplicates["input_records"] == 3
    assert duplicates["unique_records"] == 2
    assert duplicates["duplicate_records"] == 1
    assert duplicates["duplicate_sample"] == ["job-001"]
