import json
from datetime import datetime
from pathlib import Path

from typer.testing import CliRunner

from htcondor_accounting.cli import app
from htcondor_accounting.report.validate import validate_day
from htcondor_accounting.store.jsonl import write_jsonl_zst


runner = CliRunner()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _canonical_record(global_job_id: str, *, schedd: str, owner: str) -> dict:
    return {
        "site_name": "TEST-SITE",
        "source": {"schedd": schedd, "collected_at": "2026-04-21T12:00:00Z"},
        "job": {"global_job_id": global_job_id, "owner": owner, "local_user": owner},
        "timing": {"start_time": 100, "end_time": 200, "status_time": 200},
        "usage": {"wall_seconds": 10, "cpu_user_seconds": 4, "cpu_sys_seconds": 1, "processors": 1},
        "identity": {"auth_method": "scitoken"},
        "resolved_identity": {
            "vo": "atlas",
            "vo_group": "/atlas",
            "vo_role": None,
            "fqan": "/atlas",
            "resolution_method": "token_groups",
        },
        "benchmark": {"scale_factor": 1.0},
    }


def _derived_job(
    global_job_id: str,
    *,
    user: str,
    vo: str | None,
    fqan: str | None,
    auth_method: str,
    schedd: str,
    acct_group: str | None,
    resolution_method: str,
) -> dict:
    return {
        "schema_version": 1,
        "record_type": "report_job",
        "site_name": "TEST-SITE",
        "global_job_id": global_job_id,
        "owner": user,
        "local_user": user,
        "vo": vo,
        "vo_group": f"/{vo}" if vo else None,
        "vo_role": None,
        "fqan": fqan,
        "auth_method": auth_method,
        "start_time": 100,
        "end_time": 200,
        "wall_seconds": 10,
        "cpu_user_seconds": 4,
        "cpu_sys_seconds": 1,
        "cpu_total_seconds": 5,
        "processors": 1,
        "memory_real_kb": 100,
        "memory_virtual_kb": 200,
        "scale_factor": 1.0,
        "benchmark_type": "hepscore23",
        "source_schedd": schedd,
        "acct_group": acct_group,
        "acct_group_user": None,
        "accounting_group": None,
        "route_name": None,
        "day": "2026-04-21",
        "resolution_method": resolution_method,
    }


def _setup_day_fixture(root: Path) -> None:
    raw_dir = root / "raw-history" / "2026" / "04" / "21"
    canonical_dir = root / "canonical" / "2026" / "04" / "21"
    derived_dir = root / "derived" / "daily" / "2026" / "04" / "21"
    apel_staging_dir = root / "apel" / "staging" / "2026" / "04" / "21"
    apel_manifest_dir = root / "apel" / "manifests" / "2026" / "04" / "21"
    apel_ledger_sent_dir = root / "apel" / "ledger" / "sent"
    apel_ledger_resends_dir = root / "apel" / "ledger" / "resends"

    write_jsonl_zst(
        raw_dir / "schedd-a-20260422T010101Z.jsonl.zst",
        [
            {"GlobalJobId": "schedd-a.example#1.0#100", "Owner": "alice"},
            {"GlobalJobId": "schedd-a.example#2.0#100", "Owner": "bob"},
            {"GlobalJobId": "schedd-b.example#3.0#100", "Owner": "carol"},
        ],
    )
    write_jsonl_zst(
        canonical_dir / "schedd-a-20260422T010101Z.jsonl.zst",
        [
            _canonical_record("schedd-a.example#1.0#100", schedd="schedd-a.example", owner="alice"),
            _canonical_record("schedd-a.example#2.0#100", schedd="schedd-a.example", owner="bob"),
            _canonical_record("schedd-b.example#3.0#100", schedd="schedd-b.example", owner="carol"),
        ],
    )
    write_jsonl_zst(
        derived_dir / "jobs.jsonl.zst",
        [
            _derived_job(
                "schedd-a.example#1.0#100",
                user="alice",
                vo="atlas",
                fqan="/atlas",
                auth_method="scitoken",
                schedd="schedd-a.example",
                acct_group="atlas-group",
                resolution_method="token_groups",
            ),
            _derived_job(
                "schedd-a.example#2.0#100",
                user="bob",
                vo=None,
                fqan=None,
                auth_method="x509",
                schedd="schedd-a.example",
                acct_group=None,
                resolution_method="unresolved",
            ),
        ],
    )
    _write_json(
        derived_dir / "summary.json",
        {
            "day": "2026-04-21",
            "input_files": 1,
            "input_records": 3,
            "unique_records": 2,
            "duplicate_records": 1,
        },
    )
    _write_json(
        derived_dir / "duplicates.json",
        {
            "day": "2026-04-21",
            "input_files": 1,
            "input_records": 3,
            "unique_records": 2,
            "duplicate_records": 1,
        },
    )

    msg_path = apel_staging_dir / "20260422T010101Z-0001.msg"
    msg_path.parent.mkdir(parents=True, exist_ok=True)
    msg_path.write_text("%%\nSite: TEST-SITE\nLocalJobId: 1\n", encoding="utf-8")
    _write_json(
        apel_manifest_dir / "20260422T010101Z.json",
        {
            "schema_version": 1,
            "record_type": "apel_export_manifest",
            "day": "2026-04-21",
            "run_stamp": "20260422T010101Z",
            "jobs_seen": 2,
            "messages_written": 1,
            "total_bytes": msg_path.stat().st_size,
            "files_written": [
                {"path": str(msg_path), "records": 2, "bytes": msg_path.stat().st_size},
            ],
        },
    )
    _write_json(
        apel_ledger_sent_dir / ("a" * 32 + ".json"),
        {
            "schema_version": 1,
            "record_type": "apel_sent_marker",
            "message_md5": "a" * 32,
            "day": "2026-04-21",
            "staged_path": str(msg_path),
            "outgoing_path": str(root / "apel" / "outgoing" / "deadbeef" / "cafebabefeed12"),
            "records": 2,
            "bytes": msg_path.stat().st_size,
            "first_pushed_at": "2026-04-22T01:02:03Z",
            "run_stamp": "20260422T010101Z",
        },
    )
    _write_json(
        apel_ledger_resends_dir / ("20260422T020304Z-" + "a" * 32 + ".json"),
        {
            "schema_version": 1,
            "record_type": "apel_resend_event",
            "message_md5": "a" * 32,
            "day": "2026-04-21",
            "staged_path": str(msg_path),
            "outgoing_path": str(root / "apel" / "outgoing" / "deadbeef" / "cafebabefeed12"),
            "records": 2,
            "bytes": msg_path.stat().st_size,
            "resent_at": "2026-04-22T02:03:04Z",
            "run_stamp": "20260422T010101Z",
        },
    )


def test_validate_day_collects_counts_identity_quality_and_apel_state(tmp_path: Path) -> None:
    _setup_day_fixture(tmp_path)

    result = validate_day(tmp_path, day=datetime(2026, 4, 21))
    payload = result.payload

    assert payload["counts"]["raw_history_records"] == 3
    assert payload["counts"]["canonical_records"] == 3
    assert payload["counts"]["derived_unique_jobs"] == 2
    assert payload["counts"]["duplicate_jobs"] == 1
    assert payload["counts"]["apel_staged_messages"] == 1
    assert payload["counts"]["apel_sent_ledger_entries"] == 1
    assert payload["counts"]["apel_resend_events"] == 1

    assert payload["identity_quality"]["missing_resolved_vo"] == 1
    assert payload["identity_quality"]["missing_resolved_fqan"] == 1
    assert payload["identity_quality"]["missing_accounting_group"] == 1
    assert payload["identity_quality"]["auth_method_counts"] == {"scitoken": 1, "x509": 1, "local": 0}
    assert payload["identity_quality"]["unresolved_jobs"] == 1

    assert payload["warnings"] == []
    assert payload["errors"] == []


def test_validate_day_warns_on_simple_mismatches(tmp_path: Path) -> None:
    _setup_day_fixture(tmp_path)
    staged = tmp_path / "apel" / "staging" / "2026" / "04" / "21" / "20260422T010101Z-0001.msg"
    staged.unlink()

    payload = validate_day(tmp_path, day=datetime(2026, 4, 21)).payload

    assert "Sent ledger count exceeds staged message files present." in payload["errors"]
    assert "Some sent ledger entries reference missing staged files." in payload["warnings"]
    assert payload["apel"]["missing_staged_for_sent"] == ["a" * 32]


def test_validate_day_can_filter_job_data_by_schedd(tmp_path: Path) -> None:
    _setup_day_fixture(tmp_path)

    payload = validate_day(
        tmp_path,
        day=datetime(2026, 4, 21),
        schedd_name="schedd-a.example",
    ).payload

    assert payload["counts"]["canonical_records"] == 2
    assert payload["counts"]["derived_unique_jobs"] == 2
    assert payload["counts"]["apel_sent_ledger_entries"] == 1


def test_validate_day_cli_json_output_shape(tmp_path: Path) -> None:
    _setup_day_fixture(tmp_path)

    result = runner.invoke(
        app,
        [
            "validate-day",
            "--day",
            "2026-04-21",
            "--output-root",
            str(tmp_path),
            "--format",
            "json",
        ],
        terminal_width=160,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["day"] == "2026-04-21"
    assert sorted(payload.keys()) == ["apel", "counts", "day", "errors", "files", "identity_quality", "schedd", "warnings"]
    assert payload["counts"]["derived_unique_jobs"] == 2


def test_validate_day_cli_table_output_is_operator_friendly(tmp_path: Path) -> None:
    _setup_day_fixture(tmp_path)

    result = runner.invoke(
        app,
        [
            "validate-day",
            "--day",
            "2026-04-21",
            "--output-root",
            str(tmp_path),
        ],
        terminal_width=200,
    )

    assert result.exit_code == 0
    assert "Validate Day" in result.stdout
    assert "Files and Records" in result.stdout
    assert "Identity Quality" in result.stdout
    assert "APEL State" in result.stdout
    assert "Warnings and Errors" in result.stdout
