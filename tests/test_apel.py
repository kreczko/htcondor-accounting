import json
from datetime import datetime, timezone
from pathlib import Path

from htcondor_accounting.config.models import ApelConfig
from htcondor_accounting.export.apel_messages import export_apel_daily, pack_apel_messages
from htcondor_accounting.export.apel_records import apel_record_text
from htcondor_accounting.store.jsonl import read_jsonl_zst, write_jsonl_zst
from htcondor_accounting.store.layout import RunStamp


def _derived_job(global_job_id: str, *, local_user: str, vo: str, wall_seconds: int, cpu_total_seconds: int, scale_factor: float | None = 2.0) -> dict:
    return {
        "schema_version": 1,
        "record_type": "report_job",
        "site_name": "UKI-SOUTHGRID-BRIS-HEP",
        "global_job_id": global_job_id,
        "owner": local_user,
        "local_user": local_user,
        "vo": vo,
        "vo_group": f"/{vo}",
        "vo_role": None,
        "auth_method": "scitoken",
        "start_time": 1776386139,
        "end_time": 1776428989,
        "wall_seconds": wall_seconds,
        "cpu_user_seconds": cpu_total_seconds - 1,
        "cpu_sys_seconds": 1,
        "cpu_total_seconds": cpu_total_seconds,
        "processors": 1,
        "memory_real_kb": 1000,
        "memory_virtual_kb": 2000,
        "scale_factor": scale_factor,
        "benchmark_type": "hepscore23",
        "source_schedd": "lcgce02.phy.bris.ac.uk",
        "day": "2026-04-17",
    }


def _apel_config(tmp_path: Path, *, soft: int = 800000, hard: int = 1000000) -> ApelConfig:
    return ApelConfig(
        enabled=True,
        ce_id="ce.example:9619/condor",
        submit_host="submit.example",
        machine_name="worker.example",
        queue_name="condor",
        infrastructure_description="APEL-HTCondor",
        infrastructure_type="grid",
        service_level_type="hepscore23",
        service_level_value=20.0,
        staging_dir=Path("apel/staging"),
        outgoing_dir=tmp_path / "outgoing",
        message_soft_limit_bytes=soft,
        message_hard_limit_bytes=hard,
    )


def test_apel_record_text_has_expected_fields(tmp_path: Path) -> None:
    text = apel_record_text(_derived_job("host#123.0#999", local_user="alice", vo="atlas", wall_seconds=10, cpu_total_seconds=7), _apel_config(tmp_path))

    assert text.startswith("%%\n")
    assert "Site: UKI-SOUTHGRID-BRIS-HEP" in text
    assert "SubmitHost: submit.example" in text
    assert "LocalJobId: 123.0" in text
    assert "LocalUserId: alice" in text
    assert "VO: atlas" in text
    assert "WallDuration: 10" in text
    assert "CpuDuration: 7" in text
    assert "ServiceLevelType: hepscore23" in text


def test_pack_apel_messages_chunks_deterministically(tmp_path: Path) -> None:
    config = _apel_config(tmp_path, soft=400, hard=1000)
    texts = [
        apel_record_text(_derived_job(f"host#{index}.0#999", local_user=f"user{index}", vo="atlas", wall_seconds=10, cpu_total_seconds=7), config)
        for index in range(1, 4)
    ]

    chunks = pack_apel_messages(texts, config.message_soft_limit_bytes, config.message_hard_limit_bytes)

    assert len(chunks) >= 2
    assert chunks[0].body.startswith("%%\n")
    assert all(chunk.bytes <= config.message_hard_limit_bytes for chunk in chunks)
    assert chunks[0].records + chunks[1].records <= 3


def test_export_apel_daily_writes_messages_and_manifest(tmp_path: Path) -> None:
    output_root = tmp_path / "archive"
    jobs_path = output_root / "derived" / "daily" / "2026" / "04" / "17" / "jobs.jsonl.zst"
    write_jsonl_zst(
        jobs_path,
        [
            _derived_job("host#1.0#999", local_user="alice", vo="atlas", wall_seconds=10, cpu_total_seconds=7),
            _derived_job("host#2.0#999", local_user="bob", vo="cms", wall_seconds=20, cpu_total_seconds=11),
        ],
    )

    result = export_apel_daily(
        output_root,
        datetime(2026, 4, 17, tzinfo=timezone.utc),
        _apel_config(tmp_path, soft=350, hard=1000),
        RunStamp(datetime(2026, 4, 21, 12, 30, 38, tzinfo=timezone.utc)),
    )

    assert result.jobs_seen == 2
    assert result.messages_written >= 1
    assert result.manifest_path.exists()

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["record_type"] == "apel_export_manifest"
    assert manifest["day"] == "2026-04-17"
    assert manifest["jobs_seen"] == 2
    assert manifest["messages_written"] == result.messages_written
    assert manifest["soft_limit_bytes"] == 350
    assert all(entry["bytes"] <= 1000 for entry in manifest["files_written"])
    assert all(Path(entry["path"]).exists() for entry in manifest["files_written"])

    for entry in manifest["files_written"]:
        body = Path(entry["path"]).read_text(encoding="utf-8")
        assert body.startswith("%%\n")
        assert len(body.encode("utf-8")) == entry["bytes"]
