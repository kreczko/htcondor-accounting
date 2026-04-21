from pathlib import Path

from htcondor_accounting.models.canonical import (
    CanonicalJobRecord,
    IdentityInfo,
    JobInfo,
    SourceInfo,
    TimingInfo,
    UsageInfo,
)
from htcondor_accounting.store.jsonl import read_jsonl_zst, write_jsonl_zst


def test_write_and_read_jsonl_zst(tmp_path: Path) -> None:
    output_path = tmp_path / "archive" / "canonical" / "2026" / "04" / "17" / "ce02-test.jsonl.zst"

    record = CanonicalJobRecord(
        site_name="UKI-SOUTHGRID-BRIS-HEP",
        source=SourceInfo(
            schedd="lcgce02.phy.bris.ac.uk",
            collected_at="2026-04-17T12:00:00Z",
        ),
        job=JobInfo(
            global_job_id="lcgce02.phy.bris.ac.uk#684432.0#1776379934",
            owner="na62001",
            local_user="na62001",
        ),
        usage=UsageInfo(
            wall_seconds=42601,
            cpu_user_seconds=40029,
            cpu_sys_seconds=51,
            processors=1,
            memory_real_kb=1268656,
            memory_virtual_kb=1268676,
        ),
        timing=TimingInfo(
            start_time=1776386139,
            end_time=1776428989,
            status_time=1776428989,
        ),
        identity=IdentityInfo(
            dn="/C=UK/O=eScience/OU=Imperial/L=Physics/CN=dirac-pilot.grid.hep.ph.ic.ac.uk",
            token_groups=["/dune", "/dune/pilot"],
        ),
    )

    written = write_jsonl_zst(output_path, [record])
    loaded = list(read_jsonl_zst(output_path))

    assert written == 1
    assert len(loaded) == 1
    assert loaded[0]["site_name"] == "UKI-SOUTHGRID-BRIS-HEP"
    assert loaded[0]["job"]["global_job_id"] == "lcgce02.phy.bris.ac.uk#684432.0#1776379934"
    assert loaded[0]["identity"]["token_groups"] == ["/dune", "/dune/pilot"]
