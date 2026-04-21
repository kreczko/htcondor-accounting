from __future__ import annotations

from pydantic import BaseModel, Field


class ExtractManifestFileEntry(BaseModel):
    schedd: str
    source_name: str
    day: str
    path: str
    records: int


class ExtractManifest(BaseModel):
    schema_version: int = 1
    record_type: str = "extract_manifest"
    tool_version: str
    run_stamp: str
    site_name: str
    start: str
    end: str
    constraint: str
    match: int
    schedds: list[str] = Field(default_factory=list)
    output_root: str
    files_written: list[ExtractManifestFileEntry] = Field(default_factory=list)
    total_records: int = 0
    files_written_count: int = 0
    source_config_path: str | None = None
