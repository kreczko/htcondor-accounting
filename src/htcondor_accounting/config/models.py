from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field


class SiteConfig(BaseModel):
    name: str = "UNKNOWN"
    timezone: str = "UTC"


class StorageConfig(BaseModel):
    root: Path = Path("./archive")


class BenchmarkConfig(BaseModel):
    type: str = "hepscore23"
    baseline_per_core: float = 1.0


class ExtractConfig(BaseModel):
    default_match: int = 100
    default_schedds: list[str] = Field(default_factory=list)


class IdentityConfig(BaseModel):
    prefer_x509_fqan: bool = True
    use_token_groups: bool = True


class ApelConfig(BaseModel):
    enabled: bool = False
    ce_id: str | None = None
    outgoing_dir: Path = Path("/var/spool/apel/outgoing")


class ReportingConfig(BaseModel):
    output_dir: Path = Path("./output")
    publish_html: bool = True


class AppConfig(BaseModel):
    site: SiteConfig = Field(default_factory=SiteConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    benchmark: BenchmarkConfig = Field(default_factory=BenchmarkConfig)
    extract: ExtractConfig = Field(default_factory=ExtractConfig)
    identity: IdentityConfig = Field(default_factory=IdentityConfig)
    apel: ApelConfig = Field(default_factory=ApelConfig)
    reporting: ReportingConfig = Field(default_factory=ReportingConfig)
