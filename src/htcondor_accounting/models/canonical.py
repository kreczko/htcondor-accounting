from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

class SourceInfo(BaseModel):
    system: str = Field(default="htcondor")
    schedd: str
    collector_host: Optional[str] = None
    collected_at: str

class JobInfo(BaseModel):
    global_job_id: str
    routed_from_job_id: Optional[str] = None
    owner: str
    local_user: Optional[str] = None

class UsageInfo(BaseModel):
    wall_seconds: int
    cpu_user_seconds: int
    cpu_sys_seconds: int
    processors: int
    memory_real_kb: Optional[int] = None
    memory_virtual_kb: Optional[int] = None

class TimingInfo(BaseModel):
    queue_time: Optional[int] = None
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    status_time: Optional[int] = None

class IdentityInfo(BaseModel):
    dn: Optional[str] = None
    fqan: Optional[str] = None
    vo: Optional[str] = None
    vo_group: Optional[str] = None
    vo_role: Optional[str] = None
    auth_method: Optional[str] = None
    token_issuer: Optional[str] = None
    token_subject: Optional[str] = None
    token_groups: list[str] = Field(default_factory=list)
    x509_email: Optional[str] = None
    orig_dn: Optional[str] = None
    orig_fqan: Optional[str] = None
    orig_vo_name: Optional[str] = None
    orig_fqan_list: Optional[str] = None

class AccountingInfo(BaseModel):
    acct_group: Optional[str] = None
    acct_group_user: Optional[str] = None
    accounting_group: Optional[str] = None
    route_name: Optional[str] = None
    last_match_name: Optional[str] = None
    last_job_router_name: Optional[str] = None

class ResolvedIdentityInfo(BaseModel):
    vo: Optional[str] = None
    vo_group: Optional[str] = None
    vo_role: Optional[str] = None
    fqan: Optional[str] = None
    resolution_method: str = "unresolved"
    resolution_evidence: Optional[str] = None
    is_pilot: Optional[bool] = None

class BenchmarkInfo(BaseModel):
    benchmark_type: Optional[str] = None
    site_baseline_per_core: Optional[float] = None
    node_per_core: Optional[float] = None
    scale_factor: Optional[float] = None

class ExecutionInfo(BaseModel):
    ce_host: Optional[str] = None
    ce_id: Optional[str] = None
    execute_node: Optional[str] = None
    slot_name: Optional[str] = None

class CanonicalJobRecord(BaseModel):
    schema_version: int = 1
    record_type: str = "job"
    site_name: str
    source: SourceInfo
    job: JobInfo
    usage: UsageInfo
    timing: TimingInfo
    identity: IdentityInfo = Field(default_factory=IdentityInfo)
    accounting: AccountingInfo = Field(default_factory=AccountingInfo)
    resolved_identity: ResolvedIdentityInfo = Field(default_factory=ResolvedIdentityInfo)
    benchmark: BenchmarkInfo = Field(default_factory=BenchmarkInfo)
    execution: ExecutionInfo = Field(default_factory=ExecutionInfo)
