from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from htcondor_accounting.config.models import ApelConfig


APEL_FIELD_ORDER = [
    "Site",
    "SubmitHost",
    "MachineName",
    "Queue",
    "LocalJobId",
    "LocalUserId",
    "GlobalUserName",
    "FQAN",
    "VO",
    "VOGroup",
    "VORole",
    "WallDuration",
    "CpuDuration",
    "Processors",
    "NodeCount",
    "StartTime",
    "EndTime",
    "InfrastructureDescription",
    "InfrastructureType",
    "MemoryReal",
    "MemoryVirtual",
    "ServiceLevelType",
    "ServiceLevel",
]


def _parse_local_job_id(global_job_id: str | None) -> str:
    if not global_job_id:
        return "-"
    parts = global_job_id.split("#")
    if len(parts) >= 2 and parts[1]:
        return parts[1]
    return global_job_id


def _format_fqan(record: dict[str, Any]) -> str:
    if record.get("fqan"):
        return str(record["fqan"])

    vo_group = record.get("vo_group")
    vo = record.get("vo")
    vo_role = record.get("vo_role")

    if vo_group:
        fqan = str(vo_group)
    elif vo:
        fqan = f"/{vo}"
    else:
        fqan = ""

    if vo_role:
        fqan = f"{fqan}/{vo_role}/Capability=NULL"

    return fqan or "-"


def _format_timestamp(value: Any) -> str:
    if value is None:
        return "-"
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return str(value)


def _service_level(record: dict[str, Any], config: ApelConfig) -> str:
    value = record.get("scale_factor")
    if value is None:
        value = config.service_level_value
    return str(value)


def build_apel_record_fields(record: dict[str, Any], config: ApelConfig) -> dict[str, str]:
    local_user = str(record.get("local_user") or record.get("owner") or "-")
    return {
        "Site": str(record.get("site_name") or "-"),
        "SubmitHost": config.submit_host,
        "MachineName": config.machine_name,
        "Queue": config.queue_name,
        "LocalJobId": _parse_local_job_id(record.get("global_job_id")),
        "LocalUserId": local_user,
        "GlobalUserName": local_user,
        "FQAN": _format_fqan(record),
        "VO": str(record.get("vo") or "-"),
        "VOGroup": str(record.get("vo_group") or "-"),
        "VORole": str(record.get("vo_role") or "-"),
        "WallDuration": str(int(record.get("wall_seconds") or 0)),
        "CpuDuration": str(int(record.get("cpu_total_seconds") or 0)),
        "Processors": str(int(record.get("processors") or 1)),
        "NodeCount": "1",
        "StartTime": _format_timestamp(record.get("start_time")),
        "EndTime": _format_timestamp(record.get("end_time")),
        "InfrastructureDescription": config.infrastructure_description,
        "InfrastructureType": config.infrastructure_type,
        "MemoryReal": str(record.get("memory_real_kb") or 0),
        "MemoryVirtual": str(record.get("memory_virtual_kb") or 0),
        "ServiceLevelType": config.service_level_type,
        "ServiceLevel": _service_level(record, config),
    }


def apel_record_text(record: dict[str, Any], config: ApelConfig) -> str:
    fields = build_apel_record_fields(record, config)
    lines = ["%%"]
    lines.extend(f"{key}: {fields[key]}" for key in APEL_FIELD_ORDER)
    return "\n".join(lines) + "\n"
