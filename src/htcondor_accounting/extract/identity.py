from __future__ import annotations

from typing import Any, Callable

from htcondor_accounting.models.canonical import AccountingInfo, IdentityInfo, ResolvedIdentityInfo


def _ad_str(ad: dict[str, Any], key: str) -> str | None:
    value = ad.get(key)
    if value is None:
        return None
    return str(value)


def ad_token_groups(ad: dict[str, Any]) -> list[str]:
    value = ad.get("orig_AuthTokenGroups")
    if value is None:
        return []
    if isinstance(value, str):
        return [group.strip() for group in value.split(",") if group.strip()]
    return []


def _has_x509_evidence(ad: dict[str, Any]) -> bool:
    keys = [
        "x509UserProxySubject",
        "x509userproxysubject",
        "x509UserProxyFirstFQAN",
        "x509UserProxyVOName",
        "x509UserProxyFQAN",
        "x509UserProxyEmail",
        "orig_x509UserProxyFirstFQAN",
        "orig_x509UserProxyVOName",
        "orig_x509UserProxyFQAN",
        "orig_x509userproxysubject",
    ]
    return any(ad.get(key) for key in keys)


def detect_auth_method(ad: dict[str, Any]) -> str:
    if ad.get("orig_AuthTokenIssuer") or ad.get("orig_AuthTokenSubject"):
        return "scitoken"
    if _has_x509_evidence(ad):
        return "x509"
    return "local"


def _parse_fqan(value: str | None) -> tuple[str | None, str | None, str | None]:
    if not value:
        return None, None, None

    parts = [part for part in value.split("/") if part]
    if not parts:
        return None, None, None

    vo = parts[0]
    role = next((part for part in parts if part.startswith("Role=")), None)
    return vo, f"/{vo}", role


def construct_fqan(vo: str | None, role: str | None) -> str | None:
    if not vo:
        return None
    if role:
        return f"/{vo}/{role}/Capability=NULL"
    return f"/{vo}"


def extract_raw_identity(ad: dict[str, Any]) -> IdentityInfo:
    raw_fqan = _ad_str(ad, "x509UserProxyFirstFQAN")
    raw_vo, raw_vo_group, raw_vo_role = _parse_fqan(raw_fqan)
    raw_vo_name = _ad_str(ad, "x509UserProxyVOName")
    if raw_vo is None and raw_vo_name:
        raw_vo = raw_vo_name
        raw_vo_group = f"/{raw_vo_name}"

    return IdentityInfo(
        dn=_ad_str(ad, "x509UserProxySubject") or _ad_str(ad, "x509userproxysubject"),
        fqan=raw_fqan,
        vo=raw_vo,
        vo_group=raw_vo_group,
        vo_role=raw_vo_role,
        auth_method=detect_auth_method(ad),
        token_issuer=_ad_str(ad, "orig_AuthTokenIssuer"),
        token_subject=_ad_str(ad, "orig_AuthTokenSubject"),
        token_groups=ad_token_groups(ad),
        x509_email=_ad_str(ad, "x509UserProxyEmail"),
        orig_dn=_ad_str(ad, "orig_x509userproxysubject"),
        orig_fqan=_ad_str(ad, "orig_x509UserProxyFirstFQAN"),
        orig_vo_name=_ad_str(ad, "orig_x509UserProxyVOName"),
        orig_fqan_list=_ad_str(ad, "orig_x509UserProxyFQAN"),
    )


def _role_from_strings(*values: str | None) -> str | None:
    for value in values:
        if value and "sgm" in value.lower():
            return "Role=admin"
    for value in values:
        if value and ("pilot" in value.lower() or "pil" in value.lower()):
            return "Role=pilot"
    return None


def _infer_vo_from_text(value: str) -> str | None:
    lowered = value.lower()
    mappings = [
        ("lb.pilot", "lhcb"),
        ("lbpilot", "lhcb"),
        ("lhcb pilot", "lhcb"),
        ("cmspilot", "cms"),
        ("cms pilot", "cms"),
        ("cmssgm", "cms"),
        ("cmspil", "cms"),
        ("cms", "cms"),
        ("lhcb", "lhcb"),
        ("dune", "dune"),
        ("na62", "na62.vo.gridpp.ac.uk"),
        ("euclid", "eucliduk.net"),
        ("mu3e", "mu3e.org"),
        ("alice", "alice"),
        ("atlas", "atlas"),
    ]
    for needle, vo in mappings:
        if needle in lowered:
            return vo
    return None


def _build_resolved(
    vo: str | None,
    role: str | None,
    method: str,
    evidence: str | None,
    *,
    fqan: str | None = None,
) -> ResolvedIdentityInfo:
    return ResolvedIdentityInfo(
        vo=vo,
        vo_group=f"/{vo}" if vo else None,
        vo_role=role,
        fqan=fqan if vo else None,
        resolution_method=method,
        resolution_evidence=evidence,
        is_pilot=role == "Role=pilot" if role else None,
    )


def _resolve_from_fqan_value(value: str | None, method: str) -> ResolvedIdentityInfo | None:
    vo, _, role = _parse_fqan(value)
    if not vo:
        return None
    return _build_resolved(vo, role, method, value, fqan=value)


def _resolve_from_token_groups(raw_identity: IdentityInfo) -> tuple[str | None, str | None]:
    if not raw_identity.token_groups:
        return None, None
    first_group = raw_identity.token_groups[0]
    parts = [part for part in first_group.split("/") if part]
    if not parts:
        return None, None
    return parts[0], first_group


def _resolve_from_vo_name(value: str | None, method: str) -> ResolvedIdentityInfo | None:
    if not value:
        return None
    return _build_resolved(value, None, method, value, fqan=construct_fqan(value, None))


def _resolve_from_email(value: str | None) -> ResolvedIdentityInfo | None:
    if not value:
        return None
    vo = _infer_vo_from_text(value)
    if not vo:
        return None
    role = _role_from_strings(value)
    return _build_resolved(vo, role, "x509_email", value, fqan=construct_fqan(vo, role))


def _resolve_from_subject(value: str | None) -> ResolvedIdentityInfo | None:
    if not value:
        return None
    vo = _infer_vo_from_text(value)
    if not vo:
        return None
    role = _role_from_strings(value)
    return _build_resolved(vo, role, "x509_subject", value, fqan=construct_fqan(vo, role))


def _resolve_from_accounting(accounting: AccountingInfo) -> tuple[str | None, str | None, str | None]:
    candidates = [
        ("acct_group", accounting.acct_group),
        ("acct_group_user", accounting.acct_group_user),
        ("accounting_group", accounting.accounting_group),
        ("route_name", accounting.route_name),
    ]
    for method, value in candidates:
        if not value:
            continue
        vo = _infer_vo_from_text(value)
        if vo:
            return vo, method, value
    return None, None, None


def _resolve_from_owner(owner: str | None) -> tuple[str | None, str | None]:
    if not owner:
        return None, None
    vo = _infer_vo_from_text(owner)
    if not vo:
        return None, None
    return vo, owner


def _fallback_role(
    raw_identity: IdentityInfo,
    accounting: AccountingInfo,
    owner: str | None,
) -> str | None:
    email_or_subject_role = _role_from_strings(
        raw_identity.x509_email,
        raw_identity.orig_dn,
        raw_identity.dn,
    )
    if email_or_subject_role:
        return email_or_subject_role

    return _role_from_strings(
        owner,
        accounting.acct_group_user,
        accounting.acct_group,
        accounting.route_name,
    )


def resolve_reporting_identity(
    raw_identity: IdentityInfo,
    accounting: AccountingInfo,
    owner: str | None = None,
) -> ResolvedIdentityInfo:
    resolvers: list[Callable[[], ResolvedIdentityInfo | None]] = [
        lambda: _resolve_from_fqan_value(raw_identity.orig_fqan, "orig_x509_first_fqan"),
        lambda: _resolve_from_fqan_value(raw_identity.fqan, "x509_first_fqan"),
        lambda: (
            _build_resolved(
                token_vo,
                _fallback_role(raw_identity, accounting, owner),
                "token_groups",
                token_evidence,
                fqan=construct_fqan(token_vo, _fallback_role(raw_identity, accounting, owner)),
            )
            if token_vo
            else None
        ),
        lambda: (
            _build_resolved(
                raw_identity.orig_vo_name,
                _fallback_role(raw_identity, accounting, owner),
                "orig_x509_vo_name",
                raw_identity.orig_vo_name,
                fqan=construct_fqan(raw_identity.orig_vo_name, _fallback_role(raw_identity, accounting, owner)),
            )
            if raw_identity.orig_vo_name
            else None
        ),
        lambda: (
            _build_resolved(
                raw_identity.vo,
                _fallback_role(raw_identity, accounting, owner),
                "x509_vo_name",
                raw_identity.vo,
                fqan=construct_fqan(raw_identity.vo, _fallback_role(raw_identity, accounting, owner)),
            )
            if raw_identity.vo and raw_identity.fqan is None
            else None
        ),
        lambda: _resolve_from_email(raw_identity.x509_email),
        lambda: _resolve_from_subject(raw_identity.orig_dn or raw_identity.dn),
        lambda: (
            _build_resolved(
                accounting_vo,
                _fallback_role(raw_identity, accounting, owner),
                accounting_method or "acct_group",
                accounting_evidence,
                fqan=construct_fqan(accounting_vo, _fallback_role(raw_identity, accounting, owner)),
            )
            if accounting_vo
            else None
        ),
        lambda: (
            _build_resolved(
                owner_vo,
                _fallback_role(raw_identity, accounting, owner),
                "owner_heuristic",
                owner_evidence,
                fqan=construct_fqan(owner_vo, _fallback_role(raw_identity, accounting, owner)),
            )
            if owner_vo
            else None
        ),
    ]

    token_vo, token_evidence = _resolve_from_token_groups(raw_identity)
    accounting_vo, accounting_method, accounting_evidence = _resolve_from_accounting(accounting)
    owner_vo, owner_evidence = _resolve_from_owner(owner)

    for resolver in resolvers:
        resolved = resolver()
        if resolved is not None:
            return resolved

    role = _fallback_role(raw_identity, accounting, owner)
    return ResolvedIdentityInfo(
        vo=None,
        vo_group=None,
        vo_role=role,
        fqan=None,
        resolution_method="unresolved",
        resolution_evidence=None,
        is_pilot=role == "Role=pilot" if role else None,
    )
