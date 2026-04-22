from __future__ import annotations

from pathlib import Path
import re
from typing import Any

import tomllib

from htcondor_accounting.config.models import AppConfig


DEFAULT_CONFIG_PATHS = [
    Path("./site.toml"),
    Path("./examples/site-config.toml"),
    Path("/etc/htcondor-accounting/site.toml"),
]


def resolve_config_path(path: Path | None = None) -> Path | None:
    if path is not None:
        return path

    for candidate in DEFAULT_CONFIG_PATHS:
        if candidate.exists():
            return candidate

    return None


def load_config(path: Path | None = None) -> AppConfig:
    resolved_path = resolve_config_path(path)
    if resolved_path is not None:
        text = resolved_path.read_text(encoding="utf-8")
        patched = re.sub(r"=\s*null(\s*(?:#.*)?)$", r'= "__HTCONDOR_ACCOUNTING_NULL__"\1', text, flags=re.MULTILINE)
        data = tomllib.loads(patched)
        data = _replace_null_sentinel(data)
        return AppConfig.model_validate(data)

    return AppConfig()


def _replace_null_sentinel(value: Any) -> Any:
    if value == "__HTCONDOR_ACCOUNTING_NULL__":
        return None
    if isinstance(value, dict):
        return {key: _replace_null_sentinel(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_replace_null_sentinel(item) for item in value]
    return value
