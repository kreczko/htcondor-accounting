from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterable

from htcondor_accounting.store.layout import ensure_parent_dir


def _row_as_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "model_dump"):
        return row.model_dump(mode="json")
    return dict(row)


def write_csv_rows(path: Path, rows: Iterable[Any], fieldnames: list[str]) -> None:
    ensure_parent_dir(path)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(_row_as_dict(row))
