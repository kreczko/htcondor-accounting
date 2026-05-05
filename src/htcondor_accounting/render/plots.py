from __future__ import annotations

import os
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from htcondor_accounting.report.jobs import accounting_group_key
from htcondor_accounting.store.layout import ensure_parent_dir


_MPLCONFIGDIR = Path(tempfile.gettempdir()) / "htcondor-accounting-matplotlib"
_MPLCONFIGDIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_MPLCONFIGDIR))
os.environ.setdefault("MPLBACKEND", "Agg")

import matplotlib  # noqa: E402

matplotlib.use("Agg")
from matplotlib import pyplot as plt  # noqa: E402


PlotPeriod = Literal["daily", "monthly"]


def _end_datetime_utc(job: dict[str, Any]) -> datetime | None:
    value = job.get("end_time")
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def bucket_wall_hours_by_accounting_group(
    jobs: list[dict[str, Any]],
    *,
    period_type: PlotPeriod,
) -> dict[str, dict[int, float]]:
    """Bucket unscaled wall hours by job completion time in UTC."""
    bucketed: dict[str, dict[int, float]] = defaultdict(lambda: defaultdict(float))

    for job in jobs:
        end_dt = _end_datetime_utc(job)
        if end_dt is None:
            continue
        bucket = end_dt.hour if period_type == "daily" else end_dt.day
        bucketed[accounting_group_key(job)][bucket] += float(job.get("wall_seconds") or 0) / 3600.0

    return {group: dict(values) for group, values in sorted(bucketed.items())}


def write_wall_hours_by_accounting_group_plot(
    jobs: list[dict[str, Any]],
    output_path: Path,
    *,
    period_type: PlotPeriod,
    title: str,
) -> Path:
    bucketed = bucket_wall_hours_by_accounting_group(jobs, period_type=period_type)
    x_values = list(range(24)) if period_type == "daily" else sorted({bucket for values in bucketed.values() for bucket in values})

    if period_type == "monthly" and not x_values:
        x_values = [1]

    ensure_parent_dir(output_path)
    fig, ax = plt.subplots(figsize=(9, 4.8), dpi=120)
    try:
        if bucketed:
            for group, values in bucketed.items():
                ax.plot(x_values, [values.get(bucket, 0.0) for bucket in x_values], marker="o", linewidth=1.5, label=group)
            ax.legend(loc="best", fontsize="small")
        else:
            ax.plot(x_values, [0.0 for _ in x_values], linewidth=1.5, color="#4f6f8f")

        ax.set_title(title)
        ax.set_xlabel("Hour of day (UTC)" if period_type == "daily" else "Day of month (UTC)")
        ax.set_ylabel("Wall hours")
        ax.grid(True, alpha=0.25)
        if period_type == "daily":
            ax.set_xticks([0, 3, 6, 9, 12, 15, 18, 21, 23])
            ax.set_xlim(0, 23)
        else:
            ax.set_xticks(x_values)
        fig.tight_layout()
        fig.savefig(output_path, format="png")
    finally:
        plt.close(fig)

    return output_path
