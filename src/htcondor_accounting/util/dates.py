from __future__ import annotations

from datetime import date, timedelta


def month_label(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def iter_days_in_month(year: int, month: int) -> list[date]:
    first = date(year, month, 1)
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)

    current = first
    days: list[date] = []
    while current < next_month:
        days.append(current)
        current += timedelta(days=1)
    return days


def month_output_parts(year: int, month: int) -> tuple[str, str]:
    return f"{year:04d}", f"{month:02d}"
