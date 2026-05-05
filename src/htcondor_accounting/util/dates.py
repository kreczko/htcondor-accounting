from __future__ import annotations

from datetime import date, datetime, timedelta, timezone


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


def iter_inclusive_dates(start: date, end: date) -> list[date]:
    """Return all dates from start through end, inclusive."""
    if end < start:
        raise ValueError("end date must be on or after start date")

    current = start
    days: list[date] = []
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def affected_months(start: date, end: date) -> list[tuple[int, int]]:
    """Return distinct (year, month) pairs touched by an inclusive date range."""
    return sorted({(day.year, day.month) for day in iter_inclusive_dates(start, end)})


def yesterday_utc() -> date:
    """Return yesterday's date in UTC."""
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def month_output_parts(year: int, month: int) -> tuple[str, str]:
    return f"{year:04d}", f"{month:02d}"
