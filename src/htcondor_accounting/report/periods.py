from __future__ import annotations

from datetime import date, datetime


def parse_day(value: str) -> date:
    return datetime.fromisoformat(value).date()


def iso_week_parts(day: date) -> tuple[int, int]:
    iso_year, iso_week, _ = day.isocalendar()
    return iso_year, iso_week


def week_period(day: date) -> str:
    iso_year, iso_week = iso_week_parts(day)
    return f"{iso_year:04d}-W{iso_week:02d}"


def month_period(day: date) -> str:
    return f"{day.year:04d}-{day.month:02d}"


def year_period(day: date) -> str:
    return f"{day.year:04d}"
