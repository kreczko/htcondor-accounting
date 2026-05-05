from datetime import date

import pytest

from htcondor_accounting.util.dates import affected_months, iter_inclusive_dates


def test_iter_inclusive_dates_includes_start_and_end() -> None:
    assert iter_inclusive_dates(date(2026, 4, 29), date(2026, 5, 2)) == [
        date(2026, 4, 29),
        date(2026, 4, 30),
        date(2026, 5, 1),
        date(2026, 5, 2),
    ]


def test_iter_inclusive_dates_rejects_reversed_range() -> None:
    with pytest.raises(ValueError, match="end date"):
        iter_inclusive_dates(date(2026, 5, 2), date(2026, 4, 29))


def test_affected_months_from_date_range() -> None:
    assert affected_months(date(2026, 4, 28), date(2026, 6, 3)) == [
        (2026, 4),
        (2026, 5),
        (2026, 6),
    ]
