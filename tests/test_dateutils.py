import datetime
import functools
from typing import Any

import pytest

from hyperion.dateutils import (
    TimeResolution,
    TimeResolutionUnit,
    assure_timezone,
    iter_dates_between,
    iter_intervals,
    quantize_datetime,
    truncate_datetime,
)

# ignore missing timzeone for this file
# ruff: noqa: DTZ001


dt = functools.partial(datetime.datetime, tzinfo=datetime.timezone.utc)


@pytest.mark.parametrize(
    ("expr", "resolution"),
    [
        ("1d", TimeResolution(1, "d")),
        ("5s", TimeResolution(5, "s")),
        ("12m", TimeResolution(12, "m")),
        ("1y", TimeResolution(1, "y")),
        ("6M", TimeResolution(6, "M")),
        ("1w", TimeResolution(1, "w")),
    ],
)
def test_time_resolution_good_expression(expr: str, resolution: TimeResolution) -> None:
    parsed = TimeResolution.from_str(expr)
    assert parsed == resolution
    assert expr == repr(parsed)


@pytest.mark.parametrize(
    "expr",
    [
        "1k",
        "this is wrong",
        "kd",
        "?d",
        "  ",
    ],
)
def test_time_resolution_wrong_expression(expr: str) -> None:
    with pytest.raises(ValueError, match=r"Invalid time resolution specification '.*'."):
        TimeResolution.from_str(expr)


@pytest.mark.parametrize(
    ("value", "unit", "resolution"),
    [
        ("12", "d", TimeResolution(12, "d")),
        (12, "d", TimeResolution("12", "d")),  # type: ignore[arg-type]
        (7, "y", TimeResolution(7, "y")),
    ],
)
def test_time_resolution_good_args(value: int, unit: TimeResolutionUnit, resolution: TimeResolution) -> None:
    assert TimeResolution(value, unit) == resolution


@pytest.mark.parametrize(
    ("value", "unit", "error"),
    [
        ("1", "suck my unit", "Unknown time unit 'suck my unit'. Pick one of.*"),
        ("you have no value", "d", "invalid literal for int.. with base 10.*"),
    ],
)
def test_time_resolution_wrong_args(value: Any, unit: Any, error: str) -> None:
    with pytest.raises(ValueError, match=error):
        TimeResolution(value, unit)


@pytest.mark.parametrize(
    ("base", "unit", "expected"),
    [
        (dt(2023, 10, 5, 14, 30, 45, 123456), "s", dt(2023, 10, 5, 14, 30, 45)),
        (dt(2023, 10, 5, 14, 30, 45, 123456), "m", dt(2023, 10, 5, 14, 30)),
        (dt(2023, 10, 5, 14, 30, 45, 123456), "h", dt(2023, 10, 5, 14)),
        (dt(2023, 10, 5, 14, 30, 45, 123456), "d", dt(2023, 10, 5)),
        (dt(2023, 10, 5, 14, 30, 45, 123456), "w", dt(2023, 10, 2)),
        (dt(2023, 10, 5, 14, 30, 45, 123456), "M", dt(2023, 10, 1)),
        (dt(2023, 10, 5, 14, 30, 45, 123456), "y", dt(2023, 1, 1)),
    ],
)
def test_truncate_datetime(base: datetime.datetime, unit: TimeResolutionUnit, expected: datetime.datetime) -> None:
    assert truncate_datetime(base, unit) == expected


@pytest.mark.parametrize(
    "unit",
    [
        "unknown",
        "invalid",
        "x",
    ],
)
def test_truncate_datetime_invalid_unit(unit: Any) -> None:
    with pytest.raises(ValueError, match=rf"Unknown time unit '{unit}'. Pick one of.*"):
        truncate_datetime(dt(2023, 10, 5, 14, 30, 45, 123456), unit)


@pytest.mark.parametrize(
    ("base", "resolution", "expected"),
    [
        (dt(2023, 10, 5, 14, 30, 45), "5s", dt(2023, 10, 5, 14, 30, 50)),
        (dt(2023, 10, 5, 14, 30, 45), "15m", dt(2023, 10, 5, 14, 45)),
        (dt(2023, 10, 5, 14, 30, 45), "1h", dt(2023, 10, 5, 15, 0)),
        (dt(2023, 10, 5, 14, 30, 45), "1d", dt(2023, 10, 6)),
        (dt(2023, 10, 5, 14, 30, 45), "1w", dt(2023, 10, 9)),
        (dt(2023, 10, 5, 14, 30, 45), "1M", dt(2023, 11, 1)),
        (dt(2023, 10, 5, 14, 30, 45), "1y", dt(2024, 1, 1)),
    ],
)
def test_quantize_datetime(base: datetime.datetime, resolution: str, expected: datetime.datetime) -> None:
    assert quantize_datetime(base, resolution) == expected


@pytest.mark.parametrize(
    ("base", "resolution", "expected"),
    [
        (dt(2023, 10, 5, 14, 30, 45), TimeResolution(5, "s"), dt(2023, 10, 5, 14, 30, 50)),
        (dt(2023, 10, 5, 14, 30, 45), TimeResolution(15, "m"), dt(2023, 10, 5, 14, 45)),
        (dt(2023, 10, 5, 14, 30, 45), TimeResolution(1, "h"), dt(2023, 10, 5, 15, 0)),
        (dt(2023, 10, 5, 14, 30, 45), TimeResolution(1, "d"), dt(2023, 10, 6)),
        (dt(2023, 10, 5, 14, 30, 45), TimeResolution(1, "w"), dt(2023, 10, 9)),
        (dt(2023, 10, 5, 14, 30, 45), TimeResolution(1, "M"), dt(2023, 11, 1)),
        (dt(2023, 10, 5, 14, 30, 45), TimeResolution(1, "y"), dt(2024, 1, 1)),
    ],
)
def test_quantize_datetime_with_timeresolution(
    base: datetime.datetime, resolution: TimeResolution, expected: datetime.datetime
) -> None:
    assert quantize_datetime(base, resolution) == expected


@pytest.mark.parametrize(
    ("base", "resolution", "error"),
    [
        (
            dt(2023, 10, 5, 14, 30, 45),
            "1x",
            "Invalid time resolution specification '1x'. Use expressions such as 1d, 5s or 3M.",
        ),
        (
            dt(2023, 10, 5, 14, 30, 45),
            "5k",
            "Invalid time resolution specification '5k'. Use expressions such as 1d, 5s or 3M.",
        ),
    ],
)
def test_quantize_datetime_invalid_resolution(base: datetime.datetime, resolution: str, error: str) -> None:
    with pytest.raises(ValueError, match=error):
        quantize_datetime(base, resolution)


@pytest.mark.parametrize(
    ("start_date", "end_date", "granularity", "expected"),
    [
        (
            dt(2024, 1, 1),
            dt(2024, 1, 10),
            "d",
            [dt(2024, 1, d) for d in range(1, 11)],
        ),
        (
            dt(2024, 1, 1, 10, 12),
            dt(2024, 1, 10, 10, 12),
            "d",
            [dt(2024, 1, d, 10, 12) for d in range(1, 11)],
        ),
        (
            dt(2024, 1, 1),
            dt(2024, 5, 10),
            "M",
            [dt(2024, 1, 1), dt(2024, 2, 1), dt(2024, 3, 1), dt(2024, 4, 1), dt(2024, 5, 1)],
        ),
        (
            dt(2024, 10, 11),
            dt(2025, 3, 5),
            "M",
            [dt(2024, 10, 11), dt(2024, 11, 11), dt(2024, 12, 11), dt(2025, 1, 11), dt(2025, 2, 11)],
        ),
    ],
)
def test_iter_datetimes_between(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    granularity: TimeResolutionUnit,
    expected: list[datetime.datetime],
) -> None:
    assert list(iter_dates_between(start_date, end_date, granularity)) == expected


@pytest.mark.parametrize(
    ("base", "expected"),
    [
        (datetime.date(2024, 1, 1), dt(2024, 1, 1)),
        (datetime.datetime(2024, 1, 2, 3, 4, 5), dt(2024, 1, 2, 3, 4, 5)),
        (
            datetime.datetime(2024, 1, 1, 10, tzinfo=datetime.timezone(datetime.timedelta(hours=1))),
            dt(2024, 1, 1, 9),
        ),
        (
            dt(2024, 1, 1),
            dt(2024, 1, 1),
        ),
    ],
)
def test_assure_timezone(base: datetime.datetime | datetime.date, expected: datetime.datetime) -> None:
    assert assure_timezone(base) == expected


def test_iter_daily_intervals() -> None:
    intervals = list(iter_intervals(dt(2024, 1, 1), dt(2024, 1, 5), "d"))
    assert intervals == [
        (dt(2024, 1, 1), dt(2024, 1, 2)),
        (dt(2024, 1, 2), dt(2024, 1, 3)),
        (dt(2024, 1, 3), dt(2024, 1, 4)),
        (dt(2024, 1, 4), dt(2024, 1, 5)),
    ]


def test_iter_hourly_intervals() -> None:
    intervals = list(iter_intervals(dt(2024, 1, 1), dt(2024, 1, 1, 6, 30), "h"))
    assert intervals == [
        (dt(2024, 1, 1, 0, 0), dt(2024, 1, 1, 1, 0)),
        (dt(2024, 1, 1, 1, 0), dt(2024, 1, 1, 2, 0)),
        (dt(2024, 1, 1, 2, 0), dt(2024, 1, 1, 3, 0)),
        (dt(2024, 1, 1, 3, 0), dt(2024, 1, 1, 4, 0)),
        (dt(2024, 1, 1, 4, 0), dt(2024, 1, 1, 5, 0)),
        (dt(2024, 1, 1, 5, 0), dt(2024, 1, 1, 6, 0)),
        (dt(2024, 1, 1, 6, 0), dt(2024, 1, 1, 6, 30)),
    ]


def test_iter_intervals_end_before_start() -> None:
    with pytest.raises(ValueError, match=r"Start date cannot be later than end date."):
        list(iter_intervals(dt(2025, 1, 1), dt(2024, 1, 1), "M"))
