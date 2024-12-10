import datetime
from typing import Any

import pytest

from hyperion.dateutils import (
    TimeResolution,
    TimeResolutionUnit,
    iter_dates_between,
    quantize_datetime,
    truncate_datetime,
)


def utc_datetime(*args: Any, **kwargs: Any) -> datetime.datetime:
    return datetime.datetime(*args, **kwargs, tzinfo=datetime.timezone.utc)  # type: ignore[misc]


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
    with pytest.raises(ValueError, match="Invalid time resolution specification '.*'."):
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
        (utc_datetime(2023, 10, 5, 14, 30, 45, 123456), "s", utc_datetime(2023, 10, 5, 14, 30, 45)),
        (utc_datetime(2023, 10, 5, 14, 30, 45, 123456), "m", utc_datetime(2023, 10, 5, 14, 30)),
        (utc_datetime(2023, 10, 5, 14, 30, 45, 123456), "h", utc_datetime(2023, 10, 5, 14)),
        (utc_datetime(2023, 10, 5, 14, 30, 45, 123456), "d", utc_datetime(2023, 10, 5)),
        (utc_datetime(2023, 10, 5, 14, 30, 45, 123456), "w", utc_datetime(2023, 10, 2)),
        (utc_datetime(2023, 10, 5, 14, 30, 45, 123456), "M", utc_datetime(2023, 10, 1)),
        (utc_datetime(2023, 10, 5, 14, 30, 45, 123456), "y", utc_datetime(2023, 1, 1)),
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
    with pytest.raises(ValueError, match=f"Unknown time unit '{unit}'. Pick one of.*"):
        truncate_datetime(utc_datetime(2023, 10, 5, 14, 30, 45, 123456), unit)


@pytest.mark.parametrize(
    ("base", "resolution", "expected"),
    [
        (utc_datetime(2023, 10, 5, 14, 30, 45), "5s", utc_datetime(2023, 10, 5, 14, 30, 50)),
        (utc_datetime(2023, 10, 5, 14, 30, 45), "15m", utc_datetime(2023, 10, 5, 14, 45)),
        (utc_datetime(2023, 10, 5, 14, 30, 45), "1h", utc_datetime(2023, 10, 5, 15, 0)),
        (utc_datetime(2023, 10, 5, 14, 30, 45), "1d", utc_datetime(2023, 10, 6)),
        (utc_datetime(2023, 10, 5, 14, 30, 45), "1w", utc_datetime(2023, 10, 9)),
        (utc_datetime(2023, 10, 5, 14, 30, 45), "1M", utc_datetime(2023, 11, 1)),
        (utc_datetime(2023, 10, 5, 14, 30, 45), "1y", utc_datetime(2024, 1, 1)),
    ],
)
def test_quantize_datetime(base: datetime.datetime, resolution: str, expected: datetime.datetime) -> None:
    assert quantize_datetime(base, resolution) == expected


@pytest.mark.parametrize(
    ("base", "resolution", "expected"),
    [
        (utc_datetime(2023, 10, 5, 14, 30, 45), TimeResolution(5, "s"), utc_datetime(2023, 10, 5, 14, 30, 50)),
        (utc_datetime(2023, 10, 5, 14, 30, 45), TimeResolution(15, "m"), utc_datetime(2023, 10, 5, 14, 45)),
        (utc_datetime(2023, 10, 5, 14, 30, 45), TimeResolution(1, "h"), utc_datetime(2023, 10, 5, 15, 0)),
        (utc_datetime(2023, 10, 5, 14, 30, 45), TimeResolution(1, "d"), utc_datetime(2023, 10, 6)),
        (utc_datetime(2023, 10, 5, 14, 30, 45), TimeResolution(1, "w"), utc_datetime(2023, 10, 9)),
        (utc_datetime(2023, 10, 5, 14, 30, 45), TimeResolution(1, "M"), utc_datetime(2023, 11, 1)),
        (utc_datetime(2023, 10, 5, 14, 30, 45), TimeResolution(1, "y"), utc_datetime(2024, 1, 1)),
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
            utc_datetime(2023, 10, 5, 14, 30, 45),
            "1x",
            "Invalid time resolution specification '1x'. Use expressions such as 1d, 5s or 3M.",
        ),
        (
            utc_datetime(2023, 10, 5, 14, 30, 45),
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
            datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
            datetime.datetime(2024, 1, 10, tzinfo=datetime.timezone.utc),
            "d",
            [datetime.datetime(2024, 1, d, tzinfo=datetime.timezone.utc) for d in range(1, 11)],
        ),
        (
            datetime.datetime(2024, 1, 1, 10, 12, tzinfo=datetime.timezone.utc),
            datetime.datetime(2024, 1, 10, 10, 12, tzinfo=datetime.timezone.utc),
            "d",
            [datetime.datetime(2024, 1, d, 10, 12, tzinfo=datetime.timezone.utc) for d in range(1, 11)],
        ),
        (
            datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
            datetime.datetime(2024, 5, 10, tzinfo=datetime.timezone.utc),
            "M",
            [
                datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2024, 3, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2024, 4, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2024, 5, 1, tzinfo=datetime.timezone.utc),
            ],
        ),
        (
            datetime.datetime(2024, 10, 11, tzinfo=datetime.timezone.utc),
            datetime.datetime(2025, 3, 5, tzinfo=datetime.timezone.utc),
            "M",
            [
                datetime.datetime(2024, 10, 11, tzinfo=datetime.timezone.utc),
                datetime.datetime(2024, 11, 11, tzinfo=datetime.timezone.utc),
                datetime.datetime(2024, 12, 11, tzinfo=datetime.timezone.utc),
                datetime.datetime(2025, 1, 11, tzinfo=datetime.timezone.utc),
                datetime.datetime(2025, 2, 11, tzinfo=datetime.timezone.utc),
            ],
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
