import datetime
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Literal, TypeAlias, cast

from dateutil.relativedelta import relativedelta

from hyperion.log import get_logger

TIME_UNITS = ["s", "m", "h", "d", "w", "M", "y"]
PATT_TIME_RESOLUTION = re.compile(rf"(?P<value>\d+)(?P<unit>[{''.join(TIME_UNITS)}])")
TimeResolutionUnit: TypeAlias = Literal["s", "m", "h", "d", "w", "M", "y"]

logger = get_logger("hyperion-dateutils")


@dataclass(frozen=True, eq=True)
class TimeResolution:
    value: int
    unit: TimeResolutionUnit

    def __post_init__(self) -> None:
        if not isinstance(self.value, int):
            super().__setattr__("value", int(self.value))
        if self.unit not in TIME_UNITS:
            raise ValueError(f"Unknown time unit {self.unit!r}. Pick one of {', '.join(TIME_UNITS)}")

    def __repr__(self) -> str:
        return f"{self.value}{self.unit}"

    @staticmethod
    def from_str(string: str) -> "TimeResolution":
        if (rematch := PATT_TIME_RESOLUTION.match(string)) is None:
            raise ValueError(f"Invalid time resolution specification {string!r}. Use expressions such as 1d, 5s or 3M.")
        value = int(rematch.group("value"))
        unit = cast(TimeResolutionUnit, rematch.group("unit"))
        return TimeResolution(value=value, unit=unit)

    @property
    def delta(self) -> datetime.timedelta | relativedelta:
        match self.unit:
            case "s":
                return datetime.timedelta(seconds=self.value)
            case "m":
                return datetime.timedelta(minutes=self.value)
            case "h":
                return datetime.timedelta(hours=self.value)
            case "d":
                return datetime.timedelta(days=self.value)
            case "w":
                return datetime.timedelta(days=self.value * 7)
            case "M":
                return relativedelta(months=self.value)
            case "y":
                return relativedelta(years=self.value)
            case _:
                raise ValueError(f"Unsupported time unit {self.unit!r}.")


def truncate_datetime(base: datetime.datetime | datetime.date, unit: TimeResolutionUnit) -> datetime.datetime:
    """Truncate datetime to the specified unit (set all smaller units to zero)."""
    if not isinstance(base, datetime.datetime):
        base = datetime.datetime(base.year, base.month, base.day, tzinfo=datetime.timezone.utc)
    match unit:
        case "s":
            return base.replace(microsecond=0)
        case "m":
            return base.replace(second=0, microsecond=0)
        case "h":
            return base.replace(minute=0, second=0, microsecond=0)
        case "d":
            return base.replace(hour=0, minute=0, second=0, microsecond=0)
        case "w":
            return base.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=base.weekday())
        case "M":
            return base.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        case "y":
            return base.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    raise ValueError(f"Unknown time unit {unit!r}. Pick one of {', '.join(TIME_UNITS)}")


def iter_dates_between(
    start_date: datetime.datetime | datetime.date,
    end_date: datetime.datetime | datetime.date,
    granularity: TimeResolutionUnit,
) -> Iterator[datetime.datetime]:
    """
    Iterate over datetimes between start_date and end_date with steps based on the given granularity.
    Includes the start_date and may include end_date (if end date is reachable from start date with given granularity).

    :param start_date: The starting datetime.
    :param end_date: The ending datetime.
    :param granularity: The granularity for steps (e.g., "d" for days, "M" for months).
    :return: An iterator of datetime objects.
    """
    start_date = assure_timezone(start_date)
    end_date = assure_timezone(end_date)
    logger.debug(
        "Generating dates between two points.", start_date=start_date.isoformat(), end_date=end_date.isoformat()
    )
    if start_date > end_date:
        raise ValueError("Start date cannot be later than end date.")

    current = start_date

    delta: datetime.timedelta | relativedelta

    match granularity:
        case "s":
            delta = datetime.timedelta(seconds=1)
        case "m":
            delta = datetime.timedelta(minutes=1)
        case "h":
            delta = datetime.timedelta(hours=1)
        case "d":
            delta = datetime.timedelta(days=1)
        case "w":
            delta = datetime.timedelta(weeks=1)
        case "M":
            delta = relativedelta(months=1)
        case "y":
            delta = relativedelta(years=1)
        case default:
            raise ValueError(f"Unsupported granularity {default!r}.")

    while current <= end_date:
        yield current
        current += delta


def quantize_datetime(base: datetime.datetime, resolution: TimeResolution | str) -> datetime.datetime:
    """
    Quantize a datetime to the next interval based on the specified resolution.

    This function aligns a given datetime to the next moment in time defined
    by the resolution. The resolution is expressed as a unit (seconds, minutes,
    hours, or days) and a value (e.g., 5 seconds, 15 minutes).

    **Important Notes:**
    - The resolution is always calculated relative to the higher unit, which may lead
      to overlapping intervals for non-standard values. For example:
        - A 7-second resolution could result in intervals ending at 12:50:56 and 12:51:03,
          with another interval starting at 12:51:00, causing overlaps.
    - To avoid such overlaps, it is recommended to use resolutions that are divisors of
      the higher unit (e.g., 60 for seconds and minutes).

    Parameters:
    - base (datetime.datetime): The datetime to quantize.
    - resolution (TimeResolution | str): The resolution for quantization.
      If a string is provided, it should follow the format "{value}{unit}"
      (e.g., "5s", "15m", "2h").

    Returns:
    - datetime.datetime: The quantized datetime.

    Raises:
    - ValueError: If the resolution unit is unsupported.
    """
    resolution = resolution if isinstance(resolution, TimeResolution) else TimeResolution.from_str(resolution)
    base_truncated = truncate_datetime(base, resolution.unit)

    def _get_shift(value: int) -> int:
        return resolution.value - (value % resolution.value)

    match resolution.unit:
        case "s":
            seconds_shift = _get_shift(base.second)
            return base_truncated + datetime.timedelta(seconds=seconds_shift)
        case "m":
            minutes_shift = _get_shift(base.minute)
            return base_truncated + datetime.timedelta(minutes=minutes_shift)
        case "h":
            hours_shift = _get_shift(base.hour)
            return base_truncated + datetime.timedelta(hours=hours_shift)
        case "d":
            days_shift = _get_shift(base.day)
            return base_truncated + datetime.timedelta(days=days_shift)
        case "w":
            days_shift = resolution.value * 7 - (base.weekday() % resolution.value)
            return base_truncated + datetime.timedelta(days=days_shift)
        case "M":
            months_shift = _get_shift(base.month)
            return base_truncated + relativedelta(months=months_shift)
        case "y":
            years_shift = _get_shift(base.year)
            return base_truncated + relativedelta(years=years_shift)
    raise ValueError(f"Unsupported resolution unit {resolution.unit!r} for quantization.")  # pragma: no cover


def assure_timezone(
    base: datetime.datetime | datetime.date, tz: datetime.timezone = datetime.timezone.utc
) -> datetime.datetime:
    """Assure datetime has a datetime and return it timezone-aware if not."""
    if not isinstance(base, datetime.datetime):
        return datetime.datetime(base.year, base.month, base.day, tzinfo=tz)
    if base.tzinfo is not None:
        if base.tzinfo == tz:
            return base
        return base.astimezone(tz)
    logger.warning(f"A timezone-unaware timestamp was given, assuming {tz!r}.")
    return base.replace(tzinfo=tz)


def get_date_pattern(date: datetime.datetime, unit: TimeResolutionUnit) -> str:
    """Get a date pattern string up to the specified unit level.

    Examples:
        >>> get_date_pattern(datetime.datetime(2025, 1, 12, 12), "h")
        "2025-01-12T12"
        >>> get_date_pattern(datetime.datetime(2025, 1, 12, 12), "d")
        "2025-01-12"
        >>> get_date_pattern(datetime.datetime(2025, 1, 12, 12), "M")
        "2025-01"
        >>> get_date_pattern(datetime.datetime(2025, 1, 12, 12), "y")
        "2025"
    """
    truncated = truncate_datetime(date, unit)
    match unit:
        case "s":
            return truncated.strftime("%Y-%m-%dT%H:%M:%S")
        case "m":
            return truncated.strftime("%Y-%m-%dT%H:%M")
        case "h":
            return truncated.strftime("%Y-%m-%dT%H")
        case "d":
            return truncated.strftime("%Y-%m-%d")
        case "w":
            return truncated.strftime("%Y-%m-%d")  # Week is special, keep the day
        case "M":
            return truncated.strftime("%Y-%m")
        case "y":
            return truncated.strftime("%Y")
    raise ValueError(f"Unsupported time unit {unit!r}.")


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


def iter_intervals(
    start_date: datetime.datetime, end_date: datetime.datetime, granularity: TimeResolutionUnit
) -> Iterable[tuple[datetime.datetime, datetime.datetime]]:
    """Iter tuples of (interval start, interval end).

    :param start_date: The starting datetime.
    :param end_date: The ending datetime.
    :param granularity: The granularity for steps (e.g., "d" for days, "M" for months).
    :return: An iterator of intervals (tuples of (start, end)).
    """
    interval_starts = list(iter_dates_between(start_date, end_date, granularity))
    for i, interval_start in enumerate(interval_starts):
        interval_end = interval_starts[i + 1] if i + 1 < len(interval_starts) else end_date
        if interval_start == interval_end:
            continue
        yield interval_start, interval_end
