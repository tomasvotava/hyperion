import datetime
import re
from dataclasses import dataclass
from typing import Literal, TypeAlias, cast

from dateutil.relativedelta import relativedelta

from hyperion.logging import get_logger

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


def truncate_datetime(base: datetime.datetime, unit: TimeResolutionUnit) -> datetime.datetime:
    """Truncate datetime to the specified unit (set all smaller units to zero)."""
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


def assure_timezone(base: datetime.datetime, tz: datetime.timezone = datetime.timezone.utc) -> datetime.datetime:
    """Assure datetime has a datetime and return it timezone-aware if not."""
    if base.tzinfo is not None:
        if base.tzinfo == tz:
            return base
        return base.astimezone(tz)
    logger.warning(f"A timezone-unaware timestamp was given, assuming {tz!r}.")
    return base.replace(tzinfo=tz)
