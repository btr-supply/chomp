from datetime import datetime, timedelta, timezone
import re
from typing import Literal, Callable, Any, Optional, Union
from dateutil.relativedelta import relativedelta
from ..constants import UTC, DATETIME_FMT, DATETIME_FMT_TZ, DATETIME_FMT_ISO


def now(utc=True) -> datetime:
  return datetime.now(timezone.utc) if utc else datetime.now()


def ago(from_date=None, tz=UTC, **kwargs) -> datetime:
  return (from_date or datetime.now(tz)) - relativedelta(**kwargs)


# below are based on ISO 8601 capitalization (cf. https://en.wikipedia.org/wiki/ISO_8601)
TimeUnit = Literal["ns", "us", "ms", "s", "m", "h", "D", "W", "M", "Y"]
Interval = Literal[
    "s1",
    "s2",
    "s5",
    "s10",
    "s15",
    "s20",
    "s30",  # sub minute
    "m1",
    "m2",
    "m5",
    "m10",
    "m15",
    "m30",  # sub hour
    "h1",
    "h2",
    "h4",
    "h6",
    "h8",
    "h12",  # sub day
    "D1",
    "D2",
    "D3",  # sub week
    "W1",
    "W2",  # sub month
    "M1",
    "M2",
    "M3",
    "M6",  # sub year
    "Y1",
    "Y2",
    "Y3"]  # multi year

MONTH_SECONDS = round(2.592e+6)
YEAR_SECONDS = round(3.154e+7)

CRON_BY_TF: dict[str, str] = {
    "s1": "* * * * * */1",  # every second
    "s2": "* * * * * */2",  # every 2 seconds
    "s5": "* * * * * */5",  # every 5 seconds
    "s10": "* * * * * */10",  # every 10 seconds
    "s15": "* * * * * */15",  # every 15 seconds
    "s20": "* * * * * */20",  # every 20 seconds
    "s30": "* * * * * */30",  # every 30 seconds
    "m1": "*/1 * * * *",  # every minute
    "m2": "*/2 * * * *",  # every 2 minutes
    "m5": "*/5 * * * *",  # every 5 minutes
    "m10": "*/10 * * * *",  # every 10 minutes
    "m15": "*/15 * * * *",  # every 15 minutes
    "m30": "*/30 * * * *",  # every 30 minutes
    "h1": "0 * * * *",  # every hour
    "h2": "0 */2 * * *",  # every 2 hours
    "h4": "0 */4 * * *",  # every 4 hours
    "h6": "0 */6 * * *",  # every 6 hours
    "h8": "0 */8 * * *",  # every 8 hours
    "h12": "0 */12 * * *",  # every 12 hours
    "D1": "0 0 */1 * *",  # every day
    "D2": "0 0 */2 * *",  # approx. every 2 days (odd days)
    "D3": "0 0 */3 * *",  # approx. every 3 days (multiple of 3)
    "W1": "0 0 * * 0",  # every week (sunday at midnight)
    "W2": "0 0 * * 0/2",  # every 2 weeks (sunday at midnight)
    "M1": "0 0 1 */1 *",  # every month (1st of the month)
    "M2": "0 0 1 */2 *",  # every 2 months (1st of the month)
    "M3": "0 0 1 */3 *",  # every 3 months (1st of the month)
    "M6": "0 0 1 */6 *",  # every 6 months (1st of the month)
    "Y1": "0 0 1 1 *",  # every year (Jan 1)
    "Y2": "0 0 1 1 */2",  # every 2 years (Jan 1)
    "Y3": "0 0 1 1 */3",  # every 3 years (Jan 1)
}

SEC_BY_TF: dict[str, int] = {
    "s1": 1,
    "s2": 2,
    "s5": 5,
    "s10": 10,
    "s15": 15,
    "s20": 20,
    "s30": 30,
    "m1": 60,
    "m2": 120,
    "m5": 300,
    "m10": 600,
    "m15": 900,
    "m30": 1800,
    "h1": 3600,
    "h2": 7200,
    "h4": 14400,
    "h6": 21600,
    "h8": 28800,
    "h12": 43200,
    "D1": 86400,
    "D2": 172800,
    "D3": 259200,
    "W1": 604800,
    "W2": 1209600,
    "M1": 2592000,
    "M2": 5184000,
    "M3": 7776000,
    "M6": 15552000,
    "Y1": YEAR_SECONDS,
    "Y2": YEAR_SECONDS * 2,
    "Y3": YEAR_SECONDS * 3,
}

INTERVAL_TO_SQL: dict[str, str] = {
    "s1": "1 seconds",
    "s2": "2 seconds",
    "s5": "5 seconds",
    "s10": "10 seconds",
    "s15": "15 seconds",
    "s20": "20 seconds",
    "s30": "30 seconds",
    "m1": "1 minute",
    "m2": "2 minutes",
    "m5": "5 minutes",
    "m10": "10 minutes",
    "m15": "15 minutes",
    "m30": "30 minutes",
    "h1": "1 hour",
    "h2": "2 hours",
    "h4": "4 hours",
    "h6": "6 hours",
    "h8": "8 hours",
    "h12": "12 hours",
    "D1": "1 day",
    "D2": "2 days",
    "D3": "3 days",
    "W1": "1 week",
    "W2": "2 weeks",
    "M1": "1 month",
    "M2": "2 months",
    "M3": "3 months",
    "M6": "6 months",
    "Y1": "1 year",
    "Y2": "2 years",
    "Y3": "3 years"
}


def interval_to_sql(interval: str) -> Optional[str]:
  return INTERVAL_TO_SQL.get(interval, None)


def interval_to_cron(interval: str) -> str:
  cron = CRON_BY_TF.get(interval, None)
  if not cron:
    raise ValueError(f"Invalid interval: {interval}")
  return cron


delta_by_unit: dict[str, Callable[[int], Any]] = {
    "s": lambda n: timedelta(seconds=n),
    "m": lambda n: timedelta(minutes=n),
    "h": lambda n: timedelta(hours=n),
    "D": lambda n: timedelta(days=n),
    "W": lambda n: timedelta(weeks=n),
    "M": lambda n: relativedelta(months=n),
    "Y": lambda n: relativedelta(years=n),
}


def extract_time_unit(interval: str) -> tuple[str, int]:
  match = re.match(r"(\d+)([a-zA-Z]+)", interval)
  if match:
    value, unit = match.groups()
    return unit, int(value)
  else:
    raise ValueError(f"Invalid time frame format: {interval}")


def interval_to_delta(interval: str, backwards=False) -> timedelta:
  pattern = r"([smhDWMY])(\d+)"
  match = re.match(pattern, interval)
  if not match:
    raise ValueError(
        f"Invalid time unit. Only {', '.join(delta_by_unit.keys())} are supported."
    )

  unit, n = match.groups()
  n = int(n)
  if backwards:
    n = -n

  delta = delta_by_unit.get(unit, None)
  if not delta:
    raise NotImplementedError(
        f"Unsupported time unit: {unit}, please reach out at the dev team.")
  return delta(n)


def interval_to_seconds(interval: str, raw=False) -> int:
  secs = SEC_BY_TF.get(interval, None)
  if not raw or (secs is not None and secs >= 604800):  # 1 week
    delta = interval_to_delta(interval)
    n = now()
    secs = round((n + delta - n).total_seconds())
  return secs or 0


def floor_date(date: Optional[datetime] = None,
               interval: Optional[Union[str, int, float]] = None) -> datetime:
  if isinstance(interval, str):
    interval = interval_to_seconds(interval)
  if interval is None:
    raise ValueError("interval cannot be None")
  date = date or now()
  epoch_sec = int(date.timestamp())
  floored_epoch = epoch_sec - (epoch_sec % int(interval)
                               )  # only floor if not already at the floor
  floored_datetime = datetime.fromtimestamp(floored_epoch, date.tzinfo)
  return floored_datetime


def ceil_date(date: Optional[datetime] = None,
              interval: Optional[Union[str, int, float]] = None) -> datetime:
  date = date or now()
  if isinstance(interval, str):
    interval = interval_to_seconds(interval)
  if interval is None:
    raise ValueError("interval cannot be None")
  floored = floor_date(date, interval)
  return floored + timedelta(
      seconds=float(interval)
  ) if floored != date else date  # only shift if not already at the ceiling


def secs_to_ceil_date(date: Optional[datetime] = None,
                      secs: Optional[int] = None,
                      offset=0) -> int:
  date = date or now()
  return max(
      round((ceil_date(date, secs) - (date or now())).seconds) + offset, 0)


def floor_utc(interval="m1") -> datetime:
  return floor_date(now(), interval)


def ceil_utc(interval="m1") -> datetime:
  return ceil_date(now(), interval)


def shift_date(date: Optional[datetime] = None,
               timeframe: Interval = "m1",
               backwards=False):
  date = date or now()
  return date + interval_to_delta(timeframe, backwards)


def fit_interval(from_date: datetime,
                 to_date: Optional[datetime] = None,
                 target_epochs=100) -> Interval:
  to_date = to_date or now()
  diff_seconds = (to_date - from_date).total_seconds()

  if target_epochs > 0:  # Check for valid target_epochs
    target_interval_seconds = diff_seconds / target_epochs
    for interval, seconds in SEC_BY_TF.items():
      if seconds >= target_interval_seconds:  # Find the smallest interval that fits
        return interval  # type: ignore[return-value]

  return "h6"


def round_interval(seconds: float, margin: float = 0.25) -> Interval:
  for interval in SEC_BY_TF:
    if SEC_BY_TF[interval] >= seconds * (1 - margin):
      return interval  # type: ignore[return-value]
  return "h1"  # Default to hourly if no match


def fmt_date(date: datetime, iso=True, keepTz=True):
  return date.strftime(
      DATETIME_FMT_ISO if iso else DATETIME_FMT_TZ if keepTz else DATETIME_FMT)


def parse_date(date: Union[str, int, datetime]) -> Optional[datetime]:
  if isinstance(date, datetime):
    return date
  if date is None:
    return None
  try:
    if isinstance(date,
                  (int, float)) or (isinstance(date, str) and is_float(date)):
      timestamp = float(date) if isinstance(date, str) else date
      return datetime.fromtimestamp(rebase_epoch_to_sec(timestamp), tz=UTC)
    if isinstance(date, str):
      match date.lower():
        case "now":
          return datetime.now(UTC)
        case "today":
          return datetime.now(UTC).replace(hour=0,
                                           minute=0,
                                           second=0,
                                           microsecond=0)
        case "yesterday":
          return datetime.now(UTC).replace(
              hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        case "tomorrow":
          return datetime.now(UTC).replace(
              hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
      parsed_result = parser.parse(date,
                                   fuzzy_with_tokens=True,
                                   ignoretz=False)
      parsed = parsed_result[0] if isinstance(parsed_result,
                                              tuple) else parsed_result
      if not parsed.tzinfo:
        parsed = parsed.replace(tzinfo=UTC)
      return parsed
  except Exception as e:
    raise ValueError(f"Failed to parse date: {date}", e)


def rebase_epoch_to_sec(epoch: Union[int, float]) -> int:
  while epoch >= 10000000000:
    epoch /= 1000
  while epoch <= 100000000:
    epoch *= 1000
  return int(epoch)


def fit_date_params(
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    interval: Optional[Interval] = None,
    target_epochs: Optional[int] = None
) -> tuple[datetime, datetime, Interval, int]:

  from_date = parse_date(from_date) if from_date is not None else None
  to_date = parse_date(to_date) if to_date is not None else None

  if not to_date:
    if from_date and target_epochs and interval:
      to_date = from_date + timedelta(seconds=SEC_BY_TF[interval] *
                                      target_epochs)
    else:
      to_date = now()
    n = now()
    if to_date > n:
      to_date = n

  if not target_epochs:
    if from_date and interval and to_date:
      target_epochs = int(
          (to_date - from_date).total_seconds() // SEC_BY_TF[interval])
    else:
      target_epochs = 400

  if not interval:
    if from_date and to_date:
      interval = fit_interval(from_date, to_date, target_epochs=target_epochs)
    else:
      interval = "m10"  # default to 10 minutes so about 3 days for 400 epochs

  if not from_date:
    if to_date and interval and target_epochs:
      from_date = to_date - timedelta(seconds=SEC_BY_TF[interval] *
                                      target_epochs)
    else:
      from_date = now() - timedelta(hours=24)  # Default to 24 hours ago

  # Ensure all values are not None at this point
  assert from_date is not None
  assert to_date is not None
  assert interval is not None
  assert target_epochs is not None

  return from_date, to_date, interval, target_epochs
