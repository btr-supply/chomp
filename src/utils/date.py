from datetime import datetime, timedelta, timezone
import re
from typing import Literal
from dateutil.relativedelta import relativedelta
from .format import parse_date

UTC = timezone.utc

def now(utc=True) -> datetime:
  return datetime.now(UTC) if utc else datetime.now()

def ago(from_date=None, tz=UTC, **kwargs) -> datetime:
  return (from_date or datetime.now(tz)) - relativedelta(**kwargs)

# below are based on ISO 8601 capitalization (cf. https://en.wikipedia.org/wiki/ISO_8601)
TimeUnit = Literal["ns", "us", "ms", "s", "m", "h", "D", "W", "M", "Y"]
Interval = Literal[
  "s2", "s5", "s10", "s15", "s20", "s30", # sub minute
  "m1", "m2", "m5", "m10", "m15", "m30", # sub hour
  "h1", "h2", "h4", "h6", "h8", "h12", # sub day
  "D1", "D2", "D3", # sub week
  "W1", "W2", # sub month
  "M1", "M2", "M3", "M6", # sub year
  "Y1", "Y2", "Y3"] # multi year

MONTH_SECONDS = round(2.592e+6)
YEAR_SECONDS = round(3.154e+7)

CRON_BY_TF: dict[str, tuple] = {
  "s2": "* * * * * */2", # every 2 seconds
  "s5": "* * * * * */5", # every 5 seconds
  "s10": "* * * * * */10", # every 10 seconds
  "s15": "* * * * * */15", # every 15 seconds
  "s20": "* * * * * */20", # every 20 seconds
  "s30": "* * * * * */30", # every 30 seconds
  "m1": "*/1 * * * *", # every minute
  "m2": "*/2 * * * *", # every 2 minutes
  "m5": "*/5 * * * *", # every 5 minutes
  "m10": "*/10 * * * *", # every 10 minutes
  "m15": "*/15 * * * *", # every 15 minutes
  "m30": "*/30 * * * *", # every 30 minutes
  "h1": "0 * * * *", # every hour
  "h2": "0 */2 * * *", # every 2 hours
  "h4": "0 */4 * * *", # every 4 hours
  "h6": "0 */6 * * *", # every 6 hours
  "h8": "0 */8 * * *", # every 8 hours
  "h12": "0 */12 * * *", # every 12 hours
  "D1": "0 0 */1 * *", # every day
  "D2": "0 0 */2 * *",  # approx. every 2 days (odd days)
  "D3": "0 0 */3 * *",  # approx. every 3 days (multiple of 3)
  "W1": "0 0 * * 0", # every week (sunday at midnight)
  "W2": "0 0 * * 0/2", # every 2 weeks (sunday at midnight)
  "M1": "0 0 1 */1 *", # every month (1st of the month)
  "M2": "0 0 1 */2 *", # every 2 months (1st of the month)
  "M3": "0 0 1 */3 *", # every 3 months (1st of the month)
  "M6": "0 0 1 */6 *", # every 6 months (1st of the month)
  "Y1": "0 0 1 1 *", # every year (Jan 1)
  "Y2": "0 0 1 1 */2", # every 2 years (Jan 1)
  "Y3": "0 0 1 1 */3", # every 3 years (Jan 1)
}

SEC_BY_TF: dict[str, int] = {
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

def interval_to_sql(interval: str) -> str:
  return INTERVAL_TO_SQL.get(interval, None)

def interval_to_cron(interval: str) -> str:
  cron = CRON_BY_TF.get(interval, None)
  if not cron:
    raise ValueError(f"Invalid interval: {interval}")
  return cron

delta_by_unit: dict[str, callable] = {
  "s": lambda n: timedelta(seconds=n),
  "m": lambda n: timedelta(minutes=n),
  "h": lambda n: timedelta(hours=n),
  "D": lambda n: timedelta(days=n),
  "W": lambda n: timedelta(weeks=n),
  "M": lambda n: relativedelta(months=n),
  "Y": lambda n: relativedelta(years=n),
}

def extract_time_unit(interval: str) -> str:
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
    raise ValueError(f"Invalid time unit. Only {', '.join(delta_by_unit.keys())} are supported.")

  unit, n = match.groups()
  n = int(n)
  if backwards:
    n = -n

  delta = delta_by_unit.get(unit, None)
  if not delta:
    raise NotImplementedError(f"Unsupported time unit: {unit}, please reach out at the dev team.")
  return delta(n)

def interval_to_seconds(interval: str, raw=False) -> int:
  secs = SEC_BY_TF.get(interval, None)
  if not raw or secs >= 604800: # 1 week
    delta = interval_to_delta(interval)
    n = now()
    secs = round((n + delta - n).total_seconds())
  return secs

def floor_date(date: datetime=None, interval: str|int|float=None) -> datetime:
  if isinstance(interval, str):
    interval = interval_to_seconds(interval)
  date = date or now()
  epoch_sec = int(date.timestamp())
  floored_epoch = epoch_sec - (epoch_sec % interval) # only floor if not already at the floor
  floored_datetime = datetime.fromtimestamp(floored_epoch, date.tzinfo)
  return floored_datetime

def ceil_date(date: datetime=None, interval: str|int|float=None) -> datetime:
  date = date or now()
  if isinstance(interval, str):
    interval = interval_to_seconds(interval)
  floored = floor_date(date, interval)
  return floored + timedelta(seconds=interval) if floored != date else date # only shift if not already at the ceiling

def secs_to_ceil_date(date: datetime=None, secs: int=None, offset=0) -> datetime:
  date = date or now()
  return max(round((ceil_date(date, secs) - (date or now())).seconds) + offset, 0)

def floor_utc(interval="m1") -> datetime:
  return floor_date(now(), interval)

def ceil_utc(interval="m1") -> datetime:
  return ceil_date(now(), interval)

def shift_date(date: datetime=None, timeframe: Interval="m1", backwards=False):
  date = date or now()
  return date + interval_to_delta(timeframe, backwards)

def fit_interval(from_date: datetime, to_date: datetime=None, target_epochs=100) -> Interval:
  to_date = to_date or now()
  diff_seconds = (to_date - from_date).total_seconds()

  if target_epochs > 0:  # Check for valid target_epochs
    target_interval_seconds = diff_seconds / target_epochs
    for interval, seconds in SEC_BY_TF.items():
      if seconds >= target_interval_seconds:  # Find the smallest interval that fits
        return interval

  return "h6"

def round_interval(seconds: float, margin: float = 0.25) -> Interval:
  for interval in SEC_BY_TF:
    if SEC_BY_TF[interval] >= seconds * (1 - margin):
      return interval
  return "h1" # Default to hourly if no match

def fit_date_params(from_date: datetime = None, to_date: datetime = None, interval: Interval = None, target_epochs: int = None) -> Interval:

  from_date, to_date = parse_date(from_date), parse_date(to_date)

  if not to_date:
    if from_date and target_epochs and interval:
      to_date = from_date + timedelta(seconds=SEC_BY_TF[interval] * target_epochs)
    else:
      to_date = parse_date(to_date or now())
    n = now()
    if to_date > n:
      to_date = n

  if not target_epochs:
    if from_date and interval:
      target_epochs = (to_date - from_date).total_seconds() // SEC_BY_TF[interval]
    else:
      target_epochs = 400

  if not interval:
    if from_date:
      interval = fit_interval(from_date, to_date, target_epochs=target_epochs)
    else:
      interval = "m10" # default to 10 minutes so about 3 days for 400 epochs

  if not from_date:
    from_date = to_date - timedelta(seconds=SEC_BY_TF[interval] * target_epochs)

  return from_date, to_date, interval, target_epochs
