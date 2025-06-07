from datetime import datetime, timedelta, timezone
from pathlib import Path
from dateutil import parser
from hashlib import md5, sha256
import logging
from os import urandom, environ as env
from typing import Literal
import re
from .types import is_float
from ..deps import safe_import

# Safe import optional dependencies
web3_module = safe_import('web3')

UTC = timezone.utc

__all__ = [
    "UTC", "DATETIME_FMT", "DATETIME_FMT_TZ", "DATETIME_FMT_ISO",
    "GENERIC_NO_DOT_SPLITTER", "LOGFILE", "LogLevel", "split", "log",
    "log_debug", "log_info", "log_error", "log_warn", "fmt_date", "parse_date",
    "rebase_epoch_to_sec", "loggingToLevel", "LogHandler", "logger",
    "generate_hash", "split_chain_addr", "truncate", "prettify",
    "function_signature", "load_template", "selector_inputs",
    "selector_outputs"
]

DATETIME_FMT = "%Y-%m-%d %H:%M:%S"
DATETIME_FMT_TZ = f"{DATETIME_FMT} %Z"
DATETIME_FMT_ISO = "%Y-%m-%dT%H:%M:%S%z"  # complies with RFC 3339 and ISO 8601
GENERIC_NO_DOT_SPLITTER = r"[-/,;|&]"
LOGFILE = env.get("LOGFILE", "out.log")
LogLevel = Literal["INFO", "ERROR", "DEBUG", "WARN"]


def split(resources: str,
          splitter: str = GENERIC_NO_DOT_SPLITTER) -> list[str]:
  if resources in ["", None]:
    return []
  split_resources = [
      r for r in re.split(splitter, resources) if r not in ["", None]
  ]
  return split_resources


def log(level: LogLevel = "INFO", *args):
  body = ' '.join(str(arg) for arg in args)
  msg = f"[{fmt_date(datetime.now(UTC))}] {level}: {body}"
  print(msg)
  with open(LOGFILE, "a+") as log:
    log.write(msg + "\n")


def log_debug(*args):
  log("DEBUG", *args)
  return True


def log_info(*args):
  log("INFO", *args)
  return True


def log_error(*args):
  log("ERROR", *args)
  return False


def log_warn(*args):
  log("WARN", *args)
  return False


def fmt_date(date: datetime, iso=True, keepTz=True):
  return date.strftime(
      DATETIME_FMT_ISO if iso else DATETIME_FMT_TZ if keepTz else DATETIME_FMT)


def parse_date(date: str | int | datetime) -> datetime | None:
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
    log_error(f"Failed to parse date: {date}", e)
    return None


def rebase_epoch_to_sec(epoch: int | float) -> int:
  while epoch >= 10000000000:
    epoch /= 1000
  while epoch <= 100000000:
    epoch *= 1000
  return int(epoch)


loggingToLevel = {
    logging.DEBUG: "DEBUG",
    logging.INFO: "INFO",
    logging.WARNING: "WARN",
    logging.ERROR: "ERROR",
    logging.CRITICAL: "ERROR"
}


class LogHandler(logging.Handler):

  def emit(self, record: logging.LogRecord):
    level_name = loggingToLevel[record.levelno]
    return log(level_name, self.format(record))  # type: ignore


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers = [LogHandler()]


def generate_hash(length=32, derive_from="") -> str:
  derive_from = derive_from or datetime.now(UTC).isoformat()
  hash_fn = md5 if length <= 32 else sha256
  return hash_fn(
      (str(derive_from) + urandom(64).hex()).encode()).hexdigest()[:length]


def split_chain_addr(target: str) -> tuple[str | int, str]:
  if web3_module is None:
    raise ImportError(
        "Missing optional dependency 'web3'. Install with 'pip install web3' or 'pip install chomp[evm]'"
    )

  tokens = target.split(":")
  n = len(tokens)
  if n == 1:
    tokens = ["1", tokens[0]]  # default to ethereum L1
  if n > 2:
    raise ValueError(
        f"Invalid target format for evm: {target}, expected chain_id:address")
  return int(tokens[0]), web3_module.Web3.to_checksum_address(tokens[1])


def truncate(value, max_width=32):
  value = str(value)
  return value[:max_width - 3] + "..." if len(value) > max_width else value


def prettify(data, headers, max_width=32):
  headers = [truncate(header, max_width) for header in headers]
  data = [[truncate(item, max_width) for item in row] for row in data]

  col_widths = [
      max(len(str(item)) for item in column) for column in zip(headers, *data)
  ]
  row_fmt = "| " + " | ".join(f"{{:<{w}}}" for w in col_widths) + " |"
  x_sep = "+" + "+".join(["-" * (col_width + 2)
                          for col_width in col_widths]) + "+\n"
  return (x_sep + row_fmt.format(*headers) + "\n" + x_sep +
          "\n".join(row_fmt.format(*row) for row in data) + "\n" + x_sep)


def function_signature(fn):
  if isinstance(fn, str):
    return fn
  elif callable(fn):
    try:
      return f"{fn.__name__}({', '.join(fn.__code__.co_varnames)})"
    except AttributeError:
      pass
  try:
    return fn.__code__.co_code
  except AttributeError:
    return repr(fn)


def load_template(template: str) -> str:
  with open(
      Path(__file__).parent.parent / "server" / "templates" / template,
      "r") as f:
    return f.read()


def selector_inputs(selector: str) -> list[str]:
  if ")(" not in selector:
    return []
  input_part = selector.split(")(")[0]
  match = re.match(r'[^(]*\((.*)\)', input_part)
  if match:
    inputs = match.group(1)
    return [inp.strip() for inp in inputs.split(',') if inp.strip()]
  return []


def selector_outputs(selector: str) -> list[str]:
  if ")(" not in selector:
    return []
  # Extract the output part after ")("
  output_part = selector.split(")(")[1][:-1]
  # Check if outputs are in a nested tuple format
  struct = False
  if output_part.startswith("(") and output_part.endswith(")"):
    # Nested tuple outputs: Remove outer parentheses and split
    outputs = output_part[1:-1]
    struct = True
  else:
    # Flat outputs: Use as-is
    outputs = output_part
  # Parse outputs and return as a flat list
  ret = [out.strip() for out in outputs.split(',') if out.strip()]
  if struct:
    return [ret]  # type: ignore
  return ret
