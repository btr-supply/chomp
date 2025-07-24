from datetime import datetime
from pathlib import Path
import logging
from typing import Literal, Union, Any
import re
from ..constants import GENERIC_NO_DOT_SPLITTER, UTC, LOGFILE
import web3

from .decorators import cache
from .date import fmt_date


# Helper functions for common transformations
def safe_str(value: Any) -> str:
  """Safely convert value to string"""
  return str(value) if value is not None else ""


def split_words(value: Any) -> list[str]:
  """Split string into words, handling None values"""
  return safe_str(value).lower().split(" ")


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


def split_chain_addr(target: str) -> tuple[Union[str, int], str]:

  tokens = target.split(":")
  n = len(tokens)
  if n == 1:
    tokens = ["1", tokens[0]]  # default to ethereum L1
  if n > 2:
    raise ValueError(
        f"Invalid target format for evm: {target}, expected chain_id:address")
  return int(tokens[0]), web3.Web3.to_checksum_address(tokens[1])


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


@cache(ttl=-1, maxsize=512)
def selector_inputs(selector: str) -> list[str]:
  if ")(" not in selector:
    return []
  input_part = selector.split(")(")[0]
  match = re.match(r'[^(]*\((.*)\)', input_part)
  if match:
    inputs = match.group(1)
    return [inp.strip() for inp in inputs.split(',') if inp.strip()]
  return []


@cache(ttl=-1, maxsize=512)
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
