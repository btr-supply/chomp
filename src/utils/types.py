from typing import Any


def is_bool(value: Any) -> bool:
  return str(value).lower() in ["true", "false", "yes", "no", "1", "0"]


def to_bool(value: str) -> bool:
  return value.lower() in ["true", "yes", "1"]


def is_float(s: str) -> bool:
  try:
    # Check for leading/trailing whitespace - these should be invalid
    if s != s.strip():
      return False
    float(s)
    return True
  except ValueError:
    return False


def is_primitive(value: Any) -> bool:
  # return isinstance(value, (int, float, str, bool, type(None)))
  return not isinstance(value, (dict, list, tuple, set, frozenset))


def is_epoch(value: Any) -> bool:
  """Check if a value represents an epoch timestamp in seconds or milliseconds."""
  try:
    # Skip non-numeric types that can't be converted
    if isinstance(value, (list, dict, tuple, set)):
      return False
    val = float(value) if isinstance(value, str) else value
    # Fixed upper bound: 32503680000 seconds (year 3000) - not 32503680000000
    return 0 <= val <= 32503680000
  except (ValueError, TypeError):
    return False


def is_iterable(value: Any) -> bool:
  return isinstance(value, (list, tuple, set, frozenset))


def flatten(items, depth=1, current_depth=0, flatten_maps=False):
  if not is_iterable(items):
    yield items
    return

  for x in items:
    if is_iterable(x) or (flatten_maps and isinstance(x, dict)):
      if current_depth < depth:
        yield from flatten(x,
                           depth=depth,
                           current_depth=current_depth + 1,
                           flatten_maps=flatten_maps)
      else:
        yield x
    else:
      yield x
