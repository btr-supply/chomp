from typing import Any, TypeVar

T = TypeVar("T")


def is_bool(value: Any) -> bool:
  return str(value).lower() in ["true", "false", "yes", "no", "1", "0"]


def to_bool(value: Any) -> bool:
  return str(value).lower() in ["true", "yes", "1"]


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


def flatten(nested_list: list[Any]) -> list[Any]:
  """Flatten a nested list structure."""
  result = []
  for item in nested_list:
    if isinstance(item, list):
      result.extend(flatten(item))
    else:
      result.append(item)
  return result


def handle_none_value(value: Any, default_return: Any = None) -> Any:
  """
  Consistent None value handling utility function.
  Returns default_return if value is None, otherwise returns the value.

  Args:
    value: The value to check for None
    default_return: What to return if value is None (defaults to None)

  Returns:
    default_return if value is None, otherwise the original value
  """
  return default_return if value is None else value


def safe_field_value(field_value: Any, field_type: str = "string") -> str:
  """
  Safely convert field values for database insertion, handling None values.

  Args:
    field_value: The field value to convert
    field_type: The type of field for proper escaping

  Returns:
    Properly escaped string representation or NULL for None values
  """
  if field_value is None:
    return "NULL"

  if field_type in ["string", "binary", "varbinary"]:
    return f"'{field_value}'"
  elif field_type == "bool":
    return "1" if field_value else "0"
  else:
    return str(field_value)
