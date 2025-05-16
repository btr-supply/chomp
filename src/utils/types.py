def is_bool(value: any) -> bool:
  return str(value).lower() in ["true", "false", "yes", "no", "1", "0"]

def to_bool(value: str) -> bool:
  return value.lower() in ["true", "yes", "1"]

def is_float(s: str) -> bool:
  try:
    float(s)
    return True
  except ValueError:
    return False

def is_primitive(value: any) -> bool:
  # return isinstance(value, (int, float, str, bool, type(None)))
  return not isinstance(value, (dict, list, tuple, set, frozenset))

def is_iterable(value: any) -> bool:
  return isinstance(value, (list, tuple, set, frozenset))

def flatten(items, depth=1, current_depth=0, flatten_maps=False):
  if not is_iterable(items):
    yield items
  for x in items:
    if is_iterable(x) or (flatten_maps and isinstance(x, dict)):
      if current_depth < depth:
        yield from flatten(x, depth=depth, current_depth=current_depth + 1, flatten_maps=flatten_maps)
      else:
        yield x
    else:
      yield x
