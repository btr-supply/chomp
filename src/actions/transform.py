import re
import string
import orjson
from hashlib import sha256, md5
import numpy as np
from asyncio import TimeoutError as FutureTimeoutError
from typing import Callable, Any, Dict

from .. import state
from ..model import Ingester, ResourceField
from ..utils import safe_eval, interval_to_delta, log_debug, log_error
from ..server.responses import ORJSON_OPTIONS
from ..actions.load import load
from ..cache import get_cache

BASE_TRANSFORMERS: dict[str, Callable] = {
    "lower":
    lambda r, self: str(self).lower(),
    "upper":
    lambda r, self: str(self).upper(),
    "capitalize":
    lambda r, self: str(self).capitalize(),
    "title":
    lambda r, self: str(self).title(),
    "int":
    lambda r, self: int(self),
    "float":
    lambda r, self: float(self),
    "str":
    lambda r, self: str(self),
    "bool":
    lambda r, self: bool(self),
    "to_json":
    lambda r, self: orjson.dumps(self, option=ORJSON_OPTIONS).decode(),
    "to_snake":
    lambda r, self: "_".join(self.lower().split(" ")),
    "to_kebab":
    lambda r, self: "-".join(self.lower().split(" ")),
    "slugify":
    lambda r, self: "-".join(self.lower().split(" ")),
    "to_camel":
    lambda r, self: "".join([i.capitalize() for i in self.split(" ")]),
    "to_pascal":
    lambda r, self: "".join([i.capitalize() for i in self.split(" ")]),
    "strip":
    lambda r, self: str(self).strip(),
    "shorten_address":
    lambda r, self: f"{self[:6]}...{self[-4:]}",  # shorten evm address
    "remove_punctuation":
    lambda r, self: self.translate(str.maketrans('', '', string.punctuation)),
    "reverse":
    lambda r, self: str(self)[::-1],
    "bin":
    lambda r, self: bin(int(self))[2:],  # int to binary
    "hex":
    lambda r, self: hex(int(self))[2:],  # int to hex
    "sha256digest":
    lambda r, self: sha256(str(self).encode()).hexdigest(),
    "md5digest":
    lambda r, self: md5(str(self).encode()).hexdigest(),
    "round":
    lambda r, self: round(float(self)),  # TODO: genericize to roundN
    "round2":
    lambda r, self: round(float(self), 2),
    "round4":
    lambda r, self: round(float(self), 4),
    "round6":
    lambda r, self: round(float(self), 6),
    "round8":
    lambda r, self: round(float(self), 8),
    "round10":
    lambda r, self: round(float(self), 10),
}

SERIES_TRANSFORMERS: dict[str, Callable] = {
    "median": lambda r, series: np.median(series),
    "mean": lambda r, series: np.mean(series),
    "std": lambda r, series: np.std(series),
    "var": lambda r, series: np.var(series),
    "min": lambda r, series: np.min(series),
    "max": lambda r, series: np.max(series),
    "sum": lambda r, series: np.sum(series),  # single cumulative sum
    "cumsum": lambda r, series: np.cumsum(series),  # array of cumulative sums
    "prod": lambda r, series: np.prod(series)
}

# Cache for storing cached data to avoid multiple Redis calls per transformation
_cached_data_cache: Dict[str, Any] = {}


async def get_cached_field_value(ingester_name: str,
                                 field_name: str,
                                 index: int = 0):
  """
  Get cached field value from Redis.

  For 'idx' field: Returns the entire idx dict or a specific field within idx
  For other fields: Returns the direct field value
  """
  cache_key_name = f"{ingester_name}"

  # Check local cache first
  if cache_key_name in _cached_data_cache:
    cached_data = _cached_data_cache[cache_key_name]
  else:
    # Fetch from Redis
    cached_data = await get_cache(cache_key_name, pickled=True)
    if cached_data:
      _cached_data_cache[cache_key_name] = cached_data

  if not cached_data:
    log_error(f"No cached data found for {ingester_name}")
    return None

  # Handle 'idx' field access
  if field_name == "idx":
    if "idx" in cached_data:
      return cached_data["idx"]
    else:
      log_debug(
          f"No 'idx' field found in cached data for {ingester_name}, may not have been generated yet"
      )
      return None

  # Handle regular field access
  if field_name in cached_data:
    return cached_data[field_name]
  else:
    log_error(
        f"Field {field_name} not found in cached data for {ingester_name}")
    return None


def parse_cached_reference(ref_str: str):
  """
  Parse cached field references like 'AVAX.idx' or 'USDT.1'
  Returns (ingester_name, field_name)
  """
  # Split ingester.field
  if '.' in ref_str:
    ingester_name, field_name = ref_str.split('.', 1)
  else:
    # If no dot, assume it's just a field name from current ingester
    ingester_name = None
    field_name = ref_str

  return ingester_name, field_name


async def process_cached_references(c: Ingester, transformer: str):
  """
  Process cached field references like {AVAX.idx} and replace them with values
  """
  # Find all field references in the transformer
  cached_refs = re.findall(r'\{([^}]+)\}', transformer)

  for ref in cached_refs:
    # Skip if it's a series transformer (contains '::')
    if '::' in ref:
      continue

    # Skip if it's 'self' reference
    if ref == 'self':
      continue

    # Skip if it's already a known field in current ingester
    if ref in c.data_by_field:
      continue

    # Parse the reference
    ingester_name, field_name = parse_cached_reference(ref)

    # Determine if this is a cached reference
    is_cached_reference = False

    if field_name == 'idx':
      # Always a cached reference
      is_cached_reference = True
    elif ingester_name and ingester_name != c.name:
      # Cross-ingester reference
      is_cached_reference = True
    elif ingester_name is None and field_name not in c.data_by_field:
      # Could be a cached reference from same ingester
      is_cached_reference = True

    if is_cached_reference:
      target_ingester = ingester_name or c.name
      value = await get_cached_field_value(target_ingester, field_name)

      if value is not None:
        transformer = transformer.replace('{' + ref + '}', str(value))
      else:
        log_error(f"Could not resolve cached reference: {ref}")

  return transformer


# TODO: optimize
async def apply_transformer(c: Ingester, field: ResourceField,
                            transformer: str) -> Any:
  if not transformer:
    return field.value

  # Clear cache at the start of each transformation to ensure fresh data
  global _cached_data_cache
  _cached_data_cache.clear()

  # Process cached references first
  transformer = await process_cached_references(c, transformer)

  # Checks if single word and does not contain injected variables ('{' or '}')
  if bool(re.fullmatch(r"[^\s{}]+", transformer)):
    # If transformer is numeric, return numeric evaluation
    try:
      return float(transformer)
    except ValueError:
      base_transformer = BASE_TRANSFORMERS.get(transformer)
      if base_transformer:
        return base_transformer(c, field.value)
      return field.value

  # If transformer contains a series op "::", replace with the result of the series transformer
  if "{" in transformer:
    # step 0: extract the series transformers that starts with '{' and ends with '}', and contain '::'
    search = re.search(r"\{(.+?)\}", transformer)
    if search:
      for group in search.groups():
        if "::" in group:
          # step 1: identify the target series, word between '{' and '::'
          search = re.search(r"\{(.+?)::", transformer)
          target = search.group(1) if search else None
          # step 2: identify the transformer, word between '::' and '('
          search = re.search(r"::(.+?)\(", transformer)
          fn = search.group(1) if search else None
          # step 3: identify the lookback, word between '(' and ')'
          search = re.search(r"\((.+?)\)", transformer)
          lookback = search.group(1) if search else None
          # step 4: check integrity of the transformer
          if not target or not fn or not lookback:
            raise ValueError(f"Invalid transformer: {transformer}")
          # step 5: translate the lookback timeframe into a timedelta
          from_date = interval_to_delta(lookback, backwards=True)
          # step 6: make sure that target is either self (field) or other resource field
          if target == "self":
            target_field = field
          else:
            # filter the resource field that match the target
            target_field = next((f for f in c.fields if f.name == target),
                                None)  # type: ignore[arg-type]
            if target_field is None:
              raise ValueError(f"Invalid transformer target: {target}")
          # step 7: extract the series from the target field
          from datetime import datetime, timezone
          from_datetime = datetime.now(
              timezone.utc) + from_date if from_date else datetime.now(
                  timezone.utc)
          series = await load(c, from_datetime, None)
          # step 8: apply the series transformer
          series_transformer = SERIES_TRANSFORMERS.get(fn)
          if series_transformer:
            res = series_transformer(c, series)
            # step 9: inject the result in the transformer for recursive evaluation
            transformer = transformer.replace('{' + group + '}', str(res))

  # Replace field references with their values
  expr = transformer
  # Replace self first
  expr = expr.replace("{self}", str(field.value))

  # Replace other field references
  for fname, fvalue in c.data_by_field.items():
    expr = expr.replace("{" + fname + "}", str(fvalue))

  # Evaluate the resulting expression
  return safe_eval(expr)


async def transform(c: Ingester, f: ResourceField) -> Any:
  for t in f.transformers or []:
    f.value = await apply_transformer(c, f, t)
  c.data_by_field[f.name] = f.value
  return f.value


async def transform_all(c: Ingester) -> int:
  count = 0
  for field in c.fields:
    try:
      await transform(c, field)
      count += 1
    except (FutureTimeoutError, Exception) as e:
      log_error(
          f"{c.name}.{field.name} transformer error: {str(e)}, check {c.ingester_type} output and transformer chain"
      )

  if state.args.verbose:
    log_debug(
        f"Transformed {c.name} -> {orjson.dumps(dict(sorted(c.data_by_field.items())), option=ORJSON_OPTIONS).decode()}"
    )
  return count
