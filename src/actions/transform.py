import re
import string
import orjson
from hashlib import sha256, md5
from blake3 import blake3
import numpy as np
from asyncio import TimeoutError as FutureTimeoutError
from typing import Callable, Any
from dataclasses import dataclass

from .. import state
from ..models.base import SYS_FIELDS, ResourceField
from ..models.ingesters import Ingester
from ..utils import now, safe_eval, interval_to_delta, log_debug, log_error
from ..utils.decorators import cache as _cache
from ..utils.format import safe_str, split_words
# from ..utils.mitch import (
#   mitch_ticker_id_transformer,
#   mitch_tickers_transformer,
#   mitch_trades_transformer,
#   mitch_orders_transformer
# )
from ..server.responses import ORJSON_OPTIONS
from ..actions.load import load
from ..cache import get_cache


BASE_TRANSFORMERS: dict[str, Callable] = {
    "lower":
    lambda r, self: safe_str(self).lower(),
    "upper":
    lambda r, self: safe_str(self).upper(),
    "capitalize":
    lambda r, self: safe_str(self).capitalize(),
    "title":
    lambda r, self: safe_str(self).title(),
    "int":
    lambda r, self: int(self),
    "float":
    lambda r, self: float(self),
    "str":
    lambda r, self: safe_str(self),
    "bool":
    lambda r, self: bool(self),
    "to_json":
    lambda r, self: orjson.dumps(self, option=ORJSON_OPTIONS).decode(),
    "to_snake":
    lambda r, self: "_".join(split_words(self)),
    "to_kebab":
    lambda r, self: "-".join(split_words(self)),
    "slugify":
    lambda r, self: "-".join(split_words(self)),
    "to_camel":
    lambda r, self: "".join([i.capitalize() for i in split_words(self)]),
    "to_pascal":
    lambda r, self: "".join([i.capitalize() for i in split_words(self)]),
    "strip":
    lambda r, self: safe_str(self).strip(),
    "shorten_address":
    lambda r, self: f"{self[:6]}...{self[-4:]}"
    if len(safe_str(self)) > 10 else safe_str(self),
    "remove_punctuation":
    lambda r, self: safe_str(self).translate(
        str.maketrans('', '', string.punctuation)),
    "reverse":
    lambda r, self: safe_str(self)[::-1],
    "bin":
    lambda r, self: bin(int(self))[2:],
    "hex":
    lambda r, self: hex(int(self))[2:],
    "sha256digest":
    lambda r, self: sha256(safe_str(self).encode()).hexdigest(),
    "md5digest":
    lambda r, self: md5(safe_str(self).encode()).hexdigest(),
    "blake3digest":
    lambda r, self: blake3(safe_str(self).encode()).hexdigest(),
    "round":
    lambda r, self: round(float(self)),
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
    # MITCH transformers
    # "mitch_ticker_id":
    # mitch_ticker_id_transformer,
    # "mitch_tickers":
    # mitch_tickers_transformer,
    # "mitch_trades":
    # mitch_trades_transformer,
    # "mitch_orders":
    # mitch_orders_transformer,
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


@dataclass
class CompiledTransformer:
  """Represents a pre-compiled transformation function."""
  raw: str
  # The compiled function expects a dict of all required values and the field's own value.
  steps: Callable[[dict[str, Any], Any], Any]
  field_references: set[str]
  dotted_references: set[
      str]  # References with dots that may be local or cached
  series_steps: list[tuple[
      str, str, str, str]]  # (placeholder, target, function, lookback)
  has_self_reference: bool


@_cache(ttl=3600, maxsize=1024)
def compile_transformer(transformer: str) -> CompiledTransformer:
  """Compiles a transformer string into a callable function."""

  # 1. Handle simple cases first (base transformers and literals)
  if not re.search(r'\{.+\}', transformer):
    base_transformer_func = BASE_TRANSFORMERS.get(transformer)
    if base_transformer_func:
      compiled_func = lambda data, field_value: base_transformer_func(
          None, field_value)
      has_self = True
    else:
      try:
        literal_value = float(transformer)
        compiled_func = lambda data, field_value: literal_value
        has_self = False
      except ValueError:
        compiled_func = lambda data, field_value: transformer
        has_self = False

    return CompiledTransformer(raw=transformer,
                               steps=compiled_func,
                               field_references=set(),
                               dotted_references=set(),
                               series_steps=[],
                               has_self_reference=has_self)

  # 2. Parse complex transformers
  field_refs, dotted_refs, series_steps = set(), set(), []
  has_self = '{self}' in transformer
  for i, ref in enumerate(re.findall(r'\{([^}]+)\}', transformer)):
    if '::' in ref:
      target, func_lookback = ref.split('::', 1)
      match = re.match(r'(.+?)\((.+?)\)', func_lookback)
      if match:
        func, lookback = match.groups()
        series_steps.append((f"__series_{i}__", target.strip(), func.strip(),
                           lookback.strip()))
    elif '.' in ref:
      dotted_refs.add(ref)
    elif ref != 'self':
      field_refs.add(ref)

  # 3. Build lambda string
  lambda_body = transformer
  if has_self:
    lambda_body = lambda_body.replace('{self}', 'field_value')
  for ref in field_refs | dotted_refs:
    lambda_body = lambda_body.replace(f'{{{ref}}}', f"data['{ref}']")
  for placeholder, target, func, lookback in series_steps:
    lambda_body = lambda_body.replace(f'{{{target}::{func}({lookback})}}',
                                      f"data['{placeholder}']")

  # 4. Compile and return
  try:
    compiled_func = safe_eval(f"lambda data, field_value: {lambda_body}",
                              lambda_check=True)
  except Exception as e:
    log_error(f"Failed to compile transformer '{transformer}': {e}")
    compiled_func = lambda data, field_value: field_value

  return CompiledTransformer(raw=transformer,
                             steps=compiled_func,
                             field_references=field_refs,
                             dotted_references=dotted_refs,
                             series_steps=series_steps,
                             has_self_reference=has_self)


@_cache(ttl=300, maxsize=256)
async def get_cached_field_value(ingester_name: str,
                                 field_name: str,
                                 index: int = 0):
  """
  Get cached field value from Redis.

  For 'idx' field: Returns the entire idx dict or a specific field within idx
  For other fields: Returns the direct field value
  """
  cached_data = await get_cache(ingester_name, pickled=True)
  if not cached_data:
    log_error(f"No cached data found for {ingester_name}")
    return None

  # Handle field access with unified logic
  if field_name == "idx":
    result = cached_data.get("idx")
    if not result and state.args.verbose:
      log_debug(
          f"No 'idx' field found in cached data for {ingester_name}, may not have been generated yet"
      )
    return result

  # Handle regular field access
  result = cached_data.get(field_name)
  if result is None:
    log_error(
        f"Field {field_name} not found in cached data for {ingester_name}")
  return result


async def get_cached_field_values_batch(
    dotted_refs: set[str]) -> dict[str, Any]:
  """
  Batch retrieve cached field values from Redis for optimal performance.

  Args:
    dotted_refs: Set of dotted references like {"BinanceFeeds.USDT", "AVAX.idx"}

  Returns:
    Dict mapping dotted references to their field values
  """
  from ..cache import get_cache_batch

  ref_map = {ref: parse_cached_reference(ref) for ref in dotted_refs}
  ingester_names = {ingester for ingester, _ in ref_map.values() if ingester}

  if not ingester_names:
    return {}

  cached_data_batch = await get_cache_batch(list(ingester_names), pickled=True)

  results = {}
  for ref, (ingester_name, field_name) in ref_map.items():
    if not ingester_name:
      log_error(f"Invalid dotted reference format: {ref}")
      continue

    ingester_data = cached_data_batch.get(ingester_name)
    if not ingester_data:
      log_error(f"No cached data found for ingester {ingester_name}")
      results[ref] = None
      continue

    field_value = ingester_data.get(field_name)

    if field_name == "idx" and not field_value and state.args.verbose:
      log_debug(f"No 'idx' field found in cached data for {ingester_name}")
    elif field_value is None:
      log_error(
          f"Field '{field_name}' not found in cached data for {ingester_name}")

    results[ref] = field_value

  return results


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


async def apply_transformer(ing: "Ingester", field: ResourceField,
                            transformer: str) -> Any:
  """Applies a transformer to a field, using pre-compiled functions."""
  if not transformer:
    return field.value

  compiled_transformer = compile_transformer(transformer)

  if field.value is None and compiled_transformer.has_self_reference:
    return None

  data = {}

  # 1. Local field references
  for ref in compiled_transformer.field_references:
    if (local_field := ing.get_field(ref)) is not None:
      data[ref] = local_field.value
    else:
      log_error(
          f"Field '{ref}' not found in ingester '{ing.name}' for transformer '{transformer}'"
      )

  # 2. Dotted references (local or cached)
  if compiled_transformer.dotted_references:
    local_refs = {
        ref
        for ref in compiled_transformer.dotted_references
        if ing.get_field(ref) is not None
    }
    cache_refs = compiled_transformer.dotted_references - local_refs

    for ref in local_refs:
      data[ref] = ing.get_field(ref).value

    if cache_refs:
      cached_values = await get_cached_field_values_batch(cache_refs)
      for ref, value in cached_values.items():
        if value is not None:
          data[ref] = value
        else:
          log_error(
              f"Could not resolve dotted reference '{ref}' from cache for transformer '{transformer}'"
          )
          return None

  # 3. Series operations
  if compiled_transformer.series_steps:
    # This part can be further optimized with gather if multiple series ops are common
    for placeholder, target, func, lookback in compiled_transformer.series_steps:
      from_date = interval_to_delta(lookback, backwards=True)
      from_datetime = now() + from_date if from_date else now()
      series = await load(ing, from_datetime, None)

      if (series_func := SERIES_TRANSFORMERS.get(func)) is not None:
        data[placeholder] = series_func(ing, series)
      else:
        log_error(f"Unknown series transformer function: {func}")
        return None

  # 4. Execute
  try:
    return compiled_transformer.steps(data, field.value)
  except Exception as e:
    log_error(f"Error executing compiled transformer for '{transformer}': {e}")
    return field.value


async def transform(ing: Ingester, f: ResourceField) -> Any:
  for t in f.transformers or []:  # sequential transformer chain
    f.value = await apply_transformer(ing, f, t)
  return f.value


async def transform_all(ing: Ingester) -> int:
  """Transform all fields in the ingester"""
  # Cache clearing now handled automatically by @_cache decorator
  count = 0
  for field in ing.fields:  # sequential field transformation ()
    # Skip transformation for protected technical fields
    if field.name in SYS_FIELDS:
      count += 1
      continue

    try:
      await transform(ing, field)
      count += 1
    except (FutureTimeoutError, Exception) as e:
      field.value = None
      log_error(f"{ing.name}.{field.name} transformer error: {str(e)}")

  if state.args.verbose:
    log_debug(f"Transformed {ing.name} -> {ing.get_field_values()}")

  return count
