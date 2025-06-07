import orjson
import math
import polars as pl
from datetime import datetime
from asyncio import gather, get_running_loop
from io import BytesIO
from typing import Any, Optional, cast
from concurrent.futures import Executor

from ..server.responses import ORJSON_OPTIONS
from ..cache import get_cache_batch, get_cache, get_resource_status
from ..utils import now, round_sigfig, split, Interval, numeric_columns, log_info, log_debug
from .. import state
from ..model import SCOPE_ATTRS, UNALIASED_FORMATS, FillMode, Scope, ServiceResponse, DataFormat

# level 1: In-memory module cache for resources that expires after 5 minutes (lvl 2: redis)
_resources_by_scope: dict[Scope, dict[str, Any] | None] = {
    Scope.ALL: None,
    Scope.DEFAULT: None,
    Scope.DETAILED: None
}
_resources_cached_at = None


def trim_resource(resource: dict, scope: Scope = Scope.DEFAULT) -> dict:
  """Trim resource dictionary based on scope mask."""
  if not resource:
    return resource

  return {
      "name": resource.get("name"),
      "type": resource.get("type"),
      "fields": {
          name: {
              "type": field.get("type"),
              **{
                  attr: field.get(attr)
                  for flag, attr in SCOPE_ATTRS.items() if scope & flag == flag
              }, "tags": field.get("tags", []),
              "transient": field.get("transient", False)
          }
          for name, field in resource.get("fields", {}).items()
          if not (field.get("transient") and not (scope & Scope.TRANSIENT))
      }
  }


async def get_resources(scope: Scope = Scope.DEFAULT) -> ServiceResponse[dict]:
  """Get and cache resources with 5-minute expiry"""
  global _resources_by_scope, _resources_cached_at

  if not _resources_cached_at or (now() -
                                  _resources_cached_at).total_seconds() > 300:
    resources = await get_resource_status()
    tp = cast(Executor,
              state.thread_pool)  # Cast to Executor for type compatibility
    loop = get_running_loop()

    async def trim_all(scope: Scope):
      return dict(
          zip(
              resources.keys(), await
              gather(*(loop.run_in_executor(tp, trim_resource, v, scope)
                       for v in resources.values()))))

    # Run trimming tasks concurrently in tp
    trimmed_default, trimmed_detailed = await gather(trim_all(Scope.DEFAULT),
                                                     trim_all(Scope.DETAILED))

    _resources_by_scope[Scope.ALL] = resources
    _resources_by_scope[Scope.DEFAULT] = trimmed_default
    _resources_by_scope[Scope.DETAILED] = trimmed_detailed
    _resources_cached_at = now()

  cached_resource = _resources_by_scope.get(scope)
  if cached_resource is None:
    return f"Unsupported resource scope: {scope}", {}

  return "", cached_resource


async def parse_resources(
    resources: str,
    scope: Scope = Scope.DEFAULT) -> ServiceResponse[list[str]]:
  """Parse and validate resource strings"""
  try:
    resource_list = split(resources) or ['*']
    if resource_list[0] in ["all", "*"]:
      err, res = await get_resources(scope)
      if err:
        return err, []
      resource_list = list(res.keys())
    else:
      # Ensure resources are initialized and valid for scope
      if _resources_by_scope.get(scope) is None:
        log_info(f"Initializing resources for scope: {scope}")
        err, _ = await get_resources(scope
                                     )  # Initialize cache if not already done
        if err:
          return "Problem initializing resources", []
      cached_resources = _resources_by_scope.get(scope)
      if cached_resources is None:
        return "No resources found", []
      resource_list = [
          resource for resource in resource_list
          if resource in cached_resources
      ]
    if not resource_list:
      return "No resources found", []
    return "", resource_list
  except Exception as e:
    return f"Error parsing resources: {e}", []


# TODO: merge with get_schema
async def parse_fields(resource: str,
                       fields: str,
                       scope: Scope = Scope.ALL) -> ServiceResponse[list[str]]:
  """Parse and validate fields for a resource"""
  try:
    field_list = split(fields) or ['*']
    err, resources = await get_resources(scope)
    if err:
      return err, []
    if resource in resources:
      found = resources[resource].get('fields', {})
      if not found:
        return f"Fields not found: {resource}.{fields}", []
      filtered = [field for field in field_list if field in found
                  ] if field_list[0] not in ["all", "*"] else found
      return "", (list(filtered.keys())
                  if isinstance(filtered, dict) else filtered)
    return f"Resource not found: {resource}", []
  except Exception as e:
    return f"Error parsing fields: {resource}.{fields} - {e}", []


async def parse_resources_fields(
    resources: str,
    fields: str,
    scope: Scope = Scope.ALL) -> ServiceResponse[tuple[list[str], list[str]]]:

  err, parsed_resources = await parse_resources(resources, scope)
  if err:
    return err, ([], [])
  err, parsed_fields = await parse_fields(parsed_resources[0], fields, scope)
  if err:
    return err, ([], [])
  return "", (parsed_resources, parsed_fields)


async def get_schema(resources: Optional[list[str]] = None,
                     fields: Optional[list[str]] = None,
                     scope: Scope = Scope.ALL) -> ServiceResponse[dict]:

  err, resources_data = await get_resources(scope)
  if err:
    return err, {}

  partial = resources_data.copy()
  if resources:
    if len(resources) > 1:
      partial = {
          resource: partial[resource]
          for resource in resources if resource in partial
      }
    else:
      partial = partial.get(resources[0], {})

  if fields:
    filtered = {}
    for resource, resource_data in partial.items():
      filtered[resource] = {
          field: resource_data[field]
          for field in fields if field in resource_data
      }
    partial = filtered

  return "", partial


def format_table(data,
                 from_format: DataFormat = "py:row",
                 to_format: DataFormat = "py:column",
                 columns: Optional[list[str]] = None) -> ServiceResponse[Any]:

  try:
    # Check if data is empty
    if isinstance(data, pl.DataFrame):
      empty = data.is_empty()
    elif isinstance(data, pl.Series):
      empty = data.is_empty()
    else:
      empty = data in [None, "", [], {}, ()] if data is not None else True
  except Exception:
    # Fallback if data doesn't have is_empty method or isn't comparable
    empty = data in [None, "", [], {}, ()] if data is not None else True

  if empty:
    return "No dataset to format", None

  from_fmt = UNALIASED_FORMATS.get(from_format, from_format)
  to_fmt = UNALIASED_FORMATS.get(to_format, to_format)
  # Validate inputs - only return early if no conversion is needed
  needs_conversion = (isinstance(data, pl.Series)
                      and to_fmt == "polars") or columns
  if from_fmt == to_fmt and not needs_conversion:
    return "", data

  # Load JSON if needed
  if from_fmt and from_fmt.startswith("json"):
    if isinstance(data, str):
      data = orjson.loads(data)
    # Safe replacement for JSON formats
    if from_fmt == "json:row":
      from_fmt = "py:row"
    elif from_fmt == "json:column":
      from_fmt = "py:column"
    else:
      from_fmt = "py:row"  # default fallback
  try:
    # Convert to Polars DataFrame
    match from_fmt:
      case "py:row":
        df = pl.DataFrame(data, orient="row")
      case "py:column":
        if isinstance(data, pl.DataFrame):
          df = data
        else:
          df = pl.DataFrame(data)
      case "csv":
        df = pl.read_csv(data, separator=',', has_header=True)
      case "tsv":
        df = pl.read_csv(data, separator='\t', has_header=True)
      case "psv":
        df = pl.read_csv(data, separator='|', has_header=True)
      case "parquet":
        df = pl.read_parquet(data)
      case "arrow":
        result = pl.from_arrow(data)
        df = result if isinstance(result, pl.DataFrame) else result.to_frame()
      case "feather":
        df = pl.read_ipc(data)
      # case "orc": df = pl.read_orc(data) # not supported
      case "avro":
        df = pl.read_avro(data)
      case "polars":
        if isinstance(data, pl.DataFrame):
          df = data
        elif isinstance(data, pl.Series):
          df = data.to_frame()
        else:
          df = pl.DataFrame(data)
      case _:
        return f"Unsupported from_format: {from_fmt}", None

    # Assign column names if provided
    if columns:
      df = df.rename(dict(zip(df.columns, columns)))

    # Convert timestamps to milliseconds since epoch for (JS Date compatible)
    if to_fmt and not to_fmt.startswith(("py:", "polars", "np:", "pd:")):
      if 'ts' in df.columns:
        df = df.with_columns([
            (pl.col('ts').cast(pl.Datetime).dt.timestamp(time_unit="ms")).cast(
                pl.Int64).alias('ts')
        ])

    # Export from Polars DataFrame
    match to_fmt:
      case "polars":
        data = df
      case "py:row":
        data = df.to_numpy().tolist()
      case "py:column":
        data = df.to_numpy().T.tolist()
      case "np:row":
        data = df.to_numpy()
      case "np:column":
        data = df.to_numpy().T
      # case "json:row:labelled": data = orjson.dumps(df.to_dicts(), option=ORJSON_OPTIONS)
      # case "json:column:labelled": data = orjson.dumps({col: df[col].to_list() for col in df.columns}, option=ORJSON_OPTIONS)
      case "json:row":
        data = orjson.dumps(
            {
                'columns': df.columns,
                'types': [str(t).lower() for t in df.dtypes],
                'data': df.to_numpy().tolist()
            },
            option=ORJSON_OPTIONS)
      case "json:column":
        data = orjson.dumps(
            {
                'columns': df.columns,
                'types': [str(t).lower() for t in df.dtypes],
                'data': df.to_numpy().T.tolist()
            },
            option=ORJSON_OPTIONS)
      # TODO: determine float precision based on content?
      case "csv":
        data = df.write_csv(separator=',',
                            include_header=True,
                            line_terminator='\n',
                            float_precision=9)
      case "tsv":
        data = df.write_csv(separator='\t',
                            include_header=True,
                            line_terminator='\n',
                            float_precision=9)
      case "psv":
        data = df.write_csv(separator='|',
                            include_header=True,
                            line_terminator='\n',
                            float_precision=9)
      case "parquet":
        buf = BytesIO()
        df.write_parquet(buf, compression="zstd")
        data = buf.getvalue()
      case "arrow":
        data = df.to_arrow()
      case "feather":
        buf = BytesIO()
        df.write_ipc(buf, compression="zstd")
        data = buf.getvalue()
      # case "orc": data = df.write_orc() # not supported
      case "avro":
        buf = BytesIO()
        df.write_avro(buf, compression="snappy")
        data = buf.getvalue()
      case "np:row":
        data = df.to_numpy()
      case "np:column":
        data = df.to_numpy().T
      case _:
        return f"Unsupported to_format: {to_fmt}", None

  except Exception as e:
    return f"Error formatting table: {e}", None
  return "", data


async def get_last_values(resources: list[str],
                          quote: Optional[str] = None,
                          precision: int = 6) -> ServiceResponse[dict]:
  """Get latest values for resources with optional quote conversion"""
  res = await get_cache_batch(resources, pickled=True) if len(resources) > 1 else \
        {resources[0]: await get_cache(resources[0], pickled=True)}

  missing_resources = [
      resource for resource, value in res.items() if value is None
  ]
  if missing_resources:
    return f"Resources not found: {', '.join(missing_resources)}", {}

  if quote and quote != "USDC.idx":
    try:
      quote_resource, quote_field = quote.split('.', 1)
      quote_data = await get_cache(quote_resource, pickled=True)
      if quote_data is None:
        return f"Quote resource not found: {quote_resource}", {}
      quote_value = quote_data.get(quote_field)
      if quote_value is None or math.isnan(quote_value):
        return f"Quote field not found or NaN: {quote}", {}

      for resource in res:
        res[resource] = {
            k:
            round_sigfig(v *
                         quote_value, precision) if isinstance(v, float) else v
            for k, v in res[resource].items()
        }
    except ValueError:
      return "Quote must be in format resource.field", {}

  quote = quote or "USDC.idx"
  for resource in res:
    res[resource]["quote"] = quote
    res[resource]["precision"] = precision
  return "", res


async def get_history(
    resources: list[str],
    fields: list[str],
    from_date: datetime,
    to_date: datetime,
    interval: Interval,
    quote: Optional[str] = None,
    precision: int = 6,
    format: DataFormat = "json:row",
    fill_mode: FillMode = "forward_fill",
    truncate_leading_zeros: bool = True) -> ServiceResponse[Any]:
  """Get historical data with optional quote conversion and fill mode"""

  # try:
  # Fetch base data
  base_columns, base_data = await state.tsdb.fetch_batch(
      tables=resources,
      from_date=from_date,
      to_date=to_date,
      aggregation_interval=interval,
      columns=fields or [])
  if not base_data:
    return "No data found", None

  # Debug: Log the structure of base_data to understand the issue
  # if state.args.verbose:
  log_debug(f"base_data type: {type(base_data)}")
  log_debug(f"base_data length: {len(base_data) if base_data else 0}")
  if base_data and len(base_data) > 0:
    log_debug(f"base_data[0] type: {type(base_data[0])}")
    if hasattr(base_data[0], '__len__'):
      log_debug(f"base_data[0] length: {len(base_data[0])}")
    log_debug(f"base_data[0] content: {base_data[0]}")

  # Filter out rows where ts is null before creating DataFrame
  filtered_base_data = []
  for row in base_data:
    # Handle different row data formats from various databases
    if isinstance(row, (list, tuple)):
      # Row is a sequence (tuple/list) - standard format
      if row[0] is not None:
        filtered_base_data.append(row)
    else:
      # Row might be an object or single value - this can happen with some database drivers
      # Try to convert to a proper tuple format
      try:
        if hasattr(row, '__iter__') and not isinstance(row, (str, bytes)):
          # Row is iterable but not a string/bytes - convert to list
          row_list = list(row)
          if row_list and row_list[0] is not None:
            filtered_base_data.append(tuple(row_list))
        else:
          # Single value case - could be just a timestamp
          if row is not None:
            filtered_base_data.append((row, ))
      except Exception:
        # If all else fails, skip this row
        continue

  # Explicitly specify orientation and ensure datetime conversion
  df = pl.DataFrame(filtered_base_data, schema=base_columns, orient="row")

  # Get numeric columns for interpolation
  numeric_cols = numeric_columns(df)

  # Interpolate numeric columns based on fill mode
  if numeric_cols:
    if fill_mode and fill_mode != "none":
      df = df.with_columns([
          getattr(pl.col(col).cast(pl.Float64), fill_mode)().alias(
              col)  # fails if fill_mode is not a valid Polars method
          for col in numeric_cols
      ])

  # Handle quote conversion if needed
  if quote and quote != "USDC.idx":
    quote_resource, quote_field = quote.split('.', 1)
    quote_columns, quote_data = await state.tsdb.fetch(
        table=quote_resource,
        from_date=from_date,
        to_date=to_date,
        aggregation_interval=interval,
        columns=['ts', quote_field])
    if not quote_data:
      return "No quote data found", None

    filtered_quote_data = []
    for row in quote_data:
      # Handle different row data formats from various databases
      if isinstance(row, (list, tuple)):
        # Row is a sequence (tuple/list) - standard format
        if row[0] is not None:
          filtered_quote_data.append(row)
      else:
        # Row might be an object or single value - this can happen with some database drivers
        # Try to convert to a proper tuple format
        try:
          if hasattr(row, '__iter__') and not isinstance(row, (str, bytes)):
            # Row is iterable but not a string/bytes - convert to list
            row_list = list(row)
            if row_list and row_list[0] is not None:
              filtered_quote_data.append(tuple(row_list))
          else:
            # Single value case - could be just a timestamp
            if row is not None:
              filtered_quote_data.append((row, ))
        except Exception:
          # If all else fails, skip this row
          continue

    quote_col_id = f'{quote_resource}.{quote_field}'
    # Create quote DataFrame with prefixed column names
    quote_df = pl.DataFrame(
        filtered_quote_data,
        schema=['ts', quote_col_id],  # Prefix the quote field column
        orient="row")

    # Interpolate quote values with prefixed name
    quote_df = quote_df.with_columns([
        pl.col(quote_col_id).cast(pl.Float64).interpolate().alias(quote_col_id)
    ])

    # Join the dataframes on timestamp and perform multiplication using prefixed quote column
    df = df.join(quote_df, on="ts", how="left").with_columns([
        (pl.col(col) / pl.col(quote_col_id)).round(precision).alias(
            col)  # double inversion to denominate in quote
        for col in numeric_cols
    ]).drop(quote_col_id)

  # Filter out rows where all non-timestamp numeric columns are null/zero
  if numeric_cols and truncate_leading_zeros:
    non_ts_numeric_cols = [col for col in numeric_cols if col != 'ts']
    if non_ts_numeric_cols:
      non_empty_mask = df.select([
          pl.any_horizontal([(pl.col(col).is_not_null() & (pl.col(col) != 0))
                             for col in non_ts_numeric_cols]).alias("mask")
      ])
      first_valid_idx = df.filter(
          non_empty_mask["mask"]).with_row_index().select("index").row(
              0)[0] if df.filter(non_empty_mask["mask"]).height > 0 else 0
      df = df.slice(first_valid_idx)

  return format_table(df,
                      from_format="polars",
                      to_format=format,
                      columns=base_columns)

  # except Exception as e:
  #   return f"Error processing data: {e}", None
