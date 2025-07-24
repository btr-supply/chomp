import orjson
import math
import polars as pl
from datetime import datetime
from asyncio import gather, get_running_loop
from io import BytesIO
from typing import Any, cast, Optional, Union
from concurrent.futures import Executor
from fastapi import HTTPException

from ..server.responses import ORJSON_OPTIONS
from ..cache import get_cache_batch, get_cache, get_resource_status
from ..utils import round_sigfig, split, Interval, numeric_columns, log_debug, log_warn, log_error
from .. import state
from ..models import SCOPES, UNALIASED_FORMATS, FillMode, Scope, DataFormat
from ..utils.decorators import service_method, cache as _cache


@_cache(ttl=300, maxsize=1)
async def _get_all_resources() -> dict[Scope, dict[str, Any]]:
  """
    Fetches all resources and prepares them for different scopes, caching the result.
    This is an internal function.
    """
  resources = await get_resource_status()
  tp = cast(Executor,
            state.thread_pool)  # Cast to Executor for type compatibility
  loop = get_running_loop()

  async def trim_all(scope: Scope, has_auth: bool = True):
    return dict(
        zip(
            resources.keys(), await gather(
                *(loop.run_in_executor(tp, trim_resource, v, scope, has_auth)
                  for v in resources.values()))))

  # Run trimming tasks concurrently in tp for authenticated-like view
  trimmed_default, trimmed_detailed = await gather(
      trim_all(Scope.DEFAULT, True), trim_all(Scope.DETAILED, True))

  return {
      Scope.ALL: resources,
      Scope.DEFAULT: trimmed_default,
      Scope.DETAILED: trimmed_detailed
  }


def trim_resource(resource: dict,
                  scope: Scope,
                  is_admin: bool = False) -> Optional[dict]:
  """
  Elegant resource filtering based on scope and admin status.

  Args:
    resource: Resource dictionary to filter
    scope: Scope mask determining what fields to include
    is_admin: Whether the user has admin privileges

  Returns:
    Filtered resource dictionary or None if resource should be excluded
  """
  try:
    # Skip protected resources for non-admin users
    if resource.get("protected", False) and not is_admin:
      return None

    # Filter resource fields based on scope
    filtered_resource = {"name": resource.get("name", "")}

    # Apply scope-based field filtering
    for flag, attr in SCOPES.items():
      if scope & flag and attr in resource:
        filtered_resource[attr] = resource[attr]

    # Handle field-level filtering for resources with fields
    if "fields" in resource and isinstance(resource["fields"], dict):
      filtered_fields = {}
      for field_name, field_data in resource["fields"].items():
        # Skip transient fields unless TRANSIENT scope is included
        if field_data.get("transient",
                          False) and not (scope & Scope.TRANSIENT):
          continue

        # Apply scope filtering to individual fields
        filtered_field = {"name": field_data.get("name", field_name)}
        for flag, attr in SCOPES.items():
          if scope & flag and attr in field_data:
            filtered_field[attr] = field_data[attr]

        # Always include basic field info
        for basic_attr in ["type", "tags"]:
          if basic_attr in field_data:
            filtered_field[basic_attr] = field_data[basic_attr]

        filtered_fields[field_name] = filtered_field

      if filtered_fields:
        filtered_resource["fields"] = filtered_fields

    return filtered_resource

  except Exception as e:
    log_error(f"Error filtering resource: {e}")
    return None


async def get_resources(scope: Scope = Scope.DEFAULT, request=None) -> dict:
  """Get resources, filtered by scope and authentication status."""
  has_auth = await _check_authentication(request) if request else True

  all_resources = await _get_all_resources()
  scoped_resources = all_resources.get(scope)

  if scoped_resources is None:
    log_error(f"Unsupported resource scope: {scope}")
    raise ValueError(f"Unsupported resource scope: {scope}")

  # Apply resource-level protection filtering for unauthenticated users
  if not has_auth:
    return {
        name: resource
        for name, resource in scoped_resources.items()
        if resource and not resource.get("protected", False)
    }

  return scoped_resources


@service_method("parse resources")
async def parse_resources(resources: Union[str, list[str]],
                          scope: Scope = Scope.DEFAULT,
                          request=None) -> list[str]:
  """Parse and validate resource strings with protection filtering"""
  resource_list = split(resources) if isinstance(resources,
                                                 str) else resources or ['*']
  if resource_list[0] in ["all", "*"]:
    res = await get_resources(scope, request)
    resource_list = list(res.keys())
  else:
    # Ensure resources are initialized and valid for scope
    available_resources = await get_resources(scope, request)
    resource_list = [
        resource for resource in resource_list
        if resource in available_resources
    ]

  if not resource_list:
    log_warn(f"No resources found for query: {resources}")
    raise ValueError("No resources found")

  return resource_list


async def is_resource_protected(resource_name: str) -> bool:
  """Check if a resource is protected based on resource attributes"""
  try:
    all_resources = await _get_all_resources()
    cached_resources = all_resources.get(Scope.ALL)
    if not cached_resources:
      return False

    if resource_name in cached_resources:
      resource_data = cached_resources[resource_name]
      return resource_data.get("protected", False)
    return False
  except Exception as e:
    log_warn(f"Error checking protected resource {resource_name}: {e}")
    return False


async def _check_authentication(request) -> bool:
  """Check if request has valid authentication"""
  try:
    # Get authorization header
    auth_header = request.headers.get("Authorization") if getattr(
        request, 'headers', None) else ""
    if not auth_header or not auth_header.startswith("Bearer "):
      return False

    token = auth_header[7:]  # Remove "Bearer " prefix

    # Extract user ID from request (could be IP, session, etc.)
    uid = request.state.user.uid
    # Import AuthService here to avoid circular imports
    from ..services.auth import AuthService

    # Verify session
    try:
      valid = await AuthService.verify_and_renew_session(uid, token)
      if not valid:
        log_warn(f"Authentication failed for {uid}")
        return False

      if state.args.verbose:
        log_debug(f"Authentication successful for {uid}")
      return True
    except ValueError as e:
      log_warn(f"Authentication failed for {uid}: {e}")
      return False

  except Exception as e:
    log_warn(f"Error during authentication check: {e}")
    return False


@service_method("parse fields")
async def parse_fields(resource: str,
                       fields: Union[str, list[str]],
                       scope: Scope = Scope.ALL,
                       request=None) -> list[str]:
  """Parse and validate fields for a resource"""
  field_list = split(fields) if isinstance(fields, str) else fields or ['*']
  resources = await get_resources(scope, request)
  found = resources[resource].get('fields',
                                  {}) if resource in resources else None
  if not found:
    log_warn(f"Resource or fields not found: {resource}")
    raise ValueError(f"Resource not found: {resource}")

  if field_list[0] in ["all", "*"]:
    filtered = found
  else:
    filtered = [field for field in field_list if field in found]

  result = list(filtered.keys()) if isinstance(filtered, dict) else filtered
  return result


async def parse_resources_fields(resources: Union[str, list[str]],
                                 fields: Union[str, list[str]],
                                 scope: Scope = Scope.ALL,
                                 request=None) -> tuple[list[str], list[str]]:

  parsed_resources = await parse_resources(resources, scope, request)
  field_tasks = [
      parse_fields(resource, fields, scope, request)
      for resource in parsed_resources
  ]
  parsed_fields = list(set(await gather(*field_tasks))) or []
  return (parsed_resources, parsed_fields)


async def get_schema(resources: Optional[list[str]] = None,
                     fields: Optional[list[str]] = None,
                     scope: Scope = Scope.ALL,
                     request=None) -> dict:

  resources_data = await get_resources(scope, request)

  partial = resources_data
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

  return partial


def _is_empty(data: Any) -> bool:
  """Check if data is empty, handling various data types efficiently"""
  if data is None:
    return True

  try:
    if isinstance(data, pl.DataFrame):
      return data.is_empty()
    elif isinstance(data, pl.Series):
      return data.is_empty()
    else:
      return data in ["", [], {}, ()]
  except Exception:
    # Fallback for edge cases
    return data in [None, "", [], {}, ()]


def format_table(data,
                 from_format: DataFormat = "py:row",
                 to_format: DataFormat = "py:column",
                 columns: Optional[list[str]] = None) -> Any:
  """Format data table between different formats with comprehensive type support"""

  # Check if data is empty
  if _is_empty(data):
    log_warn("No dataset to format")
    raise ValueError("No dataset to format")

  from_fmt = UNALIASED_FORMATS.get(from_format, from_format)
  to_fmt = UNALIASED_FORMATS.get(to_format, to_format)
  # Validate inputs - only return early if no conversion is needed
  needs_conversion = (isinstance(data, pl.Series)
                      and to_fmt == "polars") or columns
  if from_fmt == to_fmt and not needs_conversion:
    return data

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
      log_error(f"Unsupported from_format: {from_fmt}")
      raise ValueError(f"Unsupported from_format: {from_fmt}")

  # Assign column names if provided
  if columns:
    df = df.rename(dict(zip(df.columns, columns)))

  # Convert timestamps to milliseconds since epoch for (JS Date compatible)
  if to_fmt and not to_fmt.startswith(("py:", "polars", "np:", "pd:")):
    if 'ts' in df.columns:
      df = df.with_columns([(pl.col('ts').cast(
          pl.Datetime).dt.timestamp(time_unit="ms")).cast(pl.Int64).alias('ts')
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
      log_error(f"Unsupported to_format: {to_fmt}")
      raise ValueError(f"Unsupported to_format: {to_fmt}")

  return data


@service_method("get last values")
async def get_last_values(resources: list[str],
                          quote: Optional[str] = None,
                          precision: int = 6) -> dict:
  """Get latest values for resources with optional quote conversion"""
  res = await get_cache_batch(resources, pickled=True) if len(resources) > 1 else \
        {resources[0]: await get_cache(resources[0], pickled=True)}

  missing_resources = [
      resource for resource, value in res.items() if value is None
  ]
  if missing_resources:
    log_warn(f"Resources not found: {', '.join(missing_resources)}")
    raise ValueError(f"Resources not found: {', '.join(missing_resources)}")

  if quote and quote != "USDC.idx":
    quote_resource, quote_field = quote.split('.', 1)
    quote_data = await get_cache(quote_resource, pickled=True)
    if quote_data is None:
      log_warn(f"Quote resource not found: {quote_resource}")
      raise ValueError(f"Quote resource not found: {quote_resource}")

    quote_value = quote_data.get(quote_field)
    if quote_value is None or math.isnan(quote_value):
      log_warn(f"Quote field not found or NaN: {quote}")
      raise ValueError(f"Quote field not found or NaN: {quote}")

    # Convert all numeric values using quote
    for resource in resources:
      if res[resource]:
        res[resource] = {
            k:
            round_sigfig(v *
                         quote_value, precision) if isinstance(v, float) else v
            for k, v in res[resource].items()
        }

  quote = quote or "USDC.idx"
  for resource in res:
    res[resource]["quote"] = quote
    res[resource]["precision"] = precision

  # Round all float values to specified precision
  # for resource in resources:
  #   if res[resource]:
  #     res[resource] = {
  #         k: round_sigfig(v, precision) if isinstance(v, float) else v
  #         for k, v in res[resource].items()
  #     }

  return res


@service_method("get historical data")
async def get_history(resources: list[str],
                      fields: list[str],
                      from_date: datetime,
                      to_date: datetime,
                      interval: Interval,
                      quote: Optional[str] = None,
                      precision: int = 6,
                      format: DataFormat = "json:row",
                      fill_mode: FillMode = "forward_fill",
                      truncate_leading_zeros: bool = True) -> Any:
  """Get historical data for resources with optional quote conversion"""

  # Fetch base data
  base_columns, base_data = await state.tsdb.fetch_batch(
      tables=resources,
      from_date=from_date,
      to_date=to_date,
      aggregation_interval=interval,
      columns=fields or [])

  if not base_data:
    log_warn(
        f"No data found for resources {resources} from {from_date} to {to_date}"
    )
    raise HTTPException(status_code=404, detail="No data found")

  def _process_row(row):
    """Helper to normalize database row format"""
    if isinstance(row, (list, tuple)):
      return row if row[0] is not None else None
    elif hasattr(row, '__iter__') and not isinstance(row, (str, bytes)):
      try:
        row_list = list(row)
        return tuple(
            row_list) if row_list and row_list[0] is not None else None
      except Exception:
        return None
    else:
      return (row, ) if row is not None else None

  # Filter out rows where ts is null before creating DataFrame
  filtered_base_data = [
      processed_row for row in base_data
      if (processed_row := _process_row(row)) is not None
  ]

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
      log_warn(f"No quote data found for {quote_resource}")
      raise ValueError("No quote data found")

    filtered_quote_data = [
        processed_row for row in quote_data
        if (processed_row := _process_row(row)) is not None
    ]

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
