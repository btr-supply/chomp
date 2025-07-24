from typing import Optional
from fastapi import APIRouter, Request, HTTPException, Query, Path
from fastapi.responses import HTMLResponse
import re

from ..responses import ApiResponse
from ..routes import Route
from ...models.base import DataFormat, Interval
from ...utils.date import fit_date_params, now
from ...utils.format import load_template
from ...services import config as config_service, loader as loader_service, converter
from ...services.limiter import RateLimiter
from ...services.auth import AuthService
from ...actions.load import load_resource
from ... import state

router = APIRouter(tags=["retriever"])

_index_html = load_template("index.html")
_docs_html = load_template("docs.html")


async def _get_analysis_placeholder(req: Request, analysis_type: str,
                                    from_date: Optional[str],
                                    to_date: Optional[str], periods: str):
  """Helper function to handle duplicated analysis endpoint logic."""
  from_date, to_date, _, _ = fit_date_params(from_date, to_date)
  return ApiResponse({
      "message": f"{analysis_type.capitalize()} endpoint placeholder",
      "resources": req.state.resources,
      "fields": req.state.fields,
      "from_date": from_date,
      "to_date": to_date,
      "periods": periods
  })


@router.get(Route.ROOT.endpoint, status_code=200)
@router.get(Route.INFO.endpoint, status_code=200)
async def get_info(req: Request) -> ApiResponse:
  """Get server information and status."""

  # Get project info from pyproject.toml
  engine = state.meta.name
  version = state.meta.version

  # Get server config info
  config = state.server_config
  name = config.name if config else "Chomp Data Ingester"
  description = config.description if config else "High-performance data ingestion framework"

  return ApiResponse({
      "engine": engine,
      "version": version,
      "name": name,
      "description": description,
      "status": "ok",
      "timestamp": now().isoformat()
  })


@router.get(Route.DOCS.endpoint)
async def get_docs():
  return HTMLResponse(_docs_html)


@router.get(Route.PING.endpoint, status_code=200)
async def ping(req: Request) -> ApiResponse:
  """Health check endpoint."""
  return ApiResponse({"status": "ok", "timestamp": now().isoformat()})


@router.get(Route.ANALYSIS.endpoint, status_code=200)
async def get_analysis_data(
    req: Request,
    resource: str = Query(..., description="Resource name"),
    analysis_type: str = Query("summary", description="Type of analysis"),
    from_date: Optional[str] = Query(None,
                                     description="Start date (ISO format)"),
    to_date: Optional[str] = Query(None, description="End date (ISO format)")
) -> ApiResponse:
  """Get analysis data for a specific resource."""
  # Use a simpler approach since get_analysis_data method doesn't exist
  data = {
      "resource": resource,
      "analysis_type": analysis_type,
      "from_date": from_date,
      "to_date": to_date,
      "message": "Analysis endpoint placeholder"
  }

  return ApiResponse(data)


@router.get(Route.STATUS.endpoint, status_code=200)
async def get_status(req: Request) -> ApiResponse:
  """Get system status and health information."""
  status = await config_service.get_system_status()
  return ApiResponse(status)


@router.get(Route.SCHEMA.endpoint)
@router.get(Route.SCHEMA_WITH_RESOURCES.endpoint)
async def get_schema_with_filter(req: Request,
                                 name: Optional[str] = None,
                                 tag: Optional[str] = None,
                                 ingester_type: Optional[str] = None,
                                 resource_type: Optional[str] = None,
                                 field_type: Optional[str] = None):
  """Get resource schema with optional filtering and automatic scope-based security.

  Query Parameters:
  - name: Regex pattern for ingester names (e.g., ".*Feed.*")
  - tag: Regex pattern for tags (e.g., "CEX|DEX")
  - ingester_type: Exact match for ingester type (e.g., "http_api")
  - resource_type: Exact match for resource type (e.g., "timeseries")
  - field_type: Regex pattern for field types (e.g., "float.*")

  Security: Non-admin users get DEFAULT scope (target only), admin users can request any scope via query params.
  """
  # Get parsed resources, fields and scope from request state (set by middleware)
  parsed_resources = req.state.resources
  parsed_fields = req.state.fields
  scope = req.state.scope

  # Get schema data
  schema_data = await loader_service.get_schema(parsed_resources,
                                                parsed_fields, scope)

  # Apply additional filters if any are specified
  has_filters = any([name, tag, ingester_type, resource_type, field_type])

  if has_filters:
    filtered_data = {}

    for resource_name, resource_config in schema_data.items():
      # Apply name filter first (most common filter)
      if name and not re.search(name, resource_name):
        continue

      # Apply type filters (exact matches - faster than regex)
      if ingester_type and resource_config.get(
          "ingester_type") != ingester_type:
        continue

      if resource_type and resource_config.get(
          "resource_type") != resource_type:
        continue

      # Apply tag filter (regex on tags)
      if tag:
        resource_tags = resource_config.get("tags", [])
        if not any(re.search(tag, str(t)) for t in resource_tags):
          continue

      # Apply field type filter (most expensive - do last)
      if field_type and "fields" in resource_config:
        filtered_fields = {}
        for field_name, field_config in resource_config["fields"].items():
          field_type_value = field_config.get("type", "")
          if re.search(field_type, str(field_type_value)):
            filtered_fields[field_name] = field_config

        if filtered_fields:
          # Create shallow copy to avoid mutating original
          resource_config = {**resource_config, "fields": filtered_fields}
          filtered_data[resource_name] = resource_config
      else:
        filtered_data[resource_name] = resource_config

    schema_data = filtered_data

  return ApiResponse(schema_data)


@router.get(Route.LAST_WITH_RESOURCES.endpoint)
@router.get(Route.LAST.endpoint)
async def get_last(req: Request,
                   quote: Optional[str] = None,
                   precision: int = 6):
  last_values = await loader_service.get_last_values(req.state.resources,
                                                     quote, precision)
  return ApiResponse(last_values if len(req.state.resources) >
                     1 else last_values[req.state.resources[0]])


@router.get(Route.HISTORY_WITH_RESOURCES.endpoint)
@router.get(Route.HISTORY.endpoint)
async def get_history(req: Request,
                      from_date=None,
                      to_date=None,
                      interval: Optional[Interval] = None,
                      target_epochs: Optional[int] = None,
                      precision: int = 6,
                      quote: Optional[str] = None,
                      format: DataFormat = "json:row"):
  from_date, to_date, interval, target_epochs = fit_date_params(
      from_date, to_date, interval, target_epochs)

  data = await loader_service.get_history(req.state.resources,
                                          req.state.fields, from_date, to_date,
                                          interval, quote, precision, format)
  return ApiResponse(data)


@router.get(Route.CONVERT_WITH_PAIR.endpoint)
@router.get(Route.CONVERT.endpoint)
async def get_convert(req: Request,
                      pair: str,
                      base_amount: Optional[float] = None,
                      quote_amount: Optional[float] = None,
                      precision: int = 6):
  result = await converter.convert(pair, base_amount, quote_amount, precision)
  return ApiResponse(result)


@router.get(Route.PEGCHECK_WITH_PAIR.endpoint)
@router.get(Route.PEGCHECK.endpoint)
async def get_pegcheck(req: Request,
                       pair: str,
                       factor: float = 1.0,
                       max_deviation: float = .002,
                       precision: int = 6):
  result = await converter.pegcheck(pair, factor, max_deviation, precision)
  return ApiResponse(result)


@router.get(Route.LIMITS.endpoint)
async def get_limits(req: Request):
  requester_id = AuthService.client_uid(req)
  user_limits = await RateLimiter.get_user_limits(requester_id)
  return ApiResponse(user_limits)


@router.get(Route.ANALYSIS_WITH_RESOURCES.endpoint)
@router.get(Route.ANALYSIS.endpoint)
async def get_analysis(req: Request,
                       from_date=None,
                       to_date=None,
                       periods: str = "20",
                       precision: int = 6,
                       quote: Optional[str] = None,
                       format: DataFormat = "json:row"):
  return await _get_analysis_placeholder(req, "analysis", from_date, to_date,
                                         periods)


@router.get(Route.VOLATILITY_WITH_RESOURCES.endpoint)
@router.get(Route.VOLATILITY.endpoint)
async def get_volatility(req: Request,
                         from_date=None,
                         to_date=None,
                         periods: str = "20",
                         precision: int = 6,
                         quote: Optional[str] = None,
                         format: DataFormat = "json:row"):
  return await _get_analysis_placeholder(req, "volatility", from_date, to_date,
                                         periods)


@router.get(Route.TREND_WITH_RESOURCES.endpoint)
@router.get(Route.TREND.endpoint)
async def get_trend(req: Request,
                    from_date=None,
                    to_date=None,
                    periods: str = "20",
                    precision: int = 6,
                    quote: Optional[str] = None,
                    format: DataFormat = "json:row"):
  return await _get_analysis_placeholder(req, "trend", from_date, to_date,
                                         periods)


@router.get(Route.MOMENTUM_WITH_RESOURCES.endpoint)
@router.get(Route.MOMENTUM.endpoint)
async def get_momentum(req: Request,
                       from_date=None,
                       to_date=None,
                       periods: str = "20",
                       precision: int = 6,
                       quote: Optional[str] = None,
                       format: DataFormat = "json:row"):
  return await _get_analysis_placeholder(req, "momentum", from_date, to_date,
                                         periods)


@router.get(Route.OPRANGE_WITH_RESOURCES.endpoint)
@router.get(Route.OPRANGE.endpoint)
async def get_oprange(req: Request,
                      from_date=None,
                      to_date=None,
                      precision: int = 6,
                      quote: Optional[str] = None,
                      format: DataFormat = "json:row"):
  # Return placeholder since get_oprange doesn't exist or has wrong signature
  return ApiResponse({
      "message": "Oprange endpoint placeholder",
      "resources": req.state.resources,
      "fields": req.state.fields,
      "precision": precision,
      "quote": quote
  })


@router.get(Route.LIST_RESOURCE.endpoint, status_code=200)
async def get_resource_list(
    req: Request,
    resource: str = Path(..., description="Resource name (e.g., 'user')"),
    limit: int = Query(100,
                       ge=1,
                       le=1000,
                       description="Maximum number of records"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    include_transient: bool = Query(False,
                                    description="Include transient fields")
) -> ApiResponse:
  """Get a list of records for a specific resource with pagination."""

  # Use scope from middleware (already validated)
  scope = req.state.scope

  # Load resources with proper scope
  # Use the unified load_resource function
  records = await load_resource(resource, limit=limit, offset=offset)

  # Format response
  total_count = len(records) if isinstance(records, list) else 0

  return ApiResponse({
      "resource":
      resource,
      "records":
      [r.to_dict() if hasattr(r, 'to_dict') else r
       for r in records] if records else [],
      "pagination": {
          "limit": limit,
          "offset": offset,
          "count": total_count,
          "has_more": total_count == limit  # Approximate indicator
      },
      "scope":
      scope.name,
      "timestamp":
      now().isoformat()
  })


@router.get(Route.LIST_RESOURCES.endpoint, status_code=200)
async def get_batch_resource_list(
    req: Request,
    resources: str = Query(
        ..., description="Comma-separated list of resource names"),
    limit: int = Query(100,
                       ge=1,
                       le=1000,
                       description="Maximum number of records per resource"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    include_transient: bool = Query(False,
                                    description="Include transient fields")
) -> ApiResponse:
  """Get lists of records for multiple resources with pagination."""

  resource_names = [r.strip() for r in resources.split(",") if r.strip()]
  if not resource_names:
    raise HTTPException(status_code=400, detail="No resources specified")

  # Use scope from middleware (already validated)
  scope = req.state.scope

  # Load data for each resource
  result = {}

  for resource_name in resource_names:
    # Load resources with proper scope
    # Use the unified load_resource function
    records = await load_resource(resource_name, limit=limit, offset=offset)

    result[resource_name] = {
        "records": records,
        "count": len(records) if isinstance(records, list) else 0
    }

  return ApiResponse({
      "resources": result,
      "pagination": {
          "limit": limit,
          "offset": offset,
      },
      "scope": scope.name,
      "timestamp": now().isoformat()
  })


@router.get(Route.RESOURCE_BY_UID.endpoint, status_code=200)
async def get_resource_by_uid(
    req: Request,
    resource: str = Path(..., description="Resource name (e.g., 'user')"),
    uid: str = Path(..., description="Resource UID"),
    include_transient: bool = Query(False,
                                    description="Include transient fields")
) -> ApiResponse:
  """Get a specific resource record by UID."""

  # Use scope from middleware (already validated)
  scope = req.state.scope

  # Handle special case for user resources
  if resource.lower() == "user":
    # Use AuthService.get_user() for user resources, generic load for others
    user_result = await AuthService.get_user(uid)
    # Handle the fact that get_user returns dict[str, Any] | list[dict[str, Any]] | None
    if isinstance(user_result, list) and user_result:
      raw_record = user_result[0]  # Take first user if list
    elif isinstance(user_result, dict):
      raw_record = user_result
    else:
      raw_record = None
  else:
    # Generic resource loading
    raw_record = await load_resource(resource, uid=uid)

  if not raw_record:
    raise HTTPException(status_code=404,
                        detail=f"{resource.capitalize()} not found")

  # Convert to Resource format for consistent handling
  from ...models.base import Resource

  # Handle the different return types properly
  if resource.lower() == "user":
    # raw_record is now guaranteed to be a dict
    record_dict = raw_record
  else:
    # raw_record is already a dict from load_resource
    record_dict = raw_record if isinstance(raw_record, dict) else {
        "data": raw_record
    }

  record = Resource.from_record(record_dict,
                                resource_name=resource,
                                resource_type="update")

  # Apply field filtering based on scope
  record_data = record.to_dict(scope=scope)

  return ApiResponse({
      "resource": resource,
      "uid": uid,
      "data": record_data,
      "scope": scope.name,
      "timestamp": now().isoformat()
  })


@router.get(Route.RESOURCE_BY_UID_QUERY.endpoint, status_code=200)
async def get_resource_by_uid_query(
    req: Request,
    resource: str = Path(..., description="Resource name (e.g., 'user')"),
    uid: str = Query(..., description="Resource UID"),
    include_transient: bool = Query(False,
                                    description="Include transient fields")
) -> ApiResponse:
  """Get a specific resource record by UID via query parameter."""
  return await get_resource_by_uid(req, resource, uid, include_transient)
