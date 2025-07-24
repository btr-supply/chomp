"""
Admin router for configuration management and ingester control.
Provides endpoints for runtime configuration updates and ingester lifecycle management.
"""

from typing import Any, Optional
from fastapi import APIRouter, Request, HTTPException, Query
from pydantic import BaseModel

from ...services.admin import AdminService
from ..routes import Route
from ...utils.date import now
from ...utils import log_info, log_warn
from ...models.base import UserStatus
from ..responses import ApiResponse
from ...actions.load import load_resource
from ...models.user import User

router = APIRouter(prefix="/admin", tags=["admin"])


# User Management Models
class UpdateUserStatusRequest(BaseModel):
  user_id: str
  status: UserStatus


class UserListResponse(BaseModel):
  users: list[dict]
  total: int
  offset: int
  limit: int


# Simple test endpoint
@router.get(Route.ADMIN_TEST.endpoint, status_code=200, response_model=dict)
async def test_endpoint() -> dict:
  """Test endpoint to verify admin routes are accessible."""
  return {
      "message": "Admin test endpoint working",
      "timestamp": now().isoformat(),
      "status": "success"
  }


# Request/Response Models
# NB: Configuration-related models moved to /config router


class IngesterControlRequest(BaseModel):
  action: str  # "start", "stop", "restart"
  ingester_names: list[str]
  force: bool = False


class IngesterStatusResponse(BaseModel):
  name: str
  status: str  # "running", "stopped", "error", "starting", "stopping"
  uptime: Optional[int]  # seconds
  last_error: Optional[str]
  resource_usage: dict[str, Any]


# NB: Configuration management endpoints have been moved to /config router

# Ingester Control Endpoints


@router.get(Route.ADMIN_INGESTERS.endpoint)
async def get_ingester_status(req: Request) -> ApiResponse:
  """Get status of all ingesters."""
  log_info("Admin retrieving ingester status")
  ingesters = await AdminService.get_all_ingester_status()
  return ApiResponse({
      "ingesters":
      ingesters,
      "total_count":
      len(ingesters),
      "running_count":
      len([i for i in ingesters if i["status"] == "running"]),
      "timestamp":
      now().isoformat(),
  })


@router.get(Route.ADMIN_INGESTER_DETAILS.endpoint)
async def get_ingester_details(req: Request,
                               ingester_name: str) -> ApiResponse:
  """Get detailed information about a specific ingester."""
  details = await AdminService.get_ingester_details(ingester_name)
  return ApiResponse(details)


@router.post(Route.ADMIN_INGESTERS_CONTROL.endpoint)
async def control_ingesters(
    req: Request, control_request: IngesterControlRequest) -> ApiResponse:
  """Control ingester lifecycle (start/stop/restart)."""

  if control_request.action not in ["start", "stop", "restart"]:
    log_warn(
        f"Invalid ingester control action attempted: {control_request.action}")
    raise HTTPException(
        status_code=400,
        detail="Invalid action. Must be 'start', 'stop', or 'restart'",
    )

  log_info(
      f"Admin controlling ingesters: {control_request.action} on {control_request.ingester_names}"
  )
  result = await AdminService.control_ingesters(
      action=control_request.action,
      ingester_names=control_request.ingester_names,
      force=control_request.force,
  )

  return ApiResponse({
      "action": control_request.action,
      "results": result["results"],
      "success_count": result["success_count"],
      "failure_count": result["failure_count"],
      "timestamp": now().isoformat(),
  })


@router.post(Route.ADMIN_INGESTER_RESTART.endpoint)
async def restart_ingester(req: Request,
                           ingester_name: str,
                           force: bool = False) -> ApiResponse:
  """Restart a specific ingester."""
  result = await AdminService.restart_ingester(ingester_name, force)
  return ApiResponse({
      "ingester": ingester_name,
      "success": result["success"],
      "message": result["message"],
      "timestamp": now().isoformat(),
  })


@router.get(Route.ADMIN_SYSTEM_LOGS.endpoint)
async def get_system_logs(req: Request,
                          level: str = "INFO",
                          limit: int = 100,
                          since: Optional[str] = None) -> ApiResponse:
  """Get system logs with filtering."""
  logs = await AdminService.get_system_logs(level, limit, since)
  return ApiResponse({
      "logs": logs,
      "count": len(logs),
      "level": level,
      "limit": limit
  })


# Database Management Endpoints


@router.get(Route.ADMIN_DATABASE_STATUS.endpoint)
async def get_database_status(req: Request) -> ApiResponse:
  """Get database connection and health status."""
  status = await AdminService.get_database_status()
  return ApiResponse(status)


@router.get(Route.ADMIN_DATABASE_TABLES.endpoint)
async def get_database_tables(req: Request) -> ApiResponse:
  """Get information about database tables."""
  tables = await AdminService.get_database_tables()
  return ApiResponse({"tables": tables, "count": len(tables)})


# Cache Management Endpoints


@router.get(Route.ADMIN_CACHE_STATUS.endpoint)
async def get_cache_status(req: Request) -> ApiResponse:
  """Get Redis cache status and metrics."""
  status = await AdminService.get_cache_status()
  return ApiResponse(status)


@router.post(Route.ADMIN_CACHE_CLEAR.endpoint)
async def clear_cache(req: Request,
                      pattern: Optional[str] = None) -> ApiResponse:
  """Clear cache entries, optionally by pattern."""
  result = await AdminService.clear_cache(pattern or "*")
  return ApiResponse({
      "success": result["success"],
      "cleared_count": result["deleted_count"],
      "pattern": pattern,
      "timestamp": now().isoformat(),
  })


# Registry endpoints - simplified and generic
@router.get(Route.ADMIN_REGISTRY.endpoint)
async def get_registry_data(req: Request, registry_type: str) -> ApiResponse:
  """Get registry data for any type (instances, ingesters, etc.)."""
  from ...cache import get_registry

  if registry_type not in ["instances", "ingesters"]:
    raise HTTPException(
        status_code=400,
        detail="Invalid registry type. Use 'instances' or 'ingesters'")

  registry_data = await get_registry(registry_type)
  return ApiResponse({
      f"registry_{registry_type}": registry_data,
      "total_count": len(registry_data),
      "timestamp": now().isoformat(),
      "source": "registry"
  })


# User Management Endpoints


@router.post(Route.ADMIN_USERS_STATUS.endpoint, response_model=dict)
async def update_user_status(req: Request,
                             payload: UpdateUserStatusRequest) -> ApiResponse:
  """Update a user's status (e.g., 'active', 'banned')."""
  log_info(f"Admin updating user {payload.user_id} status to {payload.status}")
  user = await AdminService.update_user_status(payload.user_id, payload.status)

  return ApiResponse({
      "message": f"User {payload.user_id} status updated to {payload.status}",
      "user": {
          "uid": user.uid,
          "status": user.status,
          "updated_at": user.updated_at
      },
      "timestamp": now().isoformat()
  })


@router.get(Route.ADMIN_USERS.endpoint, status_code=200)
async def get_users(
    req: Request,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
) -> ApiResponse:
  """Get all users with pagination"""
  try:
    # Load users directly from the user table
    users_data = await load_resource("user", limit=limit, offset=offset)

    # Convert to User objects if needed
    users = []
    if isinstance(users_data, list):
      for user_data in users_data:
        try:
          if isinstance(user_data, dict):
            users.append(User.from_dict(user_data))
          elif isinstance(user_data, User):
            users.append(user_data)
        except Exception:
          continue

    # Calculate summary statistics
    total_users = len(users)
    total_requests = sum(user.total_count for user in users)
    total_bytes = sum(user.total_bytes for user in users)

    # Status breakdown
    status_counts: dict[str, int] = {}
    for user in users:
      status_counts[user.status] = status_counts.get(user.status, 0) + 1

    # Top users by requests
    top_users_by_requests = sorted(users,
                                   key=lambda u: u.total_count,
                                   reverse=True)[:10]
    top_users_by_bytes = sorted(users,
                                key=lambda u: u.total_bytes,
                                reverse=True)[:10]

    return ApiResponse({
        "summary": {
            "total_users": total_users,
            "total_requests": total_requests,
            "total_bytes": total_bytes,
            "status_counts": status_counts,
        },
        "top_users": {
            "by_requests": [{
                "uid": user.uid,
                "total_count": user.total_count,
                "total_bytes": user.total_bytes,
                "status": user.status
            } for user in top_users_by_requests],
            "by_bytes": [{
                "uid": user.uid,
                "total_count": user.total_count,
                "total_bytes": user.total_bytes,
                "status": user.status
            } for user in top_users_by_bytes]
        },
        "timestamp": now().isoformat()
    })
  except Exception as e:
    return ApiResponse({"error": str(e)}, status_code=500)


@router.get(Route.ADMIN_USERS_SUMMARY.endpoint)
async def get_user_summary(req: Request) -> ApiResponse:
  """Get a summary of user statistics."""
  summary = await AdminService.get_user_summary()
  return ApiResponse(summary)
