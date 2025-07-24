"""
Admin service for user management and system operations.
Clean implementation with comprehensive logging and direct exception handling.
"""

from typing import Optional
from ..models.base import UserStatus
from ..models.user import User
from ..utils import log_error, log_info, log_warn, log_debug
from ..utils.decorators import service_method
from ..actions.load import load_resource
from ..actions.store import store
from .. import state
from ..cache import get_registry
from datetime import timezone

UTC = timezone.utc


# === HELPER FUNCTIONS ===

@service_method("User lookup and update")
async def _load_and_update_user(user_uid: str, update_func) -> Optional[User]:
  """Helper to load user and apply update function"""
  try:
    # Load user data from database
    user_data = await load_resource("user", uid=user_uid)
    if not user_data:
      log_warn(f"User not found: {user_uid}")
      raise ValueError(f"User not found: {user_uid}")

    # Convert to User object if needed
    user = User.from_dict(user_data) if isinstance(user_data, dict) else user_data

    # Apply update function
    updated_user = update_func(user)

    # Save the updated user
    await store(updated_user, publish=False, monitor=False)

    log_info(f"Successfully updated user {user_uid}")
    return updated_user

  except Exception:
    raise


@service_method("System component status check")
async def _get_system_component_status(component_name: str) -> dict:
  """Helper to get status of system components"""
  try:
    if component_name == "database":
      # Check TDengine connection
      if getattr(state, 'tsdb', None):
        return {"status": "healthy", "type": "tdengine"}
      else:
        return {"status": "disconnected", "type": "tdengine"}

    elif component_name == "redis":
      if getattr(state, 'redis', None):
        await state.redis.ping()
        return {"status": "healthy", "type": "redis"}
      else:
        return {"status": "disconnected", "type": "redis"}

    else:
      log_warn(f"Unknown system component requested: {component_name}")
      return {"status": "unknown", "type": component_name}

  except Exception as e:
    return {"status": "error", "type": component_name, "error": str(e)}


class AdminService:
  """Administrative operations service"""

  # === USER MANAGEMENT ===

  @staticmethod
  @service_method("Update user status")
  async def update_user_status(user_uid: str, status: UserStatus) -> User:
    """Update user status"""
    log_info(f"Updating user {user_uid} status to {status}")

    def update_status(user):
      user.status = status
      return user

    return await _load_and_update_user(user_uid, update_status)

  # === INGESTER MANAGEMENT ===

  @staticmethod
  @service_method("Get all ingester status")
  async def get_all_ingester_status() -> list[dict]:
    """Get status of all ingesters"""
    log_debug("Retrieving status for all ingesters")

    try:
      # Get ingester registry
      ingesters = await get_registry("ingesters")

      status_list = []
      for ingester_name, ingester_data in ingesters.items():
        status_list.append({
          "name": ingester_name,
          "status": ingester_data.get("status", "unknown"),
          "uptime": ingester_data.get("uptime", 0),
          "last_error": ingester_data.get("last_error"),
          "resource_usage": ingester_data.get("resource_usage", {})
        })

      log_debug(f"Retrieved status for {len(status_list)} ingesters")
      return status_list

    except Exception:
      return []

  @staticmethod
  @service_method("Get ingester details")
  async def get_ingester_details(ingester_name: str) -> dict:
    """Get detailed information about a specific ingester"""
    log_debug(f"Getting details for ingester: {ingester_name}")

    try:
      ingesters = await get_registry("ingesters")

      if ingester_name not in ingesters:
        log_warn(f"Ingester not found: {ingester_name}")
        raise ValueError(f"Ingester '{ingester_name}' not found")

      details = ingesters[ingester_name]
      log_debug(f"Retrieved details for ingester {ingester_name}")
      return details

    except ValueError:
      raise  # Re-raise validation errors
    except Exception:
      raise

  @staticmethod
  @service_method("Control ingesters")
  async def control_ingesters(action: str, ingester_names: list[str], force: bool = False) -> dict:
    """Control ingester lifecycle (start/stop/restart)"""
    log_info(f"Controlling ingesters: action={action}, names={ingester_names}, force={force}")

    results = {}
    success_count = 0
    failure_count = 0

    for ingester_name in ingester_names:
      try:
        # Implement actual ingester control logic here
        # For now, simulate the operation
        log_info(f"Performing {action} on ingester {ingester_name}")

        # Placeholder implementation
        results[ingester_name] = {
          "success": True,
          "message": f"Successfully {action}ed {ingester_name}",
          "action": action
        }
        success_count += 1

      except Exception as e:
        log_error(f"Failed to {action} ingester {ingester_name}: {e}")
        results[ingester_name] = {
          "success": False,
          "message": f"Failed to {action} {ingester_name}: {e}",
          "action": action
        }
        failure_count += 1

    log_info(f"Ingester control completed: {success_count} succeeded, {failure_count} failed")
    return {
      "results": results,
      "success_count": success_count,
      "failure_count": failure_count
    }

  @staticmethod
  @service_method("Restart ingester")
  async def restart_ingester(ingester_name: str, force: bool = False) -> dict:
    """Restart a specific ingester"""
    log_info(f"Restarting ingester {ingester_name}, force={force}")

    # Use the control_ingesters method for consistency
    result = await AdminService.control_ingesters("restart", [ingester_name], force)

    ingester_result = result["results"].get(ingester_name, {})
    return {
      "success": ingester_result.get("success", False),
      "message": ingester_result.get("message", "Unknown result")
    }

  # === SYSTEM MONITORING ===

  @staticmethod
  @service_method("Get system logs")
  async def get_system_logs(level: str = "INFO", limit: int = 100, since: Optional[str] = None) -> list[dict]:
    """Get system logs with filtering"""
    log_debug(f"Getting system logs: level={level}, limit={limit}, since={since}")

    # TODO: Implement actual log retrieval from system or log file
    # For now, return placeholder logs
    return [
      {"timestamp": "2024-01-01 12:00:00", "level": "INFO", "message": "System started"},
      {"timestamp": "2024-01-01 12:01:00", "level": "DEBUG", "message": "Debug message"}
    ]

  @staticmethod
  @service_method("Get database status")
  async def get_database_status() -> dict:
    """Get database connection status"""
    return await _get_system_component_status("database")

  @staticmethod
  @service_method("Get database tables")
  async def get_database_tables() -> list[dict]:
    """Get list of database tables with metadata"""
    log_debug("Getting database tables")

    try:
      if getattr(state, 'tsdb', None):
        # Get table list from database
        tables = await state.tsdb.list_tables()

        table_info = []
        for table in tables:
          # For each table, get basic metadata
          table_info.append({
            "name": table,
            "type": "time_series",  # Assuming TDengine time series tables
            "row_count": 0,  # TODO: Get actual row count
            "size_mb": 0     # TODO: Get actual size
          })

        log_debug(f"Retrieved {len(table_info)} tables")
        return table_info
      else:
        log_warn("Database not available")
        return []

    except Exception as e:
      log_error(f"Error getting database tables: {e}")
      return []

  @staticmethod
  @service_method("Get cache status")
  async def get_cache_status() -> dict:
    """Get cache (Redis) connection status"""
    return await _get_system_component_status("redis")

  @staticmethod
  @service_method("Clear cache")
  async def clear_cache(pattern: str = "*") -> dict:
    """Clear cache entries matching pattern"""
    log_info(f"Clearing cache with pattern: {pattern}")

    try:
      if getattr(state, 'redis', None):
        # Get keys matching pattern
        keys = await state.redis.keys(pattern)

        if keys:
          # Delete keys
          deleted_count = await state.redis.delete(*keys)
          log_info(f"Cleared {deleted_count} cache entries")
          return {"success": True, "deleted_count": deleted_count}
        else:
          log_info("No cache entries found matching pattern")
          return {"success": True, "deleted_count": 0}
      else:
        log_warn("Redis not available")
        return {"success": False, "error": "Redis not available"}

    except Exception as e:
      log_error(f"Error clearing cache: {e}")
      return {"success": False, "error": str(e)}


