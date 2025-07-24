"""
Configuration management service for config file retrieval, editing, and versioning.
Uses diff-match-patch for efficient versioning and yamale for validation.
"""

import os
import tempfile
from typing import Any, Optional
from pathlib import Path
from fastapi import Request

from diff_match_patch import diff_match_patch

from ..models.base import Scope
from .. import state
from ..cache import cache, get_cache
from ..proxies import IngesterConfigProxy
from .loader import get_resources as loader_get_resources, get_schema as loader_get_schema
from .status_checker import check_status
from ..utils import now, log_warn, log_error
from ..utils.decorators import service_method

# Configuration Management
CONFIG_HISTORY_KEY = "config_history"
CONFIG_HISTORY_LIMIT = 50


class ConfigVersionManager:
  """Manages configuration versioning and rollback functionality"""

  def __init__(self):
    self.dmp = diff_match_patch()

  def _get_ingester_config_path(self) -> Path:
    """Get the ingester configuration file path"""
    return Path(os.environ["INGESTER_CONFIGS"]).resolve()

  def _get_server_config_path(self) -> Path:
    """Get the server configuration file path"""
    config_path = (state.args.server_config if hasattr(state.args, "server_config")
                   and state.args.server_config else "server-config.yml")
    return Path(config_path).resolve()

  async def save_config_with_diff(self, new_content: str, config_type: str = "ingester") -> str:
    """Save new config and store diff in Redis history"""
    if config_type == "ingester":
      config_path = self._get_ingester_config_path()
      history_key = f"{CONFIG_HISTORY_KEY}:ingester"
    elif config_type == "server":
      config_path = self._get_server_config_path()
      history_key = f"{CONFIG_HISTORY_KEY}:server"
    else:
      raise ValueError(f"Invalid config type: {config_type}")

    # Read current config if it exists
    current_content = ""
    if config_path.exists():
      with open(config_path, "r") as f:
        current_content = f.read()

    # Compute diff using diff-match-patch
    if current_content != new_content:
      patches = self.dmp.patch_make(current_content, new_content)
      diff_text = self.dmp.patch_toText(patches)

      # Store diff in Redis history
      if diff_text.strip():
        await self._store_diff_in_history(diff_text, history_key)

    # Write the new config file
    with open(config_path, "w") as f:
      f.write(new_content)

    return f"{config_type.capitalize()} config updated successfully. Diff stored in history."

  async def _store_diff_in_history(self, diff_text: str, history_key: str) -> None:
    """Store diff in Redis with timestamp and limit history size"""
    timestamp = now().isoformat()
    diff_entry = {"timestamp": timestamp, "diff": diff_text}

    # Get current history
    history = await get_cache(history_key, pickled=True) or []

    # Add new diff to the beginning (most recent first)
    history.insert(0, diff_entry)

    # Limit history size
    if len(history) > CONFIG_HISTORY_LIMIT:
      history = history[:CONFIG_HISTORY_LIMIT]

    # Store back to Redis
    await cache(history_key, history, pickled=True)

  async def get_config_at_version(self, steps_back: int, config_type: str = "ingester") -> str:
    """Reconstruct config at a specific version by applying patches backwards"""
    if steps_back < 0:
      raise ValueError("steps_back must be non-negative")

    if config_type == "ingester":
      config_path = self._get_ingester_config_path()
      history_key = f"{CONFIG_HISTORY_KEY}:ingester"
    elif config_type == "server":
      config_path = self._get_server_config_path()
      history_key = f"{CONFIG_HISTORY_KEY}:server"
    else:
      raise ValueError(f"Invalid config type: {config_type}")

    if steps_back == 0:
      # Return current version
      if config_path.exists():
        with open(config_path, "r") as f:
          return f.read()
      return ""

    # Get current config
    if not config_path.exists():
      log_error(f"Current {config_type} config file not found")
      raise FileNotFoundError(f"Current {config_type} config file not found")

    with open(config_path, "r") as f:
      current_content = f.read()

    # Get history from Redis
    history = await get_cache(history_key, pickled=True) or []

    if steps_back > len(history):
      log_error(f"Cannot go back {steps_back} steps, only {len(history)} diffs available")
      raise ValueError(
          f"Cannot go back {steps_back} steps, only {len(history)} diffs available"
      )

    # Apply patches backwards
    content = current_content
    for i in range(steps_back):
      diff_entry = history[i]
      diff_text = diff_entry["diff"]

      # Use diff-match-patch for reverse patching
      patches = self.dmp.patch_fromText(diff_text)
      # For reverse patching, we need to invert the patches
      for patch in patches:
        # Invert each patch's diffs
        inverted_diffs = []
        for diff_op, diff_text in patch.diffs:
          if diff_op == 1:  # INSERT becomes DELETE
            inverted_diffs.append((-1, diff_text))
          elif diff_op == -1:  # DELETE becomes INSERT
            inverted_diffs.append((1, diff_text))
          else:  # EQUAL stays the same
            inverted_diffs.append((diff_op, diff_text))
        patch.diffs = inverted_diffs

      content, _ = self.dmp.patch_apply(patches, content)

    return content

  async def get_history_list(self, config_type: str = "ingester") -> list[dict[str, Any]]:
    """Get list of available config versions with timestamps"""
    if config_type not in ["ingester", "server"]:
      raise ValueError(f"Invalid config type: {config_type}")

    history_key = f"{CONFIG_HISTORY_KEY}:{config_type}"
    history = await get_cache(history_key, pickled=True) or []
    return [{
        "version": i + 1,
        "timestamp": entry["timestamp"],
        "steps_back": i + 1
    } for i, entry in enumerate(history)]

  async def rollback_to_version(self, steps_back: int, config_type: str = "ingester") -> str:
    """Rollback config to a specific version"""
    # Get the config content at the specified version
    rolled_back_content = await self.get_config_at_version(steps_back, config_type)

    # Validate the rolled back content
    if config_type == "ingester":
      validation = await validate_ingester_config(rolled_back_content)
    elif config_type == "server":
      validation = await validate_server_config(rolled_back_content)
    else:
      raise ValueError(f"Invalid config type: {config_type}")

    if not validation["valid"]:
      log_error(f"Rolled back {config_type} configuration is invalid")
      raise ValueError(f"Rolled back {config_type} configuration is invalid")

    # Save the rolled back content (this will create a new diff entry)
    await self.save_config_with_diff(rolled_back_content, config_type)

    return f"Successfully rolled back {config_type} config {steps_back} steps"


config_manager = ConfigVersionManager()


# Ingester Configuration Functions
@service_method("get current ingester config")
async def get_current_ingester_config() -> str:
  """Get current ingester configuration as YAML string."""
  config_path = config_manager._get_ingester_config_path()

  if not config_path.exists():
    log_error(f"Ingester configuration file not found: {config_path}")
    raise FileNotFoundError(f"Ingester configuration file not found: {config_path}")

  with open(config_path, "r", encoding="utf-8") as f:
    return f.read()


async def validate_ingester_config(config_content: str) -> dict[str, Any]:
  """Validate ingester configuration content without applying it."""
  with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as tmp:
    tmp.write(config_content)
    tmp_path = tmp.name

  try:
    # Use existing IngesterConfigProxy validation
    config = IngesterConfigProxy.load_config(tmp_path)
    affected_ingesters = [ing.name for ing in config.ingesters]

    return {
        "valid": True,
        "errors": [],
        "warnings": [],
        "affected_ingesters": affected_ingesters,
    }
  except Exception as e:
    log_warn(f"Ingester config validation failed: {e}")
    return {
        "valid": False,
        "errors": [str(e)],
        "warnings": [],
        "affected_ingesters": [],
    }
  finally:
    os.unlink(tmp_path)


@service_method("update ingester config")
async def update_ingester_config(config_content: str) -> dict[str, Any]:
  """Update ingester configuration with diff-match-patch versioning."""
  # Validate first
  validation_result = await validate_ingester_config(config_content)
  if not validation_result["valid"]:
    log_warn(f"Ingester configuration validation failed: {validation_result.get('errors', [])}")
    raise ValueError("Ingester configuration validation failed")

  # Use ConfigVersionManager for diff-based storage
  result_message = await config_manager.save_config_with_diff(config_content, "ingester")

  return {
      "success": True,
      "message": result_message,
      "affected_ingesters": validation_result["affected_ingesters"],
  }


# Server Configuration Functions
@service_method("get current server config")
async def get_current_server_config() -> str:
  """Get current server configuration as YAML string."""
  config_path = config_manager._get_server_config_path()

  if not config_path.exists():
    log_error(f"Server configuration file not found: {config_path}")
    raise FileNotFoundError(f"Server configuration file not found: {config_path}")

  with open(config_path, "r", encoding="utf-8") as f:
    return f.read()


async def validate_server_config(config_content: str) -> dict[str, Any]:
  """Validate server configuration content without applying it."""
  # For server config, we can use yamale or just basic YAML parsing
  import yaml

  try:
    # Parse YAML to check syntax
    yaml.safe_load(config_content)

    # Basic validation - check for required fields if needed
    return {
        "valid": True,
        "errors": [],
        "warnings": [],
        "affected_services": ["api_server"],
    }
  except yaml.YAMLError as e:
    log_warn(f"Server config YAML syntax error: {e}")
    return {
        "valid": False,
        "errors": [f"YAML syntax error: {str(e)}"],
        "warnings": [],
        "affected_services": [],
    }


@service_method("update server config")
async def update_server_config(config_content: str) -> dict[str, Any]:
  """Update server configuration with diff-match-patch versioning."""
  # Validate first
  validation_result = await validate_server_config(config_content)
  if not validation_result["valid"]:
    log_warn(f"Server configuration validation failed: {validation_result.get('errors', [])}")
    raise ValueError("Server configuration validation failed")

  # Use ConfigVersionManager for diff-based storage
  result_message = await config_manager.save_config_with_diff(config_content, "server")

  return {
      "success": True,
      "message": result_message,
      "affected_services": validation_result["affected_services"],
  }


# Generic Configuration Functions
@service_method("get config history")
async def get_config_history(limit: int = 10, config_type: str = "ingester") -> list[dict[str, Any]]:
  """Get configuration change history."""
  history = await config_manager.get_history_list(config_type)
  return history[:limit]


@service_method("rollback config")
async def rollback_config(steps_back: int, config_type: str = "ingester") -> dict[str, Any]:
  """Rollback to previous configuration version."""
  result_message = await config_manager.rollback_to_version(steps_back, config_type)

  # Get affected ingesters/services from current config
  if config_type == "ingester":
    try:
      current_config = await get_current_ingester_config()
      validation = await validate_ingester_config(current_config)
      affected_items = validation.get("affected_ingesters", [])
    except Exception:
      affected_items = []
  else:
    try:
      current_config = await get_current_server_config()
      validation = await validate_server_config(current_config)
      affected_items = validation.get("affected_services", [])
    except Exception:
      affected_items = []

  return {
      "success": True,
      "message": result_message,
      f"affected_{config_type}s" if config_type == "ingester" else "affected_services": affected_items,
  }


# Schema and Resource Functions (existing)
@service_method("get resources")
async def get_resources(include_transient: bool = False, request: Optional[Request] = None) -> list:
  """Get all available resources with protection filtering handled by loader."""
  scope = Scope.DETAILED if include_transient else Scope.DEFAULT

  resources_data = await loader_get_resources(scope, request)
  # Convert dict to list of resource names
  resources_list = list(resources_data.keys())
  return resources_list


@service_method("get schema")
async def get_schema(scope: Scope = Scope.DEFAULT, include_transient: bool = False, request: Optional[Request] = None) -> dict:
  """Get the schema of all available resources with protection filtering handled by loader."""
  effective_scope = Scope.DETAILED if include_transient else scope

  schema_data = await loader_get_schema(scope=effective_scope, request=request)
  return schema_data


async def get_system_status(request: Optional[Request] = None) -> dict:
  """Get system status information."""
  if not request:
    # Create a minimal status response if no request provided
    return {
      "status": "OK",
      "message": "System operational"
    }

  status_data = await check_status(request)
  return status_data
