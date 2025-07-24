"""
Configuration router for config file retrieval, editing, and versioning.
Provides endpoints for managing both ingester and server configurations.
"""

from fastapi import APIRouter, HTTPException, Request, UploadFile, File, Query
from typing import Optional
from pydantic import BaseModel

from ...server.responses import ApiResponse
from ..routes import Route
from ...services import config
from ...utils import log_info, now


router = APIRouter(tags=["config"])


# Request/Response Models
class ConfigUpdateRequest(BaseModel):
  config_content: str
  validate_only: bool = False
  restart_affected: bool = True


class ConfigUpdateResponse(BaseModel):
  success: bool
  message: str
  affected_ingesters: Optional[list[str]] = None
  affected_services: Optional[list[str]] = None
  validation_errors: list[str] = []


class ConfigRollbackRequest(BaseModel):
  steps_back: int
  config_type: str = "ingester"  # "ingester" or "server"


# Ingester Configuration Endpoints


@router.get(Route.CONFIG_INGESTER_GET.endpoint, status_code=200)
async def get_current_ingester_config(req: Request) -> ApiResponse:
  """Get the current ingester configuration."""
  log_info("Retrieving current ingester configuration")
  config_content = await config.get_current_ingester_config()
  return ApiResponse({
      "config": config_content,
      "timestamp": now().isoformat(),
      "format": "yaml",
      "type": "ingester"
  })


@router.post(Route.CONFIG_INGESTER_VALIDATE.endpoint, status_code=200)
async def validate_ingester_config(req: Request,
                                   config_request: ConfigUpdateRequest) -> ApiResponse:
  """Validate an ingester configuration without applying it."""
  validation_result = await config.validate_ingester_config(config_request.config_content)
  return ApiResponse({
      "valid": validation_result["valid"],
      "errors": validation_result.get("errors", []),
      "warnings": validation_result.get("warnings", []),
      "affected_ingesters": validation_result.get("affected_ingesters", []),
  })


@router.post(Route.CONFIG_INGESTER_UPDATE.endpoint, status_code=200)
async def update_ingester_config(req: Request,
                                 config_request: ConfigUpdateRequest) -> ApiResponse:
  """Update the ingester configuration and optionally restart affected ingesters."""
  if config_request.validate_only:
    return await validate_ingester_config(req, config_request)

  update_result = await config.update_ingester_config(config_request.config_content)

  return ApiResponse({
      "success": update_result["success"],
      "message": update_result["message"],
      "affected_ingesters": update_result["affected_ingesters"],
      "restart_required": update_result.get("restart_required", []),
      "timestamp": now().isoformat(),
  })


@router.post(Route.CONFIG_INGESTER_UPLOAD.endpoint)
async def upload_ingester_config(
    req: Request, file: UploadFile = File(...)) -> ApiResponse:
  """Upload an ingester configuration file."""
  if not file.filename or not file.filename.endswith((".yml", ".yaml")):
    raise HTTPException(status_code=400, detail="Only YAML files are supported")

  content = await file.read()
  config_content = content.decode("utf-8")

  # Validate the uploaded configuration
  validation_result = await config.validate_ingester_config(config_content)

  if not validation_result["valid"]:
    raise HTTPException(status_code=400, detail="Uploaded ingester configuration is invalid")

  return ApiResponse({
      "success": True,
      "message": "Ingester configuration file uploaded and validated successfully",
      "config": config_content,
      "affected_ingesters": validation_result.get("affected_ingesters", []),
  })


@router.get(Route.CONFIG_INGESTER_HISTORY.endpoint)
async def get_ingester_config_history(req: Request, limit: int = 10) -> ApiResponse:
  """Get ingester configuration change history."""
  history = await config.get_config_history(limit, "ingester")
  return ApiResponse({"history": history, "count": len(history), "type": "ingester"})


# Server Configuration Endpoints


@router.get(Route.CONFIG_SERVER_GET.endpoint, status_code=200)
async def get_current_server_config(req: Request) -> ApiResponse:
  """Get the current server configuration."""
  config_content = await config.get_current_server_config()
  return ApiResponse({
      "config": config_content,
      "timestamp": now().isoformat(),
      "format": "yaml",
      "type": "server"
  })


@router.post(Route.CONFIG_SERVER_VALIDATE.endpoint, status_code=200)
async def validate_server_config(req: Request,
                                 config_request: ConfigUpdateRequest) -> ApiResponse:
  """Validate a server configuration without applying it."""
  validation_result = await config.validate_server_config(config_request.config_content)
  return ApiResponse({
      "valid": validation_result["valid"],
      "errors": validation_result.get("errors", []),
      "warnings": validation_result.get("warnings", []),
      "affected_services": validation_result.get("affected_services", []),
  })


@router.post(Route.CONFIG_SERVER_UPDATE.endpoint, status_code=200)
async def update_server_config(req: Request,
                               config_request: ConfigUpdateRequest) -> ApiResponse:
  """Update the server configuration."""
  if config_request.validate_only:
    return await validate_server_config(req, config_request)

  update_result = await config.update_server_config(config_request.config_content)

  return ApiResponse({
      "success": update_result["success"],
      "message": update_result["message"],
      "affected_services": update_result["affected_services"],
      "restart_required": update_result.get("restart_required", []),
      "timestamp": now().isoformat(),
  })


@router.post(Route.CONFIG_SERVER_UPLOAD.endpoint)
async def upload_server_config(
    req: Request, file: UploadFile = File(...)) -> ApiResponse:
  """Upload a server configuration file."""
  if not file.filename or not file.filename.endswith((".yml", ".yaml")):
    raise HTTPException(status_code=400, detail="Only YAML files are supported")

  content = await file.read()
  config_content = content.decode("utf-8")

  # Validate the uploaded configuration
  validation_result = await config.validate_server_config(config_content)

  if not validation_result["valid"]:
    raise HTTPException(status_code=400, detail="Uploaded server configuration is invalid")

  return ApiResponse({
      "success": True,
      "message": "Server configuration file uploaded and validated successfully",
      "config": config_content,
      "affected_services": validation_result.get("affected_services", []),
  })


@router.get(Route.CONFIG_SERVER_HISTORY.endpoint)
async def get_server_config_history(req: Request, limit: int = 10) -> ApiResponse:
  """Get server configuration change history."""
  history = await config.get_config_history(limit, "server")
  return ApiResponse({"history": history, "count": len(history), "type": "server"})


# Generic Configuration Endpoints


@router.post(Route.CONFIG_ROLLBACK.endpoint)
async def rollback_config(req: Request, rollback_request: ConfigRollbackRequest) -> ApiResponse:
  """Rollback to a previous configuration version."""
  if rollback_request.config_type not in ["ingester", "server"]:
    raise HTTPException(status_code=400, detail="Invalid config type. Must be 'ingester' or 'server'")

  rollback_result = await config.rollback_config(rollback_request.steps_back, rollback_request.config_type)

  return ApiResponse({
      "success": rollback_result["success"],
      "message": rollback_result["message"],
      "config_type": rollback_request.config_type,
      "steps_back": rollback_request.steps_back,
      **{k: v for k, v in rollback_result.items() if k not in ["success", "message"]},
  })


@router.get(Route.CONFIG_HISTORY.endpoint)
async def get_all_config_history(req: Request,
                                 limit: int = Query(10, ge=1, le=100),
                                 config_type: str = Query("both", description="ingester, server, or both")) -> ApiResponse:
  """Get configuration change history for one or both config types."""
  result = {}

  if config_type in ["ingester", "both"]:
    ingester_history = await config.get_config_history(limit, "ingester")
    result["ingester"] = ingester_history

  if config_type in ["server", "both"]:
    server_history = await config.get_config_history(limit, "server")
    result["server"] = server_history

  return ApiResponse({
      "history": result,
      "requested_type": config_type,
      "limit": limit
  })
