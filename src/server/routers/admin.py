"""
Admin router for configuration management and ingester control.
Provides endpoints for runtime configuration updates and ingester lifecycle management.
"""

from fastapi import APIRouter, HTTPException, Request, UploadFile, File
from typing import Optional, Dict, Any, List
from pydantic import BaseModel
from datetime import datetime

from ...server.middlewares.limiter import limit
from ...server.responses import ApiResponse, handle_service_error as hse
from ...services import admin_service
from ...utils import log_error

router = APIRouter(tags=["admin"])


# Simple test endpoint
@router.get("/test", status_code=200, response_model=dict)
async def test_endpoint() -> dict:
    """Simple test endpoint."""
    return {
        "status": "admin router working",
        "timestamp": datetime.utcnow().isoformat(),
    }


# Request/Response Models
class ConfigUpdateRequest(BaseModel):
    config_content: str
    validate_only: bool = False
    restart_affected: bool = True


class ConfigUpdateResponse(BaseModel):
    success: bool
    message: str
    affected_ingesters: List[str]
    validation_errors: List[str] = []


class IngesterControlRequest(BaseModel):
    action: str  # "start", "stop", "restart"
    ingester_names: List[str]
    force: bool = False


class IngesterStatusResponse(BaseModel):
    name: str
    status: str  # "running", "stopped", "error", "starting", "stopping"
    uptime: Optional[int]  # seconds
    last_error: Optional[str]
    resource_usage: Dict[str, Any]


# Configuration Management Endpoints


@router.get("/config", status_code=200)
@limit(points=2)
async def get_current_config(req: Request) -> ApiResponse:
    """Get the current ingester configuration."""
    try:
        config_content = await hse(admin_service.get_current_config())
        return ApiResponse(
            {
                "config": config_content,
                "timestamp": datetime.utcnow().isoformat(),
                "format": "yaml",
            }
        )
    except Exception as e:
        log_error(f"Failed to get current config: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve configuration")


@router.post("/config/validate", status_code=200)
@limit(points=5)
async def validate_config(
    req: Request, config_request: ConfigUpdateRequest
) -> ApiResponse:
    """Validate a configuration without applying it."""
    try:
        validation_result = await hse(
            admin_service.validate_config(config_request.config_content)
        )
        return ApiResponse(
            {
                "valid": validation_result["valid"],
                "errors": validation_result.get("errors", []),
                "warnings": validation_result.get("warnings", []),
                "affected_ingesters": validation_result.get("affected_ingesters", []),
            }
        )
    except Exception as e:
        log_error(f"Config validation failed: {e}")
        raise HTTPException(status_code=400, detail="Configuration validation failed")


@router.post("/config/update", status_code=200)
@limit(points=10)
async def update_config(
    req: Request, config_request: ConfigUpdateRequest
) -> ApiResponse:
    """Update the ingester configuration and optionally restart affected ingesters."""
    try:
        if config_request.validate_only:
            return await validate_config(req, config_request)

        update_result = await hse(
            admin_service.update_config(config_request.config_content)
        )

        return ApiResponse(
            {
                "success": update_result["success"],
                "message": update_result["message"],
                "affected_ingesters": update_result["affected_ingesters"],
                "restart_required": update_result.get("restart_required", []),
                "timestamp": datetime.utcnow().isoformat(),
            }
        )
    except Exception as e:
        log_error(f"Config update failed: {e}")
        raise HTTPException(status_code=500, detail="Configuration update failed")


@router.post("/config/upload")
@limit(points=10)
async def upload_config(req: Request, file: UploadFile = File(...)) -> ApiResponse:
    """Upload a configuration file."""
    try:
        if not file.filename or not file.filename.endswith((".yml", ".yaml")):
            raise HTTPException(status_code=400, detail="Only YAML files are supported")

        content = await file.read()
        config_content = content.decode("utf-8")

        # Validate the uploaded configuration
        validation_result = await hse(admin_service.validate_config(config_content))

        if not validation_result["valid"]:
            raise HTTPException(
                status_code=400, detail="Uploaded configuration is invalid"
            )

        return ApiResponse(
            {
                "success": True,
                "message": "Configuration file uploaded and validated successfully",
                "config": config_content,
                "affected_ingesters": validation_result.get("affected_ingesters", []),
            }
        )
    except Exception as e:
        log_error(f"Config upload failed: {e}")
        raise HTTPException(status_code=500, detail="Configuration upload failed")


@router.get("/config/history")
@limit(points=2)
async def get_config_history(req: Request, limit: int = 10) -> ApiResponse:
    """Get configuration change history."""
    try:
        history = await hse(admin_service.get_config_history(limit))
        return ApiResponse({"history": history, "count": len(history)})
    except Exception as e:
        log_error(f"Failed to get config history: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to retrieve configuration history"
        )


@router.post("/config/rollback")
@limit(points=10)
async def rollback_config(req: Request, version_id: str) -> ApiResponse:
    """Rollback to a previous configuration version."""
    try:
        # Convert version_id to steps_back (assuming version_id represents steps)
        steps_back = int(version_id) if version_id.isdigit() else 1
        rollback_result = await hse(admin_service.rollback_config(steps_back))
        return ApiResponse(
            {
                "success": rollback_result["success"],
                "message": rollback_result["message"],
                "rolled_back_to": rollback_result["version_id"],
                "affected_ingesters": rollback_result["affected_ingesters"],
            }
        )
    except Exception as e:
        log_error(f"Config rollback failed: {e}")
        raise HTTPException(status_code=500, detail="Configuration rollback failed")


# Ingester Control Endpoints


@router.get("/ingesters")
@limit(points=2)
async def get_ingester_status(req: Request) -> ApiResponse:
    """Get status of all ingesters."""
    try:
        ingesters = await hse(admin_service.get_all_ingester_status())  # type: ignore[attr-defined]
        return ApiResponse(
            {
                "ingesters": ingesters,
                "total_count": len(ingesters),
                "running_count": len(
                    [i for i in ingesters if i["status"] == "running"]
                ),
                "timestamp": datetime.utcnow().isoformat(),
            }
        )
    except Exception as e:
        log_error(f"Failed to get ingester status: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to retrieve ingester status"
        )


@router.get("/ingesters/{ingester_name}")
@limit(points=1)
async def get_ingester_details(req: Request, ingester_name: str) -> ApiResponse:
    """Get detailed information about a specific ingester."""
    try:
        details = await hse(admin_service.get_ingester_details(ingester_name))  # type: ignore[attr-defined]
        return ApiResponse(details)
    except Exception as e:
        log_error(f"Failed to get ingester details for {ingester_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve details for ingester {ingester_name}",
        )


@router.post("/ingesters/control")
@limit(points=5)
async def control_ingesters(
    req: Request, control_request: IngesterControlRequest
) -> ApiResponse:
    """Control ingester lifecycle (start/stop/restart)."""
    try:
        if control_request.action not in ["start", "stop", "restart"]:
            raise HTTPException(
                status_code=400,
                detail="Invalid action. Must be 'start', 'stop', or 'restart'",
            )

        result = await hse(
            admin_service.control_ingesters(  # type: ignore[attr-defined]
                action=control_request.action,
                ingester_names=control_request.ingester_names,
                force=control_request.force,
            )
        )

        return ApiResponse(
            {
                "action": control_request.action,
                "results": result["results"],
                "success_count": result["success_count"],
                "failure_count": result["failure_count"],
                "timestamp": datetime.utcnow().isoformat(),
            }
        )
    except Exception as e:
        log_error(f"Ingester control failed: {e}")
        raise HTTPException(status_code=500, detail="Ingester control operation failed")


@router.post("/ingesters/{ingester_name}/restart")
@limit(points=3)
async def restart_ingester(
    req: Request, ingester_name: str, force: bool = False
) -> ApiResponse:
    """Restart a specific ingester."""
    try:
        result = await hse(admin_service.restart_ingester(ingester_name, force))  # type: ignore[attr-defined]
        return ApiResponse(
            {
                "ingester": ingester_name,
                "success": result["success"],
                "message": result["message"],
                "timestamp": datetime.utcnow().isoformat(),
            }
        )
    except Exception as e:
        log_error(f"Failed to restart ingester {ingester_name}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to restart ingester {ingester_name}"
        )


@router.get("/system/logs")
@limit(points=5)
async def get_system_logs(
    req: Request, level: str = "INFO", limit: int = 100, since: Optional[str] = None
) -> ApiResponse:
    """Get system logs with filtering."""
    try:
        logs = await hse(admin_service.get_system_logs(level, limit, since))  # type: ignore[attr-defined]
        return ApiResponse(
            {"logs": logs, "count": len(logs), "level": level, "limit": limit}
        )
    except Exception as e:
        log_error(f"Failed to get system logs: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve system logs")


# Database Management Endpoints


@router.get("/database/status")
@limit(points=2)
async def get_database_status(req: Request) -> ApiResponse:
    """Get database connection and health status."""
    try:
        status = await hse(admin_service.get_database_status())  # type: ignore[attr-defined]
        return ApiResponse(status)
    except Exception as e:
        log_error(f"Failed to get database status: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to retrieve database status"
        )


@router.get("/database/tables")
@limit(points=2)
async def get_database_tables(req: Request) -> ApiResponse:
    """Get information about database tables."""
    try:
        tables = await hse(admin_service.get_database_tables())  # type: ignore[attr-defined]
        return ApiResponse({"tables": tables, "count": len(tables)})
    except Exception as e:
        log_error(f"Failed to get database tables: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to retrieve database tables"
        )


# Cache Management Endpoints


@router.get("/cache/status")
@limit(points=1)
async def get_cache_status(req: Request) -> ApiResponse:
    """Get Redis cache status and metrics."""
    try:
        status = await hse(admin_service.get_cache_status())  # type: ignore[attr-defined]
        return ApiResponse(status)
    except Exception as e:
        log_error(f"Failed to get cache status: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve cache status")


@router.post("/cache/clear")
@limit(points=10)
async def clear_cache(req: Request, pattern: Optional[str] = None) -> ApiResponse:
    """Clear cache entries, optionally by pattern."""
    try:
        result = await hse(admin_service.clear_cache(pattern))  # type: ignore[attr-defined]
        return ApiResponse(
            {
                "success": result["success"],
                "cleared_count": result["cleared_count"],
                "pattern": pattern,
                "timestamp": datetime.utcnow().isoformat(),
            }
        )
    except Exception as e:
        log_error(f"Failed to clear cache: {e}")
        raise HTTPException(status_code=500, detail="Failed to clear cache")
