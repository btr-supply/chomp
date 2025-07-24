from fastapi import Request
from typing import Optional
from ..utils import now
from ..utils.decorators import service_method
from .. import state
from .auth import AuthService


@service_method("check status")
async def check_status(req: Request, utc_time: Optional[int] = None) -> dict:
  """Check service status and calculate ping time"""
  server_time = int(now().timestamp() * 1000)
  utc_time = utc_time or (server_time - 5
                          )  # Default to server_time - 5ms if not provided
  ping_ms = server_time - utc_time

  # Handle case where req.client might be None
  client_host = req.client.host if req.client else "unknown"

  return {
      "name": state.meta.name,
      "version": state.meta.version,
      "status": "OK",
      "ping_ms": ping_ms,
      "server_time": server_time,
      "id": AuthService.client_uid(req),
      "ip": client_host,
  }


ping = check_status
