from datetime import datetime, timezone
from fastapi import Request
from ..utils import now
from .. import state
from ..model import ServiceResponse
from .gatekeeeper import requester_id

async def check_status(req: Request, utc_time: int = None) -> ServiceResponse:
  """Check service status and calculate ping time"""
  try:
    server_time = int(now().timestamp() * 1000)
    utc_time = utc_time or (server_time - 5)  # Default to server_time - 5ms if not provided
    ping_ms = server_time - utc_time

    return "", {
      "name": state.meta.name,
      "version": state.meta.version,
      "status": "OK",
      "ping_ms": ping_ms,
      "server_time": server_time,
      "id": requester_id(req),
      "ip": req.client.host,
    }
  except Exception as e:
    return f"Error checking status: {str(e)}", None

ping = check_status
