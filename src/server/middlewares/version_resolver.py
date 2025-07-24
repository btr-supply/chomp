from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from ... import state


class VersionResolver(BaseHTTPMiddleware):

  async def dispatch(self, request: Request, call_next):
    latest_prefix = f"/v{state.meta.minor_version}"  # Cache prefix
    path = request.url.path
    if path.startswith(latest_prefix):
      request.scope["path"] = path.replace(
          latest_prefix, "", 1)  # replace /v{latest} with / (api root)
    return await call_next(request)
