from functools import wraps
from typing import Callable
from fastapi import FastAPI, Request, HTTPException, Response
from starlette.middleware.base import BaseHTTPMiddleware

from .. import state
from ...services import limiter as limiter_service
from ...services.gatekeeeper import requester_id


class Limiter(BaseHTTPMiddleware):

  def __init__(
      self,
      app: FastAPI,
      whitelist: list[str] = [],
      blacklist: list[str] = [],
      rpm: int = -1,
      rph: int = -1,
      rpd: int = -1,  # request per minute/hour/day -1 == limitless
      spm: int = -1,
      sph: int = -1,
      spd: int = -1,  # total size per minute/hour/day -1 == limitless
      ppm: int = -1,
      pph: int = -1,
      ppd: int = -1,  # points per minute/hour/day -1 == limitless
      ppr={}):
    super().__init__(app)
    # Use setattr to avoid mypy error about dynamic attribute
    setattr(state.server, 'limiter', self)
    self.limits = {
        'rpm': (rpm, 60),
        'rph': (rph, 3600),
        'rpd': (rpd, 86400),
        'spm': (spm, 60),
        'sph': (sph, 3600),
        'spd': (spd, 86400),
        'ppm': (ppm, 60),
        'pph': (pph, 3600),
        'ppd': (ppd, 86400),
    }
    self.whitelist = whitelist
    self.blacklist = blacklist
    self.ppr = ppr

  async def dispatch(self, req: Request, call_next: Callable) -> Response:
    user = requester_id(req)

    # Check limits
    err, check_result = await limiter_service.check_limits(user, req.url.path)
    if err:
      raise HTTPException(status_code=429 if "exceeded" in err else 403,
                          detail=err)

    if check_result.get("whitelisted"):
      return await call_next(req)

    # Execute request
    res: Response = await call_next(req)

    # Increment counters
    err, headers = await limiter_service.increment_counters(
        user, int(res.headers.get('Content-Length') or 0), check_result["ppr"])
    if err:
      # Log error but don't fail request
      print(f"Error incrementing rate limits: {err}")
    else:
      res.headers.update({
          "X-RateLimit-Limit": headers["limits"],
          "X-RateLimit-Remaining": headers["remaining"],
          "X-RateLimit-Reset": headers["reset"]
      })

    return res


# TODO: implement a simpler reflexion mechanism than request->app->middleware
def limit(points: int):

  def decorator(func: Callable):

    @wraps(func)
    async def wrapper(*args, **kwargs) -> Response:
      req: Request = kwargs.get("req") or args[0]
      limiter: Limiter = getattr(state.server, 'limiter')
      limiter.ppr.setdefault(req.url.path, points)
      return await func(*args, **kwargs)

    return wrapper

  return decorator
