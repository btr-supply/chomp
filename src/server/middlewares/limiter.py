from typing import Callable
from fastapi import Request, HTTPException, Response
from starlette.middleware.base import BaseHTTPMiddleware

from ...services.limiter import RateLimiter
from ...utils import log_error


class RateLimitMiddleware(BaseHTTPMiddleware):
  """Optimized rate limiting middleware configured from server config"""

  async def dispatch(self, req: Request, call_next: Callable) -> Response:
    """Single-pass rate limiting with user tracking and Redis optimization"""

    try:
      # Get user from request (automatically populated by auth middleware)
      user = req.user
      if not user:
        raise HTTPException(status_code=500, detail="Failed to load user data")

      # Check if user is banned
      if user.status == "banned":
        raise HTTPException(status_code=403, detail="User is banned")

      # Execute request first to get response size
      response = await call_next(req)

      # Skip rate limiting for failed requests
      # NB: Disabled cause DDOS vulnerability
      # if response.status_code >= 400:
      #   return response

      # Get response size
      response_bytes = int(response.headers.get('Content-Length') or 0)

      # Combined rate limit check and increment
      err, result = await RateLimiter.check_and_increment(
          user, req.url.path, response_bytes)

      if err and result.get("limited"):
        # Rate limit exceeded after processing - unusual but handle gracefully
        raise HTTPException(
            status_code=429,
            detail=err,
            headers={"Retry-After": str(result.get("retry_after", 60))})

      # Add rate limit headers if not bypassed
      if not result.get("bypass"):
        response.headers.update({
            "X-RateLimit-Remaining":
            result.get("remaining", ""),
            "X-RateLimit-Reset":
            result.get("reset", "")
        })

      return response

    except HTTPException:
      raise
    except Exception as e:
      log_error(f"Rate limit middleware error: {e}")
      # Continue processing on middleware errors
      return await call_next(req)
