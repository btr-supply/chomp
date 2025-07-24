from datetime import timedelta
import fnmatch
from ..utils import now, fmt_date, secs_to_ceil_date, log_warn
from ..utils.decorators import service_method, cache
from .. import state
from ..cache import NS as REDIS_NS
from ..models import User
from ..services.auth import AuthService


class RateLimiter:
  """Optimized rate limiter with Redis pipelining and config-driven logic"""

  @classmethod
  @cache(ttl=-1, maxsize=512)
  def get_route_points(cls, path: str) -> int:
    """Get points for a route from centralized configuration with caching"""
    config = getattr(state, 'server_config', None)
    if not config or not config.route_limits:
      return 10

    # Check exact match first
    if path in config.route_limits:
      return config.route_limits[path].get("points", 10)

    # Check pattern matches using fnmatch
    for pattern, route_config in config.route_limits.items():
      if fnmatch.fnmatch(path, pattern):
        return route_config.get("points", 10)

    # Cache default
    return 10

  @classmethod
  def _get_user_limits_map(cls, user: User) -> dict[str, tuple[int, int]]:
    """Get user limits as a map of {name: (ttl_seconds, max_value)}."""
    return {
        'rpm': (60, user.rate_limits.rpm),
        'rph': (3600, user.rate_limits.rph),
        'rpd': (86400, user.rate_limits.rpd),
        'spm': (60, user.rate_limits.spm),
        'sph': (3600, user.rate_limits.sph),
        'spd': (86400, user.rate_limits.spd),
        'ppm': (60, user.rate_limits.ppm),
        'pph': (3600, user.rate_limits.pph),
        'ppd': (86400, user.rate_limits.ppd),
    }

  @classmethod
  @service_method("check and increment rate limit")
  async def check_and_increment(cls,
                                user: User,
                                path: str,
                                response_bytes: int = 0) -> dict:
    """
    Atomically checks rate limits and increments counters for a user.
    This is the core rate limiting logic, optimized for performance.
    """
    config = getattr(state, 'server_config', None)
    if user.uid in getattr(config, 'blacklist', []):
      log_warn(f"Blacklisted user {user.uid} attempted to access {path}")
      raise PermissionError("User is blacklisted")

    if user.uid in getattr(config, 'whitelist', []) or user.status == "admin":
      return {"bypass": True}

    points = cls.get_route_points(path)
    user_limits = cls._get_user_limits_map(user)

    active_limits = {
        name: (ttl, limit)
        for name, (ttl, limit) in user_limits.items() if limit > 0
    }
    if not active_limits:
      return {"bypass": True}

    increments = {
        'rpm': 1,
        'rph': 1,
        'rpd': 1,
        'spm': response_bytes,
        'sph': response_bytes,
        'spd': response_bytes,
        'ppm': points,
        'pph': points,
        'ppd': points
    }

    keys = [f"{REDIS_NS}:limiter:{name}:{user.uid}" for name in active_limits]

    # Use a Lua script for an atomic check-and-increment operation
    # to avoid race conditions and reduce network latency.
    # For now, we use a pipeline to get current values first.
    current_values = await state.redis.mget(keys)
    current_counts = [int(val) if val else 0 for val in current_values]

    # Check limits before incrementing
    for i, (name, (ttl, limit)) in enumerate(active_limits.items()):
      # For points, we check if the *next* value exceeds the limit
      is_points = name.startswith('pp')
      increment = increments[name]
      current = current_counts[i]

      if (is_points and current + increment > limit) or \
         (not is_points and current >= limit):

        # Find the tightest window for retry_after
        retry_after_ttl = min(limit_data[0]
                              for n, limit_data in active_limits.items()
                              if n.endswith(name[1:]))

        log_warn(f"Rate limit exceeded for user {user.uid} on {path} ({name})")
        raise ValueError(
            f"Rate limit exceeded ({name}). Try again in {retry_after_ttl} seconds."
        )

    # All checks passed, now increment all counters in a single pipeline
    pipe = state.redis.pipeline()
    remaining_info = []

    for i, (name, (ttl, limit)) in enumerate(active_limits.items()):
      key = keys[i]
      increment = increments[name]

      new_value = current_counts[i] + increment
      remaining = max(limit - new_value, 0)

      pipe.incrby(key, increment)
      pipe.expire(key, secs_to_ceil_date(secs=ttl))
      remaining_info.append(f"{name}={remaining}")

    await pipe.execute()

    return {
        "limited": False,
        "remaining": ";".join(remaining_info),
        "reset": fmt_date(now() + timedelta(seconds=60))
    }

  @classmethod
  @service_method("get user limits")
  async def get_user_limits(cls, user_id: str) -> dict:
    """Get current limits and usage for a user by user ID."""
    # Get user object for rate limiting
    user = await AuthService.get_user(user_id)
    if not user:
      log_warn(f"Attempted to get limits for non-existent user: {user_id}")
      raise ValueError(f"User {user_id} not found")

    user_limits_map = cls._get_user_limits_map(user)
    active_limits = {
        n: (t, limit_val)
        for n, (t, limit_val) in user_limits_map.items() if limit_val > 0
    }

    if not active_limits:
      return {}

    keys = [f"{REDIS_NS}:limiter:{name}:{user.uid}" for name in active_limits]

    async with state.redis.pipeline(transaction=False) as pipe:
      for key in keys:
        pipe.get(key)
        pipe.ttl(key)
      results = await pipe.execute()

    limits_data = {}
    current_time = now()

    current_values = [int(v) if v else 0 for v in results[::2]]
    ttls = [int(t) if t is not None and t > 0 else 0 for t in results[1::2]]

    for i, (name, (ttl_seconds, max_val)) in enumerate(active_limits.items()):
      current = current_values[i]
      ttl = ttls[i] or ttl_seconds
      reset_time = current_time + timedelta(seconds=ttl)

      limits_data[name] = {
          "cap": max_val,
          "remaining": max(max_val - current, 0),
          "ttl": ttl_seconds,
          "reset": fmt_date(reset_time)
      }

    return limits_data
