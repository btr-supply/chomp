from datetime import datetime, timedelta
from typing import Optional
from ..utils import fmt_date, secs_to_ceil_date, UTC
from .. import state
from ..cache import NS as REDIS_NS
from ..model import ServiceResponse


async def check_limits(user: str,
                       path: Optional[str] = None) -> ServiceResponse[dict]:
  """Check if user has exceeded any limits"""
  limiter = getattr(state.server, 'limiter')

  if user in limiter.blacklist:
    return "User is blacklisted", {}

  if user in limiter.whitelist:
    return "", {"whitelisted": True}

  # Get points per request for this path
  ppr = limiter.ppr.get(path, 1) if path else 1

  # Prepare keys for fetching current counts
  keys = []
  for name, (max_val, _) in limiter.limits.items():
    if max_val is not None:
      key = f"{REDIS_NS}:limiter:{name}:{user}"
      keys.append(key)

  if ppr:
    points_key = f"{REDIS_NS}:limiter:points:{user}"
    keys.append(points_key)

  # Fetch current counts
  current_counts = await state.redis.mget(*keys)
  current_counts = [int(count) if count else 0 for count in current_counts]

  # Check if limits have been breached
  for (name, (max_val, _)), current in zip(limiter.limits.items(),
                                           current_counts):
    if max_val > 0 and current >= max_val:
      return f"Rate limit exceeded ({name}: {current}/{max_val})", {}

  return "", {"current_counts": current_counts, "ppr": ppr}


async def get_user_limits(user: str) -> ServiceResponse[dict]:
  """Get current limits for a user"""
  limiter = getattr(state.server, 'limiter')

  # Get all limit keys for user
  user_limit_keys = [
      f"{REDIS_NS}:limiter:{name}:{user}" for name in limiter.limits
  ]
  theoretical_ttl_by_interval = {
      i: secs_to_ceil_date(secs=i)
      for i in [60, 3600, 86400]
  }

  # Combine Redis operations in a single pipeline
  try:
    async with state.redis.pipeline(transaction=False) as pipe:
      for key in user_limit_keys:
        pipe.get(key)
        pipe.ttl(key)
      res = await pipe.execute()
      values, ttls = [int(v if v else 0) for v in res[::2]], [
          int(ttl) if ttl is not None and int(ttl) > 0 else 0
          for ttl in res[1::2]
      ]

    limits = {}
    for key, (name, (max_val,
                     interval)), current, ttl in zip(user_limit_keys,
                                                     limiter.limits.items(),
                                                     values, ttls):
      ttl = ttl or theoretical_ttl_by_interval[interval]
      reset_time_str = fmt_date(datetime.now(UTC) +
                                timedelta(seconds=ttl)) if ttl > 0 else None
      remaining = max(int(max_val) - int(current or 0), 0)

      limits[name] = {
          "cap": int(max_val),
          "remaining": remaining,
          "ttl": interval,
          "reset": reset_time_str
      }

    return "", limits
  except Exception as e:
    return f"Error fetching limits: {str(e)}", {}


async def increment_counters(user: str, response_size: int,
                             ppr: int) -> ServiceResponse[dict]:
  """Increment rate limit counters for a user"""
  limiter = getattr(state.server, 'limiter')

  increments = [
      1, 1, 1, response_size, response_size, response_size, ppr, ppr, ppr
  ]
  ttls = [
      secs_to_ceil_date(secs=60),
      secs_to_ceil_date(secs=3600),
      secs_to_ceil_date(secs=86400)
  ]

  try:
    limit_pairs, remaining_pairs = [], []
    async with state.redis.pipeline() as pipe:
      for (name, (max_val, _)), increment, ttl in zip(limiter.limits.items(),
                                                      increments, ttls * 3):
        key = f"{REDIS_NS}:limiter:{name}:{user}"
        pipe.incrby(key, increment)
        pipe.expire(key, ttl)

        # Get current value for calculating remaining
        current = int(await state.redis.get(key) or 0)
        remaining = max(max_val - (current + increment), 0)

        limit_pairs.append(f"{name}={max_val}")
        remaining_pairs.append(f"{name}={remaining}")

      await pipe.execute()

    return "", {
        "limits":
        ";".join(limit_pairs),
        "remaining":
        ";".join(remaining_pairs),
        "reset":
        f"rpm,spm,ppm={ttls[0]};rph,sph,pph={ttls[1]};rpd,spd,ppd={ttls[2]}"
    }
  except Exception as e:
    return f"Error incrementing counters: {str(e)}", {}
