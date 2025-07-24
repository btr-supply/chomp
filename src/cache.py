from asyncio import gather, iscoroutinefunction, iscoroutine, sleep
from os import environ as env
import pickle
from typing import Callable, Any, Optional, Union

from .models.ingesters import Ingester
from .models.base import Scope
from .utils import now, log_debug, log_info, log_error, log_warn, YEAR_SECONDS, merge_replace_empty, Interval
from . import state
from .utils.decorators import cache as _cache

NS = env.get("REDIS_NS", "chomp")


async def ping() -> bool:
  try:
    return await state.redis.ping()
  except Exception as e:
    log_error("Redis ping failed", e)
    return False


def claim_key(ing: Ingester) -> str:
  return f"{NS}:claim:{ing.name}:{ing.interval}"


async def claim_task(ing: Ingester, until: int = 0, key: str = "") -> bool:
  if state.args.verbose:
    log_debug(f"Claiming task {ing.name}.{ing.interval}")
  # if ing.last_ingested and ing.last_ingested > (now() - interval_to_delta(ing.interval)):
  #   log_warn(f"Ingestion time inconsistent for {ing.name}.{ing.interval}, last ingestion was {ing.last_ingested}, probably due to a slow running ingestion or local worker race... investigate!")
  key = key or claim_key(ing)
  if await is_task_claimed(ing, True, key):
    return False
  return await state.redis.setex(
      key, round(until or (ing.interval_sec + 8)),
      state.args.proc_id)  # 8 sec overtime buffer for long running tasks


async def ensure_claim_task(ing: Ingester, until: int = 0) -> bool:
  """Ensures that a task is claimed by the current worker to avoid double runs"""
  if await claim_task(ing, until):
    return True
  claims_timeout = 120
  claim_start = now().timestamp()
  backoff = [0.1, 0.3, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
  backoff_idx = 0

  while now().timestamp() - claim_start < claims_timeout:
    if await claim_task(ing, until):
      return True
    backoff_val = backoff[min(backoff_idx, len(backoff) - 1)]
    await sleep(backoff_val)
    backoff_idx += 1

  # Attempt a force claim as last resort
  log_warn(
      f"Could not claim {ing.name}.{ing.interval} - attempting force claim...")
  if await claim_task(ing, until, f"{claim_key(ing)}:force"):
    return True

  log_error(
      f"Failed to claim {ing.name}.{ing.interval} after {claims_timeout}s")
  return False


async def is_task_claimed(ing: Ingester,
                          exclude_self: bool = False,
                          key: str = "") -> bool:
  key = key or claim_key(ing)
  val = await state.redis.get(key)
  # unclaimed and uncontested
  return bool(val) and (not exclude_self or val.decode() != state.args.proc_id)


async def free_task(ing: Ingester, key: str = "") -> bool:
  key = key or claim_key(ing)
  return bool(await state.redis.delete(key))


# Monitoring-based discovery implementation


def cache_key(name: str) -> str:
  return f"{NS}:cache:{name}"


async def cache(name: str,
                value: Any,
                expiry: int = YEAR_SECONDS,
                raw_key: bool = False,
                encoding: str = "",
                pickled: bool = False) -> bool:
  key = name if raw_key else cache_key(name)
  # use pickle by default for complex objects unless explicitly disabled
  if pickled or (not isinstance(value, (str, int, float, bool, type(None)))
                 and not encoding):
    value = pickle.dumps(value)
  elif encoding:
    value = value.encode(encoding) if isinstance(
        value, str) else str(value).encode(encoding)
  else:
    value = str(value)
  return bool(await state.redis.setex(key, expiry, value))


async def cache_batch(data: dict,
                      expiry: int = YEAR_SECONDS,
                      pickled: bool = False,
                      encoding: str = "",
                      raw_key: bool = False) -> None:
  """Cache multiple key-value pairs in a single batch operation."""
  async with state.redis.pipeline() as pipe:
    for name, value in data.items():
      key = name if raw_key else cache_key(name)

      # Simplified value processing
      if pickled:
        processed_value = pickle.dumps(value)
      elif encoding:
        processed_value = (value.encode(encoding) if isinstance(value, str)
                           else str(value).encode(encoding))
      elif isinstance(value, (str, int, float, bool, type(None))):
        processed_value = str(value)
      else:
        # Non-serializable types default to pickle
        processed_value = pickle.dumps(value)

      pipe.setex(key, expiry, processed_value)

    await pipe.execute()


def decode_cache_value(value: bytes,
                       pickled: bool = False,
                       encoding: str = "") -> Any:
  """Generic cache value decoder for consistent decoding across cache functions"""
  if pickled:
    return pickle.loads(value)
  elif encoding:
    return value.decode(encoding)
  else:
    return value.decode()


async def get_cache(name: str,
                    pickled: bool = False,
                    encoding: str = "",
                    raw_key: bool = False) -> Any:
  key = name if raw_key else cache_key(name)
  value = await state.redis.get(key)
  if value is None:
    return None
  return decode_cache_value(value, pickled, encoding)


async def get_cache_batch(names: list[str],
                          pickled: bool = False,
                          encoding: str = "",
                          raw_key: bool = False) -> dict[str, Any]:
  keys = [name if raw_key else cache_key(name) for name in names]
  values = await state.redis.mget(keys)

  return {
      names[i]: decode_cache_value(value, pickled, encoding)
      for i, value in enumerate(values) if value is not None
  }


async def get_or_set_cache(name: str,
                           callback: Callable,
                           expiry: int = YEAR_SECONDS,
                           pickled: bool = False,
                           encoding: str = "") -> Any:
  value = await get_cache(name, pickled, encoding)
  if value is not None:
    return value
  if iscoroutinefunction(callback):
    value = await callback()
  elif iscoroutine(callback):
    value = await callback
  else:
    value = callback()
  await cache(name, value, expiry, pickled=pickled, encoding=encoding)
  return value


async def pub(topics: Union[list[str], str], msg: Any) -> list[Any]:
  if isinstance(topics, str):
    topics = [topics]
  tasks = [
      state.redis.publish(f"{NS}:{topic}", pickle.dumps(msg))
      for topic in topics
  ]
  return await gather(*tasks)


async def sub(topics: list[str], handler: Callable) -> None:
  # Use the centralized pubsub
  await state.redis.pubsub.subscribe(*topics)
  async for msg in state.redis.pubsub.listen():
    if msg["type"] == "message":
      handler(msg["data"])


# Monitoring-based discovery functions
async def get_active_instances() -> dict[str, Any]:
  """Get all active instances from monitoring data, optimized for speed."""
  instance_keys = await state.redis.keys(f"{NS}:cache:*.monitor")
  if not instance_keys:
    return {}

  # Extract monitor names (e.g., "my_instance.monitor") from Redis keys
  monitor_names = [
      ":".join(key.decode().split(":")[2:]) for key in instance_keys
  ]

  # Batch fetch all monitor data in a single transaction
  monitor_data_batch = await get_cache_batch(monitor_names, pickled=True)

  instances = {}
  key_map = {
      "instance_name": ("instance_name", None),
      "updated_at": ("ts", None),
      "resources_count": ("resources_count", 0),
      "cpu_usage": ("cpu_usage", 0.0),
      "memory_usage": ("memory_usage", 0.0),
      "coordinates": ("coordinates", ""),
      "location": ("location", ""),
  }
  for name, data in monitor_data_batch.items():
    instance_name = name.replace(".monitor", "")
    # An instance monitor has its name in the 'instance_name' field.
    if data and isinstance(data, dict) and instance_name in data.get(
        "instance_name", ""):
      instance_data = {
          dest: data.get(src, default)
          for dest, (src, default) in key_map.items()
      }
      instance_data.update(name=instance_name, status="active")
      instances[instance_name] = instance_data
  return instances


async def get_active_ingesters() -> dict[str, Any]:
  """Get active ingesters with full configurations, optimized for speed."""
  registered = await get_registered_ingesters()
  if not registered:
    log_warn(
        "No registry data found - ingesters may not be properly registered")
    return {}

  cached_resources = set(await get_cached_resources())

  # Filter for active ingesters and prepare for batch fetch
  active_names = [name for name in registered if name in cached_resources]
  if not active_names:
    return {}

  latest_data_batch = await get_cache_batch(active_names, pickled=True)

  active_ingesters = {}
  for name in active_names:
    config = registered[name].copy()
    latest_data = latest_data_batch.get(name)

    config.update({
        "status": "active",
        "cached": True,
        "field_count": len(config.get("fields", {})),
    })

    if latest_data and isinstance(latest_data, dict):
      config.update({
          "last_ingested": latest_data.get("ts"),
          "sample_data": {
              k: v
              for k, v in list(latest_data.items())[:3]
              if not k.startswith("_")
          }
      })
    active_ingesters[name] = config

  return active_ingesters


async def get_ingester_status(ingester_name: str) -> Optional[dict[str, Any]]:
  """Get status of a specific ingester from cache and monitoring data"""
  try:
    # Check if ingester has cached data (indicates it's active)
    cached_data = await get_cache(ingester_name, pickled=True)
    if not cached_data:
      return None

    # Check if there's monitoring data
    monitor_data = await get_cache(f"{ingester_name}.monitor", pickled=True)

    status = {
        "name": ingester_name,
        "status": "active" if cached_data else "inactive",
        "last_ingested": cached_data.get("ts") if cached_data else None,
        "cached": bool(cached_data),
        "monitored": bool(monitor_data)
    }

    if monitor_data:
      status.update({
          "latency_ms": monitor_data.get("latency_ms"),
          "response_bytes": monitor_data.get("response_bytes"),
          "status_code": monitor_data.get("status_code"),
          "instance_name": monitor_data.get("instance_name")
      })

    return status
  except Exception:
    return None


async def is_ingester_claimed(ingester_name: str,
                              interval: Interval = "h1") -> bool:
  """Check if an ingester is currently claimed by any worker"""
  # Check claim directly using the ingester name and interval
  claim_key_str = f"task_claim:{ingester_name}:{interval}"
  return await state.redis.exists(claim_key_str)


async def discover_cluster_state() -> dict[str, Any]:
  """Discover complete cluster state from monitoring data"""
  instances, ingesters, topics = await gather(get_active_instances(),
                                              get_active_ingesters(),
                                              get_topics())

  return {
      "instances": instances,
      "ingesters": ingesters,
      "topics": topics,
      "total_instances": len(instances),
      "total_ingesters": len(ingesters),
      "total_topics": len(topics),
      "timestamp": now().isoformat()
  }


# Registry functions - simplified and generic
def registry_key(registry_type: str, key: str = "all") -> str:
  """Generic registry key builder"""
  return f"{NS}:registry:{registry_type}:{key}"


async def register_item(registry_type: str, item_key: str, data: dict) -> bool:
  """Generic registration function"""
  try:
    # Store individual item
    await cache(
        registry_key(registry_type, item_key),
        data,
        expiry=86400,  # 24 hours instead of 1 year
        pickled=True)

    # Update aggregated registry
    registry = await get_cache(registry_key(registry_type), pickled=True) or {}
    registry[item_key] = data
    await cache(
        registry_key(registry_type),
        registry,
        expiry=86400,  # 24 hours instead of 1 year
        pickled=True)
    return True
  except Exception as e:
    log_error(f"Failed to register {registry_type} {item_key}: {e}")
    return False


async def get_registry(registry_type: str) -> dict[str, Any]:
  """Generic registry getter"""
  try:
    return await get_cache(registry_key(registry_type), pickled=True) or {}
  except Exception as e:
    log_error(f"Failed to get {registry_type} registry: {e}")
    return {}


# Simplified ingester registry functions
async def register_ingester(ingester: Ingester,
                            scope: Scope = Scope.ALL) -> bool:
  """Register ingester configuration"""
  return await register_item("ingesters", ingester.name,
                             ingester.to_dict(scope))


async def register_instance(instance: Any) -> bool:
  """Register instance configuration"""
  from .models import Instance
  if not isinstance(instance, Instance):
    log_error("register_instance requires Instance object")
    return False
  return await register_item("instances", instance.uid, instance.to_dict())


async def get_registered_ingesters() -> dict[str, Any]:
  """Get all registered ingester configurations"""
  return await get_registry("ingesters")


async def get_registered_instances() -> dict[str, Any]:
  """Get all registered instance configurations"""
  return await get_registry("instances")


# status
def get_status_key(name: str) -> str:
  return f"{NS}:status:{name}"


async def get_cached_resources() -> list[str]:
  r = await state.redis.keys(cache_key("*"))
  return [key.decode().split(":")[-1] for key in r]


async def get_topics(
    with_subs: bool = False) -> Union[list[str], dict[str, Any]]:
  r = await state.redis.pubsub_channels(f"{NS}:*")
  chans = [chan.decode().split(":")[-1] for chan in r]
  if with_subs:  # this is redundant since chans only contains subscribed topics
    async with state.redis.pipeline(transaction=True) as pipe:
      for chan in chans:
        pipe.pubsub_numsub(chan)
      results = await pipe.execute()
      return {chan: results[i] for i, chan in enumerate(chans)}
  return chans


async def hydrate_resources_status() -> dict[str, Any]:
  """Updated to use monitoring-based discovery instead of registry"""
  # Get active ingesters from monitoring data instead of registry
  active_ingesters = await get_active_ingesters()
  cached = set(await get_cached_resources())
  streamed_result = await get_topics(with_subs=False)
  streamed = set(streamed_result if isinstance(streamed_result, list) else
                 list(streamed_result.keys()))

  # Update the monitoring data with cache/stream status
  for name, ingester_data in active_ingesters.items():
    ingester_data["cached"] = name in cached
    ingester_data["streamed"] = name in streamed

  return active_ingesters


async def get_resource_status() -> dict[str, Any]:
  return await get_or_set_cache(get_status_key("resources"),
                                callback=hydrate_resources_status,
                                expiry=60,
                                pickled=True)


@_cache(ttl=900, maxsize=2048)
async def get_cached_topics() -> list[str]:
  """
  Returns a cached list of topic names.
  The cache expires every 15 minutes (900 seconds).
  """
  topics_result = await get_topics()
  return topics_result if isinstance(topics_result, list) else list(
      topics_result.keys())


async def topic_exist(topic: str) -> bool:
  topics = await get_cached_topics()
  return topic in topics


async def topics_exist(topics: list[str]) -> dict[str, bool]:
  cached_topics = await get_cached_topics()
  return {topic: topic in cached_topics for topic in topics}


async def load_ingester_config(ingester_name: str) -> Optional[Ingester]:
  """Load ingester configuration from registry or current config"""
  # First try registry (for running ingesters)
  registered = await get_registry("ingesters")
  if ingester_name in registered:
    config_dict = registered[ingester_name]
    # Convert back to Ingester object
    return Ingester.from_config(config_dict)

  # Fallback to current state config (for processors that depend on ingesters in same config)
  config = getattr(state, 'config', None)
  if config and getattr(config, 'ingesters', None):
    for ingester in config.ingesters:
      if ingester.name == ingester_name:
        return ingester

  return None


async def inherit_fields(ingester: Ingester) -> Ingester:
  """Process processor ingester fields to inherit metadata from dependency ingesters"""
  # dependency ingester names from field selectors
  dep_names = ingester.dependencies()
  log_info(f"{ingester.name} inheriting fields from {dep_names}...")
  dep_configs: dict[str, Optional[Ingester]] = {}
  # cached configs for each
  for dep_name in dep_names:
    dep_configs[dep_name] = await load_ingester_config(dep_name)
    if not dep_configs[dep_name]:
      continue

  # for each field in original ingester
  for field in ingester.fields:
    if not field.selector:
      continue
    split = field.selector.split('.', 1)
    if len(split) > 1 and split[0] in dep_names:
      dep_name, field_name = split
      # match field in dependency config
      dep_config = dep_configs[dep_name]
      if not dep_config:
        log_warn(
            f"Missing dependency config: {dep_name}, make sure that the ingester running"
        )
        continue
      dep_field = dep_config.get_field(field_name)
      if dep_field:
        # merge field attributes from dependency config
        # source (dep_field) takes precedence over destination (field)
        merged_dict = merge_replace_empty(field.__dict__, dep_field.__dict__)
        # Update the field object with merged attributes
        for key, value in merged_dict.items():
          setattr(field, key, value)
  return ingester
