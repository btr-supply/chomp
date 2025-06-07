# TODO: batch state.redis tx commit whenever possible (cf. limiter.py)
from asyncio import gather, iscoroutinefunction, iscoroutine, sleep
from datetime import datetime, timedelta, timezone
from time import monotonic
from os import environ as env
import pickle
from random import random
import time
from typing import Callable, Optional, Any, Union

from .model import Ingester, Scope
from .utils import log_debug, log_info, log_error, log_warn, YEAR_SECONDS, now, merge_replace_empty
from . import state

NS = env.get("REDIS_NS", "chomp")


async def ping() -> bool:
  try:
    return await state.redis.ping()
  except Exception as e:
    log_error("Redis ping failed", e)
    return False


# clustering/synchronization
def claim_key(c: Ingester) -> str:
  return f"{NS}:claims:{c.id}"


async def claim_task(c: Ingester, until: int = 0, key: str = "") -> bool:
  if state.args.verbose:
    log_debug(f"Claiming task {c.name}.{c.interval}")
  # if c.last_ingested and c.last_ingested > (datetime.now(UTC) - interval_to_delta(c.interval)):
  #   log_warn(f"Ingestion time inconsistent for {c.name}.{c.interval}, last ingestion was {c.last_ingested}, probably due to a slow running ingestion or local worker race... investigate!")
  key = key or claim_key(c)
  if await is_task_claimed(c, True, key):
    return False
  return await state.redis.setex(
      key, round(until or (c.interval_sec + 8)),
      state.args.proc_id)  # 8 sec overtime buffer for long running tasks


async def ensure_claim_task(c: Ingester, until: int = 0) -> bool:
  if state.args.verbose:
    log_debug(f"Attempting to claim task {c.name}.{c.interval}")

  if not await claim_task(c, until):
    # Check if task is claimed by another worker
    key = claim_key(c)
    current_owner = await state.redis.get(key)
    if current_owner:
      current_owner = current_owner.decode()
      if current_owner == state.args.proc_id:
        log_warn(
            f"Task {c.name}.{c.interval} already claimed by this worker ({state.args.proc_id})"
        )
        return True  # Already claimed by us, treat as success
      else:
        raise ValueError(
            f"Failed to claim task {c.name}.{c.interval}: already claimed by worker {current_owner}"
        )
    else:
      raise ValueError(
          f"Failed to claim task {c.name}.{c.interval}: unknown claiming error"
      )

  if c.probablity < 1.0 and random() > c.probablity:
    raise ValueError(
        f"Task {c.name}.{c.interval} was probabilistically skipped (probability: {c.probablity})"
    )

  if state.args.verbose:
    log_debug(f"Successfully claimed task {c.name}.{c.interval}")
  return True


async def is_task_claimed(c: Ingester,
                          exclude_self: bool = False,
                          key: str = "") -> bool:
  key = key or claim_key(c)
  val = await state.redis.get(key)
  # unclaimed and uncontested
  return bool(val) and (not exclude_self or val.decode() != state.args.proc_id)


async def free_task(c: Ingester, key: str = "") -> bool:
  key = key or claim_key(c)
  if not (await is_task_claimed(c, key=key)) or await state.redis.get(
      key) != state.args.proc_id.encode():
    return False
  return await state.redis.delete(key)


def ingester_registry_lock_key() -> str:
  return f"{NS}:locks:ingesters"


async def acquire_registry_lock(lock_key: str = "",
                                timeout_ms: int = 5000) -> bool:
  if not lock_key:
    lock_key = ingester_registry_lock_key()
  return await state.redis.set(
      lock_key,
      state.args.proc_id,
      px=timeout_ms,  # auto-expire in case of crash
      nx=True  # upsert
  )


async def wait_acquire_registry_lock(timeout_ms: int = 10_000) -> bool:
  lock_key = ingester_registry_lock_key()
  timeout = monotonic() + (timeout_ms / 1000)
  while not await acquire_registry_lock(lock_key, timeout_ms):
    if monotonic() > timeout:
      return False
    await sleep(0.1)
  return True


async def release_registry_lock() -> bool:
  lock_key = ingester_registry_lock_key()
  # to call only if we own the lock
  return await state.redis.delete(lock_key)


# caching
def cache_key(name: str) -> str:
  return f"{NS}:cache:{name}"


async def cache(name: str,
                value: Any,
                expiry: int = YEAR_SECONDS,
                raw_key: bool = False,
                encoding: str = "",
                pickled: bool = False) -> bool:
  processed_value: Union[str, bytes]
  if pickled:
    processed_value = pickle.dumps(value)
  elif encoding and isinstance(value, str):
    processed_value = value.encode(encoding)
  elif not isinstance(value, (str, bytes)):
    processed_value = str(value)
  else:
    processed_value = value

  return await state.redis.setex(name if raw_key else cache_key(name),
                                 round(expiry), processed_value)


async def cache_batch(data: dict,
                      expiry: int = YEAR_SECONDS,
                      pickled: bool = False,
                      encoding: str = "",
                      raw_key: bool = False) -> None:
  expiry = round(expiry)
  keys_values: dict[str, Union[str, bytes]] = {}
  for name, value in data.items():
    key = name if raw_key else cache_key(name)
    processed_value: Union[str, bytes]
    if pickled:
      processed_value = pickle.dumps(value)
    elif encoding and isinstance(value, str):
      processed_value = value.encode(encoding)
    elif not isinstance(value, (str, bytes)):
      processed_value = str(value)
    else:
      processed_value = value
    keys_values[key] = processed_value

  async with state.redis.pipeline() as pipe:
    for key, value in keys_values.items():
      pipe.setex(key, expiry, value)
    await pipe.execute()


async def get_cache(name: str,
                    pickled: bool = False,
                    encoding: str = "",
                    raw_key: bool = False) -> Any:
  r = await state.redis.get(name if raw_key else cache_key(name))
  if r in (None, b"", ""):
    return None
  if pickled:
    return pickle.loads(r)
  elif encoding and isinstance(r, bytes):
    return r.decode(encoding)
  else:
    return r


async def get_cache_batch(names: list[str],
                          pickled: bool = False,
                          encoding: str = "",
                          raw_key: bool = False) -> dict[str, Any]:
  keys = [(name if raw_key else cache_key(name)) for name in names]
  r = await state.redis.mget(*keys)
  res: dict[str, Any] = {}
  for name, value in zip(names, r):
    if value is None:
      res[name] = None
    else:
      res[name] = pickle.loads(value) if pickled else value.decode(
          encoding) if encoding else value
  return res


async def get_or_set_cache(name: str,
                           callback: Callable,
                           expiry: int = YEAR_SECONDS,
                           pickled: bool = False,
                           encoding: str = "") -> Any:
  key = cache_key(name)
  value = await get_cache(key, raw_key=True)
  if value:
    return pickle.loads(value) if pickled else value.decode(
        encoding) if encoding and isinstance(value, bytes) else value
  else:
    value = callback(
    ) if not iscoroutinefunction(callback) else await callback()
    if iscoroutine(value):
      value = await value
    if value in (None, b"", ""):
      log_warn(f"Cache could not be rehydrated for key: {key}")
      return None
    await cache(key, value, expiry=expiry, pickled=pickled, raw_key=True)
    return value


# pubsub
async def pub(topics: Union[list[str], str], msg: str) -> list[Any]:
  tasks = []
  if isinstance(topics, str):
    topics = [topics]
  for topic in topics:
    tasks.append(state.redis.publish(f"{NS}:{topic}", msg))
  return await gather(*tasks)


async def sub(topics: list[str], handler: Callable) -> None:
  # Use the centralized pubsub
  await state.redis.pubsub.subscribe(*topics)
  async for msg in state.redis.pubsub.listen():
    if msg["type"] == "message":
      handler(msg["data"])


# ingester registry
def ingester_registry_key(name: str = "all") -> str:
  return f"{NS}:ingesters:{name}"


async def register_ingester(ingester: Ingester,
                            scope: Scope = Scope.ALL) -> bool:
  log_info(f"Registering {ingester.name}...")
  global_key = ingester_registry_key()
  specific_key = ingester_registry_key(ingester.name)

  # lock registry to prevent race conditions
  if not await wait_acquire_registry_lock():
    raise ValueError(f"Failed to acquire registry lock for {ingester.name}")
  try:
    registry = await get_cache(global_key, pickled=True, raw_key=True) or {}
    as_dict = ingester.to_dict(scope)

    # Add runtime metadata to the registry entry
    as_dict["runtime_metadata"] = {
        "process_id":
        state.args.proc_id
        if hasattr(state, 'args') and state.args else "unknown",
        "instance_uid":
        getattr(state, 'instance_uid', 'unknown'),
        "instance_ip":
        getattr(state, 'instance_ip', 'unknown'),
        "registered_at":
        datetime.now(timezone.utc).isoformat(),
        "monitoring_enabled":
        getattr(state.args, 'monitored', False)
        if hasattr(state, 'args') else False
    }

    registry[ingester.name] = as_dict
    await gather(cache(global_key, registry, pickled=True, raw_key=True),
                 cache(specific_key, as_dict, pickled=True, raw_key=True),
                 return_exceptions=True)

    return True
  finally:
    await release_registry_lock()


async def unregister_ingester(ingester: Ingester) -> bool:
  log_info(f"Unregistering {ingester.name}...")
  global_key = ingester_registry_key()
  specific_key = ingester_registry_key(ingester.name)
  registry = await get_cache(global_key, pickled=True, raw_key=True) or {}
  registry.pop(ingester.name, None)
  results = await gather(cache(global_key,
                               registry,
                               pickled=True,
                               raw_key=True),
                         state.redis.delete(specific_key),
                         return_exceptions=True)
  return all(not isinstance(r, Exception) for r in results)


async def get_registered_ingester(name: str) -> dict[str, Any]:
  return await get_cache(
      ingester_registry_key(name), pickled=True, raw_key=True) or {}


async def get_registered_ingesters() -> dict[str, Any]:
  return await get_registered_ingester("all")


_ingesters_cache: dict[str, tuple[datetime, Ingester]] = {}
CONFIG_CACHE_TTL = 300  # 5min ttl


async def load_ingester_config(name: str) -> Optional[Ingester]:
  key = ingester_registry_key(name)
  global _ingesters_cache
  n = now()
  if key in _ingesters_cache and _ingesters_cache[key][0] > n:
    return _ingesters_cache[key][1]
  config = await get_cache(key, pickled=True, raw_key=True)
  if not config:
    log_error(f"No ingester config found for {name}")
    return None
  ingester_obj = Ingester.from_config(config)
  _ingesters_cache[key] = (n + timedelta(seconds=CONFIG_CACHE_TTL),
                           ingester_obj)
  return ingester_obj


async def inherit_fields(ingester: Ingester) -> Ingester:
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
      dep_field = dep_config.field_by_name().get(field_name)
      if dep_field:
        # merge field attributes from dependency config
        # source (dep_field) takes precedence over destination (field)
        merged_dict = merge_replace_empty(field.__dict__, dep_field.__dict__)
        # Update the field object with merged attributes
        for key, value in merged_dict.items():
          setattr(field, key, value)
  return ingester


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
  registered = await get_registered_ingesters()
  cached = set(await get_cached_resources())
  streamed_result = await get_topics(with_subs=False)
  streamed = set(streamed_result if isinstance(streamed_result, list) else
                 list(streamed_result.keys()))
  for r in registered.keys():
    registered[r]["cached"] = r in cached
    registered[r]["streamed"] = r in streamed
  return registered


async def get_resource_status() -> dict[str, Any]:
  return await get_or_set_cache(get_status_key("resources"),
                                callback=lambda: hydrate_resources_status(),
                                expiry=60,
                                pickled=True)


# Manual cache for topics with a TTL
_topics_cached_at: float = 0
_topics_cache: Optional[list[str]] = None


async def get_cached_topics(ttl: int = 900) -> list[str]:
  global _topics_cached_at, _topics_cache
  current_time = time.time()
  if current_time > _topics_cached_at + ttl:
    topics_result = await get_topics()
    _topics_cache = topics_result if isinstance(topics_result, list) else list(
        topics_result.keys())
    _topics_cached_at = current_time
  return _topics_cache or []


async def topic_exist(topic: str, ttl: int = 900) -> bool:
  topics = await get_cached_topics(ttl)
  return topic in topics


async def topics_exist(topics: list[str], ttl: int = 900) -> dict[str, bool]:
  cached_topics = await get_cached_topics(ttl)
  return {topic: topic in cached_topics for topic in topics}
