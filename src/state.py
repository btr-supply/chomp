from fastapi import FastAPI
from typing import Any, Optional
import asyncio

from .utils import PackageMeta
from .proxies import (
    ThreadPoolProxy,
    Web3Proxy,
    TsdbProxy,
    RedisProxy,
    IngesterConfigProxy,
    ServerConfigProxy,
    meta,
)

args: Any
server: FastAPI
tsdb: TsdbProxy
redis: RedisProxy
ingester_config: IngesterConfigProxy
server_config: ServerConfigProxy
web3: Web3Proxy
thread_pool: ThreadPoolProxy
meta: PackageMeta = meta
instance: Optional[Any] = None
redis_task: Optional[asyncio.Task] = None


# resource monitor ingesters are now cached per resource in a dict
async def init(_args: Any):
  global args, meta, thread_pool, web3, tsdb, redis, ingester_config, server_config, instance
  args = _args

  # Initialize config proxies based on whether we are running as a server or not
  if args.server:
    server_config, ingester_config = ServerConfigProxy(args), None
  else:
    ingester_config, server_config = IngesterConfigProxy(args), None

  thread_pool = ThreadPoolProxy()
  tsdb = TsdbProxy()
  redis = RedisProxy()
  web3 = Web3Proxy()  # Uses rotate_always=True by default

  from .models import Instance

  # Create instance first without resource count to avoid circular import
  instance = await Instance.from_dict(
      data={
          "mode": "server" if args.server else "ingester",
          "resources_count": 0,  # Will be updated after config loading
          "args": args,  # Store parsed CLI/env arguments
      })

  # Now load config with instance available for monitor initialization
  if ingester_config:
    ingester_config._instance = instance  # Pass instance to config proxy

  # Update resource count after config is loaded for ingesters
  if not args.server and ingester_config:
    instance.resources_count = len(ingester_config.ingesters)


async def start_redis_listener(pattern: str):
  """Start the Redis pubsub listener task if not already running"""
  global redis_task

  if redis_task is None:
    from .server.routers.forwarder import handle_redis_messages

    redis_task = asyncio.create_task(handle_redis_messages())


async def stop_redis_listener():
  """Stop the Redis pubsub listener task"""
  global redis_task

  if redis_task is not None:
    redis_task.cancel()
    try:
      await redis_task
    except asyncio.CancelledError:
      pass
    redis_task = None
