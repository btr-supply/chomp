from fastapi import FastAPI
from multicall import constants as mc_const

from .utils import log_info, PackageMeta
from .proxies import ThreadPoolProxy, Web3Proxy, TsdbProxy, RedisProxy, ConfigProxy, meta
import asyncio

args: any
server: FastAPI
tsdb: TsdbProxy
redis: RedisProxy
config: ConfigProxy
web3: Web3Proxy
thread_pool: ThreadPoolProxy
meta: PackageMeta = meta

# Global pubsub task management
redis_task = None

def init(args_: any):
  global args, meta, thread_pool, rpcs, web3, tsdb, redis, config
  args = args_
  config = ConfigProxy(args)
  thread_pool = ThreadPoolProxy()
  tsdb = TsdbProxy()
  redis = RedisProxy()
  web3 = Web3Proxy()

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

# TODO: PR these multicall constants upstream
mc_const.MULTICALL3_ADDRESSES[238] = "0xcA11bde05977b3631167028862bE2a173976CA11" # blast
mc_const.MULTICALL3_ADDRESSES[5000] = "0xcA11bde05977b3631167028862bE2a173976CA11" # mantle
mc_const.MULTICALL3_ADDRESSES[59144] = "0xcA11bde05977b3631167028862bE2a173976CA11" # linea
mc_const.MULTICALL3_ADDRESSES[534352] = "0xcA11bde05977b3631167028862bE2a173976CA11" # scroll
mc_const.GAS_LIMIT = 5_000_000
