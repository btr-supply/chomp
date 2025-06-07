from fastapi import FastAPI
from typing import Any, Optional
import asyncio

from .utils import PackageMeta
from .proxies import (
    ThreadPoolProxy,
    Web3Proxy,
    TsdbProxy,
    RedisProxy,
    ConfigProxy,
    meta,
)
from .deps import safe_import

# Import Instance directly to fix mypy issues

args: Any
server: FastAPI
tsdb: TsdbProxy
redis: RedisProxy
config: ConfigProxy
web3: Web3Proxy
thread_pool: ThreadPoolProxy
meta: PackageMeta = meta
instance: Optional[Any] = None
redis_task: Optional[asyncio.Task] = None
# resource monitor ingesters are now cached per resource in a dict


async def init(args_: Any):
    global args, meta, thread_pool, web3, tsdb, redis, config, instance
    args = args_
    config = ConfigProxy(args)
    thread_pool = ThreadPoolProxy()
    tsdb = TsdbProxy()
    redis = RedisProxy()
    web3 = Web3Proxy()

    from .model import Instance

    instance = await Instance.from_dict(
        {
            "mode": "server"
            if (hasattr(args, "server") and args.server)
            else "ingester",
            "monitored": hasattr(args, "monitored") and args.monitored,
            "resources_count": len(config.ingesters)
            if hasattr(config, "ingesters")
            else 0,
        }
    )


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


multicall = safe_import("multicall")

# TODO: PR these multicall constants upstream
if multicall is not None:
    mc_const = multicall.constants
    mc_const.MULTICALL3_ADDRESSES[130] = (
        "0xcA11bde05977b3631167028862bE2a173976CA11"  # unichain
    )
    mc_const.MULTICALL3_ADDRESSES[143] = (
        "0xcA11bde05977b3631167028862bE2a173976CA11"  # monad
    )
    mc_const.MULTICALL3_ADDRESSES[146] = (
        "0xcA11bde05977b3631167028862bE2a173976CA11"  # sonic
    )
    mc_const.MULTICALL3_ADDRESSES[238] = (
        "0xcA11bde05977b3631167028862bE2a173976CA11"  # blast
    )
    mc_const.MULTICALL3_ADDRESSES[3073] = (
        "0xcA11bde05977b3631167028862bE2a173976CA11"  # movement
    )
    mc_const.MULTICALL3_ADDRESSES[5000] = (
        "0xcA11bde05977b3631167028862bE2a173976CA11"  # mantle
    )
    mc_const.MULTICALL3_ADDRESSES[59144] = (
        "0xcA11bde05977b3631167028862bE2a173976CA11"  # linea
    )
    mc_const.MULTICALL3_ADDRESSES[80094] = (
        "0xcA11bde05977b3631167028862bE2a173976CA11"  # bera
    )
    mc_const.MULTICALL3_ADDRESSES[534352] = (
        "0xcA11bde05977b3631167028862bE2a173976CA11"  # scroll
    )
    mc_const.MULTICALL3_ADDRESSES[1440002] = (
        "0xcA11bde05977b3631167028862bE2a173976CA11"  # xrpl
    )
    mc_const.GAS_LIMIT = 5_000_000
