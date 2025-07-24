import orjson
from fastapi import APIRouter, HTTPException
from fastapi.websockets import WebSocketState, WebSocket, WebSocketDisconnect
from fastapi.concurrency import asynccontextmanager
from typing import Literal, Optional
from asyncio import gather, Task, create_task, CancelledError, sleep
import fnmatch
import pickle
from aiocron import crontab
from contextlib import suppress

from ...utils import log_debug, log_error, log_info, log_warn, now
from ... import state
from ..responses import ORJSON_OPTIONS
from ...services.limiter import RateLimiter
from ...services.auth import AuthService
from ...services.loader import is_resource_protected
from ...models import User
from ...constants import (
    WS_MAX_CLIENTS,
    WS_CLIENT_MAX_LIFETIME_S,
    WS_ALLOWED_TOPICS_PATTERN,
)

WsAction = Literal["subscribe", "unsubscribe", "ping"]

# Global state - optimized for memory and performance
redis_listener_task: Optional[Task] = None
clients_by_topic: dict[str, set[WebSocket]] = {}
topics_by_client: dict[WebSocket, set[str]] = {}
client_users: dict[WebSocket,
                   User] = {}  # Removed Optional - all clients must have users
client_connect_times: dict[WebSocket, float] = {}

# Message cache for broadcast optimization
_message_cache: dict[str, tuple[dict, dict, float]] = {
}  # topic -> (public_data, admin_data, timestamp)
_cache_ttl = 1.0  # 1 second cache TTL


@asynccontextmanager
async def lifespan(router: APIRouter):
  """Manage WebSocket forwarder lifecycle."""
  global redis_listener_task
  log_info("Starting WebSocket forwarder...")
  redis_listener_task = create_task(handle_redis_messages())

  try:
    yield
  finally:
    log_info("Stopping WebSocket forwarder...")
    if redis_listener_task:
      redis_listener_task.cancel()
      with suppress(CancelledError):
        await redis_listener_task


router = APIRouter(lifespan=lifespan)


async def authenticate_websocket_user(websocket: WebSocket) -> Optional[User]:
  """Authenticate websocket user by leveraging the AuthService."""
  return await AuthService.load_websocket_user(websocket)


async def check_authorization_and_filter(
    user: User, topics: list[str]) -> tuple[list[str], list[str]]:
  """Combined authorization check and topic filtering - single pass optimization."""

  allowed, rejected = [], []

  for topic in topics:
    # Pattern check
    if not fnmatch.fnmatch(topic, WS_ALLOWED_TOPICS_PATTERN):
      rejected.append(topic)
      continue

    # Resource protection check for non-admin users
    if user.status in ["public", "anonymous"]:
      resource_name = topic.split(":", 1)[-1] if ":" in topic else topic
      if await is_resource_protected(resource_name):
        rejected.append(topic)
        continue

    allowed.append(topic)

  return allowed, rejected


async def consume_rate_limit(user: User, topic_count: int) -> bool:
  """Optimized rate limiting with config caching."""
  try:
    ws_config = getattr(state.server_config, 'ws_config', None)
    base_cost = ws_config.subscription_base_cost if ws_config else 10
    per_topic_cost = ws_config.subscription_per_topic_cost if ws_config else 2

    points_needed = base_cost + (per_topic_cost * topic_count)
    result = await RateLimiter.check_and_increment(user, "/ws/subscribe",
                                                   points_needed)
    return not result.get("limited", False)
  except HTTPException as e:
    return e.status_code != 429
  except Exception:
    return False


async def get_filtered_data(topic: str, data: dict, user: User) -> dict:
  """Optimized data filtering with caching."""
  current_time = now().timestamp()

  # Check cache first
  if topic in _message_cache:
    public_data, admin_data, cache_time = _message_cache[topic]
    if current_time - cache_time < _cache_ttl:
      return admin_data if user.status not in ["public", "anonymous"
                                               ] else public_data

  # Generate filtered data
  if user.status not in ["public", "anonymous"]:
    filtered_data = data  # Admin gets all data
  else:
    # Filter protected fields for public users
    filtered_data = {
        k: v
        for k, v in data.items()
        if not (k.startswith("_") or k.endswith("_protected")
                or k in ["admin", "internal", "system"])
    }

  # Update cache
  admin_data = data
  public_data = filtered_data if user.status in ["public", "anonymous"] else {
      k: v
      for k, v in data.items()
      if not (k.startswith("_") or k.endswith("_protected")
              or k in ["admin", "internal", "system"])
  }
  _message_cache[topic] = (public_data, admin_data, current_time)

  return filtered_data


async def handle_redis_messages():
  """Redis message handler with connection resilience."""
  while True:
    try:
      await state.redis.ping()
      await state.redis.pubsub.psubscribe(WS_ALLOWED_TOPICS_PATTERN)
      log_info(f"Redis subscribed to: {WS_ALLOWED_TOPICS_PATTERN}")

      async for message in state.redis.pubsub.listen():
        if message['type'] in ['message', 'pmessage']:
          topic = message['channel'].decode('utf-8')

          # Only process if we have active subscribers
          if topic in clients_by_topic and clients_by_topic[topic]:
            try:
              data = pickle.loads(message['data'])
              await broadcast_message(topic, data)
            except Exception as e:
              log_warn(f"Failed to process message for {topic}: {e}")

    except CancelledError:
      log_info("Redis handler stopped")
      with suppress(Exception):
        await state.redis.pubsub.punsubscribe(WS_ALLOWED_TOPICS_PATTERN)
      break
    except Exception as e:
      log_error(f"Redis error: {e}. Reconnecting in 5s...")
      await sleep(5)


async def broadcast_message(topic: str, data: dict):
  """Optimized message broadcasting with batch operations and correct error handling."""
  clients = clients_by_topic.get(topic)
  if not clients:
    return

  timestamp = now().isoformat()

  # Use a copy of the clients to avoid issues with modification during iteration
  client_list = list(clients)

  # Create tasks for all clients
  send_tasks = []
  for ws in client_list:
    if ws.client_state != WebSocketState.CONNECTED:
      continue

    user = client_users.get(ws)
    # It's possible the user was disconnected and cleaned up in another task
    if not user:
      continue

    # Get filtered data for this user type
    filtered_data = await get_filtered_data(topic, data, user)

    message = {
        "type": "data",
        "topic": topic.split(":", 1)[-1],  # Remove namespace for client
        "data": filtered_data,
        "timestamp": timestamp
    }

    send_tasks.append(_send_safe(ws, message))

  # Execute all sends concurrently and check for failures
  if send_tasks:
    results = await gather(*send_tasks, return_exceptions=True)

    disconnected_clients = set()
    for i, result in enumerate(results):
      # A result of False from _send_safe or an exception indicates a failed send
      if result is False or isinstance(result, Exception):
        disconnected_clients.add(client_list[i])

    # Batch cleanup disconnected clients
    if disconnected_clients:
      await gather(*[disconnect_client(ws) for ws in disconnected_clients], return_exceptions=True)


async def _send_safe(ws: WebSocket, message: dict) -> bool:
  """Safe WebSocket send with error handling."""
  try:
    await ws.send_text(orjson.dumps(message, option=ORJSON_OPTIONS).decode())
    return True
  except Exception:
    return False


async def disconnect_client(ws: WebSocket):
  """Optimized client cleanup."""
  # Batch remove from all tracking structures
  client_topics = topics_by_client.pop(ws, set())
  client_users.pop(ws, None)
  client_connect_times.pop(ws, None)

  # Update topic subscriptions
  for topic in client_topics:
    if topic in clients_by_topic:
      clients_by_topic[topic].discard(ws)
      if not clients_by_topic[topic]:
        clients_by_topic.pop(topic, None)


@crontab("*/5 * * * *")
async def force_disconnect_expired():
  """Force disconnect expired clients."""
  current_time = now().timestamp()
  expired = [
      ws for ws, connect_time in client_connect_times.items()
      if current_time - connect_time > WS_CLIENT_MAX_LIFETIME_S
  ]

  # Use gather for concurrent disconnection
  await gather(*[disconnect_and_notify(ws) for ws in expired], return_exceptions=True)


async def disconnect_and_notify(ws: WebSocket):
  """Gracefully notify and disconnect a client."""
  try:
    if ws.client_state == WebSocketState.CONNECTED:
      await _send_safe(ws, {
          "type": "disconnect",
          "code": 1001,
          "reason": "Periodic reconnect required"
      })
      await ws.close(code=1001)
  except Exception as e:
    log_warn(f"Error during forced disconnect: {e}")
  finally:
    await disconnect_client(ws)


@crontab("*/10 * * * *")
async def maintenance():
  """Periodic maintenance and cleanup."""
  # Clean stale connections
  stale = [
      ws for ws in topics_by_client
      if ws.client_state != WebSocketState.CONNECTED
  ]
  await gather(*[disconnect_client(ws) for ws in stale],
                       return_exceptions=True)

  # Enforce limits
  if len(topics_by_client) > WS_MAX_CLIENTS:
    excess = list(topics_by_client.keys())[:len(topics_by_client) -
                                           WS_MAX_CLIENTS]
    await gather(*[disconnect_client(ws) for ws in excess],
                         return_exceptions=True)

  # Clear old cache entries
  current_time = now().timestamp()
  expired_cache = [
      topic for topic, (_, _, cache_time) in _message_cache.items()
      if current_time - cache_time > _cache_ttl * 10
  ]
  for topic in expired_cache:
    _message_cache.pop(topic, None)

  # Log stats
  if topics_by_client:
    log_info(
        f"WS: {len(topics_by_client)} clients, {len(clients_by_topic)} topics")


async def handle_subscribe(ws: WebSocket, user: User, topics: list[str]):
  """Handle subscription with optimized processing."""
  if not topics:
    await _send_safe(ws, {"type": "error", "message": "No topics provided"})
    return

  # Add namespace and check authorization
  prefixed_topics = [f"{state.NS}:{t}" for t in topics]
  allowed, rejected = await check_authorization_and_filter(
      user, prefixed_topics)

  if rejected:
    await _send_safe(
        ws, {
            "type": "error",
            "message":
            f"Access denied: {[t.split(':', 1)[-1] for t in rejected]}"
        })

  if not allowed:
    return

  # Rate limiting
  if not await consume_rate_limit(user, len(allowed)):
    await _send_safe(ws, {"type": "error", "message": "Rate limit exceeded"})
    return

  # Subscribe to new topics only
  current_topics = topics_by_client.setdefault(ws, set())
  new_topics = set(allowed) - current_topics

  for topic in new_topics:
    clients_by_topic.setdefault(topic, set()).add(ws)
    current_topics.add(topic)

  await _send_safe(ws, {
      "type": "subscribed",
      "topics": [t.split(':', 1)[-1] for t in allowed]
  })


async def handle_unsubscribe(ws: WebSocket, topics: list[str]):
  """Handle unsubscription with batch processing."""
  if ws not in topics_by_client:
    return

  prefixed_topics = [f"{state.NS}:{t}" for t in topics]
  client_topics = topics_by_client[ws]

  for topic in prefixed_topics:
    if topic in client_topics:
      client_topics.remove(topic)
      if topic in clients_by_topic:
        clients_by_topic[topic].discard(ws)
        if not clients_by_topic[topic]:
          clients_by_topic.pop(topic, None)

  await _send_safe(ws, {"type": "unsubscribed", "topics": topics})


@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
  await ws.accept()

  # Authenticate user
  user = await authenticate_websocket_user(ws)
  if not user:
    await ws.close(code=1008, reason="Authentication failed")
    return

  # Track client
  client_users[ws] = user
  client_connect_times[ws] = now().timestamp()
  log_debug(f"WebSocket connected: {user.uid}")

  try:
    async for raw_message in ws.iter_text():
      try:
        message = orjson.loads(raw_message)
        action = message.get("action")

        if action == "subscribe":
          await handle_subscribe(ws, user, message.get("topics", []))
        elif action == "unsubscribe":
          await handle_unsubscribe(ws, message.get("topics", []))
        elif action == "ping":
          await _send_safe(ws, {
              "type": "pong",
              "timestamp": now().isoformat()
          })
        else:
          await _send_safe(ws, {
              "type": "error",
              "message": f"Unknown action: {action}"
          })

      except orjson.JSONDecodeError:
        await _send_safe(ws, {"type": "error", "message": "Invalid JSON"})

  except WebSocketDisconnect:
    log_debug(f"WebSocket disconnected: {user.uid}")
  except Exception as e:
    log_error(f"WebSocket error for {user.uid}: {e}")
  finally:
    await disconnect_client(ws)
