import orjson
from fastapi import APIRouter
from fastapi.websockets import WebSocketState, WebSocket, WebSocketDisconnect
from fastapi.concurrency import asynccontextmanager
from typing import Literal
import os
import fnmatch
import pickle

from ...utils import log_debug, log_error, log_info
from ... import state
from ..responses import ORJSON_OPTIONS

WsAction = Literal["subscribe", "unsubscribe", "ping", "keepalive"]

# Redis connection and pub/sub setup
redis_client = None

# Data structures for client management
clients_by_topic: dict[str, set[WebSocket]] = {}
topics_by_client: dict[WebSocket, set[str]] = {}
forwarded_topics: set[str] = set()  # Track globally subscribed topics

# Add near the top with other globals
ALLOWED_TOPICS_PATTERN = os.getenv("WS_ALLOWED_TOPICS", "chomp:*") # Default to all ingesters topics

@asynccontextmanager
async def lifespan(router: APIRouter):
  # on startup
  await state.start_redis_listener(ALLOWED_TOPICS_PATTERN)
  
  try:
    yield
  finally:
    # on shutdown
    await state.stop_redis_listener()

router = APIRouter(lifespan=lifespan)

async def handle_redis_messages():
  try:
    # Initialize pubsub connection
    await state.redis.pubsub.ping()

    # Subscribe to the wildcard pattern
    await state.redis.pubsub.psubscribe(ALLOWED_TOPICS_PATTERN)
    log_info(f"Subscribed to topics matching pattern: {ALLOWED_TOPICS_PATTERN}")

    # Use pubsub.listen() instead of get_message() to avoid concurrent read issues
    async for message in state.redis.pubsub.listen():
      if message['type'] in ['message', 'pmessage']:  # Handle both message and pmessage types
        try:
          topic = message['channel'].decode('utf-8')
          data = pickle.loads(message['data'])
          if topic in forwarded_topics:
            await broadcast_message(topic, data)
        except Exception as e:
          log_error(f"Error processing message: {e}")
          continue

  except Exception as e:
    log_error(f"Fatal error in Redis message handler: {e}")
    # Let the error propagate so the task can be properly cleaned up
    raise

async def broadcast_message(topic, msg):
  for ws in clients_by_topic.get(topic, []):
    if ws.client_state == WebSocketState.DISCONNECTED:
      await disconnect_client(ws)
    try:
      if isinstance(msg, (str, bytes)):
        await ws.send_text(msg)
      else:
        await ws.send_text(orjson.dumps(msg, option=ORJSON_OPTIONS).decode('utf-8'))
    except Exception as e:
      log_error(f"Error sending message to client: {e}")

async def disconnect_client(ws: WebSocket):
  try:
    await ws.close()
  except RuntimeError:
    # Connection might already be closed
    pass
  
  # Unsubscribe from topics that have no more clients
  for topic in topics_by_client[ws]:
    clients = clients_by_topic[topic]
    clients.remove(ws)
    if not clients:  # No more clients for this topic
      await state.redis.pubsub.unsubscribe(topic)
      forwarded_topics.remove(topic)
      del clients_by_topic[topic]
      if state.args.verbose:
        log_debug(f"Unsubscribed from topic with no clients: {topic}")

  del topics_by_client[ws]
  if state.args.verbose:
    log_debug(f"Disconnected client {ws}")

@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
  await ws.accept()
  topics_by_client[ws] = set()

  try:
    while True:
      msg = orjson.loads(await ws.receive_text())
      act: WsAction = msg.get("action")

      match act:
        case "subscribe":
          topics = msg["topics"]
          accepted_topics = []
          rejected_topics = []
          
          for topic in topics:
            # Check if topic matches allowed pattern
            if not fnmatch.fnmatch(topic, ALLOWED_TOPICS_PATTERN):
              rejected_topics.append(topic)
              continue
            
            # Check if topic exists using the new function
            # TODO: implement a better resource status check since the first subscription will always be rejected
            # if not await topic_exist(topic):
            #   rejected_topics.append(topic)
            #   continue

            # Add to tracking sets
            clients = clients_by_topic.setdefault(topic, set())
            clients.add(ws)
            topics_by_client[ws].add(topic)
            
            # Only subscribe if not already subscribed
            if topic not in forwarded_topics:
              await state.redis.pubsub.subscribe(topic)
              forwarded_topics.add(topic)
              if state.args.verbose:
                log_debug(f"New Redis subscription: {topic}")
            
            accepted_topics.append(topic)
          
          # Send subscription response
          if rejected_topics:
            await ws.send_text(orjson.dumps({
              "error": f"Some topics were rejected: {rejected_topics}",
              "subscribed": accepted_topics
            }, option=ORJSON_OPTIONS).decode())
          else:
            await ws.send_text(orjson.dumps({
              "success": True,
              "subscribed": accepted_topics
            }, option=ORJSON_OPTIONS).decode())
            
        case "unsubscribe":
          topics = msg["topics"]
          for topic in topics:
            topics_by_client[ws].discard(topic)
            clients = clients_by_topic.get(topic, set())
            clients.discard(ws)
            if not clients:  # No more clients for this topic
              await state.redis.pubsub.unsubscribe(topic)
              forwarded_topics.remove(topic)
              del clients_by_topic[topic]
        case _:
          # TODO: return 404 and stop hadshake
          log_error(f"Invalid ws action: {msg}, skipping...")

  except WebSocketDisconnect:
    await disconnect_client(ws)
