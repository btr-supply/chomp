from asyncio import Task, create_task, CancelledError
from typing import Any, Dict, List, Optional, Set, Callable
import asyncio
from dataclasses import dataclass
from collections import deque

from ..models.ingesters import Ingester
from ..utils import log_error, log_debug, safe_eval
from ..actions import scheduler
from .. import state
from .resp3_getter import RESP3Connection


@dataclass
class RESP3Subscriber:
  """RESP3 subscription management for Redis-compatible databases"""
  connection: RESP3Connection
  subscriptions: Set[str]
  pattern_subscriptions: Set[str]
  message_handler: Optional[Callable] = None
  running: bool = False

  async def subscribe(self, channels: List[str]):
    """Subscribe to channels"""
    if not self.connection.writer:
      raise ConnectionError("Not connected")

    await self.connection._send_command(['SUBSCRIBE'] + channels)
    for channel in channels:
      response = await self.connection._read_response()
      if response[0] == 'subscribe':
        self.subscriptions.add(channel)
        log_debug(f"Subscribed to channel: {channel}")

  async def psubscribe(self, patterns: List[str]):
    """Subscribe to channel patterns"""
    if not self.connection.writer:
      raise ConnectionError("Not connected")

    await self.connection._send_command(['PSUBSCRIBE'] + patterns)
    for pattern in patterns:
      response = await self.connection._read_response()
      if response[0] == 'psubscribe':
        self.pattern_subscriptions.add(pattern)
        log_debug(f"Subscribed to pattern: {pattern}")

  async def unsubscribe(self, channels: List[str] = None):
    """Unsubscribe from channels"""
    if not self.connection.writer:
      return

    if channels:
      await self.connection._send_command(['UNSUBSCRIBE'] + channels)
      for channel in channels:
        self.subscriptions.discard(channel)
    else:
      await self.connection._send_command(['UNSUBSCRIBE'])
      self.subscriptions.clear()

  async def punsubscribe(self, patterns: List[str] = None):
    """Unsubscribe from channel patterns"""
    if not self.connection.writer:
      return

    if patterns:
      await self.connection._send_command(['PUNSUBSCRIBE'] + patterns)
      for pattern in patterns:
        self.pattern_subscriptions.discard(pattern)
    else:
      await self.connection._send_command(['PUNSUBSCRIBE'])
      self.pattern_subscriptions.clear()

  async def listen(self):
    """Listen for messages"""
    if not self.connection.reader:
      raise ConnectionError("Not connected")

    self.running = True

    try:
      while self.running:
        try:
          message = await self.connection._read_response()

          if isinstance(message, list) and len(message) >= 3:
            msg_type = message[0]

            if msg_type == 'message':
              # Regular channel message
              channel, data = message[1], message[2]
              if self.message_handler:
                await self.message_handler(channel, data, None)

            elif msg_type == 'pmessage':
              # Pattern channel message
              pattern, channel, data = message[1], message[2], message[3]
              if self.message_handler:
                await self.message_handler(channel, data, pattern)

            elif msg_type in ['subscribe', 'psubscribe', 'unsubscribe', 'punsubscribe']:
              # Subscription confirmation messages
              log_debug(f"Subscription event: {msg_type} - {message[1]}")

        except Exception as e:
          if self.running:
            log_error(f"Error in subscription listener: {e}")
          break

    except CancelledError:
      log_debug("Subscription listener cancelled")
    finally:
      self.running = False


async def schedule(ing: Ingester) -> list[Task]:
  """Schedule RESP3 subscriber ingester"""

  # State management for subscription data
  message_data: Dict[str, deque] = {}
  subscribers: Dict[str, RESP3Subscriber] = {}
  subscriber_tasks: List[Task] = []

  async def message_handler(channel: str, data: Any, pattern: Optional[str] = None):
    """Handle incoming messages from subscriptions"""
    # Store message data for processing
    if channel not in message_data:
      message_data[channel] = deque(maxlen=1000)  # Limit message history

    message_data[channel].append({
      'channel': channel,
      'data': data,
      'pattern': pattern,
      'timestamp': asyncio.get_event_loop().time()
    })

    if state.args.verbose:
      log_debug(f"Received message on {channel}: {data}")

  async def setup_subscriptions():
    """Setup subscriptions based on field targets"""
    connection_configs: Dict[str, Dict] = {}

    for field in ing.fields:
      if not field.target:
        continue

      # Parse target: redis://host:port/db/channel or channel for default
      if '://' in field.target:
        parts = field.target.split('/')
        conn_str = '/'.join(parts[:-1])
        channel = parts[-1]

        if conn_str not in connection_configs:
          # Parse connection string
          url_parts = conn_str.replace('redis://', '').split('/')
          host_part = url_parts[0]
          db = int(url_parts[1]) if len(url_parts) > 1 else 0

          if '@' in host_part:
            auth_part, host_port = host_part.split('@')
            username, password = auth_part.split(':') if ':' in auth_part else (auth_part, '')
          else:
            username, password = None, None
            host_port = host_part

          if ':' in host_port:
            host, port = host_port.split(':')
            port = int(port)
          else:
            host, port = host_port, 6379

          connection_configs[conn_str] = {
            'host': host,
            'port': port,
            'username': username,
            'password': password,
            'db': db,
            'channels': [],
            'patterns': []
          }

        # Determine if it's a pattern subscription
        if '*' in channel or '?' in channel or '[' in channel:
          connection_configs[conn_str]['patterns'].append(channel)
        else:
          connection_configs[conn_str]['channels'].append(channel)

      else:
        # Use default Redis connection
        default_conn = 'default'
        if default_conn not in connection_configs:
          connection_configs[default_conn] = {
            'host': state.redis.redis.connection_pool.connection_kwargs.get('host', 'localhost'),
            'port': state.redis.redis.connection_pool.connection_kwargs.get('port', 6379),
            'username': state.redis.redis.connection_pool.connection_kwargs.get('username'),
            'password': state.redis.redis.connection_pool.connection_kwargs.get('password'),
            'db': state.redis.redis.connection_pool.connection_kwargs.get('db', 0),
            'channels': [],
            'patterns': []
          }

        # Determine if it's a pattern subscription
        if '*' in field.target or '?' in field.target or '[' in field.target:
          connection_configs[default_conn]['patterns'].append(field.target)
        else:
          connection_configs[default_conn]['channels'].append(field.target)

    # Setup subscribers for each connection
    for conn_str, config in connection_configs.items():
      connection = RESP3Connection(
        host=config['host'],
        port=config['port'],
        username=config['username'],
        password=config['password'],
        db=config['db']
      )

      if not await connection.connect():
        log_error(f"Failed to connect to {conn_str}")
        continue

      subscriber = RESP3Subscriber(
        connection=connection,
        subscriptions=set(),
        pattern_subscriptions=set(),
        message_handler=message_handler
      )

      # Subscribe to channels and patterns
      if config['channels']:
        await subscriber.subscribe(config['channels'])
      if config['patterns']:
        await subscriber.psubscribe(config['patterns'])

      subscribers[conn_str] = subscriber

      # Start listening task
      task = create_task(subscriber.listen())
      subscriber_tasks.append(task)

  async def ingest(ing: Ingester):
    await ing.pre_ingest()

    # Process messages for each field
    for field in ing.fields:
      if not field.target:
        continue

      # Extract channel name from target
      if '://' in field.target:
        channel = field.target.split('/')[-1]
      else:
        channel = field.target

      # Check for messages in this channel
      if channel in message_data and message_data[channel]:
        messages = list(message_data[channel])

        # Apply field handler if specified
        if field.handler:
          try:
            if isinstance(field.handler, str):
              handler_func = safe_eval(field.handler, callable_check=True)
            else:
              handler_func = field.handler

            processed_value = handler_func(messages)
            field.value = processed_value
          except Exception as e:
            log_error(f"Error in field handler for {field.name}: {e}")
            field.value = messages
        else:
          # Default: use the latest message
          field.value = messages[-1] if messages else None

        # Apply reducer if specified
        if field.reducer:
          try:
            if isinstance(field.reducer, str):
              reducer_func = safe_eval(field.reducer, callable_check=True)
            else:
              reducer_func = field.reducer

            # Pass message history as epochs for reducer
            epochs = deque([{'messages': messages}])
            field.value = reducer_func(epochs)
          except Exception as e:
            log_error(f"Error in field reducer for {field.name}: {e}")

    await ing.post_ingest(response_data=message_data)

  # Setup subscriptions
  await setup_subscriptions()

  # Schedule the ingester
  task = await scheduler.add_ingester(ing, fn=ingest, start=False)

  # Return main task plus subscriber tasks
  tasks = [task] if task is not None else []
  tasks.extend(subscriber_tasks)

  return tasks
