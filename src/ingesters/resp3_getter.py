from asyncio import Task
from typing import Any, Dict, List, Optional
import asyncio
from dataclasses import dataclass

from ..models.ingesters import Ingester
from ..models.base import ResourceField
from ..utils import log_error, log_warn
from ..actions import scheduler
from .. import state


@dataclass
class RESP3Connection:
  """RESP3 connection management for Redis-compatible databases"""
  host: str
  port: int
  username: Optional[str] = None
  password: Optional[str] = None
  db: int = 0
  reader: Optional[asyncio.StreamReader] = None
  writer: Optional[asyncio.StreamWriter] = None

  async def connect(self) -> bool:
    """Establish connection to Redis-compatible server"""
    try:
      self.reader, self.writer = await asyncio.open_connection(
          self.host, self.port
      )

      # Send HELLO command to negotiate RESP3
      await self._send_command(['HELLO', '3'])
      response = await self._read_response()

      if not response or response.get('version') != 3:
        log_warn(f"RESP3 not supported by server {self.host}:{self.port}")
        return False

      # Authenticate if credentials provided
      if self.username and self.password:
        await self._send_command(['AUTH', self.username, self.password])
        auth_response = await self._read_response()
        if auth_response != 'OK':
          log_error(f"Authentication failed for {self.host}:{self.port}")
          return False

      # Select database
      if self.db != 0:
        await self._send_command(['SELECT', str(self.db)])
        select_response = await self._read_response()
        if select_response != 'OK':
          log_error(f"Database selection failed for {self.host}:{self.port}")
          return False

      return True

    except Exception as e:
      log_error(f"Connection failed to {self.host}:{self.port}: {e}")
      return False

  async def disconnect(self):
    """Close connection"""
    if self.writer:
      self.writer.close()
      await self.writer.wait_closed()
      self.writer = None
    self.reader = None

  async def _send_command(self, command: List[str]):
    """Send RESP3 command array"""
    if not self.writer:
      raise ConnectionError("Not connected")

    # Format as RESP3 array
    msg = f"*{len(command)}\r\n"
    for arg in command:
      msg += f"${len(str(arg))}\r\n{arg}\r\n"

    self.writer.write(msg.encode())
    await self.writer.drain()

  async def _read_response(self) -> Any:
    """Read RESP3 response"""
    if not self.reader:
      raise ConnectionError("Not connected")

    line = await self.reader.readline()
    if not line:
      raise ConnectionError("Connection closed")

    line = line.decode().strip()

    if line.startswith('+'):  # Simple string
      return line[1:]
    elif line.startswith('-'):  # Error
      raise Exception(f"Redis error: {line[1:]}")
    elif line.startswith(':'):  # Integer
      return int(line[1:])
    elif line.startswith('$'):  # Bulk string
      length = int(line[1:])
      if length == -1:
        return None
      data = await self.reader.read(length + 2)  # +2 for \r\n
      return data[:-2].decode()
    elif line.startswith('*'):  # Array
      count = int(line[1:])
      if count == -1:
        return None
      items = []
      for _ in range(count):
        items.append(await self._read_response())
      return items
    elif line.startswith('%'):  # Map (RESP3)
      count = int(line[1:])
      result = {}
      for _ in range(count):
        key = await self._read_response()
        value = await self._read_response()
        result[key] = value
      return result
    else:
      log_warn(f"Unknown RESP3 type: {line}")
      return line

  async def get(self, key: str) -> Optional[Any]:
    """GET command"""
    await self._send_command(['GET', key])
    return await self._read_response()

  async def mget(self, keys: List[str]) -> List[Optional[Any]]:
    """MGET command"""
    await self._send_command(['MGET'] + keys)
    return await self._read_response()


async def schedule(ing: Ingester) -> list[Task]:
  """Schedule RESP3 getter ingester"""

  # Parse connection info from first field's target
  connections: Dict[str, RESP3Connection] = {}

  async def ingest(ing: Ingester):
    await ing.pre_ingest()

    # Group keys by connection
    keys_by_connection: Dict[str, List[str]] = {}
    field_by_key: Dict[str, ResourceField] = {}

    for field in ing.fields:
      if not field.target:
        continue

      # Parse target: redis://host:port/db/key or key for default connection
      if '://' in field.target:
        parts = field.target.split('/')
        conn_str = '/'.join(parts[:-1])
        key = parts[-1]

        if conn_str not in connections:
          # Parse connection string
          # Format: redis://[username:password@]host:port/db
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

          connections[conn_str] = RESP3Connection(
              host=host,
              port=port,
              username=username,
              password=password,
              db=db
          )

        keys_by_connection.setdefault(conn_str, []).append(key)
        field_by_key[key] = field
      else:
        # Use default Redis connection
        default_conn = 'default'
        if default_conn not in connections:
          connections[default_conn] = RESP3Connection(
              host=state.redis.redis.connection_pool.connection_kwargs.get('host', 'localhost'),
              port=state.redis.redis.connection_pool.connection_kwargs.get('port', 6379),
              username=state.redis.redis.connection_pool.connection_kwargs.get('username'),
              password=state.redis.redis.connection_pool.connection_kwargs.get('password'),
              db=state.redis.redis.connection_pool.connection_kwargs.get('db', 0)
          )

        keys_by_connection.setdefault(default_conn, []).append(field.target)
        field_by_key[field.target] = field

    # Connect to all required connections
    for conn_str, conn in connections.items():
      if not await conn.connect():
        log_error(f"Failed to connect to {conn_str}")
        continue

    # Fetch data from each connection
    try:
      for conn_str, keys in keys_by_connection.items():
        if conn_str not in connections:
          continue

        conn = connections[conn_str]

        try:
          if len(keys) == 1:
            # Single GET
            value = await conn.get(keys[0])
            field = field_by_key[keys[0]]
            field.value = value
          else:
            # Batch MGET
            values = await conn.mget(keys)
            for key, value in zip(keys, values):
              field = field_by_key[key]
              field.value = value

        except Exception as e:
          log_error(f"Error fetching from {conn_str}: {e}")

    finally:
      # Close all connections
      for conn in connections.values():
        await conn.disconnect()

    await ing.post_ingest()

  task = await scheduler.add_ingester(ing, fn=ingest, start=False)
  return [task] if task is not None else []
