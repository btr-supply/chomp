from concurrent.futures import ThreadPoolExecutor
import yamale
from os import cpu_count, environ as env, path
from redis.asyncio import Redis, ConnectionPool
from httpx import Request, Response
from httpx import AsyncBaseTransport
from web3 import Web3 as EvmClient
from typing import Any

from .model import Config, Tsdb
from .utils import log_error, log_info, is_iterable, PackageMeta
from .adapters.sui_rpc import SuiRpcClient
from .adapters.solana_rpc import SolanaRpcClient

args: Any
thread_pool: ThreadPoolExecutor
meta = PackageMeta(package="chomp")

class ThreadPoolProxy:
  def __init__(self):
    self._thread_pool = None

  @property
  def thread_pool(self) -> ThreadPoolExecutor:
    if not self._thread_pool:
      self._thread_pool = ThreadPoolExecutor(max_workers=cpu_count() if args.threaded else 2)
    return self._thread_pool

  def __getattr__(self, name):
    return getattr(self.thread_pool, name)

class NoCLTransport(AsyncBaseTransport):
  async def handle_async_request(self, request: Request) -> Response:
    # rm Content-Length header if present
    for h in ["content-length", "Content-Length", "Content-length"]:
      if h in request.headers:
        del request.headers[h]
    request = Request(
        method=request.method,
      url=request.url,
      headers=request.headers,
      content=request.content,
      stream=request.stream,
    )
    return await super().handle_async_request(request)

class Web3Proxy:

  def __init__(self):
    self._by_chain = {}
    self._next_index_by_chain = {}
    self._rpcs_by_chain = {}

  def rpcs(self, chain_id: str | int, load_all=False) -> dict[str | int, list[str]]:
    if load_all and not self._rpcs_by_chain:
      self._rpcs_by_chain.update({k[:-10]: v.split(",") for k, v in env.items() if k.startswith("HTTP_RPCS")})

    if chain_id not in self._rpcs_by_chain:
      env_key = f"HTTP_RPCS_{chain_id}".upper()
      rpc_env = env.get(env_key)
      if not rpc_env:
        raise ValueError(f"Missing RPC endpoints for chain {chain_id} ({env_key} environment variable not found)")
      self._rpcs_by_chain[chain_id] = rpc_env.split(",")
    return self._rpcs_by_chain[chain_id]

  async def client(self, chain_id: str | int, roll=True) -> EvmClient | SolanaRpcClient | SuiRpcClient:
    is_evm = isinstance(chain_id, int)

    async def connect(rpc_url: str) -> EvmClient | SolanaRpcClient | SuiRpcClient | None:
      try:
        if is_evm:
          c = EvmClient(EvmClient.HTTPProvider(rpc_url))  # type: ignore
        elif chain_id == "sui":
          c = SuiRpcClient(rpc_url)  # type: ignore
        elif chain_id == "solana":
          c = SolanaRpcClient(rpc_url)  # type: ignore
        else:
          raise ValueError(f"Unsupported chain: {chain_id}")
        connected = await is_connected(c)
        if connected:
          log_info(f"Connected to chain {chain_id} using rpc {rpc_url}")
          return c
        else:
          raise Exception("Connection failure")
      except Exception as e:
        log_error(f"Could not connect to chain {chain_id} using rpc {rpc_url}: {e}")
        return None

    async def is_connected(c: EvmClient | SolanaRpcClient | SuiRpcClient) -> bool:
      try:
        if is_evm:
          result = c.is_connected()  # type: ignore
          if hasattr(result, '__await__'):
            return await result  # type: ignore
          return bool(result)
        else: # any of non evm rpc clients
          return await c.is_connected()  # type: ignore
      except Exception:
        return False

    # Initialize chain clients list if not exists
    if chain_id not in self._by_chain:
      self._by_chain[chain_id] = []
      self._next_index_by_chain[chain_id] = 0
      index = 0
    else:
      index = self._next_index_by_chain[chain_id]
      if roll:
        index = (index + 1) % len(self.rpcs(chain_id))
        self._next_index_by_chain[chain_id] = index

    clients = self._by_chain[chain_id]

    if index < len(clients):
      c = clients[index]
      if await is_connected(c):
        return c

    # Need to connect to a new RPC or roll to next one
    rpcs = self.rpcs(chain_id)
    if not rpcs:
      raise ValueError(f"Missing RPC endpoints for chain {chain_id}")

    # Try each RPC until we get a valid connection
    max_attempts = len(rpcs)
    attempts = 0

    while attempts < max_attempts:
      current_index = self._next_index_by_chain[chain_id]
      rpc_url = f"https://{rpcs[current_index]}"

      c = await connect(rpc_url)
      if c is not None:
        # Store the successful connection
        if current_index >= len(clients):
          self._by_chain[chain_id].append(c)
        else:
          self._by_chain[chain_id][current_index] = c
        return c

      # Try next RPC
      self._next_index_by_chain[chain_id] = (current_index + 1) % len(rpcs)
      attempts += 1

    raise Exception(f"Failed to connect to any RPC for chain {chain_id} after trying {max_attempts} endpoints")

class TsdbProxy:
  def __init__(self):
    self._tsdb = None

  @property
  def tsdb(self) -> Tsdb:
    if not self._tsdb:
      raise ValueError("TSDB_ADAPTER Adapter found")
    # get_loop().run_until_complete(self._tsdb.ensure_connected())
    return self._tsdb

  def set_adapter(self, db: Tsdb):
    self._tsdb = db

  def __getattr__(self, name):
    return getattr(self.tsdb, name)

class RedisProxy:
  def __init__(self):
    self._pool = None
    self._redis = None
    self._pubsub = None

  @property
  def redis(self) -> Redis:
    if not self._redis:
      if not self._pool:
        self._pool = ConnectionPool(
          host=env.get("REDIS_HOST", "localhost"),
          port=int(env.get("REDIS_PORT", 6379)),
          username=env.get("DB_RW_USER", "rw"),
          password=env.get("DB_RW_PASS", "pass"),
          db=int(env.get("REDIS_DB", 0)),
          max_connections=int(env.get("REDIS_MAX_CONNECTIONS", 2 ** 16)),
        )
      self._redis = Redis(connection_pool=self._pool)
    return self._redis

  @property
  def pubsub(self):
    if not self._pubsub:
      self._pubsub = self.redis.pubsub()
    return self._pubsub

  async def close(self):
    if self._pubsub:
      await self._pubsub.close()
      self._pubsub = None
    if self._redis:
      await self._redis.close()
      self._redis = None
    if self._pool:
      await self._pool.disconnect()
      self._pool = None

  def __getattr__(self, name):
    return getattr(self.redis, name)

class ConfigProxy:
  def __init__(self, _args):
    global args
    args = _args
    self._config = None

  @staticmethod
  def load_config(config_path: str) -> Config:

    schema_path = meta.root / 'src' / 'config-schema.yml'
    schema = yamale.make_schema(str(schema_path))

    abs_path = config_path

    def is_config_file_path(value: str) -> bool:
      return isinstance(value, str) and value.lower().endswith(('.yml', '.yaml', '.json'))

    def load_and_merge_yaml(yaml_path: str) -> dict:
      config_data = yamale.make_data(yaml_path)[0]
      config_data = config_data[0] if is_iterable(config_data) else config_data
      parent_dir = path.dirname(yaml_path)

      def resolve_nested_config(value):
        if is_config_file_path(value):
          nested_path = path.join(parent_dir, value)
          return load_and_merge_yaml(nested_path)
        elif isinstance(value, list):
          processed_list = [resolve_nested_config(item) for item in value]
          processed_list = [item for item in processed_list if item is not None]
          return processed_list if processed_list else None
        elif isinstance(value, dict):
          processed_dict = {k: resolve_nested_config(v) for k, v in value.items()}
          processed_dict = {k: v for k, v in processed_dict.items() if v is not None}
          return processed_dict if processed_dict else None
        else:
          return None

      # process all top-level keys
      if isinstance(config_data, dict):
        config_data.pop('vars', None) # remove the vars if any section used for genericity
        for key, value in config_data.items():
          nested_data = resolve_nested_config(value)
          if nested_data is None:
            config_data[key] = value
          else:
            if not is_iterable(nested_data):
              nested_data = [nested_data]
            for d in nested_data:
              el = d[key] if key in d else d
              if key in config_data and is_iterable(el) and is_iterable(config_data[key]):
                # pop the last item if any since it is the subconfig file path
                config_data[key] = [*el, *config_data[key][:-1]]
              else:
                config_data[key] = el
      return config_data

    config_data = load_and_merge_yaml(abs_path)

    try:
      # validate the merged config
      yamale.validate(schema, [(config_data, abs_path)])
    except yamale.YamaleError as e:
      msg = ""
      for result in e.results:
        msg += f"Error validating {result.data} with schema {result.schema}\n"
        for error in result.errors:
          msg += f" - {error}\n"
      log_error(msg)
      exit(1)

    return Config.from_dict(config_data)

  @property
  def config(self) -> Config:
    if not self._config:
      self._config = self.load_config(path.abspath(path.join(env.get('WORKDIR', ''), args.config_path)))
    return self._config

  def __getattr__(self, name):
    return getattr(self.config, name)
