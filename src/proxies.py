from concurrent.futures import ThreadPoolExecutor
import yamale
from os import environ as env, path
from redis.asyncio import Redis, ConnectionPool
from httpx import Request, Response
from httpx import AsyncBaseTransport
import web3
from typing import Any, Optional, Callable

from .utils import log_error, log_info, log_warn, is_iterable, PackageMeta
from .adapters.sui_rpc import SuiRpcClient
from .adapters.svm_rpc import SvmRpcClient

args: Any
thread_pool: ThreadPoolExecutor
meta = PackageMeta(package="chomp")


class ThreadPoolProxy:
  """
  Simplified ThreadPoolExecutor proxy for legacy compatibility
  """

  def __init__(self, max_workers: Optional[int] = None):
    self._thread_pool: Optional[ThreadPoolExecutor] = None
    self._max_workers = max_workers

  @property
  def thread_pool(self) -> ThreadPoolExecutor:
    if self._thread_pool is None:
      max_workers = self._max_workers or int(env.get("THREAD_POOL_SIZE", "4"))
      self._thread_pool = ThreadPoolExecutor(max_workers=max_workers,
                                             thread_name_prefix="chomp-worker")
      log_info(f"Initialized thread pool with {max_workers} workers")
    return self._thread_pool

  def submit(self, fn: Callable, *args, **kwargs):
    return self.thread_pool.submit(fn, *args, **kwargs)

  def map(self,
          func: Callable,
          *iterables,
          timeout: Optional[float] = None,
          chunksize: int = 1):
    return self.thread_pool.map(func,
                                *iterables,
                                timeout=timeout,
                                chunksize=chunksize)

  def shutdown(self, wait: bool = True, cancel_futures: bool = False):
    if self._thread_pool:
      return self.thread_pool.shutdown(wait=wait,
                                       cancel_futures=cancel_futures)


class NoCLTransport(AsyncBaseTransport):

  async def handle_async_request(self, request: Request) -> Response:
    # rm Content-Length header if present
    if "Content-Length" in request.headers:
      del request.headers["Content-Length"]
    return await super().handle_async_request(request)


class Web3Proxy:

  def __init__(self, rotate_always=True):
    self._next_index_by_chain = {}
    self._rpcs_by_chain = {}
    self.rotate_always = rotate_always

  def rpcs(self, chain_id: str | int, load_all=False) -> list[str]:
    if load_all and not self._rpcs_by_chain:
      self._rpcs_by_chain.update({
          k[:-10]: v.split(",")
          for k, v in env.items() if k.startswith("HTTP_RPCS")
      })

    if chain_id not in self._rpcs_by_chain:
      env_key = f"HTTP_RPCS_{chain_id}".upper()
      rpc_env = env.get(env_key)
      if not rpc_env:
        raise ValueError(f"Missing RPC endpoints for chain {chain_id}")
      self._rpcs_by_chain[chain_id] = rpc_env.split(",")
    return self._rpcs_by_chain[chain_id]

  def _rotate_to_next_rpc(self, chain_id: str | int) -> None:
    """Rotate to the next RPC in the list"""
    rpcs = self.rpcs(chain_id)
    if len(rpcs) > 1:
      current_index = self._next_index_by_chain.get(chain_id, 0)
      self._next_index_by_chain[chain_id] = (current_index + 1) % len(rpcs)

  async def client(self, chain_id: str | int, roll=True) -> Any:
    # Initialize or rotate RPC index
    if chain_id not in self._next_index_by_chain:
      self._next_index_by_chain[chain_id] = 0
    elif roll and self.rotate_always:
      self._rotate_to_next_rpc(chain_id)

    rpcs = self.rpcs(chain_id)
    is_evm = isinstance(chain_id, int)

    # Try each RPC until one works
    for _ in range(len(rpcs)):
      current_index = self._next_index_by_chain[chain_id]
      rpc_url = f"https://{rpcs[current_index]}"

      try:
        from . import state

        # Create client based on chain type
        if is_evm:
          client = web3.Web3(web3.Web3.HTTPProvider(rpc_url))
          if not client.is_connected():
            raise Exception("Connection failed")
        elif chain_id == "sui":
          client = SuiRpcClient(rpc_url,
                                timeout=float(state.args.ingestion_timeout))
          if not await client.is_connected():
            raise Exception("Connection failed")
        elif chain_id == "solana":
          client = SvmRpcClient(rpc_url,
                                timeout=float(state.args.ingestion_timeout))
          if not await client.is_connected():
            raise Exception("Connection failed")
        else:
          raise ValueError(f"Unsupported chain: {chain_id}")

        log_info(f"Connected to chain {chain_id} using RPC {rpc_url}")
        return client

      except Exception as e:
        log_warn(f"RPC {rpc_url} failed for chain {chain_id}: {e}")
        self._next_index_by_chain[chain_id] = (current_index + 1) % len(rpcs)

    raise Exception(f"All RPCs failed for chain {chain_id}")


class TsdbProxy:

  def __init__(self):
    self._tsdb = None

  @property
  def tsdb(self):
    if not self._tsdb:
      raise ValueError("TSDB_ADAPTER Adapter found")
    # get_loop().run_until_complete(self._tsdb.ensure_connected())
    return self._tsdb

  def set_adapter(self, db):
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
            max_connections=int(env.get("REDIS_MAX_CONNECTIONS", 2**16)),
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
      try:
        await self._pubsub.close()
      except Exception:
        pass  # Ignore close errors
      self._pubsub = None
    if self._redis:
      try:
        await self._redis.close()
      except Exception:
        pass  # Ignore close errors
      self._redis = None
    if self._pool:
      try:
        await self._pool.disconnect()
      except Exception:
        pass  # Ignore close errors
      self._pool = None

  async def ping(self) -> bool:
    """Ping the Redis server and return True if successful, False otherwise."""
    try:
      await self.redis.ping()
      return True
    except Exception:
      return False

  def __getattr__(self, name):
    return getattr(self.redis, name)


class IngesterConfigProxy:

  def __init__(self, _args):
    global args
    args = _args
    self._config = None
    self._ingester_configs = _args.ingester_configs  # Store the value to avoid proxy recursion
    self._instance = None  # Will be set by state.init()

  @staticmethod
  def load_config(INGESTER_CONFIGS: str, instance: Optional[Any] = None):

    schema_path = meta.root / 'src' / 'ingester-config-schema.yml'
    schema = yamale.make_schema(str(schema_path))

    # Handle comma-delimited list of config files
    config_files = [
        path.strip() for path in INGESTER_CONFIGS.split(',') if path.strip()
    ]

    def is_config_file_path(value: str) -> bool:
      return isinstance(value, str) and value.lower().endswith(
          ('.yml', '.yaml', '.json'))

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
          processed_list = [
              item for item in processed_list if item is not None
          ]
          return processed_list if processed_list else None
        elif isinstance(value, dict):
          processed_dict = {
              k: resolve_nested_config(v)
              for k, v in value.items()
          }
          processed_dict = {
              k: v
              for k, v in processed_dict.items() if v is not None
          }
          return processed_dict if processed_dict else None
        else:
          return None

      # process all top-level keys
      if isinstance(config_data, dict):
        config_data.pop(
            'vars', None)  # remove the vars if any section used for genericity
        for key, value in config_data.items():
          nested_data = resolve_nested_config(value)
          if nested_data is None:
            config_data[key] = value
          else:
            if not is_iterable(nested_data):
              nested_data = [nested_data]
            for d in nested_data:
              el = d[key] if key in d else d
              if key in config_data and is_iterable(el) and is_iterable(
                  config_data[key]):
                # pop the last item if any since it is the subconfig file path
                config_data[key] = [*el, *config_data[key][:-1]]
              else:
                config_data[key] = el
      return config_data

    # Process each config file and merge all configurations
    merged_config_data: dict[str, Any] = {}

    for config_file in config_files:
      workdir = env.get('WORKDIR', '')
      abs_path = path.abspath(path.join(workdir, config_file))
      if not path.exists(abs_path):
        log_error(f"Config file not found: {abs_path}")
        continue

      config_data = load_and_merge_yaml(abs_path)

      # Merge with existing config data
      for key, value in config_data.items():
        if key in merged_config_data:
          if is_iterable(value) and is_iterable(merged_config_data[key]):
            merged_config_data[key].extend(value)
          else:
            merged_config_data[key] = value
        else:
          merged_config_data[key] = value

    try:
      # validate the merged config
      yamale.validate(schema, [(merged_config_data, INGESTER_CONFIGS)])
    except yamale.YamaleError as e:
      msg = ""
      for result in e.results:
        msg += f"Error validating {result.data} with schema {result.schema}\n"
        for error in result.errors:
          msg += f" - {error}\n"
      log_error(msg)
      exit(1)

    from .models.configs import IngesterConfig
    return IngesterConfig.from_dict(merged_config_data, instance)

  @property
  def config(self):
    if not self._config:
      workdir = env.get('WORKDIR', '')
      self._config = self.load_config(
          path.abspath(path.join(workdir, self._ingester_configs)),
          self._instance)
    return self._config

  def __getattr__(self, name):
    # Delegate to the loaded config object, but avoid recursion
    if not hasattr(self, '_config') or self._config is None:
      # Load config without going through __getattr__ again
      workdir = env.get('WORKDIR', '')
      self._config = self.load_config(
          path.abspath(path.join(workdir, self._ingester_configs)),
          self._instance)
    return getattr(self._config, name)


class ServerConfigProxy:

  def __init__(self, _args):
    global args
    args = _args
    self._config = None
    # Use server_config argument or environment variable or default
    self._server_config_file = getattr(
        _args, 'server_config', env.get('SERVER_CONFIG',
                                        './server-config.yml'))

  @staticmethod
  def load_config(config_file_path: str):
    """Load and validate admin configuration"""
    schema_path = meta.root / 'src' / 'server-config-schema.yml'
    schema = yamale.make_schema(str(schema_path))

    workdir = env.get('WORKDIR', '')
    abs_path = path.abspath(path.join(workdir, config_file_path))

    # Create default config if file doesn't exist
    if not path.exists(abs_path):
      log_warn(f"Server config file not found: {abs_path}, using defaults")
      from .models.configs import ServerConfig
      return ServerConfig()

    try:
      config_data = yamale.make_data(abs_path)[0]
      config_data = config_data[0] if is_iterable(config_data) else config_data

      # Validate the config
      yamale.validate(schema, [(config_data, config_file_path)])

      from .models.configs import ServerConfig
      return ServerConfig.from_dict(config_data)
    except yamale.YamaleError as e:
      msg = ""
      for result in e.results:
        msg += f"Error validating {result.data} with schema {result.schema}\n"
        for error in result.errors:
          msg += f" - {error}\n"
      log_error(msg)
      exit(1)
    except Exception as e:
      log_error(f"Failed to load server config: {e}")
      # Return default config on any error
      from .models.configs import ServerConfig
      return ServerConfig()

  @property
  def config(self):
    if not self._config:
      self._config = self.load_config(self._server_config_file)
    return self._config

  def __getattr__(self, name):
    # Delegate to the loaded config object
    if not hasattr(self, '_config') or self._config is None:
      self._config = self.load_config(self._server_config_file)
    return getattr(self._config, name)
