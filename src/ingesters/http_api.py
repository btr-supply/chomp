from asyncio import Task, gather, sleep
from hashlib import md5
import orjson
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from typing import Any
from socket import AddressFamily

from ..utils import log_debug, log_error, log_warn, select_nested
from ..model import Ingester
from ..cache import ensure_claim_task, get_or_set_cache
from ..actions.schedule import scheduler
from ..actions.store import transform_and_store
from ..utils import safe_eval
from .. import state

# reusable ClientSession proxy for better performance (connection pooling)
class HTTPIngester:
  def __init__(self):
    self.session = None

  async def get_session(self) -> ClientSession:
    if self.session is None or self.session.closed:
      timeout = ClientTimeout(total=30, connect=10, sock_connect=10)
      connector = TCPConnector(
        verify_ssl=False,
        family=AddressFamily.AF_UNSPEC, # IPv4 + IPv6
        enable_cleanup_closed=True,
        force_close=False,
        use_dns_cache=True,
        ttl_dns_cache=300, # cache DNS results for 5 minutes to avoid DNS rate limiting
        limit=512,
        limit_per_host=32
      )
      self.session = ClientSession(connector=connector, timeout=timeout)
    return self.session

  async def close(self):
    if self.session and not self.session.closed:
      await self.session.close()

http_ingester = HTTPIngester()

async def fetch_json(url: str, retry_delay: float = 2.5, max_retries: int = state.args.max_retries) -> str:
  for attempt in range(max_retries):
    try:
      if state.args.verbose:
        log_debug(f"Fetching {url}" + (f" (attempt {attempt + 1})" if attempt > 0 else ""))
      session = await http_ingester.get_session()
      async with session.get(url) as response:
        if response.status == 200:
          return await response.text()
        log_error(f"HTTP error {response.status} for {url}")
    except Exception as e:
      if attempt < max_retries - 1:
        log_warn(f"Failed to fetch {url} (attempt {attempt + 1}): {str(e)}")
        await sleep(retry_delay)
      continue
  return ""

async def schedule(c: Ingester) -> list[Task]:

  data_by_route: dict[str, dict] = {}
  hashes: dict[str, str] = {}
  transformed_data_by_route: dict[str, Any] = {} # Store pre-transformed data

  async def ingest(c: Ingester) -> None:
    await ensure_claim_task(c)

    async def fetch_hashed(url: str) -> dict:
      try:
        data_str = await get_or_set_cache(hashes[url], lambda: fetch_json(url), c.interval_sec)
        data = orjson.loads(data_str)
        data_by_route[hashes[url]] = data

        # apply pre_transformer if defined
        if c.pre_transformer and hashes[url] not in transformed_data_by_route:
          transform_fn = safe_eval(c.pre_transformer, callable_check=True)
          transformed_data_by_route[hashes[url]] = transform_fn(data)

        return data
      except Exception as e:
        log_error(f"Failed to fetch/parse JSON response from {url}: {e}")
        return {}

    fetch_tasks = []
    for field in c.fields:
      if field.target:
        url = field.target
        url = url.strip().format(**c.data_by_field) # inject fields inside url if required

        # create a unique key using a hash of the URL and interval
        if url not in hashes:
          hashes[url] = md5(f"{url}:{c.interval}".encode()).hexdigest()

    for url, hash_key in hashes.items():
      if hash_key not in data_by_route or len(data_by_route[hash_key]) == 0:
        data_by_route[hash_key] = {} # initialize empty dict for each unique url to prevent race conditions
        fetch_tasks.append(fetch_hashed(url))

    await gather(*fetch_tasks)

    missing_fields = []
    for field in [f for f in c.fields if f.target]:
      # use transformed data if available, fallback to raw data
      source_data = transformed_data_by_route.get(hashes[field.target], data_by_route[hashes[field.target]])
      field.value = select_nested(field.selector, source_data)
      if not field.value:
        missing_fields.append(field.name)

    if len(missing_fields) > 0:
      log_warn(f"{c.name} missing fields: {', '.join(missing_fields)}")

    # reset local parser cache
    data_by_route.clear()
    transformed_data_by_route.clear()

    await transform_and_store(c)

  # globally register/schedule the ingester
  task = await scheduler.add_ingester(c, fn=ingest, start=False)
  return [task] if task is not None else []
