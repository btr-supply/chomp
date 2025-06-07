from asyncio import Task, gather, sleep
from hashlib import md5
import orjson
import httpx
from typing import Any, Optional

from ..utils import log_debug, log_error, log_warn, select_nested
from ..model import Ingester, Monitor
from ..cache import ensure_claim_task, get_or_set_cache
from ..actions.schedule import scheduler
from ..actions.store import transform_and_store
from ..utils import safe_eval
from .. import state

# reusable AsyncClient proxy for better performance (connection pooling)
class HTTPIngester:
  def __init__(self):
    self.session = None

  async def get_session(self) -> httpx.AsyncClient:
    if self.session is None or self.session.is_closed:
      limits = httpx.Limits(max_keepalive_connections=512, max_connections=512)
      self.session = httpx.AsyncClient(
        verify=False,
        timeout=httpx.Timeout(30.0, connect=10.0),
        limits=limits,
        follow_redirects=True
      )
    return self.session

  async def close(self):
    if self.session and not self.session.is_closed:
      await self.session.aclose()

http_ingester = HTTPIngester()

async def fetch_json(url: str, retry_delay: float = 2.5, max_retries: int | None = None, monitor: Optional['Monitor'] = None) -> str:
  """
  Fetch JSON data from URL
  Returns: response_text
  """
  if max_retries is None:
    max_retries = getattr(state.args, 'max_retries', 3) if hasattr(state, 'args') else 3

  for attempt in range(max_retries):
    if monitor:
      monitor.start_timer()

    try:
      if state.args.verbose:
        log_debug(f"Fetching {url}" + (f" (attempt {attempt + 1})" if attempt > 0 else ""))

      session = await http_ingester.get_session()

      response = await session.get(url)
      if response.status_code == 200:
        response_text = response.text

        if monitor:
          monitor.stop_timer(len(response_text.encode('utf-8')), response.status_code)

        return response_text
      log_error(f"HTTP error {response.status_code} for {url}")

      if monitor:
        monitor.stop_timer(0, response.status_code)

    except Exception as e:
      if monitor:
        monitor.stop_timer(0, None)

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
                # Create a wrapper for the cache that returns just the data string
        async def fetch_data_only():
          data_str = await fetch_json(url, monitor=c.monitor)
          return data_str

        data_str = await get_or_set_cache(hashes[url], fetch_data_only, c.interval_sec)
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
