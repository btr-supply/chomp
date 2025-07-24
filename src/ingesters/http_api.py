from asyncio import Task, gather, sleep
from hashlib import md5
import orjson
from typing import Any, Optional

from ..models.ingesters import Ingester
from ..models.monitors import ResourceMonitor
from ..utils import log_error, log_warn, log_debug, select_nested
from ..utils.http import get
from .. import state
from ..cache import get_or_set_cache
from ..actions.schedule import scheduler
from ..utils import safe_eval


async def fetch_json(url: str,
                     retry_delay: float = 2.5,
                     max_retries: Optional[int] = None,
                     monitor: Optional[ResourceMonitor] = None) -> str:
  """
  Fetch JSON data from URL using the singleton HTTP client
  Returns: response_text
  """
  if max_retries is None:
    max_retries = getattr(state.args, 'max_retries', 3)

  for attempt in range(max_retries):
    if monitor:
      monitor.start_timer()

    try:
      if state.args.verbose:
        log_debug(f"Fetching {url}" +
                  (f" (attempt {attempt + 1})" if attempt > 0 else ""))

      response = await get(url)
      if response.status_code == 200:
        response_text = response.text

        if monitor:
          monitor.stop_timer(len(response_text.encode('utf-8')),
                             response.status_code)

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


async def schedule(ing: Ingester) -> list[Task]:

  data_by_route: dict[str, dict] = {}
  hashes: dict[str, str] = {}
  transformed_data_by_route: dict[str, Any] = {}  # Store pre-transformed data

  async def ingest(ing: Ingester) -> None:
    await ing.pre_ingest()

    async def fetch_hashed(url: str) -> dict:
      try:
        # Create a wrapper for the cache that returns just the data string
        data_str = await get_or_set_cache(hashes[url],
                                          fetch_json(url, monitor=ing.monitor),
                                          ing.interval_sec)
        data = orjson.loads(data_str)
        data_by_route[hashes[url]] = data

        # apply pre_transformer if defined
        if ing.pre_transformer and hashes[url] not in transformed_data_by_route:
          transform_fn = safe_eval(ing.pre_transformer, callable_check=True)
          transformed_data_by_route[hashes[url]] = transform_fn(data)

        return data
      except Exception as e:
        log_error(f"Failed to fetch/parse JSON response from {url}: {e}")
        return {}

    fetch_tasks = []
    for field in ing.fields:
      if field.target:
        url = field.target
        url = url.strip().format(
            **ing.get_field_values())  # inject fields inside url if required

        # create a unique key using a hash of the URL and interval
        if url not in hashes:
          hashes[url] = md5(f"{url}:{ing.interval}".encode()).hexdigest()

    for url, hash_key in hashes.items():
      if hash_key not in data_by_route or len(data_by_route[hash_key]) == 0:
        data_by_route[hash_key] = {
        }  # initialize empty dict for each unique url to prevent race conditions
        fetch_tasks.append(fetch_hashed(url))

    await gather(*fetch_tasks)

    missing_fields = []
    for field in [f for f in ing.fields if f.target]:
      # use transformed data if available, fallback to raw data
      source_data = transformed_data_by_route.get(
          hashes[field.target], data_by_route[hashes[field.target]])
      field.value = select_nested(field.selector, source_data, field.name)
      if not field.value:
        missing_fields.append(field.name)

    if len(missing_fields) > 0:
      log_warn(f"{ing.name} missing fields: {', '.join(missing_fields)}")

    # reset local parser cache
    data_by_route.clear()
    transformed_data_by_route.clear()

    await ing.post_ingest(response_data=data_by_route)

  # globally register/schedule the ingester
  task = await scheduler.add_ingester(ing, fn=ingest, start=False)
  return [task] if task is not None else []
