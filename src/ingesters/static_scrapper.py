from asyncio import Task, gather
from aiohttp import ClientError, ClientSession
from hashlib import md5
from lxml import html
from bs4 import BeautifulSoup

from ..utils import interval_to_seconds, log_error
from ..model import Ingester
from ..cache import ensure_claim_task, get_or_set_cache
from .. import state
from ..actions import transform_and_store, scheduler

def is_xpath(selector: str) -> bool:
  return selector.startswith(("//", "./"))

async def get_page(url: str) -> str:
  async with ClientSession() as session:
    try:
      async with session.get(url) as response:
        if response.status == 200:
          return await response.text()
        else:
          log_error(f"Failed to fetch page {url}, status code: {response.status}")
    except ClientError as e:
      log_error(f"Error fetching page {url}: {e}")
  return ""

async def schedule(c: Ingester) -> list[Task]:

  # NB: not thread/async safe when multiple ingesters run with same target URL
  pages: dict[str, str] = {}
  soups: dict[str, BeautifulSoup] = {}
  trees: dict[str, html.HtmlElement] = {}
  hashes: dict[str, str] = {}

  async def ingest(c: Ingester):
    await ensure_claim_task(c)

    expiry_sec = interval_to_seconds(c.interval)

    async def fetch_hashed(url: str) -> str:
      h = hashes[url]
      if h in pages:
        return pages[h]
      page = await get_or_set_cache(h, lambda: get_page(url), expiry_sec)
      if not page:
        log_error(f"Failed to fetch page {url}, skipping...")
        return ""

      # cache page both for CSS selection and XPath
      pages[h] = page
      trees[h] = html.fromstring(page)
      soups[h] = BeautifulSoup(page, 'html.parser')
      return page

    urls = set([f.target for f in c.fields if f.target])
    fetch_tasks = []

    for url in urls:
      if url not in hashes:
        hashes[url] = md5(f"{url}:{c.interval}".encode()).hexdigest()
      fetch_tasks.append(fetch_hashed(url))

    await gather(*fetch_tasks)

    def select_field(field):
      h = hashes[field.target]
      if not field.selector:
        return pages[h] # whole page

      # css selector
      if is_xpath(field.selector):
        els = trees[h].xpath(field.selector)
        if not els or len(els) == 0:
          log_error(f"Failed to find element {field.selector} in page {field.target}, skipping...")
          return ""
        # merge all text content from matching selectors
        return "\n".join([e.text_content().lstrip() for e in els])
      else:
        # css selector
        els = soups[h].select(field.selector)
        if not els or len(els) == 0:
          log_error(f"Failed to find element {field.selector} in page {field.target}, skipping...")
          return ""
        # merge all text content from matching selectors
        return "\n".join([e.get_text().lstrip() for e in els])

    tp = state.thread_pool
    futures = [tp.submit(select_field, f) for f in c.fields if f.target] # lxlm and bs4 are sync -> parallelize
    for i in range(len(futures)):
      c.fields[i].value = futures[i].result()

    await transform_and_store(c)

    # reset local caches until next ingestion
    pages.clear()
    soups.clear()
    trees.clear()

  # globally register/schedule the ingester
  task = await scheduler.add_ingester(c, fn=ingest, start=False)
  return [task] if task is not None else []
