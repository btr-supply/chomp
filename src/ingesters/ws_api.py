from collections import deque
from hashlib import md5
from asyncio import gather, Task, sleep
import orjson
import websockets
import websockets.protocol
from typing import Callable, Any

from ..models.ingesters import Ingester
from ..models.base import ResourceField
from ..utils import log_error, log_warn, log_debug, select_nested, safe_eval
from .. import state
from ..server.responses import ORJSON_OPTIONS
from ..actions.schedule import scheduler


async def schedule(ing: Ingester) -> list[Task]:

  epochs_by_route: dict[str, deque[dict]] = {}
  default_handler_by_route: dict[str, Callable[..., Any]] = {}
  batched_fields_by_route: dict[str, list[ResourceField]] = {}
  subscriptions = set()

  # sub function (one per route)
  async def subscribe(ing: Ingester, field: ResourceField, route_hash: str):

    url = field.target
    if state.args.verbose:
      log_debug(
          f"Subscribing to {url} for {ing.name}.{field.name}.{ing.interval}..."
      )

    retry_count = 0

    while retry_count <= state.args.max_retries:
      try:
        async with websockets.connect(url) as ws:
          if field.params:
            await ws.send(
                orjson.dumps(field.params, option=ORJSON_OPTIONS)
            )  # send subscription params if any (eg. api key, stream list...)
          # initialize route state for reducers and transformers to use
          epochs_by_route.setdefault(url, deque([{}]))
          while True:
            if ws.state == websockets.protocol.State.CLOSED:
              log_error(
                  f"{url} ws connection closed for {ing.name}.{field.name}...")
              break
            res = await ws.recv()  # poll for data
            res = orjson.loads(res)
            handled: dict[str, dict[str, bool]] = {}
            for field in batched_fields_by_route[route_hash]:
              if field.handler and callable(field.handler):
                handler_name = getattr(field.handler, '__name__',
                                       str(field.handler))
                if not handled.setdefault(handler_name, {}).get(
                    field.selector, False):
                  try:
                    data = select_nested(field.selector, res, field.name)
                    if data:
                      field.handler(
                          data, epochs_by_route[url])  # map data with handler
                      pass
                  except Exception as e:
                    log_warn(
                        f"Failed to handle websocket data from {url} for {ing.name}.{field.name}: {e}"
                    )
                  handled.setdefault(handler_name, {})[field.selector] = True

            # if state.args.verbose: # <-- way too verbose
            #   log_debug(f"Handled websocket data {data} from {url} for {c.name}, route state:\n{epochs_by_route[url][0]}")

        # If we exit the loop without an exception, reset retry count
        retry_count = 0

      except (websockets.exceptions.ConnectionClosedError,
              ConnectionResetError) as e:
        retry_count += 1
        log_error(
            f"Connection error ({e}) occurred. Attempting to reconnect to {url} for {ing.name} (retry {retry_count}/{state.args.max_retries})..."
        )
        if retry_count > state.args.max_retries:
          log_error(
              f"Exceeded max retries ({state.args.max_retries}). Giving up on {url} for {ing.name}."
          )
          break
        sleep_time = state.args.retry_cooldown * retry_count
        await sleep(sleep_time)
      except Exception as e:
        log_error(f"Unexpected error occurred: {e}")
        retry_count += 1
        if retry_count > state.args.max_retries:
          log_error(
              f"Exceeded max retries ({state.args.max_retries}). Giving up on {url} for {ing.name}."
          )
          break
        sleep_time = state.args.retry_cooldown * retry_count
        await sleep(sleep_time)

  # collect function (one per ingester)
  async def ingest(ing: Ingester):
    await ing.pre_ingest()
    # batch of reducers/transformers by route
    # iterate over key/value pairs
    collected_batches = 0
    for route_hash, batch in batched_fields_by_route.items():
      url = batch[0].target
      epochs = epochs_by_route.get(url, None)
      if not epochs or not epochs[0]:
        log_warn(f"Missing state for {ing.name} {url} ingestion, skipping...")
        continue
      collected_batches += 1
      for field in batch:
        # reduce the state to a collectable value
        try:
          field.value = field.reducer(epochs) if field.reducer and callable(
              field.reducer) else None
        except Exception as e:
          log_warn(
              f"Failed to reduce {ing.name}.{field.name} for {url}, epoch attributes maye be missing: {e}"
          )
          continue
        # Keep only the last 32 epochs to prevent memory accumulation
        while len(epochs) > 32:
          epochs.pop()
      epochs.appendleft({})  # new epoch
    if collected_batches > 0:
      await ing.post_ingest(response_data=epochs_by_route)
    else:
      log_warn(
          f"No data collected for {ing.name}, waiting for ws state to aggregate..."
      )

  tasks = []
  for field in ing.fields:
    url = field.target

    # Create a unique key using a hash of the URL and interval
    route_hash = md5(f"{url}:{ing.interval}".encode()).hexdigest()
    if url:
      # make sure that a field handler is defined if a target url is set
      if field.selector and not field.handler:
        if route_hash not in default_handler_by_route:
          raise ValueError(
              f"Missing handler for field {ing.name}.{field.name} (selector {field.selector})"
          )
        log_warn(
            f"Using {field.target} default field handler for {ing.name}...")
        field.handler = default_handler_by_route[route_hash]
        batched_fields_by_route[route_hash].append(field)
        continue
      if field.handler and isinstance(field.handler, str):
        field.handler = safe_eval(field.handler, callable_check=True)
      if field.reducer and isinstance(field.reducer, str):
        try:
          field.reducer = safe_eval(field.reducer, callable_check=True)
        except Exception:
          continue
      # batch the fields by route given we only need to subscribe once per route
      batched_fields_by_route.setdefault(route_hash, []).append(field)
      if route_hash not in default_handler_by_route and field.handler and callable(
          field.handler):
        default_handler_by_route[route_hash] = field.handler
      # TODO: make sure that double subscriptions are not made possible since field.name is not part of the hash anymore
      if field.target_id in subscriptions:
        continue  # only subscribe once per socket route+selector+params combo
      subscriptions.add(field.target_id)
      tasks.append(subscribe(ing, field, route_hash))

  # subscribe all at once, run in the background
  gather(*tasks)

  # register/schedule the ingester
  task = await scheduler.add_ingester(ing, fn=ingest, start=False)
  return [task] if task is not None else []
