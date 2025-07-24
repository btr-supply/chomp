from datetime import datetime, timezone
from typing import Optional

from ..utils import floor_utc, now, log_debug
from .. import state
from ..models.ingesters import Ingester, TimeSeriesIngester, UpdateIngester
from ..cache import cache, pub
# Removed import to avoid circular dependency - imported locally where needed

UTC = timezone.utc


async def store(ing: Ingester,
                table: str = "",
                publish: bool = True,
                jsonify: bool = False,
                monitor: bool = True):
  """Store ingester data to database, cache, and optionally publish"""
  # Cache ALL field values (including transient ones)
  all_field_values = ing.get_field_values()

  # Cache and publish all fields (including transient for Redis)
  await cache(ing.name, all_field_values, pickled=not jsonify)
  if publish:
    await pub(ing.name, all_field_values)

  # Insert to database based on ingester type using type-based dispatch
  if isinstance(ing, UpdateIngester):
    result = await state.tsdb.upsert(ing, table, ing.uid)
  elif isinstance(ing, TimeSeriesIngester) or getattr(ing, 'resource_type',
                                                      None) == "timeseries":
    result = await state.tsdb.insert(ing, table)
  else:
    # Handle update type with fallback UID
    uid = getattr(ing, 'uid', ing.name)
    result = await state.tsdb.upsert(ing, table, uid)

  # Store monitor data if available
  monitor_ing = getattr(ing, 'monitor', None)
  if monitor and monitor_ing:
    # Set the monitor ingester timestamp to match the main ingester
    monitor_ing.last_ingested = ing.last_ingested
    await state.tsdb.insert(monitor_ing, f"{ing.name}.monitor")

  if state.args.verbose:
    log_debug(f"Ingested and stored {ing.name}:{ing.interval}")
  return result


async def store_batch(ing: Ingester,
                      values: list,
                      from_date: datetime,
                      to_date: Optional[datetime],
                      aggregation_interval=None) -> dict:
  if not to_date:
    to_date = now()
  if isinstance(ing, UpdateIngester):
    raise ValueError(
        "Cannot store batch for UpdateIngester (use individual upserts for update data)"
    )
  ok = await state.tsdb.insert_many(ing, values, from_date, to_date,
                                    aggregation_interval)
  if state.args.verbose:
    log_debug(
        f"Ingested and stored {len(values)} values for {ing.name}-{ing.interval} [{from_date} -> {to_date}]"
    )
  return ok


async def transform_and_store(ing: Ingester,
                              table="",
                              publish=True,
                              jsonify=False,
                              monitor=True):
  """Transform all fields and store if any values were transformed"""
  from ..actions.transform import transform_all

  if await transform_all(ing) > 0:
    ing.last_ingested = floor_utc(ing.interval)
    await store(ing, table, publish, jsonify, monitor)
    return True
  elif state.args.verbose:
    log_debug(f"No new values for {ing.name}")
  return False
