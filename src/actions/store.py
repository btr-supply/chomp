from datetime import datetime, timezone
import pickle
import orjson
from typing import Optional

from ..utils.date import floor_utc
from ..utils.format import log_debug
from .. import state
from ..model import Ingester
from ..cache import cache, pub
from ..actions.transform import transform_all
from ..server.responses import ORJSON_OPTIONS
UTC = timezone.utc

async def store(c: Ingester, table="", publish=True, jsonify=False) -> list | None:
  data = pickle.dumps(c.values_dict()) if not jsonify else orjson.dumps(c.values_dict(), default=str, option=ORJSON_OPTIONS)
  if not jsonify:
    # For pickled data, pass it as the value with pickled=True
    await cache(c.name, c.values_dict(), pickled=True) # max expiry
  else:
    # For JSON data, pass as string
    await cache(c.name, data.decode() if isinstance(data, bytes) else str(data))
  if publish:
    await pub(c.name, data.decode() if isinstance(data, bytes) else str(data))
  if c.resource_type != "value":
    return await state.tsdb.insert(c, table)
  if state.args.verbose:
    log_debug(f"Ingested and stored {c.name}-{c.interval}")
  return None

async def store_batch(c: Ingester, values: list, from_date: datetime, to_date: Optional[datetime], aggregation_interval=None) -> dict:
  if not to_date:
    to_date = datetime.now(UTC)
  if c.resource_type == "value":
    raise ValueError("Cannot store batch for inplace value ingesters (series data required)")
  ok = await state.tsdb.insert_many(c, values, from_date, to_date, aggregation_interval)
  if state.args.verbose:
    log_debug(f"Ingested and stored {len(values)} values for {c.name}-{c.interval} [{from_date} -> {to_date}]")
  return ok

async def transform_and_store(c: Ingester, table="", publish=True, jsonify=False):
  if await transform_all(c) > 0:
    c.last_ingested = floor_utc(c.interval)
    await store(c, table, publish, jsonify)
  else:
    log_debug(f"No new values for {c.name}")
