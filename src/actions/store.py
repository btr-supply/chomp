from datetime import datetime, timezone
import orjson
from typing import Optional

from ..utils.date import floor_utc, now
from ..utils.format import log_debug
from .. import state
from ..model import Ingester, ResourceField
from ..cache import cache, pub
from ..actions.transform import transform_all
from ..server.responses import ORJSON_OPTIONS
UTC = timezone.utc

MONITOR_TABLE_SUFFIX = "_monitor"

def create_monitor(ingester: Ingester) -> Ingester:
  """Create a monitor ingester for storing monitoring data"""
  return Ingester(
    name=f"{ingester.name}{MONITOR_TABLE_SUFFIX}",
    resource_type="timeseries",  # Use valid ResourceType
    interval=ingester.interval,
    fields=[
      ResourceField(name="instance_uid", type="string"),
      ResourceField(name="field_count", type="int32"),
      ResourceField(name="latency_ms", type="float64"),
      ResourceField(name="payload_bytes", type="int64"),
      ResourceField(name="status", type="string"),
    ],
    tags=ingester.tags,
    target=""  # Correct parameter name
  )

async def store(c: Ingester, table="", publish=True, jsonify=False, monitor=True) -> list | None:
  # Cache the data (cache() handles pickled vs non-pickled automatically)
  await cache(c.name, c.values_dict(), pickled=not jsonify)

  # Publish if requested
  if publish:
    json_data = orjson.dumps(c.values_dict(), default=str, option=ORJSON_OPTIONS)
    await pub(c.name, json_data.decode())

  # Insert to time series database if not a simple value type
  result = None
  if c.resource_type != "value":
    result = await state.tsdb.insert(c, table)

    # Store monitor data if ingester has a monitor with vitals
    if monitor and hasattr(c, 'monitor') and c.monitor is not None:
      # Check if monitor has collected vitals from a recent request
      if hasattr(c.monitor, 'vitals'):
        vitals = c.monitor.vitals

        # Create resource monitor ingester with unique name per resource
        resource_monitor_name = f"{c.name}_monitor"

        # Use a dict to cache resource monitor ingesters per resource
        if not hasattr(state, 'resource_monitor_ingesters'):
          state.resource_monitor_ingesters = {}  # type: ignore[attr-defined]

        if resource_monitor_name not in state.resource_monitor_ingesters:  # type: ignore[attr-defined]
          state.resource_monitor_ingesters[resource_monitor_name] = Ingester(  # type: ignore[attr-defined]
            name=resource_monitor_name,
            resource_type="timeseries",
            fields=[
              ResourceField(name="ts", type="timestamp"),
              ResourceField(name="instance_name", type="string"),
              ResourceField(name="field_count", type="int32"),
              ResourceField(name="latency_ms", type="float64"),
              ResourceField(name="response_bytes", type="int64"),
              ResourceField(name="status_code", type="int32"),
            ]
          )

        # Update field values with vitals data
        request_ingester = state.resource_monitor_ingesters[resource_monitor_name]  # type: ignore[attr-defined]
        request_ingester.fields[0].value = now()  # timestamp
        request_ingester.fields[1].value = c.name  # use ingester name
        request_ingester.fields[2].value = len([f for f in c.fields if not f.transient])
        request_ingester.fields[3].value = vitals.latency_ms
        request_ingester.fields[4].value = vitals.response_bytes
        request_ingester.fields[5].value = vitals.status_code or 0
        request_ingester.last_ingested = now()

        # Store using standard flow (cache + time series)
        await store(request_ingester, table=resource_monitor_name, monitor=False)

        # Clear the vitals after storing
        delattr(c.monitor, 'vitals')

  if state.args.verbose:
    log_debug(f"Ingested and stored {c.name}-{c.interval}")
  return result

async def store_batch(c: Ingester, values: list, from_date: datetime, to_date: Optional[datetime], aggregation_interval=None) -> dict:
  if not to_date:
    to_date = datetime.now(UTC)
  if c.resource_type == "value":
    raise ValueError("Cannot store batch for inplace value ingesters (series data required)")
  ok = await state.tsdb.insert_many(c, values, from_date, to_date, aggregation_interval)
  if state.args.verbose:
    log_debug(f"Ingested and stored {len(values)} values for {c.name}-{c.interval} [{from_date} -> {to_date}]")
  return ok

async def transform_and_store(c: Ingester, table="", publish=True, jsonify=False, monitor=True):
  if await transform_all(c) > 0:
    c.last_ingested = floor_utc(c.interval)
    await store(c, table, publish, jsonify, monitor)
  else:
    log_debug(f"No new values for {c.name}")
