from datetime import datetime, timezone
from typing import Optional, Union, List, Any, Dict

from ..models.ingesters import Ingester, UpdateIngester
from .. import state
from ..utils import now, Interval, log_error, log_warn

UTC = timezone.utc


def _format_record(record: Any,
                   columns: Optional[List[str]] = None) -> Dict[str, Any]:
  """Convert various record formats to a standardized dict format."""
  if isinstance(record, dict):
    return record
  if columns:
    return dict(zip(columns, record))
  return {"data": record}


async def load_resource(
    table_name: str,
    uid: Optional[str] = None,
    uids: Optional[List[str]] = None,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    aggregation_interval: Optional[Interval] = None,
    limit: int = 100,
    offset: int = 0) -> Union[Dict[str, Any], List[Dict[str, Any]], None]:
  """
  Load resources from the database.

  This function serves as a unified data loader, handling retrieval by:
  - Single UID (`uid`)
  - Multiple UIDs (`uids`)
  - Time range (`from_date`, `to_date`)
  - Bulk with pagination (`limit`, `offset`)

  It returns data as plain dictionaries for performance and flexibility.
  """
  try:
    if uid:
      result = await state.tsdb.fetch_by_id(table_name, uid)
      return _format_record(result) if result else None

    if uids:
      results = await state.tsdb.fetch_batch_by_ids(table_name, uids)
      return [_format_record(r) for r in results] if results else []

    # Default to time-series or bulk fetch
    start = from_date or datetime(2020, 1, 1, tzinfo=UTC)
    end = to_date or now()
    interval = aggregation_interval or "1h"

    columns, results = await state.tsdb.fetch(table_name, start, end, interval,
                                              [])

    if not results:
      return []

    records = [_format_record(row, columns) for row in results]
    return records[offset:offset + limit]

  except Exception as e:
    log_error(f"Failed to load from {table_name}: {e}")
    # Ensure correct return type on failure for different query types
    return [] if (uids is not None or from_date or to_date or
                  (uid is None and uids is None)) else None


async def load_resource_by_time_range(
    table_name: str,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    aggregation_interval: Optional[Interval] = None) -> List[Dict[str, Any]]:
  """
  Load time series data by time range.

  Optimized for time series queries. Returns raw data dictionaries for
  maximum performance.

  Args:
    table_name: Name of the time series table
    from_date: Start date (defaults to now if None)
    to_date: End date (defaults to now if None)
    aggregation_interval: Aggregation interval (defaults to "1h")

  Returns:
    List of data dictionaries
  """
  try:
    from_date = from_date or now()
    to_date = to_date or now()
    interval = aggregation_interval or "1h"

    columns, results = await state.tsdb.fetch(table_name, from_date, to_date,
                                              interval, [])
    return [_format_record(row, columns) for row in results] if results else []

  except Exception as e:
    log_error(f"Failed to load time series data from {table_name}: {e}")
    return []


# === Legacy Compatibility Layer (DEPRECATED) ===
# These functions are maintained for backward compatibility but should not be used
# in new code. Use load_resource and load_resource_by_time_range instead.


async def load_one(ing: Ingester) -> Optional[Any]:
  """
  DEPRECATED: Legacy function for loading single ingester values.

  Use load_resource(ing.name, uid=specific_uid) instead for better performance.
  This function loads from cache which may not always be the desired behavior.
  """
  log_warn(
      f"load_one() is deprecated. Use load_resource('{ing.name}', uid=...) instead"
  )

  from .. import cache
  try:
    cached_data = await cache.get_cache(ing.id)
    if cached_data:
      ing.load_values(cached_data)
      return ing
    return None
  except Exception as e:
    log_error(f"Failed to load cached data for {ing.name}: {e}")
    return None


async def load(
    ing: Union[Ingester, UpdateIngester],
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    aggregation_interval: Optional[Interval] = None,
    uids: Optional[List[str]] = None
) -> Optional[Union[List, dict, Ingester, UpdateIngester]]:
  """
  DEPRECATED: Legacy ingester loading function with expensive object reconstruction.

  This function converts database records back into full ingester objects, which is
  expensive and often unnecessary. Consider these alternatives:

  For UpdateIngester:
    - Use load_resource(ing.name, uid=ing.uid) to get raw data dict
    - Use load_resource(ing.name, uids=uids) for batch loading

  For TimeSeriesIngester:
    - Use load_resource_by_time_range(ing.name, from_date, to_date, interval)

  These alternatives return plain data dictionaries which are much faster to work with.
  """
  log_warn(
      "load() is deprecated. Use load_resource() or load_resource_by_time_range() instead"
  )

  if isinstance(ing, UpdateIngester):
    # Legacy path: expensive object reconstruction
    results = await load_resource(
        ing.name, uid=ing.uid if ing.uid and not uids else None, uids=uids)

    if not results:
      return [] if uids is not None or not ing.uid else None

    def _convert_to_ingester(record_data: Dict[str, Any]) -> UpdateIngester:
      """Convert record data to ingester object using the unified factory."""
      if "data" in record_data:
        # Handle tuple-based records from the database
        field_names = [f.name for f in ing.fields]
        data = dict(zip(field_names, record_data["data"]))
      else:
        # Handle dictionary-based records
        data = record_data
      return ing.from_dict(data)

    # Convert to ingester objects (expensive!)
    if isinstance(results, list):
      return [_convert_to_ingester(row) for row in results]
    else:
      return _convert_to_ingester(results)

  else:
    # Time series ingester - try cache first
    from .. import cache
    cached = await cache.get_cache(f"ingester:{ing.name}")
    if cached:
      return cached

    # Fallback to database
    try:
      data = await load_resource_by_time_range(
          ing.name, from_date, to_date, aggregation_interval or ing.interval)
      return data[0] if data else None
    except Exception as e:
      log_error(f"Failed to load data for {ing.name}: {e}")
      return None
