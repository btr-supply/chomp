from asyncio import gather
from datetime import datetime, timezone
from typing import Any, Optional
from os import environ as env

from ..utils import log_error, log_info, log_warn, Interval, ago, now
from ..utils.http import get, post
from ..models.base import Tsdb
from ..models.ingesters import Ingester, UpdateIngester

UTC = timezone.utc

# Prometheus/VictoriaMetrics interval mapping for queries
INTERVALS: dict[str, str] = {
    "s1": "1s",
    "s2": "2s",
    "s5": "5s",
    "s10": "10s",
    "s15": "15s",
    "s20": "20s",
    "s30": "30s",
    "m1": "1m",
    "m2": "2m",
    "m5": "5m",
    "m10": "10m",
    "m15": "15m",
    "m30": "30m",
    "h1": "1h",
    "h2": "2h",
    "h4": "4h",
    "h6": "6h",
    "h8": "8h",
    "h12": "12h",
    "D1": "1d",
    "D2": "2d",
    "D3": "3d",
    "W1": "1w",
    "M1": "4w",
    "Y1": "1y"
}


class PrometheusAdapter(Tsdb):
  """
  Base adapter for Prometheus-compatible time series databases.
  This includes Prometheus itself, VictoriaMetrics, and other compatible systems.
  """

  def __init__(self, host: str, port: int, db: str, user: str, password: str):
    super().__init__(host, port, db, user, password)
    self._setup_urls()

  def _setup_urls(self):
    """Setup API URLs. Can be overridden by subclasses for different endpoints."""
    self.base_url = f"http://{self.host}:{self.port}"
    self.insert_url = f"{self.base_url}/import/prometheus"
    self.query_url = f"{self.base_url}/query"
    self.query_range_url = f"{self.base_url}/query_range"
    self.health_url = f"{self.base_url}/health"
    self.label_values_url = f"{self.base_url}/label/__name__/values"

  @classmethod
  async def connect(cls,
                    host: Optional[str] = None,
                    port: Optional[int] = None,
                    db: Optional[str] = None,
                    user: Optional[str] = None,
                    password: Optional[str] = None) -> "PrometheusAdapter":
    self = cls(host=host or env.get("PROMETHEUS_HOST") or "localhost",
               port=int(port or env.get("PROMETHEUS_PORT") or 9090),
               db=db or env.get("PROMETHEUS_DB") or "default",
               user=user or env.get("DB_RW_USER") or "",
               password=password or env.get("DB_RW_PASS") or "")
    await self.ensure_connected()
    return self

  async def ping(self) -> bool:
    try:
      await self.ensure_connected()
      response = await get(self.health_url,
                           user=self.user,
                           password=self.password)
      return response.status_code == 200
    except Exception as e:
      log_error("Prometheus ping failed", e)
      return False

  async def ensure_connected(self):
    # Connection is handled by singleton HTTP client
    log_info(
        f"Connected to Prometheus-compatible DB on {self.host}:{self.port}")

  async def close(self):
    # Singleton client is managed globally
    pass

  async def create_db(self,
                      name: str,
                      options: dict = {},
                      force: bool = False):
    # Prometheus doesn't have separate databases, everything is in one namespace
    pass

  async def use_db(self, db: str):
    # Prometheus doesn't have separate databases
    pass

  async def create_table(self, ing: Ingester, name: str = ""):
    # Prometheus doesn't require table creation
    # Metrics are created automatically when data is inserted
    pass

  async def upsert(self, ing: UpdateIngester, table: str = "", uid: str = ""):
    """Upsert for Prometheus - same as insert since metrics are time-series based."""
    log_warn(
        "Prometheus doesn't support upsert operations - using insert instead")
    await self.insert(ing, table)

  async def fetch_by_id(self, table: str, uid: str):
    """Fetch by ID not supported in Prometheus - it's a time-series database."""
    log_warn("fetch_by_id not supported for Prometheus time-series database")
    return None

  async def fetchall(self):
    """Fetch all results - not applicable for Prometheus."""
    log_warn(
        "fetchall() not applicable for Prometheus - use specific query methods"
    )
    return []

  def _format_metric_name(self, table: str, field_name: str) -> str:
    """Format metric name. Can be overridden by subclasses."""
    return f"{table}_{field_name}"

  def _build_labels(self, ing: Ingester, field) -> list[str]:
    """Build labels for a metric. Can be overridden by subclasses."""
    labels = [f'ingester="{ing.name}"']
    for tag in field.tags:
      # Escape quotes in tag values
      escaped_tag = tag.replace('"', '\\"')
      labels.append(f'tag="{escaped_tag}"')
    return labels

  def _format_prometheus_line(self, metric_name: str, labels: list[str],
                              value: Any, timestamp: int) -> str:
    """Format a single Prometheus line. Can be overridden by subclasses."""
    labels_str = "{" + ",".join(labels) + "}" if labels else ""
    return f"{metric_name}{labels_str} {value} {timestamp}"

  async def insert(self, ing: Ingester, table: str = ""):
    await self.ensure_connected()
    table = table or ing.name

    persistent_data = [field for field in ing.fields if not field.transient]

    # Get timestamp from ts field
    ts_value = None
    for field in persistent_data:
      if field.name == 'ts':
        ts_value = field.value
        break

    # Fallback to last_ingested if no ts field found
    if ts_value is None:
      ts_value = ing.last_ingested

    if ts_value is None:
      raise Exception("No timestamp available for ingester")
    timestamp_seconds = int(ts_value.timestamp())

    # Build Prometheus-format data lines
    lines = []
    for field in persistent_data:
      if field.name == 'ts':  # Skip ts field since it's handled as timestamp
        continue

      metric_name = self._format_metric_name(table, field.name)
      labels = self._build_labels(ing, field)
      line = self._format_prometheus_line(metric_name, labels, field.value,
                                          timestamp_seconds)
      lines.append(line)

    prometheus_data = "\n".join(lines)

    try:
      response = await post(self.insert_url,
                            content=prometheus_data,
                            headers={"Content-Type": "text/plain"},
                            user=self.user if self.user else None,
                            password=self.password if self.password else None)
      if response.status_code not in [200, 204]:
        error_text = response.text
        raise Exception(
            f"Prometheus insert failed: {response.status_code} - {error_text}")
    except Exception as e:
      log_error("Failed to insert data into Prometheus", e)
      raise e

  async def insert_many(self,
                        ing: Ingester,
                        values: list[tuple],
                        table: str = ""):
    await self.ensure_connected()
    table = table or ing.name

    persistent_fields = [field for field in ing.fields if not field.transient]

    # Build Prometheus-format data lines for all values
    lines = []
    for value_tuple in values:
      timestamp = value_tuple[0]
      if isinstance(timestamp, datetime):
        timestamp_seconds = int(timestamp.timestamp())
      else:
        timestamp_seconds = int(timestamp)

      for i, field in enumerate(persistent_fields):
        field_value = value_tuple[i + 1]  # Skip timestamp at index 0
        metric_name = self._format_metric_name(table, field.name)
        labels = self._build_labels(ing, field)
        line = self._format_prometheus_line(metric_name, labels, field_value,
                                            timestamp_seconds)
        lines.append(line)

    prometheus_data = "\n".join(lines)

    try:
      response = await post(self.insert_url,
                            content=prometheus_data,
                            headers={"Content-Type": "text/plain"},
                            user=self.user,
                            password=self.password)
      if response.status_code not in [200, 204]:
        error_text = response.text
        raise Exception(
            f"Prometheus batch insert failed: {response.status_code} - {error_text}"
        )
    except Exception as e:
      log_error("Failed to batch insert data into Prometheus", e)
      raise e

  async def _get_table_metrics(self, table: str) -> list[str]:
    """Get all metrics for a table. Can be overridden by subclasses."""
    try:
      response = await get(self.label_values_url,
                           user=self.user if self.user else None,
                           password=self.password if self.password else None)
      if response.status_code == 200:
        result = await response.json()
        all_metrics = result.get("data", [])
        # Filter metrics that start with our table name
        table_metrics = [m for m in all_metrics if m.startswith(f"{table}_")]
        return [m.replace(f"{table}_", "") for m in table_metrics]
    except Exception:
      pass
    return []

  def _build_query(self, metric_name: str, step: str) -> str:
    """Build query for a metric. Can be overridden by subclasses."""
    # Use rate() function for counters, avg_over_time() for gauges
    # For simplicity, we'll use avg_over_time() with step interval
    return f'avg_over_time({metric_name}[{step}])'

  async def fetch(self,
                  table: str,
                  from_date: Optional[datetime] = None,
                  to_date: Optional[datetime] = None,
                  aggregation_interval: Interval = "m5",
                  columns: list[str] = []) -> tuple[list[str], list[tuple]]:
    await self.ensure_connected()

    to_date = to_date or now()
    from_date = from_date or ago(from_date=to_date, years=1)

    # Convert to ISO format timestamps
    start_timestamp = from_date.isoformat()
    end_timestamp = to_date.isoformat()

    step = INTERVALS[aggregation_interval]

    # Build queries for each column
    if not columns:
      columns = await self._get_table_metrics(table)

    if not columns:
      return ([], [])

    # Query each metric
    result_columns = ["ts"]
    result_data: list[tuple] = []

    for column in columns:
      metric_name = self._format_metric_name(table, column)
      query = self._build_query(metric_name, step)

      params = {
          "query": query,
          "start": start_timestamp,
          "end": end_timestamp,
          "step": step
      }

      try:
        response = await get(self.query_range_url,
                             params=params,
                             user=self.user,
                             password=self.password)
        if response.status_code != 200:
          error_text = response.text
          log_warn(
              f"Prometheus query failed for {metric_name}: {response.status_code} - {error_text}"
          )
          continue

        result = await response.json()
        data = result.get("data", {})

        if data.get("resultType") == "matrix":
          for series in data.get("result", []):
            values: list[tuple] = series.get("values", [])
            for timestamp_unix, value in values:
              timestamp_dt = datetime.fromtimestamp(float(timestamp_unix), UTC)

              # Find existing row for this timestamp or create new one
              existing_row = None
              for i, row in enumerate(result_data):
                if row[0] == timestamp_dt:
                  existing_row = i
                  break

              if existing_row is not None:
                # Update existing row
                row_list = list(result_data[existing_row])
                if column not in result_columns:
                  result_columns.append(column)
                  row_list.append(float(value))
                else:
                  col_index = result_columns.index(column)
                  row_list[col_index] = float(value)
                result_data[existing_row] = tuple(row_list)
              else:
                # Create new row
                if column not in result_columns:
                  result_columns.append(column)

                # Pad row with None values for missing columns
                new_row: list[Any] = [timestamp_dt
                                      ] + [None] * (len(result_columns) - 2)
                col_index = result_columns.index(column)
                if col_index < len(new_row):
                  new_row[col_index] = float(value)
                else:
                  new_row.append(float(value))

                result_data.append(tuple(new_row))

      except Exception as e:
        log_error(f"Failed to query metric {metric_name} from Prometheus", e)
        continue

    # Sort by timestamp descending
    result_data.sort(key=lambda x: x[0], reverse=True)

    return (result_columns, result_data)

  async def fetch_batch(
      self,
      tables: list[str],
      from_date: Optional[datetime] = None,
      to_date: Optional[datetime] = None,
      aggregation_interval: Interval = "m5",
      columns: list[str] = []) -> tuple[list[str], list[tuple]]:
    results = await gather(*[
        self.fetch(table, from_date, to_date, aggregation_interval, columns)
        for table in tables
    ])

    # Combine results from all tables
    all_columns: list[str] = []
    all_data: list[tuple] = []

    for columns_result, data in results:
      if not all_columns:
        all_columns = columns_result
      all_data.extend(data)

    return (all_columns, all_data)

  async def commit(self):
    # Prometheus commits automatically
    pass

  async def list_tables(self) -> list[str]:
    await self.ensure_connected()

    try:
      # Get all metric names and extract table prefixes
      response = await get(self.label_values_url,
                           user=self.user if self.user else None,
                           password=self.password if self.password else None)
      if response.status_code != 200:
        error_text = response.text
        raise Exception(
            f"Prometheus list metrics failed: {response.status_code} - {error_text}"
        )

      result = await response.json()
      metrics = result.get("data", [])

      # Extract table names (everything before the last underscore)
      tables = set()
      for metric in metrics:
        if "_" in metric:
          table_name = "_".join(metric.split("_")[:-1])
          tables.add(table_name)

      return list(tables)

    except Exception as e:
      log_error("Failed to list tables from Prometheus", e)
      return []

  async def fetch_batch_by_ids(self, table: str,
                               uids: list[str]) -> list[tuple]:
    """Fetch multiple records by their UIDs from Prometheus

    For Prometheus, this is essentially a no-op since it's primarily a metrics database.
    UIDs don't make as much sense in this context, but we can try to query
    for metrics with specific labels matching the UIDs.
    """
    if not uids:
      return []

    try:
      # Construct a query to find metrics with uid labels matching the list
      uid_filter = "|".join(uids)
      query = f'{table}{{uid=~"{uid_filter}"}}'

      params = {"query": query}
      resp = await get(self.query_url,
                       params=params,
                       user=self.user if self.user else None,
                       password=self.password if self.password else None)

      if resp.status_code != 200:
        log_warn(f"Prometheus query failed: {resp.status_code}")
        return []

      result = await resp.json()
      data = result.get("data", {})

      # Convert result to list of tuples format
      rows = []
      if data.get("resultType") == "vector":
        for series in data.get("result", []):
          metric = series.get("metric", {})
          value = series.get("value", [None, None])
          if len(value) >= 2:
            timestamp = value[0]
            val = value[1]
            # Create a tuple with uid, timestamp, value
            uid = metric.get("uid", "")
            rows.append((uid, timestamp, val))

      return rows

    except Exception as e:
      log_error(f"Failed to fetch batch by IDs from Prometheus: {e}")
      return []
