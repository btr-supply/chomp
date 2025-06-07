from asyncio import gather
from datetime import datetime, timezone
from os import environ as env
import aiohttp
from typing import Any

from ..utils import log_error, log_info, log_warn, Interval, ago, now
from ..model import Ingester, Tsdb

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
  session: aiohttp.ClientSession | None = None
  base_url: str
  insert_url: str
  query_url: str
  query_range_url: str
  health_url: str
  label_values_url: str

  def __init__(self, host: str, port: int, db: str, user: str, password: str):
    super().__init__(host, port, db, user, password)
    self._setup_urls()

  def _setup_urls(self):
    """Setup API URLs. Can be overridden by subclasses for different endpoints."""
    self.base_url = f"http://{self.host}:{self.port}"
    self.insert_url = f"{self.base_url}/api/v1/import/prometheus"
    self.query_url = f"{self.base_url}/api/v1/query"
    self.query_range_url = f"{self.base_url}/api/v1/query_range"
    self.health_url = f"{self.base_url}/health"
    self.label_values_url = f"{self.base_url}/api/v1/label/__name__/values"

  @classmethod
  async def connect(cls,
                    host: str | None = None,
                    port: int | None = None,
                    db: str | None = None,
                    user: str | None = None,
                    password: str | None = None) -> "PrometheusAdapter":
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
      if self.session is None:
        return False
      async with self.session.get(self.health_url) as resp:
        return resp.status == 200
    except Exception as e:
      log_error("Prometheus ping failed", e)
      return False

  async def ensure_connected(self):
    if not self.session:
      # Create session with basic auth if credentials provided
      auth = None
      if self.user and self.password:
        auth = aiohttp.BasicAuth(self.user, self.password)

      self.session = aiohttp.ClientSession(auth=auth)
      log_info(
          f"Connected to Prometheus-compatible DB on {self.host}:{self.port}")

  async def close(self):
    if self.session:
      await self.session.close()

  async def create_db(self,
                      name: str,
                      options: dict = {},
                      force: bool = False):
    # Prometheus doesn't have separate databases, everything is in one namespace
    pass

  async def use_db(self, db: str):
    # Prometheus doesn't have separate databases
    pass

  async def create_table(self, c: Ingester, name: str = ""):
    # Prometheus doesn't require table creation
    # Metrics are created automatically when data is inserted
    pass

  def _format_metric_name(self, table: str, field_name: str) -> str:
    """Format metric name. Can be overridden by subclasses."""
    return f"{table}_{field_name}"

  def _build_labels(self, c: Ingester, field) -> list[str]:
    """Build labels for a metric. Can be overridden by subclasses."""
    labels = [f'ingester="{c.name}"']
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

  async def insert(self, c: Ingester, table: str = ""):
    await self.ensure_connected()
    table = table or c.name

    persistent_data = [field for field in c.fields if not field.transient]

    # Convert timestamp to Unix timestamp (seconds)
    if c.last_ingested is None:
      raise Exception("No timestamp available for ingester")
    timestamp_seconds = int(c.last_ingested.timestamp())

    # Build Prometheus-format data lines
    lines = []
    for field in persistent_data:
      metric_name = self._format_metric_name(table, field.name)
      labels = self._build_labels(c, field)
      line = self._format_prometheus_line(metric_name, labels, field.value,
                                          timestamp_seconds)
      lines.append(line)

    prometheus_data = "\n".join(lines)

    try:
      if self.session is None:
        raise Exception("Session not initialized")
      async with self.session.post(self.insert_url,
                                   data=prometheus_data,
                                   headers={"Content-Type":
                                            "text/plain"}) as resp:
        if resp.status not in [200, 204]:
          error_text = await resp.text()
          raise Exception(
              f"Prometheus insert failed: {resp.status} - {error_text}")
    except Exception as e:
      log_error("Failed to insert data into Prometheus", e)
      raise e

  async def insert_many(self,
                        c: Ingester,
                        values: list[tuple],
                        table: str = ""):
    await self.ensure_connected()
    table = table or c.name

    persistent_fields = [field for field in c.fields if not field.transient]

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
        labels = self._build_labels(c, field)
        line = self._format_prometheus_line(metric_name, labels, field_value,
                                            timestamp_seconds)
        lines.append(line)

    prometheus_data = "\n".join(lines)

    try:
      if self.session is None:
        raise Exception("Session not initialized")
      async with self.session.post(self.insert_url,
                                   data=prometheus_data,
                                   headers={"Content-Type":
                                            "text/plain"}) as resp:
        if resp.status not in [200, 204]:
          error_text = await resp.text()
          raise Exception(
              f"Prometheus batch insert failed: {resp.status} - {error_text}")
    except Exception as e:
      log_error("Failed to batch insert data into Prometheus", e)
      raise e

  async def _get_table_metrics(self, table: str) -> list[str]:
    """Get all metrics for a table. Can be overridden by subclasses."""
    try:
      if self.session is None:
        return []
      async with self.session.get(self.label_values_url) as resp:
        if resp.status == 200:
          result = await resp.json()
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
                  from_date: datetime | None = None,
                  to_date: datetime | None = None,
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
        if self.session is None:
          continue
        async with self.session.get(self.query_range_url,
                                    params=params) as resp:
          if resp.status != 200:
            error_text = await resp.text()
            log_warn(
                f"Prometheus query failed for {metric_name}: {resp.status} - {error_text}"
            )
            continue

          result = await resp.json()
          data = result.get("data", {})

          if data.get("resultType") == "matrix":
            for series in data.get("result", []):
              values: list[tuple] = series.get("values", [])
              for timestamp_unix, value in values:
                timestamp_dt = datetime.fromtimestamp(float(timestamp_unix),
                                                      UTC)

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

  async def fetchall(self):
    # Prometheus doesn't have a universal "fetch all" - need table name
    raise NotImplementedError("fetchall requires table name for Prometheus")

  async def fetch_batch(
      self,
      tables: list[str],
      from_date: datetime | None = None,
      to_date: datetime | None = None,
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
      if self.session is None:
        return []
      async with self.session.get(self.label_values_url) as resp:
        if resp.status != 200:
          error_text = await resp.text()
          raise Exception(
              f"Prometheus list metrics failed: {resp.status} - {error_text}")

        result = await resp.json()
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
