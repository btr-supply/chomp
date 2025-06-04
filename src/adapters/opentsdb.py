# TODO: finish+test

from asyncio import gather
from datetime import datetime, timezone
from os import environ as env
import aiohttp

from ..utils import log_error, log_info, Interval, ago, now
from ..model import Ingester, Tsdb

UTC = timezone.utc

# OpenTSDB interval mapping for aggregations
INTERVALS: dict[Interval, str] = {
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
  "M1": "1n",
  "Y1": "1y"
}

class OpenTsdb(Tsdb):
  session: aiohttp.ClientSession | None = None
  base_url: str

  @classmethod
  async def connect(
    cls,
    host: str | None = None,
    port: int | None = None,
    db: str | None = None,
    user: str | None = None,
    password: str | None = None
  ) -> "OpenTsdb":
    self = cls(
      host=host or env.get("OPENTSDB_HOST") or "localhost",
      port=int(port or env.get("OPENTSDB_PORT") or 4242),
      db=db or env.get("OPENTSDB_DB") or "default",
      user=user or env.get("DB_RW_USER") or "rw",
      password=password or env.get("DB_RW_PASS") or "pass"
    )
    await self.ensure_connected()
    return self

  async def ping(self) -> bool:
    try:
      await self.ensure_connected()
      if self.session is None:
        return False
      async with self.session.get(f"{self.base_url}/api/version") as resp:
        return resp.status == 200
    except Exception as e:
      log_error("OpenTSDB ping failed", e)
      return False

  async def ensure_connected(self):
    if not self.session:
      self.base_url = f"http://{self.host}:{self.port}"
      self.session = aiohttp.ClientSession()
      log_info(f"Connected to OpenTSDB on {self.host}:{self.port}")

  async def close(self):
    if self.session:
      await self.session.close()

  async def create_db(self, name: str, options: dict = {}, force: bool = False):
    # OpenTSDB doesn't have databases like relational DBs
    # Data is organized by metrics and tags
    pass

  async def use_db(self, db: str):
    # OpenTSDB doesn't have databases
    pass

  async def create_table(self, c: Ingester, name: str = ""):
    # OpenTSDB doesn't require table creation
    # Metrics are created automatically when data is inserted
    pass

  async def insert(self, c: Ingester, table: str = ""):
    await self.ensure_connected()
    table = table or c.name

    persistent_data = [field for field in c.fields if not field.transient]
    data_points = []

    if c.last_ingested is None:
      raise Exception("No timestamp available for ingester")
    timestamp = int(c.last_ingested.timestamp())

    for field in persistent_data:
      data_point = {
        "metric": f"{table}.{field.name}",
        "timestamp": timestamp,
        "value": field.value,
        "tags": {
          "ingester": c.name,
          **{tag: tag for tag in field.tags}
        }
      }
      data_points.append(data_point)

    try:
      if self.session is None:
        raise Exception("OpenTSDB session not connected")
      async with self.session.post(
        f"{self.base_url}/api/put",
        json=data_points,
        headers={"Content-Type": "application/json"}
      ) as resp:
        if resp.status not in [200, 204]:
          error_text = await resp.text()
          raise Exception(f"OpenTSDB insert failed: {resp.status} - {error_text}")
    except Exception as e:
      log_error("Failed to insert data into OpenTSDB", e)
      raise e

  async def insert_many(self, c: Ingester, values: list[tuple], table: str = ""):
    await self.ensure_connected()
    table = table or c.name

    persistent_fields = [field for field in c.fields if not field.transient]
    data_points = []

    for value_tuple in values:
      timestamp = int(value_tuple[0].timestamp()) if isinstance(value_tuple[0], datetime) else int(value_tuple[0])

      for i, field in enumerate(persistent_fields):
        data_point = {
          "metric": f"{table}.{field.name}",
          "timestamp": timestamp,
          "value": value_tuple[i + 1],  # Skip timestamp at index 0
          "tags": {
            "ingester": c.name,
            **{tag: tag for tag in field.tags}
          }
        }
        data_points.append(data_point)

    try:
      if self.session is None:
        raise Exception("OpenTSDB session not connected")
      async with self.session.post(
        f"{self.base_url}/api/put",
        json=data_points,
        headers={"Content-Type": "application/json"}
      ) as resp:
        if resp.status not in [200, 204]:
          error_text = await resp.text()
          raise Exception(f"OpenTSDB batch insert failed: {resp.status} - {error_text}")
    except Exception as e:
      log_error("Failed to batch insert data into OpenTSDB", e)
      raise e

  async def fetch(
    self,
    table: str,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    aggregation_interval: Interval = "m5",
    columns: list[str] = []
  ) -> tuple[list[str], list[tuple]]:
    await self.ensure_connected()

    to_date = to_date or now()
    from_date = from_date or ago(from_date=to_date, years=1)

    start_timestamp = int(from_date.timestamp())
    end_timestamp = int(to_date.timestamp())

    downsample_spec = f"{INTERVALS[aggregation_interval]}-avg"

    queries = []
    if not columns:
      # Query all metrics for this table
      queries.append({
        "aggregator": "avg",
        "metric": f"{table}.*",
        "downsample": downsample_spec
      })
    else:
      for column in columns:
        queries.append({
          "aggregator": "avg",
          "metric": f"{table}.{column}",
          "downsample": downsample_spec
        })

    query_params = {
      "start": start_timestamp,
      "end": end_timestamp,
      "queries": queries
    }

    try:
      if self.session is None:
        raise Exception("OpenTSDB session not connected")
      async with self.session.post(
        f"{self.base_url}/api/query",
        json=query_params,
        headers={"Content-Type": "application/json"}
      ) as resp:
        if resp.status != 200:
          error_text = await resp.text()
          raise Exception(f"OpenTSDB query failed: {resp.status} - {error_text}")

        result = await resp.json()

        # Transform OpenTSDB response to match expected format
        if not result:
          return (columns or [], [])

        # Extract column names and data
        result_columns = []
        result_data = []

        for metric_result in result:
          metric_name = metric_result["metric"]
          column_name = metric_name.split(".")[-1]  # Extract field name
          result_columns.append(column_name)

          # Convert dps (data points) to list of tuples
          for timestamp, value in metric_result.get("dps", {}).items():
            result_data.append((datetime.fromtimestamp(int(timestamp), UTC), value))

        return (result_columns, result_data)

    except Exception as e:
      log_error("Failed to fetch data from OpenTSDB", e)
      raise e

  async def fetchall(self):
    # OpenTSDB doesn't have a direct "fetch all" concept
    # This would need to query all known metrics
    raise NotImplementedError("fetchall not implemented for OpenTSDB")

  async def fetch_batch(
    self,
    tables: list[str],
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    aggregation_interval: Interval = "m5",
    columns: list[str] = []
  ) -> tuple[list[str], list[tuple]]:
    results = await gather(*[
      self.fetch(table, from_date, to_date, aggregation_interval, columns)
      for table in tables
    ])

    # Combine results from all tables
    all_columns = []
    all_data = []

    for columns, data in results:
      all_columns.extend(columns)
      all_data.extend(data)

    return (all_columns, all_data)

  async def commit(self):
    # OpenTSDB commits data automatically
    pass

  async def list_tables(self) -> list[str]:
    await self.ensure_connected()

    try:
      if self.session is None:
        return []
      async with self.session.get(f"{self.base_url}/api/suggest?type=metrics&max=1000") as resp:
        if resp.status != 200:
          error_text = await resp.text()
          raise Exception(f"OpenTSDB list metrics failed: {resp.status} - {error_text}")

        metrics = await resp.json()

        # Extract table names (everything before the first dot)
        tables = set()
        for metric in metrics:
          if "." in metric:
            table_name = metric.split(".")[0]
            tables.add(table_name)

        return list(tables)

    except Exception as e:
      log_error("Failed to list tables from OpenTSDB", e)
      return []
